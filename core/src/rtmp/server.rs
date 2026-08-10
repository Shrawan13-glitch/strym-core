//! RTMP ingest server — the *publish* side of the product.
//!
//! The crate's transport publishes to a remote RTMP server; this module is the
//! reverse: it accepts a publisher, performs the RTMP handshake and the
//! `connect` / `createStream` / `publish` control plane, then decodes every
//! `audio` / `video` message back into [`MediaPacket`]s and feeds them to a
//! [`PublishHandler`]. That is how an incoming RTMP ingest becomes HLS output,
//! a recording, or any other [`crate::sink::PacketSink`].
//!
//! ```text
//! publisher ──▶ PublishSession ──▶ PublishHandler (HLS / recording / fan-out)
//!                 (handshake +
//!                  control plane +
//!                  FLV tag decoding)
//! ```
//!
//! Robustness is a first-class concern: every parser here is bounds-checked and
//! hostile or malformed input surfaces as an `io::Error`, never a panic. The
//! server answers the protocol-control traffic a real publisher expects (chunk
//! size changes, acknowledgement windows, pings), refuses unknown apps, and
//! ends a session cleanly when the publisher unpublishes or drops the
//! connection. Sockets are given read/write timeouts from [`ServerConfig`] so a
//! silent peer cannot pin a thread forever.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant, SystemTime};

use crate::flv;
use crate::models::MediaPacket;
use crate::rtmp::amf0;
use crate::rtmp::handshake;
use crate::rtmp::{
    frame_message, ChunkReader, Message, CHUNK_SIZE, MAX_PENDING_MESSAGES, MSG_ABORT, MSG_ACK, MSG_AMF0_COMMAND,
    MSG_AMF0_DATA, MSG_AUDIO, MSG_SET_CHUNK_SIZE, MSG_SET_PEER_BW, MSG_USER_CONTROL, MSG_VIDEO, MSG_WINDOW_ACK,
    NEGOTIATED_CHUNK_SIZE, UCM_PING_REQUEST, UCM_PING_RESPONSE,
};
use crate::sink::PacketSink;
use crate::telemetry::Level;

/// Chunk stream id the control plane (commands) is exchanged on.
const CID_COMMAND: u8 = 3;

/// Acknowledgement window the server asks publishers to honor, in bytes.
const ACK_WINDOW: u32 = 2_500_000;

/// The message stream id `createStream` grants. Publish starts on it and all
/// media messages ride it; we only ever create one stream per session.
const PUBLISH_STREAM_ID: u32 = 1;

/// Tuning knobs for an [`RtmpServer`].
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Application name the server accepts on `connect`. A publisher that
    /// connects to another app is refused with `NetConnection.Connect.Rejected`.
    pub app: String,
    /// Budget for the whole handshake + `connect`/`createStream`/`publish`
    /// exchange, and the read/write timeout applied to every socket operation
    /// once streaming (a publisher that stalls past it is dropped). Defaults to
    /// 10 s.
    pub timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            app: "live".to_owned(),
            timeout: Duration::from_secs(10),
        }
    }
}

/// What a completed ingest session was: who connected and to which app/key.
///
/// Produced by the server, read by the caller; `non_exhaustive` so new
/// attributes can be added in a minor release.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SessionInfo {
    /// The app name the publisher connected to (validated against the config).
    pub app: String,
    /// The stream key the publisher named in `publish`.
    pub key: String,
    /// Human-readable peer address, when the socket could provide one.
    pub peer: String,
}

/// Consumes the media a publisher sends. Driven from the connection's read loop
/// only: every method is called sequentially, never concurrently.
pub trait PublishHandler: Send {
    /// Codec configuration change — a new H.264 `AVCDecoderConfigurationRecord`
    /// and/or AAC `AudioSpecificConfig`. Called before the first packet that
    /// needs them; either argument is `None` while that track is absent.
    fn configure(&mut self, avcc: Option<&[u8]>, asc: Option<&[u8]>);

    /// One decoded media packet, in publish order with millisecond timestamps.
    fn packet(&mut self, pkt: &MediaPacket);

    /// The publish ended (`deleteStream`/`closeStream`/`FCUnpublish` or the
    /// connection closed): flush and finalize any container state.
    fn finished(&mut self) {}
}

/// Adapts a [`PacketSink`] (an HLS output, a recording, ...) into a
/// [`PublishHandler`], so ingest drives exactly the same outputs the engine
/// drives. The sink keeps the last-seen AVCC/ASC and forwards configuration
/// merges to the underlying sink, and a failing sink is retired with a log —
/// it stops receiving calls but never takes the ingest connection down
/// (mirroring the engine's sink semantics).
pub struct SinkHandler {
    sink: Box<dyn PacketSink>,
    avcc: Option<Vec<u8>>,
    asc: Option<Vec<u8>>,
    retired: bool,
}

impl SinkHandler {
    /// Wrap a packet sink for ingest.
    pub fn new(sink: Box<dyn PacketSink>) -> Self {
        Self {
            sink,
            avcc: None,
            asc: None,
            retired: false,
        }
    }

    /// Wrap and box in one step, for `MultiSinkHandler`.
    pub fn boxed(sink: Box<dyn PacketSink>) -> Box<Self> {
        Box::new(Self::new(sink))
    }

    /// True once the underlying sink has errored and stopped being fed.
    pub fn is_retired(&self) -> bool {
        self.retired
    }
}

impl PublishHandler for SinkHandler {
    fn configure(&mut self, avcc: Option<&[u8]>, asc: Option<&[u8]>) {
        if self.retired {
            return;
        }
        if let Some(c) = avcc {
            self.avcc = Some(c.to_vec());
        }
        if let Some(c) = asc {
            self.asc = Some(c.to_vec());
        }
        self.sink.codecs(self.avcc.as_deref(), self.asc.as_deref());
    }

    fn packet(&mut self, pkt: &MediaPacket) {
        if self.retired {
            return;
        }
        if let Err(e) = self.sink.packet(pkt) {
            self.retired = true;
            crate::log_event!(Level::Warn, "ingest sink failed, retired", "error" => e.to_string().as_str());
        }
    }

    fn finished(&mut self) {
        if self.retired {
            return;
        }
        self.retired = true;
        if let Err(e) = self.sink.finish() {
            crate::log_event!(Level::Warn, "ingest sink finish failed", "error" => e.to_string().as_str());
        }
    }
}

/// Fans one published stream out to several handlers (an HLS output *and* a
/// recording, say). Each child is independent; a failing child is skipped but
/// never ends the session.
pub struct MultiSinkHandler {
    handlers: Vec<Box<dyn PublishHandler>>,
}

impl MultiSinkHandler {
    /// Start with no handlers.
    pub fn new() -> Self {
        Self { handlers: Vec::new() }
    }

    /// Start with a pre-built set of handlers.
    pub fn with(handlers: Vec<Box<dyn PublishHandler>>) -> Self {
        Self { handlers }
    }

    /// Append a handler.
    pub fn add(&mut self, handler: Box<dyn PublishHandler>) {
        self.handlers.push(handler);
    }

    /// Number of handlers currently attached.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// True when no handlers are attached.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

impl Default for MultiSinkHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl PublishHandler for MultiSinkHandler {
    fn configure(&mut self, avcc: Option<&[u8]>, asc: Option<&[u8]>) {
        for h in &mut self.handlers {
            h.configure(avcc, asc);
        }
    }

    fn packet(&mut self, pkt: &MediaPacket) {
        for h in &mut self.handlers {
            h.packet(pkt);
        }
    }

    fn finished(&mut self) {
        for h in &mut self.handlers {
            h.finished();
        }
    }
}

/// One inbound publish session on top of any `Read + Write` stream — a real
/// socket (via [`RtmpServer`], which applies nodelay + timeouts) or an
/// in-memory duplex in tests.
///
/// Ownership flow: [`negotiate`](Self::negotiate) performs the handshake and
/// control plane and returns the accepted [`SessionInfo`]; then
/// [`pump`](Self::pump) decodes media into a [`PublishHandler`] until the
/// publisher stops. [`serve`](Self::serve) chains the two and hands both the
/// session info and the handler back.
pub struct PublishSession<S: Read + Write> {
    sock: S,
    reader: ChunkReader,
    cfg: ServerConfig,
    /// Chunk size used for this session's outgoing frames.
    outgoing_chunk: usize,
    /// Publisher-requested acknowledgement window in bytes (0 = none).
    window_ack: u32,
    /// `bytes_read` at which the last acknowledgement was sent.
    last_ack_at: u64,
    /// When the session started — the deadline for finishing the handshake.
    started_at: Instant,
    /// Last-seen codec configs, merged across sequence headers.
    avcc: Option<Vec<u8>>,
    asc: Option<Vec<u8>>,
    session: SessionInfo,
}

impl<S: Read + Write> PublishSession<S> {
    /// Wrap any stream. Real sockets should prefer [`PublishSession::for_tcp`]
    /// (or [`RtmpServer::accept`]) so timeouts are applied.
    pub fn new(sock: S, cfg: ServerConfig) -> Self {
        Self {
            sock,
            reader: ChunkReader::new(),
            outgoing_chunk: CHUNK_SIZE,
            window_ack: 0,
            last_ack_at: 0,
            started_at: Instant::now(),
            avcc: None,
            asc: None,
            cfg,
            session: SessionInfo {
                app: String::new(),
                key: String::new(),
                peer: String::new(),
            },
        }
    }

    /// The peer this session is serving (best effort; empty for non-sockets).
    pub fn peer(&self) -> &str {
        &self.session.peer
    }

    /// The app the peer connected to, once `connect` is answered.
    pub fn app(&self) -> &str {
        &self.session.app
    }

    /// The negotiated stream key, once `publish` is answered.
    pub fn key(&self) -> &str {
        &self.session.key
    }

    /// Perform the handshake and control plane, returning what was published.
    /// Fails on a malformed peer, an unknown app, or if nothing is published
    /// within [`ServerConfig::timeout`].
    pub fn negotiate(&mut self) -> io::Result<SessionInfo> {
        self.handshake()?;
        self.announce_limits()?;
        let deadline = self.started_at + self.cfg.timeout;
        loop {
            if Instant::now() >= deadline {
                return Err(timed_out("no publish within the handshake budget"));
            }
            let (name, vals) = self.read_command()?;
            match name.as_str() {
                "connect" => self.answer_connect(&vals)?,
                "releaseStream" | "FCPublish" => self.answer_empty_transaction(&vals)?,
                "createStream" => self.answer_create_stream(&vals)?,
                "publish" => {
                    self.answer_publish(&vals)?;
                    return Ok(self.session.clone());
                }
                _ => {}
            }
        }
    }

    /// Process media into `handler` until the publisher unpublishes or the
    /// connection closes. Returns `Ok` for a clean end (`deleteStream`,
    /// `closeStream`, `FCUnpublish`, or an EOF / reset from the peer); a
    /// timeout or other I/O error is returned.
    pub fn pump<H: PublishHandler>(&mut self, handler: &mut H) -> io::Result<()> {
        loop {
            let msg = match self.reader.read_message(&mut self.sock) {
                Ok(m) => {
                    self.maybe_send_ack()?;
                    m
                }
                Err(e) => {
                    // A dropped connection is the normal way ingest ends.
                    if is_closed(&e) {
                        handler.finished();
                        return Ok(());
                    }
                    return Err(e);
                }
            };
            match msg.mtype {
                MSG_VIDEO | MSG_AUDIO => self.dispatch_media(&msg, handler),
                MSG_AMF0_COMMAND => {
                    if Self::handle_stream_command(&msg, handler) {
                        return Ok(());
                    }
                }
                _ => self.handle_control(&msg)?,
            }
        }
    }

    /// `negotiate` then `pump`; returns the session info and the handler back
    /// (so the caller can inspect the HLS files the session produced, say).
    pub fn serve<H: PublishHandler>(mut self, mut handler: H) -> io::Result<(SessionInfo, H)> {
        let info = self.negotiate()?;
        self.pump(&mut handler)?;
        Ok((info, handler))
    }

    /// The RTMP handshake: read C0/C1, answer S0/S1/S2, drain C2. We serve the
    /// simple handshake (the one ffmpeg, OBS, and our own transport use): S1 is
    /// `time + zeros + random`, S2 echoes the peer's C1 — a digest-capable
    /// client simply falls back to the simple path for its C2, which we ignore.
    fn handshake(&mut self) -> io::Result<()> {
        let mut c0 = [0u8; 1];
        self.sock.read_exact(&mut c0)?;
        if c0[0] != 3 {
            return Err(invalid(format!("peer offered RTMP version {}", c0[0])));
        }
        let mut c1 = [0u8; 1536];
        self.sock.read_exact(&mut c1)?;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as u32);
        let c1_time = u32::from_be_bytes([c1[0], c1[1], c1[2], c1[3]]);
        // A per-connection seed so two sessions in the same second differ.
        let seed = u64::from(c1_time) ^ u64::from(now) ^ 0x9E37_79B9_7F4A_7C15;
        let s1 = handshake::build_s1_simple(now, seed);
        let s2 = handshake::build_s2(&c1);
        self.sock.write_all(&[3])?;
        self.sock.write_all(&s1)?;
        self.sock.write_all(&s2)?;
        self.sock.flush()?;

        let mut c2 = [0u8; 1536];
        self.sock.read_exact(&mut c2)?;
        Ok(())
    }

    /// Tell the publisher how this server sends bytes: a larger chunk size,
    /// our acknowledgement window, and our peer bandwidth limit. The chunk size
    /// message itself goes out at the old 128-byte chunking, then we switch.
    fn announce_limits(&mut self) -> io::Result<()> {
        self.send_message(
            CID_COMMAND,
            MSG_SET_CHUNK_SIZE,
            0,
            0,
            &(NEGOTIATED_CHUNK_SIZE as u32).to_be_bytes(),
        )?;
        self.outgoing_chunk = NEGOTIATED_CHUNK_SIZE;
        self.send_message(CID_COMMAND, MSG_WINDOW_ACK, 0, 0, &ACK_WINDOW.to_be_bytes())?;
        let mut peer_bw = Vec::with_capacity(5);
        peer_bw.extend_from_slice(&ACK_WINDOW.to_be_bytes());
        peer_bw.push(2); // limit type: dynamic
        self.send_message(CID_COMMAND, MSG_SET_PEER_BW, 0, 0, &peer_bw)?;
        Ok(())
    }

    /// Answer `connect`: validate the app, then reply `_result` (with the
    /// echoed transaction id) carrying the server capabilities.
    fn answer_connect(&mut self, vals: &[amf0::Val]) -> io::Result<()> {
        let txn = txn_of(vals).unwrap_or(1.0);
        let app = vals
            .iter()
            .find_map(|v| match v {
                amf0::Val::Object(fields) => str_field(fields, "app").map(str::to_owned),
                _ => None,
            })
            .unwrap_or_default();
        if app != self.cfg.app {
            self.send_error(txn, "NetConnection.Connect.Rejected", &format!("unknown app `{app}`"))?;
            return Err(invalid(format!(
                "publisher asked for app `{app}`, server serves `{}`",
                self.cfg.app
            )));
        }
        self.session.app = app;
        let mut w = amf0::Writer::new();
        w.string("_result").number(txn).null().object(&[
            ("fmsVer", amf0::ObjVal::Str("FMS/3,0,1,123")),
            ("capabilities", amf0::ObjVal::Num(31.0)),
            ("mode", amf0::ObjVal::Num(1.0)),
        ]);
        self.send_command(0, 0, &w.into_bytes())
    }

    /// `releaseStream` / `FCPublish` (sent by ffmpeg before `createStream`):
    /// they expect a plain `_result`.
    fn answer_empty_transaction(&mut self, vals: &[amf0::Val]) -> io::Result<()> {
        let txn = txn_of(vals).unwrap_or(1.0);
        let mut w = amf0::Writer::new();
        w.string("_result").number(txn).null().null();
        self.send_command(0, 0, &w.into_bytes())
    }

    /// Answer `createStream`: grant stream id 1.
    fn answer_create_stream(&mut self, vals: &[amf0::Val]) -> io::Result<()> {
        let txn = txn_of(vals).unwrap_or(2.0);
        let mut w = amf0::Writer::new();
        w.string("_result")
            .number(txn)
            .null()
            .number(f64::from(PUBLISH_STREAM_ID));
        self.send_command(0, 0, &w.into_bytes())
    }

    /// Answer `publish`: record the stream key and confirm with `onStatus`
    /// `NetStream.Publish.Start` on the publish stream.
    fn answer_publish(&mut self, vals: &[amf0::Val]) -> io::Result<()> {
        let Some(amf0::Val::String(key)) = vals.get(3) else {
            return Err(invalid("publish command carried no stream key"));
        };
        self.session.key.clone_from(key);
        let mut w = amf0::Writer::new();
        w.string("onStatus").number(0.0).null().object(&[
            ("level", amf0::ObjVal::Str("status")),
            ("code", amf0::ObjVal::Str("NetStream.Publish.Start")),
            ("description", amf0::ObjVal::Str("Started publishing stream")),
        ]);
        self.send_command(PUBLISH_STREAM_ID, 0, &w.into_bytes())?;
        crate::log_event!(
            Level::Info,
            "ingest publish accepted",
            "app" => self.session.app.as_str(),
            "key" => key.as_str(),
            "peer" => self.session.peer.as_str()
        );
        Ok(())
    }

    /// A command arriving mid-stream. Returns `true` when the command ends the
    /// publish (`deleteStream`/`closeStream`/`FCUnpublish`); anything else is
    /// ignored.
    fn handle_stream_command<H: PublishHandler>(msg: &Message, handler: &mut H) -> bool {
        let mut r = amf0::Reader::new(&msg.payload);
        let vals = r.read_all();
        let Some(amf0::Val::String(name)) = vals.first() else {
            return false;
        };
        match name.as_str() {
            "deleteStream" | "closeStream" | "FCUnpublish" => {
                handler.finished();
                true
            }
            _ => false,
        }
    }

    /// Send an `_error` command carrying `code`/`description` (best effort —
    /// failures to send the refusal don't mask the refusal itself).
    fn send_error(&mut self, txn: f64, code: &str, description: &str) -> io::Result<()> {
        let mut w = amf0::Writer::new();
        w.string("_error").number(txn).null().object(&[
            ("level", amf0::ObjVal::Str("error")),
            ("code", amf0::ObjVal::Str(code)),
            ("description", amf0::ObjVal::Str(description)),
        ]);
        self.send_command(0, 0, &w.into_bytes())
    }

    /// Turn one video/audio message into a handler call.
    fn dispatch_media<H: PublishHandler>(&mut self, msg: &Message, handler: &mut H) {
        let Some(decoded) = flv::decode_tag(msg.mtype, msg.ts, &msg.payload) else {
            crate::log_event!(Level::Debug, "skipping undecodable media message", "mtype" => msg.mtype);
            return;
        };
        match decoded {
            flv::Decoded::VideoConfig(avcc) => {
                self.avcc = Some(avcc.clone());
                handler.configure(Some(&avcc), self.asc.as_deref());
            }
            flv::Decoded::AudioConfig(asc) => {
                self.asc = Some(asc.clone());
                handler.configure(self.avcc.as_deref(), Some(&asc));
            }
            flv::Decoded::Packet(pkt) => handler.packet(&pkt),
        }
    }

    /// React to one protocol-control message from the publisher.
    fn handle_control(&mut self, msg: &Message) -> io::Result<()> {
        let p = &msg.payload;
        match msg.mtype {
            MSG_SET_CHUNK_SIZE if p.len() >= 4 => {
                // The publisher tells us the size of *its* chunks (spec 5.4.1).
                let n = u32::from_be_bytes([p[0], p[1], p[2], p[3]]) as usize;
                self.reader.set_chunk_size(n)?;
            }
            MSG_ABORT if p.len() >= 4 => {
                let cid = u32::from_be_bytes([p[0], p[1], p[2], p[3]]) as usize;
                self.reader.abort(cid);
            }
            MSG_WINDOW_ACK if p.len() >= 4 => {
                self.window_ack = u32::from_be_bytes([p[0], p[1], p[2], p[3]]);
            }
            MSG_SET_PEER_BW if p.len() >= 4 => {
                // Peer bandwidth carries the ack window in its first 4 bytes.
                self.window_ack = u32::from_be_bytes([p[0], p[1], p[2], p[3]]);
            }
            MSG_USER_CONTROL if p.len() >= 2 => {
                let event = u16::from_be_bytes([p[0], p[1]]);
                if event == UCM_PING_REQUEST && p.len() >= 6 {
                    let ts = u32::from_be_bytes([p[2], p[3], p[4], p[5]]);
                    let mut payload = Vec::with_capacity(6);
                    payload.extend_from_slice(&UCM_PING_RESPONSE.to_be_bytes());
                    payload.extend_from_slice(&ts.to_be_bytes());
                    self.send_message(CID_COMMAND, MSG_USER_CONTROL, 0, 0, &payload)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Drain messages until a command/data message arrives; return its command
    /// name and every decoded AMF0 value. Protocol-control messages are handled
    /// (not returned) along the way.
    fn read_command(&mut self) -> io::Result<(String, Vec<amf0::Val>)> {
        for _ in 0..MAX_PENDING_MESSAGES {
            let msg = self.reader.read_message(&mut self.sock)?;
            self.maybe_send_ack()?;
            match msg.mtype {
                MSG_AMF0_COMMAND | MSG_AMF0_DATA => {
                    let mut r = amf0::Reader::new(&msg.payload);
                    let vals = r.read_all();
                    if let Some(amf0::Val::String(name)) = vals.first() {
                        return Ok((name.clone(), vals));
                    }
                }
                _ => self.handle_control(&msg)?,
            }
        }
        Err(invalid("no command from publisher"))
    }

    /// Send an acknowledgement once a full window has been received since the
    /// previous one (spec 5.4.3). Does nothing if the publisher never set one.
    fn maybe_send_ack(&mut self) -> io::Result<()> {
        let received = self.reader.bytes_read();
        let window = u64::from(self.window_ack);
        if window != 0 && received >= self.last_ack_at + window {
            self.send_message(CID_COMMAND, MSG_ACK, 0, 0, &(received as u32).to_be_bytes())?;
            self.last_ack_at = received;
        }
        Ok(())
    }

    /// Frame and send one message to the publisher.
    fn send_message(&mut self, cid: u8, mtype: u8, stream_id: u32, ts: u32, payload: &[u8]) -> io::Result<()> {
        let out = frame_message(cid, mtype, stream_id, ts, payload, self.outgoing_chunk);
        self.sock.write_all(&out)?;
        self.sock.flush()?;
        Ok(())
    }

    /// Send one command message (commands ride chunk stream 3).
    fn send_command(&mut self, stream_id: u32, ts: u32, payload: &[u8]) -> io::Result<()> {
        self.send_message(CID_COMMAND, MSG_AMF0_COMMAND, stream_id, ts, payload)
    }
}

impl PublishSession<TcpStream> {
    /// Wrap a TCP socket with nodelay and the configured read/write timeouts.
    pub fn for_tcp(sock: TcpStream, cfg: ServerConfig) -> io::Result<Self> {
        sock.set_nodelay(true)?;
        sock.set_read_timeout(Some(cfg.timeout))?;
        sock.set_write_timeout(Some(cfg.timeout))?;
        Ok(Self::new(sock, cfg))
    }
}

/// A blocking RTMP ingest server: binds a port and serves one publish session
/// per accepted connection. Concurrency is yours — one thread per
/// [`accept`](Self::accept), or a pool; nothing here is shared mutable state.
pub struct RtmpServer {
    listener: TcpListener,
    cfg: ServerConfig,
}

impl RtmpServer {
    /// Bind the server to `addr` (`"0.0.0.0:1935"`, say). The returned server
    /// is already listening; connect [`RtmpTransport`](crate::rtmp::RtmpTransport)
    /// publishers to its [`local_addr`](Self::local_addr).
    pub fn bind(addr: impl Into<String>, cfg: ServerConfig) -> io::Result<Self> {
        let listener = TcpListener::bind(addr.into())?;
        Ok(Self { listener, cfg })
    }

    /// The address the server is actually listening on (useful with port 0).
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Block until the next publisher connects, and wrap the connection in a
    /// [`PublishSession`] with nodelay and the configured socket timeouts
    /// applied. Call [`PublishSession::serve`] (or `negotiate` + `pump`) on it.
    pub fn accept(&self) -> io::Result<PublishSession<TcpStream>> {
        let (sock, peer) = self.listener.accept()?;
        let mut session = PublishSession::for_tcp(sock, self.cfg.clone())?;
        session.session.peer = peer.to_string();
        Ok(session)
    }

    /// Accept one connection and run it end to end against `handler`, returning
    /// what was published and the handler back — the one-call entry point.
    pub fn serve<H: PublishHandler>(&self, handler: H) -> io::Result<(SessionInfo, H)> {
        self.accept()?.serve(handler)
    }
}

/// A publisher dropping the connection is the normal end of ingest, not an
/// error worth propagating.
fn is_closed(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
    )
}

/// Transaction id of a decoded command (`vals[1]`), if it is a number.
fn txn_of(vals: &[amf0::Val]) -> Option<f64> {
    match vals.get(1) {
        Some(amf0::Val::Number(n)) => Some(*n),
        _ => None,
    }
}

/// First string field named `key` inside an AMF0 object.
fn str_field<'a>(fields: &'a [(String, amf0::Val)], key: &str) -> Option<&'a str> {
    fields.iter().find(|(k, _)| k == key).and_then(|(_, v)| match v {
        amf0::Val::String(s) => Some(s.as_str()),
        _ => None,
    })
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn timed_out(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::MediaKind;
    use crate::rtmp::RtmpConfig;
    use crate::rtmp::RtmpTransport;
    use crate::transport::Transport;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    /// One direction of an in-memory duplex (writes go to `tx`, reads from `rx`).
    struct Half {
        tx: mpsc::Sender<Vec<u8>>,
        rx: mpsc::Receiver<Vec<u8>>,
        buf: Vec<u8>,
    }

    impl Read for Half {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            if self.buf.is_empty() {
                match self.rx.recv() {
                    Ok(chunk) => self.buf = chunk,
                    Err(_) => return Ok(0),
                }
            }
            let n = out.len().min(self.buf.len());
            out[..n].copy_from_slice(&self.buf[..n]);
            self.buf.drain(..n);
            Ok(n)
        }
    }

    impl Write for Half {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.tx
                .send(b.to_vec())
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn pair() -> (Half, Half) {
        let (a_tx, a_rx) = mpsc::channel::<Vec<u8>>();
        let (b_tx, b_rx) = mpsc::channel::<Vec<u8>>();
        (
            Half {
                tx: a_tx,
                rx: b_rx,
                buf: Vec::new(),
            },
            Half {
                tx: b_tx,
                rx: a_rx,
                buf: Vec::new(),
            },
        )
    }

    /// Records everything a publish handler saw.
    #[derive(Default)]
    struct Collect {
        configs: usize,
        avcc: Option<Vec<u8>>,
        asc: Option<Vec<u8>>,
        packets: Vec<MediaPacket>,
        finished_called: bool,
    }

    impl PublishHandler for Collect {
        fn configure(&mut self, avcc: Option<&[u8]>, asc: Option<&[u8]>) {
            self.configs += 1;
            if let Some(c) = avcc {
                self.avcc = Some(c.to_vec());
            }
            if let Some(c) = asc {
                self.asc = Some(c.to_vec());
            }
        }

        fn packet(&mut self, pkt: &MediaPacket) {
            self.packets.push(pkt.clone());
        }

        fn finished(&mut self) {
            self.finished_called = true;
        }
    }

    // --- FLV crafting helpers (mirror the client's test builders) ---

    /// A realistic H.264 `AVCDecoderConfigurationRecord` (used for the seq header).
    const AVCC: &[u8] = &[
        0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x03, 0x67, 0x42, 0x00, 0x0A, 0x01, 0x00, 0x03, 0x68, 0xCE,
    ];

    fn flv_tag(mtype: u8, ts: u32, body: &[u8]) -> Vec<u8> {
        let size = body.len();
        let mut t = Vec::with_capacity(11 + size + 4);
        t.push(mtype);
        t.extend_from_slice(&[(size >> 16) as u8, (size >> 8) as u8, size as u8]);
        t.extend_from_slice(&[(ts >> 16) as u8, (ts >> 8) as u8, ts as u8]);
        t.push(0);
        t.extend_from_slice(&[0, 0, 0]);
        t.extend_from_slice(body);
        t.extend_from_slice(&((11 + size) as u32).to_be_bytes());
        t
    }

    fn flv_header() -> Vec<u8> {
        let mut h = vec![0x46, 0x4c, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09];
        h.extend_from_slice(&[0, 0, 0, 0]);
        h
    }

    /// A video sequence-header tag carrying `AVCC`.
    fn video_seq() -> Vec<u8> {
        let mut body = vec![0x17, 0x00, 0, 0, 0];
        body.extend_from_slice(AVCC);
        flv_tag(9, 0, &body)
    }

    /// An AVC NALU frame tag (packet type 1, zero composition offset).
    fn video_tag(ts: u32, is_key: bool, nal: &[u8]) -> Vec<u8> {
        let mut body = Vec::with_capacity(9 + nal.len());
        body.push(if is_key { 0x17 } else { 0x27 });
        body.push(0x01);
        body.extend_from_slice(&[0, 0, 0]); // composition offset
        body.extend_from_slice(&(nal.len() as u32).to_be_bytes());
        body.extend_from_slice(nal);
        flv_tag(9, ts, &body)
    }

    /// A short but complete publish: metadata, video + audio sequence headers,
    /// then a couple of frames — the same shape the RTMP client test publishes.
    fn sample_flv() -> Vec<u8> {
        let mut b = flv_header();
        let mut md = vec![0x02, 0x00, 0x0A];
        md.extend_from_slice(b"onMetaData");
        md.extend_from_slice(&[0x08, 0, 0, 0, 0]);
        md.extend_from_slice(&[0, 0, 0x09]);
        b.extend(flv_tag(18, 0, &md));
        b.extend(video_seq());
        b.extend(flv_tag(8, 0, &[0xAF, 0x00, 0x12, 0x10]));
        b.extend(video_tag(40, true, &[0x65, 0x88]));
        b.extend(flv_tag(8, 40, &[0xAF, 0x01, 0x21, 0x00]));
        b.extend(video_tag(80, false, &[0x41, 0x77]));
        b
    }

    #[test]
    fn multi_sink_fans_out_everything() {
        /// Counts calls through shared counters so the (boxed) children can be
        /// inspected after being consumed by the fan-out.
        struct Counting {
            configs: Arc<Mutex<usize>>,
            packets: Arc<Mutex<usize>>,
            finished: Arc<Mutex<usize>>,
        }
        impl PublishHandler for Counting {
            fn configure(&mut self, _avcc: Option<&[u8]>, _asc: Option<&[u8]>) {
                *self.configs.lock().unwrap() += 1;
            }
            fn packet(&mut self, _pkt: &MediaPacket) {
                *self.packets.lock().unwrap() += 1;
            }
            fn finished(&mut self) {
                *self.finished.lock().unwrap() += 1;
            }
        }

        let a_configs = Arc::new(Mutex::new(0));
        let a_packets = Arc::new(Mutex::new(0));
        let a_finished = Arc::new(Mutex::new(0));
        let b_configs = Arc::new(Mutex::new(0));
        let b_packets = Arc::new(Mutex::new(0));
        let b_finished = Arc::new(Mutex::new(0));
        let mut fan = MultiSinkHandler::with(vec![
            Box::new(Counting {
                configs: a_configs.clone(),
                packets: a_packets.clone(),
                finished: a_finished.clone(),
            }),
            Box::new(Counting {
                configs: b_configs.clone(),
                packets: b_packets.clone(),
                finished: b_finished.clone(),
            }),
        ]);
        assert_eq!(fan.len(), 2);
        assert!(!fan.is_empty());
        fan.add(Box::new(Collect::default())); // a third child, empty Collect
        assert_eq!(fan.len(), 3);

        let pkt = MediaPacket::audio(5, vec![0x21]);
        fan.configure(Some(&[0x01]), Some(&[0x0A]));
        fan.packet(&pkt);
        fan.finished();

        for (configs, packets, finished) in [
            (&a_configs, &a_packets, &a_finished),
            (&b_configs, &b_packets, &b_finished),
        ] {
            assert_eq!(*configs.lock().unwrap(), 1);
            assert_eq!(*packets.lock().unwrap(), 1);
            assert_eq!(*finished.lock().unwrap(), 1);
        }
    }

    /// Drive a real [`RtmpTransport`] client against a [`PublishSession`] over
    /// an in-memory duplex — the full client/server handshake + control plane
    /// without touching the network.
    fn run_session(server_cfg: ServerConfig, client_cfg: RtmpConfig, flv: &[u8]) -> (io::Result<SessionInfo>, Collect) {
        let (client_half, server_half) = pair();
        let session = PublishSession::new(server_half, server_cfg);
        let server = thread::spawn(move || session.serve(Collect::default()));

        let mut t = RtmpTransport::connect(client_half, client_cfg).expect("client publishes");
        let _ = t.write_all(flv);
        let _ = t.flush();
        let _ = t.shutdown();
        let (info, collect) = server.join().expect("server thread").expect("session served cleanly");
        (Ok(info), collect)
    }

    #[test]
    fn ingest_accepts_a_publish_and_decodes_media() {
        let (info, collect) = run_session(
            ServerConfig::default(),
            RtmpConfig::new("live", "myStream", "rtmp://localhost/live"),
            &sample_flv(),
        );
        let info = info.unwrap();
        assert_eq!(info.app, "live");
        assert_eq!(info.key, "myStream");

        // Two configs (video then audio sequence headers) before any packet.
        assert!(collect.configs >= 2);
        assert_eq!(collect.avcc.as_deref(), Some(AVCC));
        assert_eq!(collect.asc.as_deref(), Some(&[0x12, 0x10][..]));
        // Video key + audio + video inter.
        assert_eq!(collect.packets.len(), 3);
        assert_eq!(collect.packets[0].kind, MediaKind::Video);
        assert!(collect.packets[0].is_key);
        assert_eq!(collect.packets[0].pts, 40);
        assert_eq!(collect.packets[1].kind, MediaKind::Audio);
        assert_eq!(collect.packets[2].kind, MediaKind::Video);
        assert!(!collect.packets[2].is_key);
        assert!(collect.finished_called);
    }

    #[test]
    fn rejects_a_foreign_app() {
        let (client_half, server_half) = pair();
        let mut session = PublishSession::new(server_half, ServerConfig::default());
        let server = thread::spawn(move || session.negotiate());

        let cfg = RtmpConfig::new("other", "key", "rtmp://localhost/other");
        let result = RtmpTransport::connect(client_half, cfg);
        assert!(result.is_err(), "client must see the refusal");
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("NetConnection.Connect.Rejected"), "got: {msg}");

        let err = server.join().unwrap().expect_err("server must refuse");
        assert!(err.to_string().contains("app `other`"), "got: {err}");
    }

    #[test]
    fn malformed_peer_is_rejected_not_panicked() {
        // Garbage bytes instead of an RTMP handshake.
        let mut t = std::io::Cursor::new(b"garbage that is not rtmp at all".to_vec());
        let mut session = PublishSession::new(&mut t, ServerConfig::default());
        let err = session.negotiate().expect_err("garbage must fail");
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn loopback_tcp_serve_reports_session_and_handler() {
        let server = RtmpServer::bind("127.0.0.1:0", ServerConfig::default()).unwrap();
        let addr = server.local_addr().unwrap();
        let thread = thread::spawn(move || server.serve(Collect::default()));

        let cfg = RtmpConfig::new(
            "live".to_string(),
            "loopKey".to_string(),
            format!("rtmp://{}:{}/live", addr.ip(), addr.port()),
        );
        let mut t = RtmpTransport::connect_tcp(&addr.to_string(), cfg).unwrap();
        t.write_all(&sample_flv()).unwrap();
        t.flush().unwrap();
        t.shutdown().unwrap();

        let (info, collect) = thread.join().unwrap().unwrap();
        assert_eq!(info.key, "loopKey");
        assert!(!info.peer.is_empty(), "peer address is recorded");
        assert_eq!(collect.packets.len(), 3);
    }

    #[test]
    fn handshake_budget_expires_without_a_publish() {
        // A peer that finishes the handshake then goes silent must be timed out,
        // not pinned forever. The in-memory duplex blocks on read, so drive a
        // raw TCP socket where the read timeout actually applies.
        let server = RtmpServer::bind(
            "127.0.0.1:0",
            ServerConfig {
                timeout: Duration::from_millis(150),
                ..ServerConfig::default()
            },
        )
        .unwrap();
        let addr = server.local_addr().unwrap();
        let thread = thread::spawn(move || {
            // Accept, do the handshake, then never send the control plane.
            let mut session = server.accept().unwrap();
            let _ = session.handshake();
            session.negotiate()
        });

        // Speak the handshake like a client, then stall.
        let mut sock = TcpStream::connect(addr).unwrap();
        sock.write_all(&[3]).unwrap();
        let (_c0, c1) = crate::rtmp::handshake::build_c1_simple(7);
        sock.write_all(&c1).unwrap();
        let mut s0 = [0u8; 1];
        sock.read_exact(&mut s0).unwrap();
        let mut s1 = [0u8; 1536];
        sock.read_exact(&mut s1).unwrap();
        let mut s2 = [0u8; 1536];
        sock.read_exact(&mut s2).unwrap();
        let c2 = crate::rtmp::handshake::build_c2(&s1);
        sock.write_all(&c2).unwrap();
        // ...and say nothing further.

        let result = thread.join().unwrap();
        assert!(result.is_err(), "negotiate must time out: {result:?}");
    }
}
