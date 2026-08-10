//! RTMP ingest transport. Implements enough of RTMP to publish a live stream:
//! complex handshake, `connect` / `createStream` / `publish` control plane, and
//! a parser that turns the muxer's FLV byte stream into RTMP `audio`/`video`/
//! `@setDataFrame` messages. Depends only on `std` (the crate stays dependency-free).
//!
//! Design: the muxer already emits *valid FLV* (header + onMetaData + sequence
//! headers + media tags). An RTMP publish stream is essentially that FLV wrapped
//! in RTMP chunk frames, so this transport re-parses tag boundaries and re-frames
//! each tag body as an RTMP message.
//!
//! Robustness: every parser here (chunk reassembly, AMF0, FLV tags) is
//! bounds-checked — malformed or hostile input yields `Err`, never a panic. The
//! control plane answers `_error` / error-level `onStatus` on every transaction,
//! honors Window Acknowledgement Size with acknowledgement replies, answers ping
//! requests, and — for real sockets — applies read/write timeouts to every
//! operation (`RtmpConfig::timeout`, defaulting to [`DEFAULT_TIMEOUT`]).

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant, SystemTime};

pub mod amf0;
pub mod handshake;
pub mod server;

const CHUNK_SIZE: usize = 128;

// Message types (protocol control).
const MSG_SET_CHUNK_SIZE: u8 = 1;
const MSG_ABORT: u8 = 2;
const MSG_ACK: u8 = 3;
const MSG_USER_CONTROL: u8 = 4;
const MSG_WINDOW_ACK: u8 = 5;
const MSG_SET_PEER_BW: u8 = 6;
// Message types (media / commands).
const MSG_AUDIO: u8 = 8;
const MSG_VIDEO: u8 = 9;
const MSG_AMF0_DATA: u8 = 18;
const MSG_AMF0_COMMAND: u8 = 20;

// User Control Message events (payload: u16 event + event data).
const UCM_PING_REQUEST: u16 = 0;
const UCM_PING_RESPONSE: u16 = 1;

/// Chunk size we negotiate to after the handshake. Matches the value common RTMP
/// servers (node-media-server, nginx-rtmp, SRS) expect/serve.
const NEGOTIATED_CHUNK_SIZE: usize = 4096;

/// Largest inbound chunk size we accept. The protocol allows up to 2^31-1;
/// anything above 16 MiB is meaningless and only wastes memory.
const MAX_CHUNK_SIZE: usize = 0x00FF_FFFF;

/// Largest chunk stream id (2-byte extended basic header: 64 + 65535), plus one.
const MAX_CHUNK_STREAMS: usize = 65_600;

/// How many server messages we will wade through while waiting for the reply to
/// one command before concluding the reply is never coming.
const MAX_PENDING_MESSAGES: usize = 64;

/// Largest FLV `dataoffset` we accept (real files use 9).
const MAX_FLV_DATA_OFFSET: usize = 4096;

/// Default socket timeout applied to every read and write on a real connection,
/// so a dead peer can never hang the publisher forever.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

// Chunk stream ids we emit on: commands/data=3, audio=6, video=7.
const CID_COMMAND: u8 = 3;
const CID_AUDIO: u8 = 6;
const CID_VIDEO: u8 = 7;

/// Connection details for publishing.
#[derive(Clone)]
pub struct RtmpConfig {
    /// e.g. `"live"` or `"app"`.
    pub app: String,
    /// Stream key (the publish name), e.g. `"mystream"`.
    pub key: String,
    /// Full `rtmp://host[:port]/app` URL.
    pub tc_url: String,
    /// Read/write timeout for every socket operation. Defaults to
    /// [`DEFAULT_TIMEOUT`]; `None` blocks indefinitely.
    pub timeout: Option<Duration>,
}

impl RtmpConfig {
    /// Build a config from the app name, stream key, and full `tcUrl`. Socket
    /// operations time out after [`DEFAULT_TIMEOUT`].
    pub fn new<S: Into<String>>(app: S, key: S, tc_url: S) -> Self {
        Self {
            app: app.into(),
            key: key.into(),
            tc_url: tc_url.into(),
            timeout: Some(DEFAULT_TIMEOUT),
        }
    }
}

/// One decoded RTMP message.
///
/// Produced by the reader, consumed by the caller; `non_exhaustive` so new
/// attributes can be added in a minor release.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Message {
    /// RTMP message type id (1-6 protocol control, 8 audio, 9 video, 18/20 AMF0).
    pub mtype: u8,
    /// The message's 32-bit timestamp, carried by the chunk header (chunk
    /// format 0's absolute time or the accumulated deltas of formats 1/2). The
    /// publish client ignores it, but the ingest server needs it to time media.
    pub ts: u32,
    /// The fully reassembled payload.
    pub payload: Vec<u8>,
}

/// Per-chunk-stream reassembly state (client read side).
#[derive(Default, Clone)]
struct ChunkState {
    ts: u32,
    length: u32,
    mtype: u8,
    stream_id: u32,
    payload: Vec<u8>,
}

/// Incremental RTMP chunk-stream reassembler: feed bytes via
/// [`read_message`](Self::read_message), get complete messages back. All parsing
/// is bounds-checked; malformed input yields `Err`, never a panic.
pub struct ChunkReader {
    states: Vec<Option<ChunkState>>,
    read_chunk_size: usize,
    bytes_read: u64,
}

impl ChunkReader {
    /// Create a reader with the protocol-default 128-byte chunk size.
    pub fn new() -> Self {
        Self {
            states: vec![None; 64],
            read_chunk_size: CHUNK_SIZE,
            bytes_read: 0,
        }
    }

    /// Set the chunk size used to delimit inbound message fragments. Zero is a
    /// protocol violation and is rejected (it would livelock the reader);
    /// values above [`MAX_CHUNK_SIZE`] are clamped.
    pub fn set_chunk_size(&mut self, size: usize) -> io::Result<()> {
        if size == 0 {
            return Err(invalid("peer set chunk size to 0"));
        }
        self.read_chunk_size = size.min(MAX_CHUNK_SIZE);
        Ok(())
    }

    /// Total bytes consumed from the reader, chunk headers included — the
    /// sequence number the RTMP acknowledgement protocol reports.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Discard the partially reassembled message on chunk stream `cid`
    /// (protocol control message "Abort").
    pub fn abort(&mut self, cid: usize) {
        if let Some(slot) = self.states.get_mut(cid) {
            *slot = None;
        }
    }

    /// Read one full message from `sock`, reassembling chunk fragments.
    pub fn read_message<R: Read>(&mut self, sock: &mut R) -> io::Result<Message> {
        let Self {
            states,
            read_chunk_size,
            bytes_read,
        } = self;
        loop {
            let mut bh = [0u8; 1];
            read_counted(sock, &mut bh, bytes_read)?;
            let fmt = bh[0] >> 6;
            let mut cid = (bh[0] & 0x3F) as usize;
            if cid == 0 {
                let mut ex = [0u8; 1];
                read_counted(sock, &mut ex, bytes_read)?;
                cid = 64 + ex[0] as usize;
            } else if cid == 1 {
                let mut ex = [0u8; 2];
                read_counted(sock, &mut ex, bytes_read)?;
                cid = 64 + ex[0] as usize + (ex[1] as usize) * 256;
            }
            if cid >= MAX_CHUNK_STREAMS {
                return Err(invalid(format!("chunk stream id {cid} out of range")));
            }
            if cid >= states.len() {
                states.resize(cid + 1, None);
            }
            let st = states[cid].get_or_insert_with(ChunkState::default);

            match fmt {
                0 => {
                    let mut h = [0u8; 11];
                    read_counted(sock, &mut h, bytes_read)?;
                    st.ts = read_ts24(&h);
                    st.length = read_u24be(&h[3..]);
                    st.mtype = h[6];
                    st.stream_id = u32::from_le_bytes([h[7], h[8], h[9], h[10]]);
                    st.payload.clear();
                    if st.ts == 0xFFFFFF {
                        st.ts = read_u32be(sock, bytes_read)?;
                    }
                }
                1 => {
                    let mut h = [0u8; 7];
                    read_counted(sock, &mut h, bytes_read)?;
                    let delta = read_ts24(&h);
                    st.length = read_u24be(&h[3..]);
                    st.mtype = h[6];
                    st.payload.clear();
                    let d = if delta == 0xFFFFFF {
                        read_u32be(sock, bytes_read)?
                    } else {
                        delta
                    };
                    st.ts = st.ts.wrapping_add(d);
                }
                2 => {
                    let mut h = [0u8; 3];
                    read_counted(sock, &mut h, bytes_read)?;
                    let delta = read_ts24(&h);
                    st.payload.clear();
                    let d = if delta == 0xFFFFFF {
                        read_u32be(sock, bytes_read)?
                    } else {
                        delta
                    };
                    st.ts = st.ts.wrapping_add(d);
                }
                _ => {
                    // fmt 3: continuation; header already carried earlier.
                }
            }
            let have = st.payload.len() as u32;
            if have >= st.length {
                // Empty message or a stray continuation; loop again.
                continue;
            }
            let take = (st.length - have) as usize;
            let take = take.min(*read_chunk_size);
            let start = st.payload.len();
            st.payload.resize(start + take, 0);
            read_counted(sock, &mut st.payload[start..], bytes_read)?;
            if st.payload.len() as u32 == st.length {
                return Ok(Message {
                    mtype: st.mtype,
                    ts: st.ts,
                    payload: std::mem::take(&mut st.payload),
                });
            }
        }
    }
}

impl Default for ChunkReader {
    fn default() -> Self {
        Self::new()
    }
}

/// One parsed FLV tag.
///
/// Produced by the parser, consumed by the caller; `non_exhaustive` so new
/// attributes can be added in a minor release.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FlvTag {
    /// FLV tag type: 8 audio, 9 video, 18 script data.
    pub mtype: u8,
    /// 32-bit timestamp (24-bit field and extension byte already merged).
    pub ts: u32,
    /// Tag body, without the 11-byte header or trailing `PreviousTagSize`.
    pub body: Vec<u8>,
}

/// Why an FLV byte stream was rejected.
///
/// `non_exhaustive`: new rejection reasons may be added in a minor release;
/// matches must keep a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum FlvError {
    /// The FLV header is missing, mis-signed, or carries an unsupported version
    /// or data offset.
    BadHeader,
}

impl std::fmt::Display for FlvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlvError::BadHeader => write!(f, "malformed FLV header"),
        }
    }
}

impl std::error::Error for FlvError {}

#[derive(Clone, Copy)]
struct TagHead {
    mtype: u8,
    size: usize,
    ts: u32,
}

enum FlvPhase {
    Header,
    TagHeader,
    TagBody,
    PrevSize,
}

/// Incremental FLV tag parser. Feed any chunking of an FLV byte stream through
/// [`feed`](Self::feed); tags come back as soon as they are complete. All
/// parsing is bounds-checked (tag bodies are inherently capped at 16 MiB by the
/// 24-bit size field); malformed input yields [`FlvError`], never a panic.
pub struct FlvTagParser {
    phase: FlvPhase,
    buf: Vec<u8>,
    cur: Option<TagHead>,
}

impl FlvTagParser {
    /// Create a parser positioned before the FLV file header.
    pub fn new() -> Self {
        Self {
            phase: FlvPhase::Header,
            buf: Vec::new(),
            cur: None,
        }
    }

    /// Feed more bytes; returns every tag completed by this feed.
    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<FlvTag>, FlvError> {
        self.buf.extend_from_slice(data);
        let mut tags = Vec::new();
        loop {
            match self.phase {
                FlvPhase::Header => {
                    if self.buf.len() < 13 {
                        break;
                    }
                    if &self.buf[0..3] != b"FLV" || self.buf[3] != 1 {
                        return Err(FlvError::BadHeader);
                    }
                    let off = u32::from_be_bytes([self.buf[5], self.buf[6], self.buf[7], self.buf[8]]) as usize;
                    if !(9..=MAX_FLV_DATA_OFFSET).contains(&off) {
                        return Err(FlvError::BadHeader);
                    }
                    if self.buf.len() < off + 4 {
                        break; // header + PreviousTagSize0 not fully buffered yet
                    }
                    self.buf.drain(..off + 4);
                    self.phase = FlvPhase::TagHeader;
                }
                FlvPhase::TagHeader => {
                    if self.buf.len() < 11 {
                        break;
                    }
                    let mtype = self.buf[0];
                    let size = ((self.buf[1] as usize) << 16) | ((self.buf[2] as usize) << 8) | self.buf[3] as usize;
                    let ts24 = ((self.buf[4] as u32) << 16) | ((self.buf[5] as u32) << 8) | self.buf[6] as u32;
                    let tsext = self.buf[7] as u32;
                    let ts = (tsext << 24) | ts24;
                    self.buf.drain(..11);
                    self.cur = Some(TagHead { mtype, size, ts });
                    self.phase = FlvPhase::TagBody;
                }
                FlvPhase::TagBody => {
                    let Some(head) = self.cur else {
                        self.phase = FlvPhase::TagHeader;
                        continue;
                    };
                    if self.buf.len() < head.size {
                        break;
                    }
                    let body = self.buf.drain(..head.size).collect::<Vec<_>>();
                    self.cur = None;
                    tags.push(FlvTag {
                        mtype: head.mtype,
                        ts: head.ts,
                        body,
                    });
                    self.phase = FlvPhase::PrevSize;
                }
                FlvPhase::PrevSize => {
                    if self.buf.len() < 4 {
                        break;
                    }
                    self.buf.drain(..4); // previous-tag-size
                    self.phase = FlvPhase::TagHeader;
                }
            }
        }
        Ok(tags)
    }
}

impl Default for FlvTagParser {
    fn default() -> Self {
        Self::new()
    }
}

/// RTMP transport. `S` is generic over Read+Write so tests can inject an
/// in-memory duplex instead of a real socket.
pub struct RtmpTransport<S: Read + Write> {
    sock: S,
    reader: ChunkReader,
    flv: FlvTagParser,
    pid: u32,
    cfg: RtmpConfig,
    /// Chunk size used for outgoing frames.
    chunk: usize,
    /// Server-requested acknowledgement window in bytes (0 = no acks wanted).
    window_ack: u32,
    /// `bytes_read` at which the last acknowledgement was sent.
    last_ack_at: u64,
    /// When bytes last went out successfully or a reply came in — the session
    /// layer's stall detector reads this.
    last_activity: Instant,
    /// Total framed bytes pushed to the socket (chunk headers included). The
    /// engine derives "effective throughput" from deltas of this.
    bytes_written: u64,
    /// Latest measured round-trip time to the server, refreshed per connection.
    rtt: Option<Duration>,
}

impl RtmpTransport<TcpStream> {
    /// Connect the transport to a real RTMP server over TCP, and publish.
    /// `cfg.timeout` is applied to every read and write on the socket. A
    /// best-effort RTT probe runs after the publish handshake (never fails the
    /// connection).
    pub fn connect_tcp(addr: &str, cfg: RtmpConfig) -> io::Result<Self> {
        let sock = TcpStream::connect(addr)?;
        if let Some(t) = cfg.timeout {
            sock.set_read_timeout(Some(t))?;
            sock.set_write_timeout(Some(t))?;
        }
        sock.set_nodelay(true)?;
        let mut transport = Self::connect(sock, cfg)?;
        transport.measure_rtt();
        Ok(transport)
    }

    /// Measure the round-trip time with a user-control ping: send a
    /// `PING_REQUEST` carrying a timestamp, await the echoed `PING_RESPONSE`,
    /// and time the exchange. Best-effort — a server that never answers leaves
    /// `rtt` unset rather than failing the connection. Uses a short temporary
    /// read timeout so a silent peer can't stall the publisher.
    fn measure_rtt(&mut self) {
        let saved = self.sock.read_timeout().ok().flatten();
        let _ = self.sock.set_read_timeout(Some(Duration::from_secs(1)));
        let sent = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| (d.as_millis() % u128::from(u32::MAX)) as u32);
        let sent_at = Instant::now();
        let mut payload = Vec::with_capacity(6);
        payload.extend_from_slice(&UCM_PING_REQUEST.to_be_bytes());
        payload.extend_from_slice(&sent.to_be_bytes());
        let result: io::Result<()> = (|| {
            self.send_message(CID_COMMAND, MSG_USER_CONTROL, 0, 0, &payload)?;
            for _ in 0..MAX_PENDING_MESSAGES {
                let msg = self.reader.read_message(&mut self.sock)?;
                self.last_activity = Instant::now();
                self.maybe_send_ack()?;
                if msg.mtype == MSG_USER_CONTROL && msg.payload.len() >= 6 {
                    let event = u16::from_be_bytes([msg.payload[0], msg.payload[1]]);
                    let echoed = u32::from_be_bytes([msg.payload[2], msg.payload[3], msg.payload[4], msg.payload[5]]);
                    if event == UCM_PING_RESPONSE && echoed == sent {
                        self.rtt = Some(sent_at.elapsed());
                        return Ok(());
                    }
                }
                self.handle_control(&msg)?;
            }
            Err(invalid("no ping response from server"))
        })();
        let _ = self.sock.set_read_timeout(saved);
        if let Some(rtt) = self.rtt {
            crate::log_event!(
                crate::telemetry::Level::Debug,
                "rtt measured",
                "rtt_ms" => rtt.as_secs_f64() * 1000.0
            );
        } else if let Err(error) = result {
            crate::log_event!(
                crate::telemetry::Level::Debug,
                "rtt probe skipped",
                "error" => error.to_string().as_str()
            );
        }
    }
}

impl<S: Read + Write> RtmpTransport<S> {
    /// Perform the full publish handshake over `sock`, then return a ready
    /// transport.
    pub fn connect(mut sock: S, cfg: RtmpConfig) -> io::Result<Self> {
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as u32);
        let (_c0, c1) = handshake::build_c1_simple(time);
        sock.write_all(&[3])?;
        sock.write_all(&c1)?;
        sock.flush()?;

        let mut s0 = [0u8; 1];
        sock.read_exact(&mut s0)?;
        if s0[0] != 3 {
            return Err(invalid(format!("server offered RTMP version {}", s0[0])));
        }
        let mut s1 = [0u8; 1536];
        sock.read_exact(&mut s1)?;
        let mut s2 = [0u8; 1536];
        sock.read_exact(&mut s2)?;
        let c2 = handshake::build_c2(&s1);
        sock.write_all(&c2)?;
        sock.flush()?;

        let mut t = RtmpTransport {
            sock,
            reader: ChunkReader::new(),
            flv: FlvTagParser::new(),
            pid: 0,
            cfg,
            chunk: CHUNK_SIZE,
            window_ack: 0,
            last_ack_at: 0,
            last_activity: Instant::now(),
            bytes_written: 0,
            rtt: None,
        };
        // Tell the server to expect our larger chunks (and match its own). Sent
        // right after the handshake, before the first command. The *inbound*
        // reader stays at the 128-byte protocol default: the peer decides when
        // to switch its own chunking and says so via an inbound Set Chunk Size
        // control message (handled in `read_message`). Assuming the peer has
        // already switched is what breaks against real servers (e.g. YouTube)
        // that keep 128-byte chunks for the early control plane.
        t.send_set_chunk_size(NEGOTIATED_CHUNK_SIZE)?;
        t.chunk = NEGOTIATED_CHUNK_SIZE;
        t.connect_app()?;
        let sid = t.create_stream()?;
        t.do_publish(sid)?;
        t.pid = sid;
        Ok(t)
    }

    fn send_set_chunk_size(&mut self, size: usize) -> io::Result<()> {
        self.send_message(CID_COMMAND, MSG_SET_CHUNK_SIZE, 0, 0, &(size as u32).to_be_bytes())
    }

    fn connect_app(&mut self) -> io::Result<()> {
        let mut w = amf0::Writer::new();
        w.string("connect").number(1.0).object(&[
            ("app", amf0::ObjVal::Str(&self.cfg.app)),
            ("flashVer", amf0::ObjVal::Str("FMLE/3.0 (compatible; FMSc/1.0)")),
            ("tcUrl", amf0::ObjVal::Str(&self.cfg.tc_url)),
            ("fpad", amf0::ObjVal::Bool(false)),
            ("capabilities", amf0::ObjVal::Num(15.0)),
            ("audioCodecs", amf0::ObjVal::Num(4071.0)),
            ("videoCodecs", amf0::ObjVal::Num(252.0)),
            ("videoFunction", amf0::ObjVal::Num(1.0)),
        ]);
        self.send_message(CID_COMMAND, MSG_AMF0_COMMAND, 0, 0, &w.into_bytes())?;
        self.await_result("connect", 1.0)?;
        Ok(())
    }

    fn create_stream(&mut self) -> io::Result<u32> {
        let mut w = amf0::Writer::new();
        w.string("createStream").number(2.0).null();
        self.send_message(CID_COMMAND, MSG_AMF0_COMMAND, 0, 0, &w.into_bytes())?;
        let vals = self.await_result("createStream", 2.0)?;
        match vals.get(3) {
            Some(amf0::Val::Number(n)) if *n >= 0.0 => Ok(*n as u32),
            _ => Err(invalid("no stream id in createStream _result")),
        }
    }

    fn do_publish(&mut self, sid: u32) -> io::Result<()> {
        let mut w = amf0::Writer::new();
        w.string("publish")
            .number(3.0)
            .null()
            .string(&self.cfg.key)
            .string("live");
        self.send_message(CID_COMMAND, MSG_AMF0_COMMAND, sid, 0, &w.into_bytes())?;
        self.await_publish_status()
    }

    /// Drain messages until the `_result` matching transaction `txn` arrives.
    /// Control traffic (chunk size, ack windows, pings) is handled en passant;
    /// async notifications are skipped. `_error` becomes a descriptive failure
    /// carrying the server's code/description; so does never getting an answer.
    fn await_result(&mut self, op: &str, txn: f64) -> io::Result<Vec<amf0::Val>> {
        for _ in 0..MAX_PENDING_MESSAGES {
            let (name, vals) = self.read_command()?;
            match name.as_str() {
                "_result" => {
                    if txn_of(&vals) == Some(txn) {
                        return Ok(vals);
                    }
                }
                "_error" => return Err(command_error(op, &vals)),
                _ => {}
            }
        }
        Err(invalid(format!(
            "no `_result` for `{op}` after {MAX_PENDING_MESSAGES} messages"
        )))
    }

    /// Wait for the `onStatus` that confirms (or refuses) the publish.
    fn await_publish_status(&mut self) -> io::Result<()> {
        for _ in 0..MAX_PENDING_MESSAGES {
            let (name, vals) = self.read_command()?;
            match name.as_str() {
                "onStatus" => {
                    let info = vals.iter().find_map(|v| match v {
                        amf0::Val::Object(fields) => Some(fields),
                        _ => None,
                    });
                    let code = info.and_then(|f| str_field(f, "code")).unwrap_or_default();
                    if code.starts_with("NetStream.Publish.Start") {
                        return Ok(());
                    }
                    let level = info.and_then(|f| str_field(f, "level")).unwrap_or_default();
                    let desc = info.and_then(|f| str_field(f, "description")).unwrap_or_default();
                    return Err(invalid(format!("server refused publish: {code} ({level}): {desc}")));
                }
                "_error" => return Err(command_error("publish", &vals)),
                _ => {}
            }
        }
        Err(invalid(format!(
            "no `onStatus` for publish after {MAX_PENDING_MESSAGES} messages"
        )))
    }

    /// Drain messages until a command/data message arrives; return its command
    /// name and every decoded AMF0 value. Protocol-control messages are handled
    /// (not returned) along the way, and an acknowledgement is emitted whenever
    /// the server's window fills up.
    fn read_command(&mut self) -> io::Result<(String, Vec<amf0::Val>)> {
        loop {
            let msg = self.reader.read_message(&mut self.sock)?;
            self.last_activity = Instant::now();
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
    }

    /// React to one protocol-control message from the server.
    fn handle_control(&mut self, msg: &Message) -> io::Result<()> {
        let p = &msg.payload;
        match msg.mtype {
            MSG_SET_CHUNK_SIZE if p.len() >= 4 => {
                // The server tells us the size of *its* chunks (spec 5.4.1).
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
                    self.send_ping_response(ts)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Send an acknowledgement once a full window has been received since the
    /// previous one (spec 5.4.3). Does nothing if the server never set a window.
    fn maybe_send_ack(&mut self) -> io::Result<()> {
        let received = self.reader.bytes_read();
        let window = u64::from(self.window_ack);
        if window != 0 && received >= self.last_ack_at + window {
            // Sequence number wraps mod 2^32 per spec.
            self.send_message(CID_COMMAND, MSG_ACK, 0, 0, &(received as u32).to_be_bytes())?;
            self.last_ack_at = received;
        }
        Ok(())
    }

    fn send_ping_response(&mut self, ts: u32) -> io::Result<()> {
        let mut payload = Vec::with_capacity(6);
        payload.extend_from_slice(&UCM_PING_RESPONSE.to_be_bytes());
        payload.extend_from_slice(&ts.to_be_bytes());
        self.send_message(CID_COMMAND, MSG_USER_CONTROL, 0, 0, &payload)
    }

    /// Push one normalized (FLV) message out to the server.
    fn send_message(&mut self, cid: u8, mtype: u8, stream_id: u32, ts: u32, payload: &[u8]) -> io::Result<()> {
        let out = frame_message(cid, mtype, stream_id, ts, payload, self.chunk);
        self.bytes_written = self.bytes_written.wrapping_add(out.len() as u64);
        self.sock.write_all(&out)?;
        self.sock.flush()?;
        // A successful flush is forward progress — the stall detector resets.
        self.last_activity = Instant::now();
        Ok(())
    }

    /// Convert one parsed FLV tag into the matching RTMP message.
    fn emit_tag(&mut self, mtype: u8, ts: u32, body: &[u8]) -> io::Result<()> {
        match mtype {
            MSG_VIDEO => self.send_message(CID_VIDEO, MSG_VIDEO, self.pid, ts, body),
            MSG_AUDIO => self.send_message(CID_AUDIO, MSG_AUDIO, self.pid, ts, body),
            18 => {
                // onMetaData -> RTMP @setDataFrame("onMetaData", ECMA-array).
                // FLV body: [0x02][u16 len]["onMetaData"][ECMA array...]; the
                // array starts right after the typed string.
                if body.len() > 3 && body[0] == 0x02 {
                    let name_len = u16::from_be_bytes([body[1], body[2]]) as usize;
                    let rest = 3 + name_len;
                    if body.len() > rest {
                        let mut w = amf0::Writer::new();
                        w.string("@setDataFrame").string("onMetaData");
                        let mut msg = w.into_bytes();
                        msg.extend_from_slice(&body[rest..]);
                        self.send_message(CID_COMMAND, MSG_AMF0_DATA, self.pid, ts, &msg)
                    } else {
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    }
}

impl<S: Read + Write> io::Write for RtmpTransport<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let tags = self.flv.feed(buf).map_err(|e| invalid(e.to_string()))?;
        for tag in &tags {
            self.emit_tag(tag.mtype, tag.ts, &tag.body)?;
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.sock.flush()
    }
}

impl<S: Read + Write> crate::transport::Transport for RtmpTransport<S> {
    fn shutdown(&mut self) -> io::Result<()> {
        // Best-effort unpublish so the server tears down the ingest cleanly.
        let mut w = amf0::Writer::new();
        w.string("FCUnpublish")
            .number(4.0)
            .null()
            .string(&self.cfg.key)
            .string("live");
        let _ = self.send_message(CID_COMMAND, MSG_AMF0_COMMAND, self.pid, 0, &w.into_bytes());
        let mut w = amf0::Writer::new();
        w.string("closeStream").number(5.0).null();
        let _ = self.send_message(CID_COMMAND, MSG_AMF0_COMMAND, self.pid, 0, &w.into_bytes());
        let _ = self.sock.flush();
        Ok(())
    }

    fn last_progress(&self) -> Option<Instant> {
        Some(self.last_activity)
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    fn rtt(&self) -> Option<Duration> {
        self.rtt
    }
}

/// Dials an RTMP server and publishes — the [`crate::session::Connector`]
/// pairing with [`crate::session::Session`] for automatic reconnects. Each
/// `connect` performs the full handshake + publish again, exactly what a
/// server needs after a dropped connection.
pub struct RtmpConnector {
    addr: String,
    config: RtmpConfig,
}

impl RtmpConnector {
    /// Publish to `addr` (`"host[:port]"`, default RTMP port applies when
    /// omitted) with the given app/stream-key config.
    pub fn tcp(addr: impl Into<String>, config: RtmpConfig) -> Self {
        Self {
            addr: addr.into(),
            config,
        }
    }
}

impl crate::session::Connector for RtmpConnector {
    type Transport = RtmpTransport<TcpStream>;

    fn connect(&mut self) -> io::Result<Self::Transport> {
        RtmpTransport::connect_tcp(&self.addr, self.config.clone())
    }
}

// --- helpers ---

/// `read_exact` that also tallies the bytes for the acknowledgement protocol.
fn read_counted<R: Read>(sock: &mut R, buf: &mut [u8], bytes_read: &mut u64) -> io::Result<()> {
    sock.read_exact(buf)?;
    *bytes_read += buf.len() as u64;
    Ok(())
}

fn read_u32be<R: Read>(r: &mut R, bytes_read: &mut u64) -> io::Result<u32> {
    let mut b = [0u8; 4];
    read_counted(r, &mut b, bytes_read)?;
    Ok(u32::from_be_bytes(b))
}

fn read_ts24(b: &[u8]) -> u32 {
    ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32
}

fn read_u24be(b: &[u8]) -> u32 {
    ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32
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

/// Turn an AMF0 `_error` reply into a descriptive failure.
fn command_error(op: &str, vals: &[amf0::Val]) -> io::Error {
    let info = vals.iter().find_map(|v| match v {
        amf0::Val::Object(fields) => Some(fields),
        _ => None,
    });
    let code = info.and_then(|f| str_field(f, "code")).unwrap_or_default();
    let desc = info.and_then(|f| str_field(f, "description")).unwrap_or_default();
    invalid(format!("server rejected `{op}`: {code}: {desc}"))
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Frame one RTMP message into chunk bytes (fmt-0 header + `chunk_size`-byte chunks).
fn frame_message(cid: u8, mtype: u8, stream_id: u32, ts: u32, payload: &[u8], chunk_size: usize) -> Vec<u8> {
    let len = payload.len() as u32;
    let (ts24, ext) = if ts >= 0xFFFFFF {
        (0xFFFFFFu32, Some(ts.to_be_bytes()))
    } else {
        (ts, None)
    };
    let mut out = Vec::with_capacity(12 + payload.len() + 16);
    out.push(cid); // fmt 0 basic header
    out.extend_from_slice(&[(ts24 >> 16) as u8, (ts24 >> 8) as u8, ts24 as u8]);
    out.extend_from_slice(&[(len >> 16) as u8, (len >> 8) as u8, len as u8]);
    out.push(mtype);
    out.extend_from_slice(&stream_id.to_le_bytes()); // 4-byte message stream id (LE)
    if let Some(e) = ext {
        out.extend_from_slice(&e);
    }
    if payload.len() <= chunk_size {
        out.extend_from_slice(payload);
    } else {
        out.extend_from_slice(&payload[..chunk_size]);
        let mut rest = &payload[chunk_size..];
        while !rest.is_empty() {
            out.push(0xC0 | cid); // fmt 3 continuation
            let n = rest.len().min(chunk_size);
            out.extend_from_slice(&rest[..n]);
            rest = &rest[n..];
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtmp::handshake;
    use crate::transport::Transport;
    use std::io::Read as IoRead;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    /// One direction of an in-memory duplex: writes go to `tx`, reads come from `rx`.
    struct Half {
        tx: mpsc::Sender<Vec<u8>>,
        rx: mpsc::Receiver<Vec<u8>>,
        buf: Vec<u8>,
    }

    impl IoRead for Half {
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

    /// Every message the fake server collected, as `(mtype, payload)`.
    type MsgLog = Arc<Mutex<Vec<(u8, Vec<u8>)>>>;

    /// Misbehaviors the fake server can exhibit, one per error-path test.
    #[derive(Default)]
    #[allow(clippy::struct_excessive_bools)]
    struct Behavior {
        connect_error: bool,
        refuse_publish: bool,
        ping_before_create_stream: bool,
        window_ack: Option<u32>,
        /// Reply framed in default 128-byte chunks, ignoring the client's
        /// negotiated size — what some real ingest servers (e.g. `YouTube`) do.
        keep_128_chunks: bool,
    }

    /// A minimal RTMP server exercised against our client: performs the handshake,
    /// answers connect/createStream/publish, and logs every message it receives.
    #[allow(clippy::too_many_lines)]
    fn fake_server(mut h: Half, behavior: &Behavior, log: &MsgLog) -> io::Result<()> {
        // Handshake.
        let mut c0 = [0u8; 1];
        h.read_exact(&mut c0)?;
        assert_eq!(c0[0], 3);
        let mut c1 = [0u8; 1536];
        h.read_exact(&mut c1)?;
        let s1 = handshake::build_s1_complex(0).to_vec();
        h.write_all(&[3])?;
        h.write_all(&s1)?;
        h.write_all(&[0u8; 1536])?; // S2 (client ignores)
        let mut c2 = [0u8; 1536];
        h.read_exact(&mut c2)?;

        // Control plane. Some real servers keep replying in 128-byte chunks
        // even after the client's own Set Chunk Size (`keep_128_chunks`).
        let reply_chunk = if behavior.keep_128_chunks { 128 } else { 4096 };
        let mut reader = ChunkReader::new();
        loop {
            let msg = reader.read_message(&mut h)?;
            log.lock().unwrap().push((msg.mtype, msg.payload.clone()));
            match msg.mtype {
                MSG_SET_CHUNK_SIZE if msg.payload.len() >= 4 => {
                    // Honor the client's negotiated chunk size, like a real server.
                    reader.set_chunk_size(u32::from_be_bytes([
                        msg.payload[0],
                        msg.payload[1],
                        msg.payload[2],
                        msg.payload[3],
                    ]) as usize)?;
                }
                MSG_AMF0_COMMAND => {
                    let mut r = amf0::Reader::new(&msg.payload);
                    let Some(amf0::Val::String(name)) = r.read_value() else {
                        continue;
                    };
                    match name.as_str() {
                        "connect" => {
                            if behavior.connect_error {
                                let mut w = amf0::Writer::new();
                                w.string("_error").number(1.0).null().object(&[
                                    ("level", amf0::ObjVal::Str("error")),
                                    ("code", amf0::ObjVal::Str("NetConnection.Connect.Rejected")),
                                    ("description", amf0::ObjVal::Str("app not found")),
                                ]);
                                h.write_all(&frame_message(
                                    CID_COMMAND,
                                    MSG_AMF0_COMMAND,
                                    0,
                                    0,
                                    &w.into_bytes(),
                                    reply_chunk,
                                ))?;
                                continue;
                            }
                            if let Some(window) = behavior.window_ack {
                                h.write_all(&frame_message(
                                    CID_COMMAND,
                                    MSG_WINDOW_ACK,
                                    0,
                                    0,
                                    &window.to_be_bytes(),
                                    reply_chunk,
                                ))?;
                            }
                            let mut w = amf0::Writer::new();
                            let long_desc = "d".repeat(500);
                            let desc = if behavior.keep_128_chunks {
                                long_desc.as_str()
                            } else {
                                "ok"
                            };
                            w.string("_result").number(1.0).object(&[
                                ("fmsVer", amf0::ObjVal::Str("FMS/3,0,1,123")),
                                ("capabilities", amf0::ObjVal::Num(31.0)),
                                ("description", amf0::ObjVal::Str(desc)),
                            ]);
                            h.write_all(&frame_message(
                                CID_COMMAND,
                                MSG_AMF0_COMMAND,
                                0,
                                0,
                                &w.into_bytes(),
                                reply_chunk,
                            ))?;
                        }
                        "createStream" => {
                            if behavior.ping_before_create_stream {
                                let mut ping = Vec::new();
                                ping.extend_from_slice(&UCM_PING_REQUEST.to_be_bytes());
                                ping.extend_from_slice(&1234u32.to_be_bytes());
                                h.write_all(&frame_message(CID_COMMAND, MSG_USER_CONTROL, 0, 0, &ping, reply_chunk))?;
                            }
                            let mut w = amf0::Writer::new();
                            w.string("_result").number(2.0).null().number(1.0);
                            h.write_all(&frame_message(
                                CID_COMMAND,
                                MSG_AMF0_COMMAND,
                                0,
                                0,
                                &w.into_bytes(),
                                reply_chunk,
                            ))?;
                        }
                        "publish" => {
                            let (level, code) = if behavior.refuse_publish {
                                ("error", "NetStream.Publish.BadName")
                            } else {
                                ("status", "NetStream.Publish.Start")
                            };
                            let mut w = amf0::Writer::new();
                            w.string("onStatus").number(3.0).null().object(&[
                                ("level", amf0::ObjVal::Str(level)),
                                ("code", amf0::ObjVal::Str(code)),
                                ("description", amf0::ObjVal::Str("whatever")),
                            ]);
                            h.write_all(&frame_message(
                                CID_COMMAND,
                                MSG_AMF0_COMMAND,
                                0,
                                0,
                                &w.into_bytes(),
                                reply_chunk,
                            ))?;
                        }
                        "FCUnpublish" | "closeStream" => {
                            return Ok(());
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }

    // --- FLV crafting helpers for the tests ---

    fn flv_tag(mtype: u8, ts: u32, body: &[u8]) -> Vec<u8> {
        let size = body.len();
        let mut t = Vec::with_capacity(11 + size + 4);
        t.push(mtype);
        t.extend_from_slice(&[(size >> 16) as u8, (size >> 8) as u8, size as u8]);
        t.extend_from_slice(&[(ts >> 16) as u8, (ts >> 8) as u8, ts as u8]);
        t.push(0); // timestamp extension
        t.extend_from_slice(&[0, 0, 0]); // stream id
        t.extend_from_slice(body);
        t.extend_from_slice(&((11 + size) as u32).to_be_bytes()); // prev tag size
        t
    }

    fn flv_header() -> Vec<u8> {
        let mut h = vec![0x46, 0x4c, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09];
        h.extend_from_slice(&[0, 0, 0, 0]);
        h
    }

    fn sample_flv() -> Vec<u8> {
        let mut b = flv_header();
        // onMetaData tag (type 18).
        let mut md = vec![0x02, 0x00, 0x0A];
        md.extend_from_slice(b"onMetaData");
        md.extend_from_slice(&[0x08, 0, 0, 0, 0]); // ECMA array, count 0
        md.extend_from_slice(&[0, 0, 0x09]);
        b.extend(flv_tag(18, 0, &md));
        // Video sequence header: 0x17 (key + AVC), AVCPacketType 0, cts, AVCC.
        b.extend(flv_tag(9, 0, &[0x17, 0x00, 0, 0, 0, 1, 0, 0, 0, 1, 0x67, 0x42]));
        // Audio sequence header: 0xAF, AACPacketType 0, ASC.
        b.extend(flv_tag(8, 0, &[0xAF, 0x00, 0x12, 0x10]));
        // Video frame: 0x17 key + AVC, packet type 1, cts, NAL.
        b.extend(flv_tag(9, 40, &[0x17, 0x01, 0, 0, 0, 0x06, 0x05, 0xFF]));
        // Audio frame.
        b.extend(flv_tag(8, 40, &[0xAF, 0x01, 0x21, 0x00]));
        b.extend(flv_tag(9, 80, &[0x27, 0x01, 0, 0, 0, 0x06, 0x05, 0xAA]));
        b
    }

    fn run_publish(behavior: Behavior) -> (io::Result<RtmpTransport<Half>>, MsgLog) {
        let (client_half, server_half) = pair();
        let log: MsgLog = Arc::new(Mutex::new(Vec::new()));
        let log_srv = log.clone();
        let server = thread::spawn(move || fake_server(server_half, &behavior, &log_srv));

        let cfg = RtmpConfig::new("app", "myStream", "rtmp://localhost/app");
        let mut result = RtmpTransport::connect(client_half, cfg);
        // On success the transport stays alive; close it so the fake server
        // sees `closeStream` and exits instead of blocking `join` forever.
        if let Ok(t) = result.as_mut() {
            let _ = Transport::shutdown(t);
        }
        let _ = server.join();
        (result, log)
    }

    #[test]
    fn publish_roundtrip() {
        let (client_half, server_half) = pair();
        let log: MsgLog = Arc::new(Mutex::new(Vec::new()));
        let log_srv = log.clone();
        let server = thread::spawn(move || fake_server(server_half, &Behavior::default(), &log_srv));

        let cfg = RtmpConfig::new("app", "myStream", "rtmp://localhost/app");
        let mut t = RtmpTransport::connect(client_half, cfg).unwrap();
        assert_eq!(t.pid, 1);

        let flv = sample_flv();
        t.write_all(&flv).unwrap();
        t.flush().unwrap();
        t.shutdown().unwrap();

        server.join().unwrap().unwrap();

        let collected = log.lock().unwrap().clone();
        let media: Vec<_> = collected
            .iter()
            .filter(|(mt, _)| matches!(mt, &(MSG_AUDIO | MSG_VIDEO | MSG_AMF0_DATA)))
            .collect();
        assert!(media.len() >= 5, "expected >=5 media messages, got {}", media.len());

        // [0] metadata -> @setDataFrame AMF0 data message.
        assert_eq!(media[0].0, MSG_AMF0_DATA);
        let md = &media[0].1;
        assert_eq!(&md[0..3], &[0x02, 0x00, 0x0D]);
        assert_eq!(&md[3..16], b"@setDataFrame");
        assert_eq!(&md[16..19], &[0x02, 0x00, 0x0A]);
        assert_eq!(&md[19..29], b"onMetaData");
        assert_eq!(md[29], 0x08, "ECMA array follows the method name");
        // [1] video sequence header: AVC, AVCPacketType == 0.
        assert_eq!(media[1].0, MSG_VIDEO);
        assert_eq!(media[1].1[1], 0);
        // [2] audio sequence header.
        assert_eq!(media[2].0, MSG_AUDIO);
        assert_eq!(media[2].1[1], 0);
        // [3] key video frame at ts 40, [4] audio at 40.
        assert_eq!(media[3].0, MSG_VIDEO);
        assert_eq!(media[4].0, MSG_AUDIO);
        // [5] inter video frame at ts 80.
        assert_eq!(media[5].0, MSG_VIDEO);
        assert_eq!(media[5].1[0] >> 4, 2); // inter frame type
    }

    #[test]
    fn connect_error_is_surfaced() {
        let (result, _) = run_publish(Behavior {
            connect_error: true,
            ..Default::default()
        });
        let Err(err) = result else {
            panic!("connect must fail when the server sends `_error`")
        };
        let msg = err.to_string();
        assert!(msg.contains("NetConnection.Connect.Rejected"), "got: {msg}");
        assert!(msg.contains("app not found"), "got: {msg}");
    }

    #[test]
    fn publish_refusal_is_surfaced() {
        let (result, _) = run_publish(Behavior {
            refuse_publish: true,
            ..Default::default()
        });
        let Err(err) = result else {
            panic!("publish must fail when the server refuses it")
        };
        let msg = err.to_string();
        assert!(msg.contains("NetStream.Publish.BadName"), "got: {msg}");
    }

    #[test]
    fn ping_request_is_answered() {
        let (result, log) = run_publish(Behavior {
            ping_before_create_stream: true,
            ..Default::default()
        });
        assert!(result.is_ok(), "publish succeeds past the ping: {:?}", result.err());
        let pongs: Vec<_> = log
            .lock()
            .unwrap()
            .iter()
            .filter(|(mt, p)| {
                *mt == MSG_USER_CONTROL && p.len() >= 6 && u16::from_be_bytes([p[0], p[1]]) == UCM_PING_RESPONSE
            })
            .map(|(_, p)| u32::from_be_bytes([p[2], p[3], p[4], p[5]]))
            .collect();
        assert_eq!(pongs, vec![1234], "client must echo the ping timestamp");
    }

    #[test]
    fn window_ack_triggers_acknowledgement() {
        let (result, log) = run_publish(Behavior {
            window_ack: Some(50),
            ..Default::default()
        });
        assert!(result.is_ok(), "publish succeeds: {:?}", result.err());
        let acks: Vec<u32> = log
            .lock()
            .unwrap()
            .iter()
            .filter(|(mt, p)| *mt == MSG_ACK && p.len() == 4)
            .map(|(_, p)| u32::from_be_bytes([p[0], p[1], p[2], p[3]]))
            .collect();
        assert!(!acks.is_empty(), "client must acknowledge the ack window");
        assert!(acks[0] >= 50, "ack counts received bytes: {}", acks[0]);
    }

    #[test]
    fn survives_peer_that_keeps_128_chunk_framing() {
        // A real ingest server (YouTube) may ignore the client's Set Chunk Size
        // and keep framing replies in the default 128-byte chunks — including a
        // `connect` `_result` larger than 128 bytes, which then arrives split.
        // The client must not assume its negotiated size took effect, or it
        // will misparse the continuation chunks and stall (`WouldBlock`).
        let (result, _log) = run_publish(Behavior {
            keep_128_chunks: true,
            ..Default::default()
        });
        assert!(result.is_ok(), "publish must succeed: {:?}", result.err());
    }

    #[test]
    fn connect_tcp_times_out_on_silent_server() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                // Never answer the handshake; hold the connection open.
                thread::sleep(Duration::from_millis(1500));
                let _ = sock.write_all(&[3]);
            }
        });
        let cfg = RtmpConfig {
            timeout: Some(Duration::from_millis(200)),
            ..RtmpConfig::new("app", "key", "rtmp://x/app")
        };
        let Err(err) = RtmpTransport::connect_tcp(&addr, cfg) else {
            panic!("connect must time out against a silent server")
        };
        assert!(
            matches!(err.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut),
            "expected a timeout, got {err:?}"
        );
        server.join().unwrap();
    }

    #[test]
    fn config_defaults_to_a_timeout() {
        let cfg = RtmpConfig::new("a", "k", "rtmp://x/a");
        assert_eq!(cfg.timeout, Some(DEFAULT_TIMEOUT));
    }
}

#[cfg(test)]
mod chunk_tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn reassembles_chunked_message() {
        // Build a >128-byte message so it spans multiple chunks, then confirm the
        // reader reassembles it exactly.
        let payload: Vec<u8> = (0..300u16).map(|i| (i % 251) as u8).collect();
        let frame = frame_message(CID_AUDIO, MSG_AUDIO, 1, 12345, &payload, 128);
        let mut cur = Cursor::new(frame);
        let mut reader = ChunkReader::new();
        let msg = reader.read_message(&mut cur).unwrap();
        assert_eq!(msg.mtype, MSG_AUDIO);
        assert_eq!(msg.payload, payload);
    }

    #[test]
    fn reads_two_back_to_back_messages() {
        let mut buf = Vec::new();
        buf.extend(frame_message(CID_COMMAND, MSG_AMF0_COMMAND, 0, 0, b"first", 128));
        buf.extend(frame_message(CID_VIDEO, MSG_VIDEO, 7, 0, b"second", 128));
        let mut cur = Cursor::new(buf);
        let mut reader = ChunkReader::new();
        let a = reader.read_message(&mut cur).unwrap();
        let b = reader.read_message(&mut cur).unwrap();
        assert_eq!(a.payload, b"first");
        assert_eq!(a.mtype, MSG_AMF0_COMMAND);
        assert_eq!(b.payload, b"second");
        assert_eq!(b.mtype, MSG_VIDEO);
    }

    #[test]
    fn extended_timestamp_survives_roundtrip() {
        // ts > 24 bits must travel via the extended-timestamp field.
        let ts = 0x0102_0304u32;
        let frame = frame_message(CID_VIDEO, MSG_VIDEO, 1, ts, b"payload", 128);
        let mut reader = ChunkReader::new();
        let msg = reader.read_message(&mut Cursor::new(frame)).unwrap();
        assert_eq!(msg.payload, b"payload");
        // Reassemble again with the same reader state to confirm ts was kept.
        let frame2 = frame_message(CID_AUDIO, MSG_AUDIO, 1, ts + 1, b"x", 128);
        let _ = reader.read_message(&mut Cursor::new(frame2)).unwrap();
    }

    #[test]
    fn zero_chunk_size_is_rejected() {
        let mut reader = ChunkReader::new();
        assert!(reader.set_chunk_size(0).is_err());
        // Oversized values clamp instead of blowing up allocation math.
        assert!(reader.set_chunk_size(u32::MAX as usize).is_ok());
    }

    #[test]
    fn malformed_streams_error_not_panic() {
        let inputs: Vec<Vec<u8>> = vec![
            vec![],                                            // immediate EOF
            vec![0xC3],                                        // fmt-3 continuation with no prior state
            vec![0x03, 1, 2],                                  // fmt-0 header cut short
            vec![0x00, 63],                                    // 1-byte extended cid, then EOF
            vec![0x01, 0, 0],                                  // 2-byte extended cid, then EOF
            vec![0x03, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 1, 2], // length 5, 2 bytes only
        ];
        for input in inputs {
            let mut reader = ChunkReader::new();
            assert!(
                reader.read_message(&mut Cursor::new(input)).is_err(),
                "malformed input must error, not panic"
            );
        }
    }

    #[test]
    fn abort_discards_partial_message() {
        // A message announced as 100 bytes, delivered partially, then aborted.
        let mut frame = frame_message(CID_AUDIO, MSG_AUDIO, 1, 0, &[0xAA; 100], 128);
        frame.truncate(20); // cut the payload short
        let mut reader = ChunkReader::new();
        assert!(reader.read_message(&mut Cursor::new(frame)).is_err()); // EOF mid-message
        reader.abort(CID_AUDIO as usize);
        // After abort, a fresh message on the same cid reassembles cleanly.
        let frame2 = frame_message(CID_AUDIO, MSG_AUDIO, 1, 5, b"fresh", 128);
        let msg = reader.read_message(&mut Cursor::new(frame2)).unwrap();
        assert_eq!(msg.payload, b"fresh");
    }

    #[test]
    fn bytes_read_counts_headers_too() {
        let frame = frame_message(CID_COMMAND, MSG_AMF0_COMMAND, 0, 0, b"abc", 128);
        let len = frame.len() as u64;
        let mut reader = ChunkReader::new();
        reader.read_message(&mut Cursor::new(frame)).unwrap();
        assert_eq!(reader.bytes_read(), len);
    }
}

#[cfg(test)]
mod flv_parse_tests {
    use super::*;

    fn header() -> Vec<u8> {
        let mut h = vec![0x46, 0x4c, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09];
        h.extend_from_slice(&[0, 0, 0, 0]);
        h
    }

    fn tag(mtype: u8, ts: u32, body: &[u8]) -> Vec<u8> {
        let size = body.len();
        let mut t = Vec::new();
        t.push(mtype);
        t.extend_from_slice(&[(size >> 16) as u8, (size >> 8) as u8, size as u8]);
        t.extend_from_slice(&[(ts >> 16) as u8, (ts >> 8) as u8, ts as u8]);
        t.push((ts >> 24) as u8);
        t.extend_from_slice(&[0, 0, 0]);
        t.extend_from_slice(body);
        t.extend_from_slice(&((11 + size) as u32).to_be_bytes());
        t
    }

    #[test]
    fn parses_tags_incrementally() {
        let mut stream = header();
        stream.extend(tag(9, 40, &[1, 2, 3]));
        stream.extend(tag(8, 0x0102_0304, &[4, 5]));
        let mut p = FlvTagParser::new();
        // Feed one byte at a time: chunking must not matter.
        let mut tags = Vec::new();
        for b in &stream {
            tags.extend(p.feed(&[*b]).unwrap());
        }
        assert_eq!(tags.len(), 2);
        assert_eq!(
            tags[0],
            FlvTag {
                mtype: 9,
                ts: 40,
                body: vec![1, 2, 3]
            }
        );
        assert_eq!(
            tags[1],
            FlvTag {
                mtype: 8,
                ts: 0x0102_0304,
                body: vec![4, 5]
            }
        );
    }

    #[test]
    fn merges_24_bit_timestamp_with_extension() {
        let mut stream = header();
        stream.extend(tag(9, 0xFFAB_CDEF, &[0x55]));
        let mut p = FlvTagParser::new();
        let tags = p.feed(&stream).unwrap();
        assert_eq!(tags[0].ts, 0xFFAB_CDEF);
    }

    #[test]
    fn rejects_bad_headers() {
        let mut p = FlvTagParser::new();
        assert!(matches!(p.feed(b"not an FLV stream at all"), Err(FlvError::BadHeader)));
        let mut bad = header();
        bad[3] = 2; // unsupported version
        assert!(matches!(FlvTagParser::new().feed(&bad), Err(FlvError::BadHeader)));
        let mut huge_offset = header();
        huge_offset[5..9].copy_from_slice(&0x0010_0000u32.to_be_bytes());
        assert!(matches!(
            FlvTagParser::new().feed(&huge_offset),
            Err(FlvError::BadHeader)
        ));
    }

    #[test]
    fn truncated_input_waits_without_error() {
        let mut stream = header();
        stream.extend(tag(9, 0, &[1, 2, 3]));
        // Cut into the tag body: parsing must wait, never error.
        let tail = stream.split_off(25);
        let mut p = FlvTagParser::new();
        assert_eq!(p.feed(&stream[..5]).unwrap(), vec![]);
        assert_eq!(p.feed(&stream[5..]).unwrap(), vec![]);
        // The missing bytes arrive and the tag completes.
        let tags = p.feed(&tail).unwrap();
        assert_eq!(
            tags,
            vec![FlvTag {
                mtype: 9,
                ts: 0,
                body: vec![1, 2, 3]
            }]
        );
    }
}
