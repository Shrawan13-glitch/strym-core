//! RTMP ingest transport. Implements enough of RTMP to publish a live stream:
//! complex handshake, `connect` / `createStream` / `publish` control plane, and
//! a parser that turns the muxer's FLV byte stream into RTMP `audio`/`video`/
//! `@setDataFrame` messages. Depends only on `std` (the crate stays dependency-free).
//!
//! Design: the muxer already emits *valid FLV* (header + onMetaData + sequence
//! headers + media tags). An RTMP publish stream is essentially that FLV wrapped
//! in RTMP chunk frames, so this transport re-parses tag boundaries and re-frames
//! each tag body as an RTMP message.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::SystemTime;

pub mod amf0;
pub mod handshake;

const CHUNK_SIZE: usize = 128;

// Message types.
const MSG_SET_CHUNK_SIZE: u8 = 1;
const MSG_AUDIO: u8 = 8;
const MSG_VIDEO: u8 = 9;
const MSG_AMF0_DATA: u8 = 18;
const MSG_AMF0_COMMAND: u8 = 20;

/// Chunk size we negotiate to after the handshake. Matches the value common RTMP
/// servers (node-media-server, nginx-rtmp, SRS) expect/serve.
const NEGOTIATED_CHUNK_SIZE: usize = 4096;

// Chunk stream ids we emit on: commands/data=3, audio=6, video=7.
const CID_COMMAND: u8 = 3;
const CID_AUDIO: u8 = 6;
const CID_VIDEO: u8 = 7;

/// Connection details for publishing.
pub struct RtmpConfig {
    /// e.g. `"live"` or `"app"`.
    pub app: String,
    /// Stream key (the publish name), e.g. `"mystream"`.
    pub key: String,
    /// Full `rtmp://host[:port]/app` URL.
    pub tc_url: String,
}

impl RtmpConfig {
    /// Build a config from the app name, stream key, and full `tcUrl`.
    pub fn new<S: Into<String>>(app: S, key: S, tc_url: S) -> Self {
        Self {
            app: app.into(),
            key: key.into(),
            tc_url: tc_url.into(),
        }
    }
}

/// One decoded RTMP message.
struct Message {
    mtype: u8,
    payload: Vec<u8>,
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

struct ChunkReader {
    states: Vec<Option<ChunkState>>,
    read_chunk_size: usize,
}

impl ChunkReader {
    fn new() -> Self {
        Self {
            states: vec![None; 64],
            read_chunk_size: CHUNK_SIZE,
        }
    }

    /// Read one full message from `sock`, reassembling chunk fragments.
    fn read_message<R: Read>(&mut self, sock: &mut R) -> io::Result<Message> {
        loop {
            let mut bh = [0u8; 1];
            sock.read_exact(&mut bh)?;
            let fmt = bh[0] >> 6;
            let mut cid = (bh[0] & 0x3F) as usize;
            if cid == 0 {
                let mut ex = [0u8; 1];
                sock.read_exact(&mut ex)?;
                cid = 64 + ex[0] as usize;
            } else if cid == 1 {
                let mut ex = [0u8; 2];
                sock.read_exact(&mut ex)?;
                cid = 64 + ex[0] as usize + (ex[1] as usize) * 256;
            }
            if cid >= self.states.len() {
                self.states.resize(cid + 1, None);
            }
            let st = self.states[cid].get_or_insert_with(ChunkState::default);

            match fmt {
                0 => {
                    let mut h = [0u8; 11];
                    sock.read_exact(&mut h)?;
                    st.ts = read_ts24(&h);
                    st.length = read_u24be(&h[3..]);
                    st.mtype = h[6];
                    st.stream_id = u32::from_le_bytes([h[7], h[8], h[9], h[10]]);
                    st.payload.clear();
                    if st.ts == 0xFFFFFF {
                        st.ts = read_u32be(sock)?;
                    }
                }
                1 => {
                    let mut h = [0u8; 7];
                    sock.read_exact(&mut h)?;
                    let delta = read_ts24(&h);
                    st.length = read_u24be(&h[3..]);
                    st.mtype = h[6];
                    st.payload.clear();
                    let d = if delta == 0xFFFFFF { read_u32be(sock)? } else { delta };
                    st.ts = st.ts.wrapping_add(d);
                }
                2 => {
                    let mut h = [0u8; 3];
                    sock.read_exact(&mut h)?;
                    let delta = read_ts24(&h);
                    st.payload.clear();
                    let d = if delta == 0xFFFFFF { read_u32be(sock)? } else { delta };
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
            let take = take.min(self.read_chunk_size);
            let start = st.payload.len();
            st.payload.resize(start + take, 0);
            sock.read_exact(&mut st.payload[start..])?;
            if st.payload.len() as u32 == st.length {
                return Ok(Message {
                    mtype: st.mtype,
                    payload: std::mem::take(&mut st.payload),
                });
            }
        }
    }
}

/// FLV-tag parse state, shared across `write()` calls (writes can be split anywhere).
struct FlvParse {
    phase: FlvPhase,
    buf: Vec<u8>,
    cur: Option<TagHead>,
}

#[derive(Clone, Copy)]
struct TagHead {
    mtype: u8,
    size: usize,
    ts: u32,
}

impl FlvParse {
    fn new() -> Self {
        Self {
            phase: FlvPhase::Header,
            buf: Vec::new(),
            cur: None,
        }
    }
}

enum FlvPhase {
    Header,
    TagHeader,
    TagBody,
    PrevSize,
}

/// RTMP transport. `S` is generic over Read+Write so tests can inject an
/// in-memory duplex instead of a real socket.
pub struct RtmpTransport<S: Read + Write> {
    sock: S,
    reader: ChunkReader,
    flv: FlvParse,
    pid: u32,
    cfg: RtmpConfig,
    /// Chunk size used for both outgoing frames and inbound reassembly.
    chunk: usize,
}

impl RtmpTransport<TcpStream> {
    /// Connect the transport to a real RTMP server over TCP, and publish.
    pub fn connect_tcp(addr: &str, cfg: RtmpConfig) -> io::Result<Self> {
        let sock = TcpStream::connect(addr)?;
        Self::connect(sock, cfg)
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("server offered RTMP version {}", s0[0]),
            ));
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
            flv: FlvParse::new(),
            pid: 0,
            cfg,
            chunk: CHUNK_SIZE,
        };
        // Tell the server to expect our larger chunks (and match its own). Sent
        // right after the handshake, before the first command.
        t.send_set_chunk_size(NEGOTIATED_CHUNK_SIZE)?;
        t.chunk = NEGOTIATED_CHUNK_SIZE;
        t.reader.read_chunk_size = NEGOTIATED_CHUNK_SIZE;
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
        let (name, _) = self.read_command()?;
        if name != "_result" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("connect failed (server replied `{name}`)"),
            ));
        }
        Ok(())
    }

    fn create_stream(&mut self) -> io::Result<u32> {
        let mut w = amf0::Writer::new();
        w.string("createStream").number(2.0).null();
        self.send_message(CID_COMMAND, MSG_AMF0_COMMAND, 0, 0, &w.into_bytes())?;
        let (name, payload) = self.read_command()?;
        if name != "_result" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("createStream expected `_result`, got `{name}`"),
            ));
        }
        let mut r = amf0::Reader::new(&payload);
        r.read_value(); // command name
        r.read_value(); // transaction id (typed Number)
        r.read_value(); // args (object / null)
        let sid = match r.read_value() {
            Some(amf0::Val::Number(n)) => n as u32,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "no stream id in createStream _result",
                ))
            }
        };
        Ok(sid)
    }

    fn do_publish(&mut self, sid: u32) -> io::Result<()> {
        let mut w = amf0::Writer::new();
        w.string("publish")
            .number(3.0)
            .null()
            .string(&self.cfg.key)
            .string("live");
        self.send_message(CID_COMMAND, MSG_AMF0_COMMAND, sid, 0, &w.into_bytes())?;
        let (name, payload) = self.read_command()?;
        if name == "onStatus" {
            let mut r = amf0::Reader::new(&payload);
            r.read_value(); // onStatus
            r.read_value(); // transaction id (typed Number)
            r.read_value(); // null
            if let Some(amf0::Val::Object(fields)) = r.read_value() {
                for (k, v) in fields {
                    if k == "code" {
                        if let amf0::Val::String(code) = v {
                            if code.starts_with("NetStream.Publish.Start") {
                                return Ok(());
                            }
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("server refused publish: {code}"),
                            ));
                        }
                    }
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected response to publish: `{name}`"),
        ))
    }

    /// Drain messages until a command/data message arrives; return its command
    /// name (raw UTF-8 string) and full payload.
    fn read_command(&mut self) -> io::Result<(String, Vec<u8>)> {
        loop {
            let msg = self.reader.read_message(&mut self.sock)?;
            match msg.mtype {
                MSG_SET_CHUNK_SIZE if msg.payload.len() >= 4 => {
                    // Server changed the chunk size it will use for our outbound
                    // frames (and the same value for its own). Symmetric update.
                    let n =
                        u32::from_be_bytes([msg.payload[0], msg.payload[1], msg.payload[2], msg.payload[3]]) as usize;
                    self.chunk = n;
                    self.reader.read_chunk_size = n;
                }
                MSG_AMF0_COMMAND | MSG_AMF0_DATA => {
                    let mut r = amf0::Reader::new(&msg.payload);
                    if let Some(amf0::Val::String(name)) = r.read_value() {
                        return Ok((name, msg.payload));
                    }
                }
                _ => {}
            }
        }
    }

    /// Push one normalized (FLV) message out to the server.
    fn send_message(&mut self, cid: u8, mtype: u8, stream_id: u32, ts: u32, payload: &[u8]) -> io::Result<()> {
        let out = frame_message(cid, mtype, stream_id, ts, payload, self.chunk);
        self.sock.write_all(&out)?;
        self.sock.flush()
    }

    /// Digest newly-buffered FLV bytes and re-frame them into RTMP messages.
    fn parse_flv(&mut self) -> io::Result<()> {
        loop {
            match self.flv.phase {
                FlvPhase::Header => {
                    if self.flv.buf.len() < 13 {
                        break;
                    }
                    self.flv.buf.drain(..13); // 9-byte FLV header + 4-byte prev-size
                    self.flv.phase = FlvPhase::TagHeader;
                }
                FlvPhase::TagHeader => {
                    if self.flv.buf.len() < 11 {
                        break;
                    }
                    let mtype = self.flv.buf[0];
                    let size = ((self.flv.buf[1] as usize) << 16)
                        | ((self.flv.buf[2] as usize) << 8)
                        | self.flv.buf[3] as usize;
                    let ts24 =
                        ((self.flv.buf[4] as u32) << 16) | ((self.flv.buf[5] as u32) << 8) | self.flv.buf[6] as u32;
                    let tsext = self.flv.buf[7] as u32;
                    let ts = (tsext << 24) | ts24;
                    self.flv.buf.drain(..11);
                    self.flv.cur = Some(TagHead { mtype, size, ts });
                    self.flv.phase = FlvPhase::TagBody;
                }
                FlvPhase::TagBody => {
                    let Some(head) = self.flv.cur else {
                        break;
                    };
                    if self.flv.buf.len() < head.size {
                        break;
                    }
                    let body = self.flv.buf.drain(..head.size).collect::<Vec<_>>();
                    self.emit_tag(head.mtype, head.ts, &body)?;
                    self.flv.phase = FlvPhase::PrevSize;
                }
                FlvPhase::PrevSize => {
                    if self.flv.buf.len() < 4 {
                        break;
                    }
                    self.flv.buf.drain(..4); // previous-tag-size
                    self.flv.phase = FlvPhase::TagHeader;
                }
            }
        }
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
        self.flv.buf.extend_from_slice(buf);
        self.parse_flv()?;
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
}

// --- helpers ---

fn read_ts24(b: &[u8]) -> u32 {
    ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32
}

fn read_u24be(b: &[u8]) -> u32 {
    ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32
}

fn read_u32be<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
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

    /// `(message type, payload)` pairs the fake server collected.
    type MediaLog = Arc<Mutex<Vec<(u8, Vec<u8>)>>>;

    /// A minimal RTMP server exercised against our client: performs the handshake,
    /// answers connect/createStream/publish, and collects media messages.
    fn fake_server(mut h: Half, media: &MediaLog) -> io::Result<()> {
        // Handshake.
        let mut c0 = [0u8; 1];
        h.read_exact(&mut c0)?;
        assert_eq!(c0[0], 3);
        let mut c1 = [0u8; 1536];
        h.read_exact(&mut c1)?;
        let s1 = handshake_server_s1();
        h.write_all(&[3])?;
        h.write_all(&s1)?;
        h.write_all(&[0u8; 1536])?; // S2 (client ignores)
        let mut c2 = [0u8; 1536];
        h.read_exact(&mut c2)?;

        // Control plane.
        let mut reader = ChunkReader::new();
        loop {
            let msg = reader.read_message(&mut h)?;
            match msg.mtype {
                MSG_SET_CHUNK_SIZE if msg.payload.len() >= 4 => {
                    // Honor the client's negotiated chunk size, like a real server.
                    reader.read_chunk_size =
                        u32::from_be_bytes([msg.payload[0], msg.payload[1], msg.payload[2], msg.payload[3]]) as usize;
                }
                MSG_AMF0_COMMAND => {
                    let mut r = amf0::Reader::new(&msg.payload);
                    let Some(amf0::Val::String(name)) = r.read_value() else {
                        continue;
                    };
                    match name.as_str() {
                        "connect" => {
                            let mut w = amf0::Writer::new();
                            w.string("_result").number(1.0).object(&[
                                ("fmsVer", amf0::ObjVal::Str("FMS/3,0,1,123")),
                                ("capabilities", amf0::ObjVal::Num(31.0)),
                            ]);
                            h.write_all(&frame_message(
                                CID_COMMAND,
                                MSG_AMF0_COMMAND,
                                0,
                                0,
                                &w.into_bytes(),
                                4096,
                            ))?;
                        }
                        "createStream" => {
                            let mut w = amf0::Writer::new();
                            w.string("_result").number(2.0).null().number(1.0);
                            h.write_all(&frame_message(
                                CID_COMMAND,
                                MSG_AMF0_COMMAND,
                                0,
                                0,
                                &w.into_bytes(),
                                4096,
                            ))?;
                        }
                        "publish" => {
                            let mut w = amf0::Writer::new();
                            w.string("onStatus").number(3.0).null().object(&[
                                ("level", amf0::ObjVal::Str("status")),
                                ("code", amf0::ObjVal::Str("NetStream.Publish.Start")),
                                ("description", amf0::ObjVal::Str("whatever")),
                            ]);
                            h.write_all(&frame_message(
                                CID_COMMAND,
                                MSG_AMF0_COMMAND,
                                0,
                                0,
                                &w.into_bytes(),
                                4096,
                            ))?;
                        }
                        "FCUnpublish" | "closeStream" => {
                            return Ok(());
                        }
                        _ => {}
                    }
                }
                MSG_AMF0_DATA | MSG_AUDIO | MSG_VIDEO => {
                    media.lock().unwrap().push((msg.mtype, msg.payload));
                }
                _ => {}
            }
        }
    }

    fn handshake_server_s1() -> Vec<u8> {
        // A well-formed complex server S1 (valid FMS_KEY2 digest) so the round-trip
        // exercises the client's complex C2 path.
        handshake::build_s1_complex(0).to_vec()
    }

    // --- FLV crafting helpers for the test ---

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

    #[test]
    fn publish_roundtrip() {
        let (client_half, server_half) = pair();
        let media = Arc::new(Mutex::new(Vec::new()));
        let media_srv = media.clone();
        let server = thread::spawn(move || fake_server(server_half, &media_srv).unwrap());

        let cfg = RtmpConfig::new("app", "myStream", "rtmp://localhost/app");
        let mut t = RtmpTransport::connect(client_half, cfg).unwrap();
        assert_eq!(t.pid, 1);

        let flv = sample_flv();
        t.write_all(&flv).unwrap();
        t.flush().unwrap();
        t.shutdown().unwrap();

        server.join().unwrap();

        let collected = media.lock().unwrap().clone();
        assert!(collected.len() >= 5, "expected >=5 messages, got {}", collected.len());

        // [0] metadata -> @setDataFrame AMF0 data message.
        // Metadata must arrive as a typed @setDataFrame AMF0 data message.
        assert_eq!(collected[0].0, MSG_AMF0_DATA);
        let md = &collected[0].1;
        assert_eq!(&md[0..3], &[0x02, 0x00, 0x0D]);
        assert_eq!(&md[3..16], b"@setDataFrame");
        assert_eq!(&md[16..19], &[0x02, 0x00, 0x0A]);
        assert_eq!(&md[19..29], b"onMetaData");
        assert_eq!(md[29], 0x08, "ECMA array follows the method name");
        // [1] video sequence header: AVC, AVCPacketType == 0.
        assert_eq!(collected[1].0, MSG_VIDEO);
        assert_eq!(collected[1].1[1], 0);
        // [2] audio sequence header.
        assert_eq!(collected[2].0, MSG_AUDIO);
        assert_eq!(collected[2].1[1], 0);
        // [3] key video frame at ts 40, [4] audio at 40.
        assert_eq!(collected[3].0, MSG_VIDEO);
        assert_eq!(collected[4].0, MSG_AUDIO);
        // [5] inter video frame at ts 80.
        assert_eq!(collected[5].0, MSG_VIDEO);
        assert_eq!(collected[5].1[0] >> 4, 2); // inter frame type
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
}
