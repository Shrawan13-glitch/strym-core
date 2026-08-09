//! End-to-end session resilience over a real TCP socket.
//!
//! A minimal RTMP ingest server accepts the publish, then abruptly drops the
//! connection mid-stream (exactly what a network outage or server restart
//! looks like to the client). The session must detect the failure, redial
//! with backoff, re-emit every header the fresh connection needs, and resume
//! media on a keyframe with continued timestamps — quickly.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use stream::engine::EngineConfig;
use stream::models::{MediaKind, MediaPacket};
use stream::rtmp::amf0;
use stream::rtmp::{RtmpConfig, RtmpConnector};
use stream::session::{ReconnectPolicy, Session, SessionPolicy};

const MSG_SET_CHUNK_SIZE: u8 = 1;
const MSG_WINDOW_ACK: u8 = 5;
const MSG_SET_PEER_BW: u8 = 6;
const MSG_AUDIO: u8 = 8;
const MSG_VIDEO: u8 = 9;
const MSG_AMF0_DATA: u8 = 18;
const MSG_AMF0_COMMAND: u8 = 20;

/// Kill the first connection after this many media messages.
const KILL_AFTER: usize = 20;

/// One media message as the server saw it: `(mtype, timestamp, payload)`.
type MediaMsg = (u8, u32, Vec<u8>);

/// Everything the server observed, per connection.
#[derive(Default)]
struct Report {
    conns: Vec<Vec<MediaMsg>>,
}

/// Partially assembled RTMP message for one chunk stream.
struct Partial {
    remaining: usize,
    mtype: u8,
    ts: u32,
    payload: Vec<u8>,
}

/// Minimal RTMP chunk reader for the test server: fmt0 starts a message,
/// fmt3 continues it. Mirrors what any real ingest endpoint has to do.
struct ChunkReader {
    chunk_size: usize,
    partial: HashMap<u8, Partial>,
}

impl ChunkReader {
    fn new() -> Self {
        Self {
            chunk_size: 128,
            partial: HashMap::new(),
        }
    }

    fn read_message(&mut self, sock: &mut TcpStream) -> io::Result<(u8, u32, Vec<u8>)> {
        loop {
            let mut bh = [0u8; 1];
            sock.read_exact(&mut bh)?;
            let fmt = bh[0] >> 6;
            let cid = bh[0] & 0x3F;
            assert!(cid >= 2, "test server only handles 1-byte cids");

            if fmt == 0 {
                let mut h = [0u8; 11];
                sock.read_exact(&mut h)?;
                let mut ts = u32::from_be_bytes([0, h[0], h[1], h[2]]);
                let len = u32::from_be_bytes([0, h[3], h[4], h[5]]) as usize;
                let mtype = h[6];
                // h[7..11] is the stream id (little-endian); irrelevant here.
                if ts == 0x00FF_FFFF {
                    let mut ext = [0u8; 4];
                    sock.read_exact(&mut ext)?;
                    ts = u32::from_be_bytes(ext);
                }
                self.partial.insert(
                    cid,
                    Partial {
                        remaining: len,
                        mtype,
                        ts,
                        payload: Vec::with_capacity(len),
                    },
                );
            }
            // fmt3 carries no header: it continues the message in flight on `cid`.

            let part = self
                .partial
                .get_mut(&cid)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "fmt3 without fmt0"))?;
            let take = part.remaining.min(self.chunk_size);
            let mut buf = vec![0u8; take];
            sock.read_exact(&mut buf)?;
            part.payload.extend_from_slice(&buf);
            part.remaining -= take;
            if part.remaining == 0 {
                let done = self.partial.remove(&cid).unwrap();
                return Ok((done.mtype, done.ts, done.payload));
            }
        }
    }
}

/// Frame a message as a single fmt0 chunk (all server replies are small).
fn chunk0(mtype: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + payload.len());
    out.push(0x03); // fmt0 on chunk stream 3
    out.extend_from_slice(&[0, 0, 0]); // timestamp 0
    let len = payload.len() as u32;
    out.extend_from_slice(&len.to_be_bytes()[1..4]);
    out.push(mtype);
    out.extend_from_slice(&0u32.to_le_bytes()); // stream id 0
    out.extend_from_slice(payload);
    out
}

/// Handshake, control plane, media collection — for one accepted connection.
fn serve_conn(sock: &mut TcpStream, index: usize, report: &Arc<Mutex<Report>>) -> io::Result<()> {
    // C0C1 / S0S1S2 / C2.
    let mut c0c1 = [0u8; 1537];
    sock.read_exact(&mut c0c1)?;
    assert_eq!(c0c1[0], 3);
    let mut s0s1s2 = vec![3u8];
    s0s1s2.extend_from_slice(&[0u8; 1536]); // S1 (client only validates the shape)
    s0s1s2.extend_from_slice(&c0c1[1..]); // S2 echoes C1, like real servers do
    sock.write_all(&s0s1s2)?;
    let mut c2 = [0u8; 1536];
    sock.read_exact(&mut c2)?;

    let mut reader = ChunkReader::new();
    let mut publishing = false;
    loop {
        let (mtype, ts, payload) = reader.read_message(sock)?;
        match mtype {
            MSG_SET_CHUNK_SIZE if payload.len() >= 4 => {
                reader.chunk_size = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
            }
            MSG_AMF0_COMMAND => {
                let mut r = amf0::Reader::new(&payload);
                let Some(amf0::Val::String(name)) = r.read_value() else {
                    continue;
                };
                match name.as_str() {
                    "connect" => {
                        sock.write_all(&chunk0(MSG_WINDOW_ACK, &2_500_000u32.to_be_bytes()))?;
                        let mut bw = 2_500_000u32.to_be_bytes().to_vec();
                        bw.push(2); // dynamic
                        sock.write_all(&chunk0(MSG_SET_PEER_BW, &bw))?;
                        sock.write_all(&chunk0(MSG_SET_CHUNK_SIZE, &4096u32.to_be_bytes()))?;
                        let mut w = amf0::Writer::new();
                        w.string("_result")
                            .number(1.0)
                            .object(&[("fmsVer", amf0::ObjVal::Str("FMS/3,0,1,123"))]);
                        sock.write_all(&chunk0(MSG_AMF0_COMMAND, &w.into_bytes()))?;
                    }
                    "createStream" => {
                        let mut w = amf0::Writer::new();
                        w.string("_result").number(2.0).null().number(1.0);
                        sock.write_all(&chunk0(MSG_AMF0_COMMAND, &w.into_bytes()))?;
                    }
                    "publish" => {
                        let mut w = amf0::Writer::new();
                        w.string("onStatus").number(0.0).null().object(&[
                            ("level", amf0::ObjVal::Str("status")),
                            ("code", amf0::ObjVal::Str("NetStream.Publish.Start")),
                            ("description", amf0::ObjVal::Str("publishing")),
                        ]);
                        sock.write_all(&chunk0(MSG_AMF0_COMMAND, &w.into_bytes()))?;
                        publishing = true;
                    }
                    _ => {}
                }
            }
            MSG_AUDIO | MSG_VIDEO | MSG_AMF0_DATA if publishing => {
                let mut rep = report.lock().unwrap();
                let conns = &mut rep.conns;
                while conns.len() <= index {
                    conns.push(Vec::new());
                }
                conns[index].push((mtype, ts, payload));
                // The outage: drop the first connection mid-stream.
                if index == 0 && conns[0].len() >= KILL_AFTER {
                    break;
                }
            }
            _ => {} // acks, pings, anything else: ignore
        }
        sock.flush()?;
    }
    Ok(())
}

fn run_server(listener: &TcpListener, report: &Arc<Mutex<Report>>) {
    for (index, stream) in listener.incoming().enumerate() {
        let Ok(mut sock) = stream else { break };
        let _ = sock.set_nodelay(true);
        let r = report.clone();
        // One thread per connection; the killed socket simply ends its thread.
        thread::spawn(move || {
            let _ = serve_conn(&mut sock, index, &r);
        });
    }
}

fn video_frame(pts: i64, is_key: bool) -> MediaPacket {
    // ~2 KB of (synthetic) NAL payload — big enough to force multi-chunk
    // RTMP messages, so reconnects also re-prove the chunking path.
    let mut data = vec![0u8, 0, 0, 1];
    data.push(if is_key { 0x65 } else { 0x41 });
    data.resize(2048, 0xAB);
    MediaPacket {
        kind: MediaKind::Video,
        pts,
        dts: pts,
        is_key,
        data,
    }
}

fn audio_frame(pts: i64) -> MediaPacket {
    MediaPacket {
        kind: MediaKind::Audio,
        pts,
        dts: pts,
        is_key: false,
        data: vec![0x21, 0x00, 0x49, 0x10, 0x04],
    }
}

/// Minimal H.264 `AVCDecoderConfigurationRecord` and AAC `AudioSpecificConfig`.
const AVCC: &[u8] = &[
    0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x03, 0x67, 0x42, 0x00, 0x0A, 0x01, 0x00, 0x03, 0x68, 0xCE,
];
const ASC: &[u8] = &[0x0A, 0x10];

#[test]
fn session_survives_a_killed_tcp_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let report = Arc::new(Mutex::new(Report::default()));
    let server_report = report.clone();
    let server = thread::spawn(move || run_server(&listener, &server_report));

    let mut session = Session::new(
        EngineConfig::default(),
        RtmpConnector::tcp(
            addr.to_string(),
            RtmpConfig {
                timeout: Some(Duration::from_secs(5)),
                ..RtmpConfig::new("live", "test", &format!("rtmp://{addr}/live"))
            },
        ),
        SessionPolicy {
            reconnect: ReconnectPolicy {
                initial_delay: Duration::from_millis(100),
                max_delay: Duration::from_secs(1),
                multiplier: 2.0,
                max_attempts: Some(8),
                jitter_seed: Some(7),
            },
            stall_timeout: Duration::from_secs(3),
        },
    );
    session.configure_codecs(Some(AVCC), Some(ASC)).unwrap();
    session
        .start()
        .expect("initial connect succeeds against the live server");

    // Pump synthetic capture at ~real time until the second connection has
    // received enough media to prove the resume.
    let t0 = Instant::now();
    let mut next_video = 0i64;
    let mut next_audio = 0i64;
    let mut frame = 0u64;
    let deadline = Duration::from_secs(20);
    loop {
        let now = t0.elapsed();
        assert!(now < deadline, "session never recovered within {deadline:?}");

        let now_ms = i64::try_from(now.as_millis()).unwrap();
        while next_video <= now_ms {
            session.push(video_frame(next_video, frame.is_multiple_of(30))).unwrap();
            next_video += 33;
            frame += 1;
        }
        while next_audio <= now_ms {
            session.push(audio_frame(next_audio)).unwrap();
            next_audio += 43;
        }
        session.tick().unwrap();

        let rep = report.lock().unwrap();
        if rep.conns.len() >= 2
            && rep.conns[1].len() >= 10
            && rep.conns[1]
                .iter()
                .any(|(t, _, p)| *t == MSG_VIDEO && p.get(1) == Some(&1))
        {
            break;
        }
        drop(rep);
        thread::sleep(Duration::from_millis(10));
    }
    let recovered_in = t0.elapsed();

    let rep = report.lock().unwrap();
    let (conn1, conn2) = (&rep.conns[0], &rep.conns[1]);
    assert_resume_quality(conn1, conn2, recovered_in);

    // --- Session bookkeeping. ---
    assert_eq!(session.reconnects(), 1);
    assert_eq!(session.state(), stream::session::SessionState::Connected);

    drop(rep);
    session.finish().expect("clean end-of-stream");
    drop(server);
}

/// Assert the fresh connection got a full cold-start preamble (metadata +
/// both sequence headers) and that media resumed on a keyframe with continued,
/// monotonic timestamps.
fn assert_resume_quality(conn1: &[MediaMsg], conn2: &[MediaMsg], recovered_in: Duration) {
    assert!(conn1.len() >= KILL_AFTER, "server killed a live stream");

    // --- Second connection: a decoder must be able to start cold. ---
    // The test server collects every message (metadata, sequence headers, and
    // media) in arrival order, so just scan the whole connection for the
    // re-emitted preamble.
    assert!(
        conn2.iter().any(|(t, _, _)| *t == MSG_AMF0_DATA),
        "metadata re-emitted on the fresh connection"
    );
    assert!(
        conn2.iter().any(|(t, _, p)| *t == MSG_VIDEO && p.get(1) == Some(&0)),
        "video sequence header re-emitted"
    );
    assert!(
        conn2.iter().any(|(t, _, p)| *t == MSG_AUDIO && p.get(1) == Some(&0)),
        "audio sequence header re-emitted"
    );

    // --- Resume starts on a keyframe. ---
    let first_media = conn2
        .iter()
        .position(|(t, _, p)| *t == MSG_VIDEO && p.get(1) == Some(&1))
        .expect("second connection carries video media");
    let (kind, _ts, payload) = &conn2[first_media];
    assert_eq!(*kind, MSG_VIDEO, "media resumes with video");
    assert_eq!(payload[0] >> 4, 1, "resumes on a keyframe (frame type 1)");
    assert_eq!(payload[1], 1, "...carrying NAL data, not a header repeat");

    // --- Timestamp continuity: no jump back to zero. ---
    let conn1_last_ts = conn1.iter().map(|(_, ts, _)| *ts).max().unwrap();
    let conn2_first_ts = conn2[first_media].1;
    assert!(
        conn2_first_ts >= conn1_last_ts,
        "timestamps continue across the reconnect: conn2 first {conn2_first_ts} >= conn1 last {conn1_last_ts}"
    );
    let ts2: Vec<u32> = conn2.iter().map(|(_, ts, _)| *ts).collect();
    assert!(ts2.windows(2).all(|w| w[0] <= w[1]), "monotonic after resume: {ts2:?}");

    // --- Recovery speed. ---
    assert!(
        recovered_in < Duration::from_secs(10),
        "recovered comfortably within budget: {recovered_in:?}"
    );
}
