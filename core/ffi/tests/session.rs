//! End-to-end tests of the mobile `StreamSession` facade over a real TCP
//! socket: a minimal RTMP ingest server accepts the publish and collects the
//! media, while the test drives the session through its documented lifecycle
//! (`start` → live → `stop`) and its failure paths (unreachable host,
//! mid-stream outage → reconnect).

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use stream::rtmp::amf0;
use stream_ffi::{
    default_session_config, LogLevel, LogSink, RtmpDestination, SessionConfig, SessionState, SessionStats, StreamInfo,
    StreamListener, StreamSession,
};

const MSG_SET_CHUNK_SIZE: u8 = 1;
const MSG_AMF0_DATA: u8 = 18;
const MSG_AMF0_COMMAND: u8 = 20;
const MSG_AUDIO: u8 = 8;
const MSG_VIDEO: u8 = 9;

/// Kill the first connection after this many media messages, to prove the
/// session reconnects and resumes.
const KILL_AFTER: usize = 20;

type MediaMsg = (u8, u32, Vec<u8>);

#[derive(Default)]
struct Report {
    conns: Vec<Vec<MediaMsg>>,
}

struct Partial {
    remaining: usize,
    mtype: u8,
    ts: u32,
    payload: Vec<u8>,
}

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

fn chunk0(mtype: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + payload.len());
    out.push(0x03);
    out.extend_from_slice(&[0, 0, 0]);
    let len = payload.len() as u32;
    out.extend_from_slice(&len.to_be_bytes()[1..4]);
    out.push(mtype);
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Handshake, control plane, media collection — for one accepted connection.
fn serve_conn(sock: &mut TcpStream, index: usize, report: &Arc<Mutex<Report>>) -> io::Result<()> {
    let mut c0c1 = [0u8; 1537];
    sock.read_exact(&mut c0c1)?;
    assert_eq!(c0c1[0], 3);
    let mut s0s1s2 = vec![3u8];
    s0s1s2.extend_from_slice(&[0u8; 1536]);
    s0s1s2.extend_from_slice(&c0c1[1..]);
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
                let vals = r.read_all();
                let Some(amf0::Val::String(name)) = vals.first() else {
                    continue;
                };
                match name.as_str() {
                    "connect" => {
                        sock.write_all(&chunk0(5, &2_500_000u32.to_be_bytes()))?;
                        let mut bw = 2_500_000u32.to_be_bytes().to_vec();
                        bw.push(2); // dynamic
                        sock.write_all(&chunk0(6, &bw))?;
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
                if index == 0 && conns[0].len() >= KILL_AFTER {
                    break;
                }
            }
            _ => {}
        }
        sock.flush()?;
    }
    Ok(())
}

/// A listener that records what the session told it.
#[derive(Default)]
struct Recorder {
    states: Mutex<Vec<(SessionState, Option<String>)>>,
    stats: Mutex<Vec<SessionStats>>,
}

impl StreamListener for Recorder {
    fn on_state_changed(&self, state: SessionState, detail: Option<String>) {
        self.states.lock().unwrap().push((state, detail));
    }

    fn on_stats(&self, stats: SessionStats) {
        self.stats.lock().unwrap().push(stats);
    }
}

fn dest(addr: &std::net::SocketAddr) -> RtmpDestination {
    RtmpDestination {
        url: format!("rtmp://{addr}"),
        app: "live".to_owned(),
        stream_key: "test".to_owned(),
        timeout_ms: 0,
    }
}

/// Fast, deterministic reconnect knobs for the tests.
fn fast_config(addr: &std::net::SocketAddr) -> SessionConfig {
    let mut config = default_session_config(dest(addr), StreamInfo::default());
    config.reconnect_initial_delay_ms = 50;
    config.reconnect_max_delay_ms = 250;
    config.reconnect_max_attempts = Some(20);
    config.pump_interval_ms = 5;
    config.stats_interval_ms = 100;
    config
}

fn video_frame(pts: i64, is_key: bool) -> (i64, bool, Vec<u8>) {
    let mut data = vec![0u8, 0, 0, 1];
    data.push(if is_key { 0x65 } else { 0x41 });
    data.resize(2048, 0xAB);
    (pts, is_key, data)
}

/// The AAC `AudioSpecificConfig` and H.264 `AVCDecoderConfigurationRecord`
/// the engine re-emits on every fresh connection.
const ASC: &[u8] = &[0x0A, 0x10];
const AVCC: &[u8] = &[
    0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x03, 0x67, 0x42, 0x00, 0x0A, 0x01, 0x00, 0x03, 0x68, 0xCE,
];

#[test]
fn session_publishes_to_a_loopback_server() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let report = Arc::new(Mutex::new(Report::default()));
    let server_report = report.clone();
    let server = thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut sock) = stream else { break };
            let _ = sock.set_nodelay(true);
            let r = server_report.clone();
            thread::spawn(move || {
                let _ = serve_conn(&mut sock, 0, &r);
            });
        }
    });

    let recorder = Arc::new(Recorder::default());
    let session = StreamSession::new(fast_config(&addr), recorder.clone()).unwrap();
    session
        .configure_codecs(Some(AVCC.to_vec()), Some(ASC.to_vec()))
        .unwrap();
    session.start().unwrap();

    // Pump synthetic capture until the server has received enough media.
    let t0 = Instant::now();
    let mut next_video = 0i64;
    let mut next_audio = 0i64;
    let mut frame = 0u64;
    loop {
        assert!(t0.elapsed() < Duration::from_secs(10), "never went live");
        let now_ms = i64::try_from(t0.elapsed().as_millis()).unwrap();
        while next_video <= now_ms {
            let (pts, key, data) = video_frame(next_video, frame.is_multiple_of(30));
            session.push_video(pts, key, data);
            next_video += 33;
            frame += 1;
        }
        while next_audio <= now_ms {
            session.push_audio(next_audio, vec![0x21, 0x00, 0x49, 0x10, 0x04]);
            next_audio += 43;
        }
        let rep = report.lock().unwrap();
        let got = rep.conns.first().map_or(0, Vec::len);
        if got >= 10 {
            break;
        }
        drop(rep);
        thread::sleep(Duration::from_millis(5));
    }

    // The session told us it went live, then we stopped it.
    let states = recorder.states.lock().unwrap();
    assert!(
        states.iter().any(|(s, _)| *s == SessionState::Live),
        "listener saw Live: {states:?}"
    );
    drop(states);

    let rep = report.lock().unwrap();
    let conn = rep.conns.first().unwrap();
    assert!(
        conn.iter().any(|(t, _, _)| *t == MSG_AMF0_DATA),
        "metadata reached the server"
    );
    assert!(
        conn.iter().any(|(t, _, p)| *t == MSG_VIDEO && p.get(1) == Some(&0)),
        "video sequence header reached the server"
    );
    assert!(
        conn.iter().any(|(t, _, p)| *t == MSG_AUDIO && p.get(1) == Some(&0)),
        "audio sequence header reached the server"
    );
    assert!(
        conn.iter().any(|(t, _, p)| *t == MSG_VIDEO && p.get(1) == Some(&1)),
        "video media reached the server"
    );
    drop(rep);

    // Lifecycle checks while live.
    assert_eq!(session.state(), SessionState::Live);
    let snapshot = session.stats();
    assert!(snapshot.pushed >= 10);
    assert!(snapshot.muxed > 0);
    assert!(snapshot.dropped <= snapshot.pushed);

    session.stop();
    assert_eq!(session.state(), SessionState::Stopped);
    drop(server);
}

#[test]
fn session_reconnects_after_the_server_drops_the_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let report = Arc::new(Mutex::new(Report::default()));
    let server_report = report.clone();
    let server = thread::spawn(move || {
        for (index, stream) in listener.incoming().enumerate() {
            let Ok(mut sock) = stream else { break };
            let _ = sock.set_nodelay(true);
            let r = server_report.clone();
            thread::spawn(move || {
                let _ = serve_conn(&mut sock, index, &r);
            });
        }
    });

    let recorder = Arc::new(Recorder::default());
    let session = StreamSession::new(fast_config(&addr), recorder.clone()).unwrap();
    session
        .configure_codecs(Some(AVCC.to_vec()), Some(ASC.to_vec()))
        .unwrap();
    session.start().unwrap();

    let t0 = Instant::now();
    let mut next_video = 0i64;
    let mut next_audio = 0i64;
    let mut frame = 0u64;
    loop {
        assert!(t0.elapsed() < Duration::from_secs(20), "never recovered");
        let now_ms = i64::try_from(t0.elapsed().as_millis()).unwrap();
        while next_video <= now_ms {
            let (pts, key, data) = video_frame(next_video, frame.is_multiple_of(30));
            session.push_video(pts, key, data);
            next_video += 33;
            frame += 1;
        }
        while next_audio <= now_ms {
            session.push_audio(next_audio, vec![0x21, 0x00, 0x49, 0x10, 0x04]);
            next_audio += 43;
        }
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
        thread::sleep(Duration::from_millis(5));
    }

    let recovered_in = t0.elapsed();
    assert!(recovered_in < Duration::from_secs(15), "recovery too slow");

    let rep = report.lock().unwrap();
    assert!(rep.conns[0].len() >= KILL_AFTER, "server killed a live stream");
    let conn2 = &rep.conns[1];
    assert!(
        conn2.iter().any(|(t, _, p)| *t == MSG_VIDEO && p.get(1) == Some(&0)),
        "video sequence header re-emitted after reconnect"
    );
    let first_media = conn2
        .iter()
        .position(|(t, _, p)| *t == MSG_VIDEO && p.get(1) == Some(&1))
        .expect("second connection carries video media");
    assert_eq!(conn2[first_media].0, MSG_VIDEO);
    assert_eq!(conn2[first_media].2[0] >> 4, 1, "resumes on a keyframe");
    let conn1_last_ts = rep.conns[0].iter().map(|(_, ts, _)| *ts).max().unwrap();
    assert!(
        conn2[first_media].1 >= conn1_last_ts,
        "timestamps continue across the reconnect"
    );
    drop(rep);

    // The listener saw Live and returned to Live after the outage; the core
    // bookkeeping confirms a reconnect happened. (When the server is available
    // again instantly, the reconnect completes inside one worker tick, so
    // `Reconnecting` — emitted only when backoff yields between attempts — may
    // not appear here.)
    let states = recorder.states.lock().unwrap();
    assert!(
        states.iter().any(|(s, _)| *s == SessionState::Live),
        "listener saw Live: {states:?}"
    );
    drop(states);
    assert!(session.reconnect_count() >= 1);

    session.stop();
    drop(server);
}

#[test]
fn session_reports_connect_failure_and_can_be_retried() {
    // Bind a listener then drop it, so the port is definitely closed.
    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };

    let recorder = Arc::new(Recorder::default());
    let session = StreamSession::new(fast_config(&addr), recorder.clone()).unwrap();
    session.start().unwrap();

    // The initial connect fails: the worker reports Idle and exits.
    let t0 = Instant::now();
    loop {
        assert!(t0.elapsed() < Duration::from_secs(10), "connect never failed");
        if session.state() == SessionState::Idle {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    assert!(session.last_error().is_some(), "last_error is populated");
    let states = recorder.states.lock().unwrap();
    assert!(
        states.iter().any(|(s, d)| *s == SessionState::Idle && d.is_some()),
        "listener got a failed-connect Idle with detail: {states:?}"
    );
    drop(states);

    // `retry` re-launches the worker, which fails again (still no server).
    session.retry().unwrap();
    let t0 = Instant::now();
    loop {
        assert!(t0.elapsed() < Duration::from_secs(10), "retry never re-ran");
        if session.state() == SessionState::Connecting {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    loop {
        assert!(t0.elapsed() < Duration::from_secs(10), "retry never failed");
        if session.state() == SessionState::Idle {
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }

    // After the retried failure the session is launchable once more (the
    // worker cleared its handle), so a real retry can succeed.
    assert_eq!(session.state(), SessionState::Idle);
    session.stop();
}

#[test]
fn session_rejects_bad_configuration_immediately() {
    let recorder = Arc::new(Recorder::default());
    let mut config = fast_config(&"127.0.0.1:1935".parse().unwrap());
    config.destination.url = "http://example.tv".to_owned();
    match StreamSession::new(config, recorder.clone()) {
        Err(e) => assert!(e.to_string().contains("rtmp://")),
        Ok(_) => panic!("bad URL must be rejected at construction"),
    }
}

#[test]
fn session_routes_logs_to_the_platform_sink() {
    // Install a recorder and exercise the core enough to produce a log record.
    static LOGS: AtomicU32 = AtomicU32::new(0);
    struct Sink;
    impl LogSink for Sink {
        fn on_log(&self, _level: LogLevel, _module: String, _message: String) {
            LOGS.fetch_add(1, Ordering::Relaxed);
        }
    }
    stream_ffi::set_log_sink(Some(Arc::new(Sink)));

    let addr = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    let recorder = Arc::new(Recorder::default());
    let session = StreamSession::new(fast_config(&addr), recorder.clone()).unwrap();
    session.start().unwrap();
    let t0 = Instant::now();
    while session.state() != SessionState::Idle && t0.elapsed() < Duration::from_secs(5) {
        thread::sleep(Duration::from_millis(5));
    }
    session.stop();

    assert!(
        LOGS.load(Ordering::Relaxed) > 0,
        "the core logged through the platform sink"
    );
    stream_ffi::set_log_sink(None);
}
