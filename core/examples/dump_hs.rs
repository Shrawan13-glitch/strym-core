//! Manual RTMP handshake probe: connects to a local RTMP server, runs an
//! ffmpeg-style C0/C1/C2 exchange, sends a `connect`, and hex-dumps whatever
//! the server answers. Useful for poking at real servers while developing.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, SystemTime};

use stream::rtmp::amf0;

fn frame(cid: u8, mtype: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let mut out = vec![cid, 0, 0, 0, (len >> 16) as u8, (len >> 8) as u8, len as u8, mtype];
    out.extend_from_slice(&stream_id.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Hex-encode a byte slice without pulling in a formatting crate.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn main() {
    let mut s = TcpStream::connect("127.0.0.1:1935").unwrap();
    s.set_read_timeout(Some(Duration::from_millis(1200))).unwrap();

    // ffmpeg-style handshake: C1 time=0, version bytes, random filler; C2 zeros.
    let mut c1 = [0u8; 1536];
    c1[4..8].copy_from_slice(&[0x09, 0x00, 0x7c, 0x02]);
    s.write_all(&[3]).unwrap();
    s.write_all(&c1).unwrap();
    let mut s0 = [0u8; 1];
    s.read_exact(&mut s0).unwrap();
    let mut s1 = [0u8; 1536];
    s.read_exact(&mut s1).unwrap();
    let mut s2 = [0u8; 1536];
    s.read_exact(&mut s2).unwrap();
    s.write_all(&[0u8; 1536]).unwrap();

    let mut w = amf0::Writer::new();
    w.raw_string("connect").number(1.0).object(&[
        ("app", amf0::ObjVal::Str("live")),
        ("flashVer", amf0::ObjVal::Str("FMLE/3.0 (compatible; FMSc/1.0)")),
        ("tcUrl", amf0::ObjVal::Str("rtmp://127.0.0.1:1935/live")),
        ("fpad", amf0::ObjVal::Bool(false)),
        ("capabilities", amf0::ObjVal::Num(15.0)),
        ("audioCodecs", amf0::ObjVal::Num(4071.0)),
        ("videoCodecs", amf0::ObjVal::Num(252.0)),
        ("videoFunction", amf0::ObjVal::Num(1.0)),
    ]);
    let p = w.into_bytes();
    s.write_all(&frame(3, 20, 0, &p)).unwrap();

    let mut total = Vec::new();
    let t0 = SystemTime::now();
    let mut buf = [0u8; 4096];
    while t0.elapsed().unwrap().as_millis() < 1200 {
        match s.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => total.extend_from_slice(&buf[..n]),
        }
    }
    println!("reply {} bytes: {}", total.len(), hex(&total));
}
