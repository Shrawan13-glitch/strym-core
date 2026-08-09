//! Publish a live stream to a real RTMP server using the core's `RtmpTransport`.
//!
//! Uses ffmpeg-generated H.264/AAC test media (a timestamped test pattern plus a
//! audio tone), sends it over RTMP, paced in real time. The video source is
//! `testsrc`, which draws a moving frame counter + seconds clock, so a live
//! viewer can tell at a glance the stream is actually advancing.
//!
//! Run:  `cargo run --example rtmp_demo -- <url> [stream-key] [seconds] [WxH]`
//! e.g.  `cargo run --example rtmp_demo -- rtmp://127.0.0.1:1935/live mykey 600 640x360`
//!       `cargo run --example rtmp_demo -- rtmp://a.rtmp.youtube.com/live2 <your-key> 900 1280x720`
//!
//! Verify while it runs:  `ffplay rtmp://127.0.0.1:1935/live/mykey`

use std::process::Command;
use std::time::Duration;

use stream::codecs::{aac, h264};
use stream::engine::{Engine, EngineConfig};
use stream::models::{MediaPacket, StreamConfig};
use stream::rtmp::{RtmpConfig, RtmpTransport};

#[allow(clippy::too_many_lines)]
fn main() {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "rtmp://127.0.0.1:1935/live".into());
    let key = std::env::args().nth(2).unwrap_or_else(|| "demo".into());
    // [duration] defaults to 10 minutes; [WxH] defaults to 640x360.
    let duration_s = std::env::args().nth(3).and_then(|s| s.parse().ok()).unwrap_or(600u64);
    let (width, height) = std::env::args().nth(4).map_or((640, 360), |s| {
        let mut it = s.split('x');
        let w = it.next().and_then(|p| p.parse().ok()).unwrap_or(640);
        let h = it.next().and_then(|p| p.parse().ok()).unwrap_or(360);
        (w, h)
    });
    let fps = 30.0;
    // Parse `rtmp://host[:port]/app` (port optional, defaults to 1935). The app
    // name comes from the URL path (e.g. YouTube's `live2`); tcUrl is rebuilt
    // without a trailing slash so servers don't reject the connect.
    let rest = url.strip_prefix("rtmp://").unwrap_or(&url);
    let (host_port, app) = match rest.split_once('/') {
        Some((host_part, path)) => {
            let (host, port) = match host_part.rsplit_once(':') {
                Some((h, p)) if p.parse::<u16>().is_ok() => (h.to_owned(), p.to_owned()),
                _ => (host_part.to_owned(), "1935".to_owned()),
            };
            let app = path.split('/').next().unwrap_or("live");
            (format!("{host}:{port}"), app.to_owned())
        }
        None => (format!("{rest}:1935"), "live".to_owned()),
    };
    let tc_url = format!("rtmp://{host_port}/{app}");

    let work = std::env::temp_dir().join("stream_core_demo");
    std::fs::create_dir_all(&work).ok();
    let video = work.join("video.h264");
    let audio = work.join("audio.aac");

    println!("[demo] generating {duration_s}s of test media at {width}x{height} with ffmpeg...");
    gen_media(&video, &audio, duration_s, width, height, fps);

    println!("[demo] parsing into media packets...");
    let video_packets = parse_video(&video, fps);
    let audio_packets = parse_audio(&audio);
    println!(
        "[demo]   {} video packets, {} audio packets",
        video_packets.len(),
        audio_packets.len()
    );

    let avcc = {
        let data = std::fs::read(&video).expect("read video for config");
        let (sps, pps) = h264::extract_sps_pps(&data).expect("SPS/PPS in stream");
        h264::build_avcc(&sps, &pps)
    };
    let asc = {
        let data = std::fs::read(&audio).expect("read audio for config");
        let h = aac::parse_adts(&data).expect("ADTS in stream");
        aac::build_asc(&h)
    };

    let config = EngineConfig {
        stream: StreamConfig {
            width,
            height,
            framerate: fps,
            ..Default::default()
        },
        ..Default::default()
    };
    let engine = Engine::new(config);

    // Handshake + connect + createStream + publish happen here, before any data.
    println!("[demo] connecting to {url} ...");
    let rtmp_cfg = RtmpConfig::new(&app, &key, &tc_url);
    let transport = RtmpTransport::connect_tcp(&host_port, rtmp_cfg).expect("rtmp publish handshake");
    println!("[demo] publishing (stream key set, not shown)");

    engine.attach_transport(transport);
    engine
        .configure_codecs(Some(&avcc), Some(&asc))
        .expect("configure codecs");

    println!("[demo] streaming (real-time paced)...");
    let mut vi = 0usize;
    let mut ai = 0usize;
    let start = std::time::Instant::now();
    loop {
        let now_ms = start.elapsed().as_millis() as i64;
        let mut did_push = false;
        while let Some(v) = video_packets.get(vi) {
            if v.pts <= now_ms {
                engine.push(v.clone()).unwrap();
                vi += 1;
                did_push = true;
            } else {
                break;
            }
        }
        while let Some(a) = audio_packets.get(ai) {
            if a.pts <= now_ms {
                engine.push(a.clone()).unwrap();
                ai += 1;
                did_push = true;
            } else {
                break;
            }
        }
        engine.tick().unwrap();
        if vi >= video_packets.len() && ai >= audio_packets.len() {
            break;
        }
        if !did_push {
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    engine.tick().unwrap();
    engine.finish().unwrap();

    let s = engine.stats();
    println!(
        "[demo] done: pushed={} muxed={} dropped={} buffer_ms={}",
        s.pushed, s.muxed, s.dropped, s.buffer_ms
    );
}

fn gen_media(video: &std::path::Path, audio: &std::path::Path, seconds: u64, w: u32, h: u32, fps: f64) {
    // `testsrc` carries a moving frame counter + running seconds clock, so a
    // viewer can immediately tell the stream is advancing (live), not a slab.
    let gop = (fps * 2.0).round() as u32; // full keyframe every 2s
    let v = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            &format!("testsrc=size={w}x{h}:rate={fps}:duration={seconds}"),
            "-c:v",
            "libopenh264",
            "-b:v",
            if w * h > 640 * 360 { "1800k" } else { "800k" },
            "-profile:v",
            "66",
            "-pix_fmt",
            "yuv420p",
            "-g",
            &gop.to_string(),
        ])
        .arg(video)
        .output();
    check_ffmpeg(&v, "video");
    let a = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            &format!("sine=frequency=440:duration={seconds}"),
            "-c:a",
            "aac",
            "-b:a",
            "96k",
        ])
        .arg(audio)
        .output();
    check_ffmpeg(&a, "audio");
}

fn check_ffmpeg(out: &Result<std::process::Output, std::io::Error>, what: &str) {
    let out = match out {
        Ok(o) => o,
        Err(e) => panic!("could not spawn ffmpeg for {what}: {e}"),
    };
    assert!(
        out.status.success(),
        "ffmpeg failed for {what}:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// NAL unit types that carry picture slices (VCL units).
const VCL: [u8; 5] = [1, 2, 3, 4, 5];

fn parse_video(path: &std::path::Path, fps: f64) -> Vec<MediaPacket> {
    let data = std::fs::read(path).expect("read video.h264");
    let nals = h264::split_annex_b(&data);
    let mut out = Vec::new();
    let frame_ms = 1000.0 / fps;
    let mut i = 0usize;
    for nal in nals {
        let t = nal[0] & 0x1F;
        if VCL.contains(&t) {
            let mut buf = Vec::new();
            buf.extend_from_slice(&[0, 0, 0, 1]);
            buf.extend_from_slice(nal);
            let pts = (i as f64 * frame_ms) as i64;
            out.push(MediaPacket::video(pts, h264::contains_keyframe(&buf), buf));
            i += 1;
        }
    }
    out
}

fn parse_audio(path: &std::path::Path) -> Vec<MediaPacket> {
    let data = std::fs::read(path).expect("read audio.aac");
    let mut out = Vec::new();
    let mut offset = 0usize;
    let mut pts: i64 = 0;
    let sr = 44_100f64;
    while let Some(h) = aac::parse_adts(&data[offset..]) {
        if h.frame_length < h.header_length {
            break;
        }
        let end = offset + h.frame_length.min(data.len() - offset);
        if end <= offset {
            break;
        }
        out.push(MediaPacket::audio(pts, data[offset..end].to_vec()));
        pts += (1024.0 / sr * 1000.0).round() as i64;
        offset = end;
    }
    out
}
