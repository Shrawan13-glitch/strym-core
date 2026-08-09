//! Publish a live stream to a real RTMP server using the core's `RtmpTransport`.
//!
//! Uses the same ffmpeg-generated H.264/AAC test media as `flv_demo`, but sends
//! it over RTMP instead of writing a file.
//!
//! Run:  `cargo run --example rtmp_demo -- <rtmp-url> [stream-key]`
//! e.g.  `cargo run --example rtmp_demo -- rtmp://127.0.0.1:1935/live mykey`
//!
//! Verify while it runs:  `ffplay rtmp://127.0.0.1:1935/live/mykey`

use std::process::Command;
use std::time::Duration;

use stream::codecs::{aac, h264};
use stream::engine::{Engine, EngineConfig};
use stream::models::{MediaPacket, StreamConfig};
use stream::rtmp::{RtmpConfig, RtmpTransport};

fn main() {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "rtmp://127.0.0.1:1935/live".into());
    let key = std::env::args().nth(2).unwrap_or_else(|| "demo".into());
    let host_port = url
        .strip_prefix("rtmp://")
        .unwrap_or(&url)
        .split('/')
        .next()
        .unwrap_or(&url);

    let work = std::env::temp_dir().join("stream_core_demo");
    std::fs::create_dir_all(&work).ok();
    let video = work.join("video.h264");
    let audio = work.join("audio.aac");

    println!("[demo] generating test media with ffmpeg...");
    gen_media(&video, &audio);

    println!("[demo] parsing into media packets...");
    let video_packets = parse_video(&video);
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
            width: 128,
            height: 96,
            framerate: 15.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let engine = Engine::new(config);

    // Handshake + connect + createStream + publish happen here, before any data.
    println!("[demo] connecting to {url} ...");
    let rtmp_cfg = RtmpConfig::new("live", &key, &url);
    let transport = RtmpTransport::connect_tcp(host_port, rtmp_cfg).expect("rtmp publish handshake");
    println!("[demo] publishing as stream `{key}`");

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

fn gen_media(video: &std::path::Path, audio: &std::path::Path) {
    let v = Command::new("ffmpeg")
        .args([
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=128x96:rate=15:duration=3",
            "-c:v",
            "libopenh264",
            "-b:v",
            "400k",
            "-profile:v",
            "66",
            "-pix_fmt",
            "yuv420p",
            "-g",
            "15",
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
            "sine=frequency=440:duration=3",
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

fn parse_video(path: &std::path::Path) -> Vec<MediaPacket> {
    let data = std::fs::read(path).expect("read video.h264");
    let nals = h264::split_annex_b(&data);
    let mut out = Vec::new();
    let frame_ms = 1000.0 / 15.0;
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
