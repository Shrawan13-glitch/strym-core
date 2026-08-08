//! End-to-end demo of the core: it generates real H.264 + AAC test media with
//! ffmpeg, parses it into `MediaPacket`s (as a real platform would), pushes it
//! through the engine, and muxes a real playable `.flv`.
//!
//! Run:  `cargo run --example flv_demo -- <out.flv>`
//!
//! Then verify with:  `ffplay <out.flv>`   (or `ffprobe` for a quick check)

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use stream::codecs::{aac, h264};
use stream::engine::{Engine, EngineConfig};
use stream::models::{MediaPacket, StreamConfig};
use stream::transport::FileTransport;

fn main() {
    let out = std::env::args().nth(1).unwrap_or_else(|| "assets/out.flv".into());
    let out = PathBuf::from(out);
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir).ok();
    }

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

    // Codec configs, as a real platform would hand them over.
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

    // Wire the engine up to a file.
    let config = EngineConfig {
        stream: StreamConfig {
            width: 128,
            height: 96,
            framerate: 15.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let file = std::fs::File::create(&out).expect("create out.flv");
    let engine = Engine::new(config);
    engine.attach_transport(FileTransport::new(file));
    engine
        .configure_codecs(Some(&avcc), Some(&asc))
        .expect("configure codecs");

    println!("[demo] streaming (real-time paced)...");
    // Simulate a live source: interleave video/audio in pts order, paced in
    // real time, while ticking the engine.
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

    // Drain anything left in the buffer, then close.
    engine.tick().unwrap();
    engine.finish().unwrap();

    let s = engine.stats();
    println!("[demo] done -> {}", out.display());
    println!(
        "[demo] pushed={} muxed={} dropped={} buffer_ms={}",
        s.pushed, s.muxed, s.dropped, s.buffer_ms
    );
}

/// Generate H.264 (Annex-B) and AAC (ADTS) test files.
fn gen_media(video: &std::path::Path, audio: &std::path::Path) {
    // Video: 3s, 128x96, 15fps, keyframe every 15 frames.
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
    // Audio: 3s of a sine tone as AAC-LC in ADTS.
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

/// Split the Annex-B H.264 file into per-frame packets keeping only VCL
/// slices; SPS/PPS travel separately via `configure_codecs`. Timestamps are
/// assigned assuming a constant frame rate.
fn parse_video(path: &std::path::Path) -> Vec<MediaPacket> {
    let data = std::fs::read(path).expect("read video.h264");
    let nals = h264::split_annex_b(&data);

    // Group NALs into frames, keeping **only picture slices** (VCL units). SPS/PPS
    // are handled once via `configure_codecs` (the AVCC sequence header), exactly
    // as real FLV producers do — every picture frame is a clean access unit with
    // no embedded start-code lookalikes.
    let mut frames: Vec<Vec<&[u8]>> = Vec::new();
    for nal in nals {
        let t = nal[0] & 0x1F;
        if VCL.contains(&t) {
            frames.push(vec![nal]);
        }
    }

    let mut out = Vec::new();
    let frame_ms = 1000.0 / 15.0;
    for (i, frame) in frames.iter().enumerate() {
        let mut buf = Vec::new();
        for nal in frame {
            buf.extend_from_slice(&[0, 0, 0, 1]);
            buf.extend_from_slice(nal);
        }
        let pts = (i as f64 * frame_ms) as i64;
        out.push(MediaPacket::video(pts, h264::contains_keyframe(&buf), buf));
    }
    out
}

/// Split the ADTS AAC file into per-frame packets using the real-time duration
/// of each AAC frame (1024 samples).
fn parse_audio(path: &std::path::Path) -> Vec<MediaPacket> {
    let data = std::fs::read(path).expect("read audio.aac");
    let mut out = Vec::new();
    let mut offset = 0usize;
    let mut pts: i64 = 0;
    let sr = 44_100f64;
    while let Some(h) = aac::parse_adts(&data[offset..]) {
        // Guard against a malformed header reporting a bogus/zero length.
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
