//! HLS interop tests — validates the segmenter against the reference tools.
//!
//! Round trip: `ffmpeg` encodes a synthetic source into FLV (real H.264 + real
//! AAC) → we demux it, run it through the engine into `HlsOutput` → the
//! resulting playlist + fMP4 segments are probed and fully decoded by
//! `ffprobe` / `ffmpeg`.
//!
//! These tests need `ffmpeg`/`ffprobe` on PATH; they print a notice and pass
//! vacuously when the tools are absent (CI), and run for real when present.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use stream::engine::{Engine, EngineConfig, LatencyProfile};
use stream::flv::{self, Decoded};
use stream::hls::{DirStorage, HlsConfig, HlsOutput};
use stream::models::StreamConfig;
use stream::rtmp::FlvTagParser;
use stream::transport::FileTransport;

/// Locate a tool on PATH; `None` when missing.
fn tool(name: &str) -> Option<String> {
    let out = Command::new("which").arg(name).output().ok()?;
    if out.status.success() {
        Some(name.to_owned())
    } else {
        None
    }
}

/// Unique scratch directory for one test run.
fn scratch() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let dir = std::env::temp_dir().join(format!("stream-hls-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// Which H.264 encoder this ffmpeg build ships (distro builds vary).
fn pick_h264_encoder(ffmpeg: &str) -> Option<(&'static str, &'static [&'static str])> {
    let out = Command::new(ffmpeg).args(["-hide_banner", "-encoders"]).output().ok()?;
    let list = String::from_utf8_lossy(&out.stdout);
    if list.contains("libx264") {
        Some((
            "libx264",
            &[
                "-preset",
                "ultrafast",
                "-tune",
                "zerolatency",
                "-g",
                "25",
                "-pix_fmt",
                "yuv420p",
            ],
        ))
    } else if list.contains("libopenh264") {
        Some(("libopenh264", &["-g", "25", "-allow_skip_frames", "1"]))
    } else {
        None
    }
}

/// `ffmpeg` a synthetic source (video + audio) into an FLV file.
fn make_reference_flv(ffmpeg: &str, dir: &Path) -> PathBuf {
    let Some((encoder, encoder_args)) = pick_h264_encoder(ffmpeg) else {
        panic!("no H.264 encoder in this ffmpeg build");
    };
    let out = dir.join("source.flv");
    let status = Command::new(ffmpeg)
        .arg("-y")
        .args(["-f", "lavfi", "-i", "testsrc2=size=160x120:rate=25:duration=4"])
        .args(["-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100:duration=4"])
        .args(["-c:v", encoder])
        .args(encoder_args)
        .args(["-c:a", "aac", "-b:a", "64k", "-f", "flv"])
        .arg(&out)
        .status()
        .expect("ffmpeg runs");
    assert!(status.success(), "ffmpeg must produce the reference FLV");
    out
}

/// Demux an FLV file into codec configs + chronologically ordered packets.
fn demux_flv(path: &Path) -> (Option<Vec<u8>>, Option<Vec<u8>>, Vec<stream::models::MediaPacket>) {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .expect("open flv")
        .read_to_end(&mut bytes)
        .expect("read flv");
    let mut parser = FlvTagParser::new();
    let tags = parser.feed(&bytes).expect("well-formed FLV from ffmpeg");

    let (mut avcc, mut asc) = (None, None);
    let mut packets = Vec::new();
    for tag in &tags {
        match flv::decode_tag(tag.mtype, tag.ts, &tag.body) {
            Some(Decoded::VideoConfig(v)) => avcc = Some(v),
            Some(Decoded::AudioConfig(a)) => asc = Some(a),
            Some(Decoded::Packet(p)) => packets.push(p),
            None => {}
        }
    }
    (avcc, asc, packets)
}

#[test]
fn hls_round_trip_through_ffmpeg() {
    let (Some(ffmpeg), Some(ffprobe)) = (tool("ffmpeg"), tool("ffprobe")) else {
        eprintln!("skipping hls_round_trip_through_ffmpeg: ffmpeg/ffprobe not installed");
        return;
    };

    let dir = scratch();
    let flv_path = make_reference_flv(&ffmpeg, &dir);
    let (avcc, asc, packets) = demux_flv(&flv_path);
    let avcc = avcc.expect("ffmpeg FLV carries a video sequence header");
    let asc = asc.expect("ffmpeg FLV carries an audio sequence header");
    assert!(!packets.is_empty());
    let video_packets = packets
        .iter()
        .filter(|p| p.kind == stream::models::MediaKind::Video)
        .count();
    assert!(video_packets >= 90, "about 4s at 25fps: {video_packets}");

    // Engine → HLS output writing real files. `Lenient` profile: this test
    // batch-feeds 4 s at once and must not lose packets to live-edge cuts.
    let hls_dir = dir.join("hls");
    let config = EngineConfig {
        stream: StreamConfig {
            width: 160,
            height: 120,
            ..Default::default()
        },
        profile: LatencyProfile::Lenient,
        ..Default::default()
    };
    let engine: Engine<FileTransport<Vec<u8>>> = Engine::new(config);
    engine.attach_transport(FileTransport::new(Vec::new()));
    engine.configure_codecs(Some(&avcc), Some(&asc)).unwrap();

    let hls = HlsOutput::new(
        HlsConfig {
            target_duration_secs: 2,
            window_size: 64, // keep the whole stream for the decode check
            ..Default::default()
        },
        DirStorage::new(&hls_dir).expect("hls dir"),
    );
    engine.attach_output(Box::new(hls));

    // Push in ~1 s chunks with ticks in between, like a paced live capture.
    for chunk in packets.chunks(43) {
        engine.push_all(chunk.to_vec()).unwrap();
        engine.tick().unwrap();
    }
    engine.finish().unwrap();

    let playlist_path = hls_dir.join("playlist.m3u8");
    let playlist = std::fs::read_to_string(&playlist_path).expect("playlist written");
    assert!(playlist.contains("#EXT-X-MAP:URI=\"init.mp4\""));
    assert!(playlist.contains("#EXT-X-ENDLIST"));
    assert!(hls_dir.join("init.mp4").exists());
    assert!(hls_dir.join("seg0.m4s").exists());

    // ffprobe: two streams, h264 + aac, right dimensions.
    let probe = Command::new(&ffprobe)
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name,codec_type,width,height",
            "-of",
            "csv=p=0",
        ])
        .arg(&playlist_path)
        .output()
        .expect("ffprobe runs");
    assert!(probe.status.success(), "ffprobe must accept the playlist");
    let streams = String::from_utf8_lossy(&probe.stdout);
    assert!(streams.contains("h264"), "video stream: {streams}");
    assert!(streams.contains("aac"), "audio stream: {streams}");
    assert!(
        streams.contains("160") && streams.contains("120"),
        "dimensions: {streams}"
    );

    // ffprobe: total duration close to the 4 s source.
    let dur = Command::new(&ffprobe)
        .args(["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0"])
        .arg(&playlist_path)
        .output()
        .expect("ffprobe runs");
    let duration_secs: f64 = String::from_utf8_lossy(&dur.stdout).trim().parse().unwrap_or(0.0);
    assert!(
        (3.0..=6.0).contains(&duration_secs),
        "playlist should span ~4s, got {duration_secs}s"
    );

    // Full decode: ffmpeg must read every segment without errors.
    let decode = Command::new(&ffmpeg)
        .args(["-v", "error", "-i"])
        .arg(&playlist_path)
        .args(["-f", "null", "-"])
        .output()
        .expect("ffmpeg runs");
    let stderr = String::from_utf8_lossy(&decode.stderr);
    assert!(decode.status.success(), "decode failed: {stderr}");
    assert!(stderr.trim().is_empty(), "decoder complained: {stderr}");

    let _ = std::fs::remove_dir_all(&dir);
}
