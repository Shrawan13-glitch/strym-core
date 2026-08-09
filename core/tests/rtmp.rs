//! RTMP ingest interop test — the server against a real publisher.
//!
//! Round trip: `ffmpeg` (a *real, independent* RTMP client) publishes a live
//! H.264 + AAC stream over TCP to [`RtmpServer`]; the server decodes the FLV
//! tag bodies and drives an HLS output, and the resulting playlist + fMP4
//! segments are probed and fully decoded by `ffprobe` / `ffmpeg`. This proves
//! the server speaks the protocol wire-compatible with the reference tools.
//!
//! Needs `ffmpeg`/`ffprobe` on PATH; prints a notice and passes vacuously when
//! they are absent (CI), and runs for real when present.

use std::path::PathBuf;
use std::process::Command;
use std::thread;

use stream::hls::{DirStorage, HlsConfig, HlsOutput};
use stream::rtmp::server::{MultiSinkHandler, RtmpServer, ServerConfig, SinkHandler};

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
    let dir = std::env::temp_dir().join(format!("stream-rtmp-{}-{nanos}", std::process::id()));
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

#[test]
fn ffmpeg_publishes_to_server_and_hls_roundtrips() {
    let (Some(ffmpeg), Some(ffprobe)) = (tool("ffmpeg"), tool("ffprobe")) else {
        eprintln!("skipping ffmpeg_publishes_to_server_and_hls_roundtrips: ffmpeg/ffprobe not installed");
        return;
    };
    let Some((encoder, encoder_args)) = pick_h264_encoder(&ffmpeg) else {
        eprintln!("skipping: no H.264 encoder in this ffmpeg build");
        return;
    };

    let dir = scratch();
    let hls_dir = dir.join("hls");
    let server = RtmpServer::bind(
        "127.0.0.1:0",
        ServerConfig {
            app: "live".to_owned(),
            ..Default::default()
        },
    )
    .expect("bind");
    let addr = server.local_addr().expect("local addr");
    let url = format!("rtmp://{}:{}/live/stream", addr.ip(), addr.port());

    // The ingest handler: HLS output backed by real files.
    let hls = HlsOutput::new(
        HlsConfig {
            target_duration_secs: 2,
            window_size: 64,
            ..Default::default()
        },
        DirStorage::new(&hls_dir).expect("hls dir"),
    );
    let handler = MultiSinkHandler::with(vec![SinkHandler::boxed(Box::new(hls))]);
    let server_thread = thread::spawn(move || server.serve(handler));

    // A real, independent RTMP publisher: ffmpeg encodes a synthetic source
    // and pushes it to our ingest URL.
    let publish = Command::new(&ffmpeg)
        .arg("-y")
        .arg("-hide_banner")
        .args(["-f", "lavfi", "-i", "testsrc2=size=160x120:rate=25:duration=3"])
        .args(["-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100:duration=3"])
        .args(["-c:v", encoder])
        .args(encoder_args)
        .args(["-c:a", "aac", "-b:a", "64k", "-f", "flv"])
        .arg(&url)
        .output()
        .expect("ffmpeg runs");
    assert!(
        publish.status.success(),
        "ffmpeg publish failed: {}",
        String::from_utf8_lossy(&publish.stderr)
    );

    // ffmpeg closes the publish when its source ends; the session then returns.
    let (info, _handler) = server_thread.join().expect("server thread").expect("ingest completed");
    assert_eq!(info.app, "live");
    assert_eq!(info.key, "stream");

    // The ingest produced a real HLS output.
    let playlist_path = hls_dir.join("playlist.m3u8");
    let playlist = std::fs::read_to_string(&playlist_path).expect("playlist written");
    assert!(playlist.contains("#EXT-X-MAP:URI=\"init.mp4\""));
    assert!(playlist.contains("#EXT-X-ENDLIST"), "finished(): {playlist}");
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
