//! End-to-end integration test: feeds encoded H.264 + AAC-LC packets through
//! the full engine pipeline (buffer -> muxer -> transport) and validates that
//! the resulting bytes form a well-formed, playable FLV stream: header, script
//! metadata, codec sequence headers, and correctly timestamped media tags.

use std::time::Duration;

use stream::engine::{Engine, EngineConfig};
use stream::models::{MediaPacket, StreamConfig};
use stream::telemetry::QosConfig;
use stream::transport::FileTransport;

const START_CODE: &[u8] = &[0x00, 0x00, 0x00, 0x01];

/// A minimal SPS NAL unit (header byte plus profile/compatibility/level).
fn sps() -> Vec<u8> {
    vec![0x67, 0x42, 0x00, 0x1F, 0x96, 0x55, 0x40]
}

/// A minimal PPS NAL unit.
fn pps() -> Vec<u8> {
    vec![0x68, 0xCE, 0x3C, 0x80]
}

/// An IDR-slice NAL unit wrapping `payload`.
fn idr(payload: &[u8]) -> Vec<u8> {
    let mut nal = Vec::with_capacity(1 + payload.len());
    nal.push(0x65);
    nal.extend_from_slice(payload);
    nal
}

/// Concatenate NAL units into a single Annex-B packet.
fn annex_b(nals: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for nal in nals {
        out.extend_from_slice(START_CODE);
        out.extend_from_slice(nal);
    }
    out
}

/// Build an ADTS-framed AAC-LC packet (AAC-LC, 44.1 kHz, stereo). The muxer
/// strips the ADTS header at mux time, so packets must carry it, exactly like
/// a real encoder emits them.
fn adts(payload: &[u8]) -> Vec<u8> {
    let frame_length = 7 + payload.len();
    let mut out = Vec::with_capacity(frame_length);
    out.push(0xFF); // sync word high byte
    out.push(0xF1); // sync low | MPEG-4 | layer 0 | protection absent
    out.push(0x50); // profile AAC-LC | sf index 4 (44.1k) | channel config hi
    out.push(0x80); // channel config lo | frame length hi
    out.push(((frame_length >> 3) & 0xFF) as u8);
    out.push((((frame_length & 0x07) as u8) << 5) | 0x1F); // buffer fullness
    out.push(0xFC); // one raw data block
    out.extend_from_slice(payload);
    out
}

/// A parsed FLV tag.
struct Tag {
    kind: u8,
    ts: u32,
    data: Vec<u8>,
}

/// Walk an FLV byte stream, returning every tag in order.
fn parse_flv(bytes: &[u8]) -> Result<Vec<Tag>, String> {
    if bytes.len() < 13 || &bytes[0..3] != b"FLV" {
        return Err("not an FLV stream".to_owned());
    }
    let mut tags = Vec::new();
    let mut pos = 13; // 9-byte header + previous tag size 0
    while pos < bytes.len() {
        if pos + 11 > bytes.len() {
            return Err("truncated tag header".to_owned());
        }
        let kind = bytes[pos];
        let size = u32::from_be_bytes([0, bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]) as usize;
        let ts_low = [bytes[pos + 4], bytes[pos + 5], bytes[pos + 6]];
        let ts = (u32::from(bytes[pos + 7]) << 24)
            | (u32::from(ts_low[0]) << 16)
            | (u32::from(ts_low[1]) << 8)
            | u32::from(ts_low[2]);
        let body_start = pos + 11;
        let body_end = body_start + size;
        if body_end + 4 > bytes.len() {
            return Err("truncated tag body".to_owned());
        }
        tags.push(Tag {
            kind,
            ts,
            data: bytes[body_start..body_end].to_vec(),
        });
        pos = body_end + 4; // skip PreviousTagSize
    }
    Ok(tags)
}

const TAG_AUDIO: u8 = 8;
const TAG_VIDEO: u8 = 9;
const TAG_SCRIPT: u8 = 18;

/// Expected `AudioSpecificConfig` for AAC-LC / 44.1 kHz / stereo.
const ASC_AAC_LC_44K_STEREO: [u8; 2] = [0x0A, 0x10];

/// Push a chronologically ordered mix of video and audio, drain, and validate
/// the resulting FLV end to end.
#[test]
fn engine_produces_playable_flv() {
    let key0 = annex_b(&[&sps(), &pps(), &idr(&[0x21, 0x88, 0x84])]);
    let key1 = annex_b(&[&sps(), &pps(), &idr(&[0x21, 0x90, 0x10])]);
    let inter = annex_b(&[&[0x41, 0x9A, 0x22]]);
    let audio_frame = [0x21, 0x00, 0x49, 0x90, 0x01, 0x02];
    let audio = adts(&audio_frame);

    let config = EngineConfig {
        stream: StreamConfig {
            width: 128,
            height: 96,
            ..Default::default()
        },
        ..Default::default()
    };

    let engine: Engine<FileTransport<Vec<u8>>> = Engine::new(config);
    engine.attach_transport(FileTransport::new(Vec::new()));

    let packets = [
        MediaPacket::video(0, true, key0),
        MediaPacket::audio(0, audio.clone()),
        MediaPacket::audio(23, audio.clone()),
        MediaPacket::video(33, true, key1),
        MediaPacket::audio(46, audio.clone()),
        MediaPacket::video(66, false, inter),
        MediaPacket::audio(69, audio.clone()),
    ];
    engine.push_all(packets).unwrap();

    let written = engine.tick().unwrap();
    assert_eq!(written, 7);

    let stats = engine.stats();
    assert_eq!(stats.pushed, 7);
    assert_eq!(stats.muxed, 7);
    assert_eq!(stats.dropped, 0);
    assert_eq!(stats.in_buffered_count, 0);

    let bytes = engine
        .attach_transport(FileTransport::new(Vec::new()))
        .expect("previous transport with the FLV bytes")
        .into_inner();
    assert!(!bytes.is_empty());

    let tags = parse_flv(&bytes).expect("well-formed FLV");
    let kinds = tags.iter().map(|t| t.kind).collect::<Vec<_>>();

    // metadata + video seq header + first keyframe + audio seq header + audio
    // x2 + video keyframe + audio + video inter + audio
    assert_eq!(
        kinds,
        vec![
            TAG_SCRIPT, TAG_VIDEO, TAG_VIDEO, TAG_AUDIO, TAG_AUDIO, TAG_AUDIO, TAG_VIDEO, TAG_AUDIO, TAG_VIDEO,
            TAG_AUDIO,
        ]
    );

    // timestamps must never go backwards
    for pair in tags.windows(2) {
        assert!(
            pair[0].ts <= pair[1].ts,
            "timestamps went backwards: {} > {}",
            pair[0].ts,
            pair[1].ts
        );
    }

    // first tag is the onMetaData script
    let meta = &tags[0];
    assert_eq!(meta.kind, TAG_SCRIPT);
    assert_eq!(&meta.data[0..3], &[0x02, 0x00, 0x0A]);
    assert_eq!(&meta.data[3..13], b"onMetaData");

    // video sequence header carries an AVCDecoderConfigurationRecord that
    // contains our SPS and PPS
    let vseq = &tags[1];
    assert_eq!(vseq.kind, TAG_VIDEO);
    assert_eq!(vseq.data[0], 0x17); // key frame, AVC
    assert_eq!(vseq.data[1], 0x00); // sequence header
    let avcc = &vseq.data[5..];
    assert_eq!(avcc[0], 0x01); // configurationVersion
    assert!(avcc.windows(sps().len()).any(|w| w == sps()), "AVCC missing SPS");
    assert!(avcc.windows(pps().len()).any(|w| w == pps()), "AVCC missing PPS");

    // audio sequence header carries the expected AudioSpecificConfig
    let aseq = &tags[3];
    assert_eq!(aseq.kind, TAG_AUDIO);
    assert_eq!(aseq.data[0], 0xAF); // AAC, 44.1k, stereo
    assert_eq!(aseq.data[1], 0x00); // sequence header
    assert_eq!(&aseq.data[2..4], &ASC_AAC_LC_44K_STEREO);

    // first video frame: a keyframe at ts 0, payload as length-prefixed NALs
    let first_video = &tags[2];
    assert_eq!(first_video.kind, TAG_VIDEO);
    assert_eq!(first_video.ts, 0);
    assert_eq!(first_video.data[0], 0x17); // key frame
    assert_eq!(first_video.data[1], 0x01); // NAL unit
    assert_eq!(&first_video.data[5..9], &(sps().len() as u32).to_be_bytes());
    assert_eq!(&first_video.data[9..9 + sps().len()], sps());

    // non-key video frame carries the inter frame marker
    let last_video = tags
        .iter()
        .rev()
        .find(|t| t.kind == TAG_VIDEO && t.data.get(1) == Some(&1))
        .expect("a video frame");
    assert_eq!(last_video.data[0], 0x27); // inter frame, AVC

    // audio frames carry the raw AAC payload, ADTS header stripped
    let audio_tags = tags.iter().filter(|t| t.kind == TAG_AUDIO && t.data.get(1) == Some(&1));
    for t in audio_tags {
        assert_eq!(t.data[0], 0xAF);
        assert_eq!(&t.data[2..], &audio_frame);
    }
}

/// When the platform hands over codec configs explicitly, the muxer must use
/// them and not demand SPS/PPS/ADTS inside the first packets.
#[test]
fn engine_writes_provided_codec_configs() {
    let avcc = stream::codecs::h264::build_avcc(&sps(), &pps());
    let asc = ASC_AAC_LC_44K_STEREO.to_vec();
    let key = annex_b(&[&idr(&[0x21, 0x88, 0x84])]); // no SPS/PPS inside

    let config = EngineConfig {
        stream: StreamConfig {
            width: 640,
            height: 360,
            ..Default::default()
        },
        autodetect_codecs: false,
        ..Default::default()
    };

    let engine: Engine<FileTransport<Vec<u8>>> = Engine::new(config);
    engine.attach_transport(FileTransport::new(Vec::new()));
    engine.configure_codecs(Some(&avcc), Some(&asc)).unwrap();

    let audio_frame = [0x21, 0x00, 0x49, 0x90]; // raw AAC, no ADTS wrapper
    engine
        .push_all([
            MediaPacket::video(0, true, key),
            MediaPacket::audio(0, audio_frame.to_vec()),
        ])
        .unwrap();
    engine.tick().unwrap();

    let bytes = engine
        .attach_transport(FileTransport::new(Vec::new()))
        .expect("previous transport with the FLV bytes")
        .into_inner();
    let tags = parse_flv(&bytes).expect("well-formed FLV");

    let vseq = tags
        .iter()
        .find(|t| t.kind == TAG_VIDEO && t.data.get(1) == Some(&0))
        .expect("video sequence header written");
    assert_eq!(&vseq.data[5..], avcc.as_slice());

    let aseq = tags
        .iter()
        .find(|t| t.kind == TAG_AUDIO && t.data.get(1) == Some(&0))
        .expect("audio sequence header written");
    assert_eq!(&aseq.data[2..4], &ASC_AAC_LC_44K_STEREO);
}

/// The engine must actually emit `QoS` samples through the real path — not
/// just the collector computing summaries on injected samples. With a
/// zero-interval config every `tick` produces a sample, so a quick loop can
/// fill a full hour-equivalent window (3600 samples, 1/s for 1h) into the ring
/// and verify it is retained in order and folded into a complete, queryable
/// summary.
#[test]
fn engine_emits_qos_samples_into_queryable_summary() {
    let config = EngineConfig {
        qos: QosConfig {
            interval: Duration::ZERO,
            capacity: 4096,
        },
        ..Default::default()
    };
    let engine: Engine<FileTransport<Vec<u8>>> = Engine::new(config);
    engine.attach_transport(FileTransport::new(Vec::new()));

    // A couple of seconds of 30fps video + audio: enough media that the rate
    // samples carry non-zero byte counts.
    let key = annex_b(&[&sps(), &pps(), &idr(&[0x21, 0x88, 0x84])]);
    let audio = adts(&[0x21, 0x00, 0x49, 0x90, 0x01, 0x02]);
    let mut packets = Vec::new();
    let mut pts = 0;
    for _ in 0..30 {
        packets.push(MediaPacket::video(pts, pts == 0, key.clone()));
        packets.push(MediaPacket::audio(pts, audio.clone()));
        pts += 33;
    }
    engine.push_all(packets).unwrap();

    // First tick drains and muxes the media; the rest just fill the ring.
    for _ in 0..3600 {
        engine.tick().unwrap();
    }

    let qos = engine.qos();
    assert_eq!(qos.sample_count(), 3600, "ring must retain the full window");
    let samples = qos.samples();
    assert_eq!(samples.len(), 3600);
    for pair in samples.windows(2) {
        assert!(pair[0].wall_ms <= pair[1].wall_ms, "samples not chronological");
        assert!(pair[0].uptime_ms <= pair[1].uptime_ms, "uptime went backwards");
    }

    let s = qos.summary();
    assert_eq!(s.samples, 3600);
    assert!(s.avg_bitrate_out_bps > 0.0, "media bitrate missing from summary");
    assert!(s.avg_throughput_bps > 0.0, "wire throughput missing from summary");
    assert!(s.peak_bitrate_out_bps >= s.avg_bitrate_out_bps);
    assert_eq!(s.reconnects, 0);
    assert_eq!(s.reconnect_attempts, 0);
    assert!(s.muxed <= s.pushed && s.dropped <= s.pushed);

    // Engine stats and the collector must agree on the same counters.
    let stats = engine.stats();
    assert_eq!(stats.pushed, s.pushed);
    assert_eq!(stats.muxed, s.muxed);
    assert_eq!(stats.dropped, s.dropped);
}

/// The ring only reflects elapsed stream time via the spacing between samples;
/// with a non-zero interval the summary must span the real time that passed
/// between the first and last sample.
#[test]
fn qos_summary_spans_real_elapsed_time() {
    let config = EngineConfig {
        qos: QosConfig {
            interval: Duration::from_millis(1),
            capacity: 16,
        },
        ..Default::default()
    };
    let engine: Engine<FileTransport<Vec<u8>>> = Engine::new(config);
    engine.attach_transport(FileTransport::new(Vec::new()));

    for _ in 0..6 {
        std::thread::sleep(Duration::from_millis(2));
        engine.tick().unwrap();
    }
    let s = engine.qos().summary();
    assert!(
        s.samples >= 3,
        "expected most ticks to emit a sample, got {}",
        s.samples
    );
    assert!(s.span_secs > 0.0, "summary must span the real time between samples");
}
