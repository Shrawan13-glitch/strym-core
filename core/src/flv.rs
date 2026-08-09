//! FLV tag-body decoding — the *demux* direction of the pipeline.
//!
//! Everything else in this crate muxes packets *into* FLV; this module parses
//! FLV tag bodies back into [`MediaPacket`]s and codec configs. Two consumers
//! need exactly that:
//!
//! - the **RTMP ingest server**, whose audio/video messages carry FLV tag
//!   bodies verbatim (this is the "server-side of the product" path);
//! - tests that round-trip reference streams produced by `ffmpeg`.
//!
//! All parsing is bounds-checked: malformed or hostile input yields `None`,
//! never a panic.

use crate::codecs::h264;
use crate::models::{MediaKind, MediaPacket};

/// FLV tag types (shared with the mux direction).
pub const TAG_AUDIO: u8 = 8;
/// FLV video tag type.
pub const TAG_VIDEO: u8 = 9;
/// Script data (`onMetaData`); carried through but not decoded here.
pub const TAG_SCRIPT: u8 = 18;

// Video codec ids (low nibble of the first video byte).
const VIDEO_CODEC_AVC: u8 = 7;
// Audio formats (high nibble of the first audio byte).
const AUDIO_FORMAT_AAC: u8 = 10;

/// What one decoded FLV tag body produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decoded {
    /// An H.264 `AVCDecoderConfigurationRecord` (video sequence header).
    VideoConfig(Vec<u8>),
    /// An AAC `AudioSpecificConfig` (audio sequence header).
    AudioConfig(Vec<u8>),
    /// One media packet with millisecond timestamps.
    Packet(MediaPacket),
}

/// Decode one FLV tag body. `mtype` is the tag type (8 audio, 9 video), `ts`
/// the tag's millisecond timestamp. Returns `None` for unknown codecs,
/// unsupported packet types, and any truncation — ingest must never crash on
/// what a publisher sends.
pub fn decode_tag(mtype: u8, ts: u32, body: &[u8]) -> Option<Decoded> {
    match mtype {
        TAG_VIDEO => decode_video(ts, body),
        TAG_AUDIO => decode_audio(ts, body),
        _ => None,
    }
}

/// Decode a video tag body:
/// `[frameType|codecId][AVCPacketType][CompositionTime(3)][AVC payload]`.
fn decode_video(ts: u32, body: &[u8]) -> Option<Decoded> {
    if body.len() < 5 {
        return None;
    }
    let frame_type = body[0] >> 4;
    let codec = body[0] & 0x0F;
    if codec != VIDEO_CODEC_AVC {
        return None; // only H.264 is in scope
    }
    let packet_type = body[1];
    let payload = &body[5..];
    match packet_type {
        0 => {
            // Sequence header: the AVCDecoderConfigurationRecord itself.
            if payload.is_empty() {
                return None;
            }
            Some(Decoded::VideoConfig(payload.to_vec()))
        }
        1 => {
            // NAL units, length-prefixed. Composition offset is signed 24-bit.
            let cts_raw = (u32::from(body[2]) << 16) | (u32::from(body[3]) << 8) | u32::from(body[4]);
            let cts = (cts_raw << 8).cast_signed() >> 8;
            let nals = h264::from_length_prefixed(payload)?;
            if nals.is_empty() {
                return None;
            }
            let dts = i64::from(ts);
            Some(Decoded::Packet(MediaPacket {
                kind: MediaKind::Video,
                dts,
                pts: dts + i64::from(cts),
                is_key: frame_type == 1,
                data: h264::to_annex_b(&nals),
            }))
        }
        _ => None, // end-of-sequence and anything exotic: not media
    }
}

/// Decode an audio tag body: `[format|flags][AACPacketType][AAC payload]`.
fn decode_audio(ts: u32, body: &[u8]) -> Option<Decoded> {
    if body.len() < 2 {
        return None;
    }
    let format = body[0] >> 4;
    if format != AUDIO_FORMAT_AAC {
        return None; // only AAC-LC is in scope
    }
    let payload = &body[2..];
    match body[1] {
        0 => {
            if payload.is_empty() {
                return None;
            }
            Some(Decoded::AudioConfig(payload.to_vec()))
        }
        1 => Some(Decoded::Packet(MediaPacket {
            kind: MediaKind::Audio,
            dts: i64::from(ts),
            pts: i64::from(ts),
            is_key: false,
            data: payload.to_vec(),
        })),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_video_sequence_header() {
        let avcc = [0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1];
        let mut body = vec![0x17, 0x00, 0, 0, 0];
        body.extend_from_slice(&avcc);
        assert_eq!(
            decode_tag(TAG_VIDEO, 0, &body),
            Some(Decoded::VideoConfig(avcc.to_vec()))
        );
    }

    #[test]
    fn decodes_video_frame_with_cts() {
        let nal = [0x65, 0x88];
        let mut body = vec![0x17, 0x01, 0, 0, 40]; // cts = 40 ms
        body.extend_from_slice(&(nal.len() as u32).to_be_bytes());
        body.extend_from_slice(&nal);
        let Some(Decoded::Packet(pkt)) = decode_tag(TAG_VIDEO, 100, &body) else {
            panic!("expected a packet")
        };
        assert_eq!(pkt.dts, 100);
        assert_eq!(pkt.pts, 140);
        assert!(pkt.is_key);
        assert_eq!(pkt.data, vec![0, 0, 0, 1, 0x65, 0x88]);
    }

    #[test]
    fn negative_cts_sign_extends() {
        let mut body = vec![0x27, 0x01, 0xFF, 0xFF, 0xF6]; // cts = -10
        body.extend_from_slice(&1u32.to_be_bytes());
        body.push(0x41);
        let Some(Decoded::Packet(pkt)) = decode_tag(TAG_VIDEO, 50, &body) else {
            panic!("expected a packet")
        };
        assert_eq!(pkt.pts, 40);
        assert!(!pkt.is_key);
    }

    #[test]
    fn decodes_audio_config_and_frames() {
        let body = [0xAF, 0x00, 0x0A, 0x10];
        assert_eq!(
            decode_tag(TAG_AUDIO, 0, &body),
            Some(Decoded::AudioConfig(vec![0x0A, 0x10]))
        );
        let frame = [0xAF, 0x01, 0x21, 0x00];
        let Some(Decoded::Packet(pkt)) = decode_tag(TAG_AUDIO, 23, &frame) else {
            panic!("expected a packet")
        };
        assert_eq!(pkt.kind, MediaKind::Audio);
        assert_eq!(pkt.dts, 23);
        assert_eq!(pkt.data, vec![0x21, 0x00]);
    }

    #[test]
    fn rejects_unknown_codecs_and_truncation() {
        // Screen video (codec 3), not AVC.
        assert_eq!(decode_tag(TAG_VIDEO, 0, &[0x13, 0x01, 0, 0, 0, 1]), None);
        // MP3 audio (format 2), not AAC.
        assert_eq!(decode_tag(TAG_AUDIO, 0, &[0x2F, 0x01, 0]), None);
        // Truncated bodies at every length: never a panic.
        let mut body = vec![0x17, 0x01, 0, 0, 0];
        body.extend_from_slice(&3u32.to_be_bytes());
        body.extend_from_slice(&[0x65, 0x88, 0x84]);
        for cut in 0..=body.len() {
            let _ = decode_tag(TAG_VIDEO, 0, &body[..cut]);
        }
        // Script tags are not decoded here.
        assert_eq!(decode_tag(TAG_SCRIPT, 0, &[0x02]), None);
    }

    #[test]
    fn malformed_nal_lengths_rejected() {
        // Length field overruns the body.
        let mut body = vec![0x17, 0x01, 0, 0, 0];
        body.extend_from_slice(&99u32.to_be_bytes());
        body.extend_from_slice(&[0x65]);
        assert_eq!(decode_tag(TAG_VIDEO, 0, &body), None);
    }
}
