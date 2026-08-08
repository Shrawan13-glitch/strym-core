//! FLV muxer — packs the encoded A/V packets into the FLV container, the exact
//! format RTMP expects on the wire. Writes to any `std::io::Write`, so the same
//! muxer can target a file (tests) or a socket (RTMP transport).
//!
//! FLV structure: a 9-byte header, then a stream of **tags**. Each tag carries
//! one of: audio frame, video frame, or script data (`onMetaData`).

use std::io::{self, Write};

use crate::models::{MediaKind, MediaPacket};

// FLV tag types
const TAG_AUDIO: u8 = 8;
const TAG_VIDEO: u8 = 9;
const TAG_SCRIPT: u8 = 18;

// Header flags: audio (0x04) + video (0x01) present
const FLV_FLAGS: u8 = 0x05;

/// Errors the muxer can produce. Wrapping `io::Error` keeps failures on the
/// transport (file full, socket closed) distinct from format errors.
#[derive(Debug)]
pub enum MuxError {
    /// The underlying sink failed to accept bytes.
    Io(io::Error),
    /// metadata or codec config missing when we needed it
    Config(String),
    /// packet arrived out of order relative to muxing rules
    Ordering(String),
}

impl From<io::Error> for MuxError {
    fn from(e: io::Error) -> Self {
        MuxError::Io(e)
    }
}

impl std::fmt::Display for MuxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MuxError::Io(e) => write!(f, "io: {e}"),
            MuxError::Config(m) | MuxError::Ordering(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for MuxError {}

impl MuxError {
    fn config(msg: impl Into<String>) -> Self {
        MuxError::Config(msg.into())
    }
}

/// State machine so the muxer refuses to emit packets before the player would
/// have enough context to decode them.
#[derive(Debug, Default)]
struct InitState {
    header_written: bool,
    video_seq_written: bool,
    audio_seq_written: bool,
}

/// A writer for FLV tags into a byte sink.
pub struct FlvMuxer<W: Write> {
    sink: W,
    state: InitState,
    /// clock offset: first timestamp seen becomes 0 so streams don't start at
    /// a huge value after long uptime.
    origin: Option<i64>,
}

impl<W: Write> FlvMuxer<W> {
    /// Create a muxer writing FLV tags into `sink`.
    pub fn new(sink: W) -> Self {
        Self {
            sink,
            state: InitState::default(),
            origin: None,
        }
    }

    /// Returns the inner sink, useful after finishing.
    pub fn into_inner(self) -> W {
        self.sink
    }

    /// Flush the underlying sink (used between ticks so bytes leave promptly).
    pub fn flush_sink(&mut self) -> Result<(), MuxError> {
        self.sink.flush()?;
        Ok(())
    }

    /// Write the 9-byte FLV file/stream header. Must be called exactly once,
    /// before anything else. Done lazily on the first packet if you forget.
    fn ensure_header(&mut self) -> Result<(), MuxError> {
        if self.state.header_written {
            return Ok(());
        }
        let mut hdr = Vec::with_capacity(13);
        hdr.extend_from_slice(b"FLV");
        hdr.push(0x01); // version
        hdr.push(FLV_FLAGS);
        hdr.extend_from_slice(&[0, 0, 0, 9]); // header size
        hdr.extend_from_slice(&[0, 0, 0, 0]); // first PreviousTagSize0
        self.sink.write_all(&hdr)?;
        self.state.header_written = true;
        Ok(())
    }

    /// Normalize an incoming pts so the stream begins at time 0.
    fn normalize(&mut self, pts: i64) -> i64 {
        let o = *self.origin.get_or_insert(pts);
        pts - o
    }

    /// Emit the script-data tag (`onMetaData`) with stream info. Called once,
    /// right before the first media packet (so duration-0 is honest).
    pub fn write_metadata(&mut self, config: &crate::models::StreamConfig) -> Result<(), MuxError> {
        self.ensure_header()?;
        let payload = amf0::on_metadata(config);
        self.write_tag(TAG_SCRIPT, 0, &payload)
    }

    /// Emit the H.264 sequence header (`AVCDecoderConfigurationRecord`). Must
    /// precede the first video packet.
    pub fn write_video_sequence(&mut self, avcc: &[u8]) -> Result<(), MuxError> {
        self.ensure_header()?;
        if self.state.video_seq_written {
            return Ok(());
        }
        let mut body = Vec::with_capacity(5 + avcc.len());
        body.push(0x17); // frameType=1 (key) | codecId=7 (AVC)
        body.push(0); // AVCPacketType = sequence header
        body.extend_from_slice(&[0, 0, 0]); // composition time = 0
        body.extend_from_slice(avcc);
        self.write_tag(TAG_VIDEO, 0, &body)?;
        self.state.video_seq_written = true;
        Ok(())
    }

    /// Emit the AAC sequence header (`AudioSpecificConfig`). Must precede the
    /// first audio packet.
    pub fn write_audio_sequence(&mut self, asc: &[u8]) -> Result<(), MuxError> {
        self.ensure_header()?;
        if self.state.audio_seq_written {
            return Ok(());
        }
        let mut body = Vec::with_capacity(2 + asc.len());
        body.push(0xAF); // soundFormat=10 (AAC) | 44.1k | 16-bit | stereo
        body.push(0); // AACPacketType = sequence header
        body.extend_from_slice(asc);
        self.write_tag(TAG_AUDIO, 0, &body)?;
        self.state.audio_seq_written = true;
        Ok(())
    }

    /// Convenience: set both codec configs up-front if the caller already has
    /// them (common on the platform side, where they come from `MediaFormat`).
    pub fn init_codecs(&mut self, avcc: Option<&[u8]>, asc: Option<&[u8]>) -> Result<(), MuxError> {
        if let Some(a) = avcc {
            self.write_video_sequence(a)?;
        }
        if let Some(a) = asc {
            self.write_audio_sequence(a)?;
        }
        Ok(())
    }

    /// Mux one media packet. Ordering, normalization and sequence-header
    /// emission are handled here so callers stay dumb.
    pub fn write_packet(&mut self, pkt: &MediaPacket) -> Result<(), MuxError> {
        if !self.state.video_seq_written && pkt.kind == MediaKind::Video && pkt.is_key {
            // best-effort: pull SPS/PPS out of the packet itself
            let avcc = crate::codecs::h264::annexb_to_avcc(&pkt.data)
                .ok_or_else(|| MuxError::config("video packet before sequence header, and no SPS/PPS found"))?;
            self.write_video_sequence(&avcc)?;
        }
        if !self.state.audio_seq_written && pkt.kind == MediaKind::Audio {
            let asc = crate::codecs::aac::adts_to_asc(&pkt.data)
                .ok_or_else(|| MuxError::config("audio packet before sequence header, and no ADTS found"))?;
            self.write_audio_sequence(&asc)?;
        }

        let ts = self.normalize(pkt.dts);

        match pkt.kind {
            MediaKind::Video => {
                let lp = crate::codecs::h264::annexb_to_length_prefixed(&pkt.data);
                let mut body = Vec::with_capacity(5 + lp.len());
                let frame_type = if pkt.is_key { 1 } else { 2 };
                body.push((frame_type << 4) | 0x07); // key/inter + AVC
                body.push(1); // AVCPacketType = NAL unit
                body.extend_from_slice(&composition_time(pkt.dts, pkt.pts));
                body.extend_from_slice(&lp);
                self.write_tag(TAG_VIDEO, ts, &body)
            }
            MediaKind::Audio => {
                let raw = crate::codecs::aac::strip_adts(&pkt.data);
                let mut body = Vec::with_capacity(2 + raw.len());
                body.push(0xAF); // AAC, 44.1k, 16-bit, stereo
                body.push(1); // AACPacketType = raw AAC frame
                body.extend_from_slice(&raw);
                self.write_tag(TAG_AUDIO, ts, &body)
            }
        }
    }

    fn write_tag(&mut self, tag_type: u8, ts: i64, data: &[u8]) -> Result<(), MuxError> {
        let ts = clamp_ts(ts);
        let mut header = Vec::with_capacity(11);
        header.push(tag_type);
        header.extend_from_slice(&u24(data.len() as u32));
        header.extend_from_slice(&u24(ts));
        header.push((ts >> 24) as u8); // extended timestamp
        header.extend_from_slice(&[0, 0, 0]); // stream id = 0
        self.sink.write_all(&header)?;
        self.sink.write_all(data)?;
        // PreviousTagSize = tag header (11) + data
        let prev = 11u32 + data.len() as u32;
        self.sink.write_all(&prev.to_be_bytes())?;
        Ok(())
    }

    /// Flush the sink. FLV has no end marker; players stop at stream end, so
    /// this is just an `io::flush`.
    pub fn finish(&mut self) -> Result<(), MuxError> {
        self.sink.flush()?;
        Ok(())
    }
}

/// FLV timestamps are unsigned 24-bit + extended byte (32-bit total). Clamp to
/// a sane ceiling and never let a negative (out-of-order) time become a huge
/// unsigned value that puts the frame ~100 days in the future.
fn clamp_ts(ts: i64) -> u32 {
    ts.clamp(0, 0x0FFF_FFFF) as u32
}

/// composition time offset = pts - dts, signed 24-bit per spec.
fn composition_time(dts: i64, pts: i64) -> [u8; 3] {
    let v = (pts - dts).clamp(-8_388_608, 8_388_607) as i32;
    [((v >> 16) & 0xFF) as u8, ((v >> 8) & 0xFF) as u8, (v & 0xFF) as u8]
}

fn u24(v: u32) -> [u8; 3] {
    [(v >> 16) as u8, (v >> 8) as u8, v as u8]
}

/// Minimal AMF0 encoder — just enough for `onMetaData`.
mod amf0 {
    use crate::models::StreamConfig;

    const AMF0_NUMBER: u8 = 0x00;
    const AMF0_STRING: u8 = 0x02;
    const AMF0_ECMA_ARRAY: u8 = 0x08;
    const AMF0_OBJECT_END: [u8; 3] = [0x00, 0x00, 0x09];

    fn number(v: f64, out: &mut Vec<u8>) {
        out.push(AMF0_NUMBER);
        out.extend_from_slice(&v.to_be_bytes());
    }

    fn string(s: &str, out: &mut Vec<u8>) {
        out.push(AMF0_STRING);
        let b = s.as_bytes();
        out.extend_from_slice(&(b.len() as u16).to_be_bytes());
        out.extend_from_slice(b);
    }

    fn property(key: &str, out: &mut Vec<u8>) {
        let b = key.as_bytes();
        out.extend_from_slice(&(b.len() as u16).to_be_bytes());
        out.extend_from_slice(b);
    }

    /// Build the `onMetaData` ECMA-array body.
    pub fn on_metadata(c: &StreamConfig) -> Vec<u8> {
        let mut out = Vec::new();
        string("onMetaData", &mut out);
        out.push(AMF0_ECMA_ARRAY);
        out.extend_from_slice(&7u32.to_be_bytes()); // element count

        property("duration", &mut out);
        number(0.0, &mut out);
        property("width", &mut out);
        number(c.width as f64, &mut out);
        property("height", &mut out);
        number(c.height as f64, &mut out);
        property("framerate", &mut out);
        number(c.framerate, &mut out);
        property("videocodecid", &mut out);
        number(7.0, &mut out); // AVN
        property("audiocodecid", &mut out);
        number(10.0, &mut out); // AAC
        property("audiosamplerate", &mut out);
        number(c.sample_rate as f64, &mut out);

        out.extend_from_slice(&AMF0_OBJECT_END);
        out
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn metadata_roundtrip_shape() {
            let c = StreamConfig {
                width: 128,
                height: 96,
                ..Default::default()
            };
            let b = on_metadata(&c);
            assert_eq!(&b[0..1], &[AMF0_STRING]);
            assert_eq!(&b[1..3], &10u16.to_be_bytes()); // "onMetaData" length
            assert_eq!(&b[3..13], b"onMetaData");
            assert_eq!(&b[13..14], &[AMF0_ECMA_ARRAY]);
            assert!(b.ends_with(&AMF0_OBJECT_END));
        }
    }
}
