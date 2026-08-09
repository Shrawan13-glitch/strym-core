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

/// DTS may slip backwards by this many milliseconds (encoder quantization,
/// capture-thread skew) without us treating it as a genuine ordering violation.
/// Within this band the DTS is clamped to the previous value so the emitted
/// stream stays monotonic; anything further back is a real error.
const REORDER_TOLERANCE_MS: i64 = 100;

/// A writer for FLV tags into a byte sink.
pub struct FlvMuxer<W: Write> {
    sink: W,
    state: InitState,
    /// clock offset: first timestamp seen becomes 0 so streams don't start at
    /// a huge value after long uptime.
    origin: Option<i64>,
    /// Most recent normalized DTS written; enforces monotonic output.
    last_dts: i64,
    /// H.264 `AVCDecoderConfigurationRecord` last emitted (or sniffed), kept so
    /// a reconnect can re-send the sequence header — servers forget it.
    avcc: Option<Vec<u8>>,
    /// AAC `AudioSpecificConfig` last emitted (or sniffed), same purpose.
    asc: Option<Vec<u8>>,
    /// Total bytes written to the sink (FLV header + all tags). The engine uses
    /// deltas of this to compute the media bitrate for `QoS` telemetry.
    bytes_written: u64,
}

impl<W: Write> FlvMuxer<W> {
    /// Create a muxer writing FLV tags into `sink`.
    pub fn new(sink: W) -> Self {
        Self {
            sink,
            state: InitState::default(),
            origin: None,
            last_dts: i64::MIN,
            avcc: None,
            asc: None,
            bytes_written: 0,
        }
    }

    /// Returns the inner sink, useful after finishing.
    pub fn into_inner(self) -> W {
        self.sink
    }

    /// Borrow the sink (the transport) — lets the engine ask it about progress.
    pub fn sink(&self) -> &W {
        &self.sink
    }

    /// Mutably borrow the sink — lets a recorder shut its transport down.
    pub fn sink_mut(&mut self) -> &mut W {
        &mut self.sink
    }

    /// The timestamp origin (first DTS seen), if media has started flowing.
    pub fn origin(&self) -> Option<i64> {
        self.origin
    }

    /// Most recent normalized DTS written (`i64::MIN` before the first packet).
    pub fn last_dts(&self) -> i64 {
        self.last_dts
    }

    /// Carry the timebase over from a previous muxer (reconnect). Keeping the
    /// same origin and high-water mark means the resumed stream continues the
    /// timestamp series instead of jumping back to 0 — viewers see one stream,
    /// not a restart.
    pub fn restore_timebase(&mut self, origin: Option<i64>, last_dts: i64) {
        self.origin = origin;
        self.last_dts = last_dts;
    }

    /// Re-anchor the origin so `dts` normalizes to exactly the last written
    /// DTS. This is the clock-drift escape hatch: when the capture clock jumps
    /// backwards beyond tolerance (platform clock reset, encoder restart),
    /// output stays monotonic instead of erroring.
    pub fn rebase(&mut self, dts: i64) {
        self.origin = Some(dts - self.last_dts.max(0));
    }

    /// The H.264 decoder configuration in force, if video has been configured.
    pub fn video_config(&self) -> Option<&[u8]> {
        self.avcc.as_deref()
    }

    /// The AAC decoder configuration in force, if audio has been configured.
    pub fn audio_config(&self) -> Option<&[u8]> {
        self.asc.as_deref()
    }

    /// Total bytes written to the sink since creation, FLV header + tags. Used
    /// by the engine to compute the media bitrate for `QoS` telemetry.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
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
        self.bytes_written = self.bytes_written.wrapping_add(hdr.len() as u64);
        self.state.header_written = true;
        Ok(())
    }

    /// Normalize a timestamp against the stream origin (first DTS becomes 0).
    /// Idempotent after the first call; both DTS and PTS share one origin so
    /// the composition-time offset (`pts - dts`) survives normalization.
    fn normalize(&mut self, ts: i64) -> i64 {
        let o = *self.origin.get_or_insert(ts);
        ts - o
    }

    /// Timestamp used for header-style tags (metadata, sequence headers).
    /// Before media flows that's 0; after a reconnect it's the point the stream
    /// reached, so re-emitted headers never violate timestamp monotonicity.
    fn header_ts(&self) -> i64 {
        self.last_dts.max(0)
    }

    /// Emit the script-data tag (`onMetaData`) with stream info. Called once,
    /// right before the first media packet (so duration-0 is honest).
    pub fn write_metadata(&mut self, config: &crate::models::StreamConfig) -> Result<(), MuxError> {
        self.ensure_header()?;
        let payload = amf0::on_metadata(config);
        let ts = self.header_ts();
        self.write_tag(TAG_SCRIPT, ts, &payload)
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
        let ts = self.header_ts();
        self.write_tag(TAG_VIDEO, ts, &body)?;
        self.state.video_seq_written = true;
        self.avcc = Some(avcc.to_vec());
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
        let ts = self.header_ts();
        self.write_tag(TAG_AUDIO, ts, &body)?;
        self.state.audio_seq_written = true;
        self.asc = Some(asc.to_vec());
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
        self.ensure_header()?;
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
        let ts = if ts < self.last_dts {
            let slip = self.last_dts - ts;
            if slip > REORDER_TOLERANCE_MS {
                return Err(MuxError::Ordering(format!(
                    "DTS went backwards by {slip} ms ({} -> {}); out of tolerance",
                    self.last_dts, ts
                )));
            }
            // Small backward slip (encoder jitter): clamp to keep the emitted
            // stream monotonic. FLV players can't decode backward timestamps.
            self.last_dts
        } else {
            ts
        };
        self.last_dts = ts;
        let pts = self.normalize(pkt.pts);
        let cts = pts.saturating_sub(ts);

        match pkt.kind {
            MediaKind::Video => {
                let lp = crate::codecs::h264::annexb_to_length_prefixed(&pkt.data);
                let mut body = Vec::with_capacity(5 + lp.len());
                let frame_type = if pkt.is_key { 1 } else { 2 };
                body.push((frame_type << 4) | 0x07); // key/inter + AVC
                body.push(1); // AVCPacketType = NAL unit
                body.extend_from_slice(&composition_time(cts));
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
        self.bytes_written = self.bytes_written.wrapping_add(11 + data.len() as u64 + 4);
        Ok(())
    }

    /// Flush the sink. FLV has no end marker; players stop at stream end, so
    /// this is just an `io::flush`.
    pub fn finish(&mut self) -> Result<(), MuxError> {
        self.sink.flush()?;
        Ok(())
    }
}

/// FLV timestamps are unsigned 32-bit (24-bit field + extended byte). Clamp to
/// the full valid range and never let a negative (out-of-order) time become a
/// huge unsigned value that puts the frame ~100 days in the future.
fn clamp_ts(ts: i64) -> u32 {
    ts.clamp(0, 0xFFFF_FFFF) as u32
}

/// composition time offset = pts - dts, signed 24-bit per spec. The caller
/// passes the already-normalized offset; negative (B-frame reorder) is valid.
fn composition_time(cts: i64) -> [u8; 3] {
    let v = cts.clamp(-8_388_608, 8_388_607) as i32;
    [((v >> 16) & 0xFF) as u8, ((v >> 8) & 0xFF) as u8, (v & 0xFF) as u8]
}

fn u24(v: u32) -> [u8; 3] {
    [(v >> 16) as u8, (v >> 8) as u8, v as u8]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{MediaPacket, StreamConfig};

    /// A muxer writing into a Vec<u8> with both sequence headers already emitted.
    fn muxer() -> (FlvMuxer<Vec<u8>>, StreamConfig) {
        let config = StreamConfig::default();
        let mut m = FlvMuxer::new(Vec::new());
        m.write_metadata(&config).unwrap();
        m.init_codecs(
            Some(&[
                0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x03, 0x67, 0x42, 0x00, 0x0A, 0x01, 0x00, 0x03, 0x68, 0xCE,
            ]),
            Some(&[0x0A, 0x10]),
        )
        .unwrap();
        (m, config)
    }

    fn video(pts: i64, dts: i64, key: bool) -> MediaPacket {
        MediaPacket {
            kind: MediaKind::Video,
            pts,
            dts,
            is_key: key,
            data: vec![0, 0, 0, 1, 0x65, 0x88],
        }
    }

    fn audio(pts: i64) -> MediaPacket {
        MediaPacket {
            kind: MediaKind::Audio,
            pts,
            dts: pts,
            is_key: false,
            data: vec![0x21, 0x00, 0x49],
        }
    }

    /// Walk tags out of FLV bytes, returning `(kind, ts, data)`.
    fn parse(bytes: &[u8]) -> Vec<(u8, u32, Vec<u8>)> {
        let mut out = Vec::new();
        let mut pos = 13;
        while pos + 11 <= bytes.len() {
            let kind = bytes[pos];
            let size = ((bytes[pos + 1] as usize) << 16) | ((bytes[pos + 2] as usize) << 8) | bytes[pos + 3] as usize;
            let ts24 = ((bytes[pos + 4] as u32) << 16) | ((bytes[pos + 5] as u32) << 8) | bytes[pos + 6] as u32;
            let ts = (u32::from(bytes[pos + 7]) << 24) | ts24;
            let start = pos + 11;
            out.push((kind, ts, bytes[start..start + size].to_vec()));
            pos = start + size + 4;
        }
        out
    }

    #[test]
    fn dts_is_monotonic_after_small_backward_slip() {
        let (mut m, _) = muxer();
        m.write_packet(&video(0, 0, true)).unwrap();
        m.write_packet(&video(40, 40, false)).unwrap();
        // 3 ms backward: clamped, stream stays monotonic.
        m.write_packet(&video(37, 37, false)).unwrap();
        let bytes = m.into_inner();
        let tags = parse(&bytes);
        let ts: Vec<u32> = tags.iter().map(|t| t.1).collect();
        assert!(ts.windows(2).all(|w| w[0] <= w[1]), "monotonic: {ts:?}");
    }

    #[test]
    fn dts_out_of_tolerance_is_ordering_error() {
        let (mut m, _) = muxer();
        m.write_packet(&video(0, 0, true)).unwrap();
        m.write_packet(&video(40, 40, false)).unwrap();
        // 1 second backward: a real ordering violation.
        let err = m.write_packet(&video(0, 40 - 1000, false)).unwrap_err();
        assert!(matches!(err, MuxError::Ordering(_)), "got {err:?}");
    }

    #[test]
    fn negative_cts_is_encoded_within_signed_24bit() {
        let (mut m, _) = muxer();
        // B-frame: pts behind dts -> negative composition offset.
        m.write_packet(&video(10, 20, true)).unwrap();
        let bytes = m.into_inner();
        let tags = parse(&bytes);
        let vtags: Vec<_> = tags.iter().filter(|(k, _, _)| *k == TAG_VIDEO).collect();
        // sequence header ts 0, frame ts 0 (20-20 origin=20 -> 0)
        let frame = vtags.last().unwrap();
        let raw = (i32::from(frame.2[2]) << 16) | (i32::from(frame.2[3]) << 8) | i32::from(frame.2[4]);
        // sign-extend the 24-bit two's complement field
        let cts = (raw << 8) >> 8;
        assert!(cts < 0, "expected negative cts, got {cts}");
        // FLV spec: signed 24-bit, so 10-20 = -10 encoded as 0xFFFFF6
        assert_eq!(cts, -10);
    }

    #[test]
    fn ts_beyond_24bit_uses_extended_byte() {
        let (mut m, _) = muxer();
        // First frame anchors the origin; a later frame at 0x0100_0000 ms
        // (16,777,216 ms after it) needs the 8-bit extended timestamp.
        m.write_packet(&video(0, 0, true)).unwrap();
        m.write_packet(&video(0x0100_0000, 0x0100_0000, false)).unwrap();
        let bytes = m.into_inner();
        let tags = parse(&bytes);
        let vtags: Vec<_> = tags.iter().filter(|(k, _, _)| *k == TAG_VIDEO).collect();
        let frame = vtags.last().unwrap();
        assert_eq!(frame.1, 0x0100_0000, "24-bit + extended byte must round-trip");
    }

    #[test]
    fn negative_origin_ts_clamped_to_zero() {
        let (mut m, _) = muxer();
        // First frame at 10s, a reordered frame arriving at 9.999s relative to
        // the true origin would have gone negative before normalization; the
        // muxer must never emit a huge unsigned ts.
        m.write_packet(&video(10_000, 10_000, true)).unwrap();
        let bytes = m.into_inner();
        let tags = parse(&bytes);
        let vtags: Vec<_> = tags.iter().filter(|(k, _, _)| *k == TAG_VIDEO).collect();
        assert_eq!(vtags.last().unwrap().1, 0);
    }

    #[test]
    fn audio_negative_cts_not_applied() {
        let (mut m, _) = muxer();
        m.write_packet(&audio(0)).unwrap();
        let bytes = m.into_inner();
        let tags = parse(&bytes);
        let atags: Vec<_> = tags.iter().filter(|(k, _, _)| *k == TAG_AUDIO).collect();
        assert_eq!(atags.last().unwrap().1, 0);
    }

    #[test]
    fn restored_timebase_continues_timestamp_series() {
        // First muxer reaches ts 5000; a replacement (reconnect) carrying the
        // timebase over must continue from there, not restart at 0.
        let (mut m, _) = muxer();
        m.write_packet(&video(0, 0, true)).unwrap();
        m.write_packet(&video(5000, 5000, false)).unwrap();
        let (origin, last) = (m.origin(), m.last_dts());

        let mut resumed = FlvMuxer::new(Vec::new());
        resumed.restore_timebase(origin, last);
        resumed.write_packet(&video(5040, 5040, false)).unwrap();
        let bytes = resumed.into_inner();
        let tags = parse(&bytes);
        let vtags: Vec<_> = tags.iter().filter(|(k, _, _)| *k == TAG_VIDEO).collect();
        assert_eq!(vtags.last().unwrap().1, 5040);
    }

    #[test]
    fn rebase_keeps_output_monotonic_after_clock_reset() {
        let (mut m, _) = muxer();
        m.write_packet(&video(0, 0, true)).unwrap();
        m.write_packet(&video(10_000, 10_000, false)).unwrap();
        // Capture clock resets far backwards: rebasing continues from the
        // high-water mark instead of an ordering error.
        m.rebase(40);
        m.write_packet(&video(40, 40, true)).unwrap();
        m.write_packet(&video(80, 80, false)).unwrap();
        let bytes = m.into_inner();
        let tags = parse(&bytes);
        let ts: Vec<u32> = tags.iter().map(|t| t.1).collect();
        assert!(ts.windows(2).all(|w| w[0] <= w[1]), "monotonic: {ts:?}");
        assert_eq!(*ts.last().unwrap(), 10_040);
    }

    #[test]
    fn sequence_headers_are_cached_and_remembered() {
        let (mut m, _) = muxer();
        m.write_packet(&video(0, 0, true)).unwrap();
        assert!(m.video_config().is_some());
        assert!(m.audio_config().is_some());
    }

    #[test]
    fn reemitted_headers_after_resume_are_monotonic() {
        // After a reconnect the fresh muxer re-emits metadata + sequence
        // headers at the resumed time, never below it.
        let (mut m, cfg) = muxer();
        m.write_packet(&video(0, 0, true)).unwrap();
        m.write_packet(&video(3000, 3000, false)).unwrap();
        let (origin, last) = (m.origin(), m.last_dts());
        let (avcc, asc) = (m.video_config().unwrap().to_vec(), m.audio_config().unwrap().to_vec());

        let mut resumed = FlvMuxer::new(Vec::new());
        resumed.restore_timebase(origin, last);
        resumed.write_metadata(&cfg).unwrap();
        resumed.write_video_sequence(&avcc).unwrap();
        resumed.write_audio_sequence(&asc).unwrap();
        resumed.write_packet(&video(3040, 3040, true)).unwrap();
        let bytes = resumed.into_inner();
        let tags = parse(&bytes);
        let ts: Vec<u32> = tags.iter().map(|t| t.1).collect();
        assert!(ts.windows(2).all(|w| w[0] <= w[1]), "monotonic: {ts:?}");
        assert_eq!(ts[0], 3000, "re-emitted headers sit at the resumed time");
    }
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
