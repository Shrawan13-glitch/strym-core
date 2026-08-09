//! Core domain data types. The platform side hands us these; the core turns them
//! into a live stream. Everything here is transport-agnostic and codec-opaque
//! (the core never inspects pixel/audio content, only headers it needs to mux).

/// Whether a packet carries compressed (already-encoded) video or audio.
///
/// The core deliberately works on *encoded* bytes, not raw pixels/samples:
/// feeding raw YUV/PCM across the FFI boundary would be a realtime bottleneck
/// and double-buffering nightmare. The platform encodes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MediaKind {
    /// Compressed video frame.
    Video,
    /// Compressed audio frame.
    Audio,
}

/// One unit of compressed media, ready for muxing.
///
/// Timestamps are in **milliseconds**, relative to a single shared clock
/// source (`clock::Clock`). Both `pts` and `dts` are kept so the muxer can
/// pack the composition-time offset used by players.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaPacket {
    /// Whether this packet carries video or audio.
    pub kind: MediaKind,
    /// Presentation timestamp: when the viewer should *see* this.
    pub pts: i64,
    /// Decode timestamp: when the decoder must *process* this. Differs from
    /// `pts` whenever frames are reordered (B-frames). Often == `pts` in live.
    pub dts: i64,
    /// True when this is a keyframe (I-frame) / IDR. Used to make dry-cut
    /// decisions on under load and by players to seek/sync.
    pub is_key: bool,
    /// Compressed payload. For video this is an **Annex-B** packet (start-code
    /// separated NALs, what Android `MediaCodec` emits) — the core converts to
    /// FLV's length-prefixed form at mux time. For audio it's a raw AAC-LC
    /// frame (ADTS header stripped).
    pub data: Vec<u8>,
}

impl MediaPacket {
    /// Build a video packet; `dts` defaults to `pts` (no B-frame reorder).
    pub fn video(pts: i64, is_key: bool, data: Vec<u8>) -> Self {
        Self {
            kind: MediaKind::Video,
            pts,
            dts: pts,
            is_key,
            data,
        }
    }

    /// Build an audio packet; `dts` defaults to `pts`.
    pub fn audio(pts: i64, data: Vec<u8>) -> Self {
        Self {
            kind: MediaKind::Audio,
            pts,
            dts: pts,
            is_key: false,
            data,
        }
    }
}

/// Static values the muxer writes into container headers / metadata.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Encoded video width in pixels (metadata only).
    pub width: u32,
    /// Encoded video height in pixels (metadata only).
    pub height: u32,
    /// Nominal frames per second (metadata only).
    pub framerate: f64,
    /// video bitrate in bits/s (informational, metadata only)
    pub video_bitrate: u32,
    /// audio bitrate in bits/s (informational, metadata only)
    pub audio_bitrate: u32,
    /// nominal audio sampling rate recorded in metadata
    pub sample_rate: u32,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            width: 0,
            height: 0,
            framerate: 30.0,
            video_bitrate: 2_500_000,
            audio_bitrate: 128_000,
            sample_rate: 44_100,
        }
    }
}

/// Summary of how the stream is behaving, surfaced to the platform so it can
/// react to a weak network (e.g., lower bitrate). A cheaper, counter-level view
/// than the full [`crate::telemetry::QosSample`]s retained by the collector.
#[derive(Debug, Clone, Default)]
pub struct PacketStats {
    /// Packets accepted from the platform.
    pub pushed: u64,
    /// Packets successfully muxed to the transport.
    pub muxed: u64,
    /// Packets evicted/dropped to stay near the live edge.
    pub dropped: u64,
    /// Packets currently sitting in the buffer.
    pub in_buffered_count: usize,
    /// current buffer lag in ms (largest pts not yet muxed minus exposure)
    pub buffer_ms: i64,
    /// Bytes written into the container (FLV), header + tags included.
    pub media_bytes: u64,
    /// Bytes the transport accepted for the wire, framing included.
    pub wire_bytes: u64,
    /// Latest measured round-trip time to the peer, when the transport reports it.
    pub rtt_ms: Option<f64>,
    /// Successful reconnects since the engine was created.
    pub reconnects: u64,
    /// Failed reconnect attempts since the engine was created.
    pub reconnect_attempts: u64,
    /// Milliseconds since the engine (and its clock) was created.
    pub uptime_ms: u64,
}

impl PacketStats {
    /// Fraction of pushed packets that were dropped (0.0..=1.0).
    pub fn drop_ratio(&self) -> f64 {
        if self.pushed == 0 {
            0.0
        } else {
            self.dropped as f64 / self.pushed as f64
        }
    }
}
