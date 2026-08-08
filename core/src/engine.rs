//! The engine — the single entry point the platform calls into.
//!
//! It owns the pipeline: incoming packets land in a bounded buffer (backpressure
//! valve), then are drained, time-normalized, and muxed into FLV bytes handed to
//! a `Transport`.
//!
//! ```text
//! push(packet)  ->  BoundedBuffer  ->  FlvMuxer  ->  Transport (file/RTMP/...)
//! ```
//!
//! Threading: `push`/`push_all` may be called from a capture thread while
//! `tick()` drains from another; all shared state is behind a `Mutex`, so the
//! engine is safe to use from multiple threads. The platform decides when to
//! call `tick()` (its own packet pump).

use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::backpressure::BoundedBuffer;
use crate::clock::Clock;
use crate::models::{MediaKind, MediaPacket, PacketStats, StreamConfig};
use crate::mux::{FlvMuxer, MuxError};
use crate::transport::Transport;

/// How aggressive the engine is about staying near the live edge when the
/// transport is slower than the encoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyProfile {
    /// Drop early and often: minimal lag, but may drop frames on weak networks.
    Aggressive,
    /// Default: moderate buffer cushions brief network blips.
    Balanced,
    /// Keep as much as possible: higher latency, fewer drops.
    Lenient,
}

impl LatencyProfile {
    fn packet_budget(self) -> usize {
        match self {
            LatencyProfile::Aggressive => 30,
            LatencyProfile::Balanced => 120,
            LatencyProfile::Lenient => 480,
        }
    }
}

/// Engine configuration.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Static stream parameters (resolution, framerate, bitrates) written into
    /// container headers and metadata.
    pub stream: StreamConfig,
    /// How aggressively the engine drops packets to stay near the live edge.
    pub profile: LatencyProfile,
    /// Auto-derive codec configs from the first packets (recommended when the
    /// platform can't hand over SPS/PPS/ASC directly).
    pub autodetect_codecs: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            stream: StreamConfig::default(),
            profile: LatencyProfile::Balanced,
            autodetect_codecs: true,
        }
    }
}

/// Errors surfaced by the engine.
#[derive(Debug)]
pub enum EngineError {
    /// FLV muxing failed (missing codec config, ordering violation, or an I/O
    /// failure reported by the transport).
    Mux(MuxError),
    /// The transport failed outside of muxing (e.g. shutdown).
    Transport(std::io::Error),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Mux(m) => write!(f, "mux: {m}"),
            EngineError::Transport(e) => write!(f, "transport: {e}"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<MuxError> for EngineError {
    fn from(m: MuxError) -> Self {
        EngineError::Mux(m)
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        EngineError::Transport(e)
    }
}

/// Pipeline state, shared via `Arc<Mutex<_>>`.
struct Inner<W: Write> {
    config: EngineConfig,
    clock: Clock,
    buffer: BoundedBuffer<MediaPacket>,
    muxer: Option<FlvMuxer<W>>,
    metadata_written: bool,
    muxed: u64,
}

/// Public handle. `W` is the transport sink (`Transport`), which also feeds the
/// muxer. Cloning the engine gives a shared handle (sendable to threads).
#[derive(Clone)]
pub struct Engine<W: Transport> {
    inner: Arc<Mutex<Inner<W>>>,
}

impl<W: Transport> Engine<W> {
    /// Create an engine with no transport attached yet.
    pub fn new(config: EngineConfig) -> Self {
        let budget = config.profile.packet_budget();
        Self {
            inner: Arc::new(Mutex::new(Inner {
                config,
                clock: Clock::new(),
                buffer: BoundedBuffer::new(budget),
                muxer: None,
                metadata_written: false,
                muxed: 0,
            })),
        }
    }

    /// Attach (or replace) the destination. Reuses the engine for reconnects.
    /// Returns the previous transport, if any.
    pub fn attach_transport(&self, transport: W) -> Option<W> {
        let mut inner = self.inner.lock().unwrap();
        let old = inner.muxer.take().map(super::mux::FlvMuxer::into_inner);
        inner.muxer = Some(FlvMuxer::new(transport));
        inner.metadata_written = false;
        old
    }

    /// Send a packet into the pipeline. Safe from any thread.
    pub fn push(&self, pkt: MediaPacket) -> Result<(), EngineError> {
        let mut inner = self.inner.lock().unwrap();
        inner.buffer.push(pkt);
        Ok(())
    }

    /// Push a batch — avoids per-packet lock churn.
    pub fn push_all(&self, pkts: impl IntoIterator<Item = MediaPacket>) -> Result<(), EngineError> {
        let mut inner = self.inner.lock().unwrap();
        for p in pkts {
            inner.buffer.push(p);
        }
        Ok(())
    }

    /// Emit codec sequence headers up front. The platform usually knows these
    /// (SPS/PPS from the encoder's `MediaFormat`, the AAC `AudioSpecificConfig`),
    /// which is both more reliable and cheaper than sniffing the first packet.
    /// Call after `attach_transport`, before the first `tick`.
    pub fn configure_codecs(&self, avcc: Option<&[u8]>, asc: Option<&[u8]>) -> Result<(), EngineError> {
        let mut inner = self.inner.lock().unwrap();
        let Some(muxer) = &mut inner.muxer else {
            return Ok(());
        };
        muxer.init_codecs(avcc, asc)?;
        Ok(())
    }

    /// Drain as much as is buffered, muxing each packet into the transport.
    /// Returns how many packets were written this call.
    pub fn tick(&self) -> Result<usize, EngineError> {
        let mut inner = self.inner.lock().unwrap();
        // No transport yet: leave the buffer untouched so a later `attach_transport`
        // doesn't find it already drained.
        if inner.muxer.is_none() {
            return Ok(0);
        }
        // Drain the buffer first; collect so we never hold two borrows at once.
        let mut batch = Vec::new();
        while let Some(pkt) = inner.buffer.pop_oldest() {
            batch.push(pkt);
        }
        let need_metadata = !inner.metadata_written;
        let stream = inner.config.stream.clone();
        let Some(muxer) = &mut inner.muxer else {
            return Ok(0);
        };
        if need_metadata {
            muxer.write_metadata(&stream)?;
        }
        for pkt in &batch {
            muxer.write_packet(pkt)?;
        }
        muxer.flush_sink()?;
        if need_metadata {
            inner.metadata_written = true;
        }
        inner.muxed += batch.len() as u64;
        Ok(batch.len())
    }

    /// Drain everything and end the stream cleanly (user pressed "end").
    /// Any packets still buffered are muxed before the transport shuts down.
    pub fn finish(&self) -> Result<(), EngineError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.muxer.is_none() {
            return Ok(());
        }
        let mut batch = Vec::new();
        while let Some(pkt) = inner.buffer.pop_oldest() {
            batch.push(pkt);
        }
        let muxed_now;
        {
            let muxer = inner.muxer.as_mut().unwrap();
            for pkt in &batch {
                muxer.write_packet(pkt)?;
            }
            muxer.finish()?;
            muxed_now = batch.len() as u64;
        }
        inner.muxed += muxed_now;
        if let Some(t) = inner.muxer.take() {
            t.into_inner().shutdown()?;
        }
        Ok(())
    }

    /// Telemetry for the platform's UI.
    pub fn stats(&self) -> PacketStats {
        let inner = self.inner.lock().unwrap();
        let buffered_ms = inner.buffer.last().map_or(0, |p| inner.clock.latency_ms(p.pts));
        let pushed = inner.buffer.pushed();
        let dropped = inner.buffer.dropped();
        PacketStats {
            pushed,
            muxed: inner.muxed,
            dropped,
            in_buffered_count: inner.buffer.len(),
            buffer_ms: buffered_ms,
        }
    }

    /// When badly behind, discard everything before the newest keyframe so the
    /// next `tick` resumes from a clean decode point. Returns how many packets
    /// were discarded.
    pub fn resync(&self) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        inner
            .buffer
            .resync_keep_latest_keyframe(|p| p.kind == MediaKind::Video && p.is_key)
    }

    /// Reset to a fresh state (new session / stream key).
    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        let budget = inner.config.profile.packet_budget();
        inner.buffer = BoundedBuffer::new(budget);
        inner.metadata_written = false;
        inner.muxed = 0;
        inner.muxer = None;
    }
}
