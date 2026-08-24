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
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crate::backpressure::BoundedBuffer;
use crate::clock::Clock;
use crate::models::{MediaKind, MediaPacket, PacketStats, StreamConfig};
use crate::mux::{FlvMuxer, MuxError};
use crate::sink::PacketSink;
use crate::telemetry::{wall_clock_ms, Level, Qos, QosConfig, QosSample};
use crate::transport::Transport;

/// How aggressive the engine is about staying near the live edge when the
/// transport is slower than the encoder.
///
/// `non_exhaustive`: new profiles may be added in a minor release; matches
/// must keep a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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

    /// Buffer fill level (in packets) at which the engine performs a
    /// keyframe-aligned cut back to the live edge. `Aggressive` cuts early to
    /// hug the live edge, `Balanced` only when the buffer is actually full,
    /// `Lenient` never cuts proactively (hard eviction still bounds memory).
    fn cut_threshold(self) -> Option<usize> {
        match self {
            LatencyProfile::Aggressive => Some(self.packet_budget() / 2),
            LatencyProfile::Balanced => Some(self.packet_budget()),
            LatencyProfile::Lenient => None,
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
    /// Telemetry knobs: how often `QoS` samples are emitted and how many the
    /// collector retains.
    pub qos: QosConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            stream: StreamConfig::default(),
            profile: LatencyProfile::Balanced,
            autodetect_codecs: true,
            qos: QosConfig::default(),
        }
    }
}

/// Errors surfaced by the engine.
///
/// `non_exhaustive`: new failure categories may be added in a minor release;
/// matches must keep a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
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

/// A gap between samples longer than this means the stream was idle (disconnect,
/// no capture); the baseline is reset instead of reporting a garbage-near-zero
/// rate over the whole gap.
const MAX_SAMPLE_GAP: Duration = Duration::from_secs(30);

/// Pipeline state, shared via `Arc<Mutex<_>>`.
struct Inner<W: Write> {
    config: EngineConfig,
    clock: Clock,
    buffer: BoundedBuffer<MediaPacket>,
    muxer: Option<FlvMuxer<W>>,
    metadata_written: bool,
    muxed: u64,
    /// Codec configs in force, cached so a reconnect can re-emit the sequence
    /// headers — the new server connection knows nothing about the old one.
    avcc: Option<Vec<u8>>,
    asc: Option<Vec<u8>>,
    /// Muxer timebase `(origin, last_video_dts, last_audio_dts)` snapshot taken
    /// when a transport dies, so the replacement muxer continues the timestamp
    /// series.
    timebase: Option<(Option<i64>, i64, i64)>,
    /// Set while the engine must not emit media. True when a transport died
    /// and the buffer held no keyframe to resume from: the fresh connection
    /// would otherwise start on an orphaned inter-frame a decoder cannot use.
    /// Cleared as soon as a video keyframe is drained.
    awaiting_keyframe: bool,
    /// Shared `QoS` collector; the platform reads it, the engine writes it.
    qos: Arc<Qos>,
    /// Sampling baselines: wall instant and byte counters at the last sample,
    /// used to derive bitrate/throughput deltas.
    last_sample_at: Instant,
    last_media_bytes: u64,
    last_wire_bytes: u64,
    /// Packet-level outputs (recording, HLS). They persist across transport
    /// reconnects: the publish connection may die and re-dial, but a recording
    /// or a playlist must keep going without interruption.
    outputs: Vec<Box<dyn PacketSink>>,
    /// Codec configs last pushed to `outputs`, so updates are detected without
    /// spamming every sink on every tick.
    output_codecs: (Option<Vec<u8>>, Option<Vec<u8>>),
}

/// Public handle. `W` is the transport sink (`Transport`), which also feeds the
/// muxer. Cloning the engine gives a shared handle (sendable to threads); the
/// clone happens on the `Arc`, so it never requires `W: Clone`.
pub struct Engine<W: Transport> {
    inner: Arc<Mutex<Inner<W>>>,
}

impl<W: Transport> Clone for Engine<W> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<W: Transport> Engine<W> {
    /// Lock the shared state. A poisoned mutex (a panic while someone held the
    /// lock) is recovered by continuing with the inner state rather than
    /// panicking the caller; the engine stays usable after a component bug.
    fn lock(&self) -> MutexGuard<'_, Inner<W>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Create an engine with no transport attached yet.
    pub fn new(config: EngineConfig) -> Self {
        let budget = config.profile.packet_budget();
        let qos = Arc::new(Qos::new(config.qos));
        let now = Instant::now();
        Self {
            inner: Arc::new(Mutex::new(Inner {
                config,
                clock: Clock::new(),
                buffer: BoundedBuffer::new(budget),
                muxer: None,
                metadata_written: false,
                muxed: 0,
                avcc: None,
                asc: None,
                timebase: None,
                awaiting_keyframe: false,
                qos,
                last_sample_at: now,
                last_media_bytes: 0,
                last_wire_bytes: 0,
                outputs: Vec::new(),
                output_codecs: (None, None),
            })),
        }
    }

    /// Attach a packet-level output (recording, HLS segmenter, ...). It
    /// receives every packet that reaches the wire plus codec-config updates,
    /// and — unlike the transport — survives reconnects untouched. Returns the
    /// number of outputs now attached.
    pub fn attach_output(&self, mut sink: Box<dyn PacketSink>) -> usize {
        let mut inner = self.lock();
        // Bring the sink up to date immediately when configs are already known.
        sink.codecs(inner.avcc.as_deref(), inner.asc.as_deref());
        inner.output_codecs = (inner.avcc.clone(), inner.asc.clone());
        inner.outputs.push(sink);
        crate::log_event!(Level::Info, "output attached", "outputs" => inner.outputs.len());
        inner.outputs.len()
    }

    /// Number of packet-level outputs currently attached.
    pub fn output_count(&self) -> usize {
        self.lock().outputs.len()
    }

    /// Attach (or replace) the destination. Reuses the engine for reconnects.
    /// Returns the previous transport, if any.
    ///
    /// On a reattach the muxer's timebase (origin + high-water DTS) carried
    /// over from [`detach_transport`](Self::detach_transport) is restored into
    /// the fresh muxer, so the resumed stream continues its timestamp series
    /// instead of jumping back to 0. The next `tick` re-emits the FLV header,
    /// metadata, and sequence headers that the new connection has never seen.
    pub fn attach_transport(&self, transport: W) -> Option<W> {
        let mut inner = self.lock();
        let old = inner.muxer.take().map(super::mux::FlvMuxer::into_inner);
        let mut muxer = FlvMuxer::new(transport);
        if let Some((origin, last_video, last_audio)) = inner.timebase.take() {
            muxer.restore_timebase(origin, last_video, last_audio);
        }
        inner.muxer = Some(muxer);
        inner.metadata_written = false;
        // A fresh muxer/transport starts counting bytes at 0: reset the rate
        // baselines so the first sample after a reconnect isn't distorted.
        inner.last_sample_at = Instant::now();
        inner.last_media_bytes = 0;
        inner.last_wire_bytes = 0;
        crate::log_event!(Level::Info, "transport attached");
        old
    }

    /// Drop a dead transport, keeping the buffer and everything needed to
    /// resume. The muxer's timebase and codec configs are snapshotted so the
    /// next [`attach_transport`](Self::attach_transport) continues cleanly,
    /// and the buffer is cut back to the newest keyframe so the resumed stream
    /// starts at a clean decode point (viewers resync immediately).
    pub fn detach_transport(&self) {
        let mut inner = self.lock();
        // Snapshot before dropping: timebase for timestamp continuity, configs
        // for re-emitting sequence headers on the next connection.
        let snapshot = inner.muxer.as_ref().map(|m| {
            (
                (m.origin(), m.last_dts(MediaKind::Video), m.last_dts(MediaKind::Audio)),
                m.video_config().map(<[u8]>::to_vec),
                m.audio_config().map(<[u8]>::to_vec),
            )
        });
        if let Some((tb, avcc, asc)) = snapshot {
            inner.timebase = Some(tb);
            if inner.avcc.is_none() {
                inner.avcc = avcc;
            }
            if inner.asc.is_none() {
                inner.asc = asc;
            }
        }
        inner.muxer = None;
        inner
            .buffer
            .resync_keep_latest_keyframe(|p| p.kind == MediaKind::Video && p.is_key);
        // If the cut could not find a keyframe (it was already muxed out), the
        // fresh connection must not resume on an orphaned inter-frame: hold
        // all media until the next keyframe arrives. Only streams that carry
        // video ever need to wait (audio-only streams never produce keyframes).
        inner.awaiting_keyframe = inner.avcc.is_some()
            && !inner
                .buffer
                .front()
                .is_some_and(|p| p.kind == MediaKind::Video && p.is_key);
        if inner.awaiting_keyframe {
            crate::log_event!(Level::Warn, "awaiting a keyframe before resuming");
        }
        crate::log_event!(Level::Info, "transport detached");
    }

    /// When the transport last made forward progress, if it reports any. Used
    /// by the session layer for stall detection.
    pub fn transport_progress(&self) -> Option<Instant> {
        let inner = self.lock();
        inner.muxer.as_ref().and_then(|m| m.sink().last_progress())
    }

    /// Send a packet into the pipeline. Safe from any thread.
    pub fn push(&self, pkt: MediaPacket) -> Result<(), EngineError> {
        let mut inner = self.lock();
        inner.buffer.push(pkt);
        Self::enforce_drop_policy(&mut inner);
        Ok(())
    }

    /// Push a batch — avoids per-packet lock churn.
    pub fn push_all(&self, pkts: impl IntoIterator<Item = MediaPacket>) -> Result<(), EngineError> {
        let mut inner = self.lock();
        for p in pkts {
            inner.buffer.push(p);
        }
        Self::enforce_drop_policy(&mut inner);
        Ok(())
    }

    /// Keyframe-aligned drops wired to the [`LatencyProfile`]: once the buffer
    /// passes the profile's threshold, discard everything before the newest
    /// keyframe instead of dripping single packets. Cutting at a keyframe keeps
    /// the remaining stream decodable (viewers can always resync), and jumping
    /// straight to the live edge bounds latency — one big honest drop beats a
    /// slow drift that leaves orphaned inter-frames behind.
    fn enforce_drop_policy(inner: &mut Inner<W>) {
        if let Some(threshold) = inner.config.profile.cut_threshold() {
            if inner.buffer.len() >= threshold {
                let dropped = inner
                    .buffer
                    .resync_keep_latest_keyframe(|p| p.kind == MediaKind::Video && p.is_key);
                if dropped > 0 {
                    crate::log_event!(
                        Level::Warn,
                        "cut to live edge",
                        "dropped" => dropped,
                        "threshold" => threshold
                    );
                }
            }
        }
    }

    /// Push codec-config updates to every output whenever the in-force configs
    /// changed (explicit `configure_codecs`, or sniffed out of the media).
    fn sync_output_codecs(inner: &mut Inner<W>) {
        if inner.output_codecs.0 == inner.avcc && inner.output_codecs.1 == inner.asc {
            return;
        }
        inner.output_codecs = (inner.avcc.clone(), inner.asc.clone());
        let (avcc, asc) = inner.output_codecs.clone();
        for output in &mut inner.outputs {
            output.codecs(avcc.as_deref(), asc.as_deref());
        }
    }

    /// Feed one successfully-muxed packet to every output; a failing output is
    /// retired (logged, dropped) rather than allowed to sink the stream.
    fn feed_outputs(outputs: &mut Vec<Box<dyn PacketSink>>, pkt: &MediaPacket) {
        outputs.retain_mut(|output| match output.packet(pkt) {
            Ok(()) => true,
            Err(e) => {
                crate::log_event!(Level::Warn, "output retired", "error" => e.to_string().as_str());
                false
            }
        });
    }

    /// Drain the buffer for muxing. While [`Inner::awaiting_keyframe`] is set
    /// (a transport died with no keyframe buffered) video inter-frames are
    /// discarded so the fresh connection starts at a clean GOP boundary;
    /// audio and keyframes pass through, and the flag clears as soon as a
    /// keyframe is drained.
    fn drain_for_mux(inner: &mut Inner<W>) -> Vec<MediaPacket> {
        let mut batch = Vec::new();
        while let Some(pkt) = inner.buffer.pop_oldest() {
            if inner.awaiting_keyframe && pkt.kind == MediaKind::Video && !pkt.is_key {
                inner.buffer.record_drop();
                continue;
            }
            if pkt.kind == MediaKind::Video && pkt.is_key {
                inner.awaiting_keyframe = false;
            }
            batch.push(pkt);
        }
        // The platform pushes video and audio from separate encoder threads,
        // so the arrival order in the buffer is *delivery* order, not *time*
        // order: a video encoder that catches up after a stall can deliver a
        // burst of frames whose timestamps race ahead of the audio that was
        // captured at the same wall-clock moment. Muxing in arrival order would
        // then see audio "go backwards" and trip the rebase path on every burst.
        // Sort on DTS so the muxer always sees globally time-ordered packets
        // (stable: equal timestamps keep arrival order).
        batch.sort_by_key(|p| p.dts);
        batch
    }

    /// Emit codec sequence headers up front. The platform usually knows these
    /// (SPS/PPS from the encoder's `MediaFormat`, the AAC `AudioSpecificConfig`),
    /// which is both more reliable and cheaper than sniffing the first packet.
    /// Call after `attach_transport`, before the first `tick`. The configs are
    /// cached and automatically re-emitted after every reconnect.
    pub fn configure_codecs(&self, avcc: Option<&[u8]>, asc: Option<&[u8]>) -> Result<(), EngineError> {
        let mut inner = self.lock();
        if let Some(v) = avcc {
            inner.avcc = Some(v.to_vec());
        }
        if let Some(a) = asc {
            inner.asc = Some(a.to_vec());
        }
        let Some(muxer) = &mut inner.muxer else {
            return Ok(());
        };
        muxer.init_codecs(avcc, asc)?;
        Ok(())
    }

    /// Drain as much as is buffered, muxing each packet into the transport.
    /// Returns how many packets were written this call.
    ///
    /// On a transport failure mid-batch the packets that did *not* make it are
    /// handed back to the front of the buffer, so a reconnect resumes with
    /// them (the session then cuts to the newest keyframe among them).
    pub fn tick(&self) -> Result<usize, EngineError> {
        let mut inner = self.lock();
        // No transport yet: leave the buffer untouched so a later `attach_transport`
        // doesn't find it already drained.
        if inner.muxer.is_none() {
            return Ok(0);
        }
        // Service the transport's inbound direction / health checks first:
        // a dead peer must be discovered before more bytes are fed to it.
        if let Some(m) = inner.muxer.as_mut() {
            m.sink_mut().maintain()?;
        }
        // Drain the buffer first; collect so we never hold two borrows at once.
        let mut batch = Self::drain_for_mux(&mut inner);
        let need_metadata = !inner.metadata_written;
        let stream = inner.config.stream.clone();
        let avcc = inner.avcc.clone();
        let asc = inner.asc.clone();
        let mut sent = 0usize;
        let mut failure = None;
        {
            let Some(muxer) = &mut inner.muxer else {
                return Ok(0);
            };
            if need_metadata {
                failure = muxer.write_metadata(&stream).err();
            }
            // Re-emit sequence headers (idempotent): on a fresh connection the
            // muxer has never sent them, and the server cannot decode without them.
            if failure.is_none() {
                failure = muxer.init_codecs(avcc.as_deref(), asc.as_deref()).err();
            }
            while failure.is_none() && sent < batch.len() {
                let pkt = &batch[sent];
                match muxer.write_packet(pkt) {
                    Ok(()) => sent += 1,
                    Err(MuxError::Ordering(_)) => {
                        // The capture clock jumped backwards beyond tolerance
                        // (platform clock reset, encoder restart). Re-anchor and
                        // continue rather than taking the stream down over a
                        // timestamp hiccup.
                        crate::log_event!(
                            Level::Warn,
                            "clock rebase",
                            "kind" => if pkt.kind == MediaKind::Video { "video" } else { "audio" },
                            "dts" => pkt.dts
                        );
                        muxer.rebase(pkt.kind, pkt.dts);
                        failure = muxer.write_packet(pkt).err();
                        if failure.is_none() {
                            sent += 1;
                        }
                    }
                    Err(e) => failure = Some(e),
                }
            }
            if failure.is_none() {
                failure = muxer.flush_sink().err();
            }
        }
        // Hand every packet that reached the wire to the packet-level outputs
        // (recording, HLS). Done after the mux block so the muxer borrow is
        // released; a partial batch feeds only its successful prefix.
        for pkt in batch.iter().take(sent) {
            Self::feed_outputs(&mut inner.outputs, pkt);
        }
        let outcome = match failure {
            None => {
                if need_metadata {
                    inner.metadata_written = true;
                }
                // Harvest configs the muxer sniffed out of the packets themselves,
                // so a later reconnect can re-emit them even when the platform
                // never handed them over explicitly.
                if inner.avcc.is_none() {
                    inner.avcc = inner.muxer.as_ref().and_then(|m| m.video_config().map(<[u8]>::to_vec));
                }
                if inner.asc.is_none() {
                    inner.asc = inner.muxer.as_ref().and_then(|m| m.audio_config().map(<[u8]>::to_vec));
                }
                // Outputs learn about sniffed/changed configs alongside the muxer.
                Self::sync_output_codecs(&mut inner);
                inner.muxed += batch.len() as u64;
                Ok(batch.len())
            }
            Some(e) => {
                // Hand back everything that didn't reach the wire.
                let unsent = batch.split_off(sent);
                inner.buffer.restore_front(unsent);
                inner.muxed += sent as u64;
                crate::log_event!(
                    Level::Error,
                    "mux failed",
                    "error" => e.to_string().as_str()
                );
                Err(EngineError::Mux(e))
            }
        };
        let qos = inner.qos.clone();
        let sample = Self::sample_qos(&mut inner);
        drop(inner);
        if let Some(sample) = sample {
            qos.emit_sample(sample);
        }
        outcome
    }

    /// Build one `QoS` sample from the buffer, muxer, and transport counters —
    /// but only when enough wall time has passed since the last sample (the
    /// engine samples at most once per `QosConfig.interval`). Rate fields are
    /// deltas of the byte counters over the gap; gaps longer than
    /// [`MAX_SAMPLE_GAP`] (idle stream, reconnect) reset the baselines so an
    /// idle period can't distort the reported rate.
    fn sample_qos(inner: &mut Inner<W>) -> Option<QosSample> {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(inner.last_sample_at);
        if elapsed < inner.config.qos.interval {
            return None;
        }
        let gap_ok = elapsed <= MAX_SAMPLE_GAP;
        let media = inner.muxer.as_ref().map_or(0, FlvMuxer::bytes_written);
        let wire = inner.muxer.as_ref().map_or(0, |m| m.sink().bytes_written());
        let (media_rate, wire_rate) = if gap_ok && elapsed.as_secs_f64() > 0.0 {
            let dt = elapsed.as_secs_f64();
            (
                media.saturating_sub(inner.last_media_bytes) as f64 * 8.0 / dt,
                wire.saturating_sub(inner.last_wire_bytes) as f64 * 8.0 / dt,
            )
        } else {
            (0.0, 0.0)
        };
        inner.last_sample_at = now;
        inner.last_media_bytes = media;
        inner.last_wire_bytes = wire;
        let pushed = inner.buffer.pushed();
        let dropped = inner.buffer.dropped();
        Some(QosSample {
            wall_ms: wall_clock_ms(),
            uptime_ms: inner.clock.now_ms() as u64,
            bitrate_out_bps: media_rate,
            throughput_bps: wire_rate,
            drop_ratio: if pushed > 0 {
                dropped as f64 / pushed as f64
            } else {
                0.0
            },
            buffer_ms: inner.buffer.last().map_or(0, |p| inner.clock.latency_ms(p.pts)),
            buffered_count: inner.buffer.len(),
            rtt_ms: inner
                .muxer
                .as_ref()
                .and_then(|m| m.sink().rtt())
                .map(|d| d.as_secs_f64() * 1000.0),
            reconnects: inner.qos.reconnects(),
            reconnect_attempts: inner.qos.reconnect_attempts(),
            pushed,
            muxed: inner.muxed,
            dropped,
            last_error: inner.qos.last_error(),
        })
    }

    /// Drain everything and end the stream cleanly (user pressed "end").
    /// Any packets still buffered are muxed before the transport shuts down.
    pub fn finish(&self) -> Result<(), EngineError> {
        let mut inner = self.lock();
        if inner.muxer.is_none() {
            return Ok(());
        }
        let batch = Self::drain_for_mux(&mut inner);
        for pkt in &batch {
            {
                let Some(muxer) = &mut inner.muxer else {
                    return Ok(());
                };
                muxer.write_packet(pkt)?;
            }
            Self::feed_outputs(&mut inner.outputs, pkt);
        }
        {
            let Some(muxer) = &mut inner.muxer else {
                return Ok(());
            };
            muxer.finish()?;
        }
        let muxed_now = batch.len() as u64;
        inner.muxed += muxed_now;
        crate::log_event!(
            Level::Info,
            "stream finished",
            "packets" => muxed_now
        );
        if let Some(t) = inner.muxer.take() {
            t.into_inner().shutdown()?;
        }
        // Finalize every output (recordings close their file, HLS writes the
        // end marker); a failing output is reported, not fatal.
        for output in &mut inner.outputs {
            if let Err(e) = output.finish() {
                crate::log_event!(Level::Warn, "output finish failed", "error" => e.to_string().as_str());
            }
        }
        Ok(())
    }

    /// Telemetry for the platform's UI.
    pub fn stats(&self) -> PacketStats {
        let inner = self.lock();
        let buffered_ms = inner.buffer.last().map_or(0, |p| inner.clock.latency_ms(p.pts));
        PacketStats {
            pushed: inner.buffer.pushed(),
            muxed: inner.muxed,
            dropped: inner.buffer.dropped(),
            in_buffered_count: inner.buffer.len(),
            buffer_ms: buffered_ms,
            media_bytes: inner.muxer.as_ref().map_or(0, FlvMuxer::bytes_written),
            wire_bytes: inner.muxer.as_ref().map_or(0, |m| m.sink().bytes_written()),
            rtt_ms: inner
                .muxer
                .as_ref()
                .and_then(|m| m.sink().rtt())
                .map(|d| d.as_secs_f64() * 1000.0),
            reconnects: inner.qos.reconnects(),
            reconnect_attempts: inner.qos.reconnect_attempts(),
            uptime_ms: inner.clock.now_ms() as u64,
        }
    }

    /// Shared `QoS` collector. The platform polls it (or registers a listener)
    /// to drive a UI without racing the engine thread.
    pub fn qos(&self) -> Arc<Qos> {
        self.lock().qos.clone()
    }

    /// Number of reconnects the session layer has performed on this engine.
    pub fn reconnects(&self) -> u64 {
        let inner = self.lock();
        inner.qos.reconnects()
    }

    /// Most recent fatal transport failure, if any, as recorded by the last
    /// reconnect. Cleared by [`Engine::reset`].
    pub fn last_error(&self) -> Option<String> {
        let inner = self.lock();
        inner.qos.last_error()
    }

    /// When badly behind, discard everything before the newest keyframe so the
    /// next `tick` resumes from a clean decode point. Returns how many packets
    /// were discarded.
    pub fn resync(&self) -> u64 {
        let mut inner = self.lock();
        inner
            .buffer
            .resync_keep_latest_keyframe(|p| p.kind == MediaKind::Video && p.is_key)
    }

    /// Reset to a fresh state (new session / stream key).
    pub fn reset(&self) {
        let mut inner = self.lock();
        let budget = inner.config.profile.packet_budget();
        inner.buffer = BoundedBuffer::new(budget);
        inner.metadata_written = false;
        inner.muxed = 0;
        inner.muxer = None;
        inner.avcc = None;
        inner.asc = None;
        inner.timebase = None;
        inner.awaiting_keyframe = false;
        inner.qos.reset();
        inner.last_sample_at = Instant::now();
        inner.last_media_bytes = 0;
        inner.last_wire_bytes = 0;
        // Outputs stay attached (a recorder keeps its file open); only the
        // config-tracking snapshot resets along with everything else.
        inner.output_codecs = (None, None);
        crate::log_event!(Level::Info, "engine reset");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{set_logger, Logger, Record};
    use std::io;
    use std::sync::Mutex;

    /// The AAC `AudioSpecificConfig` and H.264 `AVCDecoderConfigurationRecord`.
    const AVCC: &[u8] = &[
        0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x03, 0x67, 0x42, 0x00, 0x0A, 0x01, 0x00, 0x03, 0x68, 0xCE,
    ];
    const ASC: &[u8] = &[0x0A, 0x10];

    /// In-memory transport that just records whatever the muxer writes.
    #[derive(Clone)]
    struct MemSink {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for MemSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Transport for MemSink {
        fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Transport whose inbound health check always fails — models the
    /// half-open zombie the watchdog detects.
    struct DeadPeerSink(MemSink);

    impl io::Write for DeadPeerSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.write(buf)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.0.flush()
        }
    }

    impl Transport for DeadPeerSink {
        fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn maintain(&mut self) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "publisher watchdog: peer is dead",
            ))
        }
    }

    #[test]
    fn failed_maintain_fails_the_tick_without_muxing() {
        // A transport that reports its peer dead during maintain must fail
        // the tick (so the session reconnects) and must not have written
        // anything on this pass.
        let sink = DeadPeerSink(MemSink {
            bytes: Arc::new(Mutex::new(Vec::new())),
        });
        let engine: Engine<DeadPeerSink> = Engine::new(EngineConfig::default());
        engine.attach_transport(sink);
        engine.configure_codecs(Some(AVCC), Some(ASC)).unwrap();
        engine.push(video(0, true)).unwrap();
        let err = engine.tick().unwrap_err();
        assert!(err.to_string().contains("watchdog"), "got: {err}");
    }

    fn video(dts: i64, key: bool) -> MediaPacket {
        MediaPacket::video(dts, key, vec![0, 0, 0, 1, 0x65, 0x88])
    }

    fn audio(dts: i64) -> MediaPacket {
        MediaPacket::audio(dts, vec![0x21, 0x00, 0x49, 0x10, 0x04])
    }

    /// A logger that records the message of every record it receives, so a test
    /// can assert that a specific event (or its absence) reached the sink.
    #[derive(Clone)]
    struct CapturingLogger {
        messages: Arc<Mutex<Vec<String>>>,
    }

    impl Logger for CapturingLogger {
        fn log(&self, record: &Record<'_>) {
            self.messages.lock().unwrap().push(record.message.to_owned());
        }
    }

    /// Video and audio pushed from two encoder threads arrive in *delivery*
    /// order, not time order: here the video frames are all pushed before the
    /// audio frames that were captured at the same wall-clock moments. The
    /// engine must mux in DTS order so the muxer never sees audio "go
    /// backwards" and trip the clock-rebase path.
    #[test]
    fn bursty_delivery_is_muxed_in_dts_order_without_rebase() {
        let messages = Arc::new(Mutex::new(Vec::new()));
        set_logger(Some(Arc::new(CapturingLogger {
            messages: messages.clone(),
        })));

        let bytes = Arc::new(Mutex::new(Vec::new()));
        let engine = Engine::new(EngineConfig::default());
        engine.configure_codecs(Some(AVCC), Some(ASC)).unwrap();
        engine.attach_transport(MemSink { bytes });
        engine
            .push_all([
                // A video encoder that just caught up after a stall delivers a
                // burst whose timestamps race ahead of the audio captured at the
                // same real moments.
                video(0, true),
                video(33, false),
                video(66, false),
                video(99, false),
                video(132, false),
                // The audio that *should* interleave between those frames, but
                // which arrives later in the buffer.
                audio(21),
                audio(42),
                audio(63),
                audio(84),
                audio(105),
            ])
            .unwrap();
        let written = engine.tick().unwrap();
        assert_eq!(written, 10, "every buffered packet must reach the wire");

        let messages = messages.lock().unwrap();
        assert!(
            !messages.iter().any(|m| m.contains("clock rebase")),
            "out-of-order delivery must be sorted, not rebased: {messages:?}"
        );
        drop(messages);
        set_logger(None);
    }
}
