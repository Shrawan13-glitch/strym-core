//! `stream-ffi` — the mobile interface to the `stream` core.
//!
//! This crate wraps the dependency-free core in a single session object that
//! an app can drive from Kotlin or Swift (via [UniFFI](https://mozilla.github.io/uniffi-rs/)):
//!
//! ```text
//! ┌────────────┐        ┌────────────────���─────────────────────────────┐
//! │ app thread │ push_* │ StreamSession                                │
//! │ (encoder)  ├───────▶│  bounded buffer ─▶ worker thread             │
//! └────────────┘        │   (never blocks)    (connect/reconnect/flush)│
//!                       │        ▲                   │                 │
//!                       │ callbacks ◀────────────────┘                 │
//!                       └───────���──────────────────────────────────────┘
//! ```
//!
//! # Memory & threading contract
//!
//! - **Capture stays on the platform.** [`StreamSession::push_video`] and
//!   [`StreamSession::push_audio`] copy their bytes into the core's bounded
//!   buffer and return immediately — the platform keeps owning (and may
//!   recycle) its encoder buffers. No buffer is retained beyond the copy.
//! - **One worker thread per session.** [`StreamSession::start`] spawns it;
//!   it owns the network lifecycle (connect, jittered-backoff reconnects,
//!   draining, clean finish). Push calls never touch the socket directly.
//! - **Thread-safe surface.** [`StreamSession`] is reference-counted and
//!   every method takes `&self`; push, stats, and stop are safe from any
//!   thread. Callbacks arrive on the worker thread — hop to the UI thread
//!   before touching views.
//! - **Deterministic shutdown is [`StreamSession::stop`].** Dropping the
//!   session without stopping only signals the worker to wind down; call
//!   `stop()` to join it and flush the last media.
//! - **One session per stream.** After [`StreamSession::stop`] the session is
//!   finished; create a new [`StreamSession`] to go live again.

use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use stream::engine::{Engine, EngineConfig, LatencyProfile};
use stream::models::{MediaPacket, PacketStats, StreamConfig};
use stream::rtmp::{RtmpConfig, RtmpConnector, RtmpTransport};
use stream::session::{ReconnectPolicy, Session, SessionPolicy, SessionState as CoreState};
use stream::telemetry::{self, Level, Logger, QosSample, Record};

uniffi::setup_scaffolding!();

/// The core's transport type for a TCP RTMP publish connection.
type MediaEngine = Engine<RtmpTransport<TcpStream>>;
/// A publish session wired to an RTMP connector.
type RtmpSession = Session<RtmpConnector>;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Every way an FFI call can fail. Each variant becomes its own exception
/// type in Kotlin/Swift, carrying the `message`.
#[derive(Debug, uniffi::Error)]
#[uniffi(flat_error)]
pub enum StreamError {
    /// The configuration was rejected (bad URL, empty app/stream key, ...).
    InvalidConfig {
        /// What exactly was wrong.
        message: String,
    },
    /// The call does not fit the session's current lifecycle state.
    InvalidState {
        /// What exactly was wrong.
        message: String,
    },
    /// The core rejected the operation (codec config, ...).
    Engine {
        /// What exactly was wrong.
        message: String,
    },
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::InvalidConfig { message } => write!(f, "invalid config: {message}"),
            StreamError::InvalidState { message } => write!(f, "invalid state: {message}"),
            StreamError::Engine { message } => write!(f, "engine error: {message}"),
        }
    }
}

impl std::error::Error for StreamError {}

// ---------------------------------------------------------------------------
// Configuration records
// ---------------------------------------------------------------------------

/// The RTMP endpoint to publish to.
#[derive(Debug, Clone, uniffi::Record)]
pub struct RtmpDestination {
    /// Server base URL: `rtmp://host[:port]` (port defaults to 1935). Put the
    /// app name in [`RtmpDestination::app`], not in the URL.
    pub url: String,
    /// Application name, e.g. `"live"`.
    pub app: String,
    /// Stream key (the publish name). Never logged.
    pub stream_key: String,
    /// Socket read/write timeout in milliseconds. `0` selects the core's 10 s
    /// default.
    pub timeout_ms: u64,
}

/// How aggressively the session stays at the live edge when the network is
/// slower than the encoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum LatencyMode {
    /// Cut to the live edge at the first keyframe once the buffer lags.
    Aggressive,
    /// Cut only when the lag becomes clearly visible.
    Balanced,
    /// Never cut; the buffer absorbs stalls (recording-style ingest).
    Lenient,
}

impl From<LatencyMode> for LatencyProfile {
    fn from(mode: LatencyMode) -> Self {
        match mode {
            LatencyMode::Aggressive => Self::Aggressive,
            LatencyMode::Balanced => Self::Balanced,
            LatencyMode::Lenient => Self::Lenient,
        }
    }
}

/// Static stream parameters (what the encoder will produce).
#[derive(Debug, Clone, uniffi::Record)]
pub struct StreamInfo {
    /// Encoded video width in pixels.
    pub width: u32,
    /// Encoded video height in pixels.
    pub height: u32,
    /// Nominal frame rate in frames/s.
    pub framerate: f64,
    /// Nominal video bitrate in bits/s (metadata only).
    pub video_bitrate_bps: u32,
    /// Nominal audio bitrate in bits/s (metadata only).
    pub audio_bitrate_bps: u32,
    /// Audio sampling rate in Hz.
    pub audio_sample_rate_hz: u32,
}

impl Default for StreamInfo {
    fn default() -> Self {
        Self {
            width: 1920,
            height: 1080,
            framerate: 30.0,
            video_bitrate_bps: 4_000_000,
            audio_bitrate_bps: 128_000,
            audio_sample_rate_hz: 48_000,
        }
    }
}

impl From<&StreamInfo> for StreamConfig {
    fn from(info: &StreamInfo) -> Self {
        Self {
            width: info.width,
            height: info.height,
            framerate: info.framerate,
            video_bitrate: info.video_bitrate_bps,
            audio_bitrate: info.audio_bitrate_bps,
            sample_rate: info.audio_sample_rate_hz,
        }
    }
}

/// Everything needed to run one publish session.
///
/// Build one with [`default_session_config`] and adjust the fields you need.
#[derive(Debug, Clone, uniffi::Record)]
pub struct SessionConfig {
    /// Where to publish.
    pub destination: RtmpDestination,
    /// What the encoder produces.
    pub stream: StreamInfo,
    /// Live-edge behaviour under network pressure.
    pub latency: LatencyMode,
    /// Reconnect budget: `None` retries forever, `Some(n)` gives up after `n`
    /// failed attempts.
    pub reconnect_max_attempts: Option<u32>,
    /// Delay before the first scheduled reconnect retry, in milliseconds.
    pub reconnect_initial_delay_ms: u64,
    /// Cap on the exponential reconnect backoff, in milliseconds.
    pub reconnect_max_delay_ms: u64,
    /// Reconnect when the transport reports no forward progress for this long,
    /// in milliseconds.
    pub stall_timeout_ms: u64,
    /// Worker-loop cadence in milliseconds: how often buffered media is
    /// drained to the network.
    pub pump_interval_ms: u64,
    /// How often [`StreamListener::on_stats`] fires, in milliseconds.
    pub stats_interval_ms: u64,
}

/// A [`SessionConfig`] with production-ready defaults; tweak fields as needed.
#[uniffi::export]
pub fn default_session_config(destination: RtmpDestination, stream: StreamInfo) -> SessionConfig {
    SessionConfig {
        destination,
        stream,
        latency: LatencyMode::Balanced,
        reconnect_max_attempts: None,
        reconnect_initial_delay_ms: 500,
        reconnect_max_delay_ms: 15_000,
        stall_timeout_ms: 10_000,
        pump_interval_ms: 16,
        stats_interval_ms: 1000,
    }
}

// ---------------------------------------------------------------------------
// Live state & telemetry
// ---------------------------------------------------------------------------

/// The session lifecycle as seen from the platform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum SessionState {
    /// Not connected (before `start`, or after a failed connect).
    Idle,
    /// Dialing and handshaking the RTMP publish.
    Connecting,
    /// Media is flowing to the server.
    Live,
    /// The connection dropped; reconnection is in progress.
    Reconnecting,
    /// The reconnect budget ran out; call [`StreamSession::retry`] to try again.
    Exhausted,
    /// `stop()` completed; the session is finished and cannot restart.
    Stopped,
}

impl SessionState {
    fn as_u8(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Connecting => 1,
            Self::Live => 2,
            Self::Reconnecting => 3,
            Self::Exhausted => 4,
            Self::Stopped => 5,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Connecting,
            2 => Self::Live,
            3 => Self::Reconnecting,
            4 => Self::Exhausted,
            5 => Self::Stopped,
            _ => Self::Idle,
        }
    }
}

/// A periodic snapshot of session health, delivered by
/// [`StreamListener::on_stats`] and returned by [`StreamSession::stats`].
#[derive(Debug, Clone, uniffi::Record)]
pub struct SessionStats {
    /// Lifecycle state at sample time.
    pub state: SessionState,
    /// Packets accepted from the platform since creation.
    pub pushed: u64,
    /// Packets muxed to the network since creation.
    pub muxed: u64,
    /// Packets dropped since creation (backpressure cuts).
    pub dropped: u64,
    /// Packets currently waiting in the backpressure buffer.
    pub buffered_packets: u64,
    /// Current buffer lag in ms (newest buffered timestamp vs the live edge).
    pub buffer_lag_ms: i64,
    /// Compressed media bytes handed to the transport.
    pub media_bytes: u64,
    /// Bytes on the wire including transport framing.
    pub wire_bytes: u64,
    /// Media bitrate out in bits/s (0 before the first stats tick).
    pub bitrate_out_bps: f64,
    /// Effective wire throughput in bits/s (0 before the first stats tick).
    pub throughput_bps: f64,
    /// Fraction of pushed packets dropped (0.0..=1.0).
    pub drop_ratio: f64,
    /// Latest measured round-trip time in ms, when available.
    pub rtt_ms: Option<f64>,
    /// Successful reconnects since creation.
    pub reconnects: u64,
    /// Failed reconnect attempts since creation.
    pub reconnect_attempts: u64,
    /// Milliseconds since the session was created.
    pub uptime_ms: u64,
}

impl SessionStats {
    /// Build stats from a `QosSample` (rates included).
    fn from_sample(sample: &QosSample, state: SessionState) -> Self {
        Self {
            state,
            pushed: sample.pushed,
            muxed: sample.muxed,
            dropped: sample.dropped,
            buffered_packets: sample.buffered_count as u64,
            buffer_lag_ms: sample.buffer_ms,
            media_bytes: 0,
            wire_bytes: 0,
            bitrate_out_bps: sample.bitrate_out_bps,
            throughput_bps: sample.throughput_bps,
            drop_ratio: sample.drop_ratio,
            rtt_ms: sample.rtt_ms,
            reconnects: sample.reconnects,
            reconnect_attempts: sample.reconnect_attempts,
            uptime_ms: sample.uptime_ms,
        }
    }

    /// Build stats from packet counters alone (no rate data yet).
    fn from_packets(packets: &PacketStats, state: SessionState) -> Self {
        Self {
            state,
            pushed: packets.pushed,
            muxed: packets.muxed,
            dropped: packets.dropped,
            buffered_packets: packets.in_buffered_count as u64,
            buffer_lag_ms: packets.buffer_ms,
            media_bytes: packets.media_bytes,
            wire_bytes: packets.wire_bytes,
            bitrate_out_bps: 0.0,
            throughput_bps: 0.0,
            drop_ratio: if packets.pushed > 0 {
                packets.dropped as f64 / packets.pushed as f64
            } else {
                0.0
            },
            rtt_ms: packets.rtt_ms,
            reconnects: packets.reconnects,
            reconnect_attempts: packets.reconnect_attempts,
            uptime_ms: packets.uptime_ms,
        }
    }
}

/// Receives lifecycle and telemetry callbacks.
///
/// **Threading:** every callback fires on the session's worker thread — hop
/// to the UI thread before touching views.
#[uniffi::export(with_foreign)]
pub trait StreamListener: Send + Sync {
    /// The lifecycle state changed; `detail` carries a failure description
    /// where relevant.
    fn on_state_changed(&self, state: SessionState, detail: Option<String>);
    /// A periodic health snapshot (cadence: `stats_interval_ms`).
    fn on_stats(&self, stats: SessionStats);
}

// ---------------------------------------------------------------------------
// The session object
// ---------------------------------------------------------------------------

/// State shared between the session handle and its worker thread.
struct Shared {
    /// The core session, touched only under this lock (the worker holds it
    /// briefly per tick; lifecycle calls serialize on it too).
    session: Mutex<RtmpSession>,
    /// Set to ask the worker to finish and exit.
    stop: AtomicBool,
    /// Set to ask the worker to re-arm an exhausted reconnect budget.
    rearm: AtomicBool,
    /// Current [`SessionState`] (encoded via [`SessionState::as_u8`]).
    state: AtomicU8,
    /// The worker's join handle. The worker clears it when it exits on its own
    /// (an initial connect failure), so the session can be re-launched.
    thread: Mutex<Option<JoinHandle<()>>>,
    /// Description of the most recent failure, kept so [`StreamSession::last_error`]
    /// survives the worker exiting (the engine clears its own on a fresh start).
    last_error: Mutex<Option<String>>,
}

/// One live publish session: the object an app creates, feeds, and stops.
///
/// See the [crate-level docs](self) for the memory & threading contract.
#[derive(uniffi::Object)]
pub struct StreamSession {
    shared: Arc<Shared>,
    /// Cheap clone of the session's engine for thread-safe push/stats calls.
    engine: MediaEngine,
    listener: Arc<dyn StreamListener>,
    /// Latest `QosSample`, kept so [`Self::stats`] can include rates between
    /// ticks (shared with the `QosSample` sink closure).
    last_sample: Arc<Mutex<Option<QosSample>>>,
    pump: Duration,
}

impl StreamSession {
    /// Spawn the worker thread. The join handle is stored in `Shared` so the
    /// worker can clear it when it exits on its own.
    fn spawn_worker(&self) -> Result<(), StreamError> {
        let shared = self.shared.clone();
        let listener = self.listener.clone();
        let pump = self.pump;
        let handle = thread::Builder::new()
            .name("stream-session".into())
            .spawn(move || run_worker(&shared, &listener, pump))
            .map_err(|e| StreamError::InvalidState {
                message: format!("cannot spawn the worker thread: {e}"),
            })?;
        *lock(&self.shared.thread) = Some(handle);
        Ok(())
    }

    fn state_atomic(&self) -> SessionState {
        SessionState::from_u8(self.shared.state.load(Ordering::Acquire))
    }

    fn set_state(&self, state: SessionState, detail: Option<String>) {
        self.shared.state.store(state.as_u8(), Ordering::Release);
        self.listener.on_state_changed(state, detail);
    }
}

#[uniffi::export]
impl StreamSession {
    /// Create a session. Validates the configuration eagerly; the connection
    /// itself happens in [`Self::start`].
    #[uniffi::constructor]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(config: SessionConfig, listener: Arc<dyn StreamListener>) -> Result<Arc<Self>, StreamError> {
        let (addr, rtmp_config) = parse_destination(&config.destination)?;
        if config.pump_interval_ms == 0 {
            return Err(StreamError::InvalidConfig {
                message: "pump_interval_ms must be > 0".to_owned(),
            });
        }

        let engine_config = EngineConfig {
            stream: StreamConfig::from(&config.stream),
            profile: LatencyProfile::from(config.latency),
            qos: stream::telemetry::QosConfig {
                interval: Duration::from_millis(config.stats_interval_ms.max(50)),
                ..Default::default()
            },
            ..EngineConfig::default()
        };

        let reconnect = ReconnectPolicy {
            initial_delay: Duration::from_millis(config.reconnect_initial_delay_ms),
            max_delay: Duration::from_millis(config.reconnect_max_delay_ms),
            max_attempts: config.reconnect_max_attempts,
            ..Default::default()
        };
        let policy = SessionPolicy {
            reconnect,
            stall_timeout: Duration::from_millis(config.stall_timeout_ms),
        };

        let session = Session::new(engine_config, RtmpConnector::tcp(addr, rtmp_config), policy);
        let engine = session.engine().clone();

        let shared = Arc::new(Shared {
            session: Mutex::new(session),
            stop: AtomicBool::new(false),
            rearm: AtomicBool::new(false),
            state: AtomicU8::new(SessionState::Idle.as_u8()),
            thread: Mutex::new(None),
            last_error: Mutex::new(None),
        });

        // Wire periodic QoS samples: cache the newest one for `stats()` and
        // deliver it to the platform listener with the live state.
        let last_sample: Arc<Mutex<Option<QosSample>>> = Arc::new(Mutex::new(None));
        let sample_cache = last_sample.clone();
        let state_slot = shared.clone();
        let stats_listener = listener.clone();
        engine.qos().set_sink(Some(Box::new(move |sample: QosSample| {
            *lock(&sample_cache) = Some(sample.clone());
            let state = SessionState::from_u8(state_slot.state.load(Ordering::Acquire));
            stats_listener.on_stats(SessionStats::from_sample(&sample, state));
        })));

        Ok(Arc::new(Self {
            shared,
            engine,
            listener,
            last_sample,
            pump: Duration::from_millis(config.pump_interval_ms),
        }))
    }

    /// Start the session: spawns the worker thread, which connects and goes
    /// live asynchronously (watch [`StreamListener::on_state_changed`]).
    pub fn start(&self) -> Result<(), StreamError> {
        if lock(&self.shared.thread).is_some() {
            return Err(StreamError::InvalidState {
                message: "the session is already running".to_owned(),
            });
        }
        if self.state_atomic() == SessionState::Stopped {
            return Err(StreamError::InvalidState {
                message: "a stopped session cannot start again; create a new one".to_owned(),
            });
        }
        self.set_state(SessionState::Connecting, None);
        self.spawn_worker()
    }

    /// Provide codec configuration before (or instead of) autodetection:
    /// an H.264 `AVCDecoderConfigurationRecord` and/or an AAC
    /// `AudioSpecificConfig`.
    #[allow(clippy::needless_pass_by_value)]
    pub fn configure_codecs(
        &self,
        avc_decoder_config: Option<Vec<u8>>,
        audio_specific_config: Option<Vec<u8>>,
    ) -> Result<(), StreamError> {
        self.engine
            .configure_codecs(avc_decoder_config.as_deref(), audio_specific_config.as_deref())
            .map_err(|e| StreamError::Engine { message: e.to_string() })
    }

    /// Push one encoded video frame (Annex B) with its presentation timestamp
    /// in milliseconds. Copies into the bounded buffer and returns
    /// immediately — never blocks the encoder.
    pub fn push_video(&self, pts_ms: i64, is_keyframe: bool, annex_b: Vec<u8>) {
        let _ = self.engine.push(MediaPacket::video(pts_ms, is_keyframe, annex_b));
    }

    /// Push one encoded audio frame with its timestamp in milliseconds.
    /// Copies into the bounded buffer and returns immediately.
    pub fn push_audio(&self, pts_ms: i64, data: Vec<u8>) {
        let _ = self.engine.push(MediaPacket::audio(pts_ms, data));
    }

    /// The current lifecycle state.
    pub fn state(&self) -> SessionState {
        self.state_atomic()
    }

    /// A fresh snapshot of session health.
    pub fn stats(&self) -> SessionStats {
        let state = self.state_atomic();
        let packets = self.engine.stats();
        let mut snapshot = match lock(&self.last_sample).clone() {
            // Rates from the newest QoS sample, when one exists yet.
            Some(sample) => SessionStats::from_sample(&sample, state),
            None => SessionStats::from_packets(&packets, state),
        };
        // Counters and byte totals always come from the engine (freshest).
        snapshot.pushed = packets.pushed;
        snapshot.muxed = packets.muxed;
        snapshot.dropped = packets.dropped;
        snapshot.buffered_packets = packets.in_buffered_count as u64;
        snapshot.buffer_lag_ms = packets.buffer_ms;
        snapshot.media_bytes = packets.media_bytes;
        snapshot.wire_bytes = packets.wire_bytes;
        snapshot.reconnects = packets.reconnects;
        snapshot.reconnect_attempts = packets.reconnect_attempts;
        snapshot
    }

    /// Successful reconnects so far.
    pub fn reconnect_count(&self) -> u64 {
        self.engine.reconnects()
    }

    /// Description of the most recent failure, if any.
    pub fn last_error(&self) -> Option<String> {
        lock(&self.shared.last_error)
            .clone()
            .or_else(|| self.engine.last_error())
    }

    /// Re-arm after the reconnect budget ran out ([`SessionState::Exhausted`]),
    /// or restart a session whose initial connect failed. No-op while live.
    pub fn retry(&self) -> Result<(), StreamError> {
        if self.state_atomic() == SessionState::Stopped {
            return Err(StreamError::InvalidState {
                message: "a stopped session cannot retry; create a new one".to_owned(),
            });
        }
        if lock(&self.shared.thread).is_none() {
            // The worker exited (initial connect failed): bring it back.
            self.set_state(SessionState::Connecting, None);
            self.spawn_worker()
        } else {
            self.shared.rearm.store(true, Ordering::Release);
            Ok(())
        }
    }

    /// Reset the clock origin to the next packet (recover from a large
    /// encoder timestamp jump). Returns the rebase origin in ms.
    pub fn resync(&self) -> u64 {
        self.engine.resync()
    }

    /// Graceful shutdown: signal the worker, wait for it to drain and close
    /// the connection, and mark the session stopped. Safe to call more than
    /// once. After this the session cannot be started again.
    pub fn stop(&self) {
        self.shared.stop.store(true, Ordering::Release);
        if let Some(handle) = lock(&self.shared.thread).take() {
            let _ = handle.join();
        }
    }
}

impl Drop for StreamSession {
    fn drop(&mut self) {
        // Deterministic shutdown is `stop()`; if the platform lets go without
        // calling it, at least tell the worker to wind down on its own.
        self.shared.stop.store(true, Ordering::Release);
    }
}

/// The worker thread: owns the connect → stream → reconnect lifecycle.
///
/// Exits on its own (initial connect failure, core said finished) by clearing
/// its join handle from [`Shared::thread`], so the session can be launched
/// again via [`StreamSession::start`] or [`StreamSession::retry`].
fn run_worker(shared: &Arc<Shared>, listener: &Arc<dyn StreamListener>, pump: Duration) {
    let set_state = |state: SessionState, detail: Option<String>| {
        shared.state.store(state.as_u8(), Ordering::Release);
        listener.on_state_changed(state, detail);
    };
    let last_error = |session: &RtmpSession| session.last_error().map(str::to_owned);

    // Initial connect.
    let start_result = lock(&shared.session).start();
    if let Err(error) = start_result {
        // Drop the handle first, then report Idle: `retry()` decides whether
        // to re-launch based on the handle being gone, so it must see it gone
        // by the time it observes the Idle state.
        clear_handle(shared);
        *lock(&shared.last_error) = Some(format!("connect failed: {error}"));
        set_state(SessionState::Idle, Some(format!("connect failed: {error}")));
        return;
    }
    *lock(&shared.last_error) = None;
    set_state(SessionState::Live, None);

    let mut last_core = CoreState::Connected;
    loop {
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        if shared.rearm.swap(false, Ordering::AcqRel) {
            lock(&shared.session).retry();
        }
        let core = {
            let mut session = lock(&shared.session);
            let _ = session.tick();
            session.state()
        };
        if core != last_core {
            last_core = core;
            let session = lock(&shared.session);
            match core {
                CoreState::Connected => set_state(SessionState::Live, None),
                CoreState::Reconnecting => {
                    set_state(SessionState::Reconnecting, last_error(&session));
                }
                CoreState::Exhausted => set_state(SessionState::Exhausted, last_error(&session)),
                CoreState::Finished | CoreState::Idle => break,
                // `CoreState` is non_exhaustive: treat unknown states as idle.
                _ => set_state(SessionState::Idle, None),
            }
        }
        // While parked on an exhausted budget, poll slowly instead of busy-spinning.
        let parked = core == CoreState::Exhausted;
        thread::sleep(if parked { pump * 10 } else { pump });
    }

    let _ = lock(&shared.session).finish();
    set_state(SessionState::Stopped, None);
}

/// Drop the worker's join handle, marking the session as launchable again.
fn clear_handle(shared: &Arc<Shared>) {
    lock(&shared.thread).take();
}

/// Parse and validate an [`RtmpDestination`] into the dial address and the
/// core's [`RtmpConfig`].
fn parse_destination(dest: &RtmpDestination) -> Result<(String, RtmpConfig), StreamError> {
    let invalid = |message: &str| StreamError::InvalidConfig {
        message: message.to_owned(),
    };

    let rest = dest
        .url
        .strip_prefix("rtmp://")
        .ok_or_else(|| invalid("destination.url must start with rtmp://"))?;
    if rest.is_empty() || rest.contains('/') {
        return Err(invalid(
            "destination.url must be rtmp://host[:port] — set the app name in the `app` field",
        ));
    }
    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) => (
            host,
            port.parse::<u16>()
                .map_err(|_| invalid("bad port in destination.url"))?,
        ),
        None => (rest, 1935),
    };
    if host.is_empty() {
        return Err(invalid("empty host in destination.url"));
    }
    if dest.app.trim().is_empty() {
        return Err(invalid("app must not be empty"));
    }
    if dest.stream_key.trim().is_empty() {
        return Err(invalid("stream_key must not be empty"));
    }

    let tc_url = format!("rtmp://{host}:{port}/{}", dest.app);
    let mut config = RtmpConfig::new(&dest.app, &dest.stream_key, &tc_url);
    if dest.timeout_ms > 0 {
        config.timeout = Some(Duration::from_millis(dest.timeout_ms));
    }
    Ok((format!("{host}:{port}"), config))
}

// ---------------------------------------------------------------------------
// Structured logging bridge
// ---------------------------------------------------------------------------

/// Log severity, mirroring the core's [`Level`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum LogLevel {
    /// The stream is broken or about to be.
    Error,
    /// Something bad but recoverable happened.
    Warn,
    /// Normal lifecycle transitions.
    Info,
    /// Diagnostics useful while developing.
    Debug,
    /// Per-message chatter.
    Trace,
}

impl From<Level> for LogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => Self::Error,
            Level::Warn => Self::Warn,
            Level::Debug => Self::Debug,
            Level::Trace => Self::Trace,
            // `Level` is non_exhaustive: Info and any future levels fold here.
            _ => Self::Info,
        }
    }
}

impl From<LogLevel> for Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Error => Self::Error,
            LogLevel::Warn => Self::Warn,
            LogLevel::Info => Self::Info,
            LogLevel::Debug => Self::Debug,
            LogLevel::Trace => Self::Trace,
        }
    }
}

/// Receives structured log records from the core, for forwarding to the
/// platform's logging system (`logcat`, `os_log`, crash reporters).
#[uniffi::export(with_foreign)]
pub trait LogSink: Send + Sync {
    /// One log record. `module` is the emitting module's path.
    fn on_log(&self, level: LogLevel, module: String, message: String);
}

/// Adapts a platform [`LogSink`] to the core's [`Logger`] trait.
struct LogBridge {
    sink: Arc<dyn LogSink>,
}

impl Logger for LogBridge {
    fn log(&self, record: &Record<'_>) {
        self.sink
            .on_log(record.level.into(), record.module.to_owned(), record.message.to_owned());
    }
}

/// Install (or remove, with `None`) the process-wide log sink.
#[uniffi::export]
pub fn set_log_sink(sink: Option<Arc<dyn LogSink>>) {
    telemetry::set_logger(sink.map(|sink| Arc::new(LogBridge { sink }) as Arc<dyn Logger>));
}

/// Set the highest [`LogLevel`] that reaches the sink (default: Info).
#[uniffi::export]
pub fn set_max_log_level(level: LogLevel) {
    telemetry::set_max_level(level.into());
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Lock a mutex, recovering from poisoning (a panicked worker must not wedge
/// the platform thread).
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn destination(url: &str, app: &str, key: &str) -> RtmpDestination {
        RtmpDestination {
            url: url.to_owned(),
            app: app.to_owned(),
            stream_key: key.to_owned(),
            timeout_ms: 0,
        }
    }

    #[test]
    fn destination_parsing_happy_path() {
        let (addr, config) = parse_destination(&destination("rtmp://example.tv:1936", "live", "key")).unwrap();
        assert_eq!(addr, "example.tv:1936");
        assert_eq!(config.app, "live");
        assert_eq!(config.key, "key");
        assert_eq!(config.tc_url, "rtmp://example.tv:1936/live");
        assert_eq!(config.timeout, Some(Duration::from_secs(10)));
    }

    #[test]
    fn destination_defaults_port_1935() {
        let (addr, config) = parse_destination(&destination("rtmp://example.tv", "live", "key")).unwrap();
        assert_eq!(addr, "example.tv:1935");
        assert_eq!(config.tc_url, "rtmp://example.tv:1935/live");
    }

    #[test]
    fn destination_timeout_override() {
        let mut dest = destination("rtmp://example.tv", "live", "key");
        dest.timeout_ms = 2500;
        let (_, config) = parse_destination(&dest).unwrap();
        assert_eq!(config.timeout, Some(Duration::from_millis(2500)));
    }

    #[test]
    fn destination_rejects_bad_input() {
        for dest in [
            destination("http://example.tv", "live", "key"),      // wrong scheme
            destination("rtmp://example.tv/live", "live", "key"), // app in URL
            destination("rtmp://", "live", "key"),                // empty host
            destination("rtmp://example.tv:abc", "live", "key"),  // bad port
            destination("rtmp://example.tv", "", "key"),          // empty app
            destination("rtmp://example.tv", "live", "  "),       // blank key
        ] {
            assert!(
                matches!(parse_destination(&dest), Err(StreamError::InvalidConfig { .. })),
                "must reject {:?}",
                dest.url
            );
        }
    }

    #[test]
    fn state_encoding_roundtrips() {
        for state in [
            SessionState::Idle,
            SessionState::Connecting,
            SessionState::Live,
            SessionState::Reconnecting,
            SessionState::Exhausted,
            SessionState::Stopped,
        ] {
            assert_eq!(SessionState::from_u8(state.as_u8()), state);
        }
    }

    #[test]
    fn latency_modes_map_to_profiles() {
        assert_eq!(
            LatencyProfile::from(LatencyMode::Aggressive),
            LatencyProfile::Aggressive
        );
        assert_eq!(LatencyProfile::from(LatencyMode::Balanced), LatencyProfile::Balanced);
        assert_eq!(LatencyProfile::from(LatencyMode::Lenient), LatencyProfile::Lenient);
    }

    #[test]
    fn default_config_is_production_ready() {
        let config = default_session_config(destination("rtmp://example.tv", "live", "key"), StreamInfo::default());
        assert_eq!(config.latency, LatencyMode::Balanced);
        assert_eq!(config.reconnect_max_attempts, None);
        assert!(config.reconnect_initial_delay_ms > 0);
        assert!(config.reconnect_max_delay_ms > config.reconnect_initial_delay_ms);
        assert!(config.pump_interval_ms > 0);
        assert!(config.stats_interval_ms >= 50);
    }

    #[test]
    fn stream_info_maps_to_stream_config() {
        let info = StreamInfo {
            width: 640,
            height: 360,
            framerate: 25.0,
            video_bitrate_bps: 900_000,
            audio_bitrate_bps: 64_000,
            audio_sample_rate_hz: 44_100,
        };
        let config = StreamConfig::from(&info);
        assert_eq!(config.width, 640);
        assert_eq!(config.height, 360);
        assert!((config.framerate - 25.0).abs() < f64::EPSILON);
        assert_eq!(config.video_bitrate, 900_000);
        assert_eq!(config.audio_bitrate, 64_000);
        assert_eq!(config.sample_rate, 44_100);
    }
}
