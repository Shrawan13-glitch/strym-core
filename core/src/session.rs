//! Session resilience — a dropped connection must not end the stream.
//!
//! The engine and transport move bytes; this module decides what happens when
//! they fail. On a disconnect (write error or a stalled transport) the session
//! re-dials through a [`Connector`] with capped exponential backoff + jitter,
//! re-attaches the fresh transport (the engine re-emits the FLV header,
//! metadata, and sequence headers the new connection has never seen, and
//! continues the timestamp series), and resumes from the newest keyframe so
//! viewers can resync at once.
//!
//! Reconnection is *cooperative*: [`Session::tick`] never sleeps. It attempts
//! a reconnect when the scheduled backoff has elapsed and otherwise returns
//! `Ok(0)`, so the platform's pump loop stays responsive and capture keeps
//! filling the bounded buffer while the connection is down.

use std::io;
use std::time::{Duration, Instant, SystemTime};

use crate::engine::{Engine, EngineConfig, EngineError};
use crate::models::MediaPacket;
use crate::telemetry::{Level, QosEvent};
use crate::transport::Transport;

/// A source of fresh transports — e.g. "dial the RTMP server and publish".
/// The session calls this for the initial connection and for every reconnect
/// attempt, so it must be cheap to retry and free of one-shot state.
pub trait Connector {
    /// The transport type produced.
    type Transport: Transport;

    /// Establish a new, ready-to-write transport.
    fn connect(&mut self) -> io::Result<Self::Transport>;
}

/// Any `FnMut() -> io::Result<T>` is a connector, for ad-hoc wiring.
impl<F, T> Connector for F
where
    F: FnMut() -> io::Result<T>,
    T: Transport,
{
    type Transport = T;

    fn connect(&mut self) -> io::Result<Self::Transport> {
        self()
    }
}

/// `SplitMix64` — a tiny deterministic PRNG for backoff jitter. Keeps the crate
/// dependency-free while making retry storms reproducible in tests (and
/// decorrelated between sessions in production).
struct Rng {
    state: u64,
}

impl Rng {
    fn seeded(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Seed from the wall clock; distinct sessions get distinct streams.
    fn from_time() -> Self {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        Self::seeded((nanos as u64) ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform `f64` in `[0, 1)`.
    fn next_f64(&mut self) -> f64 {
        // Top 53 bits → the full mantissa of an f64.
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// Reconnect schedule: exponential backoff with **equal jitter**, capped.
///
/// Delay for attempt *n* is drawn from `[base/2, base)` where
/// `base = min(max_delay, initial_delay * multiplier^(n-1))`. Equal jitter
/// keeps half the delay deterministic (no pathological zero-sleep retries,
/// which full jitter allows) while the random half still decorrelates
/// publishers retrying against the same server — the "thundering herd" that
/// fixed-interval reconnects cause on recovery.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Delay before the first scheduled retry (the very first attempt after a
    /// disconnect is immediate).
    pub initial_delay: Duration,
    /// Upper bound for any single delay.
    pub max_delay: Duration,
    /// Growth factor per attempt (values below 1.0 are treated as 1.0).
    pub multiplier: f64,
    /// Give up after this many consecutive failed attempts; `None` retries
    /// forever. Counters reset on a successful connection.
    pub max_attempts: Option<u32>,
    /// Fixed jitter seed for deterministic schedules (tests); `None` seeds
    /// from the wall clock.
    pub jitter_seed: Option<u64>,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(8),
            multiplier: 2.0,
            max_attempts: Some(12),
            jitter_seed: None,
        }
    }
}

impl ReconnectPolicy {
    /// Retry forever (still bounded per-attempt by `max_delay`).
    #[must_use]
    pub fn unlimited(mut self) -> Self {
        self.max_attempts = None;
        self
    }

    /// Deterministic schedule for tests.
    #[must_use]
    pub fn deterministic(mut self, seed: u64) -> Self {
        self.jitter_seed = Some(seed);
        self
    }

    /// The unjittered base delay for a 1-based attempt number.
    fn base_delay(&self, attempt: u32) -> Duration {
        let cap = self.max_delay.as_secs_f64();
        let mut secs = self.initial_delay.as_secs_f64().min(cap);
        let growth = self.multiplier.max(1.0);
        for _ in 1..attempt {
            secs = (secs * growth).min(cap);
            if secs >= cap {
                break;
            }
        }
        Duration::from_secs_f64(secs)
    }

    /// Jittered delay for a 1-based attempt number: uniform in `[base/2, base)`.
    fn delay(&self, attempt: u32, rng: &mut Rng) -> Duration {
        let base = self.base_delay(attempt);
        let half = base.as_secs_f64() / 2.0;
        Duration::from_secs_f64(half + half * rng.next_f64())
    }
}

/// Session-level resilience knobs.
#[derive(Debug, Clone)]
pub struct SessionPolicy {
    /// Backoff schedule for reconnect attempts.
    pub reconnect: ReconnectPolicy,
    /// Reconnect when the transport reports no forward progress for this long
    /// (a zombie connection the kernel keeps happily buffering for).
    /// Transports that never report progress are never stalled out.
    pub stall_timeout: Duration,
}

impl Default for SessionPolicy {
    fn default() -> Self {
        Self {
            reconnect: ReconnectPolicy::default(),
            stall_timeout: Duration::from_secs(10),
        }
    }
}

/// Where the session currently is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Created but [`start`](Session::start) was never called.
    Idle,
    /// Transport attached and streaming.
    Connected,
    /// Transport is down; reconnect attempts run on the backoff schedule.
    Reconnecting,
    /// The retry budget ran out. [`retry`](Session::retry) re-arms.
    Exhausted,
    /// [`finish`](Session::finish) ended the stream; terminal.
    Finished,
}

/// Errors surfaced by the session.
#[derive(Debug)]
pub enum SessionError {
    /// The engine failed while muxing/draining.
    Engine(EngineError),
    /// The initial [`start`](Session::start) could not connect at all.
    Connect(io::Error),
    /// Every reconnect attempt failed; carries the attempt count and the last
    /// failure's description.
    /// Every reconnect attempt failed.
    GiveUp {
        /// How many attempts were made.
        attempts: u32,
        /// Description of the final failure.
        last: String,
    },
    /// [`tick`](Session::tick) called before [`start`](Session::start).
    NotStarted,
    /// The session already [`finish`](Session::finish)ed.
    Finished,
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionError::Engine(e) => write!(f, "engine: {e}"),
            SessionError::Connect(e) => write!(f, "connect: {e}"),
            SessionError::GiveUp { attempts, last } => {
                write!(f, "gave up after {attempts} reconnect attempts: {last}")
            }
            SessionError::NotStarted => write!(f, "session not started"),
            SessionError::Finished => write!(f, "session finished"),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<EngineError> for SessionError {
    fn from(e: EngineError) -> Self {
        SessionError::Engine(e)
    }
}

/// A stream session: engine + connector + reconnect policy, kept alive across
/// failures. Drive it from the platform's pump loop: `push` packets in,
/// `tick()` regularly.
pub struct Session<C: Connector> {
    engine: Engine<C::Transport>,
    connector: C,
    policy: SessionPolicy,
    rng: Rng,
    state: SessionState,
    /// Failed attempts since the connection last succeeded.
    attempt: u32,
    /// When the next reconnect attempt is allowed to run.
    next_attempt_at: Option<Instant>,
    /// Successful reconnects since creation (telemetry).
    reconnects: u64,
    /// Description of the most recent failure (telemetry/UI).
    last_error: Option<String>,
}

impl<C: Connector> Session<C> {
    /// Build a session from stream config, a connector, and a resilience
    /// policy. Not connected until [`start`](Self::start).
    pub fn new(config: EngineConfig, connector: C, policy: SessionPolicy) -> Self {
        let rng = match policy.reconnect.jitter_seed {
            Some(seed) => Rng::seeded(seed),
            None => Rng::from_time(),
        };
        Self {
            engine: Engine::new(config),
            connector,
            policy,
            rng,
            state: SessionState::Idle,
            attempt: 0,
            next_attempt_at: None,
            reconnects: 0,
            last_error: None,
        }
    }

    /// Shared access to the engine (stats, resync, ...).
    pub fn engine(&self) -> &Engine<C::Transport> {
        &self.engine
    }

    /// Hand over codec configs; cached by the engine and re-emitted after
    /// every reconnect.
    pub fn configure_codecs(&self, avcc: Option<&[u8]>, asc: Option<&[u8]>) -> Result<(), EngineError> {
        self.engine.configure_codecs(avcc, asc)
    }

    /// Queue a packet. Works in every state — while the connection is down,
    /// the bounded buffer absorbs capture output (drop policy applies).
    pub fn push(&self, pkt: MediaPacket) -> Result<(), EngineError> {
        self.engine.push(pkt)
    }

    /// Queue a batch of packets.
    pub fn push_all(&self, pkts: impl IntoIterator<Item = MediaPacket>) -> Result<(), EngineError> {
        self.engine.push_all(pkts)
    }

    /// Establish the initial connection. Unlike reconnects this does not retry
    /// — a broadcaster whose very first connect fails should surface that to
    /// the user, not silently churn in the background.
    pub fn start(&mut self) -> Result<(), SessionError> {
        let transport = self.connector.connect().map_err(|e| {
            self.last_error = Some(e.to_string());
            crate::log_event!(
                Level::Error,
                "initial connect failed",
                "error" => e.to_string().as_str()
            );
            SessionError::Connect(e)
        })?;
        self.engine.attach_transport(transport);
        self.state = SessionState::Connected;
        crate::log_event!(Level::Info, "stream started");
        self.engine.qos().record_event(QosEvent::Started);
        self.engine.qos().record_event(QosEvent::Connected);
        Ok(())
    }

    /// One pump step: drain buffered media while connected; while down, run
    /// the reconnect schedule. Never sleeps. Returns how many packets were
    /// muxed this step.
    pub fn tick(&mut self) -> Result<usize, SessionError> {
        match self.state {
            SessionState::Idle => return Err(SessionError::NotStarted),
            SessionState::Finished => return Err(SessionError::Finished),
            SessionState::Exhausted => {
                return Err(SessionError::GiveUp {
                    attempts: self.attempt,
                    last: self.last_error.clone().unwrap_or_default(),
                });
            }
            SessionState::Connected => {
                if self.stalled() {
                    self.last_error = Some(format!("no transport progress for {:?}", self.policy.stall_timeout));
                    self.disconnect();
                } else {
                    match self.engine.tick() {
                        Ok(n) => return Ok(n),
                        Err(e) => {
                            self.last_error = Some(e.to_string());
                            self.disconnect();
                        }
                    }
                }
            }
            SessionState::Reconnecting => {}
        }
        self.reconnect_phase()
    }

    /// Drain and end the stream cleanly. Safe to call while disconnected (the
    /// remaining buffer is simply discarded with the dead transport).
    pub fn finish(&mut self) -> Result<(), SessionError> {
        if self.state == SessionState::Connected {
            self.engine.finish()?;
        }
        self.state = SessionState::Finished;
        crate::log_event!(Level::Info, "stream finished");
        self.engine.qos().record_event(QosEvent::Finished);
        Ok(())
    }

    /// Re-arm after [`SessionState::Exhausted`]: the backoff counter resets
    /// and attempts resume on the next `tick`. No-op in other states.
    pub fn retry(&mut self) {
        if self.state == SessionState::Exhausted {
            self.state = SessionState::Reconnecting;
            self.attempt = 0;
            self.next_attempt_at = None;
        }
    }

    /// Current lifecycle state.
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Successful reconnects so far (0 = the original connection still holds).
    pub fn reconnects(&self) -> u64 {
        self.reconnects
    }

    /// Failed attempts since the connection last succeeded.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Description of the most recent failure, if any.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// True when the transport has stopped making forward progress for longer
    /// than the stall timeout. Transports that report no progress (files,
    /// test sinks) can never stall.
    fn stalled(&self) -> bool {
        match self.engine.transport_progress() {
            Some(last) => last.elapsed() >= self.policy.stall_timeout,
            None => false,
        }
    }

    /// Enter the reconnecting state: drop the dead transport (the engine cuts
    /// the buffer to the newest keyframe and snapshots the timebase) and arm
    /// an immediate first attempt.
    fn disconnect(&mut self) {
        let error = self.last_error.clone();
        self.engine.detach_transport();
        self.state = SessionState::Reconnecting;
        self.attempt = 0;
        self.next_attempt_at = None;
        crate::log_event!(
            Level::Warn,
            "transport lost, reconnecting",
            "error" => error.as_deref().unwrap_or("unknown").to_string().as_str()
        );
        self.engine.qos().record_event(QosEvent::Disconnected { error });
    }

    /// Attempt reconnects whose backoff has elapsed. One attempt per call —
    /// a failed attempt schedules the next one and yields back to the pump.
    fn reconnect_phase(&mut self) -> Result<usize, SessionError> {
        if let Some(at) = self.next_attempt_at {
            if Instant::now() < at {
                return Ok(0);
            }
        }
        match self.connector.connect() {
            Ok(transport) => {
                self.engine.attach_transport(transport);
                self.state = SessionState::Connected;
                self.attempt = 0;
                self.next_attempt_at = None;
                self.reconnects += 1;
                crate::log_event!(
                    Level::Info,
                    "reconnected",
                    "reconnects" => self.reconnects
                );
                self.engine.qos().record_event(QosEvent::Reconnected);
                self.engine.qos().record_event(QosEvent::Connected);
                // Resume immediately: flush what the buffer kept (already cut
                // to the newest keyframe by the detach).
                self.engine.tick().map_err(SessionError::Engine)
            }
            Err(e) => {
                self.attempt += 1;
                self.last_error = Some(e.to_string());
                crate::log_event!(
                    Level::Warn,
                    "reconnect attempt failed",
                    "attempt" => self.attempt,
                    "error" => e.to_string().as_str()
                );
                self.engine.qos().record_event(QosEvent::ReconnectAttempt {
                    attempt: self.attempt,
                    error: Some(e.to_string()),
                });
                if let Some(max) = self.policy.reconnect.max_attempts {
                    if self.attempt >= max {
                        self.state = SessionState::Exhausted;
                        crate::log_event!(
                            Level::Error,
                            "gave up reconnecting",
                            "attempts" => self.attempt
                        );
                        self.engine.qos().record_event(QosEvent::GaveUp {
                            attempts: self.attempt,
                            error: Some(e.to_string()),
                        });
                        return Err(SessionError::GiveUp {
                            attempts: self.attempt,
                            last: e.to_string(),
                        });
                    }
                }
                let delay = self.policy.reconnect.delay(self.attempt, &mut self.rng);
                self.next_attempt_at = Some(Instant::now() + delay);
                Ok(0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially_and_caps() {
        let policy = ReconnectPolicy {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(800),
            multiplier: 2.0,
            ..ReconnectPolicy::default()
        };
        assert_eq!(policy.base_delay(1), Duration::from_millis(100));
        assert_eq!(policy.base_delay(2), Duration::from_millis(200));
        assert_eq!(policy.base_delay(3), Duration::from_millis(400));
        assert_eq!(policy.base_delay(4), Duration::from_millis(800));
        // Capped forever after.
        assert_eq!(policy.base_delay(9), Duration::from_millis(800));
    }

    #[test]
    fn jitter_stays_within_equal_jitter_band() {
        let policy = ReconnectPolicy {
            initial_delay: Duration::from_millis(100),
            jitter_seed: Some(42),
            ..Default::default()
        };
        let mut rng = Rng::seeded(42);
        for _ in 0..1000 {
            let d = policy.delay(1, &mut rng);
            assert!(d >= Duration::from_millis(50), "equal jitter keeps the floor: {d:?}");
            assert!(d < Duration::from_millis(100), "bounded by the base delay: {d:?}");
        }
    }

    #[test]
    fn jitter_seed_is_deterministic() {
        let policy = ReconnectPolicy::default().deterministic(7);
        let mut a = Rng::seeded(7);
        let mut b = Rng::seeded(7);
        for attempt in 1..=5 {
            assert_eq!(policy.delay(attempt, &mut a), policy.delay(attempt, &mut b));
        }
    }

    #[test]
    fn zero_initial_delay_stays_zero() {
        let policy = ReconnectPolicy {
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            ..Default::default()
        };
        let mut rng = Rng::seeded(1);
        for attempt in 1..=4 {
            assert_eq!(policy.delay(attempt, &mut rng), Duration::ZERO);
        }
    }

    #[test]
    fn rng_covers_unit_interval_without_hitting_one() {
        let mut rng = Rng::seeded(1234);
        let mut saw_high = false;
        for _ in 0..10_000 {
            let v = rng.next_f64();
            assert!((0.0..1.0).contains(&v), "unit interval: {v}");
            if v > 0.99 {
                saw_high = true;
            }
        }
        assert!(saw_high, "distribution actually spreads out");
    }
}

#[cfg(test)]
mod reconnect_tests {
    use super::*;
    use crate::models::{MediaKind, MediaPacket};
    use std::io;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    /// Minimal H.264 `AVCDecoderConfigurationRecord` (SPS 0x67, PPS 0x68).
    const AVCC: &[u8] = &[
        0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x03, 0x67, 0x42, 0x00, 0x0A, 0x01, 0x00, 0x03, 0x68, 0xCE,
    ];
    /// AAC-LC `AudioSpecificConfig`: 44.1 kHz stereo.
    const ASC: &[u8] = &[0x0A, 0x10];

    /// In-memory transport with a kill switch and a stall switch, so tests can
    /// simulate connection death and zombie connections without sockets.
    #[derive(Clone)]
    struct FakeTransport {
        log: Arc<Mutex<Vec<u8>>>,
        dead: Arc<AtomicBool>,
        frozen: Arc<AtomicBool>,
        progress: Arc<Mutex<Option<Instant>>>,
    }

    impl FakeTransport {
        fn new() -> Self {
            Self {
                log: Arc::new(Mutex::new(Vec::new())),
                dead: Arc::new(AtomicBool::new(false)),
                frozen: Arc::new(AtomicBool::new(false)),
                progress: Arc::new(Mutex::new(Some(Instant::now()))),
            }
        }

        fn kill(&self) {
            self.dead.store(true, Ordering::SeqCst);
        }

        /// Writes keep "succeeding" but progress reporting stops — a zombie.
        fn freeze_progress(&self) {
            self.frozen.store(true, Ordering::SeqCst);
        }

        fn force_stale_progress(&self, how_old: Duration) {
            *self.progress.lock().unwrap() = Instant::now().checked_sub(how_old);
        }

        fn bytes(&self) -> Vec<u8> {
            self.log.lock().unwrap().clone()
        }
    }

    impl io::Write for FakeTransport {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.dead.load(Ordering::SeqCst) {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "connection died"));
            }
            if !self.frozen.load(Ordering::SeqCst) {
                *self.progress.lock().unwrap() = Some(Instant::now());
            }
            self.log.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.dead.load(Ordering::SeqCst) {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "connection died"));
            }
            Ok(())
        }
    }

    impl Transport for FakeTransport {
        fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn last_progress(&self) -> Option<Instant> {
            *self.progress.lock().unwrap()
        }
    }

    /// Shared knobs for the fake network: which transports were created, and
    /// how many dials should fail next (tests arm this after `start`).
    #[derive(Clone, Default)]
    struct FakeNet {
        made: Arc<Mutex<Vec<FakeTransport>>>,
        failures: Arc<AtomicU32>,
    }

    impl FakeNet {
        /// A [`Connector`] over the blanket `FnMut` impl. Takes `self` by
        /// value (`FakeNet` is a cheap clone of `Arc` handles) so the returned
        /// connector is `'static`.
        fn connector(self) -> impl Connector<Transport = FakeTransport> {
            let net = self;
            move || -> io::Result<FakeTransport> {
                if net.failures.load(Ordering::SeqCst) > 0 {
                    net.failures.fetch_sub(1, Ordering::SeqCst);
                    return Err(io::Error::new(io::ErrorKind::ConnectionRefused, "forced dial failure"));
                }
                let t = FakeTransport::new();
                net.made.lock().unwrap().push(t.clone());
                Ok(t)
            }
        }

        fn arm_failures(&self, n: u32) {
            self.failures.store(n, Ordering::SeqCst);
        }

        fn transports(&self) -> Vec<FakeTransport> {
            self.made.lock().unwrap().clone()
        }
    }

    /// Zero-delay policy so tests never sleep; one attempt happens per `tick`.
    fn fast_policy(max_attempts: Option<u32>) -> SessionPolicy {
        SessionPolicy {
            reconnect: ReconnectPolicy {
                initial_delay: Duration::ZERO,
                max_delay: Duration::ZERO,
                multiplier: 2.0,
                max_attempts,
                jitter_seed: Some(1),
            },
            stall_timeout: Duration::from_mins(1),
        }
    }

    fn session_with(net: &FakeNet, policy: SessionPolicy) -> Session<impl Connector<Transport = FakeTransport>> {
        let session = Session::new(EngineConfig::default(), net.clone().connector(), policy);
        session.configure_codecs(Some(AVCC), Some(ASC)).unwrap();
        session
    }

    fn key(pts: i64) -> MediaPacket {
        MediaPacket {
            kind: MediaKind::Video,
            pts,
            dts: pts,
            is_key: true,
            data: vec![0, 0, 0, 1, 0x65, 0x88],
        }
    }

    fn inter(pts: i64) -> MediaPacket {
        MediaPacket {
            kind: MediaKind::Video,
            pts,
            dts: pts,
            is_key: false,
            data: vec![0, 0, 0, 1, 0x41, 0x77],
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

    /// Parse FLV bytes into `(tag_type, timestamp, body)` triples.
    fn parse_flv(bytes: &[u8]) -> Vec<(u8, u32, Vec<u8>)> {
        let mut out = Vec::new();
        let mut pos = 13; // skip the file header
        while pos + 11 <= bytes.len() {
            let kind = bytes[pos];
            let size = u32::from_be_bytes([0, bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]) as usize;
            let ts = u32::from_be_bytes([0, bytes[pos + 4], bytes[pos + 5], bytes[pos + 6]])
                | (u32::from(bytes[pos + 7]) << 24);
            let start = pos + 11;
            let end = start + size;
            if end + 4 > bytes.len() {
                break;
            }
            out.push((kind, ts, bytes[start..end].to_vec()));
            pos = end + 4;
        }
        out
    }

    #[test]
    fn reconnects_after_transport_death_and_reemits_headers() {
        let net = FakeNet::default();
        let mut session = session_with(&net, fast_policy(Some(3)));
        session.start().unwrap();
        assert_eq!(session.state(), SessionState::Connected);

        // Stream a little on the first connection.
        session
            .push_all([key(0), audio(0), inter(40), audio(46), inter(80)])
            .unwrap();
        session.tick().unwrap();
        let first = net.transports()[0].clone();
        assert!(!first.bytes().is_empty());

        // The connection dies; capture keeps producing.
        first.kill();
        session.push(key(120)).unwrap();

        // Zero backoff: the very next tick fails, dials, and resumes.
        session.tick().unwrap();
        assert_eq!(session.state(), SessionState::Connected);
        assert_eq!(session.reconnects(), 1);
        assert_eq!(net.transports().len(), 2);

        // The second connection starts with a fresh FLV header, metadata, and
        // both sequence headers — the new server knows nothing about the old.
        let second = net.transports()[1].clone();
        let bytes = second.bytes();
        assert_eq!(&bytes[..3], b"FLV", "re-emits the FLV header");
        let tags = parse_flv(&bytes);
        let kinds: Vec<u8> = tags.iter().map(|t| t.0).collect();
        assert_eq!(kinds[0], 18, "metadata first");
        assert_eq!(kinds[1], 9, "video sequence header");
        assert_eq!(tags[1].2[1], 0, "AVCPacketType=0");
        assert_eq!(kinds[2], 8, "audio sequence header");
        assert_eq!(tags[2].2[1], 0, "AACPacketType=0");

        // Media resumes from the newest keyframe at continued timestamps —
        // no restart at 0.
        let media: Vec<_> = tags.iter().skip(3).collect();
        assert!(!media.is_empty());
        let first = media[0];
        assert_eq!(first.0, 9, "resumes with video");
        assert_eq!(first.2[0], 0x17, "resumes on a keyframe");
        assert_eq!(first.1, 120, "timestamp continues, not reset");
        let ts_all: Vec<u32> = tags.iter().map(|t| t.1).collect();
        assert!(ts_all.windows(2).all(|w| w[0] <= w[1]), "monotonic: {ts_all:?}");
    }

    #[test]
    fn in_flight_batch_survives_a_dead_transport() {
        let net = FakeNet::default();
        let mut session = session_with(&net, fast_policy(Some(3)));
        session.start().unwrap();

        // Nothing drained yet: kill immediately, then push a batch. The next
        // tick drains the batch, fails on the dead transport, and must hand
        // the packets back instead of losing them.
        net.transports()[0].kill();
        session.push_all([key(0), inter(40)]).unwrap();
        session.tick().unwrap();

        let second = net.transports()[1].clone();
        let tags = parse_flv(&second.bytes());
        let video: Vec<_> = tags.iter().filter(|t| t.0 == 9).collect();
        assert_eq!(video.len(), 3, "seq header + key + inter all recovered");
    }

    #[test]
    fn mid_gop_death_resumes_only_on_next_keyframe() {
        let net = FakeNet::default();
        let mut session = session_with(&net, fast_policy(Some(3)));
        session.start().unwrap();

        // Stream a keyframe, then more frames; all are drained to the wire.
        session
            .push_all([key(0), inter(40), inter(80), inter(120), audio(46)])
            .unwrap();
        session.tick().unwrap();

        // The connection dies mid-GOP: the buffer now holds only inter-frames,
        // the newest keyframe already left. The resumed connection must not
        // start on an orphaned inter-frame.
        net.transports()[0].kill();
        session.push(inter(160)).unwrap();
        session.tick().unwrap();
        assert_eq!(session.reconnects(), 1);

        let second = net.transports()[1].clone();
        let tags = parse_flv(&second.bytes());
        let video_media: Vec<_> = tags.iter().filter(|t| t.0 == 9 && t.2.get(1) == Some(&1)).collect();
        assert!(
            video_media.is_empty(),
            "no orphaned inter-frames before a keyframe: {video_media:?}"
        );

        // The next keyframe the encoder produces is the resume point.
        session.push(key(1000)).unwrap();
        session.tick().unwrap();
        let tags = parse_flv(&second.bytes());
        let video_media: Vec<_> = tags.iter().filter(|t| t.0 == 9 && t.2.get(1) == Some(&1)).collect();
        assert_eq!(video_media.len(), 1, "resumes at the next keyframe");
        assert_eq!(video_media[0].2[0], 0x17, "frame type 1 keyframe");
        assert_eq!(video_media[0].1, 1000, "timestamp continues");
    }

    #[test]
    fn retries_until_the_connector_recovers() {
        let net = FakeNet::default();
        let mut session = session_with(&net, fast_policy(Some(5)));
        session.start().unwrap();

        net.transports()[0].kill();
        net.arm_failures(2);
        session.push(key(0)).unwrap();

        // Attempt 1: dial fails, session parks in Reconnecting.
        session.tick().unwrap();
        assert_eq!(session.state(), SessionState::Reconnecting);
        assert_eq!(session.attempt(), 1);

        // Attempt 2 fails too.
        session.tick().unwrap();
        assert_eq!(session.state(), SessionState::Reconnecting);
        assert_eq!(session.attempt(), 2);

        // Attempt 3 succeeds and drains.
        session.tick().unwrap();
        assert_eq!(session.state(), SessionState::Connected);
        assert_eq!(session.attempt(), 0);
        assert_eq!(session.reconnects(), 1);
        // Failed dials create no transports: start + one reconnect.
        assert_eq!(net.transports().len(), 2);
    }

    #[test]
    fn gives_up_after_capped_attempts_and_rearms_with_retry() {
        let net = FakeNet::default();
        let mut session = session_with(&net, fast_policy(Some(2)));
        session.start().unwrap();

        net.transports()[0].kill();
        net.arm_failures(u32::MAX);
        session.tick().unwrap(); // attempt 1 fails
        let err = session.tick().unwrap_err(); // attempt 2 hits the cap
        match err {
            SessionError::GiveUp { attempts, .. } => assert_eq!(attempts, 2),
            other => panic!("expected GiveUp, got {other:?}"),
        }
        assert_eq!(session.state(), SessionState::Exhausted);
        // Exhausted is sticky until re-armed.
        assert!(matches!(session.tick(), Err(SessionError::GiveUp { .. })));

        session.retry();
        assert_eq!(session.state(), SessionState::Reconnecting);
        assert_eq!(session.attempt(), 0);
        // Connector still down: attempts resume and hit the cap again.
        session.tick().unwrap();
        assert_eq!(session.attempt(), 1);
        assert!(matches!(session.tick(), Err(SessionError::GiveUp { .. })));
        assert_eq!(session.state(), SessionState::Exhausted);
    }

    #[test]
    fn stall_detection_reconnects_a_zombie_transport() {
        let net = FakeNet::default();
        let mut policy = fast_policy(Some(3));
        policy.stall_timeout = Duration::from_millis(50);
        let mut session = session_with(&net, policy);
        session.start().unwrap();

        session.push(key(0)).unwrap();
        session.tick().unwrap();

        // Zombie: writes "succeed" but no forward progress is reported.
        let zombie = net.transports()[0].clone();
        zombie.freeze_progress();
        zombie.force_stale_progress(Duration::from_secs(5));

        session.push(inter(40)).unwrap();
        session.tick().unwrap();
        assert_eq!(session.state(), SessionState::Connected, "reconnected in place");
        assert_eq!(session.reconnects(), 1);
        assert_eq!(net.transports().len(), 2);
        assert!(session.last_error().unwrap().contains("no transport progress"));
    }

    #[test]
    fn never_started_and_finished_states_are_terminal_for_tick() {
        let net = FakeNet::default();
        let mut session = session_with(&net, fast_policy(Some(3)));
        assert!(matches!(session.tick(), Err(SessionError::NotStarted)));

        session.start().unwrap();
        session.finish().unwrap();
        assert_eq!(session.state(), SessionState::Finished);
        assert!(matches!(session.tick(), Err(SessionError::Finished)));
    }

    #[test]
    fn start_fails_loudly_without_retries() {
        let net = FakeNet::default();
        net.arm_failures(1);
        let mut session = session_with(&net, fast_policy(Some(5)));
        assert!(matches!(session.start(), Err(SessionError::Connect(_))));
        assert_eq!(session.state(), SessionState::Idle);
        assert!(net.transports().is_empty());
    }

    #[test]
    fn push_works_while_disconnected() {
        let net = FakeNet::default();
        let mut session = session_with(&net, fast_policy(Some(5)));
        session.start().unwrap();

        net.transports()[0].kill();
        net.arm_failures(1);
        session.push(key(0)).unwrap();
        session.tick().unwrap(); // fails, dials (forced failure), parks
        assert_eq!(session.state(), SessionState::Reconnecting);

        // Capture keeps pushing while down; nothing is lost to errors.
        session.push_all([inter(40), audio(46)]).unwrap();
        session.tick().unwrap(); // reconnects and drains
        assert_eq!(session.state(), SessionState::Connected);
        let second = net.transports()[1].clone();
        let tags = parse_flv(&second.bytes());
        let video: Vec<_> = tags.iter().filter(|t| t.0 == 9 && t.2.get(1) == Some(&1)).collect();
        assert_eq!(video.len(), 2, "key + inter delivered after resume");
    }

    #[test]
    fn backoff_schedules_future_attempts() {
        // Nonzero delays: a failed attempt must park the session until the
        // scheduled time instead of hammering the server.
        let net = FakeNet::default();
        let policy = SessionPolicy {
            reconnect: ReconnectPolicy {
                initial_delay: Duration::from_millis(200),
                max_delay: Duration::from_secs(1),
                multiplier: 2.0,
                max_attempts: Some(5),
                jitter_seed: Some(3),
            },
            stall_timeout: Duration::from_mins(1),
        };
        let mut session = session_with(&net, policy);
        session.start().unwrap();
        net.transports()[0].kill();
        net.arm_failures(1);
        session.push(key(0)).unwrap();

        session.tick().unwrap(); // attempt 1 fails, schedules attempt 2
        assert_eq!(session.state(), SessionState::Reconnecting);
        // An immediate tick does not dial again: the backoff hasn't elapsed.
        session.tick().unwrap();
        assert_eq!(session.attempt(), 1, "no retry before the scheduled time");

        std::thread::sleep(Duration::from_millis(220));
        session.tick().unwrap();
        assert_eq!(session.attempt(), 0);
        assert_eq!(session.state(), SessionState::Connected);
    }
}
