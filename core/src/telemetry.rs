//! Observability — structured logging, live metrics, and a `QoS` event stream.
//!
//! The crate stays dependency-free (a documented contract), so instead of
//! pulling in `tracing` this module ships a small, std-only telemetry facade:
//!
//! - **Structured logging**: a [`Logger`] trait plus a process-global registry.
//!   The platform installs its own sink (logcat, `os_log`, a console) and sets
//!   the maximum [`Level`]; every log call checks that filter before doing any
//!   work, so a disabled path costs one atomic load.
//! - **Metrics**: the engine records bytes out, effective throughput, drop
//!   ratio, buffer lag, RTT and reconnect counters; periodic [`QosSample`]s are
//!   computed from deltas between ticks.
//! - **`QoS` event stream**: [`Qos`] keeps a bounded, pre-allocated ring of
//!   samples (no allocation in the hot path once warm) plus two callback sinks —
//!   one for periodic samples and one for lifecycle events — for platform UI
//!   wiring. A [`QosSummary`] turns the retained ring into a queryable report.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};

/// Longest error string retained in `QoS` state (bounded memory, not a log sink).
const MAX_ERROR_LEN: usize = 512;

// ---------------------------------------------------------------------------
// Structured logging
// ---------------------------------------------------------------------------

/// Severity of a log record, ordered so `Error < Warn < Info < Debug < Trace`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Level {
    /// The stream is broken or about to be (connect failed, gave up reconnecting).
    Error = 1,
    /// Something bad but recoverable happened (a drop cut, a clock rebase).
    Warn = 2,
    /// Normal lifecycle transitions (started, reconnected, finished).
    Info = 3,
    /// Diagnostics useful while developing.
    Debug = 4,
    /// Per-message chatter; almost always off in production.
    Trace = 5,
}

impl Level {
    /// The conventional lowercase label, for log-line formatting.
    pub const fn as_str(self) -> &'static str {
        match self {
            Level::Error => "error",
            Level::Warn => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
            Level::Trace => "trace",
        }
    }
}

/// A typed value carried by a [`Field`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FieldValue<'a> {
    /// Signed integer.
    Int(i64),
    /// Unsigned integer.
    Uint(u64),
    /// Floating point.
    Float(f64),
    /// Boolean.
    Bool(bool),
    /// Borrowed string.
    Str(&'a str),
}

impl From<i8> for FieldValue<'_> {
    fn from(v: i8) -> Self {
        FieldValue::Int(v as i64)
    }
}

impl From<i16> for FieldValue<'_> {
    fn from(v: i16) -> Self {
        FieldValue::Int(v as i64)
    }
}

impl From<i32> for FieldValue<'_> {
    fn from(v: i32) -> Self {
        FieldValue::Int(v as i64)
    }
}

impl From<i64> for FieldValue<'_> {
    fn from(v: i64) -> Self {
        FieldValue::Int(v)
    }
}

impl From<isize> for FieldValue<'_> {
    fn from(v: isize) -> Self {
        FieldValue::Int(v as i64)
    }
}

impl From<u8> for FieldValue<'_> {
    fn from(v: u8) -> Self {
        FieldValue::Uint(v as u64)
    }
}

impl From<u16> for FieldValue<'_> {
    fn from(v: u16) -> Self {
        FieldValue::Uint(v as u64)
    }
}

impl From<u32> for FieldValue<'_> {
    fn from(v: u32) -> Self {
        FieldValue::Uint(v as u64)
    }
}

impl From<u64> for FieldValue<'_> {
    fn from(v: u64) -> Self {
        FieldValue::Uint(v)
    }
}

impl From<usize> for FieldValue<'_> {
    fn from(v: usize) -> Self {
        FieldValue::Uint(v as u64)
    }
}

impl From<f32> for FieldValue<'_> {
    fn from(v: f32) -> Self {
        FieldValue::Float(f64::from(v))
    }
}

impl From<f64> for FieldValue<'_> {
    fn from(v: f64) -> Self {
        FieldValue::Float(v)
    }
}

impl From<bool> for FieldValue<'_> {
    fn from(v: bool) -> Self {
        FieldValue::Bool(v)
    }
}

impl<'a> From<&'a str> for FieldValue<'a> {
    fn from(v: &'a str) -> Self {
        FieldValue::Str(v)
    }
}

impl<'a> From<&'a String> for FieldValue<'a> {
    fn from(v: &'a String) -> Self {
        FieldValue::Str(v.as_str())
    }
}

/// One structured key/value pair on a log record.
#[derive(Debug, Clone, Copy)]
pub struct Field<'a> {
    /// Attribute name.
    pub key: &'a str,
    /// Attribute value.
    pub value: FieldValue<'a>,
}

/// Build a [`Field`] from anything `Into<FieldValue>` (ints, floats, bools, `&str`).
pub fn field<'a, V>(key: &'a str, value: V) -> Field<'a>
where
    V: Into<FieldValue<'a>>,
{
    Field {
        key,
        value: value.into(),
    }
}

/// A fully-formed log event handed to a [`Logger`].
#[derive(Debug, Clone, Copy)]
pub struct Record<'a> {
    /// Severity.
    pub level: Level,
    /// `module_path!()` of the emitting code.
    pub module: &'a str,
    /// Human-readable event description.
    pub message: &'a str,
    /// Structured attributes.
    pub fields: &'a [Field<'a>],
}

/// Receives structured log records. Implementations forward to whatever logging
/// system the host platform owns (Android `logcat`, Apple `os_log`, a console).
pub trait Logger: Send + Sync {
    /// Emit one record.
    fn log(&self, record: &Record<'_>);
}

/// Process-wide logger installed by the platform. `None` (the default) is a
/// no-op that costs nothing on the emit path.
static LOGGER: Mutex<Option<Arc<dyn Logger>>> = Mutex::new(None);

/// Highest level allowed to reach the logger, checked before formatting.
static MAX_LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);

/// Install (or remove) the process-wide [`Logger`].
pub fn set_logger(logger: Option<Arc<dyn Logger>>) {
    *LOGGER.lock().unwrap_or_else(PoisonError::into_inner) = logger;
}

/// Set the highest [`Level`] that reaches the logger; the platform controls this.
pub fn set_max_level(level: Level) {
    MAX_LEVEL.store(level as u8, Ordering::Relaxed);
}

/// The level currently allowed through the filter.
pub fn max_level() -> Level {
    match MAX_LEVEL.load(Ordering::Relaxed) {
        1 => Level::Error,
        2 => Level::Warn,
        3 => Level::Info,
        4 => Level::Debug,
        _ => Level::Trace,
    }
}

/// True when a record at `level` would reach the logger — cheap, hot-path safe.
pub fn enabled(level: Level) -> bool {
    level as u8 <= MAX_LEVEL.load(Ordering::Relaxed)
}

/// Emit a structured record. No-op when the level is filtered or no logger is
/// installed. Field values are borrowed; nothing is formatted unless emitted.
pub fn log(level: Level, module: &str, message: &str, fields: &[Field<'_>]) {
    if !enabled(level) {
        return;
    }
    if let Some(logger) = LOGGER.lock().unwrap_or_else(PoisonError::into_inner).as_deref() {
        logger.log(&Record {
            level,
            module,
            message,
            fields,
        });
    }
}

/// Structured logging shorthand. Guards on `enabled` before building any fields,
/// so values are only evaluated when the record would actually be emitted.
///
/// ```text
/// log_event!(Level::Warn, "cut to live edge", "dropped" => 12);
/// ```
#[macro_export]
macro_rules! log_event {
    ($level:expr, $message:expr $(, $key:expr => $value:expr)* $(,)?) => {{
        if $crate::telemetry::enabled($level) {
            $crate::telemetry::log(
                $level,
                module_path!(),
                $message,
                &[$($crate::telemetry::field($key, $value)),*],
            );
        }
    }};
}

// ---------------------------------------------------------------------------
// Metrics & QoS
// ---------------------------------------------------------------------------

/// Telemetry collection knobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QosConfig {
    /// How often the engine emits a [`QosSample`] while streaming.
    pub interval: Duration,
    /// Ring capacity in samples; the collector keeps the most recent ones.
    pub capacity: usize,
}

impl Default for QosConfig {
    fn default() -> Self {
        Self {
            // One sample per second covers a 1-hour stream with headroom in the
            // default 4096-slot ring.
            interval: Duration::from_secs(1),
            capacity: 4096,
        }
    }
}

/// One periodic snapshot of stream health. Rates are measured between the
/// previous sample and this one; counters are cumulative since creation.
#[derive(Debug, Clone, Default)]
pub struct QosSample {
    /// Wall-clock milliseconds at sample time (Unix epoch).
    pub wall_ms: u64,
    /// Milliseconds since the engine (and its clock) was created.
    pub uptime_ms: u64,
    /// Media bitrate out in bits/s (FLV bytes handed to the transport).
    pub bitrate_out_bps: f64,
    /// Effective wire throughput in bits/s, transport framing included.
    pub throughput_bps: f64,
    /// Fraction of pushed packets dropped since creation (0.0..=1.0).
    pub drop_ratio: f64,
    /// Current buffer lag in ms (newest buffered PTS vs the live edge).
    pub buffer_ms: i64,
    /// Packets currently sitting in the backpressure buffer.
    pub buffered_count: usize,
    /// Latest measured round-trip time to the peer, when the transport reports it.
    pub rtt_ms: Option<f64>,
    /// Successful reconnects since creation.
    pub reconnects: u64,
    /// Failed reconnect attempts since creation.
    pub reconnect_attempts: u64,
    /// Packets accepted from the platform since creation.
    pub pushed: u64,
    /// Packets muxed to the transport since creation.
    pub muxed: u64,
    /// Packets dropped since creation.
    pub dropped: u64,
    /// Description of the most recent failure, if any.
    pub last_error: Option<String>,
}

/// A discrete lifecycle event, delivered to the event sink as it happens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QosEvent {
    /// `Session::start` connected for the first time.
    Started,
    /// A transport is attached and streaming (also after each reconnect).
    Connected,
    /// The current transport died; reconnection begins.
    Disconnected {
        /// Why it died.
        error: Option<String>,
    },
    /// One reconnect attempt failed.
    ReconnectAttempt {
        /// Which attempt in the current backoff run.
        attempt: u32,
        /// Why the dial failed.
        error: Option<String>,
    },
    /// A reconnect succeeded.
    Reconnected,
    /// The retry budget ran out.
    GaveUp {
        /// Attempts made before giving up.
        attempts: u32,
        /// Description of the final failure.
        error: Option<String>,
    },
    /// The stream ended cleanly.
    Finished,
}

/// Queryable summary derived from the retained [`QosSample`] ring.
#[derive(Debug, Clone, PartialEq)]
pub struct QosSummary {
    /// How many samples the summary is based on.
    pub samples: usize,
    /// Wall time spanned by the retained samples, seconds.
    pub span_secs: f64,
    /// Mean media bitrate out, bits/s.
    pub avg_bitrate_out_bps: f64,
    /// Peak media bitrate out, bits/s.
    pub peak_bitrate_out_bps: f64,
    /// Mean effective wire throughput, bits/s.
    pub avg_throughput_bps: f64,
    /// Peak effective wire throughput, bits/s.
    pub peak_throughput_bps: f64,
    /// Mean buffer lag, ms.
    pub avg_buffer_ms: f64,
    /// Worst buffer lag, ms.
    pub max_buffer_ms: i64,
    /// Cumulative drop ratio (dropped / pushed).
    pub drop_ratio: f64,
    /// Mean measured RTT, when the transport reports it.
    pub avg_rtt_ms: Option<f64>,
    /// Worst measured RTT.
    pub max_rtt_ms: Option<f64>,
    /// Successful reconnects since creation.
    pub reconnects: u64,
    /// Failed reconnect attempts since creation.
    pub reconnect_attempts: u64,
    /// Packets pushed since creation.
    pub pushed: u64,
    /// Packets muxed since creation.
    pub muxed: u64,
    /// Packets dropped since creation.
    pub dropped: u64,
    /// Most recent failure description, if any.
    pub last_error: Option<String>,
}

/// Bounded ring of [`QosSample`]s. Grows to `capacity` on first use, then wraps
/// in place — steady-state pushes are a copy into a pre-allocated slot, never an
/// allocation.
#[derive(Debug, Clone)]
struct QosRing {
    samples: Vec<QosSample>,
    capacity: usize,
    next: usize,
    filled: usize,
}

impl QosRing {
    fn new(capacity: usize) -> Self {
        Self {
            samples: Vec::new(),
            capacity: capacity.max(1),
            next: 0,
            filled: 0,
        }
    }

    fn push(&mut self, sample: QosSample) {
        if self.filled < self.capacity {
            self.samples.push(sample);
            self.filled += 1;
            self.next = self.filled % self.capacity;
        } else {
            self.samples[self.next] = sample;
            self.next = (self.next + 1) % self.capacity;
        }
    }

    fn len(&self) -> usize {
        self.filled
    }

    fn clear(&mut self) {
        self.samples.clear();
        self.next = 0;
        self.filled = 0;
    }

    /// Samples in chronological order, oldest first.
    fn iter(&self) -> impl Iterator<Item = &QosSample> {
        let start = if self.filled < self.capacity { 0 } else { self.next };
        let capacity = self.capacity;
        let len = self.filled;
        (0..len).map(move |i| &self.samples[(start + i) % capacity])
    }
}

struct QosInner {
    ring: QosRing,
    sample_sink: Option<Box<dyn FnMut(QosSample) + Send>>,
    event_sink: Option<Box<dyn FnMut(QosEvent) + Send>>,
}

/// The collector the platform reads from. Shared via `Arc` between the engine
/// (which writes) and the host (which queries); every callback is invoked with
/// no lock held, so a sink that re-enters the collector cannot deadlock.
pub struct Qos {
    inner: Mutex<QosInner>,
    reconnects: AtomicU64,
    reconnect_attempts: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl Qos {
    /// Create an empty collector with the given capacity (samples retained).
    pub fn new(config: QosConfig) -> Self {
        Self {
            inner: Mutex::new(QosInner {
                ring: QosRing::new(config.capacity),
                sample_sink: None,
                event_sink: None,
            }),
            reconnects: AtomicU64::new(0),
            reconnect_attempts: AtomicU64::new(0),
            last_error: Mutex::new(None),
        }
    }

    fn lock(&self) -> MutexGuard<'_, QosInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Install (or remove) the periodic-sample sink for the platform UI.
    pub fn set_sink(&self, sink: Option<Box<dyn FnMut(QosSample) + Send>>) {
        self.lock().sample_sink = sink;
    }

    /// Install (or remove) the lifecycle-event sink for the platform UI.
    pub fn set_event_sink(&self, sink: Option<Box<dyn FnMut(QosEvent) + Send>>) {
        self.lock().event_sink = sink;
    }

    /// Record one periodic sample: appended to the ring and handed to the sink.
    /// The sink is invoked with the collector unlocked, so re-entrancy is safe.
    pub fn emit_sample(&self, sample: QosSample) {
        let sink = {
            let mut inner = self.lock();
            inner.ring.push(sample.clone());
            inner.sample_sink.take()
        };
        if let Some(mut sink) = sink {
            sink(sample);
            let mut inner = self.lock();
            if inner.sample_sink.is_none() {
                inner.sample_sink = Some(sink);
            }
        }
    }

    /// Record a lifecycle event: updates the cumulative counters and forwards
    /// the event to the sink (invoked with the collector unlocked).
    pub fn record_event(&self, event: QosEvent) {
        match &event {
            QosEvent::Reconnected => {
                self.reconnects.fetch_add(1, Ordering::Relaxed);
            }
            QosEvent::ReconnectAttempt { error, .. } => {
                self.reconnect_attempts.fetch_add(1, Ordering::Relaxed);
                if let Some(error) = error {
                    self.set_last_error(error);
                }
            }
            QosEvent::Disconnected { error } | QosEvent::GaveUp { error, .. } => {
                if let Some(error) = error {
                    self.set_last_error(error);
                }
            }
            QosEvent::Started | QosEvent::Connected | QosEvent::Finished => {}
        }
        let sink = self.lock().event_sink.take();
        if let Some(mut sink) = sink {
            sink(event);
            let mut inner = self.lock();
            if inner.event_sink.is_none() {
                inner.event_sink = Some(sink);
            }
        }
    }

    /// Number of samples currently retained.
    pub fn sample_count(&self) -> usize {
        self.lock().ring.len()
    }

    /// The retained samples in chronological order, oldest first.
    pub fn samples(&self) -> Vec<QosSample> {
        self.lock().ring.iter().cloned().collect()
    }

    /// Successful reconnects since creation.
    pub fn reconnects(&self) -> u64 {
        self.reconnects.load(Ordering::Relaxed)
    }

    /// Failed reconnect attempts since creation.
    pub fn reconnect_attempts(&self) -> u64 {
        self.reconnect_attempts.load(Ordering::Relaxed)
    }

    /// Description of the most recent failure, if any.
    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// A queryable summary over the retained ring — the "1-hour stream report".
    pub fn summary(&self) -> QosSummary {
        let samples = self.samples();
        let n = samples.len();
        let mut avg_bitrate = 0.0f64;
        let mut peak_bitrate = 0.0f64;
        let mut avg_throughput = 0.0f64;
        let mut peak_throughput = 0.0f64;
        let mut avg_buffer = 0.0f64;
        let mut max_buffer = 0i64;
        let mut rtt_sum = 0.0f64;
        let mut rtt_count = 0u32;
        let mut max_rtt = 0.0f64;
        for sample in &samples {
            avg_bitrate += sample.bitrate_out_bps;
            peak_bitrate = peak_bitrate.max(sample.bitrate_out_bps);
            avg_throughput += sample.throughput_bps;
            peak_throughput = peak_throughput.max(sample.throughput_bps);
            avg_buffer += sample.buffer_ms as f64;
            max_buffer = max_buffer.max(sample.buffer_ms);
            if let Some(rtt) = sample.rtt_ms {
                rtt_sum += rtt;
                rtt_count += 1;
                max_rtt = max_rtt.max(rtt);
            }
        }
        let count = n as f64;
        let mean = |total: f64| if count > 0.0 { total / count } else { 0.0 };
        let span_secs = if n >= 2 {
            samples[n - 1].uptime_ms.saturating_sub(samples[0].uptime_ms) as f64 / 1000.0
        } else {
            0.0
        };
        let last = samples.last();
        QosSummary {
            samples: n,
            span_secs,
            avg_bitrate_out_bps: mean(avg_bitrate),
            peak_bitrate_out_bps: peak_bitrate,
            avg_throughput_bps: mean(avg_throughput),
            peak_throughput_bps: peak_throughput,
            avg_buffer_ms: mean(avg_buffer),
            max_buffer_ms: max_buffer,
            drop_ratio: last.map_or(0.0, |s| s.drop_ratio),
            avg_rtt_ms: (rtt_count > 0).then(|| rtt_sum / f64::from(rtt_count)),
            max_rtt_ms: (rtt_count > 0).then_some(max_rtt),
            reconnects: self.reconnects(),
            reconnect_attempts: self.reconnect_attempts(),
            pushed: last.map_or(0, |s| s.pushed),
            muxed: last.map_or(0, |s| s.muxed),
            dropped: last.map_or(0, |s| s.dropped),
            last_error: self.last_error(),
        }
    }

    /// Clear all samples, counters, and the last error. Sinks are kept (they are
    /// platform configuration, not stream state).
    pub fn reset(&self) {
        self.lock().ring.clear();
        self.reconnects.store(0, Ordering::Relaxed);
        self.reconnect_attempts.store(0, Ordering::Relaxed);
        *self.last_error.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }

    fn set_last_error(&self, error: &str) {
        let capped = error.chars().take(MAX_ERROR_LEN).collect::<String>();
        *self.last_error.lock().unwrap_or_else(PoisonError::into_inner) = Some(capped);
    }
}

/// A minimal [`Logger`] that prints records to stderr — enough for demos and
/// local debugging, and a template for platform sinks.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConsoleLogger;

impl Logger for ConsoleLogger {
    fn log(&self, record: &Record<'_>) {
        eprintln!("[{}] {}: {}", record.level.as_str(), record.module, record.message);
        for field in record.fields {
            eprintln!("    {} = {:?}", field.key, field.value);
        }
    }
}

/// Milliseconds since the Unix epoch, saturating at 0 on weird clocks.
pub(crate) fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A captured log record: `(level, module, message, fields)`.
    type Captured = (Level, String, String, Vec<(String, String)>);

    /// Records every log record as `(level, module, message, fields)`.
    #[derive(Default)]
    struct Capture {
        records: Mutex<Vec<Captured>>,
    }

    impl Logger for Capture {
        fn log(&self, record: &Record<'_>) {
            let fields = record
                .fields
                .iter()
                .map(|f| (f.key.to_owned(), format!("{:?}", f.value)))
                .collect();
            self.records.lock().unwrap_or_else(PoisonError::into_inner).push((
                record.level,
                record.module.to_owned(),
                record.message.to_owned(),
                fields,
            ));
        }
    }

    fn records(capture: &Capture) -> Vec<Captured> {
        capture.records.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Assert two floats are equal to within an epsilon (exact equality on f64
    /// is fragile and clippy-rejected).
    fn assert_close(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "expected {a} ≈ {b}");
    }

    #[test]
    fn max_level_filters_emission() {
        let capture = Arc::new(Capture::default());
        set_max_level(Level::Warn);
        set_logger(Some(capture.clone()));
        log_event!(Level::Info, "silenced-info");
        log_event!(Level::Warn, "kept-warn", "attempt" => 2);
        log_event!(Level::Error, "kept-error");
        set_logger(None);
        let recs = records(&capture);
        assert!(recs.iter().all(|(_, _, m, _)| m != "silenced-info"));
        assert!(recs.iter().any(|(_, _, m, _)| m == "kept-warn"));
        assert!(recs.iter().any(|(_, _, m, _)| m == "kept-error"));
        let warn = recs.iter().find(|(_, _, m, _)| m == "kept-warn").unwrap();
        assert!(warn.3.iter().any(|(k, v)| k == "attempt" && v == "Int(2)"));
    }

    #[test]
    fn disabled_levels_skip_work_entirely() {
        let capture = Arc::new(Capture::default());
        set_max_level(Level::Info);
        set_logger(Some(capture.clone()));
        log_event!(Level::Trace, "trace-silenced", "expensive" => "not evaluated");
        log_event!(Level::Info, "info-kept");
        set_logger(None);
        let recs = records(&capture);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].2, "info-kept");
    }

    #[test]
    fn ring_keeps_newest_within_capacity() {
        let mut ring = QosRing::new(3);
        assert_eq!(ring.len(), 0);
        for i in 0..5 {
            ring.push(QosSample {
                uptime_ms: i,
                ..QosSample::default()
            });
        }
        let kept: Vec<u64> = ring.iter().map(|s| s.uptime_ms).collect();
        assert_eq!(kept, vec![2, 3, 4], "oldest dropped, newest kept in order");
    }

    #[test]
    fn ring_handles_underfilled_and_single_slot() {
        let mut ring = QosRing::new(4);
        for i in 0..2 {
            ring.push(QosSample {
                uptime_ms: i,
                ..QosSample::default()
            });
        }
        assert_eq!(ring.len(), 2);
        assert_eq!(ring.iter().count(), 2);

        let mut one = QosRing::new(1);
        one.push(QosSample {
            uptime_ms: 1,
            ..QosSample::default()
        });
        one.push(QosSample {
            uptime_ms: 2,
            ..QosSample::default()
        });
        assert_eq!(one.iter().map(|s| s.uptime_ms).collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn summary_computes_avg_and_peaks() {
        let q = Qos::new(QosConfig {
            capacity: 16,
            ..Default::default()
        });
        for i in 1..=4u64 {
            q.emit_sample(QosSample {
                uptime_ms: i * 1000,
                bitrate_out_bps: i as f64 * 1000.0,
                throughput_bps: i as f64 * 2000.0,
                buffer_ms: i64::try_from(i * 10).unwrap(),
                rtt_ms: Some(i as f64),
                pushed: 100,
                muxed: 100 - i,
                dropped: i,
                ..Default::default()
            });
        }
        let s = q.summary();
        assert_eq!(s.samples, 4);
        assert_close(s.avg_bitrate_out_bps, 2500.0);
        assert_close(s.peak_bitrate_out_bps, 4000.0);
        assert_close(s.avg_throughput_bps, 5000.0);
        assert_close(s.peak_throughput_bps, 8000.0);
        assert_close(s.avg_buffer_ms, 25.0);
        assert_eq!(s.max_buffer_ms, 40);
        assert_eq!(s.avg_rtt_ms, Some(2.5));
        assert_eq!(s.max_rtt_ms, Some(4.0));
        assert_eq!(s.dropped, 4);
        assert!(s.span_secs >= 3.0);
    }

    #[test]
    fn empty_summary_is_zeroed() {
        let q = Qos::new(QosConfig::default());
        let s = q.summary();
        assert_eq!(s.samples, 0);
        assert_close(s.avg_bitrate_out_bps, 0.0);
        assert_eq!(s.avg_rtt_ms, None);
        assert_eq!(s.max_buffer_ms, 0);
        assert_eq!(s.last_error, None);
    }

    #[test]
    fn events_update_counters_and_dispatch() {
        let q = Qos::new(QosConfig::default());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let pushed = seen.clone();
        q.set_event_sink(Some(Box::new(move |event| {
            pushed.lock().unwrap_or_else(PoisonError::into_inner).push(event);
        })));
        q.record_event(QosEvent::Started);
        q.record_event(QosEvent::Connected);
        q.record_event(QosEvent::Disconnected {
            error: Some("died".into()),
        });
        q.record_event(QosEvent::ReconnectAttempt {
            attempt: 1,
            error: Some("refused".into()),
        });
        q.record_event(QosEvent::Reconnected);
        assert_eq!(q.reconnects(), 1);
        assert_eq!(q.reconnect_attempts(), 1);
        assert_eq!(q.last_error().as_deref(), Some("refused"));
        assert_eq!(seen.lock().unwrap().len(), 5);
        q.reset();
        assert_eq!(q.reconnects(), 0);
        assert_eq!(q.reconnect_attempts(), 0);
        assert_eq!(q.last_error(), None);
    }

    #[test]
    fn sink_dispatch_is_reentrant_safe() {
        let q = Arc::new(Qos::new(QosConfig::default()));
        let reentrant = q.clone();
        q.set_sink(Some(Box::new(move |_sample| {
            // Re-entering the collector from the sink must not deadlock or lose
            // the sample; the sink slot is released while it runs.
            reentrant.emit_sample(QosSample::default());
        })));
        q.emit_sample(QosSample::default());
        q.emit_sample(QosSample::default());
        // 2 outer samples + 2 re-entrant samples, all retained.
        assert_eq!(q.sample_count(), 4);
    }
}
