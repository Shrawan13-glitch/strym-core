//! Transport abstraction. The muxer writes FLV bytes; this trait decides where
//! those bytes go. Today: file (tests, local dev). Tomorrow: RTMP, SRT, etc.
//!
//! Because everything upstream only depends on `Write + shutdown()`, adding a
//! new transport never touches the muxer or engine.

use std::io;
use std::time::{Duration, Instant};

/// Anything that can receive stream bytes. Implementations may buffer and push
/// in their own pacing (a network socket, a file, an in-memory sink).
pub trait Transport: io::Write {
    /// Gracefully end the stream. For a file this is a flush; for RTMP this is
    /// the "stream ended" handshake that tells the server to close the ingest.
    fn shutdown(&mut self) -> io::Result<()>;

    /// When the transport last made forward progress (bytes accepted on the
    /// wire, or a reply received from the peer). `None` means the transport
    /// does not report progress (files, test sinks) and can never "stall".
    /// The session uses this to detect zombie connections: writes that the
    /// kernel happily buffers but the peer never consumes.
    fn last_progress(&self) -> Option<Instant> {
        None
    }

    /// Total bytes written to the medium, transport framing included. Defaults
    /// to 0 for transports that don't count; the engine derives "effective
    /// throughput" from the delta between samples.
    fn bytes_written(&self) -> u64 {
        0
    }

    /// Latest measured round-trip time to the peer, when the transport can
    /// measure it (RTMP measures it from a ping exchange per connection).
    fn rtt(&self) -> Option<Duration> {
        None
    }

    /// Service the connection's inbound direction and run its own health
    /// checks. Called by the engine once per tick, *before* muxing, so a
    /// dead peer is discovered here rather than after more bytes were fed
    /// into a socket nobody is reading. An error is a transport failure like
    /// any write error: the session tears down and reconnects.
    ///
    /// This is what catches the half-open zombie: the kernel happily buffers
    /// local writes to a peer that vanished, so `write` keeps "succeeding"
    /// while nothing reaches the server. Only the inbound side (acknowledge-
    /// ments, pings) proves the peer is alive.
    fn maintain(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Writes bytes to a local file — the simplest transport, used by tests and
/// the demo to prove the pipeline produces a valid playable stream.
pub struct FileTransport<W: io::Write> {
    inner: W,
    bytes: u64,
}

impl<W: io::Write> FileTransport<W> {
    /// Wrap any writer (file, socket, in-memory buffer) as a transport.
    pub fn new(inner: W) -> Self {
        Self { inner, bytes: 0 }
    }

    /// Unwrap the underlying writer, useful after the stream ends (e.g. to
    /// read back the bytes an in-memory sink collected).
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: io::Write> io::Write for FileTransport<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.bytes = self.bytes.wrapping_add(written as u64);
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: io::Write> Transport for FileTransport<W> {
    fn shutdown(&mut self) -> io::Result<()> {
        self.inner.flush()
    }

    fn bytes_written(&self) -> u64 {
        self.bytes
    }
}

/// One slot in a [`Fanout`]: a downstream transport plus its health flag.
struct FanoutSlot {
    transport: Box<dyn Transport + Send>,
    alive: bool,
}

/// A transport that tees every byte to several downstream transports at once —
/// e.g. publishing to two endpoints simultaneously, or publishing while feeding
/// a raw FLV relay.
///
/// Semantics:
/// - Every write goes to **all** live sinks, in order, each consuming the full
///   buffer before the call returns, so all downstream byte streams are
///   identical even when individual writes are partial.
/// - The **first** sink added is the primary: its failures propagate to the
///   caller (so the session layer reconnects the publish path), while
///   secondary failures only retire that sink — a dead relay must not take the
///   primary stream down with it.
/// - A retired secondary is skipped forever after (no retry storm on a dead
///   endpoint); [`Fanout::alive`] reports how many sinks are still in service.
///
/// Note: a byte-level tee re-emits container headers on every reconnect, so a
/// *recording* that must stay one contiguous file belongs behind a packet-level
/// [`crate::sink::RecordingOutput`] instead.
pub struct Fanout {
    slots: Vec<FanoutSlot>,
}

impl Fanout {
    /// An empty fanout; add sinks with [`add`](Self::add). Writing before any
    /// sink is attached fails with `BrokenPipe`.
    pub fn new() -> Self {
        Self { slots: Vec::new() }
    }

    /// A fanout whose first (primary) sink is already attached.
    pub fn with_primary(transport: impl Transport + Send + 'static) -> Self {
        let mut fanout = Self::new();
        fanout.add(transport);
        fanout
    }

    /// Attach another downstream transport. The first one added is the
    /// primary; every later one is a best-effort secondary.
    pub fn add(&mut self, transport: impl Transport + Send + 'static) {
        self.slots.push(FanoutSlot {
            transport: Box::new(transport),
            alive: true,
        });
    }

    /// Number of sinks attached (live or retired).
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// True when no sink has been attached yet.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Number of sinks still in service (not retired by a failure).
    pub fn alive(&self) -> usize {
        self.slots.iter().filter(|s| s.alive).count()
    }

    /// Borrow the primary sink, if one was attached — lets callers query
    /// transport-specific state (e.g. an RTMP session id).
    pub fn primary(&self) -> Option<&(dyn Transport + Send)> {
        self.slots.first().map(|s| &*s.transport)
    }
}

impl Default for Fanout {
    fn default() -> Self {
        Self::new()
    }
}

impl io::Write for Fanout {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut primary_error = None;
        let mut any_live = false;
        for (idx, slot) in self.slots.iter_mut().enumerate() {
            if !slot.alive {
                continue;
            }
            match slot.transport.write_all(buf) {
                Ok(()) => {
                    let _ = slot.transport.flush();
                    any_live = true;
                }
                Err(e) => {
                    if idx == 0 {
                        primary_error = Some(e);
                    } else {
                        // Retire the secondary: log once, keep the primary streaming.
                        slot.alive = false;
                        crate::log_event!(
                            crate::telemetry::Level::Warn,
                            "fanout secondary retired",
                            "error" => e.to_string().as_str()
                        );
                    }
                    any_live = true;
                }
            }
        }
        if let Some(e) = primary_error {
            return Err(e);
        }
        if !any_live {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "fanout has no live sinks"));
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        for slot in &mut self.slots {
            if slot.alive {
                slot.transport.flush()?;
            }
        }
        Ok(())
    }
}

impl Transport for Fanout {
    fn maintain(&mut self) -> io::Result<()> {
        // Mirror the write semantics: the primary's health decides the
        // fanout's, secondaries retire quietly on their own failures.
        let mut primary_error = None;
        for (idx, slot) in self.slots.iter_mut().enumerate() {
            if !slot.alive {
                continue;
            }
            if let Err(e) = slot.transport.maintain() {
                if idx == 0 {
                    primary_error = Some(e);
                } else {
                    slot.alive = false;
                    crate::log_event!(
                        crate::telemetry::Level::Warn,
                        "fanout secondary retired",
                        "error" => e.to_string().as_str()
                    );
                }
            }
        }
        match primary_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn shutdown(&mut self) -> io::Result<()> {
        // Shut down every sink; the first failure is reported, the rest are
        // still closed so no endpoint is left hanging.
        let mut first_error = None;
        for slot in &mut self.slots {
            if let Err(e) = slot.transport.shutdown() {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn last_progress(&self) -> Option<Instant> {
        // Freshest progress across live sinks: the tee is only as stalled as
        // its most-recently-active endpoint.
        self.slots
            .iter()
            .filter(|s| s.alive)
            .filter_map(|s| s.transport.last_progress())
            .max()
    }

    fn bytes_written(&self) -> u64 {
        // The primary's counter drives throughput telemetry; counting secondaries
        // too would inflate the reported wire rate of the publish path.
        self.slots.first().map_or(0, |s| s.transport.bytes_written())
    }

    fn rtt(&self) -> Option<Duration> {
        self.slots.iter().find_map(|s| s.transport.rtt())
    }
}

#[cfg(test)]
mod fanout_tests {
    use super::*;
    use std::io::Write as _;
    use std::sync::{Arc, Mutex};

    /// In-memory sink that records bytes and can be killed on demand.
    #[derive(Clone, Default)]
    struct MemSink {
        bytes: Arc<Mutex<Vec<u8>>>,
        dead: Arc<Mutex<bool>>,
    }

    impl MemSink {
        fn kill(&self) {
            *self.dead.lock().unwrap() = true;
        }

        fn collected(&self) -> Vec<u8> {
            self.bytes.lock().unwrap().clone()
        }
    }

    impl io::Write for MemSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if *self.dead.lock().unwrap() {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "dead"));
            }
            self.bytes.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if *self.dead.lock().unwrap() {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "dead"));
            }
            Ok(())
        }
    }

    impl Transport for MemSink {
        fn shutdown(&mut self) -> io::Result<()> {
            Ok(())
        }

        fn bytes_written(&self) -> u64 {
            self.bytes.lock().unwrap().len() as u64
        }
    }

    #[test]
    fn tees_identical_bytes_to_every_sink() {
        let a = MemSink::default();
        let b = MemSink::default();
        let c = MemSink::default();
        let mut fan = Fanout::with_primary(a.clone());
        fan.add(b.clone());
        fan.add(c.clone());

        fan.write_all(b"hello ").unwrap();
        fan.write_all(b"world").unwrap();

        for sink in [&a, &b, &c] {
            assert_eq!(sink.collected(), b"hello world");
        }
        assert_eq!(fan.bytes_written(), 11, "telemetry follows the primary");
        assert_eq!(fan.alive(), 3);
    }

    #[test]
    fn secondary_failure_is_contained_and_retired() {
        let primary = MemSink::default();
        let flaky = MemSink::default();
        let mut fan = Fanout::with_primary(primary.clone());
        fan.add(flaky.clone());

        fan.write_all(b"one").unwrap();
        flaky.kill();
        // The secondary dies mid-stream: the write still succeeds...
        fan.write_all(b"two").unwrap();
        // ...the secondary saw only the first write...
        assert_eq!(flaky.collected(), b"one");
        // ...and is retired, while the primary keeps receiving everything.
        assert_eq!(fan.alive(), 1);
        fan.write_all(b"three").unwrap();
        assert_eq!(primary.collected(), b"onetwothree");
        assert_eq!(fan.alive(), 1, "a retired sink is never retried");
    }

    #[test]
    fn primary_failure_propagates() {
        let primary = MemSink::default();
        let secondary = MemSink::default();
        let mut fan = Fanout::with_primary(primary.clone());
        fan.add(secondary.clone());

        primary.kill();
        let err = fan.write_all(b"x").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        // The secondary still got the bytes: only the publish path fails.
        assert_eq!(secondary.collected(), b"x");
    }

    #[test]
    fn empty_fanout_refuses_writes() {
        let mut fan = Fanout::new();
        assert!(fan.is_empty());
        let err = fan.write_all(b"x").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn all_dead_fanout_refuses_writes() {
        let primary = MemSink::default();
        let mut fan = Fanout::with_primary(primary.clone());
        primary.kill();
        // Primary error takes precedence over the "no live sinks" report.
        assert!(fan.write_all(b"x").is_err());
        assert_eq!(fan.alive(), 1, "the primary is never retired, only reported");
    }

    #[test]
    fn progress_and_rtt_follow_live_sinks() {
        struct Reporting {
            progress: Option<Instant>,
            rtt: Option<Duration>,
        }
        impl io::Write for Reporting {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Ok(0)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl Transport for Reporting {
            fn shutdown(&mut self) -> io::Result<()> {
                Ok(())
            }
            fn last_progress(&self) -> Option<Instant> {
                self.progress
            }
            fn rtt(&self) -> Option<Duration> {
                self.rtt
            }
        }

        let older = Instant::now().checked_sub(Duration::from_secs(5)).unwrap();
        let newer = Instant::now();
        let mut fan = Fanout::with_primary(Reporting {
            progress: Some(older),
            rtt: None,
        });
        fan.add(Reporting {
            progress: Some(newer),
            rtt: Some(Duration::from_millis(12)),
        });
        assert_eq!(fan.last_progress(), Some(newer));
        assert_eq!(fan.rtt(), Some(Duration::from_millis(12)));
        assert!(Fanout::new().last_progress().is_none());
    }
}
