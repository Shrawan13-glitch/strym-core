//! A single monotonic clock so every timestamp in the pipeline uses the same
//! timebase. If the platform supplies its own pts (it usually does), `Clock` is
//! still used to measure *elapsed* session time and buffer latency.

use std::time::Instant;

/// Monotonic clock producing millisecond timestamps. Never goes backwards, is
/// unaffected by wall-clock changes (NTP adjustments, user setting the time).
#[derive(Debug, Clone)]
pub struct Clock {
    origin: Instant,
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock {
    /// Create a clock whose epoch starts at creation time.
    pub fn new() -> Self {
        Self { origin: Instant::now() }
    }

    /// Milliseconds elapsed since the clock was created.
    pub fn now_ms(&self) -> i64 {
        self.origin.elapsed().as_millis() as i64
    }

    /// True when `media_pts` is "older" than `real_ms` — the rough definition
    /// of current latency: how far behind the live edge the buffer has drifted.
    pub fn latency_ms(&self, media_pts: i64) -> i64 {
        self.now_ms().saturating_sub(media_pts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_and_forward() {
        let c = Clock::new();
        let a = c.now_ms();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let b = c.now_ms();
        assert!(b >= a);
    }
}
