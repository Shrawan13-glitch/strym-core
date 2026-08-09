//! Bounded, drop-oldest buffer — the core's backpressure valve.
//!
//! The network may be slower than the encoder for a moment. We *never* let the
//! queue grow unbounded (that consumes RAM until the process dies). Instead,
//! when full we drop the oldest packet. Dropping old data reduces latency and
//! keeps memory flat — the two things a live streamer actually wants. The
//! `engine` decides which packets are sacrificial (keyframes are kept, so a
//! viewer can still resync).

use std::collections::VecDeque;

/// Generic bounded ring buffer that evicts the *oldest* item when full.
#[derive(Debug, Clone)]
pub struct BoundedBuffer<T> {
    queue: VecDeque<T>,
    capacity: usize,
    dropped: u64,
    pushed: u64,
}

impl<T> BoundedBuffer<T> {
    /// Create a buffer that holds at most `capacity` packets before evicting.
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            capacity,
            // Never be able to hold "nothing".
            dropped: 0,
            pushed: 0,
        }
    }

    /// Insert `item`. If the buffer is full, the oldest item is evicted and
    /// counted as dropped. The just-inserted item is always retained.
    pub fn push(&mut self, item: T) {
        self.pushed += 1;
        if self.queue.len() >= self.capacity {
            self.queue.pop_front();
            self.dropped += 1;
        }
        self.queue.push_back(item);
    }

    /// Pop the newest item (LIFO). Useful for draining the freshest frames when
    /// far behind — prefer `pop_freshest` over `pop_oldest` when in doubt about
    /// dropping too much.
    pub fn pop_freshest(&mut self) -> Option<T> {
        self.queue.pop_back()
    }

    /// Pop the oldest item (FIFO) — normal in-order delivery.
    pub fn pop_oldest(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    /// Return items to the **front** of the buffer, preserving their order.
    /// Used when a send fails partway through a batch: the unsent packets go
    /// back to the head of the line instead of being lost. Not counted as
    /// pushes; the caller only restores what it previously drained.
    pub fn restore_front(&mut self, items: impl IntoIterator<Item = T>) {
        let items: Vec<T> = items.into_iter().collect();
        for item in items.into_iter().rev() {
            self.queue.push_front(item);
        }
    }

    /// When behind, discard everything up to (but not including) the newest
    /// keyframe. The keyframe and all newer packets stay in the buffer, so a
    /// normal drain resumes from a clean decode point. Returns how many
    /// packets were discarded.
    pub fn resync_keep_latest_keyframe(&mut self, is_key: impl Fn(&T) -> bool) -> u64 {
        match self.queue.iter().rposition(is_key) {
            Some(idx) => {
                self.dropped += idx as u64;
                self.queue.drain(0..idx);
                idx as u64
            }
            None => 0,
        }
    }

    /// Peek at the oldest buffered packet, if any.
    pub fn front(&self) -> Option<&T> {
        self.queue.front()
    }

    /// Count one packet as dropped without touching the queue — for packets the
    /// engine discards during a drain (e.g. an inter-frame orphaned by a
    /// reconnect, when the buffer held no keyframe to resume from).
    pub fn record_drop(&mut self) {
        self.dropped += 1;
    }

    /// Number of packets currently held.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// True when no packets are buffered.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Peek at the newest buffered packet, if any.
    pub fn last(&self) -> Option<&T> {
        self.queue.back()
    }

    /// Total packets evicted since creation.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Total packets accepted since creation.
    pub fn pushed(&self) -> u64 {
        self.pushed
    }

    /// Maximum number of packets the buffer holds before evicting.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Fraction of pushed packets that were dropped (0.0..=1.0).
    pub fn drop_ratio(&self) -> f64 {
        if self.pushed == 0 {
            0.0
        } else {
            self.dropped as f64 / self.pushed as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evicts_oldest_when_full() {
        let mut b = BoundedBuffer::new(3);
        for i in 0..5 {
            b.push(i);
        }
        // capacity 3 => kept {2,3,4}, dropped [0,1]
        assert_eq!(b.len(), 3);
        assert_eq!(b.pop_oldest(), Some(2));
        assert_eq!(b.pop_oldest(), Some(3));
        assert_eq!(b.pop_oldest(), Some(4));
        assert_eq!(b.pop_oldest(), None);
        assert_eq!(b.dropped(), 2);
    }

    #[test]
    fn keep_keyframe_discards_intermediates() {
        let mut b = BoundedBuffer::new(10);
        // keyframes at 0,4,8
        for i in 0..9 {
            b.push(i);
        }
        let dropped = b.resync_keep_latest_keyframe(|n| n % 4 == 0);
        assert_eq!(dropped, 8); // 0..7 discarded
        assert_eq!(b.len(), 1); // only the keyframe 8 remains
        assert_eq!(b.pop_oldest(), Some(8));
        assert_eq!(b.pop_oldest(), None);
    }

    #[test]
    fn front_peeks_oldest_without_removing() {
        let mut b = BoundedBuffer::new(4);
        assert_eq!(b.front(), None);
        b.push(1);
        b.push(2);
        assert_eq!(b.front(), Some(&1));
        assert_eq!(b.len(), 2, "peek must not drain");
    }

    #[test]
    fn record_drop_counts_without_touching_queue() {
        let mut b = BoundedBuffer::new(4);
        b.push(1);
        b.push(2);
        b.record_drop();
        b.record_drop();
        assert_eq!(b.dropped(), 2);
        assert_eq!(b.len(), 2);
        assert_eq!(b.pop_oldest(), Some(1));
    }

    #[test]
    fn restore_front_returns_items_in_order() {
        let mut b = BoundedBuffer::new(8);
        for i in 0..4 {
            b.push(i);
        }
        // Drain, then hand back the unsent tail as a failed send would.
        let drained: Vec<u32> = std::iter::from_fn(|| b.pop_oldest()).collect();
        assert_eq!(drained, vec![0, 1, 2, 3]);
        b.restore_front(vec![1, 2, 3]);
        assert_eq!(b.len(), 3);
        assert_eq!(b.pop_oldest(), Some(1));
        assert_eq!(b.pop_oldest(), Some(2));
        assert_eq!(b.pop_oldest(), Some(3));
        // Restore is not a push: counters stay honest.
        assert_eq!(b.pushed(), 4);
        assert_eq!(b.dropped(), 0);
    }
}
