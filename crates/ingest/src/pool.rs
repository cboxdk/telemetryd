//! The memory every ingest request in flight draws from together.
//!
//! # Why the queue depth was not enough
//!
//! `limits.ingest_queue_depth` bounds how many requests are in flight — 8,192 by
//! default — and `limits.max_decoded_bytes` how far one may expand. Neither bounds their
//! product. A request holds its body, the body decompressed, and the records decoded
//! from it; the last is the big one, because resource attributes are copied into every
//! record they describe, and 73 KB of JSON was measured decoding to 138 MB. Nothing
//! stopped a few dozen of those arriving together, and the body was read in full before
//! a request was even counted.
//!
//! So a request now reserves what it holds from one pool as it comes to hold it: the
//! body before it is read, the decompressed copy once it exists, the records a step at a
//! time as they are decoded. When the pool is empty the request is refused with a status
//! that says "retry" — `429` — and what it held goes back at once. The pool is never
//! smaller than the largest request the other limits allow, so a refusal is always a
//! wait, never a request that could not have fitted.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A fixed amount of memory, shared by every request drawing from it.
#[derive(Debug)]
pub struct MemoryPool {
    available: AtomicUsize,
    capacity: usize,
}

impl MemoryPool {
    #[must_use]
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            available: AtomicUsize::new(capacity),
            capacity,
        })
    }

    /// Everything the pool holds, taken or not.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// What requests in flight hold between them now.
    #[must_use]
    pub fn in_use(&self) -> usize {
        self.capacity
            .saturating_sub(self.available.load(Ordering::Relaxed))
    }

    /// Take `bytes`, or `None` when the pool does not have them.
    #[must_use]
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Option<Reservation> {
        let mut reservation = Reservation {
            pool: Arc::clone(self),
            bytes: 0,
        };
        reservation.grow(bytes).then_some(reservation)
    }

    fn take(&self, bytes: usize) -> bool {
        self.available
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |available| {
                available.checked_sub(bytes)
            })
            .is_ok()
    }
}

/// Memory taken from a [`MemoryPool`], given back when this is dropped.
#[derive(Debug)]
pub struct Reservation {
    pool: Arc<MemoryPool>,
    bytes: usize,
}

impl Reservation {
    /// Take `bytes` more, or leave the reservation as it was and say so.
    pub fn grow(&mut self, bytes: usize) -> bool {
        if !self.pool.take(bytes) {
            return false;
        }
        self.bytes = self.bytes.saturating_add(bytes);
        true
    }

    /// What this reservation holds.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.pool.available.fetch_add(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn what_is_reserved_comes_back() {
        let pool = MemoryPool::new(100);
        let mut first = pool.reserve(60).unwrap();
        assert!(pool.reserve(50).is_none(), "only forty left");
        assert!(!first.grow(50));
        assert_eq!(first.bytes(), 60, "a failed grow takes nothing");
        assert!(first.grow(40));
        assert_eq!(pool.in_use(), 100);
        drop(first);
        assert_eq!(pool.in_use(), 0);
        assert!(pool.reserve(100).is_some());
    }
}
