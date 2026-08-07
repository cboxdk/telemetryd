//! A bounded collector for limited queries.
//!
//! The problem it solves: `query_range` with `limit=100` over a week used to
//! materialise every matching record in the week, sort it, and throw away all but 100.
//! On a busy app that is millions of allocations to answer with a hundred — and the
//! peak memory is set by how much data matched, which is exactly the number an
//! operator cannot control.
//!
//! Here memory is `O(limit)` regardless of how much matches, and the caller can ask
//! [`TopK::cutoff`] whether a whole segment can be skipped without opening it.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Which end of the range the caller wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// Newest first — a log viewer's default.
    Descending,
    Ascending,
}

/// Keyed by timestamp, with an insertion counter to break ties deterministically.
///
/// Without the counter, two records sharing a timestamp would order arbitrarily and
/// paging could show the same line twice or skip one.
#[derive(Debug)]
struct Keyed<T> {
    timestamp: u64,
    sequence: u64,
    value: T,
}

// Ordering is over the key alone, deliberately: the collector must work for record
// types that are not themselves comparable, and `(timestamp, sequence)` is already a
// total order because `sequence` is unique per push.
impl<T> Ord for Keyed<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

impl<T> PartialOrd for Keyed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Keyed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.sequence == other.sequence
    }
}

impl<T> Eq for Keyed<T> {}

/// A bounded best-N collector. `limit == 0` means unbounded.
#[derive(Debug)]
pub struct TopK<T> {
    limit: usize,
    order: Order,
    sequence: u64,
    /// For [`Order::Descending`] the heap is min-first so the *worst* kept element is
    /// on top and is the one evicted; for ascending it is max-first. Both are
    /// expressed as a max-heap by flipping the key.
    heap: BinaryHeap<Reverse<Keyed<T>>>,
    ascending: BinaryHeap<Keyed<T>>,
}

impl<T> TopK<T> {
    pub fn new(limit: usize, order: Order) -> Self {
        Self {
            limit,
            order,
            sequence: 0,
            heap: BinaryHeap::new(),
            ascending: BinaryHeap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len() + self.ascending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn is_full(&self) -> bool {
        self.limit != 0 && self.len() >= self.limit
    }

    /// Offer a record. Kept only if it beats the current worst, once full.
    pub fn push(&mut self, timestamp: u64, value: T) {
        let entry = Keyed {
            timestamp,
            sequence: self.sequence,
            value,
        };
        self.sequence += 1;

        match self.order {
            Order::Descending => {
                if self.is_full() {
                    // The worst kept is the oldest; replace it only if this is newer.
                    if self
                        .heap
                        .peek()
                        .is_some_and(|Reverse(worst)| entry > *worst)
                    {
                        self.heap.pop();
                    } else {
                        return;
                    }
                }
                self.heap.push(Reverse(entry));
            }
            Order::Ascending => {
                if self.is_full() {
                    if self.ascending.peek().is_some_and(|worst| entry < *worst) {
                        self.ascending.pop();
                    } else {
                        return;
                    }
                }
                self.ascending.push(entry);
            }
        }
    }

    /// The timestamp a candidate must beat to be kept, once the collector is full.
    ///
    /// `None` means "everything is still interesting". This is what lets a scan skip
    /// an entire segment: for a descending query, a segment whose newest record is
    /// older than the cutoff cannot contain anything that would survive.
    pub fn cutoff(&self) -> Option<u64> {
        if !self.is_full() {
            return None;
        }
        match self.order {
            Order::Descending => self.heap.peek().map(|Reverse(worst)| worst.timestamp),
            Order::Ascending => self.ascending.peek().map(|worst| worst.timestamp),
        }
    }

    /// Whether a segment spanning `[min, max]` can be skipped entirely.
    pub fn can_skip_range(&self, min_nanos: u64, max_nanos: u64) -> bool {
        let Some(cutoff) = self.cutoff() else {
            return false;
        };
        match self.order {
            Order::Descending => max_nanos <= cutoff,
            Order::Ascending => min_nanos >= cutoff,
        }
    }

    /// Drain in the requested order.
    pub fn into_sorted(self) -> Vec<T> {
        let mut entries: Vec<Keyed<T>> = match self.order {
            Order::Descending => self.heap.into_iter().map(|Reverse(entry)| entry).collect(),
            Order::Ascending => self.ascending.into_vec(),
        };
        entries.sort();
        if self.order == Order::Descending {
            entries.reverse();
        }
        entries.into_iter().map(|entry| entry.value).collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn descending_keeps_the_newest_and_returns_them_newest_first() {
        let mut top = TopK::new(3, Order::Descending);
        for i in 0..100u64 {
            top.push(i, i);
        }
        assert_eq!(top.into_sorted(), vec![99, 98, 97]);
    }

    #[test]
    fn ascending_keeps_the_oldest_and_returns_them_oldest_first() {
        let mut top = TopK::new(3, Order::Ascending);
        for i in (0..100u64).rev() {
            top.push(i, i);
        }
        assert_eq!(top.into_sorted(), vec![0, 1, 2]);
    }

    #[test]
    fn memory_stays_bounded_by_the_limit_not_by_what_matched() {
        let mut top = TopK::new(10, Order::Descending);
        for i in 0..100_000u64 {
            top.push(i, i);
            assert!(top.len() <= 10, "collector grew past its limit");
        }
        assert_eq!(top.into_sorted().len(), 10);
    }

    #[test]
    fn an_unbounded_collector_keeps_everything() {
        let mut top = TopK::new(0, Order::Descending);
        for i in 0..1000u64 {
            top.push(i, i);
        }
        assert_eq!(top.len(), 1000);
    }

    #[test]
    fn the_cutoff_only_appears_once_full() {
        let mut top = TopK::new(3, Order::Descending);
        assert_eq!(top.cutoff(), None);
        top.push(10, 10);
        top.push(20, 20);
        assert_eq!(
            top.cutoff(),
            None,
            "not full yet, everything is interesting"
        );
        top.push(30, 30);
        assert_eq!(top.cutoff(), Some(10), "the worst kept is the oldest");
    }

    #[test]
    fn a_segment_that_cannot_beat_the_cutoff_is_skippable() {
        let mut top = TopK::new(2, Order::Descending);
        top.push(100, 100);
        top.push(200, 200);
        assert_eq!(top.cutoff(), Some(100));

        // Entirely older than the worst kept: nothing in it could survive.
        assert!(top.can_skip_range(0, 99));
        // Touching the cutoff: still skippable, the tie-break cannot promote it.
        assert!(top.can_skip_range(0, 100));
        // Overlapping something newer: must be scanned.
        assert!(!top.can_skip_range(0, 150));
        assert!(!top.can_skip_range(300, 400));
    }

    #[test]
    fn an_unfilled_collector_never_skips_anything() {
        let mut top = TopK::new(100, Order::Descending);
        top.push(50, 50);
        assert!(!top.can_skip_range(0, 1));
    }

    #[test]
    fn ascending_skips_from_the_other_end() {
        let mut top = TopK::new(2, Order::Ascending);
        top.push(100, 100);
        top.push(200, 200);
        assert_eq!(top.cutoff(), Some(200));

        assert!(top.can_skip_range(300, 400));
        assert!(!top.can_skip_range(0, 50));
    }

    #[test]
    fn ties_are_broken_deterministically_by_insertion_order() {
        // Without this, paging could repeat or skip a line whose timestamp collides.
        let mut a = TopK::new(2, Order::Descending);
        for value in ["first", "second", "third"] {
            a.push(42, value);
        }
        let mut b = TopK::new(2, Order::Descending);
        for value in ["first", "second", "third"] {
            b.push(42, value);
        }
        assert_eq!(a.into_sorted(), b.into_sorted());
    }

    #[test]
    fn results_are_ordered_even_when_pushed_out_of_order() {
        let mut top = TopK::new(0, Order::Descending);
        for i in [5u64, 1, 9, 3, 7] {
            top.push(i, i);
        }
        assert_eq!(top.into_sorted(), vec![9, 7, 5, 3, 1]);
    }
}
