//! One copy of each distinct label set, shared by every segment that holds it.
//!
//! # Why this exists
//!
//! Every sealed segment carries a dictionary of the streams it holds, and the same stream
//! appears in every segment covering the period it was written to. A store with 500
//! segments and 20,000 active series therefore held up to ten million `Labels`, almost all
//! of them duplicates of twenty thousand distinct ones.
//!
//! That is the term that makes resident memory scale with how much is *stored* rather than
//! with how much is running, and nothing else bounds it: it grows with
//! `storage.disk_budget` and retention, not with the machine. Measured on a 133-segment
//! store, it was 161 MB at rest with nothing being queried and nothing ingested.
//!
//! `Labels` holds its map behind an `Arc`, so sharing costs a pointer. The dictionary
//! entries stop being copies and become references to one canonical set.
//!
//! # What this deliberately does not do
//!
//! It does not free an entry when the last segment holding it is deleted. Doing so needs
//! weak references through `Labels`'s private interior, and the payoff is small: the table
//! holds one entry per *distinct* label set, which is the same order as the series limit
//! the store is already sized for. [`CAPACITY`] stops it growing without bound in the case
//! that would matter — an instance churning through label sets faster than retention
//! removes them — by simply declining to intern beyond it. Declining costs sharing, never
//! correctness.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use telemetryd_core::Labels;

/// The most distinct label sets kept for sharing.
///
/// Above the largest series ceiling the derivation will pick, so a store within its own
/// limits always shares fully. An instance past it keeps working with less sharing rather
/// than trading one unbounded table for another.
const CAPACITY: usize = 250_000;

fn table() -> &'static Mutex<HashMap<u64, Labels>> {
    static TABLE: OnceLock<Mutex<HashMap<u64, Labels>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Return a `Labels` that shares its map with every equal set already seen.
///
/// The fingerprint is a 64-bit hash, so equality is confirmed before sharing — two
/// different label sets that collide must not become each other. A collision simply
/// declines to share.
#[must_use]
pub fn shared(labels: Labels) -> Labels {
    let fingerprint = labels.fingerprint();
    let mut table = table()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    if let Some(canonical) = table.get(&fingerprint) {
        if *canonical == labels {
            return canonical.shared_with();
        }
        return labels;
    }
    if table.len() >= CAPACITY {
        return labels;
    }
    table.insert(fingerprint, labels.shared_with());
    labels
}

/// How many distinct label sets are being shared. For `/status` and for tests.
#[must_use]
pub fn distinct() -> usize {
    table()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .len()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn labels(n: usize) -> Labels {
        let mut labels = Labels::new();
        labels.insert("app", "checkout");
        labels.insert("route", format!("/r/{n}"));
        labels
    }

    /// The point of the whole module: two equal sets end up backed by one map.
    #[test]
    fn equal_sets_end_up_sharing_one_map() {
        let first = shared(labels(1));
        let second = shared(labels(1));
        assert_eq!(first, second);

        // Mutating one must not be visible through the other, or sharing would have
        // turned a copy into an alias.
        let mut third = second.clone();
        third.insert("extra", "value");
        assert!(first.get("extra").is_none());
        assert_eq!(third.get("extra"), Some("value"));
    }

    /// The saving itself, asserted rather than assumed: equal sets must end up backed by
    /// one allocation, or this module is an expensive way to copy things.
    #[test]
    fn sharing_actually_shares_the_allocation() {
        let first = shared(labels(41));
        let second = shared(labels(41));
        assert!(
            first.shares_storage_with(&second),
            "two equal label sets must share one map"
        );

        // Copy-on-write means the moment one is written to, they part company.
        let mut third = second.clone();
        third.insert("extra", "value");
        assert!(!first.shares_storage_with(&third));
    }

    #[test]
    fn different_sets_are_kept_apart() {
        let a = shared(labels(1));
        let b = shared(labels(2));
        assert_ne!(a, b);
        assert_eq!(a.get("route"), Some("/r/1"));
        assert_eq!(b.get("route"), Some("/r/2"));
    }
}
