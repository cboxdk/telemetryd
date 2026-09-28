//! Finding a label set's place in a query's table of series, without hashing the set.
//!
//! # Why this exists
//!
//! A query meets the same series again and again: once per segment, once per buffered
//! chunk, once per slice. Looking each meeting up by the label set itself hashes every
//! name and value in it — a histogram bucket's set is a dozen pairs and two hundred
//! bytes — and on a day of one app's request histogram that hashing was a third of the
//! time a query took, more than reading and folding the samples together.
//!
//! Label sets are shared: ingest keeps one copy per series in the buffer, and segments
//! intern theirs, so the same series nearly always arrives as the same allocation. Its
//! address is then an identity that costs one multiply to hash. The set itself is still
//! the key of record: an address seen for the first time is looked up by value, so two
//! allocations holding equal sets land on one series exactly as before.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use crate::record::Labels;

/// Hashes a `usize` identity — an address or a dense index — with one multiply.
///
/// For keys that are already unique numbers, never for anything a client chooses:
/// there is no seed, so it offers no protection against keys picked to collide.
#[derive(Debug, Default, Clone, Copy)]
pub struct IdHasher(u64);

impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 = (self.0.rotate_left(8) ^ u64::from(*byte)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }

    fn write_usize(&mut self, value: usize) {
        self.write_u64(value as u64);
    }

    fn write_u32(&mut self, value: u32) {
        self.write_u64(u64::from(value));
    }

    fn write_u64(&mut self, value: u64) {
        // Addresses are aligned, so their low bits are always zero, and a table picks
        // its bucket from the low bits of the hash. Folding the high half of the
        // product back down spreads them.
        let mixed = value.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 = mixed ^ (mixed >> 29);
    }
}

/// A map keyed by an identity number.
pub type IdMap<V> = HashMap<usize, V, BuildHasherDefault<IdHasher>>;

/// A query's series, numbered densely in the order they were first met.
#[derive(Debug, Default)]
pub struct SeriesTable {
    by_address: IdMap<usize>,
    by_value: HashMap<Labels, usize>,
    series: Vec<Labels>,
    /// Every allocation whose address is in `by_address`, held so that address cannot
    /// be freed and handed to a different set while this table still trusts it.
    pinned: Vec<Labels>,
}

impl SeriesTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Where `labels` is, if it has been met.
    #[must_use]
    pub fn get(&self, labels: &Labels) -> Option<usize> {
        self.by_address
            .get(&labels.storage_id())
            .copied()
            .or_else(|| self.by_value.get(labels).copied())
    }

    /// Where `labels` is, adding it if this is the first meeting. The flag says whether
    /// it was added.
    pub fn insert(&mut self, labels: &Labels) -> (usize, bool) {
        let address = labels.storage_id();
        if let Some(&index) = self.by_address.get(&address) {
            return (index, false);
        }
        let (index, added) = if let Some(&index) = self.by_value.get(labels) {
            (index, false)
        } else {
            let index = self.series.len();
            self.series.push(labels.clone());
            self.by_value.insert(labels.clone(), index);
            (index, true)
        };
        self.by_address.insert(address, index);
        self.pinned.push(labels.clone());
        (index, added)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.series.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    #[must_use]
    pub fn labels(&self, index: usize) -> &Labels {
        &self.series[index]
    }

    /// The series, in the order they were first met.
    #[must_use]
    pub fn into_series(self) -> Vec<Labels> {
        self.series
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn labelled(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    #[test]
    fn equal_sets_in_different_allocations_are_one_series() {
        let mut table = SeriesTable::new();
        let first = labelled(&[("__name__", "up"), ("job", "api")]);
        let second = labelled(&[("__name__", "up"), ("job", "api")]);
        assert!(!first.shares_storage_with(&second));

        assert_eq!(table.insert(&first), (0, true));
        assert_eq!(table.insert(&second), (0, false));
        assert_eq!(table.insert(&first.shared_with()), (0, false));
        assert_eq!(table.get(&second), Some(0));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn different_sets_are_different_series() {
        let mut table = SeriesTable::new();
        assert_eq!(table.insert(&labelled(&[("job", "a")])), (0, true));
        assert_eq!(table.insert(&labelled(&[("job", "b")])), (1, true));
        assert_eq!(table.get(&labelled(&[("job", "c")])), None);
        assert_eq!(
            table.into_series(),
            vec![labelled(&[("job", "a")]), labelled(&[("job", "b")])]
        );
    }

    /// An address the table trusts cannot be recycled under it: every allocation it
    /// keyed on is held, so dropping the caller's copy leaves the address taken.
    #[test]
    fn an_address_it_trusts_is_held() {
        let mut table = SeriesTable::new();
        let labels = labelled(&[("job", "a")]);
        let address = labels.storage_id();
        table.insert(&labels);
        drop(labels);
        for _ in 0..1000 {
            let other = labelled(&[("job", "b")]);
            assert_ne!(other.storage_id(), address);
        }
    }

    #[test]
    fn identity_hashes_spread_aligned_addresses() {
        use std::hash::{BuildHasher, BuildHasherDefault};
        let build = BuildHasherDefault::<IdHasher>::default();
        let buckets: std::collections::HashSet<u64> = (0..1024usize)
            .map(|i| build.hash_one(0x1000_0000 + i * 64) & 1023)
            .collect();
        assert!(
            buckets.len() > 600,
            "only {} of 1024 buckets used",
            buckets.len()
        );
    }
}
