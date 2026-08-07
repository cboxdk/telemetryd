//! A small Bloom filter over a segment's exact-match key column.
//!
//! Exists for one query: fetch a trace by id. A trace id is a 32-hex string with no
//! useful ordering and cardinality equal to the row count, so neither Parquet
//! statistics nor the manifest's label index can prune on it — without this, "show me
//! this trace" reads every segment in the retention window.
//!
//! A false positive costs one segment read. A false negative would silently return an
//! incomplete trace, so the implementation is written so that it cannot produce one:
//! every key inserted always tests positive.

use std::path::Path;

use telemetryd_core::{Error, Result};

/// Target false-positive rate at the sizing below.
const BITS_PER_KEY: usize = 10;
/// `k = ln(2) * m/n`, rounded — 7 hashes for 10 bits/key gives ~1% false positives.
const HASHES: u32 = 7;
const MIN_BYTES: usize = 64;
/// Cap the filter so one enormous segment cannot produce a multi-megabyte sidecar.
const MAX_BYTES: usize = 1 << 20;

const FILE: &str = "keys.bloom";
const MAGIC: &[u8; 4] = b"TDBF";

#[derive(Debug, Clone)]
pub struct Bloom {
    bits: Vec<u8>,
    hashes: u32,
}

impl Bloom {
    /// Size a filter for an expected number of distinct keys.
    pub fn with_capacity(expected_keys: usize) -> Self {
        let bytes = (expected_keys * BITS_PER_KEY)
            .div_ceil(8)
            .clamp(MIN_BYTES, MAX_BYTES);
        Self {
            bits: vec![0; bytes],
            hashes: HASHES,
        }
    }

    pub fn insert(&mut self, key: &str) {
        let indices: Vec<usize> = self.indices(key).collect();
        for index in indices {
            self.bits[index / 8] |= 1 << (index % 8);
        }
    }

    /// `false` means definitely absent. `true` means probably present.
    pub fn may_contain(&self, key: &str) -> bool {
        self.indices(key)
            .all(|index| self.bits[index / 8] & (1 << (index % 8)) != 0)
    }

    /// Double hashing: two independent 64-bit hashes generate all `k` positions,
    /// which is standard and avoids computing seven separate digests per key.
    fn indices(&self, key: &str) -> impl Iterator<Item = usize> + '_ {
        let h1 = fnv1a(key.as_bytes(), 0xcbf2_9ce4_8422_2325);
        let h2 = fnv1a(key.as_bytes(), 0x9e37_79b9_7f4a_7c15) | 1;
        let bits = self.bits.len() * 8;

        (0..u64::from(self.hashes)).map(move |i| {
            let combined = h1.wrapping_add(i.wrapping_mul(h2));
            usize::try_from(combined % bits as u64).unwrap_or(0)
        })
    }

    pub fn write(&self, dir: &Path) -> Result<()> {
        let path = dir.join(FILE);
        let mut bytes = Vec::with_capacity(self.bits.len() + 8);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.hashes.to_le_bytes());
        bytes.extend_from_slice(&self.bits);
        std::fs::write(&path, &bytes)
            .map_err(|e| Error::io(format!("writing {}", path.display()), e))
    }

    /// Load a filter, or `None` when the segment has none.
    ///
    /// A missing or unreadable filter degrades to "scan this segment" rather than an
    /// error: the filter is an optimisation, and refusing to serve a query because a
    /// sidecar is damaged would turn a slow answer into no answer.
    pub fn read(dir: &Path) -> Option<Self> {
        let bytes = std::fs::read(dir.join(FILE)).ok()?;
        if bytes.len() < 8 || &bytes[..4] != MAGIC {
            return None;
        }
        let hashes = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if hashes == 0 || bytes.len() == 8 {
            return None;
        }
        Some(Self {
            bits: bytes[8..].to_vec(),
            hashes,
        })
    }

    pub fn size_bytes(&self) -> usize {
        self.bits.len()
    }
}

fn fnv1a(bytes: &[u8], seed: u64) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = seed;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn every_inserted_key_tests_positive() {
        // The property that matters: no false negatives, ever. One would silently
        // return an incomplete trace.
        let mut bloom = Bloom::with_capacity(1000);
        let keys: Vec<String> = (0..1000).map(|i| format!("{i:032x}")).collect();
        for key in &keys {
            bloom.insert(key);
        }
        for key in &keys {
            assert!(bloom.may_contain(key), "false negative for {key}");
        }
    }

    #[test]
    fn absent_keys_are_mostly_rejected() {
        let mut bloom = Bloom::with_capacity(1000);
        for i in 0..1000 {
            bloom.insert(&format!("{i:032x}"));
        }

        let misses = (10_000..11_000)
            .filter(|i| !bloom.may_contain(&format!("{i:032x}")))
            .count();
        // ~1% false positives at this sizing; anything under 90% rejection means the
        // filter is not doing its job.
        assert!(misses > 900, "only rejected {misses}/1000 absent keys");
    }

    #[test]
    fn an_empty_filter_rejects_everything() {
        let bloom = Bloom::with_capacity(10);
        assert!(!bloom.may_contain("anything"));
    }

    #[test]
    fn sizing_is_bounded_at_both_ends() {
        assert_eq!(Bloom::with_capacity(0).size_bytes(), MIN_BYTES);
        assert_eq!(Bloom::with_capacity(1).size_bytes(), MIN_BYTES);
        assert_eq!(
            Bloom::with_capacity(usize::MAX / 32).size_bytes(),
            MAX_BYTES
        );
        // A realistic segment stays small.
        assert!(Bloom::with_capacity(50_000).size_bytes() < 100_000);
    }

    #[test]
    fn filters_round_trip_through_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let mut bloom = Bloom::with_capacity(100);
        for i in 0..100 {
            bloom.insert(&format!("key-{i}"));
        }
        bloom.write(tmp.path()).unwrap();

        let loaded = Bloom::read(tmp.path()).unwrap();
        for i in 0..100 {
            assert!(loaded.may_contain(&format!("key-{i}")));
        }
        assert_eq!(loaded.size_bytes(), bloom.size_bytes());
    }

    #[test]
    fn a_missing_or_damaged_filter_degrades_to_none_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(Bloom::read(tmp.path()).is_none());

        std::fs::write(tmp.path().join(FILE), b"garbage").unwrap();
        assert!(Bloom::read(tmp.path()).is_none());

        std::fs::write(tmp.path().join(FILE), b"TDBF\x00\x00\x00\x00").unwrap();
        assert!(Bloom::read(tmp.path()).is_none());
    }
}
