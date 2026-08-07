//! A per-segment index over the trigrams of a record's text, for substring pruning.
//!
//! `|= "..."` in LogQL is a **substring** match, and that rules out the obvious index.
//! A token index would answer wrongly: `TimeoutError` occurs inside `MyTimeoutError`
//! as a substring but not as a token, so a token filter would report the segment as
//! not containing it and the line would silently vanish from the results.
//!
//! Trigrams do not have that problem. If a document contains a pattern as a substring,
//! it contains every three-character window of that pattern. So "every trigram of the
//! pattern is present" is a *necessary* condition, and its negation — any trigram
//! missing — is a sound reason to skip the segment without reading it.
//!
//! Why this exists at all: ADR-001 D1 set a falsifiable trigger for building a text
//! index, and measurement fired it by a factor of a hundred. A line filter matching
//! nothing costs about ten seconds per gigabyte scanned, because nothing fills the
//! collector, so no segment can be skipped and every body has to be decompressed.
//!
//! **A false positive costs one segment read. A false negative loses data.** Every
//! decision here is made in the direction of the former:
//!
//! - patterns shorter than three characters have no trigrams, so nothing is pruned
//! - the filter saturates rather than growing without bound, and a saturated filter
//!   answers "maybe" to everything
//! - a filter that fails to load is absent, and an absent filter prunes nothing

use std::path::Path;

use telemetryd_core::{Error, Result};

/// Bits per distinct trigram. Lower than the key filter's ten: there are far more
/// trigrams than trace ids, and a false positive here is cheap — one segment read that
/// finds nothing, which is what happens today for every segment.
const BITS_PER_TRIGRAM: usize = 6;
/// `k = ln(2) * m/n` for 6 bits/trigram, rounded down.
const HASHES: u32 = 4;
const MIN_BYTES: usize = 256;
/// Printable ASCII gives about 850k possible trigrams; real text uses a small fraction.
/// This caps the sidecar at a quarter of a megabyte per segment.
const MAX_BYTES: usize = 256 * 1024;

const FILE: &str = "text.bloom";
const MAGIC: &[u8; 4] = b"TDTG";

/// The shortest pattern that has a trigram. Shorter filters cannot prune.
pub const MIN_PATTERN: usize = 3;

#[derive(Debug, Clone)]
pub struct TrigramIndex {
    bits: Vec<u8>,
    hashes: u32,
}

impl TrigramIndex {
    #[must_use]
    pub fn with_capacity(expected_records: usize) -> Self {
        // Distinct trigrams grow far more slowly than record count — natural text
        // exhausts its alphabet quickly — so size against a saturating estimate rather
        // than against rows, which would allocate megabytes for no benefit.
        let expected_trigrams = expected_records.saturating_mul(4).clamp(1024, 300_000);
        let bytes = (expected_trigrams * BITS_PER_TRIGRAM)
            .div_ceil(8)
            .clamp(MIN_BYTES, MAX_BYTES);
        Self {
            bits: vec![0; bytes],
            hashes: HASHES,
        }
    }

    /// Index every three-byte window of `text`.
    pub fn insert(&mut self, text: &str) {
        let bits = self.bits.len() * 8;
        let hashes = self.hashes;
        for trigram in trigrams(text) {
            for index in positions(trigram, hashes, bits) {
                self.bits[index / 8] |= 1 << (index % 8);
            }
        }
    }

    /// `false` means the pattern is **definitely absent** from every record here.
    ///
    /// `true` means "maybe", including for any pattern too short to have a trigram —
    /// erring towards reading a segment that turns out to hold nothing.
    #[must_use]
    pub fn may_contain(&self, pattern: &str) -> bool {
        if pattern.len() < MIN_PATTERN {
            return true;
        }
        trigrams(pattern).all(|trigram| {
            self.indices(trigram)
                .all(|index| self.bits[index / 8] & (1 << (index % 8)) != 0)
        })
    }

    /// Fraction of bits set. A filter near 1.0 answers "maybe" to everything and is
    /// worth reporting rather than trusting.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn saturation(&self) -> f64 {
        let set: u32 = self.bits.iter().map(|byte| byte.count_ones()).sum();
        f64::from(set) / (self.bits.len() * 8) as f64
    }

    fn indices(&self, trigram: &[u8]) -> impl Iterator<Item = usize> {
        positions(trigram, self.hashes, self.bits.len() * 8)
    }

    pub fn write(&self, dir: &Path) -> Result<()> {
        let mut out = Vec::with_capacity(self.bits.len() + 8);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.hashes.to_le_bytes());
        out.extend_from_slice(&self.bits);
        let path = dir.join(FILE);
        std::fs::write(&path, &out).map_err(|e| Error::io(format!("writing {}", path.display()), e))
    }

    /// Load a filter, or `None` if it is missing or damaged.
    ///
    /// Damage degrades to "no index", never to a wrong answer: the caller then reads
    /// every segment, which is exactly the behaviour that existed before this file did.
    #[must_use]
    pub fn read(dir: &Path) -> Option<Self> {
        let raw = std::fs::read(dir.join(FILE)).ok()?;
        if raw.len() < 8 || &raw[..4] != MAGIC {
            return None;
        }
        let hashes = u32::from_le_bytes(raw[4..8].try_into().ok()?);
        if hashes == 0 || raw.len() == 8 {
            return None;
        }
        Some(Self {
            bits: raw[8..].to_vec(),
            hashes,
        })
    }
}

/// Every three-byte window of the input.
///
/// Deliberately over **bytes**, not characters: a multi-byte character simply yields
/// windows that straddle it, and the same is true when the pattern is windowed, so the
/// necessary condition still holds. Doing it by `char` would cost a decode per position
/// to gain nothing.
fn trigrams(text: &str) -> impl Iterator<Item = &[u8]> {
    text.as_bytes().windows(3)
}

/// Double hashing: two 64-bit hashes generate all `k` positions, which is standard and
/// avoids computing four separate digests per trigram.
///
/// A trigram is three bytes, so it packs into a `u32` and can be mixed with two
/// multiplies instead of hashed byte by byte. That matters: this runs once per
/// character of every record at seal time, and byte-wise FNV over the same data cost
/// about a fifth of ingest throughput.
fn positions(trigram: &[u8], hashes: u32, bits: usize) -> impl Iterator<Item = usize> {
    let packed = u64::from(trigram[0]) << 16 | u64::from(trigram[1]) << 8 | u64::from(trigram[2]);
    let a = mix(packed ^ 0xcbf2_9ce4_8422_2325);
    let b = mix(packed ^ 0x9e37_79b9_7f4a_7c15) | 1;
    let bits = bits as u64;
    (0..hashes).map(move |i| {
        let position = a.wrapping_add(u64::from(i).wrapping_mul(b)) % bits;
        // `position < bits`, and `bits` came from a `usize`, so this cannot truncate.
        usize::try_from(position).unwrap_or(0)
    })
}

/// splitmix64's finaliser: good avalanche for three multiplies and three shifts.
fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn an_indexed_substring_is_always_reported_as_present() {
        // The property the whole thing rests on: no false negatives, ever.
        let mut index = TrigramIndex::with_capacity(64);
        let lines = [
            "payment attempt 42 for order 1042 took 42ms",
            "connection refused talking to stripe",
            "MyTimeoutError while charging card",
        ];
        for line in lines {
            index.insert(line);
        }

        for line in lines {
            for start in 0..line.len() {
                for end in (start + MIN_PATTERN)..=line.len().min(start + 24) {
                    let Some(pattern) = line.get(start..end) else {
                        continue;
                    };
                    assert!(
                        index.may_contain(pattern),
                        "{pattern:?} is a substring of an indexed line but was reported absent"
                    );
                }
            }
        }
    }

    #[test]
    fn a_token_index_would_have_been_wrong_here() {
        // `TimeoutError` is a substring of `MyTimeoutError` but not a token of it.
        let mut index = TrigramIndex::with_capacity(8);
        index.insert("MyTimeoutError while charging card");
        assert!(index.may_contain("TimeoutError"));
    }

    #[test]
    fn an_absent_pattern_is_usually_rejected() {
        let mut index = TrigramIndex::with_capacity(256);
        for i in 0..200 {
            index.insert(&format!("order {i} processed in 42ms via stripe"));
        }
        // Not asserted as certain — it is a Bloom filter — but a pattern sharing no
        // trigrams with anything indexed should be rejected, and that is the case the
        // 98-second query hits.
        assert!(!index.may_contain("zzzzz-no-such-term"));
        assert!(!index.may_contain("qqqq"));
    }

    #[test]
    fn a_pattern_too_short_to_have_a_trigram_never_prunes() {
        let index = TrigramIndex::with_capacity(8);
        for pattern in ["", "a", "ab"] {
            assert!(
                index.may_contain(pattern),
                "{pattern:?} has no trigram, so it must not be used to skip anything"
            );
        }
    }

    #[test]
    fn a_damaged_file_reads_as_no_index() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FILE), b"not a filter").unwrap();
        assert!(TrigramIndex::read(dir.path()).is_none());

        std::fs::write(dir.path().join(FILE), b"").unwrap();
        assert!(TrigramIndex::read(dir.path()).is_none());
    }

    #[test]
    fn a_round_trip_preserves_every_answer() {
        let dir = tempfile::tempdir().unwrap();
        let mut index = TrigramIndex::with_capacity(64);
        for i in 0..50 {
            index.insert(&format!("line {i} with some words in it"));
        }
        index.write(dir.path()).unwrap();

        let loaded = TrigramIndex::read(dir.path()).unwrap();
        for pattern in ["line 7 with", "some words", "zzz-absent", "in it"] {
            assert_eq!(
                index.may_contain(pattern),
                loaded.may_contain(pattern),
                "{pattern:?} answered differently after a round trip"
            );
        }
    }

    #[test]
    fn saturation_is_reported() {
        let empty = TrigramIndex::with_capacity(1024);
        assert!(empty.saturation() < 0.01);

        // Genuinely varied text, not the same phrase with a counter: what saturates a
        // trigram filter is the number of *distinct* trigrams, and repeating a fixed
        // sentence exhausts its alphabet almost immediately.
        let mut full = TrigramIndex::with_capacity(1);
        let alphabet: Vec<char> = ('a'..='z').chain('0'..='9').collect();
        let mut seed = 1u64;
        for _ in 0..20_000 {
            let word: String = (0..12)
                .map(|_| {
                    seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    alphabet[(seed >> 33) as usize % alphabet.len()]
                })
                .collect();
            full.insert(&word);
        }
        assert!(
            full.saturation() > 0.9,
            "expected a small filter to saturate, got {}",
            full.saturation()
        );
        // Saturated or not, it must still never produce a false negative.
        assert!(full.may_contain("some quite varied text"));
    }
}
