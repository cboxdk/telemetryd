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
//! Why this exists at all: there was a falsifiable trigger for building a text
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
    /// Build from the trigrams actually present, rather than a guess from row count.
    ///
    /// The previous sizing was `records * 4`, which the comment above it argued against
    /// in the same breath — and it was wrong in both directions, measured on real text:
    ///
    /// | corpus | distinct trigrams | needed | allocated |
    /// |---|---|---|---|
    /// | repetitive logs | 1,351 | 1.0 KiB | 29.3 KiB |
    /// | varied logs | 24,231 | 17.7 KiB | 29.3 KiB |
    /// | high entropy | 511,968 | 375 KiB | 29.3 KiB |
    ///
    /// Thirty times too large on repetitive data is only waste. Ten times too *small* on
    /// high-entropy data — base64 payloads, stack traces full of hex, JSON with random
    /// ids — is worse than waste: the filter saturates, answers "maybe" to everything,
    /// and every query reads every segment while the sidecar still costs disk. Verified
    /// against a running instance: a term guaranteed absent pruned 44 of 44 segments on
    /// repetitive text and 0 of 2 on random text.
    ///
    /// The count is exact because sealing already holds every record in memory. The
    /// transient `HashSet` is bounded by the trigram space rather than by the data.
    #[must_use]
    pub fn build<'a>(texts: impl Iterator<Item = &'a str>) -> Option<Self> {
        let mut distinct: std::collections::HashSet<[u8; 3]> = std::collections::HashSet::new();
        for text in texts {
            for trigram in trigrams(text) {
                // `windows(3)` always yields three bytes.
                if let Ok(three) = <[u8; 3]>::try_from(trigram) {
                    distinct.insert(three);
                }
            }
        }
        if distinct.is_empty() {
            return None;
        }

        // Refuse rather than write one that cannot prune. A missing filter makes the
        // caller scan, which is correct and free; a saturated filter makes the caller
        // scan *and* charges for the privilege, while looking like an index.
        let needed = (distinct.len() * BITS_PER_TRIGRAM).div_ceil(8);
        if needed > MAX_BYTES {
            return None;
        }

        let mut index = Self {
            bits: vec![0; needed.max(MIN_BYTES)],
            hashes: HASHES,
        };
        let bits = index.bits.len() * 8;
        for trigram in distinct {
            for position in positions(&trigram, HASHES, bits) {
                index.bits[position / 8] |= 1 << (position % 8);
            }
        }
        Some(index)
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
        let lines = [
            "payment attempt 42 for order 1042 took 42ms",
            "connection refused talking to stripe",
            "MyTimeoutError while charging card",
        ];
        let index = TrigramIndex::build(lines.iter().copied()).unwrap();

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
        let index =
            TrigramIndex::build(["MyTimeoutError while charging card"].into_iter()).unwrap();
        assert!(index.may_contain("TimeoutError"));
    }

    #[test]
    fn an_absent_pattern_is_usually_rejected() {
        let lines: Vec<String> = (0..200)
            .map(|i| format!("order {i} processed in 42ms via stripe"))
            .collect();
        let index = TrigramIndex::build(lines.iter().map(String::as_str)).unwrap();
        // Not asserted as certain — it is a Bloom filter — but a pattern sharing no
        // trigrams with anything indexed should be rejected, and that is the case the
        // 98-second query hits.
        assert!(!index.may_contain("zzzzz-no-such-term"));
        assert!(!index.may_contain("qqqq"));
    }

    #[test]
    fn a_pattern_too_short_to_have_a_trigram_never_prunes() {
        let index = TrigramIndex::build(["anything at all"].into_iter()).unwrap();
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
        let lines: Vec<String> = (0..50)
            .map(|i| format!("line {i} with some words in it"))
            .collect();
        let index = TrigramIndex::build(lines.iter().map(String::as_str)).unwrap();
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
    fn a_filter_that_could_not_prune_is_not_written_at_all() {
        // What saturates a trigram filter is the number of *distinct* trigrams, so this
        // is genuinely varied text rather than one phrase with a counter.
        // Printable ASCII, not `a-z0-9`: a 36-character alphabet has only 46,656
        // possible trigrams, which now sizes to a perfectly good 35 KiB filter — the
        // improvement itself. Saturation needs the wider alphabet that base64 payloads
        // and hex-laden stack traces actually produce.
        let mut seed = 1u64;
        let alphabet: Vec<char> = (32u8..127).map(char::from).collect();
        let lines: Vec<String> = (0..200_000)
            .map(|_| {
                (0..12)
                    .map(|_| {
                        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                        alphabet[(seed >> 33) as usize % alphabet.len()]
                    })
                    .collect()
            })
            .collect();

        // The old sizing gave this a fixed allocation, which it saturated: every query
        // then read every segment while the sidecar still cost disk. Refusing is the
        // honest answer — the caller scans either way, and now pays nothing to be told.
        assert!(
            TrigramIndex::build(lines.iter().map(String::as_str)).is_none(),
            "a corpus too varied to index must yield no filter rather than a useless one"
        );
    }

    #[test]
    fn an_ordinary_corpus_leaves_the_filter_far_from_saturated() {
        let lines: Vec<String> = (0..5_000)
            .map(|i| format!("order {i} processed in 42ms via stripe"))
            .collect();
        let index = TrigramIndex::build(lines.iter().map(String::as_str)).unwrap();
        assert!(
            index.saturation() < 0.6,
            "sized from the trigrams present, this should have headroom, got {}",
            index.saturation()
        );
    }
}
