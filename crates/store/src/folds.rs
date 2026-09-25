//! Per-stream counter summaries, computed once when a segment seals.
//!
//! # Why this exists
//!
//! A dashboard asking for a total or a quantile over a whole period compiles to
//! `rate(metric[P])` with `P` the period. Answering it means walking every sample of
//! every matching series — thirty million rows for a week of one Laravel app's request
//! histogram, which is fourteen seconds on a VPS and cannot be made much faster: the scan
//! already costs about fifty nanoseconds a row, and a columnar rewrite of it measured
//! *slower*.
//!
//! But `rate` and `increase` over a window need six numbers per series, not the samples,
//! and those numbers are the same for a given segment no matter what the query is. So
//! they are worked out once, at seal, and a query whose window fully contains a segment
//! reads them instead of its rows.
//!
//! **Exact, not approximate.** Counters are additive: the increase over a period is the
//! sum of the increases over its parts, with the counter-reset rule applied at each
//! boundary from the previous part's last value. Histogram buckets are counters. So this
//! is not downsampling — there is no second resolution, no lookback to reconcile, and no
//! gaps. The same query returns the same number, computed from fewer reads.
//!
//! # Not resident
//!
//! Deliberately a separate file rather than a field on the manifest. Manifests are loaded
//! when the store opens and stay in memory for the life of the process — that is the term
//! that took a production box down, and adding six numbers per stream to every one of them
//! would repeat it at a smaller scale. These are read when a query wants them and dropped
//! when it is done.
//!
//! # Binary, not JSON
//!
//! A segment holding three thousand streams is a hundred and forty kilobytes here. A query
//! over a week touches five hundred segments, so the decoding cost is the point of the
//! whole exercise: JSON would spend more time parsing than the rows cost to read.

use std::path::Path;

use telemetryd_core::{Error, Result};

/// File name inside a segment directory.
pub const FOLDS_FILE: &str = "folds.bin";

const MAGIC: &[u8; 4] = b"TFLD";
const VERSION: u32 = 1;
/// `seen` plus five 8-byte fields.
const BYTES_PER_STREAM: usize = 8 * 5 + 8;

/// One stream's counter summary over one segment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StreamFold {
    /// Samples seen. Zero means the stream contributed nothing to this segment.
    pub seen: u64,
    pub first_nanos: u64,
    pub last_nanos: u64,
    pub first_value: f64,
    pub last_value: f64,
    /// Accumulated increase *within* the segment, counter resets already applied.
    ///
    /// The step from the previous segment's last value is deliberately not included: it
    /// belongs to whoever joins the two, and only they know what came before.
    pub increase: f64,
}

impl Default for StreamFold {
    fn default() -> Self {
        Self {
            seen: 0,
            first_nanos: 0,
            last_nanos: 0,
            first_value: 0.0,
            last_value: 0.0,
            increase: 0.0,
        }
    }
}

impl StreamFold {
    /// Add one sample. Samples must arrive in ascending time order.
    pub fn add(&mut self, timestamp: u64, value: f64) {
        if self.seen == 0 {
            self.first_nanos = timestamp;
            self.first_value = value;
        } else {
            // A drop means the counter restarted, so the new value *is* the increase.
            self.increase += if value < self.last_value {
                value
            } else {
                value - self.last_value
            };
        }
        self.seen = self.seen.saturating_add(1);
        self.last_nanos = timestamp;
        self.last_value = value;
    }

    /// Absorb a summary covering a *later* stretch of the same stream.
    ///
    /// The step across the join is counted here, because it is the only place both sides
    /// are known — the same reset rule as within a segment.
    /// Whether a run whose first sample is at `first_nanos` can be joined after this one.
    ///
    /// Joining adds the step from this run's last value to the next run's first, which is
    /// only the counter's increase when the next run really comes later. Late data breaks
    /// that: an agent replaying after an outage lands samples in a newer segment whose
    /// time range overlaps an older one, and joining in segment order reads the step back
    /// to the older value as a counter reset — measured at 440.9 against a true 180. A
    /// caller that finds a run out of order has to fall back to reading the rows, which
    /// sorts them first.
    #[must_use]
    pub fn precedes(&self, first_nanos: u64) -> bool {
        self.seen == 0 || first_nanos >= self.last_nanos
    }

    pub fn merge_later(&mut self, later: &Self) {
        if later.seen == 0 {
            return;
        }
        if self.seen == 0 {
            *self = *later;
            return;
        }
        self.increase += if later.first_value < self.last_value {
            later.first_value
        } else {
            later.first_value - self.last_value
        };
        self.increase += later.increase;
        self.seen = self.seen.saturating_add(later.seen);
        self.last_nanos = later.last_nanos;
        self.last_value = later.last_value;
    }
}

/// Every stream's summary for one segment, in the order of the manifest's `streams`.
#[derive(Debug, Clone, Default)]
pub struct StreamFolds(pub Vec<StreamFold>);

impl StreamFolds {
    #[must_use]
    pub fn get(&self, stream: usize) -> Option<&StreamFold> {
        self.0.get(stream)
    }

    pub fn write(&self, dir: &Path) -> Result<()> {
        let path = dir.join(FOLDS_FILE);
        let mut out = Vec::with_capacity(16 + self.0.len() * BYTES_PER_STREAM);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&u64::try_from(self.0.len()).unwrap_or(0).to_le_bytes());
        for fold in &self.0 {
            out.extend_from_slice(&fold.seen.to_le_bytes());
            out.extend_from_slice(&fold.first_nanos.to_le_bytes());
            out.extend_from_slice(&fold.last_nanos.to_le_bytes());
            out.extend_from_slice(&fold.first_value.to_le_bytes());
            out.extend_from_slice(&fold.last_value.to_le_bytes());
            out.extend_from_slice(&fold.increase.to_le_bytes());
        }
        std::fs::write(&path, out).map_err(|e| Error::io(format!("writing {}", path.display()), e))
    }

    /// Read a segment's summaries, or `None` when it has none.
    ///
    /// A missing or unreadable file is not an error: segments written before this existed
    /// have none, and a query falls back to reading their rows. Returning `Err` would turn
    /// an old segment into a failed query.
    #[must_use]
    pub fn read(dir: &Path) -> Option<Self> {
        let raw = std::fs::read(dir.join(FOLDS_FILE)).ok()?;
        if raw.len() < 16 || &raw[0..4] != MAGIC {
            return None;
        }
        if u32::from_le_bytes(raw[4..8].try_into().ok()?) != VERSION {
            return None;
        }
        let count = usize::try_from(u64::from_le_bytes(raw[8..16].try_into().ok()?)).ok()?;
        if raw.len() != 16 + count * BYTES_PER_STREAM {
            // Truncated, which a crash mid-write looks like. The rows are still there.
            return None;
        }
        let number = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().unwrap_or([0; 8]));
        let mut folds = Vec::with_capacity(count);
        for stream in 0..count {
            let at = 16 + stream * BYTES_PER_STREAM;
            folds.push(StreamFold {
                seen: number(at),
                first_nanos: number(at + 8),
                last_nanos: number(at + 16),
                first_value: f64::from_bits(number(at + 24)),
                last_value: f64::from_bits(number(at + 32)),
                increase: f64::from_bits(number(at + 40)),
            });
        }
        Some(Self(folds))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn folded(samples: &[(u64, f64)]) -> StreamFold {
        let mut fold = StreamFold::default();
        for (ts, value) in samples {
            fold.add(*ts, *value);
        }
        fold
    }

    /// The whole promise: splitting a stream across segments and joining the summaries
    /// gives the number a single pass would have. If this drifts, a dashboard shows a
    /// plausible total that is simply wrong.
    #[test]
    fn joining_two_halves_matches_one_pass() {
        let all: Vec<(u64, f64)> = (1..=10u32)
            .map(|i| (u64::from(i) * 60, f64::from(i) * 3.0))
            .collect();
        let whole = folded(&all);

        let mut joined = folded(&all[..4]);
        joined.merge_later(&folded(&all[4..]));

        assert_eq!(joined.seen, whole.seen);
        assert_eq!(joined.first_nanos, whole.first_nanos);
        assert_eq!(joined.last_nanos, whole.last_nanos);
        assert!((joined.increase - whole.increase).abs() < 1e-9);
    }

    /// A counter reset landing exactly on the join is the case a naive merge gets wrong:
    /// the step across the boundary has to apply the same rule as a step inside one.
    #[test]
    fn a_reset_across_the_join_is_counted_like_any_other() {
        let all: Vec<(u64, f64)> = vec![
            (60, 5.0),
            (120, 9.0),
            // restart
            (180, 2.0),
            (240, 11.0),
        ];
        let whole = folded(&all);

        let mut joined = folded(&all[..2]);
        joined.merge_later(&folded(&all[2..]));

        assert!((joined.increase - whole.increase).abs() < 1e-9);
    }

    /// An empty side must change nothing, in either position.
    #[test]
    fn empty_summaries_are_absorbed_without_effect() {
        let some = folded(&[(60, 1.0), (120, 4.0)]);

        let mut left = some;
        left.merge_later(&StreamFold::default());
        assert_eq!(left, some);

        let mut right = StreamFold::default();
        right.merge_later(&some);
        assert_eq!(right, some);
    }

    #[test]
    fn a_file_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let folds = StreamFolds(vec![
            folded(&[(60, 1.0), (120, 7.5)]),
            StreamFold::default(),
            folded(&[(30, 100.0)]),
        ]);
        folds.write(dir.path()).unwrap();

        let back = StreamFolds::read(dir.path()).unwrap();
        assert_eq!(back.0, folds.0);
    }

    /// A segment from before this existed, and one whose file a crash left half-written,
    /// both have to read as "no summaries" rather than as an error — the rows are still
    /// there, and a query falls back to them.
    #[test]
    fn a_missing_or_damaged_file_reads_as_nothing() {
        let dir = tempfile::tempdir().unwrap();
        assert!(StreamFolds::read(dir.path()).is_none());

        StreamFolds(vec![folded(&[(60, 1.0)])])
            .write(dir.path())
            .unwrap();
        let path = dir.path().join(FOLDS_FILE);
        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() - 8]).unwrap();
        assert!(StreamFolds::read(dir.path()).is_none());

        std::fs::write(&path, b"not a folds file at all").unwrap();
        assert!(StreamFolds::read(dir.path()).is_none());
    }
}
