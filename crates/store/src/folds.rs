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
//! # Gauges too, from format 2
//!
//! Format 2 adds four numbers per stream — the compensated sum of the values, its Kahan
//! compensation, the least and the greatest — which is what `sum_over_time`,
//! `count_over_time`, `avg_over_time`, `min_over_time` and `max_over_time` need over a
//! window, and what they need is again the same for a segment whatever the query. These
//! are exact in the same sense as the counter summary: the least of the parts is the
//! least of the whole, and a sum of compensated sums differs from one pass only in the
//! order of the additions. A sum that is not finite is not joined at all — Prometheus
//! switches to another way of averaging there — and the query reads the rows instead.
//!
//! A format 1 file still answers `rate` and `increase`; it is read with
//! [`StreamFold::gauges`] unset, so a gauge function never trusts the four numbers it
//! lacks, and the background backfill rewrites it as format 2. An older telemetryd reading
//! a format 2 file finds a version it does not know and reads the rows, so the change
//! costs it speed and never an answer.
//!
//! # Binary, not JSON
//!
//! A segment holding three thousand streams is a hundred and forty kilobytes here. A query
//! over a week touches five hundred segments, so the decoding cost is the point of the
//! whole exercise: JSON would spend more time parsing than the rows cost to read.

use std::path::Path;

use telemetryd_core::Result;

/// File name inside a segment directory.
pub const FOLDS_FILE: &str = "folds.bin";

const MAGIC: &[u8; 4] = b"TFLD";
const VERSION: u32 = 2;
/// Format 1: `seen` plus five 8-byte fields — the counter summary alone.
const V1_BYTES_PER_STREAM: usize = 8 * 5 + 8;
/// Format 2: format 1 plus the sum, its compensation, the least and the greatest.
const BYTES_PER_STREAM: usize = V1_BYTES_PER_STREAM + 8 * 4;

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
    /// The values added up, Kahan–Neumaier compensated as Prometheus adds a window up;
    /// `compensation` is the running correction, added at the end.
    pub sum: f64,
    pub compensation: f64,
    /// The least and the greatest value, chosen as `min_over_time` and `max_over_time`
    /// choose: a NaN is kept only until any number arrives.
    pub min: f64,
    pub max: f64,
    /// Whether `sum`, `compensation`, `min` and `max` were computed. Unset for a summary
    /// read from a format 1 file, which predates them, and for any join that took one in.
    pub gauges: bool,
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
            sum: 0.0,
            compensation: 0.0,
            min: 0.0,
            max: 0.0,
            gauges: true,
        }
    }
}

/// `candidate` replaces `current` as the least, the way `min_over_time` decides.
fn less(candidate: f64, current: f64) -> bool {
    candidate < current || current.is_nan()
}

/// `candidate` replaces `current` as the greatest, the way `max_over_time` decides.
fn greater(candidate: f64, current: f64) -> bool {
    candidate > current || current.is_nan()
}

impl StreamFold {
    /// Add one sample. Samples must arrive in ascending time order.
    pub fn add(&mut self, timestamp: u64, value: f64) {
        // A staleness marker ends a series; it is not a sample of it. Folded in, its NaN
        // would poison every increase it touched.
        if telemetryd_core::is_stale_marker(value) {
            return;
        }
        if self.seen == 0 {
            self.first_nanos = timestamp;
            self.first_value = value;
            self.min = value;
            self.max = value;
        } else {
            // A drop means the counter restarted, so the new value *is* the increase.
            self.increase += if value < self.last_value {
                value
            } else {
                value - self.last_value
            };
            if less(value, self.min) {
                self.min = value;
            }
            if greater(value, self.max) {
                self.max = value;
            }
        }
        (self.sum, self.compensation) =
            telemetryd_core::metric::kahan_add(value, self.sum, self.compensation);
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
        if less(later.min, self.min) {
            self.min = later.min;
        }
        if greater(later.max, self.max) {
            self.max = later.max;
        }
        (self.sum, self.compensation) =
            telemetryd_core::metric::kahan_add(later.sum, self.sum, self.compensation);
        self.compensation += later.compensation;
        self.gauges &= later.gauges;
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
            out.extend_from_slice(&fold.sum.to_le_bytes());
            out.extend_from_slice(&fold.compensation.to_le_bytes());
            out.extend_from_slice(&fold.min.to_le_bytes());
            out.extend_from_slice(&fold.max.to_le_bytes());
        }
        crate::sidecar::write(&path, &out)
    }

    /// Whether the file in `dir` is the current format, from its first eight bytes alone.
    ///
    /// Asked once per segment when the store opens, so that a query can tell a segment
    /// whose summary answers gauge functions from one whose summary predates them without
    /// reading either. Anything unreadable is `false`: the summary is then rewritten or
    /// ignored, never trusted.
    #[must_use]
    pub fn is_current(dir: &Path) -> bool {
        use std::io::Read;
        let mut header = [0u8; 8];
        std::fs::File::open(dir.join(FOLDS_FILE))
            .and_then(|mut file| file.read_exact(&mut header))
            .is_ok()
            && &header[0..4] == MAGIC
            && header[4..8] == VERSION.to_le_bytes()
    }

    /// Read a segment's summaries, or `None` when it has none.
    ///
    /// A missing or unreadable file is not an error: segments written before this existed
    /// have none, and a query falls back to reading their rows. Returning `Err` would turn
    /// an old segment into a failed query.
    #[must_use]
    pub fn read(dir: &Path) -> Option<Self> {
        let raw = crate::sidecar::read(&dir.join(FOLDS_FILE))?;
        if raw.len() < 16 || &raw[0..4] != MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(raw[4..8].try_into().ok()?);
        let width = match version {
            1 => V1_BYTES_PER_STREAM,
            VERSION => BYTES_PER_STREAM,
            // From a newer telemetryd: whatever it holds, it is not known to mean what
            // this build would take it to mean. The rows are still there.
            _ => return None,
        };
        let count = usize::try_from(u64::from_le_bytes(raw[8..16].try_into().ok()?)).ok()?;
        if raw.len() != 16 + count.checked_mul(width)? {
            // Truncated, which a crash mid-write looks like. The rows are still there.
            return None;
        }
        let number = |at: usize| u64::from_le_bytes(raw[at..at + 8].try_into().unwrap_or([0; 8]));
        let float = |at: usize| f64::from_bits(number(at));
        let mut folds = Vec::with_capacity(count);
        for stream in 0..count {
            let at = 16 + stream * width;
            let gauges = version == VERSION;
            folds.push(StreamFold {
                seen: number(at),
                first_nanos: number(at + 8),
                last_nanos: number(at + 16),
                first_value: float(at + 24),
                last_value: float(at + 32),
                increase: float(at + 40),
                sum: if gauges { float(at + 48) } else { 0.0 },
                compensation: if gauges { float(at + 56) } else { 0.0 },
                min: if gauges { float(at + 64) } else { 0.0 },
                max: if gauges { float(at + 72) } else { 0.0 },
                gauges,
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
        // Cut into the contents, not just the checksum trailer behind them.
        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() - 20]).unwrap();
        assert!(StreamFolds::read(dir.path()).is_none());

        std::fs::write(&path, b"not a folds file at all").unwrap();
        assert!(StreamFolds::read(dir.path()).is_none());
    }

    /// The gauge numbers join like the counter ones: the least of the parts is the least
    /// of the whole, and so on — including a NaN, which a number displaces from either
    /// side of the join as it would mid-stream.
    #[test]
    fn gauge_numbers_join_like_one_pass() {
        let all: Vec<(u64, f64)> = vec![
            (60, f64::NAN),
            (120, 4.5),
            (180, -2.0),
            (240, 9.25),
            (300, f64::NAN),
            (360, 0.125),
        ];
        let whole = folded(&all);
        for split in 0..=all.len() {
            let mut joined = folded(&all[..split]);
            joined.merge_later(&folded(&all[split..]));
            assert_eq!(joined.seen, whole.seen, "split at {split}");
            assert_eq!(
                joined.min.to_bits(),
                whole.min.to_bits(),
                "min, split at {split}"
            );
            assert_eq!(
                joined.max.to_bits(),
                whole.max.to_bits(),
                "max, split at {split}"
            );
            assert!(joined.gauges);
        }
        assert!((whole.min - -2.0).abs() < f64::EPSILON);
        assert!((whole.max - 9.25).abs() < f64::EPSILON);

        let finite: Vec<(u64, f64)> = (1..=40u32)
            .map(|i| (u64::from(i) * 60, f64::from(i) * 0.1))
            .collect();
        let whole = folded(&finite);
        let mut joined = folded(&finite[..17]);
        joined.merge_later(&folded(&finite[17..]));
        let total = |f: &StreamFold| f.sum + f.compensation;
        assert!((total(&joined) - total(&whole)).abs() <= 1e-12 * total(&whole).abs());
    }

    /// A format 1 file — what every store held before format 2 — still answers `rate`,
    /// and says it cannot answer the gauge functions.
    #[test]
    fn a_format_1_file_reads_without_gauges() {
        let dir = tempfile::tempdir().unwrap();
        let fold = folded(&[(60, 1.0), (120, 7.5), (180, 3.0)]);
        let mut raw = Vec::new();
        raw.extend_from_slice(MAGIC);
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(&1u64.to_le_bytes());
        for number in [fold.seen, fold.first_nanos, fold.last_nanos] {
            raw.extend_from_slice(&number.to_le_bytes());
        }
        for number in [fold.first_value, fold.last_value, fold.increase] {
            raw.extend_from_slice(&number.to_le_bytes());
        }
        crate::sidecar::write(&dir.path().join(FOLDS_FILE), &raw).unwrap();

        assert!(!StreamFolds::is_current(dir.path()));
        let back = StreamFolds::read(dir.path()).unwrap();
        assert_eq!(back.0.len(), 1);
        assert!(!back.0[0].gauges);
        assert_eq!(back.0[0].seen, 3);
        assert!((back.0[0].increase - fold.increase).abs() < f64::EPSILON);

        // Joined into anything, it takes the "no gauges" with it.
        let mut joined = StreamFold::default();
        joined.merge_later(&back.0[0]);
        assert!(!joined.gauges);

        StreamFolds(vec![fold]).write(dir.path()).unwrap();
        assert!(StreamFolds::is_current(dir.path()));
        assert!(StreamFolds::read(dir.path()).unwrap().0[0].gauges);
    }
}
