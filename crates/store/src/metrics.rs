//! The Arrow/Parquet schema for metric samples.
//!
//! Three columns: the interned series id, a timestamp, and a float. The series
//! dictionary the record store already maintains *is* the series index, so
//! label matching costs one evaluation per series rather than one per sample, and
//! `/api/v1/labels` and `/api/v1/series` are answered from segment metadata alone.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use telemetryd_core::metric::{METRIC_NAME_LABEL, MetricKind, MetricSample};
use telemetryd_core::{Error, Labels, Result, Signal};

use crate::schema::arrow_util::{string_column, u32_column, u64_column};
use crate::schema::{RecordSchema, Rows, schema_ref};

#[derive(Debug, Clone, Copy)]
pub struct MetricSchema;

impl crate::RecordStore<MetricSchema> {
    /// Compute and store summaries for segments that have none.
    ///
    /// Segments sealed before summaries existed carry none, and a query over them falls
    /// back to reading their rows — correct, and as slow as it was. Without this the
    /// speed-up would arrive only as old segments aged out, which on a week's retention
    /// means a week.
    ///
    /// Bounded per call and driven from maintenance, because it reads whole segments: a
    /// backfill that tried to do a hundred at once would compete with ingest for exactly
    /// the memory the reaper is there to protect.
    ///
    /// Returns how many it wrote. A segment that cannot be read is left alone and counted
    /// as done, so one damaged file does not make this retry forever.
    pub fn backfill_folds(&self, limit: usize) -> Result<usize> {
        let mut written = 0;
        for segment in &self.segments() {
            if written >= limit {
                break;
            }
            if segment.has_folds() || segment.is_unreadable() {
                continue;
            }
            let Ok(records) = segment.read::<MetricSchema>() else {
                continue;
            };
            let mut rows: Vec<(usize, u64, f64)> = Vec::with_capacity(records.len());
            let index: std::collections::HashMap<u64, usize> = segment
                .manifest
                .streams
                .iter()
                .enumerate()
                .map(|(id, labels)| (labels.fingerprint(), id))
                .collect();
            for record in &records {
                if let Some(&id) = index.get(&record.series.fingerprint()) {
                    rows.push((id, record.timestamp_nanos, record.value));
                }
            }
            rows.sort_unstable_by_key(|(id, at, _)| (*id, *at));
            let mut folds =
                vec![crate::folds::StreamFold::default(); segment.manifest.streams.len()];
            for (id, at, value) in rows {
                folds[id].add(at, value);
            }
            crate::folds::StreamFolds(folds).write(&segment.dir)?;
            segment.mark_folds_written();
            written += 1;
        }
        Ok(written)
    }

    /// Per-stream counter summaries over `(start, end]`.
    ///
    /// # Why the store answers this rather than the query layer
    ///
    /// The shortcut is a storage fact. A segment lying wholly inside the window
    /// contributes exactly its precomputed summary: every row in it is in the window, so
    /// there is nothing to filter and nothing to read. Only the segments straddling an
    /// edge, and the unsealed tail, are walked row by row.
    ///
    /// For a week over hourly segments that is a few hundred file reads of a hundred and
    /// forty kilobytes instead of thirty million rows. The answer is the same either way —
    /// counters are additive, and the step across a join is applied by `merge_later`
    /// exactly as it would be mid-segment.
    ///
    /// Segments are visited oldest first, and the unsealed tail last, because the merge
    /// requires it: joined out of order, a later value would be taken as the window's
    /// first and the step back to it read as a counter reset.
    pub fn fold_window(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[telemetryd_core::LabelMatcher],
    ) -> Result<Option<Vec<(Labels, crate::folds::StreamFold)>>> {
        use crate::folds::StreamFold;
        use std::collections::HashMap;

        // How many segments would have to be read row by row. Reading one is cheap and
        // reading many is not: each is read *whole*, because a time-range scan would also
        // touch the neighbours already taken from summaries. Past a handful the single
        // pruned scan the store has always used is faster, and falling back to it is what
        // keeps a store whose segments are not summarised yet from being slower than one
        // that never had summaries at all — measured at thirty seconds against five.
        const READ_WHOLE_LIMIT: usize = 4;

        // Keyed by the label set itself, not by the allocation behind it. Segments
        // normally share one, so a pointer would do — but "normally" is not a property to
        // rest an answer on, and getting it wrong here multiplies a rate by the number of
        // segments in the window while looking entirely ordinary. This runs once per
        // stream per segment, not once per row.
        let mut by_series: HashMap<Labels, StreamFold> = HashMap::new();
        let mut order: Vec<Labels> = Vec::new();
        let note = |by_series: &mut HashMap<Labels, StreamFold>,
                    order: &mut Vec<Labels>,
                    labels: &Labels| {
            if !by_series.contains_key(labels) {
                by_series.insert(labels.clone(), StreamFold::default());
                order.push(labels.clone());
            }
            labels.clone()
        };

        let segments = self.segments();

        // Nothing is summarised yet, which is every store's first half hour after an
        // upgrade. Say so before sorting or inspecting anything: the answer is the same
        // either way, and this is the one case where the shortcut is pure overhead.
        if !segments.iter().any(|segment| segment.has_folds()) {
            return Ok(None);
        }

        let needs_rows = segments
            .iter()
            .filter(|segment| {
                let m = &segment.manifest;
                m.min_time_nanos <= end_nanos
                    && m.max_time_nanos > start_nanos
                    && !(m.min_time_nanos > start_nanos
                        && m.max_time_nanos <= end_nanos
                        && segment.has_folds())
            })
            .count();
        if needs_rows > READ_WHOLE_LIMIT {
            // Decline. The caller then takes the ordinary pruned scan it has always taken,
            // untouched, so a store whose segments are not summarised yet is never slower
            // than one that never had summaries at all. Replacing that scan with a second
            // fold-shaped implementation was tried and measured: thirty seconds against
            // five. Summaries are a shortcut over the existing path, not a rewrite of it.
            return Ok(None);
        }

        // Ordered only now: joining two summaries means adding the step between them, so
        // the loop below has to see segments in time order. The two exits above do not.
        let mut segments = segments;
        segments
            .sort_by_key(|segment| (segment.manifest.min_time_nanos, segment.manifest.id.clone()));

        for segment in &segments {
            let manifest = &segment.manifest;
            if manifest.min_time_nanos > end_nanos || manifest.max_time_nanos <= start_nanos {
                continue;
            }
            let allowed: Vec<bool> = manifest
                .streams
                .iter()
                .map(|labels| telemetryd_core::matches_all(matchers, labels))
                .collect();
            if !manifest.streams.is_empty() && !allowed.iter().any(|ok| *ok) {
                continue;
            }

            let wholly_inside =
                manifest.min_time_nanos > start_nanos && manifest.max_time_nanos <= end_nanos;
            if wholly_inside && let Some(precomputed) = segment.folds() {
                for (stream, labels) in manifest.streams.iter().enumerate() {
                    if !allowed.get(stream).copied().unwrap_or(false) {
                        continue;
                    }
                    let Some(fold) = precomputed.get(stream).filter(|f| f.seen > 0) else {
                        continue;
                    };
                    let key = note(&mut by_series, &mut order, labels);
                    if let Some(entry) = by_series.get_mut(&key) {
                        entry.merge_later(fold);
                    }
                }
                continue;
            }

            // Straddles an edge, or predates summaries: read this one segment. Reading the
            // *segment* rather than a time range matters — a range scan would also touch
            // its neighbours, which the loop has already taken from their summaries.
            let rows = segment.read::<MetricSchema>()?;
            fold_rows(
                rows.into_iter(),
                start_nanos,
                end_nanos,
                matchers,
                &mut by_series,
                &mut order,
            );
        }

        fold_rows(
            self.buffered_between(start_nanos, end_nanos).into_iter(),
            start_nanos,
            end_nanos,
            matchers,
            &mut by_series,
            &mut order,
        );

        let mut out: Vec<(Labels, StreamFold)> = order
            .into_iter()
            .filter_map(|labels| by_series.remove(&labels).map(|fold| (labels, fold)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Some(out))
    }
}

/// Fold a run of raw samples into `by_series`, in time order per stream.
///
/// Order is the whole point: a fold applies the counter-reset rule by comparing each
/// sample with the previous one, so feeding it rows in storage order would invent resets
/// that never happened and inflate the result. Sorting here rather than at every call
/// site is what keeps that from being something each caller has to remember.
fn fold_rows(
    records: impl Iterator<Item = MetricSample>,
    start_nanos: u64,
    end_nanos: u64,
    matchers: &[telemetryd_core::LabelMatcher],
    by_series: &mut std::collections::HashMap<Labels, crate::folds::StreamFold>,
    order: &mut Vec<Labels>,
) {
    let mut rows: Vec<(Labels, u64, f64)> = Vec::new();
    for record in records {
        if record.timestamp_nanos <= start_nanos
            || record.timestamp_nanos > end_nanos
            || !telemetryd_core::matches_all(matchers, &record.series)
        {
            continue;
        }
        if !by_series.contains_key(&record.series) {
            by_series.insert(record.series.clone(), crate::folds::StreamFold::default());
            order.push(record.series.clone());
        }
        rows.push((record.series, record.timestamp_nanos, record.value));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    for (key, at, value) in rows {
        if let Some(entry) = by_series.get_mut(&key) {
            entry.add(at, value);
        }
    }
}

impl RecordSchema for MetricSchema {
    type Record = MetricSample;

    const SIGNAL: Signal = Signal::Metrics;

    /// Deliberately narrow. Everything identifying the series lives in the dictionary;
    /// a row is 4 + 8 + 8 bytes before encoding, and sorted timestamps plus a
    /// dictionary-encoded id compress well.
    fn arrow_schema() -> SchemaRef {
        schema_ref(vec![
            Field::new("timestamp_nanos", DataType::UInt64, false),
            Field::new("stream_id", DataType::UInt32, false),
            Field::new("value", DataType::Float64, false),
            // Hoisted so a `__name__` matcher can prune on row-group statistics.
            Field::new("name", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
        ])
    }

    fn to_batch(records: &[Self::Record]) -> Result<(RecordBatch, Vec<Labels>)> {
        let mut interner = crate::segment::StreamInterner::default();

        let timestamps = UInt64Array::from_iter_values(records.iter().map(|s| s.timestamp_nanos));
        let stream_ids =
            UInt32Array::from_iter_values(records.iter().map(|s| interner.intern(&s.series)));
        let values = Float64Array::from_iter_values(records.iter().map(|s| s.value));
        let names = StringArray::from_iter_values(records.iter().map(MetricSample::name));
        let kinds = StringArray::from_iter_values(records.iter().map(|s| s.kind.as_str()));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamps),
            Arc::new(stream_ids),
            Arc::new(values),
            Arc::new(names),
            Arc::new(kinds),
        ];

        let batch = RecordBatch::try_new(Self::arrow_schema(), columns)
            .map_err(|e| Error::Config(format!("building a metric record batch: {e}")))?;
        Ok((batch, interner.into_streams()))
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self::Record>> {
        let rows: Rows = (0..u32::try_from(batch.num_rows()).unwrap_or(u32::MAX)).collect();
        Self::materialize(batch, &rows, &[])
    }

    fn select_rows(
        batch: &RecordBatch,
        start_nanos: u64,
        end_nanos: u64,
        allowed_streams: &[bool],
    ) -> Result<Rows> {
        let timestamps = u64_column(batch, "timestamp_nanos")?;
        let stream_ids = u32_column(batch, "stream_id")?;

        let mut rows = Rows::new();
        for row in 0..batch.num_rows() {
            let ts = timestamps.value(row);
            if ts < start_nanos || ts > end_nanos {
                continue;
            }
            let id = stream_ids.value(row) as usize;
            if !allowed_streams.is_empty() && allowed_streams.get(id).is_some_and(|ok| !ok) {
                continue;
            }
            rows.push(u32::try_from(row).unwrap_or(u32::MAX));
        }
        Ok(rows)
    }

    fn materialize(
        batch: &RecordBatch,
        rows: &Rows,
        streams: &[Labels],
    ) -> Result<Vec<Self::Record>> {
        let timestamps = u64_column(batch, "timestamp_nanos")?;
        let stream_ids = u32_column(batch, "stream_id")?;
        let values = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| Error::WalCorrupt {
                path: std::path::PathBuf::from("<segment>"),
                detail: "metric segment is missing a Float64 `value` column".to_owned(),
            })?;
        let names = string_column(batch, "name")?;
        let kinds = string_column(batch, "kind")?;

        let mut out = Vec::with_capacity(rows.len());
        for &row in rows {
            let row = row as usize;
            let series = streams.get(stream_ids.value(row) as usize).map_or_else(
                || {
                    // No dictionary: at least keep the name, so the sample is not
                    // completely unattributable.
                    let mut series = Labels::new();
                    series.insert(METRIC_NAME_LABEL, names.value(row));
                    series
                },
                Clone::clone,
            );

            out.push(MetricSample {
                timestamp_nanos: timestamps.value(row),
                series,
                value: values.value(row),
                kind: MetricKind::from_str_lossy(kinds.value(row)),
            });
        }
        Ok(out)
    }

    fn counter_value(record: &Self::Record) -> Option<f64> {
        Some(record.value)
    }

    fn timestamp(record: &Self::Record) -> u64 {
        record.timestamp_nanos
    }

    fn index_labels(record: &Self::Record) -> &Labels {
        &record.series
    }

    fn size_estimate(record: &Self::Record) -> usize {
        record.size_estimate()
    }

    fn filter_columns() -> &'static [&'static str] {
        &["timestamp_nanos", "stream_id"]
    }

    fn selection_mask(
        batch: &RecordBatch,
        start_nanos: u64,
        end_nanos: u64,
        allowed_streams: &[bool],
    ) -> Result<arrow::array::BooleanArray> {
        let timestamps = u64_column(batch, "timestamp_nanos")?;
        let stream_ids = u32_column(batch, "stream_id")?;

        Ok((0..batch.num_rows())
            .map(|row| {
                let ts = timestamps.value(row);
                if ts < start_nanos || ts > end_nanos {
                    return Some(false);
                }
                let id = stream_ids.value(row) as usize;
                Some(allowed_streams.is_empty() || allowed_streams.get(id).copied().unwrap_or(true))
            })
            .collect())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::*;

    fn materialize_all(batch: &RecordBatch, streams: &[Labels]) -> Vec<MetricSample> {
        let rows: Rows = (0..u32::try_from(batch.num_rows()).unwrap_or(u32::MAX)).collect();
        MetricSchema::materialize(batch, &rows, streams).unwrap()
    }

    fn sample(i: u64) -> MetricSample {
        let mut series = Labels::new();
        series.insert(METRIC_NAME_LABEL, "http_requests_total");
        series.insert("app", "checkout");
        series.insert("status", if i.is_multiple_of(2) { "200" } else { "500" });

        MetricSample {
            timestamp_nanos: 1_750_000_000_000_000_000 + i * 1_000_000_000,
            series,
            #[allow(clippy::cast_precision_loss)]
            value: i as f64 * 1.5,
            kind: MetricKind::Counter,
        }
    }

    #[test]
    fn samples_round_trip_through_arrow_unchanged() {
        let records: Vec<MetricSample> = (0..128).map(sample).collect();
        let (batch, streams) = MetricSchema::to_batch(&records).unwrap();
        assert_eq!(batch.num_rows(), 128);
        assert_eq!(materialize_all(&batch, &streams), records);
    }

    #[test]
    fn repeated_series_collapse_into_a_small_dictionary() {
        // The point of interning: 1000 samples across two series is two entries, and a
        // selector is evaluated twice rather than a thousand times.
        let records: Vec<MetricSample> = (0..1000).map(sample).collect();
        let (_, streams) = MetricSchema::to_batch(&records).unwrap();
        assert_eq!(streams.len(), 2, "status=200 and status=500");
    }

    #[test]
    fn awkward_float_values_survive() {
        for value in [0.0, -1.5, f64::MAX, f64::MIN_POSITIVE, 1e-300, 1e300] {
            let record = MetricSample { value, ..sample(1) };
            let (batch, streams) = MetricSchema::to_batch(std::slice::from_ref(&record)).unwrap();
            assert!(
                (materialize_all(&batch, &streams)[0].value - value).abs()
                    <= f64::EPSILON.max(value.abs() * f64::EPSILON),
                "{value} did not survive"
            );
        }
    }

    #[test]
    fn nan_and_infinity_survive_as_themselves() {
        // Prometheus uses NaN as a real signal (staleness, absent buckets), so it must
        // not be quietly turned into zero.
        let (batch, streams) = MetricSchema::to_batch(&[
            MetricSample {
                value: f64::NAN,
                ..sample(1)
            },
            MetricSample {
                value: f64::INFINITY,
                ..sample(2)
            },
        ])
        .unwrap();

        let restored = materialize_all(&batch, &streams);
        assert!(restored[0].value.is_nan());
        assert!(restored[1].value.is_infinite());
    }

    #[test]
    fn an_empty_batch_is_valid() {
        let (batch, _) = MetricSchema::to_batch(&[]).unwrap();
        assert!(materialize_all(&batch, &[]).is_empty());
    }

    #[test]
    fn selection_filters_by_time_and_series() {
        let records: Vec<MetricSample> = (0..10).map(sample).collect();
        let (batch, streams) = MetricSchema::to_batch(&records).unwrap();

        let all = MetricSchema::select_rows(&batch, 0, u64::MAX, &[]).unwrap();
        assert_eq!(all.len(), 10);

        let window = MetricSchema::select_rows(
            &batch,
            records[2].timestamp_nanos,
            records[5].timestamp_nanos,
            &[],
        )
        .unwrap();
        assert_eq!(window.len(), 4);

        // Only the first interned series.
        let mut allowed = vec![false; streams.len()];
        allowed[0] = true;
        let one_series = MetricSchema::select_rows(&batch, 0, u64::MAX, &allowed).unwrap();
        assert_eq!(one_series.len(), 5);
    }

    #[test]
    fn the_selection_mask_agrees_with_select_rows() {
        // They are two implementations of the same predicate — one for Parquet
        // pushdown, one for the in-memory pass — and a disagreement would mean rows
        // silently missing from a result.
        let records: Vec<MetricSample> = (0..64).map(sample).collect();
        let (batch, _) = MetricSchema::to_batch(&records).unwrap();

        let start = records[10].timestamp_nanos;
        let end = records[40].timestamp_nanos;

        let rows = MetricSchema::select_rows(&batch, start, end, &[]).unwrap();
        let mask = MetricSchema::selection_mask(&batch, start, end, &[]).unwrap();

        let from_mask: Vec<u32> = (0..batch.num_rows())
            .filter(|&row| mask.value(row))
            .map(|row| u32::try_from(row).unwrap())
            .collect();
        assert_eq!(rows, from_mask);
    }
}
