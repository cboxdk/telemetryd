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

use crate::schema::arrow_util::{f64_column, string_column, u32_column, u64_column};
use crate::schema::{RecordSchema, Rows, schema_ref};

#[derive(Debug, Clone, Copy)]
pub struct MetricSchema;

/// A run of metric samples from one place in the store, as columns.
///
/// `streams[stream_ids[i]]` is the series of sample `i`. Stream ids are numbered per
/// source, so `source` changing is the signal that a consumer's per-stream cache no
/// longer applies.
#[derive(Debug, Clone, Copy)]
pub struct SampleRun<'a> {
    pub source: usize,
    pub streams: &'a [Labels],
    pub stream_ids: &'a [u32],
    pub timestamps: &'a [u64],
    pub values: &'a [f64],
}

impl crate::RecordStore<MetricSchema> {
    /// Hand `visit` every sample that may fall in `windows` — sorted, disjoint
    /// `(start, end)` pairs, both ends included — a batch of columns at a time.
    ///
    /// Only three columns are read and nothing is materialised: no labels are cloned,
    /// no sample is built, and nothing is held past the batch. A week-long chart that
    /// used to decode every sample of the week into a record reads the timestamps,
    /// stream ids and values of the row groups its windows touch.
    ///
    /// `matchers` prune segments only; which streams a consumer wants is its decision,
    /// made once per stream. Sources come oldest first — sealed segments by their
    /// earliest sample, then the unsealed buffer — and rows within one come in time
    /// order. Across sources they need not: late data lands in a later segment. A
    /// consumer that needs order checks it and returns `Break`, and this returns
    /// `Ok(false)` so it can read another way.
    pub fn scan_samples(
        &self,
        windows: &[(u64, u64)],
        matchers: &[telemetryd_core::LabelMatcher],
        visit: &mut dyn FnMut(SampleRun<'_>) -> std::ops::ControlFlow<()>,
    ) -> Result<bool> {
        use std::sync::atomic::Ordering;

        let (Some(&(start, _)), Some(&(_, end))) = (windows.first(), windows.last()) else {
            return Ok(true);
        };
        let wanted = |min: u64, max: u64| {
            let first = windows.partition_point(|(_, window_end)| *window_end < min);
            windows
                .get(first)
                .is_some_and(|(window_start, _)| *window_start <= max)
        };
        let (mut chunks, mut segments) = self.view();
        segments.sort_by_key(|segment| segment.manifest.min_time_nanos);
        chunks.sort_by_key(|chunk| chunk.min_nanos);

        let mut source = 0;
        for segment in &segments {
            let manifest = &segment.manifest;
            if !wanted(manifest.min_time_nanos, manifest.max_time_nanos)
                || !manifest.might_match(matchers)
            {
                self.stats.segments_pruned.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if segment.is_unreadable() {
                self.stats
                    .segments_unreadable
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            self.stats.segments_scanned.fetch_add(1, Ordering::Relaxed);
            source += 1;
            let mut abandoned = false;
            let outcome = segment.scan_columns(
                &["timestamp_nanos", "stream_id", "value"],
                windows,
                |batch| {
                    let run = SampleRun {
                        source,
                        streams: &manifest.streams,
                        stream_ids: u32_column(batch, "stream_id")?.values(),
                        timestamps: u64_column(batch, "timestamp_nanos")?.values(),
                        values: f64_column(batch, "value")?.values(),
                    };
                    if visit(run).is_break() {
                        abandoned = true;
                        return Ok(crate::segment::Flow::Stop);
                    }
                    Ok(crate::segment::Flow::Continue)
                },
            );
            self.settle_scan(segment, outcome)?;
            if abandoned {
                return Ok(false);
            }
        }

        // The buffer holds records, not columns, so each chunk is laid out as one run.
        // Its label sets are numbered by allocation: the chunk holds every one alive
        // while the run is built, so an address cannot be reused under it.
        for chunk in &chunks {
            if !wanted(chunk.min_nanos, chunk.max_nanos) {
                continue;
            }
            source += 1;
            let mut numbered: std::collections::HashMap<usize, u32> =
                std::collections::HashMap::new();
            let mut streams: Vec<Labels> = Vec::new();
            let mut stream_ids = Vec::with_capacity(chunk.records.len());
            let mut timestamps = Vec::with_capacity(chunk.records.len());
            let mut values = Vec::with_capacity(chunk.records.len());
            for record in &chunk.records {
                if record.timestamp_nanos < start || record.timestamp_nanos > end {
                    continue;
                }
                let id = *numbered
                    .entry(record.series.storage_id())
                    .or_insert_with(|| {
                        streams.push(record.series.clone());
                        u32::try_from(streams.len() - 1).unwrap_or(u32::MAX)
                    });
                stream_ids.push(id);
                timestamps.push(record.timestamp_nanos);
                values.push(record.value);
            }
            let run = SampleRun {
                source,
                streams: &streams,
                stream_ids: &stream_ids,
                timestamps: &timestamps,
                values: &values,
            };
            if visit(run).is_break() {
                return Ok(false);
            }
        }
        Ok(true)
    }

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
            let index: std::collections::HashMap<&Labels, usize> = segment
                .manifest
                .streams
                .iter()
                .enumerate()
                .map(|(id, labels)| (labels, id))
                .collect();
            for record in &records {
                if let Some(&id) = index.get(&record.series) {
                    rows.push((id, record.timestamp_nanos, record.value));
                }
            }
            rows.sort_unstable_by_key(|(id, at, _)| (*id, *at));
            let mut folds =
                vec![crate::folds::StreamFold::default(); segment.manifest.streams.len()];
            for (id, at, value) in rows {
                folds[id].add(at, value);
            }
            // One segment that cannot take its file — deleted by retention while this
            // ran, a full disk — is skipped, not the end of the backfill for every other.
            // The write is atomic, so a half file is never left for a reader to trust.
            if let Err(error) = crate::folds::StreamFolds(folds).write(&segment.dir) {
                if !crate::segment::is_gone(&error) && segment.dir.exists() {
                    tracing::warn!(segment = %segment.manifest.id, %error, "could not write a segment's summaries");
                }
                continue;
            }
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
        whole_reads_allowed: usize,
    ) -> Result<Option<Vec<(Labels, crate::folds::StreamFold)>>> {
        use crate::folds::StreamFold;
        use std::collections::HashMap;

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

        if !shortcut_pays(&segments, start_nanos, end_nanos, whole_reads_allowed) {
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
                    // A summary sealed before staleness markers were skipped may have
                    // folded one in, and its NaN cannot be taken back out. The rows can:
                    // decline, and the ordinary scan leaves the marker out.
                    if fold.increase.is_nan() || fold.last_value.is_nan() {
                        return Ok(None);
                    }
                    let key = note(&mut by_series, &mut order, labels);
                    if let Some(entry) = by_series.get_mut(&key) {
                        if !entry.precedes(fold.first_nanos) {
                            return Ok(None);
                        }
                        entry.merge_later(fold);
                    }
                }
                continue;
            }

            // Straddles an edge, or predates summaries: read this one segment. Reading the
            // *segment* rather than a time range matters — a range scan would also touch
            // its neighbours, which the loop has already taken from their summaries.
            // Deleted by retention since it was listed: decline, and the ordinary scan
            // answers from what is left. It used to fail the query.
            let rows = match segment.read::<MetricSchema>() {
                Ok(rows) => rows,
                Err(error) if crate::segment::is_gone(&error) => return Ok(None),
                Err(error) => return Err(error),
            };
            if !fold_rows(
                rows.into_iter(),
                start_nanos,
                end_nanos,
                matchers,
                &mut by_series,
                &mut order,
            ) {
                return Ok(None);
            }
        }

        if !fold_rows(
            self.buffered_between(start_nanos, end_nanos).into_iter(),
            start_nanos,
            end_nanos,
            matchers,
            &mut by_series,
            &mut order,
        ) {
            return Ok(None);
        }

        let mut out: Vec<(Labels, StreamFold)> = order
            .into_iter()
            .filter_map(|labels| by_series.remove(&labels).map(|fold| (labels, fold)))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Some(out))
    }
}

/// Whether answering a window from summaries costs less than reading it.
///
/// Decided from manifests alone — time bounds and whether a summary exists — so asking
/// is cheap even when the answer is no.
fn shortcut_pays(
    segments: &[std::sync::Arc<crate::segment::Segment>],
    start_nanos: u64,
    end_nanos: u64,
    whole_reads_allowed: usize,
) -> bool {
    let mut needs_rows = 0usize;
    let mut summarised = 0usize;
    for segment in segments {
        let m = &segment.manifest;
        if m.min_time_nanos > end_nanos || m.max_time_nanos <= start_nanos {
            continue;
        }
        if m.min_time_nanos > start_nanos && m.max_time_nanos <= end_nanos && segment.has_folds() {
            summarised += 1;
        } else {
            needs_rows += 1;
        }
    }

    // Nothing in this window is answerable from a summary, which is what a window
    // narrower than a segment looks like: a chart's fifteen minutes sits inside one
    // segment rather than containing any. Taking the shortcut anyway would read those
    // segments *whole* to answer a quarter of an hour, once per point — measured at a
    // thirty-second timeout on a panel the ordinary sliced scan answers in under two.
    //
    // And each segment the window does not contain whole is read *whole*, because a
    // time-range scan would also touch the neighbours already taken from summaries. The
    // caller says how many such reads it can afford: one evaluation point can carry a
    // handful, two hundred and fifty can carry none. Declining sends the caller to the
    // ordinary pruned scan, untouched, so a store whose segments are not summarised yet is
    // never slower than one that never had summaries at all.
    summarised > 0 && needs_rows <= whole_reads_allowed
}

/// Fold a run of raw samples into `by_series`, in time order per stream.
///
/// Order is the whole point: a fold applies the counter-reset rule by comparing each
/// sample with the previous one, so feeding it rows in storage order would invent resets
/// that never happened and inflate the result. Sorting here rather than at every call
/// site is what keeps that from being something each caller has to remember.
///
/// Returns `false` when a stream's rows start before what is already folded for it —
/// late data in a segment that overlaps an earlier one. The fold cannot be corrected
/// after the fact, so the caller abandons the shortcut.
#[must_use]
fn fold_rows(
    records: impl Iterator<Item = MetricSample>,
    start_nanos: u64,
    end_nanos: u64,
    matchers: &[telemetryd_core::LabelMatcher],
    by_series: &mut std::collections::HashMap<Labels, crate::folds::StreamFold>,
    order: &mut Vec<Labels>,
) -> bool {
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
            if !entry.precedes(at) {
                return false;
            }
            entry.add(at, value);
        }
    }
    true
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
