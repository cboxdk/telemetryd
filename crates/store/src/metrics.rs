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

/// One series' samples from one place in the store, in time order.
///
/// `source` numbers the places — a segment, a buffered chunk — oldest first. A series
/// can appear once per source; its runs from different sources need not follow on from
/// each other in time, because late data lands in a later segment.
#[derive(Debug, Clone, Copy)]
pub struct SeriesRun<'a> {
    pub source: usize,
    pub series: &'a Labels,
    pub timestamps: &'a [u64],
    pub values: &'a [f64],
}

/// Rows gathered from one segment, before they are grouped into runs. Kept between
/// segments so a scan allocates once, not once per segment.
#[derive(Debug, Default)]
struct Gathered {
    /// Rows the reader decoded for the last segment gathered, wanted or not.
    decoded: usize,
    streams: Vec<u32>,
    timestamps: Vec<u64>,
    values: Vec<f64>,
    /// The same rows grouped by stream, stable, so each stream's stay in time order.
    grouped_timestamps: Vec<u64>,
    grouped_values: Vec<f64>,
    /// `starts[s]..starts[s + 1]` is stream `s` in the grouped columns.
    starts: Vec<usize>,
}

impl Gathered {
    fn clear(&mut self) {
        self.decoded = 0;
        self.streams.clear();
        self.timestamps.clear();
        self.values.clear();
    }

    /// Read one segment's rows in `[start, end]` of the streams `allowed` says, replacing
    /// whatever was gathered before. The flag is `false` when a row names a stream the
    /// dictionary does not hold, and nothing gathered is to be used.
    fn gather(
        &mut self,
        segment: &crate::segment::Segment,
        windows: &[(u64, u64)],
        (start, end): (u64, u64),
        allowed: &[bool],
    ) -> (Result<()>, bool) {
        self.clear();
        let mut named = true;
        let outcome = scan_wanted(segment, windows, allowed, |batch| {
            self.decoded += batch.num_rows();
            let ids = u32_column(batch, "stream_id")?.values();
            let timestamps = u64_column(batch, "timestamp_nanos")?.values();
            let values = f64_column(batch, "value")?.values();
            for ((&id, &at), &value) in ids.iter().zip(timestamps).zip(values) {
                if at < start || at > end {
                    continue;
                }
                match allowed.get(id as usize) {
                    Some(true) => {
                        self.streams.push(id);
                        self.timestamps.push(at);
                        self.values.push(value);
                    }
                    Some(false) => {}
                    None => {
                        named = false;
                        return Ok(crate::segment::Flow::Stop);
                    }
                }
            }
            Ok(crate::segment::Flow::Continue)
        });
        (outcome, named)
    }

    /// Group the gathered rows by stream: a counting sort, one pass to count and one to
    /// place, because a comparison sort of a segment's rows cost more than reading them.
    fn group(&mut self, stream_count: usize) {
        self.starts.clear();
        self.starts.resize(stream_count + 1, 0);
        for &stream in &self.streams {
            self.starts[stream as usize + 1] += 1;
        }
        for at in 1..self.starts.len() {
            self.starts[at] += self.starts[at - 1];
        }
        let rows = self.streams.len();
        self.grouped_timestamps.resize(rows, 0);
        self.grouped_values.resize(rows, 0.0);
        let mut next = self.starts[..stream_count].to_vec();
        for row in 0..rows {
            let slot = &mut next[self.streams[row] as usize];
            self.grouped_timestamps[*slot] = self.timestamps[row];
            self.grouped_values[*slot] = self.values[row];
            *slot += 1;
        }
    }
}

impl crate::RecordStore<MetricSchema> {
    /// Hand `visit` every series matching `matchers` that has samples in `[start, end]`
    /// — the ends of `windows`, sorted, disjoint `(start, end)` pairs, both ends included
    /// — one run per series per source.
    ///
    /// # Why by series
    ///
    /// Everything that answers `rate` and `increase` works a series at a time: the
    /// samples of one, in order, against the windows of the evaluation points. The store
    /// used to hand out its rows as they lie — every series of a segment interleaved —
    /// and the consumer found each row's series, checked it was wanted, and bisected
    /// the evaluation points for it, row by row. On a day of one app's request
    /// histogram, a query about fifty of its eight hundred series walked all of them.
    /// Here each series is decided once per source, and the rows of the ones not wanted
    /// are never handed over, and for buffered chunks never looked at.
    ///
    /// Sources come oldest first — sealed segments by their earliest sample, then the
    /// unsealed buffer. A consumer that needs one series' runs to follow on in time
    /// checks, and returns `Break`; this then returns `Ok(false)` so it can read another
    /// way. So does a segment with a row naming no stream in its dictionary.
    pub fn scan_series(
        &self,
        windows: &[(u64, u64)],
        matchers: &[telemetryd_core::LabelMatcher],
        visit: &mut dyn FnMut(SeriesRun<'_>) -> std::ops::ControlFlow<()>,
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
        let mut gathered = Gathered::default();
        for segment in &segments {
            let manifest = &segment.manifest;
            if !wanted(manifest.min_time_nanos, manifest.max_time_nanos)
                || !manifest.might_match(matchers)
            {
                self.stats.segments_pruned.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let allowed: Vec<bool> = manifest
                .streams
                .iter()
                .map(|labels| telemetryd_core::matches_all(matchers, labels))
                .collect();
            if !allowed.iter().any(|ok| *ok) {
                // Also a segment without a dictionary, whose rows no stream id can name:
                // the caller's other read handles those.
                if manifest.streams.is_empty() && manifest.rows > 0 {
                    return Ok(false);
                }
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

            let (outcome, named) = gathered.gather(segment, windows, (start, end), &allowed);
            self.stats
                .rows_read
                .fetch_add(gathered.decoded as u64, Ordering::Relaxed);
            self.settle_scan(segment, outcome)?;
            if !named {
                return Ok(false);
            }
            gathered.group(manifest.streams.len());
            for (stream, labels) in manifest.streams.iter().enumerate() {
                let (from, to) = (gathered.starts[stream], gathered.starts[stream + 1]);
                if from == to {
                    continue;
                }
                let run = SeriesRun {
                    source,
                    series: labels,
                    timestamps: &gathered.grouped_timestamps[from..to],
                    values: &gathered.grouped_values[from..to],
                };
                if visit(run).is_break() {
                    return Ok(false);
                }
            }
        }

        let rows_read = &self.stats.rows_read;
        Ok(visit_buffered(
            &chunks,
            (start, end),
            matchers,
            &|min, max| wanted(min, max),
            source,
            &mut |run| {
                rows_read.fetch_add(run.timestamps.len() as u64, Ordering::Relaxed);
                visit(run)
            },
        ))
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

    /// Fold the buffered samples in `(start, end]` of every series matching `matchers`
    /// into `folds`, a series at a time. `false` when a series' buffered runs do not
    /// follow on from each other or from what is folded already — late data — and the
    /// caller declines the shortcut for the ordinary scan, which sorts.
    fn fold_buffered(
        &self,
        (start_nanos, end_nanos): (u64, u64),
        matchers: &[telemetryd_core::LabelMatcher],
        table: &mut telemetryd_core::series::SeriesTable,
        folds: &mut Vec<crate::folds::StreamFold>,
    ) -> bool {
        let (mut chunks, _) = self.view();
        chunks.sort_by_key(|chunk| chunk.min_nanos);
        let mut in_order = true;
        visit_buffered(
            &chunks,
            (start_nanos.saturating_add(1), end_nanos),
            matchers,
            &|min, max| min <= end_nanos && max > start_nanos,
            0,
            &mut |run| {
                let (index, added) = table.insert(run.series);
                if added {
                    folds.push(crate::folds::StreamFold::default());
                }
                let entry = &mut folds[index];
                for (&at, &value) in run.timestamps.iter().zip(run.values) {
                    if !entry.precedes(at) {
                        in_order = false;
                        return std::ops::ControlFlow::Break(());
                    }
                    entry.add(at, value);
                }
                std::ops::ControlFlow::Continue(())
            },
        );
        in_order
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

        // Keyed by the label set itself, not by the allocation behind it. Segments
        // normally share one, so a pointer would do — but "normally" is not a property to
        // rest an answer on, and getting it wrong here multiplies a rate by the number of
        // segments in the window while looking entirely ordinary. The table tries the
        // allocation first and falls back to the set, so the shared case costs no hashing
        // of names and values and the unshared case is still one series.
        let mut table = telemetryd_core::series::SeriesTable::new();
        let mut folds: Vec<StreamFold> = Vec::new();

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
            let precomputed = if wholly_inside { segment.folds() } else { None };
            let per_stream = if let Some(precomputed) = precomputed {
                // A summary sealed before staleness markers were skipped may have folded
                // one in, and its NaN cannot be taken back out. The rows can: decline, and
                // the ordinary scan leaves the marker out.
                if precomputed.0.iter().zip(&allowed).any(|(fold, ok)| {
                    *ok && fold.seen > 0 && (fold.increase.is_nan() || fold.last_value.is_nan())
                }) {
                    return Ok(None);
                }
                precomputed.0
            } else if manifest.streams.is_empty() {
                // No dictionary to number the rows by: a segment from before there was
                // one. Its records carry their own labels.
                let rows = match segment.read::<MetricSchema>() {
                    Ok(rows) => rows,
                    Err(error) if crate::segment::is_gone(&error) => return Ok(None),
                    Err(error) => return Err(error),
                };
                if !fold_rows(
                    rows.into_iter(),
                    (start_nanos, end_nanos),
                    matchers,
                    &mut table,
                    &mut folds,
                ) {
                    return Ok(None);
                }
                continue;
            } else {
                // Straddles an edge, or predates summaries: fold this one segment's
                // columns. The *segment*, rather than a time range, because a range scan
                // would also touch its neighbours, which this loop takes from their
                // summaries.
                match fold_segment_columns(
                    segment,
                    (start_nanos, end_nanos),
                    &allowed,
                    &self.stats.rows_read,
                )? {
                    Some(per_stream) => per_stream,
                    None => return Ok(None),
                }
            };
            for (stream, labels) in manifest.streams.iter().enumerate() {
                if !allowed.get(stream).copied().unwrap_or(false) {
                    continue;
                }
                let Some(fold) = per_stream.get(stream).filter(|f| f.seen > 0) else {
                    continue;
                };
                let (index, added) = table.insert(labels);
                if added {
                    folds.push(StreamFold::default());
                }
                let entry = &mut folds[index];
                if !entry.precedes(fold.first_nanos) {
                    return Ok(None);
                }
                entry.merge_later(fold);
            }
        }

        if !self.fold_buffered((start_nanos, end_nanos), matchers, &mut table, &mut folds) {
            return Ok(None);
        }

        let mut out: Vec<(Labels, StreamFold)> = table
            .into_series()
            .into_iter()
            .zip(folds)
            .filter(|(_, fold)| fold.seen > 0)
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Some(out))
    }
}

/// Read the columns a fold needs from the rows of `segment` that can matter: the streams
/// `allowed` names, within `windows`.
///
/// A segment laid out series by series is read as exactly those streams' rows, which is
/// where a query naming a few series of many saves its time. Any other is read by time
/// range, every stream's rows, and the caller skips the ones it does not want.
fn scan_wanted<F>(
    segment: &crate::segment::Segment,
    windows: &[(u64, u64)],
    allowed: &[bool],
    visit: F,
) -> Result<()>
where
    F: FnMut(&RecordBatch) -> Result<crate::segment::Flow>,
{
    const COLUMNS: [&str; 3] = ["timestamp_nanos", "stream_id", "value"];
    match stream_row_ranges(&segment.manifest, allowed, windows) {
        Some(ranges) => segment.scan_row_ranges(&COLUMNS, &ranges, visit),
        None => segment.scan_columns(&COLUMNS, windows, visit),
    }
}

/// The row ranges of the streams `allowed` names whose samples reach into `windows`, in a
/// segment laid out series by series. `None` for a segment laid out in time order, or one
/// whose per-stream row counts do not add up to its rows — read by time range instead.
fn stream_row_ranges(
    manifest: &crate::segment::SegmentManifest,
    allowed: &[bool],
    windows: &[(u64, u64)],
) -> Option<Vec<(usize, usize)>> {
    if !manifest.stream_major
        || manifest.stream_rows.len() != manifest.streams.len()
        || allowed.len() != manifest.streams.len()
        || manifest
            .stream_rows
            .iter()
            .map(|rows| u64::from(*rows))
            .sum::<u64>()
            != manifest.rows
    {
        return None;
    }
    let bounded = manifest.stream_bounds.len() == manifest.streams.len();
    let reaches = |stream: usize| {
        if !bounded {
            return true;
        }
        let (min, max) = manifest.stream_bounds[stream];
        let first = windows.partition_point(|(_, end)| *end < min);
        windows.get(first).is_some_and(|(start, _)| *start <= max)
    };
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut at = 0usize;
    for (stream, rows) in manifest.stream_rows.iter().enumerate() {
        let end = at + *rows as usize;
        if allowed[stream] && end > at && reaches(stream) {
            match ranges.last_mut() {
                Some(last) if last.1 == at => last.1 = end,
                _ => ranges.push((at, end)),
            }
        }
        at = end;
    }
    Some(ranges)
}

/// A chunk as [`visit_buffered`] reads it: its records, and them grouped by series.
type Grouped<'a> = (
    &'a crate::records::Chunk<MetricSchema>,
    &'a crate::records::SeriesOrder,
);

/// Every label set among `chunks` by address, ascending, each with whether `matchers`
/// select it.
///
/// Every chunk lists its runs by address, and nearly always the same addresses as the
/// chunk before — the same series, a few samples further on — so a chunk whose list is
/// the last one's again costs one comparison of the two, and only a chunk bringing new
/// series is walked.
fn decide<'a>(
    chunks: &[Grouped<'a>],
    matchers: &[telemetryd_core::LabelMatcher],
) -> Vec<(usize, Option<&'a Labels>)> {
    let mut decided: Vec<(usize, Option<&'a Labels>)> = Vec::new();
    let mut previous: Option<&[usize]> = None;
    for &(chunk, order) in chunks {
        if previous == Some(order.keys.as_slice()) {
            continue;
        }
        previous = Some(order.keys.as_slice());
        let mut fresh = Vec::new();
        let mut known = 0usize;
        for (run, &key) in order.keys.iter().enumerate() {
            while known < decided.len() && decided[known].0 < key {
                known += 1;
            }
            if known < decided.len() && decided[known].0 == key {
                continue;
            }
            let set = &chunk.records[order.row(order.run(run).start)].series;
            fresh.push((
                key,
                telemetryd_core::matches_all(matchers, set).then_some(set),
            ));
        }
        if !fresh.is_empty() {
            decided.extend(fresh);
            decided.sort_unstable_by_key(|(key, _)| *key);
            decided.dedup_by_key(|(key, _)| *key);
        }
    }
    decided
}

/// Hand `visit` the buffered samples in `[start, end]` of every series matching
/// `matchers`, one run per series for the whole buffer, as source `source + 1`. `false`
/// when `visit` stopped the scan.
///
/// The buffer is hundreds of small chunks — every query freezes the one being filled, so
/// on a server being read they hold seconds each — and every chunk holds every series a
/// few samples deep. Handed out chunk by chunk, a series came in runs of a handful, and
/// the work per run was the cost of a query. So a wanted series' pieces from every chunk
/// are joined into one run, series after series in address order, each chunk keeping a
/// cursor into its own address-ordered runs that only moves forward: nothing is searched
/// and nothing is held but the run being built. The records of a series not wanted are
/// never touched.
///
/// Chunks are taken in order of their earliest sample, so a series' pieces normally
/// join in time order. When late data breaks that the run is sorted, stably: samples at
/// one instant keep the order their chunks came in.
fn visit_buffered(
    chunks: &[std::sync::Arc<crate::records::Chunk<MetricSchema>>],
    (start, end): (u64, u64),
    matchers: &[telemetryd_core::LabelMatcher],
    wanted: &dyn Fn(u64, u64) -> bool,
    source: usize,
    visit: &mut dyn FnMut(SeriesRun<'_>) -> std::ops::ControlFlow<()>,
) -> bool {
    let chunks: Vec<Grouped<'_>> = chunks
        .iter()
        .filter(|chunk| wanted(chunk.min_nanos, chunk.max_nanos))
        .map(|chunk| (&**chunk, chunk.by_series()))
        .collect();
    let mut cursors = vec![0usize; chunks.len()];
    let mut timestamps: Vec<u64> = Vec::new();
    let mut values: Vec<f64> = Vec::new();
    for (key, series) in decide(&chunks, matchers) {
        let Some(series) = series else {
            continue;
        };
        timestamps.clear();
        values.clear();
        let mut ordered = true;
        for (&(chunk, order), cursor) in chunks.iter().zip(&mut cursors) {
            while *cursor < order.keys.len() && order.keys[*cursor] < key {
                *cursor += 1;
            }
            if order.keys.get(*cursor) != Some(&key) {
                continue;
            }
            let run = order.run(*cursor);
            let at = |position: usize| chunk.records[order.row(position)].timestamp_nanos;
            // A chunk wholly inside the span, which for a long window is nearly all of
            // them, needs no search.
            let (first, last) = if chunk.min_nanos >= start && chunk.max_nanos <= end {
                (run.start, run.end)
            } else {
                (
                    first_where(run.clone(), |position| at(position) >= start),
                    first_where(run, |position| at(position) > end),
                )
            };
            for position in first..last {
                let record = &chunk.records[order.row(position)];
                ordered &= timestamps
                    .last()
                    .is_none_or(|previous| *previous <= record.timestamp_nanos);
                timestamps.push(record.timestamp_nanos);
                values.push(record.value);
            }
        }
        if timestamps.is_empty() {
            continue;
        }
        if !ordered {
            let mut paired: Vec<(u64, f64)> = timestamps
                .iter()
                .copied()
                .zip(values.iter().copied())
                .collect();
            paired.sort_by_key(|(at, _)| *at);
            timestamps = paired.iter().map(|(at, _)| *at).collect();
            values = paired.iter().map(|(_, value)| *value).collect();
        }
        let run = SeriesRun {
            source: source + 1,
            series,
            timestamps: &timestamps,
            values: &values,
        };
        if visit(run).is_break() {
            return false;
        }
    }
    true
}

/// The first position in `range` where `holds` becomes true, for a predicate that is
/// false and then true across it; `range.end` when it never does.
fn first_where(range: std::ops::Range<usize>, holds: impl Fn(usize) -> bool) -> usize {
    let (mut low, mut high) = (range.start, range.end);
    while low < high {
        let middle = low + (high - low) / 2;
        if holds(middle) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    low
}

/// One segment's rows in `(start, end]`, folded per stream straight from its columns.
///
/// A segment's rows are in time order, so each stream's rows are too, and a fold walks
/// them as they come: nothing is materialised and nothing is sorted. Reading the segment
/// into records and sorting them by label set was two thirds of a day-long quantile.
///
/// `None` when the rows cannot be folded this way — a row naming a stream the dictionary
/// does not hold, or a segment deleted by retention since it was listed — and the caller
/// declines the shortcut.
fn fold_segment_columns(
    segment: &crate::segment::Segment,
    (start_nanos, end_nanos): (u64, u64),
    allowed: &[bool],
    rows_read: &std::sync::atomic::AtomicU64,
) -> Result<Option<Vec<crate::folds::StreamFold>>> {
    let mut folds = vec![crate::folds::StreamFold::default(); segment.manifest.streams.len()];
    let mut usable = true;
    let scanned = scan_wanted(
        segment,
        &[(start_nanos.saturating_add(1), end_nanos)],
        allowed,
        |batch| {
            rows_read.fetch_add(
                batch.num_rows() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            let ids = u32_column(batch, "stream_id")?.values();
            let timestamps = u64_column(batch, "timestamp_nanos")?.values();
            let values = f64_column(batch, "value")?.values();
            for ((&id, &at), &value) in ids.iter().zip(timestamps).zip(values) {
                if at <= start_nanos || at > end_nanos {
                    continue;
                }
                let stream = id as usize;
                let (Some(true), Some(fold)) =
                    (allowed.get(stream).copied(), folds.get_mut(stream))
                else {
                    if stream >= allowed.len() {
                        usable = false;
                        return Ok(crate::segment::Flow::Stop);
                    }
                    continue;
                };
                if !fold.precedes(at) {
                    usable = false;
                    return Ok(crate::segment::Flow::Stop);
                }
                fold.add(at, value);
            }
            Ok(crate::segment::Flow::Continue)
        },
    );
    match scanned {
        Ok(()) if usable => Ok(Some(folds)),
        Ok(()) => Ok(None),
        Err(error) if crate::segment::is_gone(&error) => Ok(None),
        Err(error) => Err(error),
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
    (start_nanos, end_nanos): (u64, u64),
    matchers: &[telemetryd_core::LabelMatcher],
    table: &mut telemetryd_core::series::SeriesTable,
    folds: &mut Vec<crate::folds::StreamFold>,
) -> bool {
    let mut rows: Vec<(usize, u64, f64)> = Vec::new();
    for record in records {
        if record.timestamp_nanos <= start_nanos
            || record.timestamp_nanos > end_nanos
            || !telemetryd_core::matches_all(matchers, &record.series)
        {
            continue;
        }
        let (index, added) = table.insert(&record.series);
        if added {
            folds.push(crate::folds::StreamFold::default());
        }
        rows.push((index, record.timestamp_nanos, record.value));
    }
    // By series and then time; the sort is stable, so two samples at one instant keep
    // the order they were stored in.
    rows.sort_by_key(|(index, at, _)| (*index, *at));
    for (index, at, value) in rows {
        let entry = &mut folds[index];
        if !entry.precedes(at) {
            return false;
        }
        entry.add(at, value);
    }
    true
}

/// A logged sample's timestamp, the bytes of its label set, and what follows them.
///
/// Postcard lays a [`MetricSample`] out field by field: the timestamp as a varint, the
/// labels as a varint count of varint-length-prefixed strings, then the value and kind.
/// `None` when the bytes do not walk that way, and the caller decodes in full.
fn split_sample(payload: &[u8]) -> Option<(u64, &[u8], &[u8])> {
    fn varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *bytes.get(*at)?;
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }
    let mut at = 0;
    let timestamp = varint(payload, &mut at)?;
    let start = at;
    let pairs = varint(payload, &mut at)?;
    for _ in 0..pairs.checked_mul(2)? {
        let len = usize::try_from(varint(payload, &mut at)?).ok()?;
        at = at.checked_add(len).filter(|end| *end <= payload.len())?;
    }
    Some((timestamp, &payload[start..at], &payload[at..]))
}

impl RecordSchema for MetricSchema {
    type Record = MetricSample;

    const STREAM_MAJOR: bool = true;

    fn replay_decode(
        payload: &[u8],
        seen: &mut crate::schema::ReplayLabels,
    ) -> postcard::Result<Self::Record> {
        let Some((timestamp_nanos, series, rest)) = split_sample(payload) else {
            return Self::decode_wal(payload);
        };
        if let Some(labels) = seen.get(series) {
            let (value, kind): (f64, MetricKind) = postcard::from_bytes(rest)?;
            return Ok(MetricSample {
                timestamp_nanos,
                series: labels.clone(),
                value,
                kind,
            });
        }
        let sample = Self::decode_wal(payload)?;
        seen.remember(series, &sample.series);
        Ok(sample)
    }

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

    fn to_batch_by_stream(records: &[Self::Record]) -> Result<(RecordBatch, Vec<Labels>)> {
        let mut interner = crate::segment::StreamInterner::default();
        let mut ranked: Vec<u32> = records.iter().map(|s| interner.intern(&s.series)).collect();
        let streams = interner.into_streams();

        // Each stream's place in label-set order, which the ids are rewritten to.
        let mut ordered: Vec<usize> = (0..streams.len()).collect();
        ordered.sort_by(|a, b| streams[*a].cmp(&streams[*b]));
        let mut rank = vec![0u32; streams.len()];
        for (new, old) in ordered.iter().enumerate() {
            rank[*old] = u32::try_from(new).unwrap_or(u32::MAX);
        }
        for id in &mut ranked {
            *id = rank[*id as usize];
        }

        // Where each stream's rows begin once grouped.
        let mut counts = vec![0usize; streams.len()];
        for id in &ranked {
            counts[*id as usize] += 1;
        }
        let mut next = Vec::with_capacity(streams.len());
        let mut at = 0usize;
        for count in &counts {
            next.push(at);
            at += count;
        }

        // Scattered, not gathered: the records are read once, in the order they lie, and
        // each written to its stream's next slot. Every stream writes forward through its
        // own stretch, so the writes stay in cache where reading the records out of order
        // — once per column — missed it on nearly every row.
        let mut timestamps = vec![0u64; records.len()];
        let mut values = vec![0f64; records.len()];
        let mut kinds = vec![MetricKind::Unknown; records.len()];
        for (record, id) in records.iter().zip(&ranked) {
            let slot = &mut next[*id as usize];
            timestamps[*slot] = record.timestamp_nanos;
            values[*slot] = record.value;
            kinds[*slot] = record.kind;
            *slot += 1;
        }
        drop(ranked);

        let sorted: Vec<Labels> = ordered
            .into_iter()
            .map(|old| streams[old].clone())
            .collect();
        // `counts` is by new id, so the grouped ids and names are each stream's, repeated
        // for its rows, in order.
        let ids =
            UInt32Array::from_iter_values(counts.iter().enumerate().flat_map(|(id, count)| {
                std::iter::repeat_n(u32::try_from(id).unwrap_or(u32::MAX), *count)
            }));
        let mut names =
            arrow::array::StringBuilder::with_capacity(records.len(), records.len() * 16);
        for (series, count) in sorted.iter().zip(&counts) {
            let name = series.get(METRIC_NAME_LABEL).unwrap_or("");
            for _ in 0..*count {
                names.append_value(name);
            }
        }
        let names = names.finish();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(timestamps)),
            Arc::new(ids),
            Arc::new(Float64Array::from(values)),
            Arc::new(names),
            Arc::new(StringArray::from_iter_values(
                kinds.iter().map(|k| k.as_str()),
            )),
        ];
        let batch = RecordBatch::try_new(Self::arrow_schema(), columns)
            .map_err(|e| Error::Config(format!("building a metric record batch: {e}")))?;
        Ok((batch, sorted))
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

    fn index_labels_mut(record: &mut Self::Record) -> &mut Labels {
        &mut record.series
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

    /// Replay's shortcut must decode exactly what the full decoder does — including
    /// label values long enough for multi-byte varints, and values that are not finite.
    #[test]
    fn replay_decoding_agrees_with_full_decoding() {
        let long = "x".repeat(300);
        let mut series = Labels::new();
        series.insert(METRIC_NAME_LABEL, "m");
        series.insert("long", long);
        series.insert("i", "é");
        let mut seen = crate::schema::ReplayLabels::default();
        for (i, value) in [0.0, -1.5, f64::INFINITY, 1e300, f64::MIN_POSITIVE]
            .into_iter()
            .enumerate()
        {
            for kind in [MetricKind::Gauge, MetricKind::Counter, MetricKind::Unknown] {
                let original = MetricSample {
                    timestamp_nanos: u64::MAX - i as u64,
                    series: series.clone(),
                    value,
                    kind,
                };
                let payload = postcard::to_allocvec(&original).unwrap();
                // Twice: the first decodes in full and remembers, the second hits.
                for _ in 0..2 {
                    let decoded = MetricSchema::replay_decode(&payload, &mut seen).unwrap();
                    assert_eq!(decoded, original);
                }
            }
        }
        assert!(
            split_sample(&[0xff]).is_none(),
            "a truncated varint is no sample"
        );
        assert!(
            split_sample(&[1, 1, 9, b'a']).is_none(),
            "nor a string past the end"
        );
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
