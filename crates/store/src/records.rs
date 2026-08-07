//! The record store: write-ahead log, in-memory buffer, sealing and the segment
//! catalogue, generic over [`RecordSchema`].
//!
//! # The query path reads two places
//!
//! Sealed segments *and* the live buffer. Data is queryable the moment it is
//! accepted, not an hour later when its segment seals — a telemetry store where the
//! last hour is invisible is not useful for the thing people actually do with it,
//! which is look at what just happened.
//!
//! # Buffering is by arrival, not by event time
//!
//! The buffer window is wall-clock since it opened. Keying it on event time would
//! mean a single late-arriving record forces a seal and produces a one-row segment,
//! and late records are normal — a batching client, a retry, a clock skew. The
//! manifest records the actual event-time bounds, which can span more than the
//! window, and query pruning uses those.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use telemetryd_core::config::{Compression, StorageConfig, WalSync};
use telemetryd_core::{Error, LabelMatcher, Labels, Result, matches_all};

/// Below this many segments a query stays on one thread.
///
/// Spawning costs more than the manifest check a pruned segment needs, and the queries
/// that touch few segments are the ones already answered in about a millisecond.
const MIN_SEGMENTS_PER_EXTRA_WORKER: usize = 4;

use crate::schema::RecordSchema;
use crate::segment::{Flow, SealOptions, Segment, seal};
use crate::topk::{Order, SharedCutoff, TopK};
use crate::wal::{self, Wal};

/// Sizing and durability knobs, lifted out of [`StorageConfig`].
#[derive(Debug, Clone, Copy)]
pub struct StoreSettings {
    pub segment_duration: Duration,
    pub max_segment_bytes: u64,
    pub wal_sync: WalSync,
    pub wal_sync_interval: Duration,
    pub compression: Compression,
    /// Upper bound on threads used to scan sealed segments for one query.
    pub query_parallelism: usize,
}

impl From<&StorageConfig> for StoreSettings {
    fn from(config: &StorageConfig) -> Self {
        Self {
            segment_duration: config.segment_duration.get(),
            max_segment_bytes: config.max_segment_bytes.as_u64(),
            wal_sync: config.wal_sync,
            wal_sync_interval: config.wal_sync_interval,
            compression: config.compression,
            query_parallelism: config.resolved_query_parallelism(),
        }
    }
}

/// One signal's records: everything buffered, everything sealed.
pub struct RecordStore<S: RecordSchema> {
    segments_dir: PathBuf,
    tmp_dir: PathBuf,
    settings: StoreSettings,
    /// The write-ahead log **and** the in-memory buffer, under one lock.
    ///
    /// They were separate locks once, and that was a data-loss bug: a seal landing
    /// between "appended to the WAL" and "pushed to the buffer" took a buffer that did
    /// not contain the record, then truncated the WAL segment that did. The record
    /// existed only in memory and vanished on restart. Nothing errored — it is exactly
    /// the failure a concurrency test exists to find. A record must become durable and
    /// queryable atomically.
    writer: Mutex<Writer<S>>,
    catalogue: RwLock<Vec<Arc<Segment>>>,
    seal_sequence: AtomicU64,
    stats: Stats,
}

/// The write side: log and buffer, kept consistent with each other.
struct Writer<S: RecordSchema> {
    wal: Wal,
    buffer: Buffer<S>,
}

impl<S: RecordSchema> std::fmt::Debug for RecordStore<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordStore")
            .field("signal", &S::SIGNAL)
            .field("segments", &self.catalogue.read().map_or(0, |c| c.len()))
            .finish_non_exhaustive()
    }
}

struct Buffer<S: RecordSchema> {
    records: Vec<S::Record>,
    bytes: usize,
    opened_at: Instant,
}

impl<S: RecordSchema> Buffer<S> {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            bytes: 0,
            opened_at: Instant::now(),
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    appended: AtomicU64,
    sealed_segments: AtomicU64,
    sealed_records: AtomicU64,
    recovered: AtomicU64,
    /// Segments actually opened and decoded. The counterpart to `segments_pruned`:
    /// together they say how much of the store a query had to touch, which is the
    /// number to watch when queries get slow.
    segments_scanned: AtomicU64,
    /// Segments a query skipped without any I/O — by time range, label index, Bloom
    /// filter, or the limit cutoff.
    segments_pruned: AtomicU64,
}

/// A bounded query request.
#[derive(Clone, Copy)]
pub struct Scan<'a> {
    pub start_nanos: u64,
    pub end_nanos: u64,
    /// `0` means unbounded.
    pub limit: usize,
    pub order: Order,
    /// An exact value for the schema's key column, when the query is a point lookup.
    /// Lets the per-segment Bloom filter rule segments out before any I/O.
    pub exact_key: Option<&'a str>,
    /// Optional columnar narrowing, applied before any row is decoded.
    ///
    /// May over-select; the record predicate remains the authority. Supplying one is
    /// what turns a line filter from "decode every row, then test" into "test the
    /// Arrow string buffer, then decode the few that matched".
    pub columns: Option<crate::schema::ColumnFilter<'a>>,
}

impl std::fmt::Debug for Scan<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scan")
            .field("start_nanos", &self.start_nanos)
            .field("end_nanos", &self.end_nanos)
            .field("limit", &self.limit)
            .field("order", &self.order)
            .field("exact_key", &self.exact_key)
            .field("columns", &self.columns.is_some())
            .finish()
    }
}

impl<'a> Scan<'a> {
    /// An unbounded scan over a time range.
    #[must_use]
    pub fn range(start_nanos: u64, end_nanos: u64) -> Self {
        Self {
            start_nanos,
            end_nanos,
            limit: 0,
            order: Order::Ascending,
            exact_key: None,
            columns: None,
        }
    }

    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    #[must_use]
    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    #[must_use]
    pub fn exact_key(mut self, key: &'a str) -> Self {
        self.exact_key = Some(key);
        self
    }

    #[must_use]
    pub fn columns(mut self, filter: crate::schema::ColumnFilter<'a>) -> Self {
        self.columns = Some(filter);
        self
    }
}

/// A point-in-time view of one signal's storage.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RecordStoreStatus {
    pub buffered_records: u64,
    pub buffered_bytes: u64,
    pub segments: u64,
    pub segment_rows: u64,
    pub segment_bytes: u64,
    pub appended_records: u64,
    pub sealed_segments: u64,
    pub recovered_records: u64,
    pub oldest_record_nanos: Option<u64>,
    pub newest_record_nanos: Option<u64>,
    pub segments_scanned: u64,
    pub segments_pruned: u64,
}

impl<S: RecordSchema> RecordStore<S> {
    /// Open the store, rebuilding the catalogue and replaying whatever the WAL still
    /// holds that is not already durable in a sealed segment.
    pub fn open(
        wal_dir: &std::path::Path,
        segments_dir: PathBuf,
        tmp_dir: PathBuf,
        settings: StoreSettings,
    ) -> Result<Self> {
        std::fs::create_dir_all(&segments_dir)
            .map_err(|e| Error::io(format!("creating {}", segments_dir.display()), e))?;

        let segments = crate::segment::scan(&segments_dir)?;
        // Only WAL segments *after* the highest sealed sequence still hold records we
        // do not have on disk. Replaying more would duplicate them.
        let sealed_through = segments
            .iter()
            .map(|s| s.manifest.wal_sequence)
            .max()
            .unwrap_or(0);

        let mut buffer = Buffer::<S>::new();
        let replayed = wal::replay_from(wal_dir, sealed_through, |payload| {
            match postcard::from_bytes::<S::Record>(payload) {
                Ok(record) => {
                    buffer.bytes += S::size_estimate(&record);
                    buffer.records.push(record);
                    Ok(())
                }
                Err(e) => {
                    // A record we cannot decode is a format problem, not a reason to
                    // refuse to start and lose everything else in the log.
                    tracing::error!(
                        signal = %S::SIGNAL,
                        error = %e,
                        "skipping an undecodable write-ahead log record"
                    );
                    Ok(())
                }
            }
        })?;

        let wal = Wal::open(
            wal_dir,
            settings.wal_sync,
            settings.wal_sync_interval,
            settings.max_segment_bytes,
        )?;

        if !buffer.records.is_empty() {
            tracing::info!(
                signal = %S::SIGNAL,
                records = buffer.records.len(),
                "recovered buffered records from the write-ahead log"
            );
        }
        let recovered = buffer.records.len() as u64;
        let _ = replayed;

        let seal_sequence = segments
            .iter()
            .filter_map(|s| s.manifest.id.rsplit_once('-'))
            .filter_map(|(_, seq)| seq.parse::<u64>().ok())
            .max()
            .unwrap_or(0);

        let stats = Stats::default();
        stats.recovered.store(recovered, Ordering::Relaxed);

        Ok(Self {
            segments_dir,
            tmp_dir,
            settings,
            writer: Mutex::new(Writer { wal, buffer }),
            catalogue: RwLock::new(segments.into_iter().map(Arc::new).collect()),
            seal_sequence: AtomicU64::new(seal_sequence),
            stats,
        })
    }

    /// Append records durably, then buffer them for query.
    ///
    /// The WAL write comes first: a record that is in the buffer but not the log would
    /// be lost by a crash after we told the client it was accepted.
    pub fn append(&self, records: &[S::Record]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let should_seal = {
            let mut writer = lock(&self.writer);
            for record in records {
                let payload = postcard::to_stdvec(record)
                    .map_err(|e| Error::Config(format!("encoding a {} record: {e}", S::SIGNAL)))?;
                writer.wal.append(&payload)?;
                writer.buffer.bytes += S::size_estimate(record);
                writer.buffer.records.push(record.clone());
            }
            writer.buffer.bytes as u64 >= self.settings.max_segment_bytes
        };

        self.stats
            .appended
            .fetch_add(records.len() as u64, Ordering::Relaxed);

        if should_seal {
            self.seal_now()?;
        }
        Ok(())
    }

    /// Seal if the buffer has been open longer than the segment window.
    ///
    /// Driven by a background ticker; separate from [`Self::seal_now`] so a caller can
    /// force a seal (shutdown, tests) without waiting for the window.
    pub fn maybe_seal(&self) -> Result<Option<Arc<Segment>>> {
        let due = {
            let writer = lock(&self.writer);
            !writer.buffer.records.is_empty()
                && writer.buffer.opened_at.elapsed() >= self.settings.segment_duration
        };
        if due { self.seal_now() } else { Ok(None) }
    }

    /// Seal the current buffer into an immutable segment.
    ///
    /// The Parquet write happens **without** holding the buffer lock, so a large seal
    /// does not stall ingest. Correctness across that gap comes from the WAL: the log
    /// is rotated before the buffer is taken, and only truncated after the segment is
    /// published, so a crash anywhere in between recovers rather than loses.
    pub fn seal_now(&self) -> Result<Option<Arc<Segment>>> {
        let (records, wal_sequence) = {
            let mut writer = lock(&self.writer);
            if writer.buffer.records.is_empty() {
                return Ok(None);
            }
            // Rotate and drain under the same lock that appends hold, so the boundary
            // between "in this segment" and "still in the log" is exact.
            let wal_sequence = writer.wal.rotate()?;
            let records = std::mem::take(&mut writer.buffer.records);
            writer.buffer.bytes = 0;
            writer.buffer.opened_at = Instant::now();
            (records, wal_sequence)
        };

        let sequence = self.seal_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let segment = seal::<S>(
            &records,
            SealOptions {
                segments_dir: &self.segments_dir,
                tmp_dir: &self.tmp_dir,
                compression: self.settings.compression,
                now_nanos: crate::now_nanos(),
                sequence,
                wal_sequence,
            },
        );

        let segment = match segment {
            Ok(segment) => Arc::new(segment),
            Err(error) => {
                // Put the records back rather than dropping them on the floor. They
                // are still in the WAL, so they would survive a restart either way,
                // but a running process must not silently lose queryable data.
                let mut writer = lock(&self.writer);
                let bytes: usize = records.iter().map(S::size_estimate).sum();
                let mut restored = records;
                restored.append(&mut writer.buffer.records);
                writer.buffer.records = restored;
                writer.buffer.bytes += bytes;
                tracing::error!(
                    signal = %S::SIGNAL,
                    error = %error,
                    "sealing failed; records remain buffered and in the write-ahead log"
                );
                return Err(error);
            }
        };

        self.stats.sealed_segments.fetch_add(1, Ordering::Relaxed);
        self.stats
            .sealed_records
            .fetch_add(segment.manifest.rows, Ordering::Relaxed);

        lock_write(&self.catalogue).push(Arc::clone(&segment));

        // Only now is the log redundant.
        if let Err(e) = lock(&self.writer).wal.remove_up_to(wal_sequence) {
            tracing::warn!(
                signal = %S::SIGNAL,
                error = %e,
                "could not truncate the write-ahead log after sealing; \
                 the records are safe, the log is just larger than it needs to be"
            );
        }

        Ok(Some(segment))
    }

    /// Flush and fsync the write-ahead log without sealing.
    pub fn sync(&self) -> Result<()> {
        lock(&self.writer).wal.sync()
    }

    /// Apply the configured sync policy; called from the background ticker.
    pub fn maybe_sync(&self) -> Result<()> {
        lock(&self.writer).wal.maybe_sync()
    }

    /// Every sealed segment, oldest first.
    pub fn segments(&self) -> Vec<Arc<Segment>> {
        lock_read(&self.catalogue).clone()
    }

    /// Drop a segment from the catalogue and delete it from disk. Used by retention.
    pub fn remove_segment(&self, id: &str) -> Result<bool> {
        let removed = {
            let mut catalogue = lock_write(&self.catalogue);
            catalogue
                .iter()
                .position(|s| s.manifest.id == id)
                .map(|index| catalogue.remove(index))
        };
        match removed {
            Some(segment) => {
                segment.delete()?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Query records in `[start_nanos, end_nanos]` matching `matchers`, unbounded.
    ///
    /// Prefer [`Self::scan`] with a limit wherever the caller has one: this variant
    /// materialises every match.
    pub fn query(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&S::Record) -> bool + Sync),
    ) -> Result<Vec<S::Record>> {
        self.scan(
            Scan {
                start_nanos,
                end_nanos,
                limit: 0,
                order: Order::Ascending,
                exact_key: None,
                columns: None,
            },
            matchers,
            extra,
        )
    }

    /// Query with a bound, in a chosen order.
    ///
    /// Three things keep this from scaling with the size of the store rather than the
    /// size of the answer:
    ///
    /// 1. **Manifest pruning** skips segments that cannot match, without opening them.
    /// 2. **Streaming decode** processes one Arrow batch at a time, so a query that
    ///    matches three rows never allocates a whole segment.
    /// 3. **A bounded collector** holds `limit` records rather than every match, and
    ///    tells us when a remaining segment is entirely worse than what we already
    ///    have — at which point the scan stops.
    pub fn scan(
        &self,
        request: Scan,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&S::Record) -> bool + Sync),
    ) -> Result<Vec<S::Record>> {
        let mut collector = TopK::new(request.limit, request.order);

        // The live buffer first: it holds the newest data, so filling the collector
        // from it maximises how many sealed segments the cutoff can then skip.
        {
            collector.set_unit(0);
            let writer = lock(&self.writer);
            for record in &writer.buffer.records {
                if Self::selects(record, &request, matchers, extra) {
                    collector.push(S::timestamp(record), record.clone());
                }
            }
        }

        // Walk segments from the end the caller cares about, so the cutoff tightens as
        // fast as possible.
        let mut segments = self.segments();
        match request.order {
            Order::Descending => {
                segments.sort_by_key(|s| std::cmp::Reverse(s.manifest.max_time_nanos));
            }
            Order::Ascending => segments.sort_by_key(|s| s.manifest.min_time_nanos),
        }

        let workers = self.scan_workers(&request, segments.len());
        if workers <= 1 {
            for (ordinal, segment) in segments.iter().enumerate() {
                self.scan_segment(segment, ordinal, &request, matchers, extra, &mut collector)?;
            }
            return Ok(collector.into_sorted());
        }

        // Parallel: each worker keeps its own collector and they are merged at the end.
        //
        // The sequential walk is not just a loop — the collector's cutoff tightens as it
        // goes, and later segments get skipped because of what earlier ones found. Split
        // that across threads naively and every worker starts from nothing, so the
        // pruning that makes bounded queries fast disappears exactly when there is most
        // work to divide.
        //
        // So the workers share a cutoff. For a descending query the merged top-k is the
        // top-k of the union, and the union already contains every worker's k results —
        // so the merged cutoff is at least the largest individual one. Publishing the
        // maximum is therefore always safe: it can only ever be tighter than the truth
        // in the direction that skips *less*, never more. Ascending is the mirror image.
        let next = std::sync::atomic::AtomicUsize::new(0);
        let shared = SharedCutoff::new(request.order);
        let collected = std::sync::Mutex::new(Vec::with_capacity(workers));
        let first_error = std::sync::Mutex::new(None::<Error>);

        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    let mut local = TopK::new(request.limit, request.order);
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        let Some(segment) = segments.get(index) else {
                            break;
                        };

                        // Another worker may already have proved this segment cannot
                        // contribute. Checking before opening it is the whole point.
                        if shared.can_skip(
                            segment.manifest.min_time_nanos,
                            segment.manifest.max_time_nanos,
                        ) {
                            self.stats.segments_pruned.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        match self
                            .scan_segment(segment, index, &request, matchers, extra, &mut local)
                        {
                            Ok(()) => shared.publish(&local),
                            Err(err) => {
                                let mut slot = lock(&first_error);
                                if slot.is_none() {
                                    *slot = Some(err);
                                }
                                break;
                            }
                        }
                    }
                    lock(&collected).push(local);
                });
            }
        });

        if let Some(err) = lock(&first_error).take() {
            return Err(err);
        }
        for local in lock(&collected).drain(..) {
            collector.merge(local);
        }

        Ok(collector.into_sorted())
    }

    /// How many threads to scan with.
    ///
    /// **Only unbounded queries are parallelised**, and that is the measured result
    /// rather than a guess. A limited query is fast because the collector's cutoff
    /// tightens on the first segment and the other nineteen are then skipped without
    /// being opened; four workers instead race ahead and do real work on segments the
    /// cutoff would have discarded. On the benchmark store that made `limit=100` go
    /// from 1.45 ms to 2.33 ms — parallelism bought nothing and cost 60%.
    ///
    /// An unbounded scan has no cutoff to lose, so the work divides. It gains about
    /// 1.3× at four workers — real, but nothing like linear, because materialising a
    /// hundred thousand records is bound by allocation rather than by decode.
    ///
    /// Conservative on purpose besides: this process is accepting writes at the same
    /// time, and handing every core to one query makes ingest stutter under exactly
    /// the load an operator is trying to look at.
    fn scan_workers(&self, request: &Scan, segments: usize) -> usize {
        let configured = self.settings.query_parallelism;
        if configured <= 1 || request.limit != 0 || segments < MIN_SEGMENTS_PER_EXTRA_WORKER {
            return 1;
        }
        configured
            .min(segments / MIN_SEGMENTS_PER_EXTRA_WORKER)
            .max(1)
    }

    /// Scan one sealed segment into `collector`.
    ///
    /// Shared verbatim by the sequential and parallel drivers: the difference between
    /// them is only which thread calls this and what the collector is, and a second
    /// copy of this logic would be a correctness gap waiting to open.
    fn scan_segment(
        &self,
        segment: &Segment,
        ordinal: usize,
        request: &Scan,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&S::Record) -> bool + Sync),
        collector: &mut TopK<S::Record>,
    ) -> Result<()> {
        // Position 0 is the live buffer, so sealed segments start at 1. Setting it here
        // rather than in each driver is what makes the sequential and parallel paths
        // produce the same answer instead of two defensible ones.
        collector.set_unit(u32::try_from(ordinal + 1).unwrap_or(u32::MAX));

        let manifest = &segment.manifest;
        // Evaluate the matchers once per distinct stream, not once per row. A
        // segment with a million rows across fifty streams does fifty
        // evaluations; before interning it did a million.
        let allowed: Vec<bool> = manifest
            .streams
            .iter()
            .map(|labels| matches_all(matchers, labels))
            .collect();
        let no_stream_matches = !manifest.streams.is_empty() && !allowed.iter().any(|ok| *ok);

        let prunable = no_stream_matches
            || !manifest.overlaps(request.start_nanos, request.end_nanos)
            || !manifest.might_match(matchers)
            // An exact-key lookup (a trace id) can rule out a segment outright.
            || request
                .exact_key
                .is_some_and(|key| !segment.may_contain_key(key))
            || collector.can_skip_range(manifest.min_time_nanos, manifest.max_time_nanos);

        if prunable {
            self.stats.segments_pruned.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.stats.segments_scanned.fetch_add(1, Ordering::Relaxed);

        // Push the time-and-stream predicate into the Parquet reader so the wide
        // columns are never decompressed for rows we are going to discard.
        let selection = {
            let allowed = allowed.clone();
            let (start, end) = (request.start_nanos, request.end_nanos);
            crate::segment::Selection {
                columns: S::filter_columns().to_vec(),
                mask: std::sync::Arc::new(move |batch: &arrow::record_batch::RecordBatch| {
                    S::selection_mask(batch, start, end, &allowed)
                }),
            }
        };

        segment.scan_batches_where(Some(selection), |batch| {
            // Rows here already passed the pushed-down predicate; `select_rows`
            // re-checks because a batch may still carry rows the reader kept for
            // its own alignment reasons, and correctness must not depend on that.
            let mut rows = S::select_rows(batch, request.start_nanos, request.end_nanos, &allowed)?;
            if let Some(refine) = request.columns {
                refine(batch, &mut rows)?;
            }
            if rows.is_empty() {
                return Ok(Flow::Continue);
            }

            for record in S::materialize(batch, &rows, &manifest.streams)? {
                // The record predicate stays the authority; the columnar filter
                // above is only allowed to over-select.
                if extra(&record) {
                    collector.push(S::timestamp(&record), record);
                }
            }
            Ok(Flow::Continue)
        })
    }

    fn selects(
        record: &S::Record,
        request: &Scan,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&S::Record) -> bool + Sync),
    ) -> bool {
        let ts = S::timestamp(record);
        ts >= request.start_nanos
            && ts <= request.end_nanos
            && matches_all(matchers, S::index_labels(record))
            && extra(record)
    }

    /// Distinct stream label names across the time range.
    ///
    /// Answered from segment metadata alone. The stream dictionary already lists every
    /// distinct label set in the segment, so this never opens a Parquet file — which
    /// is the whole reason label discovery in a UI feels instant instead of scanning.
    pub fn label_names(&self, start_nanos: u64, end_nanos: u64) -> Vec<String> {
        let mut names = std::collections::BTreeSet::new();
        for segment in self.segments() {
            if !segment.manifest.overlaps(start_nanos, end_nanos) {
                continue;
            }
            for stream in &segment.manifest.streams {
                names.extend(stream.names().map(str::to_owned));
            }
            // Segments written before stream interning have no dictionary; their label
            // index still lists the names.
            if segment.manifest.streams.is_empty() {
                names.extend(segment.manifest.labels.keys().cloned());
            }
        }
        for record in &lock(&self.writer).buffer.records {
            let ts = S::timestamp(record);
            if ts >= start_nanos && ts <= end_nanos {
                names.extend(S::index_labels(record).names().map(str::to_owned));
            }
        }
        names.into_iter().collect()
    }

    /// Distinct values for one stream label across the time range.
    ///
    /// Also metadata-only, and *exact* — the dictionary holds every distinct stream,
    /// so there is no cardinality cutoff to fall off and no under-reporting that would
    /// make a UI dropdown quietly wrong.
    pub fn label_values(
        &self,
        name: &str,
        start_nanos: u64,
        end_nanos: u64,
    ) -> Result<Vec<String>> {
        let mut values = std::collections::BTreeSet::new();

        for segment in self.segments() {
            if !segment.manifest.overlaps(start_nanos, end_nanos) {
                continue;
            }

            if segment.manifest.streams.is_empty() {
                // Pre-dictionary segment: fall back to the label index, and read the
                // data only when that index gave up on this label.
                match segment.manifest.labels.get(name) {
                    Some(crate::segment::LabelValues::Values(set)) => {
                        values.extend(set.iter().cloned());
                    }
                    Some(crate::segment::LabelValues::Unbounded { .. }) => {
                        segment.scan_batches(|batch| {
                            let rows: crate::schema::Rows =
                                (0..u32::try_from(batch.num_rows()).unwrap_or(u32::MAX)).collect();
                            for record in S::materialize(batch, &rows, &segment.manifest.streams)? {
                                if let Some(value) = S::index_labels(&record).get(name) {
                                    values.insert(value.to_owned());
                                }
                            }
                            Ok(Flow::Continue)
                        })?;
                    }
                    None => {}
                }
                continue;
            }

            for stream in &segment.manifest.streams {
                if let Some(value) = stream.get(name) {
                    values.insert(value.to_owned());
                }
            }
        }

        for record in &lock(&self.writer).buffer.records {
            let ts = S::timestamp(record);
            if ts >= start_nanos
                && ts <= end_nanos
                && let Some(value) = S::index_labels(record).get(name)
            {
                values.insert(value.to_owned());
            }
        }

        Ok(values.into_iter().collect())
    }

    /// Distinct stream label sets in the range, for `/loki/api/v1/series`.
    ///
    /// Metadata-only when there are no matchers to apply beyond the label set itself —
    /// which is every call the UI makes.
    pub fn streams(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>> {
        let mut seen: std::collections::BTreeSet<Labels> = std::collections::BTreeSet::new();

        for segment in self.segments() {
            if !segment.manifest.overlaps(start_nanos, end_nanos) {
                continue;
            }
            if segment.manifest.streams.is_empty() {
                // Pre-dictionary segment: the only way to know is to read it.
                for record in segment.read::<S>()? {
                    let ts = S::timestamp(&record);
                    if ts >= start_nanos
                        && ts <= end_nanos
                        && matches_all(matchers, S::index_labels(&record))
                    {
                        seen.insert(S::index_labels(&record).clone());
                    }
                }
                continue;
            }
            seen.extend(
                segment
                    .manifest
                    .streams
                    .iter()
                    .filter(|labels| matches_all(matchers, labels))
                    .cloned(),
            );
        }

        for record in &lock(&self.writer).buffer.records {
            let ts = S::timestamp(record);
            if ts >= start_nanos
                && ts <= end_nanos
                && matches_all(matchers, S::index_labels(record))
            {
                seen.insert(S::index_labels(record).clone());
            }
        }

        Ok(seen.into_iter().collect())
    }

    pub fn status(&self) -> RecordStoreStatus {
        let segments = self.segments();
        let writer = lock(&self.writer);
        let buffer = &writer.buffer;

        let mut oldest = segments.iter().map(|s| s.manifest.min_time_nanos).min();
        let mut newest = segments.iter().map(|s| s.manifest.max_time_nanos).max();
        for record in &buffer.records {
            let ts = S::timestamp(record);
            oldest = Some(oldest.map_or(ts, |o| o.min(ts)));
            newest = Some(newest.map_or(ts, |n| n.max(ts)));
        }

        RecordStoreStatus {
            buffered_records: buffer.records.len() as u64,
            buffered_bytes: buffer.bytes as u64,
            segments: segments.len() as u64,
            segment_rows: segments.iter().map(|s| s.manifest.rows).sum(),
            segment_bytes: segments.iter().map(|s| s.manifest.bytes).sum(),
            appended_records: self.stats.appended.load(Ordering::Relaxed),
            sealed_segments: self.stats.sealed_segments.load(Ordering::Relaxed),
            recovered_records: self.stats.recovered.load(Ordering::Relaxed),
            oldest_record_nanos: oldest,
            newest_record_nanos: newest,
            segments_scanned: self.stats.segments_scanned.load(Ordering::Relaxed),
            segments_pruned: self.stats.segments_pruned.load(Ordering::Relaxed),
        }
    }
}

/// A poisoned lock means a previous holder panicked. The data structures here are
/// plain collections with no invariant a panic could have broken mid-update, so
/// recovering beats poisoning every subsequent request.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
