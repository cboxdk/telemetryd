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

use std::collections::HashMap;
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
    /// Lock order: whoever holds both takes `catalogue` first, then `writer`. Publishing a
    /// segment does, and so does every query that reads the buffer beside the segments —
    /// which is what lets a seal hand records from one to the other without a query
    /// seeing them twice or not at all.
    catalogue: RwLock<Vec<Arc<Segment>>>,
    /// Held for a whole seal, so two cannot overlap. They could — the ticker and an
    /// append crossing the size threshold — and the later one, finishing first,
    /// truncated the log through its own epoch: the earlier seal's records, rotated out
    /// and not yet published, went with it, lost to a crash in that window. Replay then
    /// skipped them as well, since it resumes after the highest published epoch.
    seal_lock: Mutex<()>,
    /// When the last seal failed, while failures continue; cleared by a success.
    seal_failed_at: Mutex<Option<Instant>>,
    seal_sequence: AtomicU64,
    pub(crate) stats: Stats,
    /// The torn tail replay found and cut off at startup, if it found one.
    wal_truncation: Option<crate::wal::Truncation>,
}

/// The write side: log and buffer, kept consistent with each other.
struct Writer<S: RecordSchema> {
    wal: Wal,
    buffer: Buffer<S>,
    /// Records taken from the buffer by a seal that has not published its segment yet.
    ///
    /// They used to be in neither place for the length of the Parquet write, so a
    /// query in that window missed them — or, taking the buffer before the drain and the
    /// segments after the publish, counted them twice. Queries read this slot beside the
    /// buffer; the seal empties it in the same step that publishes the segment.
    sealing: Option<Arc<Chunk<S>>>,
}

impl<S: RecordSchema> std::fmt::Debug for RecordStore<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordStore")
            .field("signal", &S::SIGNAL)
            .field("segments", &self.catalogue.read().map_or(0, |c| c.len()))
            .finish_non_exhaustive()
    }
}

/// Records per frozen chunk.
///
/// This is the unit of work a reader can never be forced to copy: it snapshots whole
/// chunks by cloning an `Arc`, and the only thing it ever has to wait for is the
/// active chunk being frozen, which is a pointer move. Smaller chunks mean shorter
/// lock holds and more `Arc`s to clone; a few thousand records puts both well inside
/// the noise.
const CHUNK_RECORDS: usize = 4096;

/// Scan positions reserved for live-buffer chunks, above which sealed segments start.
///
/// Ties break on scan position, so buffer and segment records need positions from one
/// shared sequence. The buffer always sorts before sealed data for a descending query
/// — it holds the newest records — and this reserves it enough room that a segment can
/// never be mistaken for a chunk.
const BUFFER_UNITS: usize = 1 << 20;

/// The unsealed records, as a list of immutable chunks plus the one being filled.
///
/// It used to be a single `Vec`, and queries walked it while holding the same lock
/// that appends need. That made every query block all ingest for as long as it took to
/// scan the whole buffer — measured at a 45% throughput loss from a *single* reader,
/// with query latency of 777 ms against a benchmark of 1.4 ms.
///
/// Freezing filled chunks behind `Arc`s decouples the two: a reader takes the lock
/// only long enough to freeze the active chunk and clone a handful of pointers, then
/// releases it and scans the frozen data while writes continue underneath.
struct Buffer<S: RecordSchema> {
    /// Immutable once pushed, which is what makes sharing them safe.
    chunks: Vec<Arc<Chunk<S>>>,
    /// The chunk being appended to. Never read by a query without being frozen first.
    active: Vec<S::Record>,
    active_min: u64,
    active_max: u64,
    records: usize,
    bytes: usize,
    opened_at: Instant,
    /// One copy of each label set buffered, by fingerprint.
    ///
    /// A decoder builds a fresh label map for every record — OTLP carries the resource
    /// and point attributes on each — so a series scraped every fifteen seconds held a
    /// copy of its labels per sample, and the buffer was charged for each. On telemetry1
    /// that came to 1,258 bytes a sample, a 256 MiB buffer full in nineteen minutes, and
    /// three segments an hour where one was configured. Records now share the first
    /// copy of their set, and the buffer is charged for it once.
    series: HashMap<u64, Labels>,
}

/// A frozen run of buffered records, with the time bounds a query prunes on.
///
/// The bounds are the reason this is a struct rather than a bare `Vec`. Without them a
/// `limit=100` query had to examine every buffered record, because nothing said which
/// ones could not possibly be in the newest hundred — the same problem sealed segments
/// solve with their manifest, and the same solution.
pub(crate) struct Chunk<S: RecordSchema> {
    pub(crate) records: Vec<S::Record>,
    pub(crate) min_nanos: u64,
    pub(crate) max_nanos: u64,
}

impl<S: RecordSchema> Chunk<S> {
    pub(crate) fn overlaps(&self, start_nanos: u64, end_nanos: u64) -> bool {
        self.min_nanos <= end_nanos && self.max_nanos >= start_nanos
    }
}

impl<S: RecordSchema> Buffer<S> {
    fn new() -> Self {
        Self {
            chunks: Vec::new(),
            active: Vec::with_capacity(CHUNK_RECORDS),
            active_min: u64::MAX,
            active_max: u64::MIN,
            records: 0,
            bytes: 0,
            opened_at: Instant::now(),
            series: HashMap::new(),
        }
    }

    fn push(&mut self, record: S::Record) {
        let fingerprint = S::index_labels(&record).fingerprint();
        self.push_fingerprinted(record, fingerprint);
    }

    /// [`Self::push`], with the label set's fingerprint worked out beforehand — outside
    /// the lock this is called under.
    fn push_fingerprinted(&mut self, mut record: S::Record, fingerprint: u64) {
        let ts = S::timestamp(&record);
        self.active_min = self.active_min.min(ts);
        self.active_max = self.active_max.max(ts);
        let mut bytes = S::size_estimate(&record);
        match self.series.entry(fingerprint) {
            std::collections::hash_map::Entry::Occupied(held) => {
                let labels = S::index_labels_mut(&mut record);
                // Equal, not merely the same fingerprint: a collision keeps its own set.
                if held.get() == labels {
                    bytes = bytes.saturating_sub(telemetryd_core::sizing::labels_bytes(labels));
                    *labels = held.get().shared_with();
                }
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(S::index_labels(&record).shared_with());
            }
        }
        self.bytes += bytes;
        self.active.push(record);
        self.records += 1;
        if self.active.len() >= CHUNK_RECORDS {
            self.freeze();
        }
    }

    /// Move the active chunk into the frozen list. O(1) — the `Vec` is moved, not
    /// copied, and none of the records are cloned.
    fn freeze(&mut self) {
        if !self.active.is_empty() {
            let records = std::mem::replace(&mut self.active, Vec::with_capacity(CHUNK_RECORDS));
            self.chunks.push(Arc::new(Chunk {
                records,
                min_nanos: self.active_min,
                max_nanos: self.active_max,
            }));
            self.active_min = u64::MAX;
            self.active_max = u64::MIN;
        }
    }

    /// A consistent view of everything buffered, cheap enough to take under the lock.
    ///
    /// Cost is one pointer move plus one `Arc` clone per chunk, independent of how many
    /// records are buffered. That independence is the entire point.
    fn snapshot(&mut self) -> Vec<Arc<Chunk<S>>> {
        self.freeze();
        self.chunks.clone()
    }

    fn is_empty(&self) -> bool {
        self.records == 0
    }

    fn len(&self) -> usize {
        self.records
    }

    /// Take everything for sealing, as one contiguous chunk.
    ///
    /// Chunks whose only holder is the buffer are moved out; a chunk a query is still
    /// reading is copied instead, so sealing never waits on a reader and a reader never
    /// sees records vanish mid-scan. The bounds come from the chunks', not a pass over
    /// the records, since this runs under the lock appends need.
    fn drain(&mut self) -> Chunk<S> {
        self.freeze();
        let chunks = std::mem::take(&mut self.chunks);
        let mut records = Vec::with_capacity(self.records);
        let (mut min_nanos, mut max_nanos) = (u64::MAX, u64::MIN);
        for chunk in chunks {
            min_nanos = min_nanos.min(chunk.min_nanos);
            max_nanos = max_nanos.max(chunk.max_nanos);
            match Arc::try_unwrap(chunk) {
                Ok(chunk) => records.extend(chunk.records),
                Err(shared) => records.extend(shared.records.iter().cloned()),
            }
        }
        self.records = 0;
        self.bytes = 0;
        self.series.clear();
        Chunk {
            records,
            min_nanos,
            max_nanos,
        }
    }

    /// Put a drained chunk back at the front, preserving order, after a failed seal.
    fn restore(&mut self, chunk: Arc<Chunk<S>>) {
        if chunk.records.is_empty() {
            return;
        }
        self.bytes += chunk.records.iter().map(S::size_estimate).sum::<usize>();
        self.records += chunk.records.len();
        self.chunks.insert(0, chunk);
    }
}

#[derive(Debug, Default)]
pub(crate) struct Stats {
    appended: AtomicU64,
    sealed_segments: AtomicU64,
    /// Reads that failed because a segment file is damaged.
    pub(crate) segments_unreadable: AtomicU64,
    sealed_records: AtomicU64,
    recovered: AtomicU64,
    /// Segments actually opened and decoded. The counterpart to `segments_pruned`:
    /// together they say how much of the store a query had to touch, which is the
    /// number to watch when queries get slow.
    pub(crate) segments_scanned: AtomicU64,
    /// Segments a query skipped without any I/O — by time range, label index, Bloom
    /// filter, or the limit cutoff.
    pub(crate) segments_pruned: AtomicU64,
}

/// A bounded query request.
#[derive(Clone, Copy)]
pub struct Scan<'a> {
    pub start_nanos: u64,
    pub end_nanos: u64,
    /// `0` means unbounded.
    ///
    /// This is *top-N*, not a ceiling: the collector keeps the best `limit` records by
    /// time, evicting as it goes, which costs a heap sift on every record offered. Use it
    /// when the caller genuinely wants the newest or oldest N. A caller that wants "refuse
    /// if there are more than N" wants [`Scan::abort_over`] instead — passing that
    /// through `limit` turns an append into a heap build over the whole result.
    pub limit: usize,
    /// Refuse the scan once this many records have been collected. `0` means no ceiling.
    ///
    /// Separate from `limit` because it is a different question with a different answer.
    /// `limit` orders and evicts; this one only counts, so the collector keeps its fast
    /// append path and the scan stops early instead of returning a truncated answer.
    ///
    /// Added after the PromQL sample ceiling was first built on `limit`, which quietly
    /// put every metric scan through a `BinaryHeap` — measured at six seconds to load a
    /// six-hour window that had been well under one.
    pub abort_over: usize,
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
    /// A substring every matching record must contain.
    ///
    /// Only set from a filter that *requires* the text — a positive `|=`, never a
    /// negated one — because it is used to skip segments unread via the trigram index.
    /// A wrong value here silently drops results, so the rule is: if in doubt, `None`.
    pub required_text: Option<&'a str>,
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
            abort_over: 0,
            order: Order::Ascending,
            exact_key: None,
            columns: None,
            required_text: None,
        }
    }

    /// Declare a substring every match must contain, for trigram pruning.
    #[must_use]
    pub fn required_text(mut self, text: &'a str) -> Self {
        self.required_text = Some(text);
        self
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
    /// Query-time reads skipped because the segment file is damaged. Non-zero means
    /// data has been lost and the operator needs to know.
    pub segments_unreadable: u64,
    pub recovered_records: u64,
    pub oldest_record_nanos: Option<u64>,
    pub newest_record_nanos: Option<u64>,
    pub segments_scanned: u64,
    pub segments_pruned: u64,
    /// The write-ahead log's own numbers. `unsynced_records` is the one to watch: it
    /// is how much would be lost to a power cut right now.
    pub wal: crate::wal::WalStats,
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
            match S::decode_wal(payload) {
                Ok(record) => {
                    // `push` counts its size. Counting it here as well charged every
                    // replayed record twice, and a restart sealed early for it.
                    buffer.push(record);
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

        let wal = Wal::open_after(
            wal_dir,
            settings.wal_sync,
            settings.wal_sync_interval,
            settings.max_segment_bytes,
            sealed_through,
        )?;

        if !buffer.is_empty() {
            tracing::info!(
                signal = %S::SIGNAL,
                records = buffer.len(),
                "recovered buffered records from the write-ahead log"
            );
        }
        let recovered = buffer.len() as u64;

        // Never lower than the highest ever used, even once retention has deleted every
        // segment that carried it: a relay orders what to ship by this number, and one
        // that went back to 1 would read every new segment as already delivered.
        let seal_sequence = segments
            .iter()
            .filter_map(|s| crate::segment::seal_sequence_of(&s.manifest.id))
            .max()
            .unwrap_or(0)
            .max(crate::segment::recorded_sequence(&segments_dir));

        let stats = Stats::default();
        stats.recovered.store(recovered, Ordering::Relaxed);

        Ok(Self {
            segments_dir,
            tmp_dir,
            settings,
            writer: Mutex::new(Writer {
                wal,
                buffer,
                sealing: None,
            }),
            seal_lock: Mutex::new(()),
            seal_failed_at: Mutex::new(None),
            catalogue: RwLock::new(segments.into_iter().map(Arc::new).collect()),
            seal_sequence: AtomicU64::new(seal_sequence),
            stats,
            wal_truncation: replayed.truncated,
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
        let failing = lock(&self.seal_failed_at).is_some();
        // Encoded and fingerprinted before the lock: every writer and every seal waits
        // on it, and neither piece of work needs it.
        let payloads = records
            .iter()
            .map(|record| {
                postcard::to_stdvec(record)
                    .map_err(|e| Error::Config(format!("encoding a {} record: {e}", S::SIGNAL)))
            })
            .collect::<Result<Vec<_>>>()?;
        let fingerprints: Vec<u64> = records
            .iter()
            .map(|record| S::index_labels(record).fingerprint())
            .collect();

        let should_seal = {
            let mut writer = lock(&self.writer);
            // Sealing is what turns the buffer into disk, and while it fails the buffer
            // only grows. Past twice a segment's worth, refuse — before writing, so
            // nothing is half-accepted — with an error senders retry. Holding more
            // would end with the process out of memory and the records with it.
            if failing && writer.buffer.bytes as u64 >= 2 * self.settings.max_segment_bytes {
                return Err(Error::Unavailable(format!(
                    "{} records are waiting to be sealed into segments and sealing is \
                     failing, so no more are accepted until it recovers; the server log \
                     says why",
                    S::SIGNAL
                )));
            }
            for ((record, payload), fingerprint) in records.iter().zip(&payloads).zip(&fingerprints)
            {
                writer.wal.append(payload)?;
                writer
                    .buffer
                    .push_fingerprinted(record.clone(), *fingerprint);
            }
            writer.buffer.bytes as u64 >= self.settings.max_segment_bytes
        };

        self.stats
            .appended
            .fetch_add(records.len() as u64, Ordering::Relaxed);

        // The records are durable and queryable already, so a seal that fails here is
        // not this request's failure: it is logged, retried by the ticker after a pause,
        // and the client is told what is true — accepted. Returning it used to answer
        // 5xx for records that were stored, and the sender's retry stored them twice.
        if should_seal && !self.backing_off() {
            let _ = self.seal_now();
        }
        Ok(())
    }

    /// Whether automatic seals are pausing after a failure.
    ///
    /// A failing seal was retried on every append past the threshold — each attempt
    /// rotating the log and writing a staging directory — which on a full disk is a
    /// failure per request. Forced seals (shutdown, `seal_now`) are not held back.
    fn backing_off(&self) -> bool {
        const PAUSE: std::time::Duration = std::time::Duration::from_secs(30);
        lock(&self.seal_failed_at).is_some_and(|at| at.elapsed() < PAUSE)
    }

    /// Seal if the buffer has been open longer than the segment window.
    ///
    /// Driven by a background ticker; separate from [`Self::seal_now`] so a caller can
    /// force a seal (shutdown, tests) without waiting for the window.
    pub fn maybe_seal(&self) -> Result<Option<Arc<Segment>>> {
        let due = {
            let writer = lock(&self.writer);
            !writer.buffer.is_empty()
                && (writer.buffer.opened_at.elapsed() >= self.settings.segment_duration
                    || writer.buffer.bytes as u64 >= self.settings.max_segment_bytes)
        };
        if due && !self.backing_off() {
            self.seal_now()
        } else {
            Ok(None)
        }
    }

    /// Seal the current buffer into an immutable segment.
    ///
    /// The Parquet write happens **without** holding the buffer lock, so a large seal
    /// does not stall ingest. Correctness across that gap comes from the WAL: the log
    /// is rotated before the buffer is taken, and only truncated after the segment is
    /// published, so a crash anywhere in between recovers rather than loses.
    pub fn seal_now(&self) -> Result<Option<Arc<Segment>>> {
        let _one_at_a_time = lock(&self.seal_lock);
        let (chunk, wal_sequence) = {
            let mut writer = lock(&self.writer);
            if writer.buffer.is_empty() {
                return Ok(None);
            }
            // Rotate and drain under the same lock that appends hold, so the boundary
            // between "in this segment" and "still in the log" is exact.
            let wal_sequence = writer.wal.rotate()?;
            let chunk = Arc::new(writer.buffer.drain());
            writer.buffer.opened_at = Instant::now();
            writer.sealing = Some(Arc::clone(&chunk));
            (chunk, wal_sequence)
        };

        let sequence = self.seal_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        if let Err(error) = crate::segment::record_sequence(&self.segments_dir, sequence) {
            tracing::warn!(signal = %S::SIGNAL, %error, "could not record the seal sequence");
        }
        let segment = seal::<S>(
            &chunk.records,
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
                *lock(&self.seal_failed_at) = Some(Instant::now());
                let mut writer = lock(&self.writer);
                writer.sealing = None;
                writer.buffer.restore(chunk);
                tracing::error!(
                    signal = %S::SIGNAL,
                    error = %error,
                    "sealing failed; records remain buffered and in the write-ahead log"
                );
                return Err(error);
            }
        };

        *lock(&self.seal_failed_at) = None;
        self.stats.sealed_segments.fetch_add(1, Ordering::Relaxed);
        self.stats
            .sealed_records
            .fetch_add(segment.manifest.rows, Ordering::Relaxed);

        // Publish and retire the sealing slot as one step. A query takes the catalogue
        // before the writer, so it sees these records in the slot or in the segment,
        // never both and never neither.
        {
            let mut catalogue = lock_write(&self.catalogue);
            catalogue.push(Arc::clone(&segment));
            lock(&self.writer).sealing = None;
        }
        drop(chunk);

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

    /// The segments and the buffered records, taken together.
    ///
    /// Together, under both locks in their order, because a seal moves records from the
    /// one to the other: taken apart, a query could read the buffer before a drain and
    /// the segments after the publish, and count a seal's records twice — or the other
    /// way round and miss them.
    pub(crate) fn view(&self) -> (Vec<Arc<Chunk<S>>>, Vec<Arc<Segment>>) {
        let catalogue = lock_read(&self.catalogue);
        let chunks = Self::buffered_in(&mut lock(&self.writer));
        (chunks, catalogue.clone())
    }

    /// Everything unsealed: the buffer, and a seal's records until it publishes.
    fn buffered_in(writer: &mut Writer<S>) -> Vec<Arc<Chunk<S>>> {
        let mut chunks = writer.buffer.snapshot();
        if let Some(sealing) = &writer.sealing {
            chunks.push(Arc::clone(sealing));
        }
        chunks
    }

    /// The torn tail replay cut off when this store opened, if there was one. Held for
    /// the process's life so `/status` keeps saying so; it was logged and forgotten.
    #[must_use]
    pub fn wal_truncation(&self) -> Option<crate::wal::Truncation> {
        self.wal_truncation.clone()
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

    /// Rewrite up to `limit` segments of the previous format as the current one.
    ///
    /// Returns how many were rewritten. A segment retention removed meanwhile is passed
    /// over; any other failure ends the pass, to be tried again on the next.
    pub fn upgrade_segments(&self, limit: usize) -> Result<usize> {
        let mut upgraded = 0;
        for segment in self.segments() {
            if upgraded >= limit {
                break;
            }
            match segment.upgrade() {
                Ok(true) => upgraded += 1,
                Ok(false) => {}
                Err(error) if crate::segment::is_gone(&error) || !segment.dir.exists() => {}
                Err(error) => return Err(error),
            }
        }
        Ok(upgraded)
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
    /// Records sitting in the unsealed buffer within `(start, end]`.
    ///
    /// The newest and smallest part of the store, and the one part with no segment to
    /// carry a precomputed summary — so a caller folding a window has to walk it.
    pub(crate) fn buffered_between(&self, start_nanos: u64, end_nanos: u64) -> Vec<S::Record> {
        let chunks = Self::buffered_in(&mut lock(&self.writer));
        let mut out = Vec::new();
        for chunk in &chunks {
            if !chunk.overlaps(start_nanos, end_nanos) {
                continue;
            }
            for record in &chunk.records {
                let at = S::timestamp(record);
                if at > start_nanos && at <= end_nanos {
                    out.push(record.clone());
                }
            }
        }
        out
    }

    pub fn query(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&S::Record) -> bool + Sync),
    ) -> Result<Vec<S::Record>> {
        self.query_bounded(start_nanos, end_nanos, matchers, extra, 0)
    }

    /// `query`, refusing to collect more than `limit` records.
    ///
    /// The bound is a *ceiling to fail at*, not a top-N: a caller passes one more than it
    /// is willing to handle and treats a full result as an overflow. Truncating instead
    /// would answer a PromQL query from part of its data, which is a wrong chart rather
    /// than a refused one.
    pub fn query_bounded(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&S::Record) -> bool + Sync),
        ceiling: usize,
    ) -> Result<Vec<S::Record>> {
        self.scan(
            Scan {
                abort_over: ceiling,
                start_nanos,
                end_nanos,
                limit: 0,
                order: Order::Ascending,
                exact_key: None,
                columns: None,
                required_text: None,
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
        let segments_seen;

        // The live buffer first: it holds the newest data, so filling the collector
        // from it maximises how many sealed segments the cutoff can then skip.
        {
            collector.set_unit(0);
            // Snapshot, then release. Holding the lock across the scan is what made a
            // single reader cost 45% of ingest throughput: appends need the same lock,
            // so every query stalled every writer for a full buffer walk.
            let (mut buffered, from_catalogue) = self.view();
            segments_seen = from_catalogue;

            // Visit chunks from the end the caller asked for, so the collector's cutoff
            // tightens immediately and the rest can be skipped on their bounds alone.
            // A `limit=100` query used to read every buffered record — at a quarter of
            // a million of them that was the whole cost of the query.
            match request.order {
                Order::Descending => buffered.sort_by_key(|c| std::cmp::Reverse(c.max_nanos)),
                Order::Ascending => buffered.sort_by_key(|c| c.min_nanos),
            }

            for (ordinal, chunk) in buffered.iter().enumerate() {
                if !chunk.overlaps(request.start_nanos, request.end_nanos)
                    || collector.can_skip_range(chunk.min_nanos, chunk.max_nanos)
                {
                    continue;
                }
                // Chunks are ordered among themselves, so they are scan positions in
                // their own right; ties inside one still break on row order.
                collector.set_unit(u32::try_from(ordinal).unwrap_or(u32::MAX));
                for record in &chunk.records {
                    if Self::selects(record, &request, matchers, extra) {
                        collector.push(S::timestamp(record), record.clone());
                    }
                }
            }
        }

        // Walk segments from the end the caller cares about, so the cutoff tightens as
        // fast as possible.
        let mut segments = segments_seen;
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
                Self::over_ceiling(&request, collector.len())?;
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
        // What every worker holds between them, against `abort_over`. The sequential walk
        // checks its one collector after each segment; the workers used not to check at
        // all, so with `query_parallelism` above one a query the ceiling should have
        // refused collected everything first — measured at 889,200 rows against a ceiling
        // of 200,001 — and was refused only after the memory was spent.
        let held = std::sync::atomic::AtomicUsize::new(0);
        let over = std::sync::atomic::AtomicBool::new(false);
        // The first read that failed for a reason worth retrying; it ends every worker.
        let failed: std::sync::Mutex<Option<Error>> = std::sync::Mutex::new(None);

        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    let mut local = TopK::new(request.limit, request.order);
                    loop {
                        if over.load(Ordering::Relaxed) {
                            break;
                        }
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

                        let before = local.len();
                        if let Err(error) =
                            self.scan_segment(segment, index, &request, matchers, extra, &mut local)
                        {
                            lock(&failed).get_or_insert(error);
                            over.store(true, Ordering::Relaxed);
                            break;
                        }
                        let grown = local.len().saturating_sub(before);
                        let now = held.fetch_add(grown, Ordering::Relaxed) + grown;
                        if request.abort_over != 0 && now > request.abort_over {
                            over.store(true, Ordering::Relaxed);
                            break;
                        }
                        shared.publish(&local);
                    }
                    lock(&collected).push(local);
                });
            }
        });

        if let Some(error) = lock(&failed).take() {
            return Err(error);
        }
        if over.load(Ordering::Relaxed) {
            Self::over_ceiling(&request, held.load(Ordering::Relaxed))?;
        }
        for local in lock(&collected).drain(..) {
            collector.merge(local);
        }

        Ok(collector.into_sorted())
    }

    /// Stop a scan that has collected more than the caller is willing to hold.
    ///
    /// Checked between segments rather than per record: the granularity costs at most one
    /// segment's worth of overshoot, and a branch on every record of a hundred-million-row
    /// store costs more than the bound is worth.
    fn over_ceiling(request: &Scan, collected: usize) -> Result<()> {
        if request.abort_over != 0 && collected > request.abort_over {
            return Err(telemetryd_core::Error::BadRequest(format!(
                "this query matched more than {} records, which is more than one query \
                 is allowed to hold at once",
                request.abort_over
            )));
        }
        Ok(())
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
        // Buffer chunks occupy the low positions, so sealed segments continue above
        // them. Setting it here rather than in each driver is what makes the sequential
        // and parallel paths produce the same answer instead of two defensible ones.
        collector.set_unit(u32::try_from(ordinal.saturating_add(BUFFER_UNITS)).unwrap_or(u32::MAX));

        let manifest = &segment.manifest;
        // The cheap refusals first: a segment wholly outside the range, or one whose
        // label index rules the matchers out, costs a comparison or a set lookup to
        // skip. Evaluating every stream's labels first cost a week-long chart three
        // seconds, most of it on segments the time range alone would have dropped.
        if manifest.min_time_nanos > request.end_nanos
            || manifest.max_time_nanos < request.start_nanos
            || !manifest.might_match(matchers)
        {
            self.stats.segments_pruned.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        // Evaluate the matchers once per distinct stream, not once per row. A
        // segment with a million rows across fifty streams does fifty
        // evaluations; before interning it did a million.
        let allowed: Vec<bool> = manifest
            .streams
            .iter()
            .map(|labels| matches_all(matchers, labels))
            .collect();
        let no_stream_matches = !manifest.streams.is_empty() && !allowed.iter().any(|ok| *ok);

        // Prune on the time range of the streams this query selected, not the segment's
        // overall span. They differ whenever one producer's clock sits away from the
        // others', and then the overall span is wide enough that nothing is ever
        // skippable.
        let (min_nanos, max_nanos) = manifest.bounds_for(&allowed);

        let prunable = no_stream_matches
            || min_nanos > request.end_nanos
            || max_nanos < request.start_nanos
            // An exact-key lookup (a trace id) can rule out a segment outright.
            || request
                .exact_key
                .is_some_and(|key| !segment.may_contain_key(key))
            // As can a line filter whose trigrams are not all present.
            || request
                .required_text
                .is_some_and(|text| !segment.may_contain_text(text))
            || collector.can_skip_range(min_nanos, max_nanos);

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

        // A read failure here is a damaged file, not a bad query. Aborting denied the
        // caller every healthy segment in the same time range because of one bad
        // sector, which is the opposite of useful in the tool you reach for when
        // things are already broken. Skip it, say so once, and count it.
        if segment.is_unreadable() {
            self.stats
                .segments_unreadable
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }

        let outcome = segment.scan_batches_where(Some(selection), |batch| {
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
        });

        self.settle_scan(segment, outcome)
    }

    /// What a failed segment read means for the query that made it.
    ///
    /// Shared by every path that reads segments, so a damaged file is treated the same
    /// whichever query found it.
    pub(crate) fn settle_scan(&self, segment: &Segment, outcome: Result<()>) -> Result<()> {
        let manifest = &segment.manifest;
        match outcome {
            Ok(()) => Ok(()),
            // Deleted by retention while this query was on its way to it: the data was
            // meant to go, and the answer is what is left.
            Err(Error::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                Ok(())
            }
            // Could not open or read it just now — out of descriptors, a device
            // hiccup. That says nothing about the file, so it is not written off: the
            // query fails with an error the client retries. It used to be marked
            // unreadable for the life of the process, and every later answer quietly
            // lacked its rows.
            Err(error @ Error::Io { .. }) => Err(error),
            // The file itself is damaged. Skipped — one bad segment must not deny every
            // healthy one in the range — said once, and counted.
            Err(error) => {
                if segment.mark_unreadable() {
                    tracing::error!(
                        signal = %S::SIGNAL,
                        segment = %manifest.id,
                        rows = manifest.rows,
                        %error,
                        "segment is unreadable and will be skipped by every query from now \
                         on; the data in it is lost. Delete the segment directory to stop \
                         this being reported."
                    );
                }
                self.stats
                    .segments_unreadable
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
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

    /// Segments sealed since the process started.
    ///
    /// One relaxed atomic load. Retention polls it to notice that there is new data on
    /// disk without walking the filesystem to find out.
    #[must_use]
    pub fn sealed_count(&self) -> u64 {
        self.stats.sealed_segments.load(Ordering::Relaxed)
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
        // Taken in its own statement. In the loop header the lock guard is a temporary
        // of the `for` expression and lives until the loop ends, so every append waited
        // for this walk of the buffer.
        let buffered = Self::buffered_in(&mut lock(&self.writer));
        // Buffered records of one stream share its labels, so each set is looked at once.
        let mut visited = std::collections::HashSet::new();
        for chunk in &buffered {
            for record in &chunk.records {
                let ts = S::timestamp(record);
                if ts >= start_nanos
                    && ts <= end_nanos
                    && visited.insert(S::index_labels(record).storage_id())
                {
                    names.extend(S::index_labels(record).names().map(str::to_owned));
                }
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

        // Taken in its own statement. In the loop header the lock guard is a temporary
        // of the `for` expression and lives until the loop ends, so every append waited
        // for this walk of the buffer.
        let buffered = Self::buffered_in(&mut lock(&self.writer));
        // Buffered records of one stream share its labels, so each set is looked at once.
        let mut visited = std::collections::HashSet::new();
        for chunk in &buffered {
            for record in &chunk.records {
                let ts = S::timestamp(record);
                if ts >= start_nanos
                    && ts <= end_nanos
                    && visited.insert(S::index_labels(record).storage_id())
                    && let Some(value) = S::index_labels(record).get(name)
                {
                    values.insert(value.to_owned());
                }
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
                // Pre-dictionary segment: the only way to know is to read it — unless
                // retention deleted it since it was listed.
                let records = match segment.read::<S>() {
                    Ok(records) => records,
                    Err(error) if crate::segment::is_gone(&error) => continue,
                    Err(error) => return Err(error),
                };
                for record in records {
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

        // Taken in its own statement. In the loop header the lock guard is a temporary
        // of the `for` expression and lives until the loop ends, so every append waited
        // for this walk of the buffer.
        let buffered = Self::buffered_in(&mut lock(&self.writer));
        // Buffered records of one stream share its labels, so each set is looked at once.
        let mut visited = std::collections::HashSet::new();
        for chunk in &buffered {
            for record in &chunk.records {
                let ts = S::timestamp(record);
                if ts >= start_nanos
                    && ts <= end_nanos
                    && visited.insert(S::index_labels(record).storage_id())
                    && matches_all(matchers, S::index_labels(record))
                {
                    seen.insert(S::index_labels(record).clone());
                }
            }
        }

        Ok(seen.into_iter().collect())
    }

    pub fn status(&self) -> RecordStoreStatus {
        let segments = self.segments();
        let (buffered, buffered_records, buffered_bytes) = {
            let mut writer = lock(&self.writer);
            let counts = (writer.buffer.len() as u64, writer.buffer.bytes as u64);
            (Self::buffered_in(&mut writer), counts.0, counts.1)
        };

        let mut oldest = segments.iter().map(|s| s.manifest.min_time_nanos).min();
        let mut newest = segments.iter().map(|s| s.manifest.max_time_nanos).max();
        for chunk in &buffered {
            for record in &chunk.records {
                let ts = S::timestamp(record);
                oldest = Some(oldest.map_or(ts, |o| o.min(ts)));
                newest = Some(newest.map_or(ts, |n| n.max(ts)));
            }
        }

        RecordStoreStatus {
            buffered_records,
            buffered_bytes,
            segments: segments.len() as u64,
            segment_rows: segments.iter().map(|s| s.manifest.rows).sum(),
            segment_bytes: segments.iter().map(|s| s.manifest.bytes).sum(),
            appended_records: self.stats.appended.load(Ordering::Relaxed),
            sealed_segments: self.stats.sealed_segments.load(Ordering::Relaxed),
            segments_unreadable: self.stats.segments_unreadable.load(Ordering::Relaxed),
            recovered_records: self.stats.recovered.load(Ordering::Relaxed),
            oldest_record_nanos: oldest,
            newest_record_nanos: newest,
            wal: lock(&self.writer).wal.stats(),
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::logs::LogSchema;
    use telemetryd_core::{Labels, LogRecord, Severity};

    fn record(i: u64) -> LogRecord {
        let mut stream = Labels::new();
        stream.insert("app", "checkout");
        LogRecord {
            timestamp_nanos: 1_750_000_000_000_000_000 + i,
            stream,
            severity: Severity::Info,
            severity_text: "INFO".to_owned(),
            body: format!("line {i}"),
            attributes: Labels::new(),
            trace_id: None,
            span_id: None,
        }
    }

    fn open(dir: &std::path::Path) -> RecordStore<LogSchema> {
        RecordStore::<LogSchema>::open(
            &dir.join("wal"),
            dir.join("segments"),
            dir.join("tmp"),
            StoreSettings {
                segment_duration: std::time::Duration::from_secs(3600),
                max_segment_bytes: 1 << 30,
                wal_sync: telemetryd_core::config::WalSync::Always,
                wal_sync_interval: std::time::Duration::ZERO,
                compression: telemetryd_core::config::Compression::Zstd,
                query_parallelism: 1,
            },
        )
        .unwrap()
    }

    fn count(store: &RecordStore<LogSchema>) -> usize {
        store
            .scan(Scan::range(0, u64::MAX), &[], &|_| true)
            .unwrap()
            .len()
    }

    /// Records of one stream share its labels in the buffer and are charged for them
    /// once, however many separate copies the decoder handed in — and a restart charges
    /// what was buffered the same again, not twice.
    #[test]
    fn a_stream_is_charged_once_and_shared() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open(tmp.path());
        let one = LogSchema::size_estimate(&record(0));
        let labels = telemetryd_core::sizing::labels_bytes(&record(0).stream);
        store
            .append(&(0..100).map(record).collect::<Vec<_>>())
            .unwrap();
        let bytes = store.status().buffered_bytes;
        // Each record's own size, less its labels for every one after the first. The
        // bodies differ in length by a byte or two, so allow for them.
        let expected = (100 * one - 99 * labels) as u64;
        assert!(
            bytes.abs_diff(expected) < 200,
            "{bytes} buffered, expected about {expected}"
        );

        let chunks = lock(&store.writer).buffer.snapshot();
        let records: Vec<&LogRecord> = chunks.iter().flat_map(|c| c.records.iter()).collect();
        assert!(
            records
                .iter()
                .all(|r| r.stream.shares_storage_with(&records[0].stream))
        );

        drop(store);
        let reopened = open(tmp.path());
        assert_eq!(
            reopened.status().buffered_bytes,
            bytes,
            "replay charges the same"
        );
    }

    /// A query in the middle of a seal — records out of the buffer, segment not yet
    /// published — sees each record once. It used to see none of them for the length
    /// of the Parquet write; the window is held open here by doing the drain half of a
    /// seal by hand, since no timing could.
    #[test]
    fn a_query_during_a_seal_sees_every_record_once() {
        let tmp = tempfile::tempdir().unwrap();
        let store = open(tmp.path());
        store
            .append(&(0..10).map(record).collect::<Vec<_>>())
            .unwrap();

        {
            let mut writer = lock(&store.writer);
            let chunk = Arc::new(writer.buffer.drain());
            writer.sealing = Some(chunk);
        }
        store
            .append(&(10..15).map(record).collect::<Vec<_>>())
            .unwrap();
        assert_eq!(
            count(&store),
            15,
            "mid-seal, records in the slot are still found"
        );

        // Finishing the seal publishes them and empties the slot in one step: still
        // fifteen, not twenty-five.
        let chunk = lock(&store.writer).sealing.take().unwrap();
        lock(&store.writer).buffer.restore(chunk);
        store.seal_now().unwrap();
        assert_eq!(count(&store), 15);
        assert!(lock(&store.writer).sealing.is_none());
    }
}
