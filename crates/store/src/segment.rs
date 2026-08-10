//! Immutable on-disk segments: Parquet data plus a manifest that describes it well
//! enough to skip opening the data at all.
//!
//! A segment is one directory, written to `tmp/` and moved into place with
//! `rename(2)`, so a segment is either completely visible or not visible — there is no
//! partially-published state a reader can observe. Retention deletes whole
//! directories; there are no tombstones and no row-level deletes.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
    RowFilter,
};
use parquet::basic::{Compression as ParquetCompression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use telemetryd_core::config::Compression;
use telemetryd_core::{Error, LabelMatcher, Labels, Result, Signal};

use crate::bloom::Bloom;
use crate::schema::RecordSchema;
use crate::trigram::TrigramIndex;

/// Bumped when a sealed segment written by an older build can no longer be read.
pub const SEGMENT_FORMAT_VERSION: u32 = 2;

/// Above this many distinct values, a label stops being tracked individually.
///
/// The index exists to skip segments, and a label with thousands of values (a request
/// id, a user id) skips nothing while costing real bytes in every manifest. Past the
/// cap we record only that it was unbounded, and queries on it scan.
const MAX_TRACKED_LABEL_VALUES: usize = 256;

const DATA_FILE: &str = "data.parquet";
const MANIFEST_FILE: &str = "manifest.json";

/// What a segment contains, without opening the data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentManifest {
    pub format_version: u32,
    pub signal: Signal,
    pub id: String,
    pub min_time_nanos: u64,
    pub max_time_nanos: u64,
    pub rows: u64,
    /// Size of the Parquet file on disk.
    pub bytes: u64,
    pub created_at_nanos: u64,
    /// The write-ahead log sequence whose records this segment made durable.
    ///
    /// Replay skips WAL segments at or below this, which is what prevents a crash
    /// between publishing a segment and truncating the log from storing the same
    /// records twice.
    #[serde(default)]
    pub wal_sequence: u64,
    /// Per-label value sets, for pruning.
    pub labels: BTreeMap<String, LabelValues>,
    /// The distinct stream label sets in this segment, indexed by the `stream_id`
    /// column.
    ///
    /// Interning them here is what makes label matching cost one evaluation per
    /// *stream* instead of one per *row*. A segment holding a million rows across
    /// fifty streams evaluates the matchers fifty times.
    #[serde(default)]
    pub streams: Vec<Labels>,
    /// `(min, max)` event time per entry of `streams`, in the same order.
    ///
    /// A segment's overall time range spans every stream in it, which makes it useless
    /// for pruning as soon as one producer's clock sits far from the others'. A backfill
    /// job, or a host with a skewed clock, gives every segment a range wide enough that
    /// no limited query can ever skip one — measured at seven seconds for a `limit=100`
    /// query across 5,500 segments, degrading as the store grew.
    ///
    /// Per-stream bounds let the collector's cutoff apply to the streams the query
    /// actually selected. Empty on segments written before this existed, which fall back
    /// to the whole-segment range.
    #[serde(default)]
    pub stream_bounds: Vec<(u64, u64)>,
    /// Rows contributed by each entry of `streams`, in the same order.
    ///
    /// A segment holds every app that was writing when it sealed, so `rows` alone
    /// cannot answer "which app is filling my disk" — the question an operator has
    /// when the budget alarm fires. Empty on segments written before this existed.
    #[serde(default)]
    pub stream_rows: Vec<u64>,
}

impl SegmentManifest {
    /// The time range of just the streams a query selected.
    ///
    /// `allowed` is indexed by stream id. Falls back to the whole-segment range when
    /// bounds are absent or nothing was selected, which is always sound: a wider range
    /// can only cause the segment to be read when it need not have been.
    #[must_use]
    pub fn bounds_for(&self, allowed: &[bool]) -> (u64, u64) {
        if self.stream_bounds.len() != self.streams.len() || allowed.len() != self.streams.len() {
            return (self.min_time_nanos, self.max_time_nanos);
        }
        let mut min = u64::MAX;
        let mut max = u64::MIN;
        for (bounds, permitted) in self.stream_bounds.iter().zip(allowed) {
            if *permitted {
                min = min.min(bounds.0);
                max = max.max(bounds.1);
            }
        }
        if min > max {
            (self.min_time_nanos, self.max_time_nanos)
        } else {
            (min, max)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LabelValues {
    /// Every distinct value this label took in this segment.
    Values(BTreeSet<String>),
    /// Too many distinct values to be worth tracking; this label cannot prune.
    Unbounded { distinct_at_cutoff: u64 },
}

impl LabelValues {
    pub fn as_set(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::Values(set) => Some(set),
            Self::Unbounded { .. } => None,
        }
    }
}

/// Accumulates the label index while a buffer fills.
#[derive(Debug, Default)]
pub struct LabelIndexBuilder {
    labels: BTreeMap<String, LabelValues>,
}

impl LabelIndexBuilder {
    pub fn observe(&mut self, labels: &Labels) {
        for (name, value) in labels.iter() {
            match self.labels.get_mut(name) {
                Some(LabelValues::Values(set)) => {
                    if set.len() >= MAX_TRACKED_LABEL_VALUES && !set.contains(value) {
                        let distinct = set.len() as u64 + 1;
                        self.labels.insert(
                            name.to_owned(),
                            LabelValues::Unbounded {
                                distinct_at_cutoff: distinct,
                            },
                        );
                    } else {
                        set.insert(value.to_owned());
                    }
                }
                Some(LabelValues::Unbounded { .. }) => {}
                None => {
                    let mut set = BTreeSet::new();
                    set.insert(value.to_owned());
                    self.labels
                        .insert(name.to_owned(), LabelValues::Values(set));
                }
            }
        }
    }

    pub fn build(self) -> BTreeMap<String, LabelValues> {
        self.labels
    }
}

impl SegmentManifest {
    /// Whether this segment's time range intersects `[start, end]`, inclusive.
    pub fn overlaps(&self, start_nanos: u64, end_nanos: u64) -> bool {
        self.min_time_nanos <= end_nanos && self.max_time_nanos >= start_nanos
    }

    /// Whether any row here *could* satisfy the matchers.
    ///
    /// Conservative by construction: it may return `true` for a segment that turns
    /// out to hold nothing, but it must never return `false` for one that holds a
    /// match. A wrong `false` silently drops data from a query result, which looks
    /// like an answer rather than an error.
    pub fn might_match(&self, matchers: &[LabelMatcher]) -> bool {
        for matcher in matchers {
            if !matcher.is_selective() {
                // Negative and match-empty matchers are satisfied by streams that lack
                // the label entirely, so they can never rule a segment out.
                continue;
            }
            match self.labels.get(&matcher.name) {
                // The label never appeared in this segment, but the matcher demands a
                // value for it — nothing here can match.
                None => return false,
                Some(LabelValues::Unbounded { .. }) => {}
                Some(LabelValues::Values(values)) => {
                    if !values.iter().any(|v| matcher.matches_value(v)) {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Distinct values recorded for a label, if it was tracked.
    pub fn label_values(&self, name: &str) -> Option<&BTreeSet<String>> {
        self.labels.get(name).and_then(LabelValues::as_set)
    }
}

/// How many rows to decode at a time when scanning.
///
/// Small enough that a highly selective query does not decode a whole row group to
/// find one row; large enough that per-batch overhead stays amortised.
const SCAN_BATCH_ROWS: usize = 8192;

/// Evaluates a selection over a projected batch.
pub type SelectionMask =
    std::sync::Arc<dyn Fn(&RecordBatch) -> Result<arrow::array::BooleanArray> + Send + Sync>;

/// A predicate pushed into the Parquet reader.
///
/// Owned rather than borrowed: arrow-rs boxes the predicate into the reader and may
/// evaluate it from its own threads, so it has to outlive this call.
#[derive(Clone)]
pub struct Selection {
    /// Column names the predicate reads. Only these are decompressed to evaluate it.
    pub columns: Vec<&'static str>,
    /// Given a batch projected to `columns`, which rows are wanted.
    pub mask: SelectionMask,
}

impl std::fmt::Debug for Selection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Selection")
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

/// Whether a scan should keep going.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
    Continue,
    Stop,
}

/// A sealed segment on disk.
#[derive(Debug, Clone)]
pub struct Segment {
    pub manifest: SegmentManifest,
    pub dir: PathBuf,
    /// Exact-match filter over the schema's key column, when it declares one.
    pub(crate) bloom: Option<Bloom>,
    /// Trigram index over the searchable text, when the signal has any.
    pub(crate) text: Option<TrigramIndex>,
    /// Set once a read of this segment has failed.
    ///
    /// A damaged Parquet file used to abort the whole query, so one bad sector denied
    /// access to every healthy segment in the same time range. Marking the segment
    /// instead lets the rest of the answer through, and makes the failure something
    /// reported rather than something that has to be re-discovered on every query.
    pub(crate) unreadable: Arc<AtomicBool>,
    /// Parquet footer, parsed once and reused.
    ///
    /// Segments are immutable, so their metadata can never go stale. Re-reading and
    /// re-parsing the footer on every query is pure fixed cost, and it dominates a
    /// selective query that only touches a few hundred rows.
    metadata: Arc<OnceLock<ArrowReaderMetadata>>,
}

impl Segment {
    /// Whether a previous read of this segment failed.
    #[must_use]
    pub fn is_unreadable(&self) -> bool {
        self.unreadable.load(Ordering::Relaxed)
    }

    /// Mark it unreadable. Returns `true` the first time, so the caller can log once
    /// rather than on every query.
    pub fn mark_unreadable(&self) -> bool {
        !self.unreadable.swap(true, Ordering::Relaxed)
    }

    pub fn data_path(&self) -> PathBuf {
        self.dir.join(DATA_FILE)
    }

    /// Load a segment from its directory.
    ///
    /// Returns `Ok(None)` for a directory without a readable manifest rather than an
    /// error: that is what a crash mid-seal looks like, and the catalogue should skip
    /// it and let the janitor remove it, not refuse to start.
    pub fn load(dir: &Path) -> Result<Option<Self>> {
        let manifest_path = dir.join(MANIFEST_FILE);
        let raw = match fs::read_to_string(&manifest_path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::io(format!("reading {}", manifest_path.display()), e)),
        };

        let manifest: SegmentManifest = match serde_json::from_str(&raw) {
            Ok(manifest) => manifest,
            Err(e) => {
                tracing::warn!(
                    path = %manifest_path.display(),
                    error = %e,
                    "ignoring a segment with an unreadable manifest"
                );
                return Ok(None);
            }
        };

        if manifest.format_version != SEGMENT_FORMAT_VERSION {
            return Err(Error::StorageVersionMismatch {
                path: dir.to_path_buf(),
                found: manifest.format_version,
                expected: SEGMENT_FORMAT_VERSION,
            });
        }
        if !dir.join(DATA_FILE).is_file() {
            tracing::warn!(
                path = %dir.display(),
                "ignoring a segment whose manifest has no data file"
            );
            return Ok(None);
        }

        Ok(Some(Self {
            bloom: Bloom::read(dir),
            text: TrigramIndex::read(dir),
            manifest,
            dir: dir.to_path_buf(),
            unreadable: Arc::new(AtomicBool::new(false)),
            metadata: Arc::new(OnceLock::new()),
        }))
    }

    /// Stream the segment one Arrow batch at a time.
    ///
    /// This is the read path that matters for scale. Materialising a whole segment
    /// before filtering means a query that matches three rows still allocates every
    /// row in the file; at a 256 MiB segment that is the difference between a few
    /// microseconds and a few hundred milliseconds, plus the memory to hold it.
    ///
    /// `visit` returns [`Flow::Stop`] to end the scan early — which is what lets a
    /// `limit`-bounded query stop as soon as it can prove nothing better remains.
    pub fn scan<S, F>(&self, mut visit: F) -> Result<()>
    where
        S: RecordSchema,
        F: FnMut(Vec<S::Record>) -> Result<Flow>,
    {
        let path = self.data_path();
        let file = File::open(&path)
            .map_err(|e| Error::io(format!("opening segment {}", path.display()), e))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| segment_corrupt(&path, &e))?
            .with_batch_size(SCAN_BATCH_ROWS)
            .build()
            .map_err(|e| segment_corrupt(&path, &e))?;

        for batch in reader {
            let batch = batch.map_err(|e| segment_corrupt(&path, &e))?;
            if visit(S::from_batch(&batch)?)? == Flow::Stop {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Stream raw Arrow batches, without decoding anything.
    pub fn scan_batches<F>(&self, visit: F) -> Result<()>
    where
        F: FnMut(&RecordBatch) -> Result<Flow>,
    {
        self.scan_batches_where(None, visit)
    }

    /// Stream batches with a selection predicate pushed **into** the Parquet reader.
    ///
    /// The predicate is evaluated over a projection of just the filter columns, so a
    /// selective query never decompresses the wide columns — bodies, attribute maps,
    /// events — for rows it is going to discard. Rows that survive come back fully
    /// decoded; rows that do not are never touched.
    pub fn scan_batches_where<F>(&self, selection: Option<Selection>, mut visit: F) -> Result<()>
    where
        F: FnMut(&RecordBatch) -> Result<Flow>,
    {
        let path = self.data_path();
        let file = File::open(&path)
            .map_err(|e| Error::io(format!("opening segment {}", path.display()), e))?;

        // Parse the footer once per segment, then reuse it for every later query.
        let metadata = if let Some(metadata) = self.metadata.get() {
            metadata.clone()
        } else {
            let loaded = ArrowReaderMetadata::load(&file, ArrowReaderOptions::default())
                .map_err(|e| segment_corrupt(&path, &e))?;
            let _ = self.metadata.set(loaded.clone());
            loaded
        };

        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata)
            .with_batch_size(SCAN_BATCH_ROWS);

        if let Some(selection) = selection {
            let mask = ProjectionMask::columns(
                builder.parquet_schema(),
                selection.columns.iter().copied(),
            );
            let evaluate = selection.mask;
            let predicate = ArrowPredicateFn::new(mask, move |batch| {
                evaluate(&batch).map_err(|e| {
                    arrow::error::ArrowError::ComputeError(format!("selection failed: {e}"))
                })
            });
            builder = builder.with_row_filter(RowFilter::new(vec![Box::new(predicate)]));
        }

        let reader = builder.build().map_err(|e| segment_corrupt(&path, &e))?;

        for batch in reader {
            let batch = batch.map_err(|e| segment_corrupt(&path, &e))?;
            if visit(&batch)? == Flow::Stop {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Read every record back, resolving stream ids through the manifest dictionary.
    ///
    /// Convenience over [`Self::scan_batches`] for callers that genuinely want the
    /// whole segment; the query path does not.
    pub fn read<S: RecordSchema>(&self) -> Result<Vec<S::Record>> {
        let mut records = Vec::with_capacity(usize::try_from(self.manifest.rows).unwrap_or(0));
        self.scan_batches(|batch| {
            let rows: crate::schema::Rows =
                (0..u32::try_from(batch.num_rows()).unwrap_or(u32::MAX)).collect();
            records.extend(S::materialize(batch, &rows, &self.manifest.streams)?);
            Ok(Flow::Continue)
        })?;
        Ok(records)
    }

    /// Whether this segment might contain `key`, per its exact-match filter.
    ///
    /// False positives are possible, false negatives are not — so a `false` here is
    /// always safe to act on.
    /// Whether any record here might contain `pattern` as a substring.
    ///
    /// `false` is a hard no and the segment can be skipped unread. Without an index —
    /// an older segment, a signal with no text, a damaged file — the answer is `true`,
    /// which is exactly the behaviour that existed before the index did.
    #[must_use]
    pub fn may_contain_text(&self, pattern: &str) -> bool {
        self.text
            .as_ref()
            .is_none_or(|index| index.may_contain(pattern))
    }

    pub fn may_contain_key(&self, key: &str) -> bool {
        self.bloom
            .as_ref()
            .is_none_or(|bloom| bloom.may_contain(key))
    }

    /// Remove the segment directory. Retention is exactly this.
    pub fn delete(&self) -> Result<()> {
        match fs::remove_dir_all(&self.dir) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::io(format!("removing {}", self.dir.display()), e)),
        }
    }
}

fn segment_corrupt(path: &Path, error: &dyn std::fmt::Display) -> Error {
    Error::WalCorrupt {
        path: path.to_path_buf(),
        detail: format!("unreadable Parquet: {error}"),
    }
}

/// Event-time bounds and row count for each interned stream, in stream-id order.
///
/// One pass over the records with a hash lookup each. Sealing already encodes every
/// record into Arrow and compresses it, so this is not the expensive part.
fn stream_statistics<S: RecordSchema>(
    records: &[S::Record],
    streams: &[Labels],
) -> (Vec<(u64, u64)>, Vec<u64>) {
    let index: std::collections::HashMap<u64, usize> = streams
        .iter()
        .enumerate()
        .map(|(id, labels)| (labels.fingerprint(), id))
        .collect();

    let mut bounds = vec![(u64::MAX, u64::MIN); streams.len()];
    let mut rows = vec![0u64; streams.len()];
    for record in records {
        if let Some(&id) = index.get(&S::index_labels(record).fingerprint()) {
            let at = S::timestamp(record);
            bounds[id].0 = bounds[id].0.min(at);
            bounds[id].1 = bounds[id].1.max(at);
            rows[id] += 1;
        }
    }
    (bounds, rows)
}

/// Assigns a dense id to each distinct stream label set.
#[derive(Debug, Default)]
pub struct StreamInterner {
    ids: std::collections::HashMap<Labels, u32>,
    streams: Vec<Labels>,
}

impl StreamInterner {
    pub fn intern(&mut self, labels: &Labels) -> u32 {
        if let Some(id) = self.ids.get(labels) {
            return *id;
        }
        let id = u32::try_from(self.streams.len()).unwrap_or(u32::MAX);
        self.ids.insert(labels.clone(), id);
        self.streams.push(labels.clone());
        id
    }

    pub fn into_streams(self) -> Vec<Labels> {
        self.streams
    }
}

/// Everything needed to seal a buffer.
#[derive(Debug, Clone, Copy)]
pub struct SealOptions<'a> {
    pub segments_dir: &'a Path,
    pub tmp_dir: &'a Path,
    pub compression: Compression,
    pub now_nanos: u64,
    /// Disambiguates two segments sealed within the same nanosecond.
    pub sequence: u64,
    /// The WAL sequence these records came from; recorded in the manifest.
    pub wal_sequence: u64,
}

/// Write `records` as a new immutable segment.
///
/// Staged in `tmp/` and moved with `rename(2)`, so a reader never sees a half-written
/// segment and a crash mid-seal leaves only a `tmp/` directory the janitor removes.
/// The label index and the two filters, built in one pass over the records.
///
/// A schema opts into each by returning `Some` from `exact_key` or `searchable_text`;
/// a signal that returns `None` pays nothing and gets no sidecar file.
fn build_sidecars<S: RecordSchema>(
    records: &[S::Record],
) -> (LabelIndexBuilder, Option<Bloom>, Option<TrigramIndex>) {
    let mut index = LabelIndexBuilder::default();
    let mut bloom = S::exact_key(&records[0]).map(|_| Bloom::with_capacity(records.len()));
    let mut text =
        S::searchable_text(&records[0]).map(|_| TrigramIndex::with_capacity(records.len()));

    for record in records {
        index.observe(S::index_labels(record));
        if let (Some(text), Some(body)) = (text.as_mut(), S::searchable_text(record)) {
            text.insert(body);
        }
        if let (Some(bloom), Some(key)) = (bloom.as_mut(), S::exact_key(record)) {
            bloom.insert(key);
        }
    }
    (index, bloom, text)
}

pub fn seal<S: RecordSchema>(records: &[S::Record], options: SealOptions<'_>) -> Result<Segment> {
    if records.is_empty() {
        return Err(Error::Config(
            "refusing to seal an empty segment".to_owned(),
        ));
    }

    // Sort by time before writing. Records arrive out of order — batching clients,
    // retries, clock skew — and an unsorted timestamp column gives Parquet's row-group
    // statistics nothing to prune with, because every group's min/max ends up spanning
    // the whole segment.
    //
    // Checked first rather than sorted unconditionally: records normally arrive in
    // order, and `to_vec()` on a full buffer is a real cost (it measured at ~20% of
    // seal). Paying it only when the data is actually out of order keeps the common
    // case free.
    let sorted_storage: Vec<S::Record>;
    let records = if records
        .windows(2)
        .all(|w| S::timestamp(&w[0]) <= S::timestamp(&w[1]))
    {
        records
    } else {
        let mut owned = records.to_vec();
        owned.sort_by_key(|record| S::timestamp(record));
        sorted_storage = owned;
        &sorted_storage[..]
    };

    let (index, bloom, text) = build_sidecars::<S>(records);
    // Sorted, so the bounds are simply the endpoints.
    let min_time = S::timestamp(&records[0]);
    let max_time = S::timestamp(&records[records.len() - 1]);

    let id = format!("{min_time:020}-{:08}", options.sequence);
    let staging = options.tmp_dir.join(format!("{}-{id}", S::SIGNAL.as_str()));
    // A leftover from a previous crashed attempt must not be merged into this one.
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .map_err(|e| Error::io(format!("clearing stale staging {}", staging.display()), e))?;
    }
    fs::create_dir_all(&staging)
        .map_err(|e| Error::io(format!("creating {}", staging.display()), e))?;

    let (batch, streams) = S::to_batch(records)?;
    let (stream_bounds, stream_rows) = stream_statistics::<S>(records, &streams);
    let data_path = staging.join(DATA_FILE);
    let bytes = write_parquet(&data_path, &batch, options.compression)?;

    let manifest = SegmentManifest {
        format_version: SEGMENT_FORMAT_VERSION,
        signal: S::SIGNAL,
        id: id.clone(),
        min_time_nanos: min_time,
        max_time_nanos: max_time,
        rows: records.len() as u64,
        bytes,
        created_at_nanos: options.now_nanos,
        wal_sequence: options.wal_sequence,
        labels: index.build(),
        streams,
        stream_bounds,
        stream_rows,
    };
    write_manifest(&staging.join(MANIFEST_FILE), &manifest)?;
    if let Some(bloom) = &bloom {
        bloom.write(&staging)?;
    }
    if let Some(text) = &text {
        text.write(&staging)?;
    }

    // Sync the staging directory so the rename cannot be reordered ahead of the file
    // contents on a crash.
    sync_dir(&staging)?;

    let final_dir = options.segments_dir.join(&id);
    if final_dir.exists() {
        fs::remove_dir_all(&final_dir).map_err(|e| {
            Error::io(
                format!("clearing {} before replacing it", final_dir.display()),
                e,
            )
        })?;
    }
    fs::rename(&staging, &final_dir).map_err(|e| {
        Error::io(
            format!(
                "publishing segment {} -> {}",
                staging.display(),
                final_dir.display()
            ),
            e,
        )
    })?;
    sync_dir(options.segments_dir)?;

    tracing::debug!(
        signal = %S::SIGNAL,
        id = %id,
        rows = manifest.rows,
        bytes = manifest.bytes,
        "sealed a segment"
    );

    Ok(Segment {
        manifest,
        dir: final_dir,
        bloom,
        text,
        unreadable: Arc::new(AtomicBool::new(false)),
        metadata: Arc::new(OnceLock::new()),
    })
}

fn write_parquet(path: &Path, batch: &RecordBatch, compression: Compression) -> Result<u64> {
    let compression = match compression {
        Compression::Zstd => ParquetCompression::ZSTD(ZstdLevel::default()),
        Compression::Snappy => ParquetCompression::SNAPPY,
        Compression::None => ParquetCompression::UNCOMPRESSED,
    };
    let properties = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let file =
        File::create(path).map_err(|e| Error::io(format!("creating {}", path.display()), e))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))
        .map_err(|e| Error::Config(format!("opening a Parquet writer: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| Error::Config(format!("writing Parquet: {e}")))?;
    let file = writer
        .into_inner()
        .map_err(|e| Error::Config(format!("finishing Parquet: {e}")))?;
    file.sync_all()
        .map_err(|e| Error::io(format!("syncing {}", path.display()), e))?;

    Ok(fs::metadata(path)
        .map_err(|e| Error::io(format!("stat {}", path.display()), e))?
        .len())
}

fn write_manifest(path: &Path, manifest: &SegmentManifest) -> Result<()> {
    let json = serde_json::to_vec_pretty(manifest)
        .map_err(|e| Error::Config(format!("serialising a segment manifest: {e}")))?;
    fs::write(path, &json).map_err(|e| Error::io(format!("writing {}", path.display()), e))?;
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(|e| Error::io(format!("syncing {}", path.display()), e))
}

/// fsync a directory so its entries are durable, not just the files inside it.
fn sync_dir(path: &Path) -> Result<()> {
    match File::open(path) {
        Ok(dir) => {
            // Directory fsync is a no-op or an error on some platforms; a failure here
            // is not worth refusing the write that already succeeded.
            if let Err(e) = dir.sync_all() {
                tracing::debug!(path = %path.display(), error = %e, "directory fsync unavailable");
            }
            Ok(())
        }
        Err(e) => Err(Error::io(format!("opening {} to sync", path.display()), e)),
    }
}

/// Scan a signal's segment directory and load every readable segment, oldest first.
pub fn scan(segments_dir: &Path) -> Result<Vec<Segment>> {
    let entries = match fs::read_dir(segments_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(Error::io(format!("reading {}", segments_dir.display()), e)),
    };

    let mut segments = Vec::new();
    for entry in entries {
        let entry =
            entry.map_err(|e| Error::io(format!("reading {}", segments_dir.display()), e))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let Some(segment) = Segment::load(&path)? {
            segments.push(segment);
        }
    }
    segments.sort_by(|a, b| {
        a.manifest
            .min_time_nanos
            .cmp(&b.manifest.min_time_nanos)
            .then_with(|| a.manifest.id.cmp(&b.manifest.id))
    });
    Ok(segments)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use telemetryd_core::MatchOp;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn manifest_with(labels_map: BTreeMap<String, LabelValues>) -> SegmentManifest {
        SegmentManifest {
            format_version: SEGMENT_FORMAT_VERSION,
            signal: Signal::Logs,
            id: "test".to_owned(),
            min_time_nanos: 100,
            max_time_nanos: 200,
            rows: 1,
            bytes: 1,
            created_at_nanos: 0,
            wal_sequence: 0,
            labels: labels_map,
            streams: Vec::new(),
            stream_bounds: Vec::new(),
            stream_rows: Vec::new(),
        }
    }

    #[test]
    fn the_label_index_records_distinct_values() {
        let mut builder = LabelIndexBuilder::default();
        builder.observe(&labels(&[("app", "a"), ("level", "info")]));
        builder.observe(&labels(&[("app", "b"), ("level", "info")]));

        let index = builder.build();
        assert_eq!(
            index["app"].as_set().unwrap(),
            &["a".to_owned(), "b".to_owned()]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        assert_eq!(index["level"].as_set().unwrap().len(), 1);
    }

    #[test]
    fn a_high_cardinality_label_stops_being_tracked() {
        let mut builder = LabelIndexBuilder::default();
        for i in 0..(MAX_TRACKED_LABEL_VALUES + 50) {
            builder.observe(&labels(&[
                ("request_id", &format!("r{i}")),
                ("app", "checkout"),
            ]));
        }
        let index = builder.build();

        // The unbounded label costs nothing further; the bounded one is still exact.
        assert!(matches!(index["request_id"], LabelValues::Unbounded { .. }));
        assert_eq!(index["app"].as_set().unwrap().len(), 1);
    }

    #[test]
    fn pruning_skips_segments_that_cannot_match() {
        let manifest = manifest_with(
            [(
                "app".to_owned(),
                LabelValues::Values(["checkout".to_owned()].into_iter().collect()),
            )]
            .into_iter()
            .collect(),
        );

        assert!(manifest.might_match(&[LabelMatcher::equal("app", "checkout")]));
        assert!(!manifest.might_match(&[LabelMatcher::equal("app", "cart")]));
        assert!(manifest.might_match(&[]));
    }

    #[test]
    fn a_label_absent_from_the_index_prunes_a_selective_matcher() {
        let manifest = manifest_with(BTreeMap::new());
        // No record in this segment had `app` at all.
        assert!(!manifest.might_match(&[LabelMatcher::equal("app", "checkout")]));
    }

    #[test]
    fn pruning_never_skips_a_segment_a_negative_matcher_could_match() {
        let manifest = manifest_with(
            [(
                "app".to_owned(),
                LabelValues::Values(["checkout".to_owned()].into_iter().collect()),
            )]
            .into_iter()
            .collect(),
        );

        // Streams lacking `env` satisfy env!="prod", so this must not prune.
        let negative = LabelMatcher::new("env", MatchOp::NotEqual, "prod").unwrap();
        assert!(manifest.might_match(&[negative]));

        // .* matches the empty string, so it cannot prune either.
        let permissive = LabelMatcher::new("env", MatchOp::Regex, ".*").unwrap();
        assert!(manifest.might_match(&[permissive]));
    }

    #[test]
    fn an_unbounded_label_disables_pruning_rather_than_guessing() {
        let manifest = manifest_with(
            [(
                "request_id".to_owned(),
                LabelValues::Unbounded {
                    distinct_at_cutoff: 257,
                },
            )]
            .into_iter()
            .collect(),
        );
        // We no longer know the values, so we must scan rather than risk a wrong skip.
        assert!(manifest.might_match(&[LabelMatcher::equal("request_id", "anything")]));
    }

    #[test]
    fn regex_matchers_prune_against_recorded_values() {
        let manifest = manifest_with(
            [(
                "app".to_owned(),
                LabelValues::Values(
                    ["checkout".to_owned(), "cart".to_owned()]
                        .into_iter()
                        .collect(),
                ),
            )]
            .into_iter()
            .collect(),
        );

        let hit = LabelMatcher::new("app", MatchOp::Regex, "che.*").unwrap();
        assert!(manifest.might_match(&[hit]));

        let miss = LabelMatcher::new("app", MatchOp::Regex, "billing.*").unwrap();
        assert!(!manifest.might_match(&[miss]));
    }

    #[test]
    fn time_overlap_is_inclusive_on_both_ends() {
        let manifest = manifest_with(BTreeMap::new());
        assert!(manifest.overlaps(100, 200));
        assert!(manifest.overlaps(0, 100), "touching the start counts");
        assert!(manifest.overlaps(200, 999), "touching the end counts");
        assert!(manifest.overlaps(120, 130), "fully contained");
        assert!(manifest.overlaps(0, 9999), "fully containing");
        assert!(!manifest.overlaps(0, 99));
        assert!(!manifest.overlaps(201, 300));
    }
}
