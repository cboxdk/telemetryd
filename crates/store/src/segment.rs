//! Immutable on-disk segments: Parquet data plus a manifest that describes it well
//! enough to skip opening the data at all.
//!
//! A segment is one directory, written to `tmp/` and moved into place with
//! `rename(2)`, so a segment is either completely visible or not visible — there is no
//! partially-published state a reader can observe. Retention deletes whole
//! directories; there are no tombstones and no row-level deletes (ADR-001).

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression as ParquetCompression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use telemetryd_core::config::Compression;
use telemetryd_core::{Error, LabelMatcher, Labels, Result, Signal};

use crate::schema::RecordSchema;

/// Bumped when a sealed segment written by an older build can no longer be read.
pub const SEGMENT_FORMAT_VERSION: u32 = 1;

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

/// A sealed segment on disk.
#[derive(Debug, Clone)]
pub struct Segment {
    pub manifest: SegmentManifest,
    pub dir: PathBuf,
}

impl Segment {
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
            manifest,
            dir: dir.to_path_buf(),
        }))
    }

    /// Read every record back.
    pub fn read<S: RecordSchema>(&self) -> Result<Vec<S::Record>> {
        let path = self.data_path();
        let file = File::open(&path)
            .map_err(|e| Error::io(format!("opening segment {}", path.display()), e))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| segment_corrupt(&path, &e))?
            .build()
            .map_err(|e| segment_corrupt(&path, &e))?;

        // Row counts come from our own manifest and are bounded by max_segment_bytes,
        // so this cannot overflow a usize on any target we ship.
        let mut records = Vec::with_capacity(usize::try_from(self.manifest.rows).unwrap_or(0));
        for batch in reader {
            let batch = batch.map_err(|e| segment_corrupt(&path, &e))?;
            records.extend(S::from_batch(&batch)?);
        }
        Ok(records)
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
pub fn seal<S: RecordSchema>(records: &[S::Record], options: SealOptions<'_>) -> Result<Segment> {
    if records.is_empty() {
        return Err(Error::Config(
            "refusing to seal an empty segment".to_owned(),
        ));
    }

    let mut min_time = u64::MAX;
    let mut max_time = 0;
    let mut index = LabelIndexBuilder::default();
    for record in records {
        let ts = S::timestamp(record);
        min_time = min_time.min(ts);
        max_time = max_time.max(ts);
        index.observe(S::index_labels(record));
    }

    let id = format!("{min_time:020}-{:08}", options.sequence);
    let staging = options.tmp_dir.join(format!("{}-{id}", S::SIGNAL.as_str()));
    // A leftover from a previous crashed attempt must not be merged into this one.
    if staging.exists() {
        fs::remove_dir_all(&staging)
            .map_err(|e| Error::io(format!("clearing stale staging {}", staging.display()), e))?;
    }
    fs::create_dir_all(&staging)
        .map_err(|e| Error::io(format!("creating {}", staging.display()), e))?;

    let batch = S::to_batch(records)?;
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
    };
    write_manifest(&staging.join(MANIFEST_FILE), &manifest)?;

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
