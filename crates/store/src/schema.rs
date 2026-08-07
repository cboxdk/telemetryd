//! The contract a signal must satisfy to use the record store.
//!
//! Logs, spans and events share identical storage machinery and differ only in their
//! Arrow schema (ADR-001). This trait is that difference, and nothing else — so the
//! WAL, buffering, sealing, manifest, catalogue, pruning and retention are written
//! once and M2's traces reuse them rather than growing a parallel copy.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde::Serialize;
use serde::de::DeserializeOwned;
use telemetryd_core::{Labels, Result, Signal};

/// Row indices within a batch, in ascending order.
pub type Rows = Vec<u32>;

/// Narrows a candidate row set using column data only.
///
/// **May over-select, must never under-select.** The record-level predicate stays the
/// authority, so a columnar filter that is approximate is safe — it only decides how
/// much work is skipped, never what the answer is. Same contract as the Bloom filter.
pub type ColumnFilter<'a> = &'a dyn Fn(&RecordBatch, &mut Rows) -> Result<()>;

pub trait RecordSchema: Send + Sync + 'static {
    /// The decoded record type, as it exists in memory and in the WAL.
    type Record: Serialize + DeserializeOwned + Clone + Send + Sync + std::fmt::Debug + 'static;

    /// Which signal this is; decides the on-disk directory.
    const SIGNAL: Signal;

    /// Arrow schema for the sealed Parquet file.
    fn arrow_schema() -> SchemaRef;

    /// Encode records, returning the batch **and** the stream dictionary its
    /// `stream_id` column refers to.
    ///
    /// Returned together on purpose: the ids and the dictionary are one artefact, and
    /// building them in two places and trusting them to agree is the kind of coupling
    /// that silently mislabels rows the first time the two walks diverge.
    fn to_batch(records: &[Self::Record]) -> Result<(RecordBatch, Vec<Labels>)>;

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self::Record>>;

    /// Select rows by time range and permitted streams, **without decoding them**.
    ///
    /// This is the difference between a query costing the size of the answer and
    /// costing the size of the data. Decoding a record allocates a handful of strings
    /// and two maps; doing that for every row before filtering means a `limit=100`
    /// query pays for every row it scans. Here only the timestamp and stream-id
    /// columns are touched, and materialisation happens once the survivors are known.
    ///
    /// `allowed_streams` is indexed by stream id — the matchers were evaluated once
    /// per distinct stream in the segment, not once per row.
    fn select_rows(
        batch: &RecordBatch,
        start_nanos: u64,
        end_nanos: u64,
        allowed_streams: &[bool],
    ) -> Result<Rows>;

    /// Decode exactly these rows, resolving stream ids through the segment dictionary.
    fn materialize(
        batch: &RecordBatch,
        rows: &Rows,
        streams: &[Labels],
    ) -> Result<Vec<Self::Record>>;

    /// The batch column holding the searchable text body, if the signal has one.
    /// Lets a line filter run over the Arrow buffer with no allocation at all.
    fn text_column() -> Option<&'static str> {
        None
    }

    /// Columns needed to decide whether a row is wanted, before decoding it.
    ///
    /// These are pushed into Parquet as a projection so the reader decompresses only
    /// them during filtering. Everything else — bodies, attribute maps, events — is
    /// only touched for rows that survive. On a selective query that is the difference
    /// between decompressing a whole row group and decompressing two columns of it.
    fn filter_columns() -> &'static [&'static str];

    /// Build a selection mask from a batch projected to [`Self::filter_columns`].
    fn selection_mask(
        batch: &RecordBatch,
        start_nanos: u64,
        end_nanos: u64,
        allowed_streams: &[bool],
    ) -> Result<arrow::array::BooleanArray>;

    /// Event time in Unix nanoseconds. Drives segment time bounds and query pruning.
    fn timestamp(record: &Self::Record) -> u64;

    /// The labels that identify this record's stream. Indexed in the manifest, so
    /// these are what a selector can prune on without opening the Parquet file.
    fn index_labels(record: &Self::Record) -> &Labels;

    /// Approximate heap cost, used to decide when a buffer is full.
    fn size_estimate(record: &Self::Record) -> usize;

    /// A high-cardinality identifier that queries look up by exact value.
    ///
    /// `Some` builds a per-segment Bloom filter over it, which is what makes
    /// "fetch this trace by id" read one segment instead of the whole retention
    /// window. Return `None` when the signal has no such lookup — logs do not.
    fn exact_key(record: &Self::Record) -> Option<&str> {
        let _ = record;
        None
    }
}

/// Shared Arrow helpers so each schema does not reimplement them.
pub(crate) mod arrow_util {
    use arrow::array::{Array, StringArray, UInt64Array};
    use telemetryd_core::{Error, Labels, Result};

    /// Serialise a label set for storage.
    ///
    /// Used for *record* attributes, which are genuinely per-row and high cardinality.
    /// Stream labels do **not** go through here — they are interned into a per-segment
    /// dictionary and referenced by a `u32`, because decoding JSON per row was
    /// measured at ~500 ns/record and dominated every query.
    pub(crate) fn labels_to_json(labels: &Labels) -> String {
        serde_json::to_string(labels).unwrap_or_else(|_| "{}".to_owned())
    }

    pub(crate) fn labels_from_json(raw: &str) -> Labels {
        serde_json::from_str(raw).unwrap_or_default()
    }

    pub(crate) fn string_column<'a>(
        batch: &'a arrow::record_batch::RecordBatch,
        name: &str,
    ) -> Result<&'a StringArray> {
        batch
            .column_by_name(name)
            .ok_or_else(|| missing(name))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| wrong_type(name, "Utf8"))
    }

    pub(crate) fn u32_column<'a>(
        batch: &'a arrow::record_batch::RecordBatch,
        name: &str,
    ) -> Result<&'a arrow::array::UInt32Array> {
        batch
            .column_by_name(name)
            .ok_or_else(|| missing(name))?
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .ok_or_else(|| wrong_type(name, "UInt32"))
    }

    pub(crate) fn u64_column<'a>(
        batch: &'a arrow::record_batch::RecordBatch,
        name: &str,
    ) -> Result<&'a UInt64Array> {
        batch
            .column_by_name(name)
            .ok_or_else(|| missing(name))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| wrong_type(name, "UInt64"))
    }

    /// Read a nullable string cell, mapping empty-or-null to `None`.
    pub(crate) fn optional_string(array: &StringArray, row: usize) -> Option<String> {
        if array.is_null(row) {
            return None;
        }
        let value = array.value(row);
        (!value.is_empty()).then(|| value.to_owned())
    }

    fn missing(name: &str) -> Error {
        Error::WalCorrupt {
            path: std::path::PathBuf::from("<segment>"),
            detail: format!("segment is missing the {name:?} column"),
        }
    }

    fn wrong_type(name: &str, expected: &str) -> Error {
        Error::WalCorrupt {
            path: std::path::PathBuf::from("<segment>"),
            detail: format!("column {name:?} is not {expected}"),
        }
    }
}

/// Convenience alias for the reference-counted schema each impl returns.
pub(crate) fn schema_ref(fields: Vec<arrow::datatypes::Field>) -> SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(fields))
}
