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

pub trait RecordSchema: Send + Sync + 'static {
    /// The decoded record type, as it exists in memory and in the WAL.
    type Record: Serialize + DeserializeOwned + Clone + Send + Sync + std::fmt::Debug + 'static;

    /// Which signal this is; decides the on-disk directory.
    const SIGNAL: Signal;

    /// Arrow schema for the sealed Parquet file.
    fn arrow_schema() -> SchemaRef;

    fn to_batch(records: &[Self::Record]) -> Result<RecordBatch>;

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self::Record>>;

    /// Event time in Unix nanoseconds. Drives segment time bounds and query pruning.
    fn timestamp(record: &Self::Record) -> u64;

    /// The labels that identify this record's stream. Indexed in the manifest, so
    /// these are what a selector can prune on without opening the Parquet file.
    fn index_labels(record: &Self::Record) -> &Labels;

    /// Approximate heap cost, used to decide when a buffer is full.
    fn size_estimate(record: &Self::Record) -> usize;
}

/// Shared Arrow helpers so each schema does not reimplement them.
pub(crate) mod arrow_util {
    use arrow::array::{Array, StringArray, UInt64Array};
    use telemetryd_core::{Error, Labels, Result};

    /// Serialise a label set for storage.
    ///
    /// JSON rather than an Arrow `Map` column: streams repeat heavily within a
    /// segment, so Parquet's dictionary encoding collapses the repetition to
    /// near-nothing, and the read path stays simple enough to audit. The hot filters
    /// (`app`, `level`) are hoisted into real columns, so this is only paid when a
    /// query actually inspects the full label set. Revisit if profiling says
    /// otherwise — the segment format is versioned.
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
