//! The Arrow/Parquet schema for log records.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use telemetryd_core::record::{APP_LABEL, LEVEL_LABEL, UNKNOWN_APP};
use telemetryd_core::{Error, Labels, LogRecord, Result, Severity, Signal};

use crate::schema::arrow_util::{
    labels_from_json, labels_to_json, optional_string, string_column, u64_column,
};
use crate::schema::{RecordSchema, schema_ref};

#[derive(Debug, Clone, Copy)]
pub struct LogSchema;

impl RecordSchema for LogSchema {
    type Record = LogRecord;

    const SIGNAL: Signal = Signal::Logs;

    /// `app` and `level` are hoisted into real columns because they are on nearly
    /// every query — that gives Parquet row-group statistics something to prune with.
    /// The rest of the label set travels as JSON; see `arrow_util::labels_to_json`.
    fn arrow_schema() -> SchemaRef {
        schema_ref(vec![
            Field::new("timestamp_nanos", DataType::UInt64, false),
            Field::new("app", DataType::Utf8, false),
            Field::new("level", DataType::Utf8, false),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, false),
            Field::new("stream", DataType::Utf8, false),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
        ])
    }

    fn to_batch(records: &[Self::Record]) -> Result<RecordBatch> {
        let timestamps = UInt64Array::from_iter_values(records.iter().map(|r| r.timestamp_nanos));
        let apps = StringArray::from_iter_values(records.iter().map(LogRecord::app));
        let levels = StringArray::from_iter_values(records.iter().map(|r| r.severity.as_str()));
        let severity_texts: StringArray = records
            .iter()
            .map(|r| Some(r.severity_text.clone()))
            .collect();
        let bodies = StringArray::from_iter_values(records.iter().map(|r| r.body.as_str()));
        let streams =
            StringArray::from_iter_values(records.iter().map(|r| labels_to_json(&r.stream)));
        let attributes =
            StringArray::from_iter_values(records.iter().map(|r| labels_to_json(&r.attributes)));
        let trace_ids: StringArray = records.iter().map(|r| r.trace_id.clone()).collect();
        let span_ids: StringArray = records.iter().map(|r| r.span_id.clone()).collect();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamps),
            Arc::new(apps),
            Arc::new(levels),
            Arc::new(severity_texts),
            Arc::new(bodies),
            Arc::new(streams),
            Arc::new(attributes),
            Arc::new(trace_ids),
            Arc::new(span_ids),
        ];

        RecordBatch::try_new(Self::arrow_schema(), columns)
            .map_err(|e| Error::Config(format!("building a log record batch: {e}")))
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self::Record>> {
        let timestamps = u64_column(batch, "timestamp_nanos")?;
        let levels = string_column(batch, "level")?;
        let severity_texts = string_column(batch, "severity_text")?;
        let bodies = string_column(batch, "body")?;
        let streams = string_column(batch, "stream")?;
        let attributes = string_column(batch, "attributes")?;
        let trace_ids = string_column(batch, "trace_id")?;
        let span_ids = string_column(batch, "span_id")?;
        let apps = string_column(batch, "app")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut stream = labels_from_json(streams.value(row));
            // The hoisted columns are authoritative: they are what queries prune on,
            // so a segment where the JSON and the column disagree must resolve the
            // same way at read time as it did at write time.
            if !stream.contains_key(APP_LABEL) {
                stream.insert(APP_LABEL, apps.value(row));
            }
            if !stream.contains_key(LEVEL_LABEL) {
                stream.insert(LEVEL_LABEL, levels.value(row));
            }

            out.push(LogRecord {
                timestamp_nanos: timestamps.value(row),
                stream,
                severity: Severity::from_text(levels.value(row)),
                severity_text: optional_string(severity_texts, row).unwrap_or_default(),
                body: bodies.value(row).to_owned(),
                attributes: labels_from_json(attributes.value(row)),
                trace_id: optional_string(trace_ids, row),
                span_id: optional_string(span_ids, row),
            });
        }
        Ok(out)
    }

    fn timestamp(record: &Self::Record) -> u64 {
        record.timestamp_nanos
    }

    fn index_labels(record: &Self::Record) -> &Labels {
        &record.stream
    }

    fn size_estimate(record: &Self::Record) -> usize {
        record.size_estimate()
    }
}

/// The app a record belongs to, for retention and quota accounting.
pub fn record_app(record: &LogRecord) -> &str {
    record.stream.get(APP_LABEL).unwrap_or(UNKNOWN_APP)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn sample(i: u64) -> LogRecord {
        let mut stream = Labels::new();
        stream.insert("app", "checkout");
        stream.insert("level", "error");
        stream.insert("service_name", "checkout");

        let mut attributes = Labels::new();
        attributes.insert("order_id", format!("{i}"));

        LogRecord {
            timestamp_nanos: 1_750_000_000_000_000_000 + i,
            stream,
            severity: Severity::Error,
            severity_text: "ERROR".to_owned(),
            body: format!("payment declined #{i}"),
            attributes,
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_owned()),
            span_id: None,
        }
    }

    #[test]
    fn records_round_trip_through_arrow_unchanged() {
        let records: Vec<LogRecord> = (0..64).map(sample).collect();
        let batch = LogSchema::to_batch(&records).unwrap();
        assert_eq!(batch.num_rows(), 64);

        let restored = LogSchema::from_batch(&batch).unwrap();
        assert_eq!(restored, records);
    }

    #[test]
    fn optional_columns_survive_being_absent() {
        let mut record = sample(1);
        record.trace_id = None;
        record.span_id = None;
        record.severity_text = String::new();
        record.attributes = Labels::new();

        let batch = LogSchema::to_batch(&[record.clone()]).unwrap();
        let restored = LogSchema::from_batch(&batch).unwrap();
        assert_eq!(restored[0], record);
        assert_eq!(restored[0].trace_id, None);
        assert!(restored[0].attributes.is_empty());
    }

    #[test]
    fn an_empty_batch_is_valid() {
        let batch = LogSchema::to_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(LogSchema::from_batch(&batch).unwrap().is_empty());
    }

    #[test]
    fn bodies_with_awkward_content_survive() {
        let mut record = sample(1);
        // Newlines, quotes, unicode and NUL all appear in real stack traces.
        record.body = "line1\nline2 \"quoted\" \\ æøå 🎉 \u{0}end".to_owned();
        record.attributes.insert("weird\"key", "va\\lue\n");

        let batch = LogSchema::to_batch(&[record.clone()]).unwrap();
        let restored = LogSchema::from_batch(&batch).unwrap();
        assert_eq!(restored[0], record);
    }

    #[test]
    fn the_hoisted_columns_match_the_label_set() {
        let batch = LogSchema::to_batch(&[sample(1)]).unwrap();
        let apps = string_column(&batch, "app").unwrap();
        let levels = string_column(&batch, "level").unwrap();
        assert_eq!(apps.value(0), "checkout");
        assert_eq!(levels.value(0), "error");
    }
}
