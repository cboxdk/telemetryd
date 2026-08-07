//! The Arrow/Parquet schema for log records.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use telemetryd_core::record::{APP_LABEL, LEVEL_LABEL, UNKNOWN_APP};
use telemetryd_core::{Error, Labels, LogRecord, Result, Severity, Signal};

use crate::schema::arrow_util::{
    labels_from_json, labels_to_json, optional_string, string_column, u32_column, u64_column,
};
use crate::schema::{RecordSchema, Rows, schema_ref};

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
            // Index into the segment's stream dictionary, not the label set itself.
            // Decoding a JSON label map per row measured at ~500ns and dominated
            // every query; an integer costs nothing and lets matchers be evaluated
            // once per distinct stream.
            Field::new("stream_id", DataType::UInt32, false),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
        ])
    }

    fn to_batch(records: &[Self::Record]) -> Result<(RecordBatch, Vec<Labels>)> {
        let timestamps = UInt64Array::from_iter_values(records.iter().map(|r| r.timestamp_nanos));
        let apps = StringArray::from_iter_values(records.iter().map(LogRecord::app));
        let levels = StringArray::from_iter_values(records.iter().map(|r| r.severity.as_str()));
        let severity_texts: StringArray = records
            .iter()
            .map(|r| Some(r.severity_text.clone()))
            .collect();
        let bodies = StringArray::from_iter_values(records.iter().map(|r| r.body.as_str()));
        // Interned identically to `segment::seal`, so ids line up with the manifest
        // dictionary. Both walk the records in the same (sorted) order.
        let mut interner = crate::segment::StreamInterner::default();
        let stream_ids =
            UInt32Array::from_iter_values(records.iter().map(|r| interner.intern(&r.stream)));
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
            Arc::new(stream_ids),
            Arc::new(attributes),
            Arc::new(trace_ids),
            Arc::new(span_ids),
        ];

        let batch = RecordBatch::try_new(Self::arrow_schema(), columns)
            .map_err(|e| Error::Config(format!("building a log record batch: {e}")))?;
        Ok((batch, interner.into_streams()))
    }

    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self::Record>> {
        // No dictionary available (this path is used by tests and by `read`), so the
        // stream is rebuilt from the hoisted columns.
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
            // An id past the dictionary means the segment predates interning; keep the
            // row and let the record-level predicate decide, rather than dropping it.
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
        let levels = string_column(batch, "level")?;
        let severity_texts = string_column(batch, "severity_text")?;
        let bodies = string_column(batch, "body")?;
        let stream_ids = u32_column(batch, "stream_id")?;
        let attributes = string_column(batch, "attributes")?;
        let trace_ids = string_column(batch, "trace_id")?;
        let span_ids = string_column(batch, "span_id")?;
        let apps = string_column(batch, "app")?;

        let mut out = Vec::with_capacity(rows.len());
        for &row in rows {
            let row = row as usize;
            // No dictionary (a pre-interning segment, or `from_batch`): rebuild what
            // queries prune on from the hoisted columns so the record stays usable.
            let stream = streams.get(stream_ids.value(row) as usize).map_or_else(
                || {
                    let mut stream = Labels::new();
                    stream.insert(APP_LABEL, apps.value(row));
                    stream.insert(LEVEL_LABEL, levels.value(row));
                    stream
                },
                Clone::clone,
            );

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
                // An id past the dictionary means a segment written before interning;
                // keep the row and let the record predicate decide rather than dropping it.
                Some(allowed_streams.is_empty() || allowed_streams.get(id).copied().unwrap_or(true))
            })
            .collect())
    }

    fn text_column() -> Option<&'static str> {
        Some("body")
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

    /// Decode a whole batch through the dictionary its ids refer to.
    fn materialize_all<S: RecordSchema>(batch: &RecordBatch, streams: &[Labels]) -> Vec<S::Record> {
        let rows: Rows = (0..u32::try_from(batch.num_rows()).unwrap_or(u32::MAX)).collect();
        S::materialize(batch, &rows, streams).unwrap()
    }

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
        let (batch, streams) = LogSchema::to_batch(&records).unwrap();
        assert_eq!(batch.num_rows(), 64);

        let restored = materialize_all::<LogSchema>(&batch, &streams);
        assert_eq!(restored, records);
    }

    #[test]
    fn optional_columns_survive_being_absent() {
        let mut record = sample(1);
        record.trace_id = None;
        record.span_id = None;
        record.severity_text = String::new();
        record.attributes = Labels::new();

        let (batch, streams) = LogSchema::to_batch(&[record.clone()]).unwrap();
        let restored = materialize_all::<LogSchema>(&batch, &streams);
        assert_eq!(restored[0], record);
        assert_eq!(restored[0].trace_id, None);
        assert!(restored[0].attributes.is_empty());
    }

    #[test]
    fn an_empty_batch_is_valid() {
        let (batch, streams) = LogSchema::to_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert!(materialize_all::<LogSchema>(&batch, &streams).is_empty());
    }

    #[test]
    fn bodies_with_awkward_content_survive() {
        let mut record = sample(1);
        // Newlines, quotes, unicode and NUL all appear in real stack traces.
        record.body = "line1\nline2 \"quoted\" \\ æøå 🎉 \u{0}end".to_owned();
        record.attributes.insert("weird\"key", "va\\lue\n");

        let (batch, streams) = LogSchema::to_batch(&[record.clone()]).unwrap();
        let restored = materialize_all::<LogSchema>(&batch, &streams);
        assert_eq!(restored[0], record);
    }

    #[test]
    fn the_hoisted_columns_match_the_label_set() {
        let (batch, _streams) = LogSchema::to_batch(&[sample(1)]).unwrap();
        let apps = string_column(&batch, "app").unwrap();
        let levels = string_column(&batch, "level").unwrap();
        assert_eq!(apps.value(0), "checkout");
        assert_eq!(levels.value(0), "error");
    }
}
