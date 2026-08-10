//! The Arrow/Parquet schema for trace spans.
//!
//! Reuses every piece of the record store — WAL, buffering, sealing, manifest,
//! pruning, retention. Only the columns differ, which is what the [`RecordSchema`]
//! trait exists for.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use telemetryd_core::span::{SpanEvent, SpanKind, SpanRecord, SpanStatus};
use telemetryd_core::{Error, Labels, Result, Signal};

use crate::schema::arrow_util::{
    labels_from_json, labels_to_json, optional_string, string_column, u32_column, u64_column,
};
use crate::schema::{RecordSchema, Rows, schema_ref};

#[derive(Debug, Clone, Copy)]
pub struct SpanSchema;

impl RecordSchema for SpanSchema {
    type Record = SpanRecord;

    const SIGNAL: Signal = Signal::Traces;

    /// `trace_id` is a real column, not just a label: fetching a trace by id is the
    /// single most common trace query, and it needs Parquet statistics to prune on.
    fn arrow_schema() -> SchemaRef {
        schema_ref(vec![
            Field::new("start_nanos", DataType::UInt64, false),
            Field::new("end_nanos", DataType::UInt64, false),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("app", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
            Field::new("status_message", DataType::Utf8, true),
            // Index into the segment's stream dictionary — see LogSchema.
            Field::new("stream_id", DataType::UInt32, false),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("events", DataType::Utf8, false),
        ])
    }

    fn to_batch(records: &[Self::Record]) -> Result<(RecordBatch, Vec<Labels>)> {
        let starts = UInt64Array::from_iter_values(records.iter().map(|s| s.start_nanos));
        let ends = UInt64Array::from_iter_values(records.iter().map(|s| s.end_nanos));
        let trace_ids = StringArray::from_iter_values(records.iter().map(|s| s.trace_id.as_str()));
        let span_ids = StringArray::from_iter_values(records.iter().map(|s| s.span_id.as_str()));
        let parents: StringArray = records.iter().map(|s| s.parent_span_id.clone()).collect();
        let apps = StringArray::from_iter_values(records.iter().map(SpanRecord::app));
        let names = StringArray::from_iter_values(records.iter().map(|s| s.name.as_str()));
        let kinds = StringArray::from_iter_values(records.iter().map(|s| s.kind.as_str()));
        let statuses = StringArray::from_iter_values(records.iter().map(|s| s.status.as_str()));
        let messages: StringArray = records
            .iter()
            .map(|s| Some(s.status_message.clone()))
            .collect();
        let mut interner = crate::segment::StreamInterner::default();
        let stream_ids =
            UInt32Array::from_iter_values(records.iter().map(|s| interner.intern(&s.stream)));
        let attributes =
            StringArray::from_iter_values(records.iter().map(|s| labels_to_json(&s.attributes)));
        let events =
            StringArray::from_iter_values(records.iter().map(|s| encode_events(&s.events)));

        let columns: Vec<ArrayRef> = vec![
            Arc::new(starts),
            Arc::new(ends),
            Arc::new(trace_ids),
            Arc::new(span_ids),
            Arc::new(parents),
            Arc::new(apps),
            Arc::new(names),
            Arc::new(kinds),
            Arc::new(statuses),
            Arc::new(messages),
            Arc::new(stream_ids),
            Arc::new(attributes),
            Arc::new(events),
        ];

        let batch = RecordBatch::try_new(Self::arrow_schema(), columns)
            .map_err(|e| Error::Config(format!("building a span record batch: {e}")))?;
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
        let starts = u64_column(batch, "start_nanos")?;
        let stream_ids = u32_column(batch, "stream_id")?;

        let mut rows = Rows::new();
        for row in 0..batch.num_rows() {
            let ts = starts.value(row);
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
        let starts = u64_column(batch, "start_nanos")?;
        let ends = u64_column(batch, "end_nanos")?;
        let trace_ids = string_column(batch, "trace_id")?;
        let span_ids = string_column(batch, "span_id")?;
        let parents = string_column(batch, "parent_span_id")?;
        let names = string_column(batch, "name")?;
        let kinds = string_column(batch, "kind")?;
        let statuses = string_column(batch, "status")?;
        let messages = string_column(batch, "status_message")?;
        let stream_ids = u32_column(batch, "stream_id")?;
        let attributes = string_column(batch, "attributes")?;
        let events = string_column(batch, "events")?;
        let apps = string_column(batch, "app")?;

        let mut out = Vec::with_capacity(rows.len());
        for &row in rows {
            let row = row as usize;
            let stream = streams.get(stream_ids.value(row) as usize).map_or_else(
                || {
                    let mut stream = Labels::new();
                    stream.insert(telemetryd_core::record::APP_LABEL, apps.value(row));
                    stream
                },
                Clone::clone,
            );

            out.push(SpanRecord {
                trace_id: trace_ids.value(row).to_owned(),
                span_id: span_ids.value(row).to_owned(),
                parent_span_id: optional_string(parents, row),
                name: names.value(row).to_owned(),
                kind: parse_kind(kinds.value(row)),
                start_nanos: starts.value(row),
                end_nanos: ends.value(row),
                status: parse_status(statuses.value(row)),
                status_message: optional_string(messages, row).unwrap_or_default(),
                stream,
                attributes: labels_from_json(attributes.value(row)),
                events: decode_events(events.value(row)),
            });
        }
        Ok(out)
    }

    fn filter_columns() -> &'static [&'static str] {
        &["start_nanos", "stream_id"]
    }

    fn selection_mask(
        batch: &RecordBatch,
        start_nanos: u64,
        end_nanos: u64,
        allowed_streams: &[bool],
    ) -> Result<arrow::array::BooleanArray> {
        let timestamps = u64_column(batch, "start_nanos")?;
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
        Some("name")
    }

    /// Spans are indexed by **start** time. A query's time range means "spans that
    /// started in this window", which is what a trace list shows.
    fn timestamp(record: &Self::Record) -> u64 {
        record.start_nanos
    }

    fn index_labels(record: &Self::Record) -> &Labels {
        &record.stream
    }

    fn size_estimate(record: &Self::Record) -> usize {
        record.size_estimate()
    }

    /// Trace id. Fetching a trace by id is the most common trace query and the one
    /// nothing else can prune, so it gets the Bloom filter.
    fn exact_key(record: &Self::Record) -> Option<&str> {
        Some(&record.trace_id)
    }
}

fn parse_kind(raw: &str) -> SpanKind {
    match raw {
        "internal" => SpanKind::Internal,
        "server" => SpanKind::Server,
        "client" => SpanKind::Client,
        "producer" => SpanKind::Producer,
        "consumer" => SpanKind::Consumer,
        _ => SpanKind::Unspecified,
    }
}

fn parse_status(raw: &str) -> SpanStatus {
    match raw {
        "ok" => SpanStatus::Ok,
        "error" => SpanStatus::Error,
        _ => SpanStatus::Unset,
    }
}

/// Events travel as JSON in one column.
///
/// They are a nested, optional, usually-empty list; an Arrow list-of-struct column
/// would cost a schema that is markedly harder to read back for data that most spans
/// do not have at all.
fn encode_events(events: &[SpanEvent]) -> String {
    if events.is_empty() {
        return "[]".to_owned();
    }
    serde_json::to_string(events).unwrap_or_else(|_| "[]".to_owned())
}

fn decode_events(raw: &str) -> Vec<SpanEvent> {
    serde_json::from_str(raw).unwrap_or_default()
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

    fn span(i: u64) -> SpanRecord {
        let mut stream = Labels::new();
        stream.insert("app", "checkout");
        stream.insert("service_name", "checkout");

        let mut attributes = Labels::new();
        attributes.insert("http_method", "POST");
        attributes.insert("http_status_code", "500");

        SpanRecord {
            trace_id: format!("{i:032x}"),
            span_id: format!("{i:016x}"),
            parent_span_id: (i > 0).then(|| format!("{:016x}", i - 1)),
            name: format!("operation {i}"),
            kind: SpanKind::Server,
            start_nanos: 1_750_000_000_000_000_000 + i,
            end_nanos: 1_750_000_000_150_000_000 + i,
            status: SpanStatus::Error,
            status_message: "payment declined".to_owned(),
            stream,
            attributes,
            events: vec![SpanEvent {
                time_nanos: 1_750_000_000_100_000_000,
                name: "exception".to_owned(),
                attributes: [("exception_type".to_owned(), "PaymentError".to_owned())]
                    .into_iter()
                    .collect(),
            }],
        }
    }

    #[test]
    fn spans_round_trip_through_arrow_unchanged() {
        let records: Vec<SpanRecord> = (0..32).map(span).collect();
        let (batch, streams) = SpanSchema::to_batch(&records).unwrap();
        assert_eq!(batch.num_rows(), 32);
        assert_eq!(materialize_all::<SpanSchema>(&batch, &streams), records);
    }

    #[test]
    fn every_kind_and_status_survives_the_round_trip() {
        let mut records = Vec::new();
        for kind in [
            SpanKind::Unspecified,
            SpanKind::Internal,
            SpanKind::Server,
            SpanKind::Client,
            SpanKind::Producer,
            SpanKind::Consumer,
        ] {
            for status in [SpanStatus::Unset, SpanStatus::Ok, SpanStatus::Error] {
                let mut record = span(1);
                record.kind = kind;
                record.status = status;
                records.push(record);
            }
        }

        let (batch, streams) = SpanSchema::to_batch(&records).unwrap();
        let restored = materialize_all::<SpanSchema>(&batch, &streams);
        for (before, after) in records.iter().zip(&restored) {
            assert_eq!(before.kind, after.kind);
            assert_eq!(before.status, after.status, "unset must not become ok");
        }
    }

    #[test]
    fn a_root_span_keeps_its_absent_parent() {
        let mut record = span(0);
        record.parent_span_id = None;
        record.events = Vec::new();
        record.status_message = String::new();

        let (batch, streams) = SpanSchema::to_batch(&[record.clone()]).unwrap();
        let restored = materialize_all::<SpanSchema>(&batch, &streams);
        assert_eq!(restored[0], record);
        assert!(restored[0].is_root());
    }

    #[test]
    fn events_survive_including_their_attributes() {
        let (batch, streams) = SpanSchema::to_batch(&[span(1)]).unwrap();
        let restored = materialize_all::<SpanSchema>(&batch, &streams);
        assert_eq!(restored[0].events.len(), 1);
        assert_eq!(
            restored[0].events[0].attributes.get("exception_type"),
            Some("PaymentError")
        );
    }

    #[test]
    fn an_empty_batch_is_valid() {
        let (batch, streams) = SpanSchema::to_batch(&[]).unwrap();
        assert!(materialize_all::<SpanSchema>(&batch, &streams).is_empty());
    }

    #[test]
    fn spans_are_indexed_by_start_time() {
        let record = span(0);
        assert_eq!(SpanSchema::timestamp(&record), record.start_nanos);
    }
}
