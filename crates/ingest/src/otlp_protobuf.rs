//! OTLP/HTTP **protobuf** decoding, for senders that are not `laravel-telemetry`.
//!
//! # Why this exists
//!
//! Every official OpenTelemetry SDK defaults to `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`.
//! telemetryd served JSON only, so a stock exporter pointed at it was refused on every
//! batch and stored nothing until someone found `http/json` in the documentation. The
//! refusal was clear, which is not the same as being usable.
//!
//! # What this file is, and is not
//!
//! It decodes the OTLP wire format into the **same structs the JSON decoder produces**,
//! and then hands them to the same [`crate::logs::decode`], [`crate::traces::decode`] and
//! [`crate::otlp_metrics::decode`] conversion paths. Timestamp rescaling, the attribute
//! rules, every limit, every rejection reason and every counter are therefore shared
//! rather than reimplemented — a protobuf batch and the equivalent JSON batch cannot
//! disagree about anything except how they arrived, and a test asserts exactly that.
//!
//! Reusing the JSON structs costs one intermediate allocation per batch. That buys the
//! guarantee that the two encodings can never drift, which is worth more than the copy:
//! a second conversion path is a second place for a limit to be forgotten.
//!
//! # Why not `prost`
//!
//! The same reason [`crate::protobuf`] gives for `remote_write`, and it has not changed:
//! `prost` wants a `protoc` at build time or a vendored one plus a code-generation step,
//! on a project whose product constraint is one static binary cross-compiled to four
//! targets. Committing pre-generated code would sidestep the build issue and replace it
//! with ten thousand lines nobody reviews.
//!
//! What made that trade affordable is that the *supported surface* is small. telemetryd
//! stores gauges, sums and histograms — not summaries, not exponential histograms — so
//! this decodes the messages that reach storage and skips the rest as unknown fields,
//! which is what the wire format asks an implementation to do anyway.
//!
//! # Field numbers
//!
//! From `opentelemetry-proto` v1.x. They are part of the wire contract and cannot change
//! without a new major, which is what makes writing them down here safe.

use telemetryd_core::{Error, Result};

use crate::logs::{LogRecordJson, LogsData, ResourceLogs, ScopeLogs};
use crate::otlp::{
    AnyValue, ArrayValue, FlexEnum, FlexU64, InstrumentationScope, KeyValue, KvList, Resource,
};
use crate::otlp_metrics::{
    HistogramData, HistogramPoint, MetricJson, MetricsData, NumberData, NumberPoint,
    ResourceMetrics, ScopeMetrics, SumData,
};
use crate::protobuf::{Reader, WireType};
use crate::traces::{ResourceSpans, ScopeSpans, SpanEventJson, SpanJson, StatusJson, TracesData};

/// Guard against a hostile nesting depth in `AnyValue`, which is recursive through
/// `array_value` and `kvlist_value`.
///
/// A few hundred bytes of crafted body can otherwise describe a structure thousands of
/// levels deep and take the stack down with it — and this decoder runs before any limit
/// the configuration knows about. Attribute values in practice nest once or twice.
const MAX_VALUE_DEPTH: u32 = 16;

/// Elements one request may materialise in any single repeated field.
///
/// `server.max_body_bytes` was assumed to bound the memory a request costs, and it does
/// not: it bounds the *body*. An empty protobuf message is two bytes, so a 16 MiB body
/// holds eight million of them — measured at 801 MB of resident memory, from one request
/// that answered `200`. The equivalent JSON is sixteen bytes per container and reached
/// 111 MB, so this is a pre-existing shape that the denser encoding made an order of
/// magnitude worse.
///
/// A hundred thousand is far above any real batch — `laravel-telemetry` sends hundreds —
/// and far below the eight million it takes to hurt. Past it the request is refused
/// rather than truncated: a silently shortened batch is the failure this project rejects
/// everywhere else.
const MAX_REPEATED: usize = 100_000;

/// Refuse a repeated field that has grown past what any real producer sends.
fn bounded(len: usize, field: &str) -> Result<()> {
    if len >= MAX_REPEATED {
        return Err(Error::BadRequest(format!(
            "{field} exceeds {MAX_REPEATED} elements in one request; split the batch"
        )));
    }
    Ok(())
}

/// Hex, lowercase. Trace and span ids are raw bytes on the wire and hex strings in JSON;
/// the conversion path downstream expects the JSON spelling.
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Base64 with padding, matching what OTLP/JSON puts in `bytesValue`.
///
/// The JSON decoder passes that field through as text rather than decoding it, so the
/// protobuf path has to produce the same text for the two encodings to agree.
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..=chunk.len() {
            out.push(ALPHABET[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
        }
        for _ in chunk.len()..3 {
            out.push('=');
        }
    }
    out
}

// ---------------------------------------------------------------------------
// common.proto
// ---------------------------------------------------------------------------

fn any_value(reader: &mut Reader<'_>, depth: u32) -> Result<AnyValue> {
    if depth > MAX_VALUE_DEPTH {
        return Err(Error::BadRequest(format!(
            "attribute value nested deeper than {MAX_VALUE_DEPTH} levels"
        )));
    }

    let mut value = AnyValue::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                value.string_value = Some(reader.string()?.to_owned());
            }
            (2, WireType::Varint) => value.bool_value = Some(reader.varint()? != 0),
            (3, WireType::Varint) => value.int_value = FlexU64::Number(reader.varint()?),
            (4, WireType::Fixed64) => value.double_value = Some(reader.double()?),
            (5, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                let mut values = Vec::new();
                while let Some((inner, inner_wire)) = nested.next_field()? {
                    if inner == 1 && inner_wire == WireType::LengthDelimited {
                        let mut item = nested.message()?;
                        values.push(any_value(&mut item, depth + 1)?);
                    } else {
                        nested.skip(inner_wire)?;
                    }
                }
                value.array_value = Some(ArrayValue { values });
            }
            (6, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                let mut values = Vec::new();
                while let Some((inner, inner_wire)) = nested.next_field()? {
                    if inner == 1 && inner_wire == WireType::LengthDelimited {
                        let mut item = nested.message()?;
                        values.push(key_value(&mut item, depth + 1)?);
                    } else {
                        nested.skip(inner_wire)?;
                    }
                }
                value.kvlist_value = Some(KvList { values });
            }
            (7, WireType::LengthDelimited) => {
                value.bytes_value = Some(base64(reader.bytes()?));
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(value)
}

fn key_value(reader: &mut Reader<'_>, depth: u32) -> Result<KeyValue> {
    let mut key = String::new();
    let mut value = None;
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => reader.string()?.clone_into(&mut key),
            (2, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                value = Some(any_value(&mut nested, depth + 1)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(KeyValue { key, value })
}

/// Read a `repeated KeyValue` field into an existing vector.
fn push_attribute(reader: &mut Reader<'_>, into: &mut Vec<KeyValue>) -> Result<()> {
    let mut nested = reader.message()?;
    into.push(key_value(&mut nested, 0)?);
    Ok(())
}

fn resource(reader: &mut Reader<'_>) -> Result<Resource> {
    let mut attributes = Vec::new();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => push_attribute(reader, &mut attributes)?,
            _ => reader.skip(wire)?,
        }
    }
    Ok(Resource { attributes })
}

fn scope(reader: &mut Reader<'_>) -> Result<InstrumentationScope> {
    let mut scope = InstrumentationScope::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => reader.string()?.clone_into(&mut scope.name),
            (2, WireType::LengthDelimited) => reader.string()?.clone_into(&mut scope.version),
            (3, WireType::LengthDelimited) => push_attribute(reader, &mut scope.attributes)?,
            _ => reader.skip(wire)?,
        }
    }
    Ok(scope)
}

// ---------------------------------------------------------------------------
// logs.proto
// ---------------------------------------------------------------------------

/// Decode an `ExportLogsServiceRequest` into the shape the JSON decoder produces.
pub fn logs(body: &[u8]) -> Result<LogsData> {
    let mut reader = Reader::new(body);
    let mut data = LogsData::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                bounded(data.resource_logs.len(), "resource_logs")?;
                data.resource_logs.push(resource_logs(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(data)
}

fn resource_logs(reader: &mut Reader<'_>) -> Result<ResourceLogs> {
    let mut out = ResourceLogs::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.resource = Some(resource(&mut nested)?);
            }
            (2, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                bounded(out.scope_logs.len(), "scope_logs")?;
                out.scope_logs.push(scope_logs(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn scope_logs(reader: &mut Reader<'_>) -> Result<ScopeLogs> {
    let mut out = ScopeLogs::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.scope = Some(scope(&mut nested)?);
            }
            (2, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                bounded(out.log_records.len(), "log_records")?;
                out.log_records.push(log_record(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn log_record(reader: &mut Reader<'_>) -> Result<LogRecordJson> {
    let mut out = LogRecordJson::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            // Timestamps are `fixed64` here and decimal strings in JSON. Both end up in
            // the same `FlexU64`, so `normalize_timestamp` sees one type either way.
            (1, WireType::Fixed64) => out.time_unix_nano = FlexU64::Number(reader.fixed64()?),
            (11, WireType::Fixed64) => {
                out.observed_time_unix_nano = FlexU64::Number(reader.fixed64()?);
            }
            (2, WireType::Varint) => {
                let raw = reader.varint()?;
                out.severity_number = FlexEnum::Number(i32::try_from(raw).unwrap_or(0));
            }
            (3, WireType::LengthDelimited) => reader.string()?.clone_into(&mut out.severity_text),
            (5, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.body = Some(any_value(&mut nested, 0)?);
            }
            (6, WireType::LengthDelimited) => push_attribute(reader, &mut out.attributes)?,
            (9, WireType::LengthDelimited) => out.trace_id = hex(reader.bytes()?),
            (10, WireType::LengthDelimited) => out.span_id = hex(reader.bytes()?),
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// trace.proto
// ---------------------------------------------------------------------------

/// Decode an `ExportTraceServiceRequest`.
pub fn traces(body: &[u8]) -> Result<TracesData> {
    let mut reader = Reader::new(body);
    let mut data = TracesData::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                bounded(data.resource_spans.len(), "resource_spans")?;
                data.resource_spans.push(resource_spans(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(data)
}

fn resource_spans(reader: &mut Reader<'_>) -> Result<ResourceSpans> {
    let mut out = ResourceSpans::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.resource = Some(resource(&mut nested)?);
            }
            (2, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                bounded(out.scope_spans.len(), "scope_spans")?;
                out.scope_spans.push(scope_spans(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn scope_spans(reader: &mut Reader<'_>) -> Result<ScopeSpans> {
    let mut out = ScopeSpans::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.scope = Some(scope(&mut nested)?);
            }
            (2, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                bounded(out.spans.len(), "spans")?;
                out.spans.push(span(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn span(reader: &mut Reader<'_>) -> Result<SpanJson> {
    let mut out = SpanJson::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => out.trace_id = hex(reader.bytes()?),
            (2, WireType::LengthDelimited) => out.span_id = hex(reader.bytes()?),
            (4, WireType::LengthDelimited) => out.parent_span_id = hex(reader.bytes()?),
            (5, WireType::LengthDelimited) => reader.string()?.clone_into(&mut out.name),
            (6, WireType::Varint) => {
                let raw = reader.varint()?;
                out.kind = FlexEnum::Number(i32::try_from(raw).unwrap_or(0));
            }
            (7, WireType::Fixed64) => {
                out.start_time_unix_nano = FlexU64::Number(reader.fixed64()?);
            }
            (8, WireType::Fixed64) => out.end_time_unix_nano = FlexU64::Number(reader.fixed64()?),
            (9, WireType::LengthDelimited) => push_attribute(reader, &mut out.attributes)?,
            (11, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.events.push(span_event(&mut nested)?);
            }
            (15, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.status = Some(status(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn span_event(reader: &mut Reader<'_>) -> Result<SpanEventJson> {
    let mut out = SpanEventJson::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::Fixed64) => out.time_unix_nano = FlexU64::Number(reader.fixed64()?),
            (2, WireType::LengthDelimited) => reader.string()?.clone_into(&mut out.name),
            (3, WireType::LengthDelimited) => push_attribute(reader, &mut out.attributes)?,
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn status(reader: &mut Reader<'_>) -> Result<StatusJson> {
    let mut out = StatusJson::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (2, WireType::LengthDelimited) => reader.string()?.clone_into(&mut out.message),
            (3, WireType::Varint) => {
                let raw = reader.varint()?;
                out.code = FlexEnum::Number(i32::try_from(raw).unwrap_or(0));
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// metrics.proto
// ---------------------------------------------------------------------------

/// Decode an `ExportMetricsServiceRequest`.
///
/// Gauge, sum and histogram only — the three telemetryd stores. A summary or an
/// exponential histogram is skipped here exactly as an unknown field would be, which
/// leaves the metric with no data points and produces the same outcome as the JSON path
/// gives for the same payload: nothing stored, and no claim that something was.
pub fn metrics(body: &[u8]) -> Result<MetricsData> {
    let mut reader = Reader::new(body);
    let mut data = MetricsData::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                bounded(data.resource_metrics.len(), "resource_metrics")?;
                data.resource_metrics.push(resource_metrics(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(data)
}

fn resource_metrics(reader: &mut Reader<'_>) -> Result<ResourceMetrics> {
    let mut out = ResourceMetrics::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.resource = Some(resource(&mut nested)?);
            }
            (2, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                bounded(out.scope_metrics.len(), "scope_metrics")?;
                out.scope_metrics.push(scope_metrics(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn scope_metrics(reader: &mut Reader<'_>) -> Result<ScopeMetrics> {
    let mut out = ScopeMetrics::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.scope = Some(scope(&mut nested)?);
            }
            (2, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                bounded(out.metrics.len(), "metrics")?;
                out.metrics.push(metric(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn metric(reader: &mut Reader<'_>) -> Result<MetricJson> {
    let mut out = MetricJson::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => reader.string()?.clone_into(&mut out.name),
            (3, WireType::LengthDelimited) => reader.string()?.clone_into(&mut out.unit),
            (5, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.gauge = Some(NumberData {
                    data_points: number_points(&mut nested)?,
                });
            }
            (7, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.sum = Some(sum(&mut nested)?);
            }
            (9, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.histogram = Some(HistogramData {
                    data_points: histogram_points(&mut nested)?,
                });
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn number_points(reader: &mut Reader<'_>) -> Result<Vec<NumberPoint>> {
    let mut points = Vec::new();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                points.push(number_point(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(points)
}

fn sum(reader: &mut Reader<'_>) -> Result<SumData> {
    let mut out = SumData::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                out.data_points.push(number_point(&mut nested)?);
            }
            (3, WireType::Varint) => out.is_monotonic = reader.varint()? != 0,
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn number_point(reader: &mut Reader<'_>) -> Result<NumberPoint> {
    let mut out = NumberPoint::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (3, WireType::Fixed64) => out.time_unix_nano = FlexU64::Number(reader.fixed64()?),
            (4, WireType::Fixed64) => out.as_double = Some(reader.double()?),
            // `as_int` is `sfixed64`: eight raw bytes, not a varint. Read as fixed64 and
            // let the shared conversion decide what the number means.
            (6, WireType::Fixed64) => out.as_int = FlexU64::Number(reader.fixed64()?),
            (7, WireType::LengthDelimited) => push_attribute(reader, &mut out.attributes)?,
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

fn histogram_points(reader: &mut Reader<'_>) -> Result<Vec<HistogramPoint>> {
    let mut points = Vec::new();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut nested = reader.message()?;
                points.push(histogram_point(&mut nested)?);
            }
            _ => reader.skip(wire)?,
        }
    }
    Ok(points)
}

fn histogram_point(reader: &mut Reader<'_>) -> Result<HistogramPoint> {
    let mut out = HistogramPoint::default();
    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            (3, WireType::Fixed64) => out.time_unix_nano = FlexU64::Number(reader.fixed64()?),
            (4, WireType::Fixed64) => out.count = FlexU64::Number(reader.fixed64()?),
            (5, WireType::Fixed64) => out.sum = Some(reader.double()?),
            // `bucket_counts` and `explicit_bounds` are packed repeated scalars: one
            // length-delimited field holding every element, not one field per element.
            // Reading them as single values would take the first and silently lose a
            // histogram's shape.
            (6, WireType::LengthDelimited) => {
                let mut packed = reader.message()?;
                while !packed.is_empty() {
                    out.bucket_counts.push(FlexU64::Number(packed.fixed64()?));
                }
            }
            (7, WireType::LengthDelimited) => {
                let mut packed = reader.message()?;
                while !packed.is_empty() {
                    out.explicit_bounds.push(packed.double()?);
                }
            }
            (9, WireType::LengthDelimited) => push_attribute(reader, &mut out.attributes)?,
            _ => reader.skip(wire)?,
        }
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn hex_matches_the_json_spelling_of_an_id() {
        assert_eq!(
            hex(&[0x4b, 0xf9, 0x2f, 0x35]),
            "4bf92f35",
            "ids are lowercase hex in OTLP/JSON"
        );
        assert_eq!(hex(&[0x00, 0x0f]), "000f", "a leading zero must be kept");
        assert_eq!(hex(&[]), "");
    }

    #[test]
    fn base64_matches_what_otlp_json_puts_in_bytes_value() {
        // Padded and standard-alphabet, unlike the URL-safe unpadded form `init` uses
        // for tokens. The JSON decoder passes `bytesValue` through as text, so the two
        // encodings only agree if this produces the same string.
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foob"), "Zm9vYg==");
        assert_eq!(base64(&[0xff, 0xef]), "/+8=");
        assert_eq!(base64(b""), "");
    }

    #[test]
    fn a_wide_batch_is_refused_before_it_allocates() {
        // Depth had a guard; width did not. An empty protobuf message is two bytes, so a
        // body inside `server.max_body_bytes` holds eight million of them — measured at
        // 801 MB resident, from one request that answered `200`. The body limit bounds
        // the body, and that was being read as bounding the memory.
        let hostile: Vec<u8> = [0x0a, 0x00].repeat(MAX_REPEATED + 1);
        let error = logs(&hostile).unwrap_err();
        assert!(error.to_string().contains("resource_logs"), "{error}");
        assert!(error.to_string().contains("split the batch"), "{error}");

        // Refused, not truncated: a silently shortened batch is the failure mode this
        // project rejects everywhere else.
        let fine: Vec<u8> = [0x0a, 0x00].repeat(16);
        assert_eq!(logs(&fine).unwrap().resource_logs.len(), 16);
    }

    #[test]
    fn a_deeply_nested_value_is_refused_rather_than_taking_the_stack_down() {
        // `AnyValue` recurses through `array_value`, so a small crafted body can
        // describe an arbitrarily deep structure. This runs before any configured limit.
        let mut body = Vec::new();
        for _ in 0..(MAX_VALUE_DEPTH + 4) {
            // field 5 (array_value), wire type 2; then field 1 (values), wire type 2.
            let mut wrapped = vec![0x2a, u8::try_from(body.len() + 2).unwrap(), 0x0a];
            wrapped.push(u8::try_from(body.len()).unwrap());
            wrapped.extend_from_slice(&body);
            body = wrapped;
        }
        let mut reader = Reader::new(&body);
        let error = any_value(&mut reader, 0).unwrap_err();
        assert!(error.to_string().contains("nested deeper"), "{error}");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod equivalence {
    //! The property that makes this file safe to have: for the same logical payload, the
    //! protobuf and JSON paths must produce the same records. Not similar — equal.
    //!
    //! These build protobuf by hand rather than through an encoder, because an encoder
    //! written alongside the decoder would agree with its own mistakes.

    use telemetryd_core::config::{IngestConfig, LimitsConfig};

    use super::*;
    use crate::logs::DecodeContext;

    /// A protobuf tag is `(field << 3) | wire_type`, encoded as a varint — not a byte.
    /// The first version of these helpers assumed one byte, which works up to field 15
    /// and then silently produces a different field number.
    fn tag(field: u32, wire: u32) -> Vec<u8> {
        raw_varint(u64::from((field << 3) | wire))
    }

    fn raw_varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = u8::try_from(value & 0x7f).unwrap();
            value >>= 7;
            if value > 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                return out;
            }
        }
    }

    /// Length-delimited field: tag, length, payload.
    fn delim(field: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = tag(field, 2);
        out.extend(raw_varint(payload.len() as u64));
        out.extend_from_slice(payload);
        out
    }

    fn varint(field: u32, value: u64) -> Vec<u8> {
        let mut out = tag(field, 0);
        out.extend(raw_varint(value));
        out
    }

    fn fixed64(field: u32, value: u64) -> Vec<u8> {
        let mut out = tag(field, 1);
        out.extend_from_slice(&value.to_le_bytes());
        out
    }

    fn double(field: u32, value: f64) -> Vec<u8> {
        let mut out = tag(field, 1);
        out.extend_from_slice(&value.to_le_bytes());
        out
    }

    fn string_attr(key: &str, value: &str) -> Vec<u8> {
        let inner = delim(1, value.as_bytes());
        let mut kv = delim(1, key.as_bytes());
        kv.extend(delim(2, &inner));
        kv
    }

    fn context() -> (LimitsConfig, IngestConfig) {
        (LimitsConfig::default(), IngestConfig::default())
    }

    #[test]
    fn a_log_batch_decodes_identically_from_both_encodings() {
        let timestamp = 1_700_000_000_000_000_000_u64;

        // --- protobuf ---
        let mut record = fixed64(1, timestamp);
        record.extend(varint(2, 17)); // severityNumber = ERROR
        record.extend(delim(3, b"ERROR"));
        record.extend(delim(5, &delim(1, b"checkout failed"))); // body.stringValue
        record.extend(delim(6, &string_attr("order.id", "A-99")));
        record.extend(delim(9, &[0x4b, 0xf9, 0x2f, 0x35])); // traceId bytes
        record.extend(delim(10, &[0x00, 0xf0, 0x67, 0xaa]));

        // ScopeLogs wraps the records; ResourceLogs wraps the scopes. Skipping a level
        // here produced an empty batch and no rejection, which is what an unknown field
        // correctly looks like.
        let scope_logs = delim(2, &record);
        let mut resource_body = delim(1, &string_attr("service.name", "checkout"));
        resource_body.extend(delim(1, &string_attr("k8s.pod.name", "pod-7f")));
        let mut resource_logs = delim(1, &resource_body);
        resource_logs.extend(delim(2, &scope_logs));
        let wire = delim(1, &resource_logs);

        // --- the same thing as JSON ---
        let json = format!(
            r#"{{"resourceLogs":[{{"resource":{{"attributes":[
                 {{"key":"service.name","value":{{"stringValue":"checkout"}}}},
                 {{"key":"k8s.pod.name","value":{{"stringValue":"pod-7f"}}}}]}},
               "scopeLogs":[{{"logRecords":[
                 {{"timeUnixNano":"{timestamp}","severityNumber":17,"severityText":"ERROR",
                   "body":{{"stringValue":"checkout failed"}},
                   "attributes":[{{"key":"order.id","value":{{"stringValue":"A-99"}}}}],
                   "traceId":"4bf92f35","spanId":"00f067aa"}}]}}]}}]}}"#
        );

        let (limits, ingest) = context();
        let ctx = DecodeContext {
            limits: &limits,
            ingest: &ingest,
            now_nanos: timestamp,
        };

        let from_proto = crate::logs::convert_data(&logs(&wire).unwrap(), ctx);
        let from_json = crate::logs::decode(json.as_bytes(), ctx).unwrap();

        assert_eq!(from_proto.records.len(), 1, "{:?}", from_proto.rejections);
        assert_eq!(from_json.records.len(), 1);

        let (p, j) = (&from_proto.records[0], &from_json.records[0]);
        assert_eq!(p.timestamp_nanos, j.timestamp_nanos);
        assert_eq!(p.severity, j.severity);
        assert_eq!(p.severity_text, j.severity_text);
        assert_eq!(p.body, j.body);
        assert_eq!(
            p.trace_id, j.trace_id,
            "ids are bytes on the wire, hex in JSON"
        );
        assert_eq!(p.span_id, j.span_id);
        assert_eq!(format!("{:?}", p.stream), format!("{:?}", j.stream));
        assert_eq!(
            format!("{:?}", p.attributes),
            format!("{:?}", j.attributes),
            "the resource attribute no label claimed must survive both paths alike"
        );
    }

    #[test]
    fn a_histogram_keeps_its_shape_through_the_packed_fields() {
        // `bucket_counts` and `explicit_bounds` are packed repeated scalars. Reading them
        // as single values takes the first element and silently flattens the histogram,
        // which still produces a plausible-looking metric.
        let timestamp = 1_700_000_000_000_000_000_u64;

        let mut point = fixed64(3, timestamp);
        point.extend(fixed64(4, 7)); // count
        point.extend(double(5, 12.5));
        let counts: Vec<u8> = [1_u64, 2, 4].iter().flat_map(|v| v.to_le_bytes()).collect();
        point.extend(delim(6, &counts));
        let bounds: Vec<u8> = [0.5_f64, 1.5]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        point.extend(delim(7, &bounds));

        let histogram = delim(1, &point);
        let mut metric = delim(1, b"request_duration");
        metric.extend(delim(9, &histogram));
        let scope_metrics = delim(2, &metric);
        let mut resource_metrics = delim(1, &delim(1, &string_attr("service.name", "api")));
        resource_metrics.extend(delim(2, &scope_metrics));
        let wire = delim(1, &resource_metrics);

        let data = metrics(&wire).unwrap();
        let point = &data.resource_metrics[0].scope_metrics[0].metrics[0]
            .histogram
            .as_ref()
            .unwrap()
            .data_points[0];

        assert_eq!(point.count.get(), Some(7));
        assert_eq!(point.sum, Some(12.5));
        assert_eq!(
            point
                .bucket_counts
                .iter()
                .map(|v| v.get())
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(4)],
            "packed bucket counts were flattened"
        );
        assert_eq!(point.explicit_bounds, vec![0.5, 1.5]);
    }

    #[test]
    fn an_unknown_field_is_skipped_rather_than_failing_the_batch() {
        // A newer OTLP producer sends fields this build has never heard of. The wire
        // format says skip them; refusing would make every SDK upgrade a breaking change.
        let timestamp = 1_700_000_000_000_000_000_u64;
        let mut record = fixed64(1, timestamp);
        record.extend(delim(5, &delim(1, b"hello")));
        record.extend(varint(999, 42)); // from the future
        record.extend(delim(998, b"also from the future"));

        let scope_logs = delim(2, &record);
        let mut resource_logs = delim(1, &delim(1, &string_attr("service.name", "app")));
        resource_logs.extend(delim(2, &scope_logs));
        let wire = delim(1, &resource_logs);

        let (limits, ingest) = context();
        let decoded = crate::logs::convert_data(
            &logs(&wire).unwrap(),
            DecodeContext {
                limits: &limits,
                ingest: &ingest,
                now_nanos: timestamp,
            },
        );
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].body, "hello");
    }
}
