//! Records back out as OTLP/HTTP JSON.
//!
//! The inverse of the decoders next door, and the reason relay mode and bulk export
//! can send anywhere without either side learning the other's storage format. OTLP is
//! the one shape every backend already accepts, including telemetryd itself — which
//! makes `encode` followed by our own `/v1/logs` a complete round trip, and makes the
//! round trip the test that keeps this file honest.
//!
//! # Sanitised names are mapped back
//!
//! Ingest promotes `service.name` into the stream as `service_name`, because a label
//! name with a dot in it is awkward in every query language we serve. Emitting that
//! back verbatim would hand a foreign receiver a resource attribute it does not
//! recognise, and quietly lose service identity. The promotions are a known, fixed
//! set, so they are mapped back to their dotted spelling here.
//!
//! Anything else is emitted as it is stored. Reversing underscores in general is not
//! possible — a producer's own `order_id` is not `order.id` — and guessing would
//! corrupt attribute names to make a handful look tidier.

use serde_json::{Map, Value, json};
use telemetryd_core::metric::{METRIC_NAME_LABEL, MetricKind, MetricSample};
use telemetryd_core::record::{LogRecord, Severity};
use telemetryd_core::span::{SpanKind, SpanRecord, SpanStatus};
use telemetryd_core::{Labels, span::SpanEvent};

/// Stream labels that ingest produced by sanitising a dotted OTLP convention.
///
/// Kept in step with `ingest.stream_labels`' defaults. A name not listed here is
/// emitted unchanged, which is the safe direction: an unrecognised attribute is worse
/// than an unfamiliar one.
const DOTTED: &[(&str, &str)] = &[
    ("service_name", "service.name"),
    ("service_namespace", "service.namespace"),
    ("service_version", "service.version"),
    ("deployment_environment", "deployment.environment"),
    ("deployment_environment_name", "deployment.environment.name"),
];

fn dotted(name: &str) -> &str {
    DOTTED
        .iter()
        .find(|(stored, _)| *stored == name)
        .map_or(name, |(_, otlp)| *otlp)
}

/// OTLP JSON renders 64-bit integers as strings: a JSON number cannot hold a
/// nanosecond timestamp without losing precision in any JavaScript reader.
fn nanos(value: u64) -> Value {
    Value::String(value.to_string())
}

fn attributes(labels: &Labels, map_names: bool) -> Value {
    Value::Array(
        labels
            .iter()
            .map(|(key, value)| {
                let key = if map_names { dotted(key) } else { key };
                json!({"key": key, "value": {"stringValue": value}})
            })
            .collect(),
    )
}

/// Group by stream identity, preserving first-seen order.
///
/// The stream labels *are* the resource in OTLP terms, so one group becomes one
/// `resource`. Grouping matters beyond tidiness: a receiver applies resource
/// attributes to every record beneath them, so emitting one resource per record would
/// be correct but many times larger on the wire.
fn group<'a, T, F>(records: &'a [T], stream_of: F) -> Vec<(&'a Labels, Vec<&'a T>)>
where
    F: Fn(&'a T) -> &'a Labels,
{
    let mut groups: Vec<(&'a Labels, Vec<&'a T>)> = Vec::new();
    for record in records {
        let stream = stream_of(record);
        if let Some(entry) = groups.iter_mut().find(|(labels, _)| *labels == stream) {
            entry.1.push(record);
        } else {
            groups.push((stream, vec![record]));
        }
    }
    groups
}

/// The representative `severityNumber` for a level.
///
/// The mapping is lossy in one direction by design: OTLP has four numbers per level
/// (`INFO`, `INFO2`..) and we store the level. The lowest of each range round trips
/// through `Severity::from_otlp_number` to the same level, which is the property that
/// matters.
fn severity_number(severity: Severity) -> i32 {
    match severity {
        Severity::Trace => 1,
        Severity::Debug => 5,
        Severity::Info => 9,
        Severity::Warn => 13,
        Severity::Error => 17,
        Severity::Fatal => 21,
        Severity::Unknown => 0,
    }
}

fn span_kind_number(kind: SpanKind) -> i32 {
    match kind {
        SpanKind::Unspecified => 0,
        SpanKind::Internal => 1,
        SpanKind::Server => 2,
        SpanKind::Client => 3,
        SpanKind::Producer => 4,
        SpanKind::Consumer => 5,
    }
}

fn span_status_number(status: SpanStatus) -> i32 {
    match status {
        SpanStatus::Unset => 0,
        SpanStatus::Ok => 1,
        SpanStatus::Error => 2,
    }
}

/// `{"resourceLogs":[…]}`, ready to POST at `/v1/logs`.
#[must_use]
pub fn encode_logs(records: &[LogRecord]) -> Value {
    let resources: Vec<Value> = group(records, |record| &record.stream)
        .into_iter()
        .map(|(stream, records)| {
            let entries: Vec<Value> = records
                .iter()
                .map(|record| {
                    let mut entry = Map::new();
                    entry.insert("timeUnixNano".into(), nanos(record.timestamp_nanos));
                    entry.insert("observedTimeUnixNano".into(), nanos(record.timestamp_nanos));
                    entry.insert(
                        "severityNumber".into(),
                        json!(severity_number(record.severity)),
                    );
                    if !record.severity_text.is_empty() {
                        entry.insert("severityText".into(), json!(record.severity_text));
                    }
                    entry.insert("body".into(), json!({"stringValue": record.body}));
                    entry.insert("attributes".into(), attributes(&record.attributes, false));
                    if let Some(trace) = &record.trace_id {
                        entry.insert("traceId".into(), json!(trace));
                    }
                    if let Some(span) = &record.span_id {
                        entry.insert("spanId".into(), json!(span));
                    }
                    Value::Object(entry)
                })
                .collect();

            json!({
                "resource": {"attributes": attributes(stream, true)},
                "scopeLogs": [{"logRecords": entries}],
            })
        })
        .collect();

    json!({ "resourceLogs": resources })
}

fn encode_events(events: &[SpanEvent]) -> Value {
    Value::Array(
        events
            .iter()
            .map(|event| {
                json!({
                    "timeUnixNano": nanos(event.time_nanos),
                    "name": event.name,
                    "attributes": attributes(&event.attributes, false),
                })
            })
            .collect(),
    )
}

/// `{"resourceSpans":[…]}`, ready to POST at `/v1/traces`.
#[must_use]
pub fn encode_spans(records: &[SpanRecord]) -> Value {
    let resources: Vec<Value> = group(records, |record| &record.stream)
        .into_iter()
        .map(|(stream, records)| {
            let spans: Vec<Value> = records
                .iter()
                .map(|record| {
                    let mut span = Map::new();
                    span.insert("traceId".into(), json!(record.trace_id));
                    span.insert("spanId".into(), json!(record.span_id));
                    if let Some(parent) = &record.parent_span_id {
                        span.insert("parentSpanId".into(), json!(parent));
                    }
                    span.insert("name".into(), json!(record.name));
                    span.insert("kind".into(), json!(span_kind_number(record.kind)));
                    span.insert("startTimeUnixNano".into(), nanos(record.start_nanos));
                    span.insert("endTimeUnixNano".into(), nanos(record.end_nanos));
                    span.insert("attributes".into(), attributes(&record.attributes, false));
                    // Only when set: an empty status object round trips as `Unset`
                    // anyway, and TraceQL's `status = error` must not match a span
                    // nobody gave a status.
                    if record.status != SpanStatus::Unset || !record.status_message.is_empty() {
                        let mut status = Map::new();
                        status.insert("code".into(), json!(span_status_number(record.status)));
                        if !record.status_message.is_empty() {
                            status.insert("message".into(), json!(record.status_message));
                        }
                        span.insert("status".into(), Value::Object(status));
                    }
                    if !record.events.is_empty() {
                        span.insert("events".into(), encode_events(&record.events));
                    }
                    Value::Object(span)
                })
                .collect();

            json!({
                "resource": {"attributes": attributes(stream, true)},
                "scopeSpans": [{"spans": spans}],
            })
        })
        .collect();

    json!({ "resourceSpans": resources })
}

/// The series labels minus `__name__`, which becomes the metric's name rather than one
/// of its data-point attributes.
fn series_attributes(series: &Labels) -> Value {
    Value::Array(
        series
            .iter()
            .filter(|(key, _)| *key != METRIC_NAME_LABEL)
            .map(|(key, value)| json!({"key": dotted(key), "value": {"stringValue": value}}))
            .collect(),
    )
}

/// `{"resourceMetrics":[…]}`, ready to POST at `/v1/metrics`.
///
/// Every sample becomes its own metric entry with a single data point. Merging points
/// into one metric per series would be smaller on the wire, but it means holding a
/// whole segment's samples grouped in memory to do it — and the shipper's whole design
/// is to stream a segment rather than assemble one.
///
/// **Counters go out as non-monotonic sums with no aggregation temporality.** We do not
/// store either fact: `remote_write` carries no type at all, so the kind is often a
/// guess already. Claiming `AGGREGATION_TEMPORALITY_CUMULATIVE` on a sample whose
/// provenance we do not know would be inventing a property, and a receiver that acts on
/// it computes wrong rates.
#[must_use]
pub fn encode_metrics(samples: &[MetricSample]) -> Value {
    let resources: Vec<Value> = group(samples, |sample| &sample.series)
        .into_iter()
        .map(|(series, samples)| {
            let metrics: Vec<Value> = samples
                .iter()
                .map(|sample| {
                    let point = json!({
                        "timeUnixNano": nanos(sample.timestamp_nanos),
                        "asDouble": sample.value,
                        "attributes": series_attributes(series),
                    });
                    let body = match sample.kind {
                        MetricKind::Counter => json!({"sum": {
                            "dataPoints": [point],
                            "isMonotonic": true,
                            "aggregationTemporality": 0,
                        }}),
                        _ => json!({"gauge": {"dataPoints": [point]}}),
                    };
                    let mut metric = Map::new();
                    metric.insert("name".into(), json!(sample.name()));
                    if let Value::Object(body) = body {
                        for (key, value) in body {
                            metric.insert(key, value);
                        }
                    }
                    Value::Object(metric)
                })
                .collect();

            json!({
                "resource": {"attributes": attributes(&Labels::default(), true)},
                "scopeMetrics": [{"metrics": metrics}],
            })
        })
        .collect();

    json!({ "resourceMetrics": resources })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::default();
        for (key, value) in pairs {
            labels.insert((*key).to_owned(), (*value).to_owned());
        }
        labels
    }

    /// Shaped like something the decoder produced, not like something convenient.
    ///
    /// `level` is in the stream because ingest always derives it from the severity —
    /// leaving it out of a fixture makes a round-trip test fail for a reason that has
    /// nothing to do with the encoder.
    fn log(app: &str, body: &str) -> LogRecord {
        LogRecord {
            timestamp_nanos: 1_760_000_000_000_000_000,
            stream: labels(&[("app", app), ("level", "error"), ("service_name", app)]),
            severity: Severity::Error,
            severity_text: "ERROR".into(),
            body: body.into(),
            attributes: labels(&[("order.id", "42")]),
            trace_id: Some("5b8efff798038103d269b633813fc60c".into()),
            span_id: Some("eee19b7ec3c1b174".into()),
        }
    }

    #[test]
    fn a_sanitised_resource_name_goes_back_out_dotted() {
        // `service_name` is our storage spelling; a foreign receiver knows
        // `service.name`, and would silently lose service identity otherwise.
        let encoded = encode_logs(&[log("checkout", "hello")]);
        let attrs = &encoded["resourceLogs"][0]["resource"]["attributes"];
        let names: Vec<&str> = attrs
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["key"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"service.name"), "{names:?}");
        assert!(!names.contains(&"service_name"), "{names:?}");
    }

    #[test]
    fn a_producers_own_underscore_is_left_alone() {
        // Only the known promotions are mapped. Reversing underscores in general
        // would rename a producer's `order_id` to `order.id`, which is corruption
        // dressed up as tidying.
        let mut record = log("checkout", "hello");
        record.stream = labels(&[("app", "checkout"), ("order_id", "42")]);
        let encoded = encode_logs(&[record]);
        let attrs = encoded["resourceLogs"][0]["resource"]["attributes"].clone();
        let names: Vec<&str> = attrs
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["key"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"order_id"), "{names:?}");
    }

    #[test]
    fn records_sharing_a_stream_share_one_resource() {
        let encoded = encode_logs(&[log("checkout", "a"), log("checkout", "b"), log("api", "c")]);
        let resources = encoded["resourceLogs"].as_array().unwrap();
        assert_eq!(resources.len(), 2, "one resource per distinct stream");
        assert_eq!(
            resources[0]["scopeLogs"][0]["logRecords"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn timestamps_are_strings_because_they_do_not_fit_a_json_number() {
        let encoded = encode_logs(&[log("checkout", "hello")]);
        let at = &encoded["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["timeUnixNano"];
        assert_eq!(at.as_str(), Some("1760000000000000000"));
    }

    #[test]
    fn a_severity_number_survives_the_round_trip_to_the_same_level() {
        // The mapping is many-to-one going in, so the test is that it lands on the
        // same level coming back — not that the number is preserved.
        for severity in [
            Severity::Trace,
            Severity::Debug,
            Severity::Info,
            Severity::Warn,
            Severity::Error,
            Severity::Fatal,
        ] {
            assert_eq!(
                Severity::from_otlp_number(severity_number(severity)),
                severity,
                "{severity:?} did not survive"
            );
        }
    }

    #[test]
    fn an_unset_span_status_is_omitted_rather_than_asserted() {
        // TraceQL's `status = error` must not match a span nobody gave a status, and
        // emitting `code: 0` invites a receiver to record one.
        let span = SpanRecord {
            trace_id: "5b8efff798038103d269b633813fc60c".into(),
            span_id: "eee19b7ec3c1b174".into(),
            parent_span_id: None,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_nanos: 1,
            end_nanos: 2,
            status: SpanStatus::Unset,
            status_message: String::new(),
            stream: labels(&[("app", "checkout")]),
            attributes: Labels::default(),
            events: Vec::new(),
        };
        let encoded = encode_spans(&[span]);
        let out = &encoded["resourceSpans"][0]["scopeSpans"][0]["spans"][0];
        assert!(out.get("status").is_none(), "{out}");
        assert!(
            out.get("parentSpanId").is_none(),
            "a root span has no parent"
        );
    }

    #[test]
    fn a_metric_name_is_a_name_and_not_an_attribute() {
        let sample = MetricSample {
            timestamp_nanos: 1_760_000_000_000_000_000,
            series: labels(&[
                (METRIC_NAME_LABEL, "http_requests_total"),
                ("app", "checkout"),
            ]),
            value: 42.0,
            kind: MetricKind::Counter,
        };
        let encoded = encode_metrics(&[sample]);
        let metric = &encoded["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0];
        assert_eq!(metric["name"].as_str(), Some("http_requests_total"));
        assert!(metric.get("sum").is_some(), "a counter is a sum");

        let attrs = metric["sum"]["dataPoints"][0]["attributes"]
            .as_array()
            .unwrap();
        let names: Vec<&str> = attrs.iter().map(|a| a["key"].as_str().unwrap()).collect();
        assert!(!names.iter().any(|n| n.starts_with("__")), "{names:?}");
    }

    #[test]
    fn nothing_in_produces_an_empty_envelope_rather_than_null() {
        // A receiver should get a well-formed request it can accept and do nothing
        // with, not something it has to special-case.
        assert_eq!(encode_logs(&[]), json!({"resourceLogs": []}));
        assert_eq!(encode_spans(&[]), json!({"resourceSpans": []}));
        assert_eq!(encode_metrics(&[]), json!({"resourceMetrics": []}));
    }

    /// The test the whole file rests on.
    ///
    /// Structurally plausible JSON is easy; JSON our own decoder turns back into the
    /// same record is the actual claim, and it is what makes relay shipping and export
    /// trustworthy. Anything the encoder invents or drops shows up here as a mismatch
    /// rather than as a support question about missing fields.
    #[test]
    fn a_log_record_survives_being_encoded_and_decoded() {
        let original = log("checkout", "payment declined");
        let body = serde_json::to_vec(&encode_logs(std::slice::from_ref(&original))).unwrap();

        let limits = telemetryd_core::config::LimitsConfig::default();
        let ingest = telemetryd_core::config::IngestConfig::default();
        let decoded = crate::logs::decode(
            &body,
            crate::logs::DecodeContext {
                limits: &limits,
                ingest: &ingest,
                now_nanos: 0,
            },
        )
        .unwrap();

        assert_eq!(decoded.rejections.len(), 0, "{:?}", decoded.rejections);
        assert_eq!(decoded.records.len(), 1);
        let back = &decoded.records[0];

        assert_eq!(back.timestamp_nanos, original.timestamp_nanos);
        assert_eq!(back.body, original.body);
        assert_eq!(back.severity, original.severity);
        assert_eq!(back.severity_text, original.severity_text);
        assert_eq!(back.trace_id, original.trace_id);
        assert_eq!(back.span_id, original.span_id);
        assert_eq!(back.attributes, original.attributes);
        // The stream must come back identical, which is what proves the dotted
        // mapping is a true inverse of ingest's sanitising rather than a guess.
        assert_eq!(back.stream, original.stream);
    }

    #[test]
    fn a_span_survives_being_encoded_and_decoded() {
        let original = SpanRecord {
            trace_id: "5b8efff798038103d269b633813fc60c".into(),
            span_id: "eee19b7ec3c1b174".into(),
            parent_span_id: Some("aaa19b7ec3c1b174".into()),
            name: "POST /charge".into(),
            kind: SpanKind::Server,
            start_nanos: 1_760_000_000_000_000_000,
            end_nanos: 1_760_000_000_100_000_000,
            status: SpanStatus::Error,
            status_message: "card declined".into(),
            stream: labels(&[("app", "checkout"), ("service_name", "checkout")]),
            attributes: labels(&[("http.method", "POST")]),
            events: vec![SpanEvent {
                time_nanos: 1_760_000_000_050_000_000,
                name: "retry".into(),
                attributes: labels(&[("attempt", "2")]),
            }],
        };
        let body = serde_json::to_vec(&encode_spans(std::slice::from_ref(&original))).unwrap();

        let limits = telemetryd_core::config::LimitsConfig::default();
        let ingest = telemetryd_core::config::IngestConfig::default();
        let decoded = crate::traces::decode(
            &body,
            crate::logs::DecodeContext {
                limits: &limits,
                ingest: &ingest,
                now_nanos: 0,
            },
        )
        .unwrap();

        assert_eq!(decoded.rejections.len(), 0, "{:?}", decoded.rejections);
        assert_eq!(decoded.records, vec![original]);
    }
}
