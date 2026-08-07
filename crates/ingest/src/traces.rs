//! OTLP/HTTP JSON traces decoding.
//!
//! Same shape as the logs decoder, sharing the wire-type quirks handled in
//! [`crate::otlp`]. What differs is what a malformed span means: a log line with no
//! body is still a log line, but a span with no trace id or span id cannot be joined
//! to anything, so it is rejected rather than stored as an orphan.

use serde::Deserialize;
use telemetryd_core::Labels;
use telemetryd_core::config::{IngestConfig, LimitsConfig};
use telemetryd_core::record::{APP_LABEL, UNKNOWN_APP, sanitize_label_name};
use telemetryd_core::span::{SpanEvent, SpanKind, SpanRecord, SpanStatus};

use crate::logs::{DecodeContext, normalize_timestamp};
use crate::otlp::{
    FlexEnum, FlexU64, InstrumentationScope, KeyValue, Resource, extend_attributes, extend_labels,
    normalize_id,
};
use crate::{Decoded, RejectReason, Rejection};

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TracesData {
    #[serde(alias = "resource_spans")]
    pub resource_spans: Vec<ResourceSpans>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ResourceSpans {
    pub resource: Option<Resource>,
    #[serde(alias = "scope_spans")]
    pub scope_spans: Vec<ScopeSpans>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ScopeSpans {
    pub scope: Option<InstrumentationScope>,
    pub spans: Vec<SpanJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SpanJson {
    #[serde(alias = "trace_id")]
    pub trace_id: String,
    #[serde(alias = "span_id")]
    pub span_id: String,
    #[serde(alias = "parent_span_id")]
    pub parent_span_id: String,
    pub name: String,
    pub kind: FlexEnum,
    #[serde(alias = "start_time_unix_nano")]
    pub start_time_unix_nano: FlexU64,
    #[serde(alias = "end_time_unix_nano")]
    pub end_time_unix_nano: FlexU64,
    pub attributes: Vec<KeyValue>,
    pub events: Vec<SpanEventJson>,
    pub status: Option<StatusJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SpanEventJson {
    #[serde(alias = "time_unix_nano")]
    pub time_unix_nano: FlexU64,
    pub name: String,
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct StatusJson {
    pub code: FlexEnum,
    pub message: String,
}

/// Decode an `ExportTraceServiceRequest`.
pub fn decode(
    body: &[u8],
    ctx: DecodeContext<'_>,
) -> Result<Decoded<SpanRecord>, serde_json::Error> {
    let data: TracesData = serde_json::from_slice(body)?;
    let mut decoded = Decoded::default();

    for resource_spans in &data.resource_spans {
        let mut resource_labels = Labels::new();
        if let Some(resource) = &resource_spans.resource {
            extend_labels(&mut resource_labels, &resource.attributes);
        }

        for scope_spans in &resource_spans.scope_spans {
            let mut scope_labels = resource_labels.clone();
            if let Some(scope) = &scope_spans.scope {
                extend_labels(&mut scope_labels, &scope.attributes);
                if !scope.name.is_empty() {
                    scope_labels.insert("scope_name", scope.name.clone());
                }
            }

            for span in &scope_spans.spans {
                match convert(span, &scope_labels, ctx, &mut decoded) {
                    Ok(record) => decoded.records.push(record),
                    Err(rejection) => decoded.rejections.push(rejection),
                }
            }
        }
    }

    Ok(decoded)
}

fn convert(
    raw: &SpanJson,
    inherited: &Labels,
    ctx: DecodeContext<'_>,
    decoded: &mut Decoded<SpanRecord>,
) -> Result<SpanRecord, Rejection> {
    // A span with no ids cannot be joined to a trace or to a parent, so storing it
    // would produce a row nothing can ever retrieve.
    let trace_id = normalize_id(&raw.trace_id).ok_or_else(|| {
        Rejection::new(
            RejectReason::MissingTraceId,
            format!("span {:?} has no usable traceId", raw.name),
        )
    })?;
    let span_id = normalize_id(&raw.span_id).ok_or_else(|| {
        Rejection::new(
            RejectReason::MissingSpanId,
            format!("span {:?} has no usable spanId", raw.name),
        )
    })?;

    let (start_nanos, start_unit) = raw
        .start_time_unix_nano
        .get()
        .filter(|v| *v > 0)
        .and_then(normalize_timestamp)
        .unwrap_or((ctx.now_nanos, crate::logs::TimeUnit::Nanos));
    if start_unit != crate::logs::TimeUnit::Nanos {
        decoded.rescaled_timestamps += 1;
    }

    let end_nanos = raw
        .end_time_unix_nano
        .get()
        .filter(|v| *v > 0)
        .and_then(normalize_timestamp)
        .map_or(start_nanos, |(value, _)| value);

    let kind = raw
        .kind
        .resolve(|name| SpanKind::from_otlp_name(name).map(SpanKind::as_otlp_number))
        .map_or(SpanKind::Unspecified, SpanKind::from_otlp_number);

    let (status, status_message) = match &raw.status {
        Some(status) => (
            status
                .code
                .resolve(|name| SpanStatus::from_otlp_name(name).map(SpanStatus::as_otlp_number))
                .map_or(SpanStatus::Unset, SpanStatus::from_otlp_number),
            status.message.clone(),
        ),
        None => (SpanStatus::Unset, String::new()),
    };

    let mut attributes = Labels::new();
    extend_attributes(&mut attributes, &raw.attributes);
    if attributes.len() > ctx.limits.max_attrs_per_record as usize {
        return Err(Rejection::new(
            RejectReason::TooManyAttributes,
            format!(
                "{} span attributes exceeds max_attrs_per_record ({})",
                attributes.len(),
                ctx.limits.max_attrs_per_record
            ),
        ));
    }

    let events = raw
        .events
        .iter()
        .map(|event| {
            let mut event_attributes = Labels::new();
            extend_attributes(&mut event_attributes, &event.attributes);
            SpanEvent {
                time_nanos: event
                    .time_unix_nano
                    .get()
                    .filter(|v| *v > 0)
                    .and_then(normalize_timestamp)
                    .map_or(start_nanos, |(value, _)| value),
                name: event.name.clone(),
                attributes: event_attributes,
            }
        })
        .collect();

    let stream = build_stream_labels(inherited, ctx)?;

    Ok(SpanRecord {
        trace_id,
        span_id,
        parent_span_id: normalize_id(&raw.parent_span_id),
        name: raw.name.clone(),
        kind,
        start_nanos,
        end_nanos,
        status,
        status_message,
        stream,
        attributes,
        events,
    })
}

/// Spans get `app` plus the configured promotions, but no `level` — severity is a log
/// concept, and a synthetic one on spans would pollute the label space for nothing.
fn build_stream_labels(inherited: &Labels, ctx: DecodeContext<'_>) -> Result<Labels, Rejection> {
    let mut stream = Labels::new();

    let app = inherited
        .get(APP_LABEL)
        .or_else(|| inherited.get("service_name"))
        .unwrap_or(UNKNOWN_APP)
        .to_owned();
    stream.insert(APP_LABEL, app);

    for name in &ctx.ingest.stream_labels {
        let name = sanitize_label_name(name);
        if name == APP_LABEL {
            continue;
        }
        if let Some(value) = inherited.get(&name) {
            stream.insert(name, value);
        }
    }

    if stream.len() > ctx.limits.max_labels_per_series as usize {
        return Err(Rejection::new(
            RejectReason::TooManyLabels,
            format!(
                "{} stream labels exceeds max_labels_per_series ({})",
                stream.len(),
                ctx.limits.max_labels_per_series
            ),
        ));
    }
    Ok(stream)
}

/// Convenience for callers that only have the config.
pub fn context<'a>(
    limits: &'a LimitsConfig,
    ingest: &'a IngestConfig,
    now_nanos: u64,
) -> DecodeContext<'a> {
    DecodeContext {
        limits,
        ingest,
        now_nanos,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const NOW: u64 = 1_750_000_000_000_000_000;

    fn decode_str(json: &str) -> Decoded<SpanRecord> {
        let limits = LimitsConfig::default();
        let ingest = IngestConfig::default();
        decode(json.as_bytes(), context(&limits, &ingest, NOW)).unwrap()
    }

    const REALISTIC: &str = r#"{
      "resourceSpans": [{
        "resource": {"attributes": [
          {"key":"service.name","value":{"stringValue":"checkout"}},
          {"key":"deployment.environment","value":{"stringValue":"production"}}
        ]},
        "scopeSpans": [{
          "scope": {"name":"laravel-telemetry"},
          "spans": [{
            "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
            "spanId":"00f067aa0ba902b7",
            "name":"POST /checkout",
            "kind":2,
            "startTimeUnixNano":"1750000000000000000",
            "endTimeUnixNano":"1750000000150000000",
            "attributes":[
              {"key":"http.method","value":{"stringValue":"POST"}},
              {"key":"http.status_code","value":{"intValue":"500"}}
            ],
            "status":{"code":2,"message":"payment declined"},
            "events":[{
              "timeUnixNano":"1750000000100000000",
              "name":"exception",
              "attributes":[{"key":"exception.type","value":{"stringValue":"PaymentError"}}]
            }]
          },{
            "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
            "spanId":"aaaaaaaaaaaaaaaa",
            "parentSpanId":"00f067aa0ba902b7",
            "name":"SELECT orders",
            "kind":3,
            "startTimeUnixNano":"1750000000020000000",
            "endTimeUnixNano":"1750000000030000000"
          }]
        }]
      }]
    }"#;

    #[test]
    fn decodes_a_realistic_trace() {
        let decoded = decode_str(REALISTIC);
        assert_eq!(decoded.records.len(), 2);
        assert!(decoded.rejections.is_empty());

        let root = &decoded.records[0];
        assert_eq!(root.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(root.span_id, "00f067aa0ba902b7");
        assert_eq!(root.name, "POST /checkout");
        assert_eq!(root.kind, SpanKind::Server);
        assert_eq!(root.status, SpanStatus::Error);
        assert_eq!(root.status_message, "payment declined");
        assert_eq!(root.duration_nanos(), 150_000_000);
        assert!(root.is_root());
        assert_eq!(root.app(), "checkout");
        // Span attributes keep the producer's spelling, and are reachable by either.
        assert_eq!(root.attributes.get("http.status_code"), Some("500"));
        assert_eq!(root.attributes.get_relaxed("http_status_code"), Some("500"));

        let child = &decoded.records[1];
        assert_eq!(child.parent_span_id.as_deref(), Some("00f067aa0ba902b7"));
        assert_eq!(child.kind, SpanKind::Client);
        assert!(!child.is_root());
    }

    #[test]
    fn span_events_are_preserved_with_their_attributes() {
        let root = &decode_str(REALISTIC).records[0];
        assert_eq!(root.events.len(), 1);
        assert_eq!(root.events[0].name, "exception");
        assert_eq!(root.events[0].time_nanos, 1_750_000_000_100_000_000);
        assert_eq!(
            root.events[0].attributes.get("exception.type"),
            Some("PaymentError")
        );
    }

    #[test]
    fn spans_get_no_level_label() {
        // Severity is a log concept; a synthetic `level` on spans would pollute the
        // label space and make Tempo tag lists misleading.
        let root = &decode_str(REALISTIC).records[0];
        assert!(!root.stream.contains_key("level"));
        assert_eq!(
            root.stream.get("deployment_environment"),
            Some("production")
        );
    }

    #[test]
    fn a_span_without_ids_is_rejected_rather_than_orphaned() {
        let decoded = decode_str(
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[
                {"spanId":"00f067aa0ba902b7","name":"no trace id"},
                {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","name":"no span id"},
                {"traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7","name":"fine"}
            ]}]}]}"#,
        );
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.rejections.len(), 2);
        assert_eq!(decoded.rejections[0].reason, RejectReason::MissingTraceId);
        assert_eq!(decoded.rejections[1].reason, RejectReason::MissingSpanId);
    }

    #[test]
    fn an_all_zero_parent_means_root_not_a_real_parent() {
        let decoded = decode_str(
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{
                "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
                "spanId":"00f067aa0ba902b7",
                "parentSpanId":"0000000000000000",
                "name":"root"}]}]}]}"#,
        );
        assert!(decoded.records[0].is_root());
    }

    #[test]
    fn kind_and_status_accept_proto_enum_names() {
        let decoded = decode_str(
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{
                "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
                "spanId":"00f067aa0ba902b7",
                "name":"named enums",
                "kind":"SPAN_KIND_CLIENT",
                "status":{"code":"STATUS_CODE_ERROR"}}]}]}]}"#,
        );
        assert_eq!(decoded.records[0].kind, SpanKind::Client);
        assert_eq!(decoded.records[0].status, SpanStatus::Error);
    }

    #[test]
    fn a_missing_end_time_yields_a_zero_duration_not_a_huge_one() {
        let decoded = decode_str(
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{
                "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
                "spanId":"00f067aa0ba902b7",
                "name":"unfinished",
                "startTimeUnixNano":"1750000000000000000"}]}]}]}"#,
        );
        assert_eq!(decoded.records[0].duration_nanos(), 0);
    }

    #[test]
    fn snake_case_payloads_decode_identically() {
        let decoded = decode_str(
            r#"{"resource_spans":[{"scope_spans":[{"spans":[{
                "trace_id":"4bf92f3577b34da6a3ce929d0e0e4736",
                "span_id":"00f067aa0ba902b7",
                "parent_span_id":"aaaaaaaaaaaaaaaa",
                "name":"snake",
                "start_time_unix_nano":"1750000000000000000",
                "end_time_unix_nano":"1750000000001000000"}]}]}]}"#,
        );
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].duration_nanos(), 1_000_000);
        assert!(!decoded.records[0].is_root());
    }

    #[test]
    fn timestamps_in_the_wrong_unit_are_corrected_and_counted() {
        let decoded = decode_str(
            r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{
                "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
                "spanId":"00f067aa0ba902b7",
                "name":"seconds",
                "startTimeUnixNano":"1750000000",
                "endTimeUnixNano":"1750000001"}]}]}]}"#,
        );
        assert_eq!(decoded.records[0].start_nanos, NOW);
        assert_eq!(decoded.records[0].duration_nanos(), 1_000_000_000);
        assert_eq!(decoded.rescaled_timestamps, 1);
    }

    #[test]
    fn an_empty_payload_is_valid_and_yields_nothing() {
        for json in ["{}", r#"{"resourceSpans":[]}"#] {
            let decoded = decode_str(json);
            assert!(decoded.records.is_empty(), "{json}");
        }
    }
}
