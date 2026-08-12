//! OTLP/HTTP JSON logs decoding.
//!
//! Turns an `ExportLogsServiceRequest` into [`LogRecord`]s, enforcing the configured
//! limits as it goes. A bad record never costs the batch: rejections are collected
//! per-record and reported through OTLP's own `partialSuccess` mechanism, so a client
//! sending 500 lines with one 2 MB body still gets the other 499 stored.

use serde::Deserialize;
use telemetryd_core::config::{IngestConfig, LimitsConfig};
use telemetryd_core::record::{APP_LABEL, LEVEL_LABEL, UNKNOWN_APP, sanitize_label_name};
use telemetryd_core::{Labels, LogRecord, Severity};

use crate::otlp::{
    AnyValue, FlexEnum, FlexU64, InstrumentationScope, KeyValue, Resource, extend_attributes,
    extend_labels, keep_unpromoted, normalize_id,
};
use crate::{Decoded, RejectReason, Rejection};

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LogsData {
    #[serde(alias = "resource_logs")]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ResourceLogs {
    pub resource: Option<Resource>,
    #[serde(alias = "scope_logs")]
    pub scope_logs: Vec<ScopeLogs>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ScopeLogs {
    pub scope: Option<InstrumentationScope>,
    #[serde(alias = "log_records")]
    pub log_records: Vec<LogRecordJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LogRecordJson {
    #[serde(alias = "time_unix_nano")]
    pub time_unix_nano: FlexU64,
    #[serde(alias = "observed_time_unix_nano")]
    pub observed_time_unix_nano: FlexU64,
    #[serde(alias = "severity_number")]
    pub severity_number: FlexEnum,
    #[serde(alias = "severity_text")]
    pub severity_text: String,
    pub body: Option<AnyValue>,
    pub attributes: Vec<KeyValue>,
    #[serde(alias = "trace_id")]
    pub trace_id: String,
    #[serde(alias = "span_id")]
    pub span_id: String,
}

/// Map the proto enum names for `severityNumber`.
fn severity_number_by_name(name: &str) -> Option<i32> {
    let suffix = name.strip_prefix("SEVERITY_NUMBER_")?;
    let (base, offset) = match suffix.chars().next()? {
        'T' => (1, "TRACE"),
        'D' => (5, "DEBUG"),
        'I' => (9, "INFO"),
        'W' => (13, "WARN"),
        'E' => (17, "ERROR"),
        'F' => (21, "FATAL"),
        _ => return None,
    };
    let rest = suffix.strip_prefix(offset)?;
    // The proto spells the levels within a group as TRACE, TRACE2, TRACE3, TRACE4.
    match rest {
        "" => Some(base),
        "2" => Some(base + 1),
        "3" => Some(base + 2),
        "4" => Some(base + 3),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

// Plausible range for an event, as Unix *seconds*: 2001-01-01 to 2100-01-01.
const MIN_SECONDS: u64 = 978_307_200;
const MAX_SECONDS: u64 = 4_102_444_800;

/// How a raw timestamp was interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Nanos,
    Micros,
    Millis,
    Seconds,
}

/// Interpret a raw timestamp, correcting the unit when it is unambiguous.
///
/// OTLP says nanoseconds, and sending seconds or milliseconds instead is one of the
/// most common integration mistakes there is — it puts every record in 1970 and makes
/// the data look silently lost. The magnitudes do not overlap for any date between
/// 2001 and 2100, so the intended unit is recoverable rather than a guess. Rescaling
/// is counted on `telemetryd_ingest_timestamps_rescaled_total`, so a producer bug
/// stays visible instead of being papered over.
pub fn normalize_timestamp(raw: u64) -> Option<(u64, TimeUnit)> {
    const NANOS: u64 = 1_000_000_000;
    if (MIN_SECONDS * NANOS..MAX_SECONDS * NANOS).contains(&raw) {
        Some((raw, TimeUnit::Nanos))
    } else if (MIN_SECONDS * 1_000_000..MAX_SECONDS * 1_000_000).contains(&raw) {
        Some((raw * 1_000, TimeUnit::Micros))
    } else if (MIN_SECONDS * 1_000..MAX_SECONDS * 1_000).contains(&raw) {
        Some((raw * 1_000_000, TimeUnit::Millis))
    } else if (MIN_SECONDS..MAX_SECONDS).contains(&raw) {
        Some((raw * NANOS, TimeUnit::Seconds))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Everything a decode pass needs that is not the payload itself.
#[derive(Debug, Clone, Copy)]
pub struct DecodeContext<'a> {
    pub limits: &'a LimitsConfig,
    pub ingest: &'a IngestConfig,
    /// Used when a record carries no usable timestamp of its own.
    pub now_nanos: u64,
}

/// Decode an `ExportLogsServiceRequest`.
///
/// A parse failure of the *envelope* is a request-level error; anything wrong with an
/// individual record is a rejection recorded in [`Decoded::rejections`].
pub fn decode(
    body: &[u8],
    ctx: DecodeContext<'_>,
) -> Result<Decoded<LogRecord>, serde_json::Error> {
    let data: LogsData = serde_json::from_slice(body)?;
    Ok(convert_data(&data, ctx))
}

/// Convert an already-parsed payload.
///
/// Split from [`decode`] so the protobuf decoder can reach it with the same structs.
/// Every limit, rejection reason and counter lives below this line, which is what stops
/// the two encodings drifting: there is one conversion, not one per encoding.
pub fn convert_data(data: &LogsData, ctx: DecodeContext<'_>) -> Decoded<LogRecord> {
    let mut decoded = Decoded::default();

    for resource_logs in &data.resource_logs {
        let mut resource_labels = Labels::new();
        // The same attributes twice, under two spellings: sanitised for the ones that
        // become stream labels, verbatim for the ones that do not and are kept as record
        // attributes. Rewriting `k8s.pod.name` to `k8s_pod_name` on the way into storage
        // would show a key nobody sent, which is the reason record attributes have always
        // been kept verbatim.
        let mut resource_attributes = Labels::new();
        if let Some(resource) = &resource_logs.resource {
            extend_labels(&mut resource_labels, &resource.attributes);
            extend_attributes(&mut resource_attributes, &resource.attributes);
        }

        for scope_logs in &resource_logs.scope_logs {
            let mut scope_labels = resource_labels.clone();
            let mut scope_attributes = resource_attributes.clone();
            if let Some(scope) = &scope_logs.scope {
                // Scope attributes are narrower than resource attributes, so they win.
                extend_labels(&mut scope_labels, &scope.attributes);
                extend_attributes(&mut scope_attributes, &scope.attributes);
                if !scope.name.is_empty() {
                    scope_labels.insert("scope_name", scope.name.clone());
                }
            }

            for record in &scope_logs.log_records {
                match convert(record, &scope_labels, &scope_attributes, ctx, &mut decoded) {
                    Ok(converted) => decoded.records.push(converted),
                    Err(rejection) => decoded.rejections.push(rejection),
                }
            }
        }
    }

    decoded
}

fn convert(
    raw: &LogRecordJson,
    inherited: &Labels,
    inherited_attributes: &Labels,
    ctx: DecodeContext<'_>,
    decoded: &mut Decoded<LogRecord>,
) -> Result<LogRecord, Rejection> {
    // Event time, falling back to observation time, then to arrival.
    let (timestamp_nanos, unit) = raw
        .time_unix_nano
        .get()
        .filter(|v| *v > 0)
        .and_then(normalize_timestamp)
        .or_else(|| {
            raw.observed_time_unix_nano
                .get()
                .filter(|v| *v > 0)
                .and_then(normalize_timestamp)
        })
        .unwrap_or((ctx.now_nanos, TimeUnit::Nanos));
    if unit != TimeUnit::Nanos {
        decoded.rescaled_timestamps += 1;
    }

    let severity = raw
        .severity_number
        .resolve(severity_number_by_name)
        .map(Severity::from_otlp_number)
        .filter(|s| *s != Severity::Unknown)
        .unwrap_or_else(|| Severity::from_text(&raw.severity_text));

    let mut body = raw
        .body
        .as_ref()
        .and_then(AnyValue::to_text)
        .unwrap_or_default();

    let max_body = usize::try_from(ctx.limits.max_log_line_bytes.as_u64()).unwrap_or(usize::MAX);
    if body.len() > max_body {
        if !ctx.ingest.truncate_oversized_bodies {
            return Err(Rejection::new(
                RejectReason::BodyTooLarge,
                format!(
                    "log body of {} bytes exceeds max_log_line_bytes",
                    body.len()
                ),
            ));
        }
        let original = body.len();
        truncate_on_char_boundary(&mut body, max_body.saturating_sub(TRUNCATION_MARK.len()));
        body.push_str(TRUNCATION_MARK);
        decoded.truncated_bodies += 1;
        tracing::debug!(original_bytes = original, "truncated an oversized log body");
    }

    // Record attributes: per-record, deliberately not part of stream identity, and
    // kept under the producer's own key spelling.
    let mut attributes = Labels::new();
    extend_attributes(&mut attributes, &raw.attributes);

    let stream = build_stream_labels(inherited, severity, ctx)?;

    // Resource and scope attributes that no stream label claimed. Merged before the
    // limit is checked rather than after: `max_attrs_per_record` bounds what is stored
    // per record, and these are now stored per record. Counting only the producer's own
    // attributes would make the limit describe something other than the width it is
    // there to cap.
    keep_unpromoted(&mut attributes, inherited_attributes, &stream);

    if attributes.len() > ctx.limits.max_attrs_per_record as usize {
        return Err(Rejection::new(
            RejectReason::TooManyAttributes,
            format!(
                "{} attributes exceeds max_attrs_per_record ({})",
                attributes.len(),
                ctx.limits.max_attrs_per_record
            ),
        ));
    }

    Ok(LogRecord {
        timestamp_nanos,
        stream,
        severity,
        severity_text: raw.severity_text.clone(),
        body,
        attributes,
        trace_id: normalize_id(&raw.trace_id),
        span_id: normalize_id(&raw.span_id),
    })
}

const TRUNCATION_MARK: &str = "…[truncated by telemetryd]";

/// Truncate to at most `max` bytes without splitting a UTF-8 character.
fn truncate_on_char_boundary(text: &mut String, max: usize) {
    if text.len() <= max {
        return;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
}

/// Derive the stream label set: `app`, `level`, and the configured promotions.
fn build_stream_labels(
    inherited: &Labels,
    severity: Severity,
    ctx: DecodeContext<'_>,
) -> Result<Labels, Rejection> {
    let mut stream = Labels::new();

    // `app` is never absent — retention, quotas and queries all key off it, and
    // "sometimes missing" would mean every one of them needs a special case.
    let app = inherited
        .get(APP_LABEL)
        .or_else(|| inherited.get("service_name"))
        .unwrap_or(UNKNOWN_APP)
        .to_owned();
    stream.insert(APP_LABEL, app);
    stream.insert(LEVEL_LABEL, severity.as_str());

    for name in &ctx.ingest.stream_labels {
        let name = sanitize_label_name(name);
        if name == APP_LABEL || name == LEVEL_LABEL {
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
    for (name, value) in stream.iter() {
        if name.len() > ctx.limits.max_label_name_bytes as usize {
            return Err(Rejection::new(
                RejectReason::LabelNameTooLong,
                format!("label name {name:?} exceeds max_label_name_bytes"),
            ));
        }
        if value.len() > ctx.limits.max_label_value_bytes as usize {
            return Err(Rejection::new(
                RejectReason::LabelValueTooLong,
                format!("value of label {name:?} exceeds max_label_value_bytes"),
            ));
        }
    }

    Ok(stream)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const NOW: u64 = 1_750_000_000_000_000_000;

    fn ctx<'a>(limits: &'a LimitsConfig, ingest: &'a IngestConfig) -> DecodeContext<'a> {
        DecodeContext {
            limits,
            ingest,
            now_nanos: NOW,
        }
    }

    fn decode_str(json: &str) -> Decoded<LogRecord> {
        let limits = LimitsConfig::default();
        let ingest = IngestConfig::default();
        decode(json.as_bytes(), ctx(&limits, &ingest)).unwrap()
    }

    const REALISTIC: &str = r#"{
      "resourceLogs": [{
        "resource": { "attributes": [
          {"key":"service.name","value":{"stringValue":"checkout"}},
          {"key":"deployment.environment","value":{"stringValue":"production"}},
          {"key":"host.id","value":{"stringValue":"i-0abc123"}}
        ]},
        "scopeLogs": [{
          "scope": {"name":"laravel-telemetry","version":"1.2.0"},
          "logRecords": [{
            "timeUnixNano":"1750000000000000000",
            "severityNumber":17,
            "severityText":"ERROR",
            "body":{"stringValue":"Payment declined"},
            "attributes":[{"key":"order.id","value":{"intValue":"9912"}}],
            "traceId":"4bf92f3577b34da6a3ce929d0e0e4736",
            "spanId":"00f067aa0ba902b7"
          }]
        }]
      }]
    }"#;

    #[test]
    fn decodes_a_realistic_laravel_telemetry_payload() {
        let decoded = decode_str(REALISTIC);
        assert_eq!(decoded.records.len(), 1);
        assert!(decoded.rejections.is_empty());

        let record = &decoded.records[0];
        assert_eq!(record.timestamp_nanos, 1_750_000_000_000_000_000);
        assert_eq!(record.severity, Severity::Error);
        assert_eq!(record.severity_text, "ERROR");
        assert_eq!(record.body, "Payment declined");
        assert_eq!(record.app(), "checkout");
        assert_eq!(
            record.trace_id.as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        assert_eq!(record.span_id.as_deref(), Some("00f067aa0ba902b7"));

        // Record attributes keep the producer's key spelling — a UI that shows
        // attributes should show what was sent, not a rewritten form.
        assert_eq!(record.attributes.get("order.id"), Some("9912"));
        // …and are reachable by the label-safe form too.
        assert_eq!(record.attributes.get_relaxed("order_id"), Some("9912"));
        // But they are never part of stream identity.
        assert!(!record.stream.contains_key("order.id"));
        assert!(!record.stream.contains_key("order_id"));
    }

    #[test]
    fn only_configured_resource_attributes_become_stream_labels() {
        let record = &decode_str(REALISTIC).records[0];

        assert_eq!(record.stream.get("app"), Some("checkout"));
        assert_eq!(record.stream.get("level"), Some("error"));
        assert_eq!(record.stream.get("service_name"), Some("checkout"));
        assert_eq!(
            record.stream.get("deployment_environment"),
            Some("production")
        );

        // host.id is per-instance. Promoting it would multiply streams on every
        // deploy, so it stays out of the stream by default.
        assert!(!record.stream.contains_key("host_id"));
    }

    #[test]
    fn a_resource_attribute_no_label_claimed_is_kept_rather_than_dropped() {
        // The bug: resource attributes were read only to build stream labels, and the
        // five that `ingest.stream_labels` promotes were the five that survived.
        // Everything a non-Laravel sender puts in `resource` — k8s, host, cloud,
        // container — was discarded with no counter, no warning and no `partialSuccess`.
        let decoded = decode_str(
            r#"{"resourceLogs":[{"resource":{"attributes":[
                 {"key":"service.name","value":{"stringValue":"third-party"}},
                 {"key":"service.version","value":{"stringValue":"2.1.0"}},
                 {"key":"k8s.pod.name","value":{"stringValue":"pod-7f"}},
                 {"key":"host.name","value":{"stringValue":"box1"}}]},
               "scopeLogs":[{"logRecords":[
                 {"timeUnixNano":"1700000000000000000","severityNumber":9,
                  "body":{"stringValue":"hello"},
                  "attributes":[{"key":"order.id","value":{"stringValue":"A-99"}}]}]}]}]}"#,
        );
        let record = &decoded.records[0];

        // Kept, and under the spelling the producer sent — not `k8s_pod_name`, which is
        // a key nobody put on the wire.
        assert_eq!(record.attributes.get("k8s.pod.name"), Some("pod-7f"));
        assert_eq!(record.attributes.get("host.name"), Some("box1"));
        // The producer's own record attributes are untouched.
        assert_eq!(record.attributes.get("order.id"), Some("A-99"));

        // Still not stream identity: that is the cardinality rule, and it is unchanged.
        assert!(!record.stream.contains_key("k8s.pod.name"));
        assert!(!record.stream.contains_key("k8s_pod_name"));

        // A promoted attribute is not stored twice. `service.version` is the label
        // `service_version`; they are one attribute under two spellings, not two.
        assert_eq!(record.stream.get("service_version"), Some("2.1.0"));
        assert!(record.attributes.get("service.version").is_none());
        assert!(record.attributes.get("service.name").is_none());
    }

    #[test]
    fn a_record_attribute_beats_the_resource_attribute_of_the_same_name() {
        // Narrowest scope closest to the data, which is the precedence used for stream
        // labels already. A resource-level default must not overwrite the value the
        // record itself carried.
        let decoded = decode_str(
            r#"{"resourceLogs":[{"resource":{"attributes":[
                 {"key":"service.name","value":{"stringValue":"app"}},
                 {"key":"region","value":{"stringValue":"resource-level"}}]},
               "scopeLogs":[{"logRecords":[
                 {"timeUnixNano":"1700000000000000000","severityNumber":9,
                  "body":{"stringValue":"hello"},
                  "attributes":[{"key":"region","value":{"stringValue":"record-level"}}]}]}]}]}"#,
        );
        assert_eq!(
            decoded.records[0].attributes.get("region"),
            Some("record-level")
        );
    }

    #[test]
    fn an_explicit_app_attribute_beats_service_name() {
        let decoded = decode_str(
            r#"{"resourceLogs":[{"resource":{"attributes":[
                {"key":"service.name","value":{"stringValue":"svc"}},
                {"key":"app","value":{"stringValue":"explicit"}}]},
              "scopeLogs":[{"logRecords":[{"timeUnixNano":"1750000000000000000","body":{"stringValue":"x"}}]}]}]}"#,
        );
        assert_eq!(decoded.records[0].app(), "explicit");
    }

    #[test]
    fn a_record_with_no_service_identity_still_gets_an_app() {
        let decoded = decode_str(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
                {"timeUnixNano":"1750000000000000000","body":{"stringValue":"orphan"}}]}]}]}"#,
        );
        assert_eq!(decoded.records[0].app(), "unknown");
    }

    #[test]
    fn severity_falls_back_from_number_to_text() {
        let decoded = decode_str(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
                {"timeUnixNano":"1750000000000000000","severityText":"warning","body":{"stringValue":"x"}}]}]}]}"#,
        );
        assert_eq!(decoded.records[0].severity, Severity::Warn);
        assert_eq!(decoded.records[0].stream.get("level"), Some("warn"));
    }

    #[test]
    fn severity_accepts_the_proto_enum_name() {
        for (name, expected) in [
            ("SEVERITY_NUMBER_ERROR", Severity::Error),
            ("SEVERITY_NUMBER_INFO", Severity::Info),
            ("SEVERITY_NUMBER_WARN2", Severity::Warn),
            ("SEVERITY_NUMBER_FATAL4", Severity::Fatal),
        ] {
            let decoded = decode_str(&format!(
                r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[
                    {{"timeUnixNano":"1750000000000000000","severityNumber":"{name}","body":{{"stringValue":"x"}}}}]}}]}}]}}"#
            ));
            assert_eq!(decoded.records[0].severity, expected, "{name}");
        }
    }

    #[test]
    fn timestamps_in_the_wrong_unit_are_corrected_and_counted() {
        // Sending seconds or millis instead of nanos is the single most common OTLP
        // integration mistake; it must not silently put every record in 1970.
        for raw in ["1750000000", "1750000000000", "1750000000000000"] {
            let decoded = decode_str(&format!(
                r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[
                    {{"timeUnixNano":"{raw}","body":{{"stringValue":"x"}}}}]}}]}}]}}"#
            ));
            assert_eq!(
                decoded.records[0].timestamp_nanos, 1_750_000_000_000_000_000,
                "{raw}"
            );
            assert_eq!(
                decoded.rescaled_timestamps, 1,
                "rescaling must be counted for {raw}"
            );
        }
    }

    #[test]
    fn a_correct_nanosecond_timestamp_is_never_rescaled() {
        let decoded = decode_str(REALISTIC);
        assert_eq!(decoded.rescaled_timestamps, 0);
    }

    #[test]
    fn a_missing_timestamp_falls_back_to_observed_then_to_now() {
        let observed = decode_str(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
                {"observedTimeUnixNano":"1750000000000000000","body":{"stringValue":"x"}}]}]}]}"#,
        );
        assert_eq!(
            observed.records[0].timestamp_nanos,
            1_750_000_000_000_000_000
        );

        let neither = decode_str(
            r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"x"}}]}]}]}"#,
        );
        assert_eq!(neither.records[0].timestamp_nanos, NOW);
    }

    #[test]
    fn an_oversized_body_is_truncated_on_a_char_boundary_and_marked() {
        let limits = LimitsConfig {
            max_log_line_bytes: bytesize::ByteSize::b(64),
            ..LimitsConfig::default()
        };
        let ingest = IngestConfig::default();

        // Multi-byte characters, so a naive byte truncation would split one.
        let body = "æøå".repeat(100);
        let json = format!(
            r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[
                {{"timeUnixNano":"1750000000000000000","body":{{"stringValue":"{body}"}}}}]}}]}}]}}"#
        );
        let decoded = decode(json.as_bytes(), ctx(&limits, &ingest)).unwrap();

        assert_eq!(
            decoded.records.len(),
            1,
            "truncating must not lose the record"
        );
        assert_eq!(decoded.truncated_bodies, 1);
        let stored = &decoded.records[0].body;
        assert!(
            stored.ends_with(TRUNCATION_MARK),
            "truncation must be visible"
        );
        assert!(stored.len() <= 64);
        assert!(std::str::from_utf8(stored.as_bytes()).is_ok());
    }

    #[test]
    fn oversized_bodies_are_rejected_when_truncation_is_disabled() {
        let limits = LimitsConfig {
            max_log_line_bytes: bytesize::ByteSize::b(16),
            ..LimitsConfig::default()
        };
        let ingest = IngestConfig {
            truncate_oversized_bodies: false,
            ..IngestConfig::default()
        };

        let json = format!(
            r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[
                {{"timeUnixNano":"1750000000000000000","body":{{"stringValue":"{}"}}}}]}}]}}]}}"#,
            "x".repeat(100)
        );
        let decoded = decode(json.as_bytes(), ctx(&limits, &ingest)).unwrap();
        assert!(decoded.records.is_empty());
        assert_eq!(decoded.rejections[0].reason, RejectReason::BodyTooLarge);
    }

    #[test]
    fn one_bad_record_does_not_cost_the_batch() {
        let limits = LimitsConfig {
            max_attrs_per_record: 1,
            ..LimitsConfig::default()
        };
        let ingest = IngestConfig::default();

        let json = r#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[
            {"timeUnixNano":"1750000000000000000","body":{"stringValue":"good"}},
            {"timeUnixNano":"1750000000000000000","body":{"stringValue":"bad"},
             "attributes":[{"key":"a","value":{"stringValue":"1"}},{"key":"b","value":{"stringValue":"2"}}]},
            {"timeUnixNano":"1750000000000000000","body":{"stringValue":"also good"}}]}]}]}"#;

        let decoded = decode(json.as_bytes(), ctx(&limits, &ingest)).unwrap();
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.rejections.len(), 1);
        assert_eq!(
            decoded.rejections[0].reason,
            RejectReason::TooManyAttributes
        );
    }

    #[test]
    fn an_empty_payload_is_valid_and_yields_nothing() {
        for json in [
            "{}",
            r#"{"resourceLogs":[]}"#,
            r#"{"resourceLogs":[{"scopeLogs":[]}]}"#,
        ] {
            let decoded = decode_str(json);
            assert!(decoded.records.is_empty(), "{json}");
            assert!(decoded.rejections.is_empty(), "{json}");
        }
    }

    #[test]
    fn malformed_json_is_a_request_level_error_not_a_rejection() {
        let limits = LimitsConfig::default();
        let ingest = IngestConfig::default();
        assert!(decode(b"{not json", ctx(&limits, &ingest)).is_err());
    }

    #[test]
    fn snake_case_payloads_decode_identically() {
        let decoded = decode_str(
            r#"{"resource_logs":[{"resource":{"attributes":[
                {"key":"service.name","value":{"string_value":"snake"}}]},
              "scope_logs":[{"log_records":[
                {"time_unix_nano":"1750000000000000000","severity_number":17,
                 "body":{"string_value":"hi"},"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"}]}]}]}"#,
        );
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].app(), "snake");
        assert_eq!(decoded.records[0].severity, Severity::Error);
        assert!(decoded.records[0].trace_id.is_some());
    }

    #[test]
    fn timestamp_normalisation_rejects_implausible_values() {
        assert_eq!(normalize_timestamp(0), None);
        assert_eq!(normalize_timestamp(12345), None);
        assert_eq!(normalize_timestamp(u64::MAX), None);
        assert_eq!(
            normalize_timestamp(1_750_000_000_000_000_000),
            Some((1_750_000_000_000_000_000, TimeUnit::Nanos))
        );
    }
}
