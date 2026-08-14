//! The M1 acceptance test: OTLP/HTTP JSON in, Loki query API out.
//!
//! This is the contract `cboxdk/laravel-telemetry` and `laravel-telemetry-ui` sit on
//! either side of, so it is tested as one path rather than as two halves that each
//! pass in isolation.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use telemetryd_core::Config;
use telemetryd_core::config::StorageConfig;
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use tower::ServiceExt;

const NOW: u64 = 1_750_000_000_000_000_000;
const MS: u64 = 1_000_000;

struct Harness {
    router: axum::Router,
    state: AppState,
    _tmp: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        Self::configured(|_| {})
    }

    fn configured(customise: impl FnOnce(&mut Config)) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config {
            storage: StorageConfig {
                data_dir: Some(tmp.path().join("data")),
                ..StorageConfig::default()
            },
            ..Config::default()
        };
        customise(&mut config);
        config.validate().unwrap();

        let store = Arc::new(Store::open(&config).unwrap());
        let state = AppState::new(Arc::new(config), store).unwrap();
        Self {
            router: router(state.clone()),
            state,
            _tmp: tmp,
        }
    }

    async fn post_logs(&self, payload: &Value) -> (StatusCode, Value) {
        let request = Request::post("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        let (status, body) = self.send(request).await;
        let json = serde_json::from_str(&body).unwrap_or(Value::Null);
        (status, json)
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let request = Request::get(path).body(Body::empty()).unwrap();
        let (status, body) = self.send(request).await;
        let json = serde_json::from_str(&body).unwrap_or(Value::String(body));
        (status, json)
    }

    async fn send(&self, request: Request<Body>) -> (StatusCode, String) {
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    fn seal(&self) {
        self.state.store.seal_all().unwrap();
    }
}

/// A payload shaped like what `cboxdk/laravel-telemetry` emits.
fn otlp_logs(entries: &[(u64, &str, &str, &str)]) -> Value {
    let records: Vec<Value> = entries
        .iter()
        .map(|(offset, severity, body, order_id)| {
            json!({
                "timeUnixNano": (NOW + offset * MS).to_string(),
                "severityNumber": match *severity {
                    "error" => 17,
                    "warn" => 13,
                    _ => 9,
                },
                "severityText": severity.to_uppercase(),
                "body": {"stringValue": body},
                "attributes": [
                    {"key": "order.id", "value": {"stringValue": order_id}}
                ],
                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                "spanId": "00f067aa0ba902b7"
            })
        })
        .collect();

    json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "deployment.environment", "value": {"stringValue": "production"}}
            ]},
            "scopeLogs": [{
                "scope": {"name": "laravel-telemetry", "version": "1.0.0"},
                "logRecords": records
            }]
        }]
    })
}

fn range(path: &str, query: &str) -> String {
    format!(
        "{path}?query={}&start={}&end={}",
        urlencode(query),
        NOW - 3_600_000_000_000,
        NOW + 3_600_000_000_000
    )
}

fn urlencode(raw: &str) -> String {
    raw.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// the round trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn otlp_json_in_loki_query_out() {
    let harness = Harness::new();

    let (status, body) = harness
        .post_logs(&otlp_logs(&[
            (0, "info", "order created", "1001"),
            (1, "error", "payment declined", "1002"),
        ]))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({}), "a clean batch reports no partial success");

    let (status, response) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;
    assert_eq!(status, StatusCode::OK);

    // The exact Loki envelope the UI parses.
    assert_eq!(response["status"], "success");
    assert_eq!(response["data"]["resultType"], "streams");

    let streams = response["data"]["result"].as_array().unwrap();
    assert_eq!(streams.len(), 2, "info and error are different streams");

    let all_values: Vec<&Value> = streams
        .iter()
        .flat_map(|s| s["values"].as_array().unwrap())
        .collect();
    assert_eq!(all_values.len(), 2);

    // Timestamps are strings of nanoseconds — a JSON number would lose precision in a
    // JavaScript client.
    let first = &all_values[0];
    assert!(first[0].is_string(), "timestamp must be a string: {first}");
    assert!(first[0].as_str().unwrap().parse::<u64>().unwrap() >= NOW);
    assert!(first[1].is_string());

    // Stream labels come from the resource attributes we promote.
    let stream = &streams[0]["stream"];
    assert_eq!(stream["app"], "checkout");
    assert_eq!(stream["service_name"], "checkout");
    assert_eq!(stream["deployment_environment"], "production");
    assert!(stream["level"].is_string());

    assert!(response["data"]["stats"]["summary"]["totalEntriesReturned"].is_number());
}

#[tokio::test]
async fn data_is_queryable_before_it_is_sealed_and_after() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[(0, "info", "before sealing", "1")]))
        .await;

    let (_, live) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;
    assert_eq!(
        live["data"]["result"][0]["values"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    harness.seal();

    let (_, sealed) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;
    assert_eq!(
        sealed["data"]["result"][0]["values"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "sealing must not change what a query returns"
    );
}

#[tokio::test]
async fn records_survive_the_seal_boundary_exactly_once() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[(0, "info", "first", "1")]))
        .await;
    harness.seal();
    harness
        .post_logs(&otlp_logs(&[(1, "info", "second", "2")]))
        .await;

    let (_, response) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;
    let bodies: Vec<&str> = response["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["values"].as_array().unwrap())
        .map(|v| v[1].as_str().unwrap())
        .collect();

    assert_eq!(bodies.len(), 2, "got {bodies:?}");
    assert!(bodies.contains(&"first"));
    assert!(bodies.contains(&"second"));
}

// ---------------------------------------------------------------------------
// LogQL over the wire
// ---------------------------------------------------------------------------

#[tokio::test]
async fn line_filters_narrow_the_result() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[
            (0, "info", "order created", "1"),
            (1, "error", "payment declined", "2"),
            (2, "error", "payment retried", "3"),
        ]))
        .await;

    let (_, response) = harness
        .get(&range(
            "/loki/api/v1/query_range",
            r#"{app="checkout"} |= "payment" != "retried""#,
        ))
        .await;

    let bodies: Vec<&str> = response["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["values"].as_array().unwrap())
        .map(|v| v[1].as_str().unwrap())
        .collect();
    assert_eq!(bodies, vec!["payment declined"]);
}

#[tokio::test]
async fn label_filters_reach_record_attributes() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[
            (0, "info", "a", "1001"),
            (1, "info", "b", "1002"),
        ]))
        .await;

    // The label-safe spelling still reaches the attribute, which is what a LogQL
    // label filter can express.
    let (status, response) = harness
        .get(&range(
            "/loki/api/v1/query_range",
            r#"{app="checkout"} | order_id="1002""#,
        ))
        .await;
    assert_eq!(status, StatusCode::OK);

    let bodies: Vec<&str> = response["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["values"].as_array().unwrap())
        .map(|v| v[1].as_str().unwrap())
        .collect();
    assert_eq!(bodies, vec!["b"]);
}

#[tokio::test]
async fn level_selects_by_normalised_severity() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[
            (0, "info", "fine", "1"),
            (1, "error", "broken", "2"),
        ]))
        .await;

    let (_, response) = harness
        .get(&range(
            "/loki/api/v1/query_range",
            r#"{app="checkout", level="error"}"#,
        ))
        .await;
    let streams = response["data"]["result"].as_array().unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0]["values"][0][1], "broken");
}

#[tokio::test]
async fn direction_and_limit_select_the_right_end_of_the_range() {
    let harness = Harness::new();
    let entries: Vec<(u64, &str, &str, &str)> = (0..10).map(|i| (i, "info", "line", "1")).collect();
    harness.post_logs(&otlp_logs(&entries)).await;

    let newest = format!(
        "{}&limit=1&direction=backward",
        range("/loki/api/v1/query_range", r#"{app="checkout"}"#)
    );
    let (_, response) = harness.get(&newest).await;
    let ts: u64 = response["data"]["result"][0]["values"][0][0]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(ts, NOW + 9 * MS, "backward must return the newest entry");

    let oldest = format!(
        "{}&limit=1&direction=forward",
        range("/loki/api/v1/query_range", r#"{app="checkout"}"#)
    );
    let (_, response) = harness.get(&oldest).await;
    let ts: u64 = response["data"]["result"][0]["values"][0][0]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(ts, NOW, "forward must return the oldest entry");
}

#[tokio::test]
async fn the_time_range_actually_filters() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[
            (0, "info", "inside", "1"),
            (5000, "info", "outside", "2"),
        ]))
        .await;

    let path = format!(
        "/loki/api/v1/query_range?query={}&start={}&end={}",
        urlencode(r#"{app="checkout"}"#),
        NOW - MS,
        NOW + MS
    );
    let (_, response) = harness.get(&path).await;
    let bodies: Vec<&str> = response["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["values"].as_array().unwrap())
        .map(|v| v[1].as_str().unwrap())
        .collect();
    assert_eq!(bodies, vec!["inside"]);
}

// ---------------------------------------------------------------------------
// label discovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn labels_and_values_reflect_ingested_data() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[(0, "info", "a", "1"), (1, "error", "b", "2")]))
        .await;

    let (status, response) = harness
        .get(&format!(
            "/loki/api/v1/labels?start={}&end={}",
            NOW - MS,
            NOW + 3_600_000_000_000_u64
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["status"], "success");

    let names: Vec<&str> = response["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    for expected in ["app", "level", "service_name", "deployment_environment"] {
        assert!(names.contains(&expected), "missing {expected} in {names:?}");
    }

    let (_, response) = harness
        .get(&format!(
            "/loki/api/v1/label/level/values?start={}&end={}",
            NOW - MS,
            NOW + 3_600_000_000_000_u64
        ))
        .await;
    let values: Vec<&str> = response["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(values, vec!["error", "info"]);
}

#[tokio::test]
async fn series_returns_distinct_streams_for_a_selector() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[
            (0, "info", "a", "1"),
            (1, "info", "b", "2"),
            (2, "error", "c", "3"),
        ]))
        .await;

    let path = format!(
        "/loki/api/v1/series?match[]={}&start={}&end={}",
        urlencode(r#"{app="checkout"}"#),
        NOW - MS,
        NOW + 3_600_000_000_000_u64
    );
    let (status, response) = harness.get(&path).await;
    assert_eq!(status, StatusCode::OK);

    let series = response["data"].as_array().unwrap();
    assert_eq!(series.len(), 2, "info and error, deduplicated: {series:?}");
    assert!(series.iter().all(|s| s["app"] == "checkout"));
}

// ---------------------------------------------------------------------------
// error behaviour
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unsupported_logql_feature_is_named_with_a_400() {
    let harness = Harness::new();
    let (status, response) = harness
        .get(&range(
            "/loki/api/v1/query_range",
            r#"rate({app="checkout"}[5m])"#,
        ))
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"]["code"], "unsupported_feature");
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("rate"),
        "{response}"
    );
    assert!(
        response["error"]["docs"]
            .as_str()
            .unwrap()
            .ends_with("COMPATIBILITY.md"),
        "{response}"
    );
}

#[tokio::test]
async fn a_malformed_query_is_a_400_not_a_500() {
    let harness = Harness::new();
    for query in ["{app=", "{}", "not a selector"] {
        let (status, _) = harness.get(&range("/loki/api/v1/query_range", query)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
    }
}

#[tokio::test]
async fn a_missing_query_parameter_is_refused_clearly() {
    let harness = Harness::new();
    let (status, response) = harness.get("/loki/api/v1/query_range").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("query"),
        "{response}"
    );
}

#[tokio::test]
async fn malformed_otlp_json_is_a_400_naming_the_problem() {
    let harness = Harness::new();
    let request = Request::post("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{not json"))
        .unwrap();
    let (status, body) = harness.send(request).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["error"]["code"], "bad_request");
}

/// A stock OpenTelemetry SDK sends protobuf, and it has to arrive.
///
/// This test replaced one asserting the opposite — that protobuf was refused with
/// `unsupported_feature`. That was correct while telemetryd served JSON only, and it is
/// precisely what made every official SDK, all of which default to
/// `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`, store nothing at all.
///
/// The bytes are built by hand rather than by an encoder of ours, because an encoder
/// written next to the decoder would agree with its own mistakes.
#[tokio::test]
async fn a_protobuf_log_batch_is_stored_and_queryable() {
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
    fn delim(field: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = raw_varint(u64::from((field << 3) | 2));
        out.extend(raw_varint(payload.len() as u64));
        out.extend_from_slice(payload);
        out
    }
    fn varint(field: u32, value: u64) -> Vec<u8> {
        let mut out = raw_varint(u64::from(field << 3));
        out.extend(raw_varint(value));
        out
    }
    fn fixed64(field: u32, value: u64) -> Vec<u8> {
        let mut out = raw_varint(u64::from((field << 3) | 1));
        out.extend_from_slice(&value.to_le_bytes());
        out
    }
    fn attr(key: &str, value: &str) -> Vec<u8> {
        let mut kv = delim(1, key.as_bytes());
        kv.extend(delim(2, &delim(1, value.as_bytes())));
        kv
    }

    let mut record = fixed64(1, NOW);
    record.extend(varint(2, 17));
    record.extend(delim(5, &delim(1, b"payment declined")));
    record.extend(delim(6, &attr("order.id", "A-99")));

    let scope_logs = delim(2, &record);
    let mut resource_body = delim(1, &attr("service.name", "checkout"));
    resource_body.extend(delim(1, &attr("k8s.pod.name", "pod-7f")));
    let mut resource_logs = delim(1, &resource_body);
    resource_logs.extend(delim(2, &scope_logs));
    let wire = delim(1, &resource_logs);

    let harness = Harness::new();
    let request = Request::post("/v1/logs")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(wire))
        .unwrap();
    let (status, body) = harness.send(request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        !body.contains("partialSuccess"),
        "the protobuf batch was accepted but every record was rejected: {body}"
    );

    // NOW is a fixed 2025 timestamp, not the wall clock, so the range has to be
    // explicit — the default window is relative to real time and excludes it.
    let path = format!(
        "/loki/api/v1/query_range?query={}&start={}&end={}",
        urlencode(r#"{app="checkout"}"#),
        NOW - MS,
        NOW + MS
    );
    let (status, json) = harness.get(&path).await;
    assert_eq!(status, StatusCode::OK);
    let result = &json["data"]["result"];
    assert_eq!(result[0]["stream"]["app"], "checkout");
    assert_eq!(result[0]["stream"]["level"], "error");
    assert_eq!(result[0]["values"][0][1], "payment declined");

    // The attributes survive both the encoding and the resource/record distinction.
    let metadata = &result[0]["values"][0][2];
    assert_eq!(metadata["order_id"], "A-99");
    assert_eq!(
        metadata["k8s_pod_name"], "pod-7f",
        "a resource attribute no stream label claimed must survive the protobuf path too"
    );
}

/// A parser stage must return what it parsed, not just filter on it.
///
/// `| json` and `| logfmt` were filter-only: they parsed the body to decide whether a
/// record matched and then discarded the result. `| json | level="error"` returned the
/// line, and a stream whose `level` label said `info` — telemetryd's severity-derived
/// label, not the field the filter had matched. The answer looked like it contradicted
/// the question, and the value that had been selected on was nowhere in the response.
#[tokio::test]
async fn a_parser_stage_returns_the_fields_it_extracted() {
    let harness = Harness::new();
    harness
        .post_logs(&json!({
            "resourceLogs": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": "checkout"}}
                ]},
                "scopeLogs": [{"logRecords": [{
                    "timeUnixNano": NOW.to_string(),
                    "severityNumber": 9,
                    "body": {"stringValue": r#"{"level":"error","order":{"id":"A-1"}}"#},
                    "attributes": [{"key": "trace.id", "value": {"stringValue": "T-9"}}]
                }]}]
            }]
        }))
        .await;

    let path = format!(
        "/loki/api/v1/query_range?query={}&start={}&end={}&direction=forward",
        urlencode(r#"{app="checkout"} | json | level="error""#),
        NOW,
        NOW + MS
    );
    let (status, response) = harness.get(&path).await;
    assert_eq!(status, StatusCode::OK, "{response}");

    let entry = &response["data"]["result"][0];
    let metadata = &entry["values"][0][2];

    // The severity label is untouched: it is a different fact from the body's own idea
    // of level, and overwriting it would lose telemetryd's.
    assert_eq!(entry["stream"]["level"], "info");
    // …and the matched value is present under the name Loki uses for a collision, so a
    // reader of either system looks in the same place.
    assert_eq!(metadata["level_extracted"], "error");

    // Nested objects flatten with an underscore, which is what the label filter already
    // matched on — `| json | order_id="A-1"` worked before this and still does.
    assert_eq!(metadata["order_id"], "A-1");
    // Record attributes still come through beside them.
    assert_eq!(metadata["trace_id"], "T-9");
}

/// A query with no parser stage pays nothing for the one above.
#[tokio::test]
async fn a_query_without_a_parser_extracts_nothing() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[(0, "info", r#"{"level":"error"}"#, "1")]))
        .await;

    let path = format!(
        "/loki/api/v1/query_range?query={}&start={}&end={}&direction=forward",
        urlencode(r#"{app="checkout"}"#),
        NOW,
        NOW + MS
    );
    let (_, response) = harness.get(&path).await;
    let metadata = &response["data"]["result"][0]["values"][0][2];

    // The body is valid JSON, and nothing asked for it to be parsed.
    assert!(metadata.get("level_extracted").is_none(), "{metadata}");
    assert_eq!(metadata["order_id"], "1", "attributes are unaffected");
}

// ---------------------------------------------------------------------------
// range semantics
// ---------------------------------------------------------------------------

/// Both ends of a time range include a record landing exactly on them.
///
/// Matches Prometheus, differs from Loki where `end` is exclusive, and is written down
/// in COMPATIBILITY.md because the difference is exactly one record and it is invisible
/// until someone pages two adjacent windows and sees it twice. Pinned here so the
/// documented answer and the real one cannot drift apart.
#[tokio::test]
async fn a_range_includes_records_sitting_on_either_edge() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[
            (0, "info", "first", "1"),
            (1, "info", "middle", "2"),
            (2, "info", "last", "3"),
        ]))
        .await;

    let bodies = |response: &Value| -> Vec<String> {
        response["data"]["result"]
            .as_array()
            .map(|streams| {
                streams
                    .iter()
                    .flat_map(|s| s["values"].as_array().unwrap().iter())
                    .map(|v| v[1].as_str().unwrap().to_owned())
                    .collect()
            })
            .unwrap_or_default()
    };
    let at = |start: u64, end: u64| {
        format!(
            "/loki/api/v1/query_range?query={}&start={}&end={}&direction=forward",
            urlencode(r#"{app="checkout"}"#),
            start,
            end
        )
    };

    // A zero-width window on the first record returns that record and nothing else.
    let (_, only_first) = harness.get(&at(NOW, NOW)).await;
    assert_eq!(bodies(&only_first), vec!["first"]);

    // …and on the last.
    let (_, only_last) = harness.get(&at(NOW + 2 * MS, NOW + 2 * MS)).await;
    assert_eq!(bodies(&only_last), vec!["last"]);

    // Two adjacent windows meeting at the middle record each return it: that is the one
    // record of overlap, and the reason to advance `start` past what you last saw.
    let (_, left) = harness.get(&at(NOW, NOW + MS)).await;
    let (_, right) = harness.get(&at(NOW + MS, NOW + 2 * MS)).await;
    assert_eq!(bodies(&left), vec!["first", "middle"]);
    assert_eq!(bodies(&right), vec!["middle", "last"]);

    // Nothing outside the data.
    let (_, before) = harness.get(&at(NOW - 2 * MS, NOW - MS)).await;
    assert!(bodies(&before).is_empty());
}

/// `direction=backward` with a limit returns the *newest* N, not the oldest N reversed.
///
/// The failure this guards is silent and plausible-looking: a dashboard showing the
/// oldest entries of a range while claiming to show the latest. Checked across sealed
/// segments and the live buffer at once, because the two are read by different code and
/// only their combination is what a real query sees.
#[tokio::test]
async fn a_limited_query_takes_the_end_the_direction_asked_for() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[
            (0, "info", "oldest", "1"),
            (1, "info", "second", "2"),
            (2, "info", "third", "3"),
        ]))
        .await;
    harness.seal();
    harness
        .post_logs(&otlp_logs(&[
            (3, "info", "fourth", "4"),
            (4, "info", "newest", "5"),
        ]))
        .await;

    let first_body = |response: &Value| -> String {
        response["data"]["result"][0]["values"][0][1]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let path = |direction: &str| {
        format!(
            "/loki/api/v1/query_range?query={}&start={}&end={}&limit=1&direction={direction}",
            urlencode(r#"{app="checkout"}"#),
            NOW,
            NOW + 10 * MS
        )
    };

    let (_, backward) = harness.get(&path("backward")).await;
    let (_, forward) = harness.get(&path("forward")).await;
    assert_eq!(
        first_body(&backward),
        "newest",
        "backward must reach past the sealed segment into the live buffer"
    );
    assert_eq!(first_body(&forward), "oldest");
}

// ---------------------------------------------------------------------------
// limits and partial success
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_bad_record_does_not_cost_the_batch() {
    let harness = Harness::configured(|config| {
        config.limits.max_attrs_per_record = 1;
    });

    let payload = json!({
        "resourceLogs": [{
            "resource": {"attributes": [{"key":"service.name","value":{"stringValue":"checkout"}}]},
            "scopeLogs": [{"logRecords": [
                {"timeUnixNano": NOW.to_string(), "body": {"stringValue": "good"}},
                {"timeUnixNano": NOW.to_string(), "body": {"stringValue": "bad"},
                 "attributes": [
                     {"key":"a","value":{"stringValue":"1"}},
                     {"key":"b","value":{"stringValue":"2"}}
                 ]},
                {"timeUnixNano": NOW.to_string(), "body": {"stringValue": "also good"}}
            ]}]
        }]
    });

    let (status, body) = harness.post_logs(&payload).await;
    assert_eq!(status, StatusCode::OK, "a partial batch still succeeds");
    assert_eq!(body["partialSuccess"]["rejectedLogRecords"], "1");
    assert!(
        body["partialSuccess"]["errorMessage"]
            .as_str()
            .unwrap()
            .contains("too_many_attributes"),
        "{body}"
    );

    let (_, response) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;
    let count: usize = response["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["values"].as_array().unwrap().len())
        .sum();
    assert_eq!(count, 2, "the two good records must still be stored");
}

#[tokio::test]
async fn rejections_and_acceptances_are_counted_in_metrics() {
    let harness = Harness::configured(|config| {
        config.limits.max_attrs_per_record = 1;
    });

    let payload = json!({
        "resourceLogs": [{"scopeLogs": [{"logRecords": [
            {"timeUnixNano": NOW.to_string(), "body": {"stringValue": "ok"}},
            {"timeUnixNano": NOW.to_string(), "body": {"stringValue": "bad"},
             "attributes": [
                 {"key":"a","value":{"stringValue":"1"}},
                 {"key":"b","value":{"stringValue":"2"}}
             ]}
        ]}]}]
    });
    harness.post_logs(&payload).await;

    let (_, metrics) = harness.get("/metrics").await;
    let text = metrics.as_str().unwrap();

    assert!(
        text.contains(
            r#"telemetryd_ingest_rejected_total{reason="too_many_attributes",signal="logs"} 1"#
        ),
        "rejections must be counted, not just returned: {text}"
    );
    assert!(
        text.contains(r#"telemetryd_ingest_accepted_total{signal="logs"} 1"#),
        "{text}"
    );
}

#[tokio::test]
async fn a_timestamp_in_the_wrong_unit_is_corrected_and_counted() {
    let harness = Harness::new();
    let payload = json!({
        "resourceLogs": [{
            "resource": {"attributes": [{"key":"service.name","value":{"stringValue":"checkout"}}]},
            "scopeLogs": [{"logRecords": [
                // Seconds instead of nanoseconds — the classic integration bug.
                {"timeUnixNano": "1750000000", "body": {"stringValue": "rescaled"}}
            ]}]
        }]
    });
    harness.post_logs(&payload).await;

    let (_, response) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;
    let ts: u64 = response["data"]["result"][0]["values"][0][0]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(ts, NOW, "the record must land in the present, not in 1970");

    let (_, metrics) = harness.get("/metrics").await;
    assert!(
        metrics
            .as_str()
            .unwrap()
            .contains(r#"telemetryd_ingest_timestamps_rescaled_total{signal="logs"} 1"#),
        "a producer bug must stay visible"
    );
}

#[tokio::test]
async fn an_oversized_body_is_truncated_visibly_rather_than_dropped() {
    let harness = Harness::configured(|config| {
        config.limits.max_log_line_bytes = bytesize::ByteSize::b(128);
    });

    let payload = json!({
        "resourceLogs": [{
            "resource": {"attributes": [{"key":"service.name","value":{"stringValue":"checkout"}}]},
            "scopeLogs": [{"logRecords": [
                {"timeUnixNano": NOW.to_string(), "body": {"stringValue": "x".repeat(5000)}}
            ]}]
        }]
    });
    let (status, _) = harness.post_logs(&payload).await;
    assert_eq!(status, StatusCode::OK);

    let (_, response) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;
    let line = response["data"]["result"][0]["values"][0][1]
        .as_str()
        .unwrap();

    assert!(line.len() <= 128);
    assert!(
        line.contains("truncated"),
        "truncation must be visible in the line"
    );
}

// ---------------------------------------------------------------------------
// auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ingest_and_query_tokens_guard_their_own_surfaces() {
    let harness = Harness::configured(|config| {
        config.auth.ingest_token = serde_json::from_str(r#""ingest-secret""#).unwrap();
        config.auth.query_token = serde_json::from_str(r#""query-secret""#).unwrap();
    });

    // No token: both refused.
    let (status, _) = harness
        .post_logs(&otlp_logs(&[(0, "info", "x", "1")]))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Correct tokens: both work, end to end.
    let request = Request::post("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, "Bearer ingest-secret")
        .body(Body::from(
            otlp_logs(&[(0, "info", "authorised", "1")]).to_string(),
        ))
        .unwrap();
    let (status, _) = harness.send(request).await;
    assert_eq!(status, StatusCode::OK);

    let request = Request::get(range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .header(header::AUTHORIZATION, "Bearer query-secret")
        .body(Body::empty())
        .unwrap();
    let (status, body) = harness.send(request).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("authorised"));
}

// ---------------------------------------------------------------------------
// durability across a restart, through the API
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ingested_logs_survive_a_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");

    let make = || {
        let config = Config {
            storage: StorageConfig {
                data_dir: Some(data_dir.clone()),
                ..StorageConfig::default()
            },
            ..Config::default()
        };
        let store = Arc::new(Store::open(&config).unwrap());
        let state = AppState::new(Arc::new(config), store).unwrap();
        (router(state.clone()), state)
    };

    {
        let (app, state) = make();
        let request = Request::post("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                otlp_logs(&[(0, "error", "persisted", "1")]).to_string(),
            ))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        state.store.sync_all().unwrap();
    }

    let (app, _state) = make();
    let request = Request::get(range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(
        json["data"]["result"][0]["values"][0][1], "persisted",
        "a record accepted over HTTP must survive a restart"
    );
}

// ---------------------------------------------------------------------------
// UI compatibility, verified against what laravel-telemetry-ui actually sends
// ---------------------------------------------------------------------------

#[tokio::test]
async fn record_attributes_are_returned_as_structured_metadata() {
    // The UI reads a third element from each `values` tuple and merges it over the
    // stream labels. Without it, order.id / trace_id are invisible in the UI even
    // though telemetryd stored them.
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[(0, "error", "payment declined", "1002")]))
        .await;

    let (_, response) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;

    let entry = &response["data"]["result"][0]["values"][0];
    let tuple = entry.as_array().unwrap();
    assert_eq!(tuple.len(), 3, "expected structured metadata, got {entry}");
    // Sanitised on the way out, because a Loki structured-metadata key must be a valid
    // label name and a label name cannot contain a dot. The client reads
    // `labels['order_id']`; the dotted form is a key nothing on this API can address.
    // Trace attributes are the other convention and stay dotted — see the Tempo tests.
    assert_eq!(tuple[2]["order_id"], "1002");
    assert!(
        tuple[2].get("order.id").is_none(),
        "the dotted spelling must not also be emitted: {}",
        tuple[2]
    );

    // Stream labels stay in `stream`, not duplicated into the metadata.
    assert!(tuple[2].get("app").is_none());
}

#[tokio::test]
async fn an_entry_without_attributes_stays_a_two_element_tuple() {
    // Loki omits the element entirely rather than sending {}, and a client that
    // checks `count($value) < 2` should not see a surprise shape.
    let harness = Harness::new();
    let payload = json!({
        "resourceLogs": [{
            "resource": {"attributes": [{"key":"service.name","value":{"stringValue":"checkout"}}]},
            "scopeLogs": [{"logRecords": [
                {"timeUnixNano": NOW.to_string(), "body": {"stringValue": "no attributes"}}
            ]}]
        }]
    });
    harness.post_logs(&payload).await;

    let (_, response) = harness
        .get(&range("/loki/api/v1/query_range", r#"{app="checkout"}"#))
        .await;
    let tuple = response["data"]["result"][0]["values"][0]
        .as_array()
        .unwrap();
    assert_eq!(tuple.len(), 2);
}

#[tokio::test]
async fn the_uis_default_selector_returns_data() {
    // LogqlCompiler emits {service_name=~".+"} when no stream matcher is given.
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[(0, "info", "hello", "1")]))
        .await;

    let (status, response) = harness
        .get(&range(
            "/loki/api/v1/query_range",
            r#"{service_name=~".+"}"#,
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["data"]["result"][0]["values"][0][1], "hello");
}

#[tokio::test]
async fn label_filters_with_or_work_over_the_wire() {
    let harness = Harness::new();
    harness
        .post_logs(&otlp_logs(&[
            (0, "info", "a", "1001"),
            (1, "info", "b", "1002"),
            (2, "info", "c", "1003"),
        ]))
        .await;

    let (status, response) = harness
        .get(&range(
            "/loki/api/v1/query_range",
            r#"{app="checkout"} | order_id="1001" or order_id="1003""#,
        ))
        .await;
    assert_eq!(status, StatusCode::OK);

    let mut bodies: Vec<&str> = response["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["values"].as_array().unwrap())
        .map(|v| v[1].as_str().unwrap())
        .collect();
    bodies.sort_unstable();
    assert_eq!(bodies, vec!["a", "c"]);
}

#[tokio::test]
async fn the_probe_the_ui_uses_to_recognise_a_log_backend_succeeds() {
    // LokiSource::probe() GETs /loki/api/v1/labels with no parameters and requires
    // {"status":"success"}. A bare 200 with some other JSON is treated as "not Loki".
    let harness = Harness::new();
    let (status, response) = harness.get("/loki/api/v1/labels").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["status"], "success");
    assert!(response["data"].is_array());
}
