//! The M2 acceptance test: OTLP/HTTP JSON traces in, Tempo query API out.
//!
//! Endpoint shapes and parameter units come from `TempoSource` in
//! `laravel-telemetry-ui`, not from the upstream docs — notably `q` carrying
//! TraceQL, and tag values on the v2 path.

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

/// 2025-06-15T15:06:40Z.
const NOW_NANOS: u64 = 1_750_000_000_000_000_000;
const NOW_SECONDS: u64 = 1_750_000_000;
const MS: u64 = 1_000_000;

const TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const ROOT_SPAN: &str = "00f067aa0ba902b7";

struct Harness {
    router: axum::Router,
    state: AppState,
    _tmp: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            storage: StorageConfig {
                data_dir: Some(tmp.path().join("data")),
                ..StorageConfig::default()
            },
            ..Config::default()
        };
        config.validate().unwrap();

        let store = Arc::new(Store::open(&config).unwrap());
        let state = AppState::new(Arc::new(config), store).unwrap();
        Self {
            router: router(state.clone()),
            state,
            _tmp: tmp,
        }
    }

    async fn post_traces(&self, payload: &Value) -> (StatusCode, Value) {
        let request = Request::post("/v1/traces")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        let (status, body) = self.send(request).await;
        (status, serde_json::from_str(&body).unwrap_or(Value::Null))
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let request = Request::get(path).body(Body::empty()).unwrap();
        let (status, body) = self.send(request).await;
        (status, serde_json::from_str(&body).unwrap_or(Value::Null))
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

/// Tempo's start/end are **seconds**, unlike Loki's nanoseconds.
fn window() -> String {
    format!("start={}&end={}", NOW_SECONDS - 3600, NOW_SECONDS + 3600)
}

/// A two-span trace shaped like what `cboxdk/laravel-telemetry` emits.
fn trace_payload() -> Value {
    json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "deployment.environment", "value": {"stringValue": "production"}}
            ]},
            "scopeSpans": [{
                "scope": {"name": "laravel-telemetry"},
                "spans": [
                    {
                        "traceId": TRACE,
                        "spanId": ROOT_SPAN,
                        "name": "POST /checkout",
                        "kind": 2,
                        "startTimeUnixNano": NOW_NANOS.to_string(),
                        "endTimeUnixNano": (NOW_NANOS + 150 * MS).to_string(),
                        "attributes": [
                            {"key": "http.method", "value": {"stringValue": "POST"}},
                            {"key": "http.status_code", "value": {"intValue": "500"}}
                        ],
                        "status": {"code": 2, "message": "payment declined"},
                        "events": [{
                            "timeUnixNano": (NOW_NANOS + 100 * MS).to_string(),
                            "name": "exception",
                            "attributes": [
                                {"key": "exception.type", "value": {"stringValue": "PaymentError"}}
                            ]
                        }]
                    },
                    {
                        "traceId": TRACE,
                        "spanId": "aaaaaaaaaaaaaaaa",
                        "parentSpanId": ROOT_SPAN,
                        "name": "SELECT orders",
                        "kind": 3,
                        "startTimeUnixNano": (NOW_NANOS + 20 * MS).to_string(),
                        "endTimeUnixNano": (NOW_NANOS + 30 * MS).to_string(),
                        "attributes": [
                            {"key": "db.system", "value": {"stringValue": "mysql"}}
                        ]
                    }
                ]
            }]
        }]
    })
}

// ---------------------------------------------------------------------------
// the round trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn otlp_traces_in_tempo_trace_out() {
    let harness = Harness::new();
    let (status, body) = harness.post_traces(&trace_payload()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({}), "a clean batch reports no partial success");

    let (status, response) = harness.get(&format!("/api/traces/{TRACE}")).await;
    assert_eq!(status, StatusCode::OK);

    // The UI reads `batches` (or `resourceSpans`) in OTLP shape.
    let batches = response["batches"].as_array().unwrap();
    assert_eq!(batches.len(), 1, "one resource, one batch");

    // service.name is restored with its dot, which is the key the UI looks up.
    let attrs = batches[0]["resource"]["attributes"].as_array().unwrap();
    let service = attrs
        .iter()
        .find(|kv| kv["key"] == "service.name")
        .expect("resource must carry service.name");
    assert_eq!(service["value"]["stringValue"], "checkout");

    let spans = batches[0]["scopeSpans"][0]["spans"].as_array().unwrap();
    assert_eq!(spans.len(), 2);

    let root = &spans[0];
    assert_eq!(root["traceId"], TRACE);
    assert_eq!(root["spanId"], ROOT_SPAN);
    assert_eq!(root["name"], "POST /checkout");
    assert_eq!(root["kind"], 2);
    assert_eq!(root["status"]["code"], 2);
    assert_eq!(root["status"]["message"], "payment declined");
    // Nanosecond timestamps as strings, as in OTLP.
    assert_eq!(root["startTimeUnixNano"], NOW_NANOS.to_string());
    assert!(
        root.get("parentSpanId").is_none(),
        "a root span has no parent"
    );

    let child = &spans[1];
    assert_eq!(child["parentSpanId"], ROOT_SPAN);
    assert_eq!(child["kind"], 3);
}

#[tokio::test]
async fn span_events_survive_the_round_trip() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let (_, response) = harness.get(&format!("/api/traces/{TRACE}")).await;
    let events = response["batches"][0]["scopeSpans"][0]["spans"][0]["events"]
        .as_array()
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["name"], "exception");
    let attrs = events[0]["attributes"].as_array().unwrap();
    assert!(attrs.iter().any(|kv| kv["key"] == "exception.type"));
}

#[tokio::test]
async fn a_trace_is_found_before_and_after_sealing() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let (status, _) = harness.get(&format!("/api/traces/{TRACE}")).await;
    assert_eq!(status, StatusCode::OK, "queryable before it is sealed");

    harness.seal();

    let (status, response) = harness.get(&format!("/api/traces/{TRACE}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        response["batches"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "sealing must not change what a lookup returns"
    );
}

#[tokio::test]
async fn an_unknown_trace_is_a_404_not_an_empty_trace() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let (status, _) = harness
        .get("/api/traces/ffffffffffffffffffffffffffffffff")
        .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "an empty trace view reads as a broken UI; 404 reads as 'not found'"
    );
}

// ---------------------------------------------------------------------------
// search
// ---------------------------------------------------------------------------

#[tokio::test]
async fn search_returns_the_summary_shape_the_ui_reads() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let (status, response) = harness
        .get(&format!("/api/search?q={}&{}", urlencode("{}"), window()))
        .await;
    assert_eq!(status, StatusCode::OK);

    let traces = response["traces"].as_array().unwrap();
    assert_eq!(traces.len(), 1);

    let summary = &traces[0];
    assert_eq!(summary["traceID"], TRACE);
    assert_eq!(summary["rootServiceName"], "checkout");
    assert_eq!(summary["rootTraceName"], "POST /checkout");
    assert!(summary["startTimeUnixNano"].is_string());
    // The trace spans 0..150ms.
    assert!((summary["durationMs"].as_f64().unwrap() - 150.0).abs() < 1.0);
    assert!(summary["spanSets"][0]["spans"].is_array());
}

#[tokio::test]
async fn traceql_conditions_filter_the_search() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    // Exactly the form TraceqlCompiler emits.
    let hit = urlencode(r#"{ resource.service.name = "checkout" && status = error }"#);
    let (status, response) = harness
        .get(&format!("/api/search?q={hit}&{}", window()))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["traces"].as_array().unwrap().len(), 1);

    let miss = urlencode(r#"{ resource.service.name = "billing" }"#);
    let (_, response) = harness
        .get(&format!("/api/search?q={miss}&{}", window()))
        .await;
    assert!(response["traces"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn traceql_can_filter_on_duration_and_span_attributes() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    for (query, expected) in [
        ("{ duration > 100ms }", 1),
        ("{ duration > 500ms }", 0),
        ("{ span.http.status_code = 500 }", 1),
        ("{ span.http.status_code > 499 }", 1),
        (r#"{ span.db.system = "mysql" }"#, 1),
        ("{ kind = client }", 1),
        (r#"{ name =~ "POST.*" }"#, 1),
    ] {
        let (status, response) = harness
            .get(&format!("/api/search?q={}&{}", urlencode(query), window()))
            .await;
        assert_eq!(status, StatusCode::OK, "{query}");
        assert_eq!(
            response["traces"].as_array().unwrap().len(),
            expected,
            "{query}"
        );
    }
}

#[tokio::test]
async fn the_select_projection_is_accepted() {
    // telemetryd always returns matched spans in full, but refusing the clause would
    // break a query the UI legitimately sends.
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let query = urlencode("{ status = error } | select(span.http.method)");
    let (status, response) = harness
        .get(&format!("/api/search?q={query}&{}", window()))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["traces"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn an_unsupported_traceql_feature_is_named_with_a_400() {
    let harness = Harness::new();
    let query = urlencode("{ a = 1 } || { b = 2 }");
    let (status, response) = harness
        .get(&format!("/api/search?q={query}&{}", window()))
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"]["code"], "unsupported_feature");
    assert!(
        response["error"]["docs"]
            .as_str()
            .unwrap()
            .ends_with("COMPATIBILITY.md")
    );
}

// ---------------------------------------------------------------------------
// tags
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_probe_the_ui_uses_to_recognise_a_trace_backend_succeeds() {
    // TempoSource::probe() GETs /api/search/tags and requires `tagNames` or `scopes`.
    // A bare 200 with other JSON is treated as "not Tempo".
    let harness = Harness::new();
    let (status, response) = harness.get("/api/search/tags").await;

    assert_eq!(status, StatusCode::OK);
    assert!(response["tagNames"].is_array(), "{response}");
}

#[tokio::test]
async fn tags_include_resource_labels_span_attributes_and_intrinsics() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let (_, response) = harness.get(&format!("/api/search/tags?{}", window())).await;
    let names: Vec<&str> = response["tagNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();

    // Resource-derived stream labels are sanitised (they are label names); span
    // attributes keep the producer's spelling (they are data).
    for expected in [
        "app",
        "service_name",
        "http.method",
        "db.system",
        "name",
        "status",
    ] {
        assert!(names.contains(&expected), "missing {expected} in {names:?}");
    }
}

#[tokio::test]
async fn tag_values_use_the_v2_object_shape() {
    // The UI calls the v2 path and reads {type, value} objects.
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let (status, response) = harness
        .get(&format!(
            "/api/v2/search/tag/service_name/values?{}",
            window()
        ))
        .await;
    assert_eq!(status, StatusCode::OK);

    let values = response["tagValues"].as_array().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["type"], "string");
    assert_eq!(values[0]["value"], "checkout");
}

#[tokio::test]
async fn tag_values_accept_scoped_names_and_intrinsics() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    for (tag, expected) in [
        ("resource.service.name", "checkout"),
        ("span.http.method", "POST"),
        // The label-safe spelling reaches the same attribute.
        ("span.http_method", "POST"),
        ("status", "error"),
    ] {
        let (status, response) = harness
            .get(&format!(
                "/api/v2/search/tag/{}/values?{}",
                urlencode(tag),
                window()
            ))
            .await;
        assert_eq!(status, StatusCode::OK, "{tag}");

        let values: Vec<&str> = response["tagValues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["value"].as_str().unwrap())
            .collect();
        assert!(values.contains(&expected), "{tag} -> {values:?}");
    }
}

#[tokio::test]
async fn the_v1_tag_values_path_still_answers() {
    // Not called by the UI, kept for older clients.
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let (status, _) = harness
        .get(&format!("/api/search/tag/service_name/values?{}", window()))
        .await;
    assert_eq!(status, StatusCode::OK);
}

// ---------------------------------------------------------------------------
// robustness
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_span_without_ids_is_rejected_without_costing_the_batch() {
    let harness = Harness::new();
    let payload = json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeSpans": [{"spans": [
                {"name": "no ids at all"},
                {"traceId": TRACE, "spanId": ROOT_SPAN, "name": "fine",
                 "startTimeUnixNano": NOW_NANOS.to_string()}
            ]}]
        }]
    });

    let (status, body) = harness.post_traces(&payload).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["partialSuccess"]["rejectedSpans"], "1");

    let (status, _) = harness.get(&format!("/api/traces/{TRACE}")).await;
    assert_eq!(status, StatusCode::OK, "the good span was still stored");
}

#[tokio::test]
async fn traces_survive_a_restart() {
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
        let request = Request::post("/v1/traces")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(trace_payload().to_string()))
            .unwrap();
        assert_eq!(app.oneshot(request).await.unwrap().status(), StatusCode::OK);
        state.store.sync_all().unwrap();
    }

    let (app, _state) = make();
    let response = app
        .oneshot(
            Request::get(format!("/api/traces/{TRACE}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        json["batches"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn logs_and_traces_coexist_without_interfering() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let logs = json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeLogs": [{"logRecords": [
                {"timeUnixNano": NOW_NANOS.to_string(), "body": {"stringValue": "a log line"}}
            ]}]
        }]
    });
    let request = Request::post("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(logs.to_string()))
        .unwrap();
    assert_eq!(harness.send(request).await.0, StatusCode::OK);

    harness.seal();

    // Each signal still answers its own query, from its own segments.
    let (status, trace) = harness.get(&format!("/api/traces/{TRACE}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        trace["batches"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let path = format!(
        "/loki/api/v1/query_range?query={}&start={}&end={}",
        urlencode(r#"{app="checkout"}"#),
        NOW_NANOS - 3_600_000_000_000,
        NOW_NANOS + 3_600_000_000_000
    );
    let (status, logs) = harness.get(&path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(logs["data"]["result"][0]["values"][0][1], "a log line");
}
