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

    // Protobuf's JSON, as Tempo writes it: ids in base64, kind and status by name.
    let root = &spans[0];
    assert_eq!(root["traceId"], "S/kvNXezTaajzpKdDg5HNg==");
    assert_eq!(root["spanId"], "APBnqgupArc=");
    assert_eq!(root["name"], "POST /checkout");
    assert_eq!(root["kind"], "SPAN_KIND_SERVER");
    assert_eq!(root["status"]["code"], "STATUS_CODE_ERROR");
    assert_eq!(root["status"]["message"], "payment declined");
    // Nanosecond timestamps as strings, as in OTLP.
    assert_eq!(root["startTimeUnixNano"], NOW_NANOS.to_string());
    assert!(
        root.get("parentSpanId").is_none(),
        "a root span has no parent"
    );

    let child = &spans[1];
    assert_eq!(child["parentSpanId"], "APBnqgupArc=");
    assert_eq!(child["kind"], "SPAN_KIND_CLIENT");

    // Every spelling of the id a client might hand back finds the trace: the hex it
    // sent, Tempo's search spelling without leading zeros, and base64.
    for id in [TRACE, "S/kvNXezTaajzpKdDg5HNg=="] {
        let (status, _) = harness.get(&format!("/api/traces/{}", urlencode(id))).await;
        assert_eq!(status, StatusCode::OK, "{id}");
    }
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
    let query = urlencode("{ .a = 1 } || { .b = 2 }");
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

    // Every name as OTLP spelled it, resource and span alike, as Tempo lists them.
    for expected in [
        "service.name",
        "deployment.environment",
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

/// A search row describes the trace, not the part of it the query matched. Matching
/// the database span used to name the row after it and time it at its 10 ms.
#[tokio::test]
async fn a_search_row_describes_the_whole_trace() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;

    let query = urlencode(r#"{ span.db.system = "mysql" }"#);
    let (status, response) = harness
        .get(&format!("/api/search?q={query}&{}", window()))
        .await;
    assert_eq!(status, StatusCode::OK);
    let summary = &response["traces"][0];
    assert_eq!(summary["rootTraceName"], "POST /checkout", "{response}");
    assert!((summary["durationMs"].as_f64().unwrap() - 150.0).abs() < 1.0);
    // The span set is still what matched.
    assert_eq!(summary["spanSets"][0]["matched"], 1);
    assert_eq!(summary["spanSets"][0]["spans"][0]["name"], "SELECT orders");
}

/// `minDuration` and `maxDuration` bound the trace, as in Tempo — here 200 ms made of two
/// 10 ms spans, which a per-span bound got backwards both ways.
#[tokio::test]
async fn search_duration_bounds_apply_to_the_trace() {
    let harness = Harness::new();
    let span = |id: &str, parent: Option<&str>, from: u64, to: u64| {
        let mut span = json!({
            "traceId": TRACE,
            "spanId": id,
            "name": id,
            "startTimeUnixNano": (NOW_NANOS + from * MS).to_string(),
            "endTimeUnixNano": (NOW_NANOS + to * MS).to_string(),
        });
        if let Some(parent) = parent {
            span["parentSpanId"] = json!(parent);
        }
        span
    };
    harness
        .post_traces(&json!({"resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeSpans": [{"spans": [
                span(ROOT_SPAN, None, 0, 10),
                span("aaaaaaaaaaaaaaaa", Some(ROOT_SPAN), 190, 200),
            ]}]
        }]}))
        .await;

    let found = |bounds: &str| {
        // No `q`: a search by tags, the one Tempo applies the bounds to.
        let path = format!("/api/search?{}&{bounds}", window());
        let harness = &harness;
        async move {
            let (status, response) = harness.get(&path).await;
            assert_eq!(status, StatusCode::OK, "{response}");
            response["traces"].as_array().unwrap().len()
        }
    };
    assert_eq!(found("minDuration=100ms").await, 1);
    assert_eq!(found("maxDuration=50ms").await, 0);
    assert_eq!(found("minDuration=150ms&maxDuration=250ms").await, 1);

    // With a TraceQL `q`, Tempo does not read them at all: the bound goes in the query.
    let path = format!(
        "/api/search?q={}&{}&maxDuration=50ms",
        urlencode("{}"),
        window()
    );
    let (_, response) = harness.get(&path).await;
    assert_eq!(
        response["traces"].as_array().unwrap().len(),
        1,
        "{response}"
    );
}

/// An exporter that retries a batch it was not sure had landed sends every span twice.
/// Each is one span, in the search and in the trace.
#[tokio::test]
async fn a_span_sent_twice_is_one_span() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;
    harness.post_traces(&trace_payload()).await;

    let (_, response) = harness
        .get(&format!("/api/search?q={}&{}", urlencode("{}"), window()))
        .await;
    assert_eq!(
        response["traces"][0]["spanSets"][0]["matched"], 2,
        "{response}"
    );

    let (status, response) = harness.get(&format!("/api/traces/{TRACE}")).await;
    assert_eq!(status, StatusCode::OK);
    let spans: usize = response["batches"]
        .as_array()
        .unwrap()
        .iter()
        .map(|batch| batch["scopeSpans"][0]["spans"].as_array().unwrap().len())
        .sum();
    assert_eq!(spans, 2);
}

/// Every resource attribute is reachable as `resource.X` and shown on the resource, not
/// only the few promoted to stream labels — and never confused with a span's own
/// attribute of the same name.
#[tokio::test]
async fn resource_attributes_beyond_the_stream_are_the_resource_s() {
    let harness = Harness::new();
    let mut payload = trace_payload();
    payload["resourceSpans"][0]["resource"]["attributes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"key": "k8s.pod.name", "value": {"stringValue": "checkout-7f9"}}));
    payload["resourceSpans"][0]["scopeSpans"][0]["spans"][1]["attributes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"key": "k8s.pod.name", "value": {"stringValue": "span-own"}}));
    let (status, _) = harness.post_traces(&payload).await;
    assert_eq!(status, StatusCode::OK);

    let count = |response: &Value| response["traces"].as_array().unwrap().len();
    for (query, expected) in [
        (r#"{ resource.k8s.pod.name = "checkout-7f9" }"#, 1),
        (r#"{ resource.k8s.pod.name = "span-own" }"#, 0),
        (r#"{ span.k8s.pod.name = "span-own" }"#, 1),
        (r#"{ span.k8s.pod.name = "checkout-7f9" }"#, 0),
        (r#"{ .k8s.pod.name = "checkout-7f9" }"#, 1),
    ] {
        let (_, response) = harness
            .get(&format!("/api/search?q={}&{}", urlencode(query), window()))
            .await;
        assert_eq!(count(&response), expected, "{query}: {response}");
    }

    let (_, response) = harness.get(&format!("/api/traces/{TRACE}")).await;
    let batch = &response["batches"][0];
    let resource = batch["resource"]["attributes"].as_array().unwrap();
    assert!(
        resource
            .iter()
            .any(|kv| kv["key"] == "k8s.pod.name" && kv["value"]["stringValue"] == "checkout-7f9"),
        "{resource:?}"
    );
    let spans = batch["scopeSpans"][0]["spans"].as_array().unwrap();
    for span in spans {
        let keys: Vec<&str> = span["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|kv| kv["key"].as_str().unwrap())
            .collect();
        assert!(
            keys.iter().all(|k| !k.starts_with("resource.")),
            "a resource attribute shown as the span's: {keys:?}"
        );
    }
    let child = spans.iter().find(|s| s["name"] == "SELECT orders").unwrap();
    assert!(
        child["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|kv| kv["key"] == "k8s.pod.name" && kv["value"]["stringValue"] == "span-own")
    );

    // Tag listing and values put it under the resource scope.
    let (_, tags) = harness
        .get(&format!("/api/v2/search/tags?scope=resource&{}", window()))
        .await;
    assert!(tags.to_string().contains("k8s.pod.name"), "{tags}");
    let (_, values) = harness
        .get(&format!(
            "/api/v2/search/tag/resource.k8s.pod.name/values?{}",
            window()
        ))
        .await;
    assert_eq!(values["tagValues"][0]["value"], "checkout-7f9", "{values}");
}

/// TraceQL metrics, in Tempo's response shape: Grafana's metrics queries and the
/// Traces Drilldown read `series[].labels`, `samples[].timestampMs` and `value`.
#[tokio::test]
async fn traceql_metrics_answer_in_tempo_s_shape() {
    let harness = Harness::new();
    harness.post_traces(&trace_payload()).await;
    let range = |q: &str| {
        format!(
            "/api/metrics/query_range?q={}&start={}&end={}&step=60s",
            urlencode(q),
            NOW_SECONDS - 60,
            NOW_SECONDS + 60
        )
    };

    let (status, body) = harness
        .get(&range("{} | count_over_time() by (name)"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let series = body["series"].as_array().unwrap();
    assert_eq!(series.len(), 2, "{body}");
    let checkout = series
        .iter()
        .find(|s| s["labels"][0]["value"]["stringValue"] == "POST /checkout")
        .unwrap();
    assert_eq!(checkout["labels"][0]["key"], "name");
    // Tempo's grid: from the start rounded down to a whole minute to the end rounded
    // up, each point counting the minute before it — so the span at NOW counts at the
    // next whole minute, and the other points are counted zeros.
    let samples = checkout["samples"].as_array().unwrap();
    let points: Vec<(String, f64)> = samples
        .iter()
        .map(|p| {
            (
                p["timestampMs"].as_str().unwrap().to_owned(),
                p["value"].as_f64().unwrap(),
            )
        })
        .collect();
    let next_minute = NOW_SECONDS.div_ceil(60) * 60;
    assert_eq!(
        points,
        vec![
            (((next_minute - 120) * 1000).to_string(), 0.0),
            (((next_minute - 60) * 1000).to_string(), 0.0),
            ((next_minute * 1000).to_string(), 1.0),
            (((next_minute + 60) * 1000).to_string(), 0.0),
        ],
        "{body}"
    );

    let (_, body) = harness.get(&range("{ status = error } | rate()")).await;
    // With no `by`, Tempo names the series after its function.
    assert_eq!(body["series"][0]["labels"][0]["key"], "__name__", "{body}");
    assert_eq!(
        body["series"][0]["labels"][0]["value"]["stringValue"],
        "rate"
    );
    let samples = body["series"][0]["samples"].as_array().unwrap();
    assert!(
        (samples[2]["value"].as_f64().unwrap() - 1.0 / 60.0).abs() < 1e-12,
        "{body}"
    );

    let (_, body) = harness
        .get(&range(
            "{} | quantile_over_time(duration, .5, 1) by (resource.service.name)",
        ))
        .await;
    let series = body["series"].as_array().unwrap();
    assert_eq!(series.len(), 2, "one series per quantile: {body}");
    let max = series
        .iter()
        .find(|s| s["labels"][1]["value"]["doubleValue"] == 1.0)
        .unwrap();
    assert_eq!(max["labels"][0]["key"], "resource.service.name");
    assert!(
        (max["samples"][0]["value"].as_f64().unwrap() - 0.15).abs() < 1e-9,
        "{body}"
    );

    let (_, body) = harness
        .get(&range("{} | histogram_over_time(duration)"))
        .await;
    assert!(body.to_string().contains("__bucket"), "{body}");

    let (status, body) = harness
        .get(&format!(
            "/api/metrics/query?q={}&start={}&end={}",
            urlencode("{} | max_over_time(duration)"),
            NOW_SECONDS - 60,
            NOW_SECONDS + 60
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        (body["series"][0]["value"].as_f64().unwrap() - 0.15).abs() < 1e-9,
        "{body}"
    );

    // A metrics query is not a search, and a search is not a metrics query.
    let (status, _) = harness
        .get(&format!(
            "/api/search?q={}&{}",
            urlencode("{} | rate()"),
            window()
        ))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = harness.get(&range("{ status = error }")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// With the metrics generator on, spans become the series Grafana's service graph and
/// RED table read, under Tempo's names — answered by the Prometheus API beside them.
#[tokio::test]
async fn spans_become_service_graph_and_span_metrics() {
    let harness = Harness::configured(|config| {
        config.metrics_generator.processors = vec!["service-graphs".into(), "span-metrics".into()];
        config.metrics_generator.wait = std::time::Duration::ZERO;
    });
    harness.post_traces(&trace_payload()).await;
    // Settle the waiting halves, then write: twice, as two intervals would.
    harness.state.flush_generator().unwrap();
    assert!(harness.state.flush_generator().unwrap() > 0);

    let ask = |query: &str| {
        let path = format!("/api/v1/query?query={}", urlencode(query));
        let harness = &harness;
        async move {
            let (status, body) = harness.get(&path).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let mut rows: Vec<(Value, String)> = body["data"]["result"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r["metric"].clone(),
                        r["value"][1].as_str().unwrap().to_owned(),
                    )
                })
                .collect();
            rows.sort_by_key(|(m, _)| m.to_string());
            rows
        }
    };

    // The checkout's root span is called by `user`; its SELECT goes to a database no
    // span reports from, which is a virtual node named by `db.system`.
    assert_eq!(
        ask("sum by (client, server, connection_type) (traces_service_graph_request_total)").await,
        vec![
            (
                json!({"client": "checkout", "connection_type": "database", "server": "mysql"}),
                "1".to_owned()
            ),
            (
                json!({"client": "user", "connection_type": "virtual_node", "server": "checkout"}),
                "1".to_owned()
            ),
        ]
    );
    assert_eq!(
        ask("sum by (client, server) (traces_service_graph_request_failed_total)").await,
        vec![
            (
                json!({"client": "checkout", "server": "mysql"}),
                "0".to_owned()
            ),
            (
                json!({"client": "user", "server": "checkout"}),
                "1".to_owned()
            ),
        ]
    );
    assert_eq!(
        ask(r#"sum by (span_name, status_code) (traces_spanmetrics_calls_total{service="checkout"})"#)
            .await,
        vec![
            (
                json!({"span_name": "POST /checkout", "status_code": "STATUS_CODE_ERROR"}),
                "1".to_owned()
            ),
            (
                json!({"span_name": "SELECT orders", "status_code": "STATUS_CODE_UNSET"}),
                "1".to_owned()
            ),
        ]
    );
    // The latency histogram answers `histogram_quantile`, as a RED panel asks it.
    let p50 = ask(
        r#"histogram_quantile(0.5, sum by (le) (traces_spanmetrics_latency_bucket{span_name="POST /checkout"}))"#,
    )
    .await;
    let p50: f64 = p50[0].1.parse().unwrap();
    assert!(
        p50 > 0.128 && p50 <= 0.256,
        "150 ms lands in the 128–256 ms bucket: {p50}"
    );
}
