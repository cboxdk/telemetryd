//! The M3 acceptance test: metrics in (both ways), Prometheus query API out.
//!
//! Endpoint shapes, units and the probe order come from `PrometheusSource` in
//! `laravel-telemetry-ui` (ADR-005): seconds not nanoseconds, values as strings, and
//! `/api/v1/status/buildinfo` first.

// Test fixtures build wire payloads from small literal numbers; the casts are exact
// and the float comparisons are the assertion.
#![allow(
    clippy::unwrap_used,
    clippy::float_cmp,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap
)]

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

const NOW_SECONDS: u64 = 1_750_000_000;
const NOW_NANOS: u64 = 1_750_000_000_000_000_000;

struct Harness {
    router: axum::Router,
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
            router: router(state),
            _tmp: tmp,
        }
    }

    async fn post_otlp(&self, payload: &Value) -> (StatusCode, Value) {
        let request = Request::post("/v1/metrics")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        let (status, body) = self.send(request).await;
        (status, serde_json::from_str(&body).unwrap_or(Value::Null))
    }

    async fn post_remote_write(&self, payload: Vec<u8>) -> StatusCode {
        let request = Request::post("/api/v1/write")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header("content-encoding", "snappy")
            .body(Body::from(payload))
            .unwrap();
        self.send(request).await.0
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

/// A counter climbing by 10/sec across a minute.
fn otlp_counter() -> Value {
    let points: Vec<Value> = (0..7)
        .map(|i| {
            json!({
                "timeUnixNano": (NOW_NANOS + i * 10_000_000_000_u64).to_string(),
                "asDouble": (i * 100) as f64,
                "attributes": [{"key": "status", "value": {"stringValue": "200"}}]
            })
        })
        .collect();

    json!({
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeMetrics": [{"metrics": [{
                "name": "http.requests",
                "sum": {"isMonotonic": true, "dataPoints": points}
            }]}]
        }]
    })
}

// -- remote_write encoding ---------------------------------------------------

fn varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

fn key(number: u32, wire: u8) -> Vec<u8> {
    varint(u64::from(number) << 3 | u64::from(wire))
}

fn delimited(number: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = key(number, 2);
    out.extend(varint(payload.len() as u64));
    out.extend_from_slice(payload);
    out
}

fn remote_write_payload(labels: &[(&str, &str)], samples: &[(f64, i64)]) -> Vec<u8> {
    let mut ts = Vec::new();
    for (name, value) in labels {
        let mut label = delimited(1, name.as_bytes());
        label.extend(delimited(2, value.as_bytes()));
        ts.extend(delimited(1, &label));
    }
    for (value, timestamp) in samples {
        let mut sample = key(1, 1);
        sample.extend_from_slice(&value.to_bits().to_le_bytes());
        sample.extend(key(2, 0));
        sample.extend(varint(timestamp.cast_unsigned()));
        ts.extend(delimited(2, &sample));
    }
    let request = delimited(1, &ts);
    snap::raw::Encoder::new().compress_vec(&request).unwrap()
}

// ---------------------------------------------------------------------------
// the round trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn otlp_metrics_in_promql_out() {
    let harness = Harness::new();
    let (status, body) = harness.post_otlp(&otlp_counter()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({}), "a clean batch reports no partial success");

    // The counter climbs 100 every 10s, so the rate is 10/sec.
    let query = urlencode(r#"rate(http_requests{app="checkout"}[60s])"#);
    let (status, response) = harness
        .get(&format!(
            "/api/v1/query?query={query}&time={}",
            NOW_SECONDS + 60
        ))
        .await;
    assert_eq!(status, StatusCode::OK);

    assert_eq!(response["status"], "success");
    assert_eq!(response["data"]["resultType"], "vector");

    let result = response["data"]["result"].as_array().unwrap();
    assert_eq!(result.len(), 1, "{response}");

    let value: f64 = result[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((value - 10.0).abs() < 0.01, "expected ~10/sec, got {value}");

    // Prometheus sends [seconds_as_number, "value_as_string"].
    assert!(result[0]["value"][0].is_number());
    assert!(result[0]["value"][1].is_string());
    assert_eq!(result[0]["metric"]["app"], "checkout");
    assert_eq!(result[0]["metric"]["status"], "200");
}

#[tokio::test]
async fn a_range_query_returns_a_matrix_with_one_point_per_step() {
    let harness = Harness::new();
    harness.post_otlp(&otlp_counter()).await;

    let query = urlencode("http_requests");
    let (status, response) = harness
        .get(&format!(
            "/api/v1/query_range?query={query}&start={}&end={}&step=15",
            NOW_SECONDS,
            NOW_SECONDS + 60
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["data"]["resultType"], "matrix");

    let series = response["data"]["result"].as_array().unwrap();
    assert_eq!(series.len(), 1);

    // 0, 15, 30, 45, 60 seconds.
    let values = series[0]["values"].as_array().unwrap();
    assert_eq!(values.len(), 5, "{values:?}");
    assert!(values[0][0].is_number());
    assert!(values[0][1].is_string());
}

#[tokio::test]
async fn remote_write_ingests_and_is_queryable() {
    let harness = Harness::new();
    let payload = remote_write_payload(
        &[
            ("__name__", "queue_depth"),
            ("app", "checkout"),
            ("queue", "emails"),
        ],
        &[(42.0, NOW_SECONDS as i64 * 1000)],
    );

    // Prometheus expects 204 and retries anything else.
    assert_eq!(
        harness.post_remote_write(payload).await,
        StatusCode::NO_CONTENT
    );

    let query = urlencode(r#"queue_depth{queue="emails"}"#);
    let (status, response) = harness
        .get(&format!("/api/v1/query?query={query}&time={NOW_SECONDS}"))
        .await;
    assert_eq!(status, StatusCode::OK);

    let result = response["data"]["result"].as_array().unwrap();
    assert_eq!(result.len(), 1, "{response}");
    assert_eq!(result[0]["value"][1], "42");
    assert_eq!(result[0]["metric"]["app"], "checkout");
}

#[tokio::test]
async fn aggregations_and_scalar_arithmetic_work_over_the_wire() {
    let harness = Harness::new();
    for (app, value) in [("checkout", 2.0), ("cart", 3.0)] {
        let payload = remote_write_payload(
            &[("__name__", "up"), ("app", app)],
            &[(value, NOW_SECONDS as i64 * 1000)],
        );
        harness.post_remote_write(payload).await;
    }

    let cases = [
        ("sum(up)", "5"),
        ("count(up)", "2"),
        ("max(up)", "3"),
        ("min(up)", "2"),
        ("sum(up) * 10", "50"),
    ];
    for (query, expected) in cases {
        let (status, response) = harness
            .get(&format!(
                "/api/v1/query?query={}&time={NOW_SECONDS}",
                urlencode(query)
            ))
            .await;
        assert_eq!(status, StatusCode::OK, "{query}");
        assert_eq!(
            response["data"]["result"][0]["value"][1], expected,
            "{query}: {response}"
        );
    }
}

#[tokio::test]
async fn histograms_become_buckets_that_histogram_quantile_can_read() {
    let harness = Harness::new();
    let payload = json!({
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeMetrics": [{"metrics": [{
                "name": "http.duration",
                "histogram": {"dataPoints": [{
                    "timeUnixNano": NOW_NANOS.to_string(),
                    "count": "100",
                    "sum": 12.5,
                    "bucketCounts": ["50", "40", "10"],
                    "explicitBounds": [0.1, 0.5]
                }]}
            }]}]
        }]
    });
    assert_eq!(harness.post_otlp(&payload).await.0, StatusCode::OK);

    let query = urlencode("histogram_quantile(0.5, http_duration_bucket)");
    let (status, response) = harness
        .get(&format!("/api/v1/query?query={query}&time={NOW_SECONDS}"))
        .await;
    assert_eq!(status, StatusCode::OK);

    let result = response["data"]["result"].as_array().unwrap();
    assert_eq!(result.len(), 1, "{response}");
    let p50: f64 = result[0]["value"][1].as_str().unwrap().parse().unwrap();
    assert!((p50 - 0.1).abs() < 1e-6, "got {p50}");
    // `le` is dropped from the result, as Prometheus does.
    assert!(result[0]["metric"].get("le").is_none());
}

#[tokio::test]
async fn the_uis_counter_increase_form_works_end_to_end() {
    // clamp_min(sel - (sel offset 5m or sel * 0), 0) — needs offset, vector `or` and
    // clamp_min, all three of which the original plan listed as out of scope.
    let harness = Harness::new();
    let payload = remote_write_payload(
        &[("__name__", "requests"), ("app", "checkout")],
        &[
            (100.0, (NOW_SECONDS as i64 - 300) * 1000),
            (250.0, NOW_SECONDS as i64 * 1000),
        ],
    );
    harness.post_remote_write(payload).await;

    let query = urlencode("clamp_min(requests - (requests offset 5m or requests * 0), 0)");
    let (status, response) = harness
        .get(&format!("/api/v1/query?query={query}&time={NOW_SECONDS}"))
        .await;
    assert_eq!(status, StatusCode::OK);

    let result = response["data"]["result"].as_array().unwrap();
    assert_eq!(result.len(), 1, "{response}");
    assert_eq!(
        result[0]["value"][1], "150",
        "250 now minus 100 five minutes ago"
    );
}

// ---------------------------------------------------------------------------
// metadata
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_probe_the_ui_uses_to_recognise_a_metric_backend_succeeds() {
    // PrometheusSource::probe() calls buildinfo first, then falls back to query=1.
    let harness = Harness::new();

    let (status, response) = harness.get("/api/v1/status/buildinfo").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["status"], "success");
    assert!(response["data"]["version"].is_string());

    let (status, response) = harness.get("/api/v1/query?query=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["status"], "success");
}

#[tokio::test]
async fn labels_and_series_reflect_ingested_metrics() {
    let harness = Harness::new();
    for app in ["checkout", "cart"] {
        let payload = remote_write_payload(
            &[("__name__", "up"), ("app", app)],
            &[(1.0, NOW_SECONDS as i64 * 1000)],
        );
        harness.post_remote_write(payload).await;
    }

    let window = format!("start={}&end={}", NOW_SECONDS - 3600, NOW_SECONDS + 3600);

    let (status, response) = harness.get(&format!("/api/v1/labels?{window}")).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = response["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(names.contains(&"app"), "{names:?}");
    assert!(names.contains(&"__name__"), "{names:?}");

    let (_, response) = harness
        .get(&format!("/api/v1/label/app/values?{window}"))
        .await;
    let values: Vec<&str> = response["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(values, vec!["cart", "checkout"]);

    let (status, response) = harness
        .get(&format!(
            "/api/v1/series?match[]={}&{window}",
            urlencode(r#"up{app="checkout"}"#)
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    let series = response["data"].as_array().unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["app"], "checkout");
}

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unsupported_promql_function_is_named_with_a_400() {
    let harness = Harness::new();
    let (status, response) = harness
        .get(&format!(
            "/api/v1/query?query={}",
            urlencode("predict_linear(up[1h], 3600)")
        ))
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"]["code"], "unsupported_feature");
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("predict_linear"),
        "{response}"
    );
    assert!(
        response["error"]["docs"]
            .as_str()
            .unwrap()
            .ends_with("COMPATIBILITY.md")
    );
}

#[tokio::test]
async fn a_malformed_query_is_a_400_not_a_500() {
    let harness = Harness::new();
    for query in ["up{", "sum by", "((("] {
        let (status, _) = harness
            .get(&format!("/api/v1/query?query={}", urlencode(query)))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
    }
}

#[tokio::test]
async fn an_absurd_step_is_refused_with_a_usable_message() {
    let harness = Harness::new();
    let (status, response) = harness
        .get(&format!(
            "/api/v1/query_range?query=up&start={}&end={}&step=1",
            NOW_SECONDS,
            NOW_SECONDS + 10_000_000
        ))
        .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = response["error"]["message"].as_str().unwrap();
    assert!(message.contains("points per series"), "{message}");
    assert!(message.contains("step"), "{message}");
}

#[tokio::test]
async fn garbage_remote_write_is_a_client_error_not_a_panic() {
    let harness = Harness::new();
    for payload in [vec![0xffu8; 32], b"not protobuf at all".to_vec()] {
        let status = harness.post_remote_write(payload).await;
        assert!(
            status == StatusCode::BAD_REQUEST || status == StatusCode::NO_CONTENT,
            "got {status}"
        );
    }
}

// ---------------------------------------------------------------------------
// durability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metrics_survive_a_restart() {
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
        let payload = remote_write_payload(
            &[("__name__", "persisted"), ("app", "checkout")],
            &[(7.0, NOW_SECONDS as i64 * 1000)],
        );
        let request = Request::post("/api/v1/write")
            .body(Body::from(payload))
            .unwrap();
        assert_eq!(
            app.oneshot(request).await.unwrap().status(),
            StatusCode::NO_CONTENT
        );
        state.store.sync_all().unwrap();
    }

    let (app, _state) = make();
    let response = app
        .oneshot(
            Request::get(format!("/api/v1/query?query=persisted&time={NOW_SECONDS}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["data"]["result"][0]["value"][1], "7");
}

#[tokio::test]
async fn all_three_signals_coexist() {
    let harness = Harness::new();

    harness.post_otlp(&otlp_counter()).await;
    harness
        .post_remote_write(remote_write_payload(
            &[("__name__", "up"), ("app", "checkout")],
            &[(1.0, NOW_SECONDS as i64 * 1000)],
        ))
        .await;

    let logs = json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeLogs": [{"logRecords": [
                {"timeUnixNano": NOW_NANOS.to_string(), "body": {"stringValue": "hello"}}
            ]}]
        }]
    });
    let request = Request::post("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(logs.to_string()))
        .unwrap();
    assert_eq!(harness.send(request).await.0, StatusCode::OK);

    // Each signal answers from its own store.
    let (status, metrics) = harness
        .get(&format!("/api/v1/query?query=up&time={NOW_SECONDS}"))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(metrics["data"]["result"][0]["value"][1], "1");

    let path = format!(
        "/loki/api/v1/query_range?query={}&start={}&end={}",
        urlencode(r#"{app="checkout"}"#),
        NOW_NANOS - 3_600_000_000_000,
        NOW_NANOS + 3_600_000_000_000
    );
    let (status, logs) = harness.get(&path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(logs["data"]["result"][0]["values"][0][1], "hello");

    // …and /status reports all three.
    let (_, status_body) = harness.get("/status").await;
    for signal in ["logs", "traces", "metrics"] {
        assert!(
            status_body["storage"][signal].is_object(),
            "missing {signal} in /status"
        );
    }
}
