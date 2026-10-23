//! The same spans, sent to an instance on the local store and to one on ClickHouse, give
//! the same answer to every Tempo API query.
//!
//! Beyond what the Tempo corpus reaches: events and links with trace state through the
//! trace-by-id lookup, resource attributes promoted and not, relaxed attribute names,
//! numeric attribute comparisons against text, status, kind and duration, search with
//! limits, tag values and TraceQL metrics.
//!
//! Run when `TELEMETRYD_TEST_CLICKHOUSE_URL=http://user:password@host:8123` is set.

#![allow(
    clippy::unwrap_used,
    clippy::needless_pass_by_value,
    clippy::too_many_lines
)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use telemetryd_core::Config;
use telemetryd_core::config::{ClickHouseConfig, StorageConfig};
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use tower::ServiceExt;

mod common;
use common::clickhouse_under_test;

const BASE: u64 = 1_790_000_000_000_000_000;
const SECOND: u64 = 1_000_000_000;

fn instance(dir: &std::path::Path, clickhouse: ClickHouseConfig) -> axum::Router {
    let config = Config {
        storage: StorageConfig {
            data_dir: Some(dir.to_path_buf()),
            ..StorageConfig::default()
        },
        clickhouse,
        retention: common::long_retention(),
        ..Config::default()
    };
    config.validate().unwrap();
    let store = Arc::new(Store::open(&config).unwrap());
    router(AppState::new(Arc::new(config), store).unwrap())
}

fn kv(key: &str, value: Value) -> Value {
    json!({"key": key, "value": value})
}

fn text(value: &str) -> Value {
    json!({"stringValue": value})
}

/// Forty traces over ten minutes from two services: a server span, a database client
/// span under it, and on every fifth trace a failing outbound call with an event and a
/// link to the previous trace.
fn payload() -> Value {
    let mut resource_spans = Vec::new();
    for (service, version) in [("geo", "1.2"), ("billing", "7.0")] {
        let mut spans = Vec::new();
        for i in 0..40u64 {
            let trace = format!("{:016x}{:016x}", u64::from(service == "geo") + 1, i + 1);
            let start = BASE + i * 15 * SECOND;
            let route = ["/geocode", "/reverse", "/batch"][(i % 3) as usize];
            let status = if i % 7 == 0 { 500 } else { 200 };
            spans.push(json!({
                "traceId": trace,
                "spanId": format!("{:016x}", i * 10 + 1),
                "name": format!("GET {route}"),
                "kind": 2,
                "startTimeUnixNano": start.to_string(),
                "endTimeUnixNano": (start + (20 + i * 3) * 1_000_000).to_string(),
                "status": {"code": if status == 500 { 2 } else { 1 }, "message": if status == 500 { "boom" } else { "" }},
                "attributes": [
                    kv("http.route", text(route)),
                    kv("http.response.status_code", json!({"intValue": status.to_string()})),
                    kv("http.request.method", text("GET")),
                    // Values Rust's regex and ClickHouse's read differently: a word with
                    // a non-ASCII letter, a value across a newline.
                    kv("user", text(["José", "ana", "Øystein", "x\ny"][(i % 4) as usize])),
                ],
            }));
            spans.push(json!({
                "traceId": trace,
                "spanId": format!("{:016x}", i * 10 + 2),
                "parentSpanId": format!("{:016x}", i * 10 + 1),
                "name": "db.query",
                "kind": 3,
                "startTimeUnixNano": (start + 2_000_000).to_string(),
                "endTimeUnixNano": (start + (4 + i % 5) * 1_000_000).to_string(),
                "attributes": [
                    kv("db.system.name", text("mysql")),
                    kv("db.query.text", text(&format!("select * from places where id = {i}"))),
                    kv("db_rows", json!({"intValue": (i * 13 % 100).to_string()})),
                ],
            }));
            if i % 5 == 0 {
                spans.push(json!({
                    "traceId": trace,
                    "spanId": format!("{:016x}", i * 10 + 3),
                    "parentSpanId": format!("{:016x}", i * 10 + 1),
                    "name": "http.client",
                    "kind": 3,
                    "startTimeUnixNano": (start + 5_000_000).to_string(),
                    "endTimeUnixNano": (start + 15_000_000).to_string(),
                    "status": {"code": 2},
                    "events": [{
                        "timeUnixNano": (start + 9_000_000).to_string(),
                        "name": "exception",
                        "attributes": [kv("exception.type", text("Timeout"))],
                    }],
                    "links": [{
                        "traceId": format!("{:016x}{:016x}", 9, i),
                        "spanId": format!("{:016x}", 77),
                        "traceState": "vendor=1",
                        "attributes": [kv("link.kind", text("retry"))],
                    }],
                }));
            }
        }
        resource_spans.push(json!({
            "resource": {"attributes": [
                kv("service.name", text(service)),
                kv("service.version", text(version)),
                kv("host.name", text("web-1")),
                kv("cloud.region", text("eu-west-1")),
            ]},
            "scopeSpans": [{"scope": {"name": "laravel"}, "spans": spans}],
        }));
    }
    json!({"resourceSpans": resource_spans})
}

async fn send(app: &axum::Router, body: &Value) {
    let request = Request::post("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

async fn ask(app: &axum::Router, path: &str) -> Value {
    let request = Request::get(path).body(Body::empty()).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let mut value: Value =
        serde_json::from_slice(&body).unwrap_or_else(|_| json!({"status": status.as_u16()}));
    // Work done is each store's own.
    if let Some(metrics) = value.get_mut("metrics").and_then(Value::as_object_mut) {
        metrics.remove("inspectedSpans");
    }
    value
}

fn encode(raw: &str) -> String {
    raw.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[tokio::test]
async fn clickhouse_answers_traces_as_the_local_store_does() {
    let Some(clickhouse) = clickhouse_under_test("trace_parity") else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_URL");
        return;
    };
    let local_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let local = instance(local_dir.path(), ClickHouseConfig::default());
    let remote = instance(remote_dir.path(), clickhouse);
    let body = payload();
    send(&local, &body).await;
    send(&remote, &body).await;

    let (start, end) = (BASE / SECOND, BASE / SECOND + 700);
    let search = |q: &str, limit: u32| {
        format!(
            "/api/search?q={}&start={start}&end={end}&limit={limit}",
            encode(q)
        )
    };
    let mut paths = vec![
        format!("/api/traces/{:016x}{:016x}", 2, 6),
        format!("/api/traces/{:016x}{:016x}", 1, 11),
        format!("/api/v2/traces/{:016x}{:016x}", 2, 1),
        // Without its leading zeros, as a search row shows it.
        format!("/api/traces/{:x}{:016x}", 1, 26),
        "/api/traces/00000000000000000000000000000abc".to_owned(),
        format!("/api/search/tags?start={start}&end={end}"),
        format!("/api/v2/search/tags?start={start}&end={end}"),
        format!("/api/v2/search/tags?scope=resource&start={start}&end={end}"),
        format!("/api/v2/search/tag/span.http.route/values?start={start}&end={end}"),
        format!(
            "/api/v2/search/tag/resource.service.version/values?start={start}&end={end}&q={}",
            encode(r"{ status = error }")
        ),
        format!(
            "/api/v2/search/tag/name/values?start={start}&end={end}&q={}",
            encode(r#"{ resource.service.name = "billing" }"#)
        ),
    ];
    for (q, limit) in [
        (r#"{ span.user =~ "\\w+" }"#, 50),
        (r#"{ span.user !~ "\\w+" }"#, 50),
        (r#"{ span.user =~ "x.y" }"#, 50),
        (r#"{ span.user =~ "Jos." }"#, 50),
        (r"{ .db.rows > 10 }", 50),
        (r"{ .db_rows <= 10 && kind = client }", 50),
        (
            r#"{ nestedSetParent < 0 && span.http.route = "/geocode" }"#,
            10,
        ),
        ("{}", 20),
        ("{}", 100),
        (r#"{ resource.service.name = "geo" }"#, 50),
        (r#"{ .service.name = "billing" && status = error }"#, 50),
        (r#"{ span.http.route = "/reverse" }"#, 50),
        (r"{ span.http.response.status_code = 500 }", 50),
        (r"{ span.http.response.status_code >= 500 }", 50),
        (r#"{ .http_route =~ "/(geo|re).*" }"#, 50),
        (r"{ span.db_rows > 80 }", 50),
        (r"{ span.db.rows < 10 }", 50),
        (
            r#"{ resource.cloud.region = "eu-west-1" && kind = client }"#,
            50,
        ),
        (
            r#"{ resource.host.name = "web-1" && name = "db.query" }"#,
            5,
        ),
        (r"{ duration > 100ms }", 50),
        (r"{ duration >= 4ms && duration < 6ms }", 50),
        (r#"{ name =~ "GET .*" && status != error }"#, 50),
        (r"{ status = ok }", 50),
        (
            r#"{ kind = server && span.http.request.method = "GET" }"#,
            50,
        ),
        (r#"{ span.missing = nil && name = "http.client" }"#, 50),
        (r#"{ span.http.route != "/batch" }"#, 50),
        (r#"{ .db.query.text =~ ".*id = 1[0-9]" }"#, 50),
        (r#"{ statusMessage = "boom" }"#, 50),
        (
            r#"{ resource.service.version = "7.0" && duration > 50ms }"#,
            3,
        ),
    ] {
        paths.push(search(q, limit));
    }
    for q in [
        r#"{ span.user =~ "\\w+" } | rate()"#,
        r#"{ span.user !~ "\\d" } | count_over_time() by (span.user)"#,
        r"{ } | count_over_time() by (.db.rows)",
        r"{ kind = client } | count_over_time() by (span.db.rows)",
        r"{ } | rate() by (.service.name)",
        "{ } | histogram_over_time(duration)",
        r"{ kind = server } | max_over_time(duration) by (span.http.route)",
        r"{ kind = client } | min_over_time(duration) by (name)",
        r#"{ resource.service.version = "1.2" } | avg_over_time(duration) by (status)"#,
        r#"{ span.http.route != "/batch" } | count_over_time() by (span.http.route)"#,
        r"{ span.db_rows > 50 } | rate()",
        r"{ span.db.rows <= 10 } | count_over_time() by (resource.service.name)",
        r"{ span.missing = nil && kind = client } | count_over_time()",
        r"{ span.http.response.status_code = 500 } | rate() by (resource.service.name)",
        "{ } | rate() by (kind) | topk(1)",
        r"{ } | quantile_over_time(duration, 0.99) by (resource.service.name)",
        r"{ } | rate() by (resource.service.name)",
        r"{ status = error } | count_over_time() by (name)",
        r#"{ name = "db.query" } | quantile_over_time(duration, 0.5, 0.9)"#,
        r#"{ span.http.route = "/geocode" } | sum_over_time(duration) by (span.http.response.status_code)"#,
    ] {
        paths.push(format!(
            "/api/metrics/query_range?q={}&start={start}&end={end}&step=60",
            encode(q)
        ));
    }

    let mut differences = Vec::new();
    for path in &paths {
        let want = ask(&local, path).await;
        let got = ask(&remote, path).await;
        if !close(&want, &got) {
            differences.push(format!(
                "{path}\n  local:      {}\n  clickhouse: {}",
                want.to_string().chars().take(600).collect::<String>(),
                got.to_string().chars().take(600).collect::<String>(),
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "{} of {} answers differ:\n{}",
        differences.len(),
        paths.len(),
        differences.join("\n")
    );
}

/// Equal, numbers within a relative 1e-9: a sum or a quantile over the same spans in a
/// different order differs in its last bits, on either store, from one run to the next.
fn close(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let (x, y) = (x.as_f64().unwrap(), y.as_f64().unwrap());
            (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0)
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| close(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| close(v, w)))
        }
        _ => a == b,
    }
}

/// A search reads every row of the windows it covers, however many the predicate turns
/// down: with a limit on rows rather than on spans taken, the newest span the predicate
/// takes, past that many it did not, was never read.
#[tokio::test]
async fn a_search_reads_past_spans_the_predicate_turns_down() {
    let Some(clickhouse) = clickhouse_under_test("trace_window") else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_URL");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        storage: StorageConfig {
            data_dir: Some(dir.path().to_path_buf()),
            ..StorageConfig::default()
        },
        clickhouse,
        retention: common::long_retention(),
        ..Config::default()
    };
    config.validate().unwrap();
    let store = Arc::new(Store::open(&config).unwrap());
    let app = router(AppState::new(Arc::new(config), Arc::clone(&store)).unwrap());
    // Ten spans a millisecond apart, the newest six called "skip".
    let spans: Vec<Value> = (0..10u64)
        .map(|i| {
            json!({
                "traceId": format!("{:032x}", i + 1),
                "spanId": format!("{:016x}", i + 1),
                "name": if i >= 4 { "skip" } else { "keep" },
                "kind": 2,
                "startTimeUnixNano": (BASE + i * 1_000_000).to_string(),
                "endTimeUnixNano": (BASE + i * 1_000_000 + 500).to_string(),
            })
        })
        .collect();
    send(
        &app,
        &json!({"resourceSpans": [{
            "resource": {"attributes": [kv("service.name", text("geo"))]},
            "scopeSpans": [{"spans": spans}],
        }]}),
    )
    .await;
    let mut scan = telemetryd_store::Scan::range(BASE, BASE + SECOND);
    scan.limit = 4;
    scan.order = telemetryd_store::Order::Descending;
    let ids = store
        .span_store()
        .search_traces(scan, &[], &|span| span.name == "keep", 1)
        .unwrap();
    // The newest span kept is the fourth.
    assert!(ids.contains(&format!("{:032x}", 4)), "{ids:?}");
}
