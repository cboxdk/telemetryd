//! Two instances, each in front of a different replica of one replicated ClickHouse
//! cluster, give the same answers: what one accepts, the other reads.
//!
//! Run when `TELEMETRYD_TEST_CLICKHOUSE_CLUSTER=<cluster>` and
//! `TELEMETRYD_TEST_CLICKHOUSE_REPLICAS=http://user:pw@host1:8123,http://user:pw@host2:8123`
//! are set; the cluster needs `{shard}` and `{replica}` macros and a Keeper.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

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

const BASE: u64 = 1_790_000_000_000_000_000;
const SECOND: u64 = 1_000_000_000;

fn replicas() -> Option<(String, Vec<ClickHouseConfig>)> {
    let cluster = std::env::var("TELEMETRYD_TEST_CLICKHOUSE_CLUSTER").ok()?;
    let urls = std::env::var("TELEMETRYD_TEST_CLICKHOUSE_REPLICAS").ok()?;
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let configs = urls
        .split(',')
        .map(|raw| {
            let (scheme, rest) = raw.trim().split_once("://").unwrap();
            let (credentials, host) = rest.rsplit_once('@').unwrap_or(("default:", rest));
            let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
            ClickHouseConfig {
                url: format!("{scheme}://{host}"),
                database: format!("td_test_cluster_{unique}"),
                user: user.to_owned(),
                password: telemetryd_core::Secret::new(password),
                cluster: cluster.clone(),
                ..ClickHouseConfig::default()
            }
        })
        .collect();
    Some((cluster, configs))
}

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

fn kv(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

async fn post(app: &axum::Router, path: &str, body: &Value) {
    let request = Request::post(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{path}");
}

async fn ask(app: &axum::Router, path: &str) -> Value {
    let request = Request::get(path).body(Body::empty()).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let mut value: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    if let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) {
        data.remove("stats");
    }
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
async fn instances_on_different_replicas_answer_alike() {
    let Some((cluster, configs)) = replicas() else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_CLUSTER");
        return;
    };
    assert!(configs.len() >= 2, "two replicas are needed");
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    // Both create the schema, as two instances starting together do.
    let writer = instance(dir_a.path(), configs[0].clone());
    let reader = instance(dir_b.path(), configs[1].clone());

    let records: Vec<Value> = (0..600u64)
        .map(|i| {
            json!({
                "timeUnixNano": (BASE + i * SECOND).to_string(),
                "severityNumber": if i % 10 == 0 { 17 } else { 9 },
                "body": {"stringValue": format!("request {i} status={}", if i % 10 == 0 { 500 } else { 200 })},
            })
        })
        .collect();
    post(
        &writer,
        "/v1/logs",
        &json!({"resourceLogs": [{
            "resource": {"attributes": [kv("service.name", "geo")]},
            "scopeLogs": [{"logRecords": records}],
        }]}),
    )
    .await;
    let spans: Vec<Value> = (0..50u64)
        .map(|i| {
            json!({
                "traceId": format!("{:032x}", i + 1),
                "spanId": format!("{:016x}", i + 1),
                "name": "GET /geocode",
                "kind": 2,
                "startTimeUnixNano": (BASE + i * 10 * SECOND).to_string(),
                "endTimeUnixNano": (BASE + i * 10 * SECOND + 30_000_000).to_string(),
                "status": {"code": if i % 5 == 0 { 2 } else { 1 }},
            })
        })
        .collect();
    post(
        &writer,
        "/v1/traces",
        &json!({"resourceSpans": [{
            "resource": {"attributes": [kv("service.name", "geo")]},
            "scopeSpans": [{"spans": spans}],
        }]}),
    )
    .await;

    let points: Vec<Value> = (0..60u64)
        .map(|i| {
            json!({
                "timeUnixNano": (BASE + i * 10 * SECOND).to_string(),
                "asDouble": f64::from(u32::try_from(i * i).unwrap()),
                "attributes": [kv("http.route", if i % 2 == 0 { "/geocode" } else { "/reverse" })],
            })
        })
        .collect();
    post(
        &writer,
        "/v1/metrics",
        &json!({"resourceMetrics": [{
            "resource": {"attributes": [kv("service.name", "geo")]},
            "scopeMetrics": [{"metrics": [{
                "name": "http.requests",
                "sum": {"isMonotonic": true, "aggregationTemporality": 2, "dataPoints": points},
            }]}],
        }]}),
    )
    .await;

    // The second replica has what the first took once it has fetched the parts.
    let replica = telemetryd_store::clickhouse::Client::new(&configs[1]).unwrap();
    let _ = cluster;
    for table in [
        "otel_logs",
        "otel_log_streams",
        "otel_log_volume",
        "otel_log_volume_1m",
        "otel_traces",
        "otel_traces_trace_id_ts",
        "otel_trace_streams",
        "otel_trace_keys",
        "telemetryd_samples",
        "telemetryd_series",
        "telemetryd_sample_counts",
        "telemetryd_folds",
    ] {
        // On several shards the replicated table is the shard's own, under the
        // Distributed one.
        if replica
            .execute(&format!("SYSTEM SYNC REPLICA {table}_local"))
            .is_err()
        {
            replica
                .execute(&format!("SYSTEM SYNC REPLICA {table}"))
                .unwrap();
        }
    }

    let (start, end) = (BASE / SECOND, BASE / SECOND + 600);
    let paths = [
        format!(
            "/loki/api/v1/query_range?query={}&start={start}&end={end}&limit=50",
            encode(r#"{app="geo", level="error"}"#)
        ),
        format!(
            "/loki/api/v1/query_range?query={}&start={start}&end={end}&step=60",
            encode(r#"sum by (level) (count_over_time({app="geo"}[1m]))"#)
        ),
        format!("/loki/api/v1/labels?start={start}&end={end}"),
        format!(
            "/api/search?q={}&start={start}&end={end}&limit=20",
            encode("{ status = error }")
        ),
        format!("/api/traces/{:032x}", 6),
        format!(
            "/api/v1/query_range?query={}&start={start}&end={end}&step=30",
            encode("sum by (http_route) (rate(http_requests_total[2m]))")
        ),
        format!(
            "/api/v1/query?query={}&time={end}",
            encode("increase(http_requests_total[10m])")
        ),
        format!("/api/v1/label/http_route/values?start={start}&end={end}"),
    ];
    for path in &paths {
        let want = ask(&writer, path).await;
        let got = ask(&reader, path).await;
        assert_ne!(want, Value::Null, "{path}");
        assert_eq!(want, got, "{path}");
    }
    let volume = ask(&reader, &paths[1]).await;
    let total: f64 = volume["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|s| s["values"].as_array().unwrap().clone())
        .map(|p| p[1].as_str().unwrap().parse::<f64>().unwrap())
        .sum();
    assert!(total > 0.0, "the reader counted nothing: {volume}");
}
