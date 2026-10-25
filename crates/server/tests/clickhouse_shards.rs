//! A ClickHouse cluster of several shards: instances in front of different shards give
//! the same answers, every shard holds part of every signal, and a cluster telemetryd
//! cannot lay its tables out on correctly is refused at startup.
//!
//! The whole ClickHouse suite also runs on such a cluster — every parity and
//! conformance test, with `TELEMETRYD_TEST_CLICKHOUSE_URL_CLUSTER` naming it — which is
//! what holds its answers to the local store's. This is what that cannot see: where the
//! rows went, and the second instance.
//!
//! Run when `TELEMETRYD_TEST_CLICKHOUSE_SHARDED_CLUSTER=<cluster>` and
//! `TELEMETRYD_TEST_CLICKHOUSE_SHARDED_URLS=http://user:pw@shard1:8123,http://user:pw@shard2:8123`
//! name a cluster of two shards and a server on each; `scripts/clickhouse/sharded`
//! is one.

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

/// One configuration per server named, all on one database of their own.
fn servers() -> Option<Vec<ClickHouseConfig>> {
    let cluster = std::env::var("TELEMETRYD_TEST_CLICKHOUSE_SHARDED_CLUSTER").ok()?;
    let urls = std::env::var("TELEMETRYD_TEST_CLICKHOUSE_SHARDED_URLS").ok()?;
    let database = format!("td_test_shards_{}", common::unique());
    Some(
        urls.split(',')
            .map(|raw| {
                let (scheme, rest) = raw.trim().split_once("://").unwrap();
                let (credentials, host) = rest.rsplit_once('@').unwrap_or(("default:", rest));
                let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
                ClickHouseConfig {
                    url: format!("{scheme}://{host}"),
                    database: database.clone(),
                    user: user.to_owned(),
                    password: telemetryd_core::Secret::new(password),
                    cluster: cluster.clone(),
                    ..ClickHouseConfig::default()
                }
            })
            .collect(),
    )
}

fn config(dir: &std::path::Path, clickhouse: ClickHouseConfig) -> Config {
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
    config
}

fn instance(dir: &std::path::Path, clickhouse: ClickHouseConfig) -> (axum::Router, Arc<Store>) {
    let config = config(dir, clickhouse);
    let store = Arc::new(Store::open(&config).unwrap());
    (
        router(AppState::new(Arc::new(config), Arc::clone(&store)).unwrap()),
        store,
    )
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

/// `SELECT count()` of a table on one server: its own shard's rows only, for a table
/// under a `Distributed` one.
fn count(client: &telemetryd_store::clickhouse::Client, table: &str) -> u64 {
    let mut n = 0;
    client
        .query_lines(&format!("SELECT count() AS n FROM {table}"), |line| {
            let value: Value = serde_json::from_slice(line).unwrap();
            n = value["n"].as_u64().unwrap();
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .unwrap();
    n
}

#[tokio::test]
async fn instances_on_different_shards_answer_alike_and_every_shard_holds_rows() {
    let Some(servers) = servers() else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_SHARDED_CLUSTER");
        return;
    };
    assert!(
        servers.len() >= 2,
        "a server on each of two shards is needed"
    );
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (writer, writer_store) = instance(dir_a.path(), servers[0].clone());
    let (reader, _) = instance(dir_b.path(), servers[1].clone());

    // Lines in three streams, spans of fifty traces, a counter of forty series.
    let records: Vec<Value> = (0..900u64)
        .map(|i| {
            let severity = [9, 13, 17][usize::try_from(i % 3).unwrap()];
            json!({
                "timeUnixNano": (BASE + i * SECOND).to_string(),
                "severityNumber": severity,
                "body": {"stringValue": format!("request {i} status={}", if i % 3 == 2 { 500 } else { 200 })},
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
    let spans: Vec<Value> = (0..200u64)
        .map(|i| {
            let trace = i % 50 + 1;
            json!({
                "traceId": format!("{trace:032x}"),
                "spanId": format!("{:016x}", i + 1),
                "parentSpanId": if i < 50 { String::new() } else { format!("{trace:016x}") },
                "name": if i < 50 { "GET /geocode" } else { "SELECT" },
                "kind": if i < 50 { 2 } else { 3 },
                "startTimeUnixNano": (BASE + trace * 10 * SECOND + i).to_string(),
                "endTimeUnixNano": (BASE + trace * 10 * SECOND + i + 30_000_000).to_string(),
                "status": {"code": if trace % 5 == 0 { 2 } else { 1 }},
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
    let points: Vec<Value> = (0..40u64)
        .flat_map(|series| {
            (0..30u64).map(move |i| {
                json!({
                    "timeUnixNano": (BASE + i * 20 * SECOND).to_string(),
                    "asDouble": f64::from(u32::try_from(i * (series + 1)).unwrap()),
                    "attributes": [kv("http.route", &format!("/r{series}"))],
                })
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
    // Summarised, so the folds are written through the Distributed table too and a
    // long window reads them.
    let later = BASE + 3 * 86_400 * SECOND;
    while writer_store
        .roll_up_metrics(later)
        .unwrap()
        .is_some_and(|pass| pass.folds_written > 0)
    {}

    // Every shard holds part of each: what was written spread, not piled on one.
    let shards: Vec<_> = servers
        .iter()
        .map(|s| telemetryd_store::clickhouse::Client::new(s).unwrap())
        .collect();
    let tables = [
        "otel_logs_local",
        "otel_log_streams_local",
        "otel_log_volume_local",
        "otel_traces_local",
        "otel_traces_trace_id_ts_local",
        "telemetryd_samples_local",
        "telemetryd_series_local",
        "telemetryd_folds_local",
    ];
    for client in &shards {
        for table in tables {
            client
                .execute(&format!("SYSTEM SYNC REPLICA {table}"))
                .unwrap();
        }
    }
    for table in tables {
        let counts: Vec<u64> = shards.iter().map(|client| count(client, table)).collect();
        assert!(
            counts.iter().all(|n| *n > 0),
            "{table} is not on every shard: {counts:?}"
        );
    }
    // A series' samples and its series rows share a shard, as the reads joining them
    // need: no shard holds the samples of a series it has no series row for.
    for client in &shards {
        let mut orphans = 0;
        client
            .query_lines(
                "SELECT count() AS n FROM telemetryd_samples_local \
                 WHERE SeriesHash NOT IN (SELECT SeriesHash FROM telemetryd_series_local)",
                |line| {
                    orphans = serde_json::from_slice::<Value>(line).unwrap()["n"]
                        .as_u64()
                        .unwrap();
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )
            .unwrap();
        assert_eq!(orphans, 0, "samples on a shard without their series");
    }
    // And the settings, markers and lease: one copy, the same on every server.
    let markers: Vec<u64> = shards
        .iter()
        .map(|client| count(client, "telemetryd_meta"))
        .collect();
    assert!(
        markers.iter().all(|n| *n > 0 && *n == markers[0]),
        "telemetryd_meta differs between shards: {markers:?}"
    );

    let (start, end) = (BASE / SECOND, BASE / SECOND + 900);
    let paths = [
        format!(
            "/loki/api/v1/query_range?query={}&start={start}&end={end}&limit=50",
            encode(r#"{app="geo", level="error"}"#)
        ),
        format!(
            "/loki/api/v1/query_range?query={}&start={start}&end={end}&limit=1000&direction=forward",
            encode(r#"{app="geo"} |= "status=500""#)
        ),
        format!(
            "/loki/api/v1/query_range?query={}&start={start}&end={end}&step=60",
            encode(r#"sum by (level) (count_over_time({app="geo"}[1m]))"#)
        ),
        format!("/loki/api/v1/labels?start={start}&end={end}"),
        format!("/loki/api/v1/label/level/values?start={start}&end={end}"),
        format!(
            "/api/search?q={}&start={start}&end={end}&limit=20",
            encode("{ status = error }")
        ),
        format!(
            "/api/search?q={}&start={start}&end={end}&limit=100",
            encode("{ name = \"SELECT\" }")
        ),
        format!("/api/traces/{:032x}", 6),
        format!("/api/traces/{:032x}", 7),
        format!(
            "/api/metrics/query_range?q={}&start={start}&end={end}&step=60",
            encode("{ } | rate() by (status)")
        ),
        format!(
            "/api/v1/query_range?query={}&start={start}&end={end}&step=30",
            encode("sum by (http_route) (rate(http_requests_total[2m]))")
        ),
        format!(
            "/api/v1/query?query={}&time={}",
            encode("sum(increase(http_requests_total[2d]))"),
            later / SECOND
        ),
        format!(
            "/api/v1/query?query={}&time={}",
            encode("count(http_requests_total)"),
            start + 580
        ),
        format!("/api/v1/label/http_route/values?start={start}&end={end}"),
    ];
    for path in &paths {
        let want = ask(&writer, path).await;
        let got = ask(&reader, path).await;
        assert_ne!(want, Value::Null, "{path}");
        assert_eq!(want, got, "{path}");
    }
    let lines = ask(&reader, &paths[1]).await;
    let found: usize = lines["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|stream| stream["values"].as_array().unwrap().len())
        .sum();
    assert_eq!(found, 300, "every line with status=500, from both shards");
    let series = ask(&reader, &paths[12]).await;
    assert_eq!(
        series["data"]["result"][0]["value"][1], "40",
        "every series, from both shards: {series}"
    );
    for trace in [6, 7] {
        let trace = ask(&reader, &format!("/api/traces/{trace:032x}")).await;
        let spans: usize = trace["batches"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|batch| batch["scopeSpans"].as_array().unwrap().clone())
            .map(|scope| scope["spans"].as_array().unwrap().len())
            .sum();
        assert_eq!(spans, 4, "every span of the trace: {trace}");
    }

    // What each app holds, counted on every shard.
    let usage = writer_store.app_usage();
    let geo = usage.iter().find(|u| u.app == "geo").unwrap();
    assert_eq!(geo.rows, 900 + 200 + 40 * 30, "{usage:?}");
    assert!(geo.estimated_bytes > 0, "{usage:?}");
}

#[test]
fn a_database_laid_out_for_one_shard_is_refused_on_several() {
    let Some(servers) = servers() else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_SHARDED_CLUSTER");
        return;
    };
    let server = &servers[0];
    let client = telemetryd_store::clickhouse::Client::new(server).unwrap();
    let on = format!(" ON CLUSTER '{}'", server.cluster);
    let setup = client.in_database("default");
    setup
        .execute(&format!("CREATE DATABASE `{}`{on}", server.database))
        .unwrap();
    // As telemetryd lays it out on one shard: replicated, not distributed.
    client
        .execute(&format!(
            "CREATE TABLE otel_logs{on} (Timestamp DateTime64(9, 'UTC'), Body String) \
             ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{{database}}/otel_logs', '{{replica}}') \
             ORDER BY Timestamp"
        ))
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let error = Store::open(&config(dir.path(), server.clone()))
        .map(|_| ())
        .unwrap_err();
    assert!(
        error.to_string().contains("was laid out for one shard"),
        "{error}"
    );
}

#[test]
fn several_shards_without_internal_replication_are_refused() {
    let Some(mut servers) = servers() else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_SHARDED_CLUSTER");
        return;
    };
    // The same servers as a cluster without `internal_replication`: the test cluster
    // defines one beside the other, named so.
    let Ok(unsafe_cluster) = std::env::var("TELEMETRYD_TEST_CLICKHOUSE_SHARDED_UNSAFE_CLUSTER")
    else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_SHARDED_UNSAFE_CLUSTER");
        return;
    };
    let mut server = servers.remove(0);
    server.cluster = unsafe_cluster;
    let dir = tempfile::tempdir().unwrap();
    let error = Store::open(&config(dir.path(), server))
        .map(|_| ())
        .unwrap_err();
    assert!(
        error.to_string().contains("internal_replication"),
        "{error}"
    );
}

#[test]
fn a_cluster_that_has_gained_a_shard_since_is_refused() {
    let Some(servers) = servers() else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_SHARDED_CLUSTER");
        return;
    };
    let server = &servers[0];
    let dir = tempfile::tempdir().unwrap();
    drop(Store::open(&config(dir.path(), server.clone())).unwrap());
    // As if the tables had been laid out when the cluster had three shards.
    telemetryd_store::clickhouse::Client::new(server)
        .unwrap()
        .execute("INSERT INTO telemetryd_meta SELECT 'shards', '3', now64(9)")
        .unwrap();
    let error = Store::open(&config(dir.path(), server.clone()))
        .map(|_| ())
        .unwrap_err();
    assert!(error.to_string().contains("was laid out for 3"), "{error}");
}
