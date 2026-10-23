//! The same logs, sent to an instance on the local store and to one on ClickHouse, give
//! the same answer to every query — byte for byte, through the Loki API.
//!
//! The conformance corpus holds both stores to Loki. This holds ClickHouse to the local
//! store on what that corpus does not reach: resource attributes arriving over OTLP, an
//! attribute standing in for a label a count is grouped by, lines that share a
//! timestamp across the pages and windows a limited read is cut into, and both
//! directions.
//!
//! Run when `TELEMETRYD_TEST_CLICKHOUSE_URL=http://user:password@host:8123` is set.

#![allow(clippy::unwrap_used)]

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

/// A fixed hour, so both stores hold the same timestamps and every query names them.
const BASE: u64 = 1_790_000_000_000_000_000;
const SECOND: u64 = 1_000_000_000;

fn instance(dir: &std::path::Path, clickhouse: ClickHouseConfig) -> axum::Router {
    let config = Config {
        storage: StorageConfig {
            data_dir: Some(dir.to_path_buf()),
            ..StorageConfig::default()
        },
        clickhouse,
        ..Config::default()
    };
    config.validate().unwrap();
    let store = Arc::new(Store::open(&config).unwrap());
    router(AppState::new(Arc::new(config), store).unwrap())
}

fn kv(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

/// Three services' worth of lines over ten minutes.
///
/// - `geo` sends `service.version` (promoted) and `host.name` (not) as resource
///   attributes, and some of its lines carry an `env` attribute of their own while its
///   stream has none — the attribute a `by (env)` count must group them under.
/// - `api` lines come in bursts sharing one timestamp, a thousand at a time, so every
///   page and window boundary a limited read makes falls inside a burst.
/// - `shop` carries a `level` attribute that disagrees with its severity.
fn payload() -> Value {
    let mut resource_logs = Vec::new();
    let mut geo = Vec::new();
    for i in 0..600u64 {
        let severity = if i % 10 == 0 { 17 } else { 9 };
        let mut attributes = vec![kv("http.route", ["/geocode", "/reverse"][(i % 2) as usize])];
        if i % 7 == 0 {
            attributes.push(kv("env", ["prod", "staging"][(i % 2) as usize]));
        }
        geo.push(json!({
            "timeUnixNano": (BASE + i * SECOND).to_string(),
            "severityNumber": severity,
            "body": {"stringValue": format!("geocode q={i} status={}", if severity == 17 { 502 } else { 200 })},
            "attributes": attributes,
        }));
    }
    resource_logs.push(json!({
        "resource": {"attributes": [kv("service.name", "geo"), kv("service.version", "1.2"), kv("host.name", "web-3")]},
        "scopeLogs": [{"logRecords": geo}],
    }));

    let mut api = Vec::new();
    for burst in 0..5u64 {
        for i in 0..1000u64 {
            api.push(json!({
                "timeUnixNano": (BASE + 100 * SECOND + burst * 60 * SECOND).to_string(),
                "severityNumber": if i % 3 == 0 { 13 } else { 9 },
                "body": {"stringValue": format!("request {burst}-{i:04}")},
            }));
        }
    }
    resource_logs.push(json!({
        "resource": {"attributes": [kv("service.name", "api")]},
        "scopeLogs": [{"logRecords": api}],
    }));

    let shop: Vec<Value> = (0..50u64)
        .map(|i| {
            json!({
                "timeUnixNano": (BASE + 30 * SECOND + i * 3 * SECOND).to_string(),
                "severityNumber": 9,
                "body": {"stringValue": format!("order {i}")},
                "attributes": [kv("level", "debug")],
            })
        })
        .collect();
    resource_logs.push(json!({
        "resource": {"attributes": [kv("service.name", "shop")]},
        "scopeLogs": [{"logRecords": shop}],
    }));
    json!({"resourceLogs": resource_logs})
}

async fn send(app: &axum::Router, body: &Value) {
    let request = Request::post("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

async fn ask(app: &axum::Router, path: &str, params: &[(&str, String)]) -> Value {
    let query: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect();
    let request = Request::get(format!("/loki/api/v1/{path}?{}", query.join("&")))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let mut value: Value =
        serde_json::from_slice(&body).unwrap_or_else(|_| json!({"status": status.as_u16()}));
    // Timing and work done are each store's own.
    if let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) {
        data.remove("stats");
    }
    without_tie_order(&mut value);
    value
}

/// Lines that share a timestamp come back in no defined order, and where a limit falls
/// among them, which of them make it is not defined either. The local store breaks ties
/// by the order it received lines in, which has no meaning across instances writing to
/// one database. So: each timestamp's lines compared as a set, and at the timestamp the
/// limit cut — the last one of the answer — only how many there are across streams.
fn without_tie_order(value: &mut Value) {
    let Some(result) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    if !result.iter().any(|s| s.get("stream").is_some()) {
        return;
    }
    let timestamps: Vec<u128> = result
        .iter()
        .flat_map(|s| s["values"].as_array().into_iter().flatten())
        .filter_map(|v| v[0].as_str()?.parse().ok())
        .collect();
    let (Some(&oldest), Some(&newest)) = (timestamps.iter().min(), timestamps.iter().max()) else {
        return;
    };
    let forward = result.iter().any(|s| {
        let values = s["values"].as_array().cloned().unwrap_or_default();
        values
            .windows(2)
            .any(|w| w[0][0].as_str() < w[1][0].as_str())
    });
    let cut = if forward { newest } else { oldest };
    let mut at_cut = 0usize;
    for stream in result.iter_mut() {
        let Some(values) = stream["values"].as_array_mut() else {
            continue;
        };
        values.retain(|v| {
            let at: u128 = v[0].as_str().and_then(|t| t.parse().ok()).unwrap_or(0);
            if at == cut {
                at_cut += 1;
                false
            } else {
                true
            }
        });
        values.sort_by_key(ToString::to_string);
    }
    result.push(json!({"lines_at_the_cut": at_cut}));
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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn clickhouse_answers_as_the_local_store_does() {
    let Some(clickhouse) = clickhouse_under_test("parity") else {
        eprintln!("TELEMETRYD_TEST_CLICKHOUSE_URL is not set; skipped");
        return;
    };
    let local_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let local = instance(local_dir.path(), ClickHouseConfig::default());
    let remote = instance(remote_dir.path(), clickhouse);
    let body = payload();
    send(&local, &body).await;
    send(&remote, &body).await;

    let start = (BASE / SECOND).to_string();
    let end = (BASE / SECOND + 600).to_string();
    let range = |query: &str, extra: &[(&'static str, &str)]| {
        let mut params = vec![
            ("query", query.to_owned()),
            ("start", start.clone()),
            ("end", end.clone()),
        ];
        params.extend(extra.iter().map(|(k, v)| (*k, (*v).to_owned())));
        params
    };

    let mut cases: Vec<(&str, Vec<(&str, String)>)> = vec![
        (
            "labels",
            vec![("start", start.clone()), ("end", end.clone())],
        ),
        (
            "label/level/values",
            vec![("start", start.clone()), ("end", end.clone())],
        ),
        (
            "series",
            vec![
                ("match[]", r#"{app=~".+"}"#.to_owned()),
                ("start", start.clone()),
                ("end", end.clone()),
            ],
        ),
    ];
    for (query, extra) in [
        (r#"{app="geo"}"#, &[("limit", "50")][..]),
        (
            r#"{app="geo", level="error"} |= "502""#,
            &[("limit", "100")],
        ),
        (
            r#"{app="geo"} != "q=1""#,
            &[("limit", "30"), ("direction", "forward")],
        ),
        (r#"{app="api"}"#, &[("limit", "1500")]),
        (
            r#"{app="api"}"#,
            &[("limit", "2500"), ("direction", "forward")],
        ),
        (r#"{app="api", level="warn"}"#, &[("limit", "700")]),
        (r#"{app=~"api|shop"}"#, &[("limit", "1100")]),
        (r#"{level="info"} |= "order""#, &[("limit", "20")]),
        (r#"{app="nothing"}"#, &[("limit", "10")]),
        (
            r#"sum by (env) (count_over_time({app="geo"}[1m]))"#,
            &[("step", "60")],
        ),
        (
            r#"sum by (level) (count_over_time({app=~".+"}[1m]))"#,
            &[("step", "30")],
        ),
        (
            r#"sum by (detected_level) (count_over_time({app=~".+"}[2m]))"#,
            &[("step", "60")],
        ),
        (
            r#"sum by (service_version, app) (rate({app=~"geo|api"} |= "e" [90s]))"#,
            &[("step", "45")],
        ),
        (
            r#"sum(bytes_over_time({app="api"}[1m]))"#,
            &[("step", "60")],
        ),
        (
            r#"sum by (http_route) (count_over_time({app="geo"}[5m]))"#,
            &[("step", "60")],
        ),
        (
            r#"sum by (app) (count_over_time({app=~".+"} |= "q=" != "q=5" [1m] offset 1m))"#,
            &[("step", "60")],
        ),
    ] {
        cases.push(("query_range", range(query, extra)));
    }

    // Bounds that fall inside a second, as Grafana's millisecond ranges do: every bucket
    // edge splits a second, which the rollup cannot answer and the lines must.
    let split_start = (BASE + 7 * SECOND + 250_000_000).to_string();
    let split_end = (BASE + 590 * SECOND + 875_000_000).to_string();
    for (query, step) in [
        (r#"sum by (level) (count_over_time({app=~".+"}[1m]))"#, "60"),
        (
            r#"sum by (detected_level) (count_over_time({app=~".+"}[30s]))"#,
            "15",
        ),
        (r#"sum by (env) (count_over_time({app="geo"}[1m]))"#, "30"),
        (r#"sum(bytes_rate({app=~".+"}[2m]))"#, "45"),
        (r#"sum by (app) (rate({app=~".+"}[1m]))"#, "0.5"),
    ] {
        cases.push((
            "query_range",
            vec![
                ("query", query.to_owned()),
                ("start", split_start.clone()),
                ("end", split_end.clone()),
                ("step", step.to_owned()),
            ],
        ));
    }

    let mut differences = Vec::new();
    for (path, params) in &cases {
        let want = ask(&local, path, params).await;
        let got = ask(&remote, path, params).await;
        if want != got {
            differences.push(format!(
                "{path} {:?}\n  local:      {}\n  clickhouse: {}",
                params.first().map(|(_, v)| v),
                want.to_string().chars().take(400).collect::<String>(),
                got.to_string().chars().take(400).collect::<String>(),
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "{} of {} answers differ:\n{}",
        differences.len(),
        cases.len(),
        differences.join("\n")
    );
}
