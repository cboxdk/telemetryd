//! Metric LogQL over the HTTP API: what `rate`, `count_over_time`, `sum by` and
//! `unwrap` answer on `/loki/api/v1/query` and `/loki/api/v1/query_range`, in Loki's
//! response shapes — the queries Grafana's log volume, alerting rules and dashboards send.

#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
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

/// A whole second, so lines pushed at `T + n` seconds sit on step boundaries.
const T: u64 = 1_750_000_000;

struct Harness {
    router: axum::Router,
    _tmp: tempfile::TempDir,
}

impl Harness {
    /// Lines at `T + 0s`, `T + 10s` and `T + 20s`: two info, one error.
    async fn with_lines() -> Self {
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
        let harness = Self {
            router: router(state),
            _tmp: tmp,
        };
        let at = |seconds: u64| ((T + seconds) * 1_000_000_000).to_string();
        let push = json!({"streams": [
            {"stream": {"app": "x", "level": "info"}, "values": [
                [at(0), "GET /a took=250ms size=1KB"],
                [at(10), "GET /b took=1.5s size=2KB"],
            ]},
            {"stream": {"app": "x", "level": "error"}, "values": [
                [at(20), "error: upstream took=4s size=10B"],
            ]},
        ]});
        let request = Request::post("/loki/api/v1/push")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(push.to_string()))
            .unwrap();
        let (status, body) = harness.send(request).await;
        assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
        harness
    }

    async fn send(&self, request: Request<Body>) -> (StatusCode, String) {
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    async fn get(&self, path: &str, params: &[(&str, String)]) -> (StatusCode, Value) {
        let query: String = params
            .iter()
            .map(|(k, v)| format!("{k}={}", urlencoding(v)))
            .collect::<Vec<_>>()
            .join("&");
        let request = Request::get(format!("{path}?{query}"))
            .body(Body::empty())
            .unwrap();
        let (status, body) = self.send(request).await;
        (
            status,
            serde_json::from_str(&body).unwrap_or(Value::String(body)),
        )
    }

    /// An instant query at `T + seconds`.
    async fn instant(&self, query: &str, seconds: u64) -> (StatusCode, Value) {
        self.get(
            "/loki/api/v1/query",
            &[
                ("query", query.to_owned()),
                ("time", (T + seconds).to_string()),
            ],
        )
        .await
    }

    /// `(labels, value)` of an instant vector answer, sorted.
    async fn vector(&self, query: &str, seconds: u64) -> Vec<(Value, f64)> {
        let (status, body) = self.instant(query, seconds).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
        assert_eq!(body["data"]["resultType"], "vector", "{body}");
        let mut out: Vec<(Value, f64)> = body["data"]["result"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                (
                    s["metric"].clone(),
                    s["value"][1].as_str().unwrap().parse().unwrap(),
                )
            })
            .collect();
        out.sort_by_key(|(labels, _)| labels.to_string());
        out
    }
}

fn urlencoding(raw: &str) -> String {
    raw.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

fn one(values: &[(Value, f64)]) -> f64 {
    assert_eq!(values.len(), 1, "{values:?}");
    values[0].1
}

#[tokio::test]
async fn counts_and_rates_group_as_loki_does() {
    let h = Harness::with_lines().await;

    let by_level = h
        .vector(r#"sum by (level) (count_over_time({app="x"}[1m]))"#, 30)
        .await;
    assert_eq!(
        by_level,
        vec![
            (json!({"level": "error"}), 1.0),
            (json!({"level": "info"}), 2.0)
        ]
    );
    // The window is `(t - 1m, t]`: at T+65s the line at T+0s has left it.
    let later = h.vector(r#"sum(count_over_time({app="x"}[1m]))"#, 65).await;
    assert!((one(&later) - 2.0).abs() < 1e-9, "{later:?}");

    let rate = h.vector(r#"sum(rate({app="x"}[1m]))"#, 30).await;
    assert!((one(&rate) - 3.0 / 60.0).abs() < 1e-12, "{rate:?}");

    // Without an aggregation, each stream is its own series, labels intact.
    let bare = h.vector(r#"count_over_time({app="x"}[1m])"#, 30).await;
    assert_eq!(bare.len(), 2, "{bare:?}");
    assert!(
        bare.iter().all(|(labels, _)| labels["app"] == "x"),
        "{bare:?}"
    );

    let filtered = h
        .vector(r#"sum(count_over_time({app="x"} |= "upstream" [1m]))"#, 30)
        .await;
    assert!((one(&filtered) - 1.0).abs() < 1e-9);

    let bytes = h.vector(r#"sum(bytes_over_time({app="x"}[1m]))"#, 30).await;
    let expected = ("GET /a took=250ms size=1KB".len()
        + "GET /b took=1.5s size=2KB".len()
        + "error: upstream took=4s size=10B".len()) as f64;
    assert!((one(&bytes) - expected).abs() < 1e-9, "{bytes:?}");

    // `offset` moves the window back: at T+80s offset 1m reads (T-40s, T+20s].
    let offset = h
        .vector(r#"sum(count_over_time({app="x"}[1m] offset 1m))"#, 80)
        .await;
    assert!((one(&offset) - 3.0).abs() < 1e-9, "{offset:?}");
}

#[tokio::test]
async fn unwrapped_values_aggregate_in_their_units() {
    let h = Harness::with_lines().await;

    let seconds = h
        .vector(
            r#"sum(sum_over_time({app="x"} | logfmt | unwrap duration(took) [1m]))"#,
            30,
        )
        .await;
    assert!((one(&seconds) - 5.75).abs() < 1e-9, "{seconds:?}");

    let bytes = h
        .vector(
            r#"max_over_time({app="x"} | logfmt | unwrap bytes(size) [1m]) by (level)"#,
            30,
        )
        .await;
    assert_eq!(
        bytes,
        vec![
            (json!({"level": "error"}), 10.0),
            (json!({"level": "info"}), 2000.0)
        ]
    );

    let median = h
        .vector(
            r#"quantile_over_time(0.5, {app="x"} | logfmt | unwrap duration(took) [1m]) by (app)"#,
            30,
        )
        .await;
    assert_eq!(median, vec![(json!({"app": "x"}), 1.5)]);

    // A value that does not unwrap refuses the query, with Loki's message and fix —
    // unless a `sum` with no labels swallows the error, as Loki's does.
    let (status, body) = h
        .instant(
            r#"sum by (app) (sum_over_time({app="x"} | logfmt | unwrap size [1m]))"#,
            30,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.to_string().contains("SampleExtractionErr"), "{body}");
    let swallowed = h
        .vector(
            r#"sum(sum_over_time({app="x"} | logfmt | unwrap size [1m]))"#,
            30,
        )
        .await;
    assert_eq!(swallowed, vec![(json!({}), 0.0)]);
    let skipped = h
        .vector(
            r#"sum(sum_over_time({app="x"} | logfmt | unwrap size | __error__="" [1m]))"#,
            30,
        )
        .await;
    assert!(skipped.is_empty(), "every size has a unit: {skipped:?}");
}

#[tokio::test]
async fn pipeline_errors_refuse_until_filtered() {
    let h = Harness::with_lines().await;
    let (status, body) = h
        .instant(
            r#"sum by (app) (count_over_time({app="x"} | json [1m]))"#,
            30,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body.to_string().contains("pipeline error: 'JSONParserErr'"),
        "{body}"
    );

    let kept = h
        .vector(
            r#"sum(count_over_time({app="x"} | json | __error__="" [1m]))"#,
            30,
        )
        .await;
    assert!(kept.is_empty(), "{kept:?}");

    // Grafana's log volume: errors dropped, grouped by level.
    let volume = h
        .vector(
            r#"sum by (level, detected_level) (count_over_time({app="x"} | json | drop __error__, __error_details__ [1m]))"#,
            30,
        )
        .await;
    assert_eq!(volume.len(), 2, "{volume:?}");
}

#[tokio::test]
async fn vector_operators_are_promql_s() {
    let h = Harness::with_lines().await;
    let ratio = h
        .vector(
            r#"sum(count_over_time({app="x"} |= "error" [1m])) / sum(count_over_time({app="x"}[1m]))"#,
            30,
        )
        .await;
    assert!((one(&ratio) - 1.0 / 3.0).abs() < 1e-12, "{ratio:?}");

    let top = h
        .vector(
            r#"topk(1, sum by (level) (count_over_time({app="x"}[1m])))"#,
            30,
        )
        .await;
    assert_eq!(top, vec![(json!({"level": "info"}), 2.0)]);

    let over = h
        .vector(r#"sum by (level) (count_over_time({app="x"}[1m])) > 1"#, 30)
        .await;
    assert_eq!(over, vec![(json!({"level": "info"}), 2.0)]);

    let absent = h.vector(r#"absent_over_time({app="nope"}[1m])"#, 30).await;
    assert_eq!(absent, vec![(json!({"app": "nope"}), 1.0)]);

    // Grafana's datasource health check still answers.
    let (status, body) = h.instant("vector(1)+vector(1)", 30).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["result"][0]["value"][1], "2", "{body}");
}

#[tokio::test]
async fn a_range_query_answers_a_matrix() {
    let h = Harness::with_lines().await;
    let (status, body) = h
        .get(
            "/loki/api/v1/query_range",
            &[
                (
                    "query",
                    r#"sum by (level) (count_over_time({app="x"}[30s]))"#.to_owned(),
                ),
                ("start", T.to_string()),
                ("end", (T + 60).to_string()),
                ("step", "15s".to_owned()),
            ],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["data"]["resultType"], "matrix", "{body}");
    let result = body["data"]["result"].as_array().unwrap();
    let info = result
        .iter()
        .find(|s| s["metric"]["level"] == "info")
        .unwrap();
    // Loki's grid is whole steps, so T is rounded down to T-10s: steps at T-10, T+5,
    // T+20, T+35, T+50, T+65 over (t-30s, t] count 1, 2, 1 info lines, then none.
    let points: Vec<(u64, &str)> = info["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (p[0].as_f64().unwrap() as u64 - T, p[1].as_str().unwrap()))
        .collect();
    assert_eq!(points, vec![(5, "1"), (20, "2"), (35, "1")], "{body}");
    let error = result
        .iter()
        .find(|s| s["metric"]["level"] == "error")
        .unwrap();
    // The error line at T+20s is in (-10, 20] and (5, 35], not (20, 50].
    assert_eq!(error["values"].as_array().unwrap().len(), 2, "{body}");
    assert!(body["data"]["stats"]["summary"]["execTime"].is_number());
}

#[tokio::test]
async fn what_logql_lacks_is_refused_by_name() {
    let h = Harness::with_lines().await;
    for (query, says) in [
        ("rate(foo[5m])", "stream selector"),
        (r#"sum_over_time({app="x"}[1m])"#, "unwrap"),
        (
            r#"count_over_time({app="x"} | logfmt | unwrap took [1m])"#,
            "takes no",
        ),
        (r#"count_over_time({app="x"}[1m]) by (app)"#, "grouping"),
        (r#"sum(count_over_time({app="x"}[1m])) + up"#, "Prometheus"),
        (
            r#"histogram_quantile(0.9, count_over_time({app="x"}[1m]))"#,
            "histogram_quantile",
        ),
    ] {
        let (status, body) = h.instant(query, 30).await;
        assert!(
            status.is_client_error() || status == StatusCode::NOT_IMPLEMENTED,
            "{query}: {status} {body}"
        );
        assert!(
            body.to_string().contains(says),
            "{query} should say {says}: {body}"
        );
    }
}
