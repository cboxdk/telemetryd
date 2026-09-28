//! LogQL answers, held to what Loki itself answers.
//!
//! `conformance/logql.json` holds log lines, queries, and Loki's answer to each —
//! written by Loki through `scripts/loki-conformance.py --update`, and re-asked of Loki
//! in CI, so the expectations cannot drift from what Loki says. Here the same lines go
//! in through `/loki/api/v1/push`, the same queries are asked, and the answers have to
//! match: every stream and its labels, every entry with its structured metadata and
//! parsed labels, every series and value. Grafana's `categorize-labels` shape is the
//! one compared, because it is what Grafana reads.

#![allow(clippy::unwrap_used, clippy::float_cmp)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Map, Value, json};
use telemetryd_core::Config;
use telemetryd_core::config::StorageConfig;
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use tower::ServiceExt;

#[tokio::test]
async fn every_query_answers_as_loki_does() {
    let doc: Value = serde_json::from_str(include_str!("conformance/logql.json")).unwrap();
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
    let app = router(AppState::new(Arc::new(config), store).unwrap());

    let push = Request::post("/loki/api/v1/push")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({"streams": doc["lines"]}).to_string()))
        .unwrap();
    let response = app.clone().oneshot(push).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let cases = doc["cases"].as_array().unwrap();
    let mut failures = Vec::new();
    for case in cases {
        let query = case["query"].as_str().unwrap();
        let got = ask(&app, case).await;
        if !close(&got, &case["expect"]) {
            failures.push(format!(
                "{query}\n  loki:       {}\n  telemetryd: {}",
                clip(&case["expect"]),
                clip(&got)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} answers differ from Loki:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

/// Long enough to see where two answers part; `LOGQL_CONFORMANCE_FULL=1` shows them whole.
fn clip(value: &Value) -> String {
    let text = value.to_string();
    if std::env::var_os("LOGQL_CONFORMANCE_FULL").is_some() {
        return text;
    }
    text.chars().take(600).collect()
}

async fn ask(app: &axum::Router, case: &Value) -> Value {
    let path = if case["kind"] == "range" {
        "query_range"
    } else {
        "query"
    };
    let params: Vec<String> = case
        .as_object()
        .unwrap()
        .iter()
        .filter(|(k, _)| *k != "kind" && *k != "expect")
        .map(|(k, v)| {
            let v = v.as_str().map_or_else(|| v.to_string(), str::to_owned);
            format!("{k}={}", encode(&v))
        })
        .collect();
    let request = Request::get(format!("/loki/api/v1/{path}?{}", params.join("&")))
        .header("X-Loki-Response-Encoding-Flags", "categorize-labels")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    if status != StatusCode::OK {
        return json!({"error": status.as_u16()});
    }
    canonical(&serde_json::from_slice(&body).unwrap())
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

fn sorted(map: &Value) -> Value {
    let mut entries: Vec<(&String, &Value)> = map
        .as_object()
        .map(|m| m.iter().collect())
        .unwrap_or_default();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    Value::Object(
        entries
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Map<_, _>>(),
    )
}

fn number(value: &Value) -> Value {
    json!(
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
            .unwrap()
    )
}

/// The answer as `scripts/loki-conformance.py` reduces Loki's: the same fields, ordered
/// the same way.
fn canonical(body: &Value) -> Value {
    let data = &body["data"];
    let result = data["result"].as_array();
    let by_labels = |key: &str, mut items: Vec<Value>| {
        items.sort_by_key(|item| item[key].to_string());
        items
    };
    match data["resultType"].as_str().unwrap() {
        "streams" => {
            let streams = result
                .unwrap()
                .iter()
                .map(|s| {
                    let entries: Vec<Value> = s["values"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| {
                            let extra = v.get(2).cloned().unwrap_or(Value::Null);
                            json!([
                                v[0],
                                v[1],
                                sorted(&extra["structuredMetadata"]),
                                sorted(&extra["parsed"])
                            ])
                        })
                        .collect();
                    json!({"stream": sorted(&s["stream"]), "entries": entries})
                })
                .collect();
            json!({"streams": by_labels("stream", streams)})
        }
        "matrix" => {
            let series = result
                .unwrap()
                .iter()
                .map(|s| {
                    let values: Vec<Value> = s["values"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|p| json!([number(&p[0]), number(&p[1])]))
                        .collect();
                    json!({"metric": sorted(&s["metric"]), "values": values})
                })
                .collect();
            json!({"matrix": by_labels("metric", series)})
        }
        "vector" => {
            let series = result
                .unwrap()
                .iter()
                .map(|s| {
                    json!({
                        "metric": sorted(&s["metric"]),
                        "value": [number(&s["value"][0]), number(&s["value"][1])],
                    })
                })
                .collect();
            json!({"vector": by_labels("metric", series)})
        }
        _ => json!({"scalar": [number(&data["result"][0]), number(&data["result"][1])]}),
    }
}

/// Equal, with floats allowed the last bits of rounding; an expected error matches on
/// its status alone.
fn close(got: &Value, expected: &Value) -> bool {
    if let Some(status) = expected.get("error") {
        return got.get("error") == Some(status);
    }
    match (got, expected) {
        (Value::Number(a), Value::Number(b)) => {
            let (a, b) = (a.as_f64().unwrap(), b.as_f64().unwrap());
            a == b || (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
        }
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| close(x, y))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|w| close(v, w)))
        }
        _ => got == expected,
    }
}
