//! Tempo answers, held to what Tempo itself answers.
//!
//! `conformance/tempo.json` holds OTLP traces, queries — a trace by id, TraceQL search,
//! tag listings and values, TraceQL metrics — and Tempo's answer to each, written by
//! Tempo through `scripts/tempo-conformance.py --update` and re-asked of Tempo in CI.
//! Here the same traces go in through `/v1/traces`, the same queries are asked, and the
//! answers have to match, reduced the way the script reduces Tempo's.

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
async fn every_query_answers_as_tempo_does() {
    let doc: Value = serde_json::from_str(include_str!("conformance/tempo.json")).unwrap();
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

    let send = Request::post("/v1/traces")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(doc["traces"].to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(send).await.unwrap().status(),
        StatusCode::OK
    );

    let cases = doc["cases"].as_array().unwrap();
    let mut failures = Vec::new();
    for case in cases {
        let got = ask(&app, case).await;
        if !close(&got, &case["expect"]) {
            let mut shown = case.clone();
            shown.as_object_mut().unwrap().remove("expect");
            failures.push(format!(
                "{shown}\n  tempo:      {}\n  telemetryd: {}",
                clip(&case["expect"]),
                clip(&got)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} answers differ from Tempo:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

/// `TEMPO_CONFORMANCE_FULL=1` shows the answers whole.
fn clip(value: &Value) -> String {
    let text = value.to_string();
    if std::env::var_os("TEMPO_CONFORMANCE_FULL").is_some() {
        return text;
    }
    text.chars().take(600).collect()
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

fn query(case: &Value, keys: &[&str]) -> String {
    keys.iter()
        .filter_map(|key| {
            case.get(*key).map(|v| {
                let v = v.as_str().map_or_else(|| v.to_string(), str::to_owned);
                format!("{key}={}", encode(&v))
            })
        })
        .collect::<Vec<_>>()
        .join("&")
}

async fn ask(app: &axum::Router, case: &Value) -> Value {
    let kind = case["kind"].as_str().unwrap();
    let path = match kind {
        "trace" => format!("/api/traces/{}", case["id"].as_str().unwrap()),
        "search" => format!(
            "/api/search?{}",
            query(case, &["q", "start", "end", "limit", "minDuration"])
        ),
        "tags" => format!(
            "/api/v2/search/tags?{}",
            query(case, &["scope", "start", "end"])
        ),
        "values" => format!(
            "/api/v2/search/tag/{}/values?{}",
            case["tag"].as_str().unwrap(),
            query(case, &["start", "end"])
        ),
        _ => format!(
            "/api/metrics/query_range?{}",
            query(case, &["q", "start", "end", "step"])
        ),
    };
    let request = Request::get(path).body(Body::empty()).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    if status != StatusCode::OK {
        return json!({"error": status.as_u16()});
    }
    canonical(kind, &serde_json::from_slice(&body).unwrap())
}

fn attribute_map(attributes: &Value) -> Value {
    let mut out = Map::new();
    for kv in attributes.as_array().into_iter().flatten() {
        let value = kv["value"]
            .as_object()
            .and_then(|v| v.values().next())
            .map_or_else(String::new, |v| {
                v.as_str().map_or_else(|| v.to_string(), str::to_owned)
            });
        out.insert(kv["key"].as_str().unwrap().to_owned(), json!(value));
    }
    sorted(&Value::Object(out))
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
            .collect(),
    )
}

fn nanos(value: &Value) -> Value {
    json!(
        value
            .as_str()
            .map_or_else(|| value.as_u64().unwrap(), |s| s.parse().unwrap())
    )
}

/// A trace, span by span, with its resource beside each.
fn canonical_trace(body: &Value) -> Value {
    let mut spans = Vec::new();
    for batch in body["batches"].as_array().into_iter().flatten() {
        let resource = attribute_map(&batch["resource"]["attributes"]);
        for scope in batch["scopeSpans"].as_array().into_iter().flatten() {
            for s in scope["spans"].as_array().into_iter().flatten() {
                let status = &s["status"];
                spans.push(json!({
                    "resource": resource,
                    "spanId": s["spanId"],
                    "parentSpanId": s.get("parentSpanId").cloned().unwrap_or(json!("")),
                    "name": s["name"],
                    "kind": s["kind"],
                    "start": nanos(&s["startTimeUnixNano"]),
                    "end": nanos(&s["endTimeUnixNano"]),
                    "status": [
                        status.get("code").cloned().unwrap_or(json!(0)),
                        status.get("message").cloned().unwrap_or(json!("")),
                    ],
                    "attributes": attribute_map(&s["attributes"]),
                    "events": s["events"].as_array().into_iter().flatten().map(|e| json!([
                        nanos(&e["timeUnixNano"]), e["name"], attribute_map(&e["attributes"])
                    ])).collect::<Vec<_>>(),
                }));
            }
        }
    }
    spans.sort_by_key(|s| s["spanId"].to_string());
    json!({"spans": spans})
}

/// The answer as `scripts/tempo-conformance.py` reduces Tempo's.
fn canonical(kind: &str, body: &Value) -> Value {
    match kind {
        "trace" => canonical_trace(body),
        "search" => {
            let mut rows: Vec<Value> = body["traces"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|t| {
                    let mut matched: Vec<String> = t["spanSets"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .flat_map(|ss| ss["spans"].as_array().cloned().unwrap_or_default())
                        .map(|s| s["spanID"].as_str().unwrap().to_owned())
                        .collect();
                    matched.sort();
                    json!({
                        "traceID": t["traceID"],
                        "rootServiceName": t["rootServiceName"],
                        "rootTraceName": t["rootTraceName"],
                        "start": nanos(&t["startTimeUnixNano"]),
                        "durationMs": t.get("durationMs").cloned().unwrap_or(json!(0)),
                        "matched": matched,
                    })
                })
                .collect();
            rows.sort_by_key(|r| r["traceID"].to_string());
            json!({"traces": rows})
        }
        "tags" => {
            let mut tags: Vec<String> = body["scopes"]
                .as_array()
                .into_iter()
                .flatten()
                .flat_map(|s| s["tags"].as_array().cloned().unwrap_or_default())
                .map(|t| t.as_str().unwrap().to_owned())
                .collect();
            tags.sort();
            json!({"tags": tags})
        }
        "values" => {
            let mut values: Vec<String> = body["tagValues"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|v| v["value"].as_str().unwrap().to_owned())
                .collect();
            values.sort();
            json!({"values": values})
        }
        _ => {
            let mut series: Vec<Value> = body["series"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|s| {
                    let labels = attribute_map(&s["labels"]);
                    let mut points: Vec<Value> = s["samples"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|p| {
                            let ms: u64 = p["timestampMs"].as_str().unwrap().parse().unwrap();
                            json!([
                                ms / 1000,
                                p.get("value").and_then(Value::as_f64).unwrap_or(0.0)
                            ])
                        })
                        .collect();
                    points.sort_by_key(|p| p[0].as_u64());
                    json!({"labels": labels, "points": points})
                })
                .collect();
            series.sort_by_key(|s| s["labels"].to_string());
            json!({"series": series})
        }
    }
}

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
