//! The same metrics, sent to an instance on the local store and to one on ClickHouse,
//! give the same answer to every query — byte for byte, through the Prometheus API.
//!
//! The PromQL corpus holds both stores to Prometheus on promtool's small series. This
//! holds ClickHouse to the local store on what a dashboard sends over what an app
//! sends: OTLP histograms expanded into buckets, a counter that resets, gauges, a
//! `remote_write` series carrying a NaN, infinities and a staleness marker, windows wider
//! than an hour at bounds that fall inside a second, and the label APIs.
//!
//! Run when `TELEMETRYD_TEST_CLICKHOUSE_URL=http://user:password@host:8123` is set.

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use telemetryd_core::Config;
use telemetryd_core::config::{ClickHouseConfig, StorageConfig};
use telemetryd_core::metric::STALE_MARKER_BITS;
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use tower::ServiceExt;

const SECOND: u64 = 1_000_000_000;
/// Two hours before this the data starts; a fixed instant so both stores and every
/// query see the same timestamps.
const END: u64 = 1_790_007_200 * SECOND;
const START: u64 = END - 7_200 * SECOND;
const STEP: u64 = 15 * SECOND;
const DAY: u64 = 86_400 * SECOND;

fn clickhouse() -> Option<ClickHouseConfig> {
    common::clickhouse_under_test("metrics_parity")
}

fn instance(dir: &std::path::Path, clickhouse: ClickHouseConfig) -> (axum::Router, Arc<Store>) {
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
    (
        router(AppState::new(Arc::new(config), Arc::clone(&store)).unwrap()),
        store,
    )
}

/// Run the summariser to the end of the data and past it, as the server's maintenance
/// would over the next hours.
fn summarise(store: &Store, now: u64) -> usize {
    let mut written = 0;
    for _ in 0..64 {
        let done = store.roll_up_metrics(now).unwrap().unwrap();
        written += done.folds_written;
        if done.hours == 0 && done.days == 0 && done.repaired_hours == 0 {
            break;
        }
    }
    written
}

/// Two answers the same up to floating-point rounding: an answer from summaries adds a
/// window up in a different order than one from the samples, as the local store's own
/// summaries do; the conformance corpus holds every road to the same `1e-9`.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(x), Value::String(y)) => {
            x == y
                || match (x.parse::<f64>(), y.parse::<f64>()) {
                    (Ok(x), Ok(y)) => {
                        (x.is_nan() && y.is_nan())
                            || (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0)
                    }
                    _ => false,
                }
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| same(v, w)))
        }
        _ => a == b,
    }
}

fn kv(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

/// A small deterministic generator, so both stores get the same numbers.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
}

/// OTLP batches: a request-duration histogram per route and method, cumulative, one
/// of whose series restarts halfway; and a memory gauge per route.
fn otlp_batches() -> Vec<Value> {
    let routes = ["/geocode", "/reverse", "/batch", "/lists", "/places"];
    let methods = ["GET", "POST"];
    let bounds = [0.01, 0.05, 0.1, 0.5, 1.0];
    let mut rng = Lcg(7);
    let mut state: Vec<(u64, f64, Vec<u64>)> = vec![(0, 0.0, vec![0; 6]); 10];
    let steps = (END - START) / STEP;
    let mut batches = Vec::new();
    let mut points_hist: Vec<Value> = Vec::new();
    let mut points_gauge: Vec<Value> = Vec::new();
    for step in 0..=steps {
        let at = START + step * STEP;
        for (series, slot) in state.iter_mut().enumerate() {
            // The fourth series restarts at the halfway mark: its counters drop to zero.
            if series == 3 && step == steps / 2 {
                *slot = (0, 0.0, vec![0; 6]);
            }
            let requests = rng.next() % 40;
            for _ in 0..requests {
                let bucket = usize::try_from(rng.next() % 6).unwrap();
                slot.2[bucket] += 1;
                #[allow(clippy::cast_precision_loss)]
                {
                    slot.1 += [0.005, 0.03, 0.07, 0.3, 0.8, 2.0][bucket];
                }
            }
            slot.0 += requests;
            let route = routes[series / 2];
            let method = methods[series % 2];
            points_hist.push(json!({
                "timeUnixNano": at.to_string(),
                "count": slot.0.to_string(),
                "sum": slot.1,
                "bucketCounts": slot.2.iter().map(u64::to_string).collect::<Vec<_>>(),
                "explicitBounds": bounds,
                "attributes": [kv("http.route", route), kv("http.request.method", method)],
            }));
            if series % 2 == 0 {
                #[allow(clippy::cast_precision_loss)]
                points_gauge.push(json!({
                    "timeUnixNano": at.to_string(),
                    "asDouble": (rng.next() % 1_000_000) as f64 / 7.0,
                    "attributes": [kv("http.route", route)],
                }));
            }
        }
        if points_hist.len() >= 400 || step == steps {
            batches.push(json!({
                "resourceMetrics": [{
                    "resource": {"attributes": [kv("service.name", "geo")]},
                    "scopeMetrics": [{"metrics": [
                        {
                            "name": "http.server.request.duration",
                            "unit": "s",
                            "histogram": {
                                "aggregationTemporality": 2,
                                "dataPoints": std::mem::take(&mut points_hist),
                            },
                        },
                        {
                            "name": "process.memory",
                            "unit": "By",
                            "gauge": {"dataPoints": std::mem::take(&mut points_gauge)},
                        },
                    ]}],
                }],
            }));
        }
    }
    batches
}

// -- remote_write ---------------------------------------------------------------

fn varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = u8::try_from(value & 0x7f).unwrap();
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

/// A series' labels and its `(value, nanoseconds)` samples.
type WriteSeries<'a> = (Vec<(&'a str, &'a str)>, Vec<(f64, u64)>);

fn remote_write(series: &[WriteSeries<'_>]) -> Vec<u8> {
    let mut request = Vec::new();
    for (labels, samples) in series {
        let mut ts = Vec::new();
        for (name, value) in labels {
            let mut label = delimited(1, name.as_bytes());
            label.extend(delimited(2, value.as_bytes()));
            ts.extend(delimited(1, &label));
        }
        for (value, nanos) in samples {
            let mut sample = key(1, 1);
            sample.extend_from_slice(&value.to_bits().to_le_bytes());
            sample.extend(key(2, 0));
            sample.extend(varint(nanos / 1_000_000));
            ts.extend(delimited(2, &sample));
        }
        request.extend(delimited(1, &ts));
    }
    snap::raw::Encoder::new().compress_vec(&request).unwrap()
}

/// A counter that resets twice, a gauge with a NaN, both infinities and a staleness
/// marker, every 30 s.
fn remote_write_batch() -> Vec<u8> {
    let steps = (END - START) / (30 * SECOND);
    let mut counter = Vec::new();
    let mut gauge = Vec::new();
    let mut total = 0.0;
    for step in 0..=steps {
        let at = START + step * 30 * SECOND;
        if step % 90 == 45 {
            total = 0.0;
        }
        #[allow(clippy::cast_precision_loss)]
        {
            total += (step % 7) as f64 + 0.25;
        }
        counter.push((total, at));
        #[allow(clippy::cast_precision_loss)]
        let value = match step % 50 {
            10 => f64::NAN,
            20 => f64::INFINITY,
            30 => f64::NEG_INFINITY,
            40 => f64::from_bits(STALE_MARKER_BITS),
            n => n as f64 * 1.5,
        };
        gauge.push((value, at));
    }
    remote_write(&[
        (
            vec![
                ("__name__", "jobs_processed_total"),
                ("app", "worker"),
                ("queue", "default"),
            ],
            counter,
        ),
        (
            vec![
                ("__name__", "queue_depth"),
                ("app", "worker"),
                ("queue", "default"),
            ],
            gauge,
        ),
    ])
}

async fn send(app: &axum::Router, request: Request<Body>) -> StatusCode {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let _ = response.into_body().collect().await.unwrap();
    status
}

async fn load(app: &axum::Router) {
    for batch in otlp_batches() {
        let request = Request::post("/v1/metrics")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(batch.to_string()))
            .unwrap();
        assert_eq!(send(app, request).await, StatusCode::OK);
    }
    let request = Request::post("/api/v1/write")
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .header("content-encoding", "snappy")
        .body(Body::from(remote_write_batch()))
        .unwrap();
    assert!(send(app, request).await.is_success());
}

async fn ask(app: &axum::Router, path: &str, params: &[(&str, String)]) -> Value {
    let query: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect();
    let request = Request::get(format!("{path}?{}", query.join("&")))
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let mut value: Value =
        serde_json::from_slice(&body).unwrap_or_else(|_| json!({"status": status.as_u16()}));
    // How much work each store did is its own.
    if let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) {
        data.remove("stats");
    }
    value
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

fn seconds(nanos: u64) -> String {
    format!("{}.{:09}", nanos / SECOND, nanos % SECOND)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn clickhouse_answers_metrics_as_the_local_store_does() {
    let Some(clickhouse) = clickhouse() else {
        eprintln!("TELEMETRYD_TEST_CLICKHOUSE_URL is not set; skipped");
        return;
    };
    let local_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let (local, _) = instance(local_dir.path(), ClickHouseConfig::default());
    let (remote, remote_store) = instance(remote_dir.path(), clickhouse);
    load(&local).await;
    load(&remote).await;
    assert!(summarise(&remote_store, END + 3_600 * SECOND) > 0);

    let bucket = "http_server_request_duration_seconds_bucket";
    let count = "http_server_request_duration_seconds_count";
    let sum = "http_server_request_duration_seconds_sum";
    let expressions = [
        format!("histogram_quantile(0.95, sum by (le) (rate({bucket}[5m])))"),
        format!("histogram_quantile(0.95, sum by (le, http_route) (rate({bucket}[5m])))"),
        format!("histogram_quantile(0.5, sum by (le) (increase({bucket}[1h])))"),
        format!("sum by (http_route, http_request_method) (increase({count}[1h]))"),
        format!("sum by (http_route) (rate({count}[90s]))"),
        format!("sum(rate({sum}[5m])) / sum(rate({count}[5m]))"),
        format!("topk(3, sum by (http_route) (rate({count}[10m])))"),
        format!("{count}{{http_route=~\"/geo.*|/batch\", http_request_method!=\"POST\"}}"),
        "max_over_time(process_memory_bytes[30m])".to_owned(),
        "avg by (http_route) (avg_over_time(process_memory_bytes[20m]))".to_owned(),
        "min_over_time(process_memory_bytes[2h])".to_owned(),
        "rate(jobs_processed_total[5m])".to_owned(),
        "increase(jobs_processed_total[2h])".to_owned(),
        "resets(jobs_processed_total[2h])".to_owned(),
        "queue_depth".to_owned(),
        "last_over_time(queue_depth[10m])".to_owned(),
        "count_over_time(queue_depth[1h])".to_owned(),
        "max_over_time(queue_depth[1h])".to_owned(),
        "absent(nothing_here)".to_owned(),
        "count by (__name__) ({app=~\".+\"})".to_owned(),
    ];

    let mut cases: Vec<(&str, Vec<(&str, String)>)> = Vec::new();
    for expression in &expressions {
        // An instant at the end, one mid-way, a chart at a 60 s step on whole seconds and
        // one whose bounds fall inside a second at an uneven step.
        cases.push((
            "/api/v1/query",
            vec![("query", expression.clone()), ("time", seconds(END))],
        ));
        cases.push((
            "/api/v1/query",
            vec![
                ("query", expression.clone()),
                ("time", seconds(START + 3_601 * SECOND + 250_000_000)),
            ],
        ));
        cases.push((
            "/api/v1/query_range",
            vec![
                ("query", expression.clone()),
                ("start", seconds(START + 600 * SECOND)),
                ("end", seconds(END)),
                ("step", "60".to_owned()),
            ],
        ));
        cases.push((
            "/api/v1/query_range",
            vec![
                ("query", expression.clone()),
                ("start", seconds(START + 7 * SECOND + 125_000_000)),
                ("end", seconds(END - 3 * SECOND)),
                ("step", "47.5".to_owned()),
            ],
        ));
    }
    let range = || vec![("start", seconds(START)), ("end", seconds(END))];
    cases.push(("/api/v1/labels", range()));
    cases.push(("/api/v1/label/__name__/values", range()));
    cases.push(("/api/v1/label/http_route/values", range()));
    let mut series = range();
    series.push(("match[]", format!("{count}{{http_route=\"/geocode\"}}")));
    cases.push(("/api/v1/series", series));
    let mut scoped = range();
    scoped.push(("match[]", "{app=\"worker\"}".to_owned()));
    cases.push(("/api/v1/labels", scoped));

    let mut differences = Vec::new();
    let mut answered_with_data = 0usize;
    for (path, params) in &cases {
        let want = ask(&local, path, params).await;
        let got = ask(&remote, path, params).await;
        let data = &want["data"];
        if want["status"] == "success"
            && (data["result"].as_array().is_some_and(|r| !r.is_empty())
                || data.as_array().is_some_and(|r| !r.is_empty()))
        {
            answered_with_data += 1;
        }
        if !same(&want, &got) {
            differences.push(format!(
                "{path} {:?}\n  local:      {}\n  clickhouse: {}",
                params.first().map(|(_, v)| v),
                want.to_string().chars().take(500).collect::<String>(),
                got.to_string().chars().take(500).collect::<String>(),
            ));
        }
    }
    let answered = cases.len();
    // Two empty answers agree too; this is only worth something if they say something.
    assert!(
        answered_with_data * 10 >= answered * 9,
        "only {answered_with_data} of {answered} answers held data"
    );
    assert!(
        differences.is_empty(),
        "{} of {answered} answers differ:\n{}",
        differences.len(),
        differences.join("\n")
    );
}

/// Three days of counters and a gauge, every minute: windows of days answered from
/// daily summaries, of hours from hourly ones, with uneven edges read as samples — and
/// the same answers after late data lands in an hour already summarised.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn summaries_answer_long_windows_as_the_samples_do() {
    let Some(clickhouse) = clickhouse() else {
        eprintln!("TELEMETRYD_TEST_CLICKHOUSE_URL is not set; skipped");
        return;
    };
    // Three whole days from a UTC midnight, so days in the data are days in the table.
    let start = (END / DAY - 3) * DAY;
    let end = start + 3 * DAY;
    let minute = 60 * SECOND;
    let steps = (end - start) / minute;
    let mut requests = Vec::new();
    let mut errors = Vec::new();
    let mut memory = Vec::new();
    let (mut r, mut e) = (0.0f64, 0.0f64);
    for step in 1..=steps {
        let at = start + step * minute;
        if step % 1_500 == 0 {
            r = 0.0;
        }
        #[allow(clippy::cast_precision_loss)]
        {
            r += (step % 11) as f64 * 0.75 + 0.1;
            e += (step % 3) as f64;
            memory.push(((step % 977) as f64 * 3.25, at));
        }
        requests.push((r, at));
        errors.push((e, at));
    }
    let batch = remote_write(&[
        (
            vec![
                ("__name__", "requests_total"),
                ("app", "geo"),
                ("route", "/geocode"),
            ],
            requests,
        ),
        (
            vec![
                ("__name__", "requests_total"),
                ("app", "geo"),
                ("route", "/reverse"),
            ],
            errors,
        ),
        (vec![("__name__", "memory_bytes"), ("app", "geo")], memory),
    ]);
    let local_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let (local, _) = instance(local_dir.path(), ClickHouseConfig::default());
    let (remote, remote_store) = instance(remote_dir.path(), clickhouse);
    for app in [&local, &remote] {
        let request = Request::post("/api/v1/write")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header("content-encoding", "snappy")
            .body(Body::from(batch.clone()))
            .unwrap();
        assert!(send(app, request).await.is_success());
    }
    assert!(summarise(&remote_store, end + 2 * 3_600 * SECOND) > 0);

    let expressions = [
        "increase(requests_total[1d])",
        "sum by (route) (rate(requests_total[6h]))",
        "rate(requests_total[2d])",
        "sum(increase(requests_total[3d]))",
        "resets(requests_total[1d])",
        "sum_over_time(memory_bytes[1d])",
        "max_over_time(memory_bytes[2d])",
        "min_over_time(memory_bytes[1d])",
        "avg_over_time(memory_bytes[12h])",
        "count_over_time(memory_bytes[1d])",
    ];
    let mut cases: Vec<(&str, Vec<(&str, String)>)> = Vec::new();
    for expression in expressions {
        for time in [
            end,
            start + 2 * DAY,
            start + 2 * DAY + 37 * minute + 125_000_000,
        ] {
            cases.push((
                "/api/v1/query",
                vec![("query", expression.to_owned()), ("time", seconds(time))],
            ));
        }
        for (from, step) in [
            (start + DAY, "3600"),
            (start + DAY, "86400"),
            (start + DAY + 7 * minute, "5400"),
        ] {
            cases.push((
                "/api/v1/query_range",
                vec![
                    ("query", expression.to_owned()),
                    ("start", seconds(from)),
                    ("end", seconds(end)),
                    ("step", step.to_owned()),
                ],
            ));
        }
    }

    let compare = |round: &'static str| {
        let cases = cases.clone();
        let local = local.clone();
        let remote = remote.clone();
        async move {
            let mut differences = Vec::new();
            for (path, params) in &cases {
                let want = ask(&local, path, params).await;
                let got = ask(&remote, path, params).await;
                if !same(&want, &got) {
                    differences.push(format!(
                        "{round} {path} {:?}\n  local:      {}\n  clickhouse: {}",
                        params.iter().map(|(_, v)| v.as_str()).collect::<Vec<_>>(),
                        want.to_string().chars().take(400).collect::<String>(),
                        got.to_string().chars().take(400).collect::<String>(),
                    ));
                }
            }
            differences
        }
    };
    let mut differences = compare("summarised").await;

    // Late data: a sample for an hour of the first day, long summarised.
    let late = remote_write(&[(
        vec![("__name__", "memory_bytes"), ("app", "geo")],
        vec![(123_456.0, start + 5 * 3_600 * SECOND + 30 * SECOND)],
    )]);
    for app in [&local, &remote] {
        let request = Request::post("/api/v1/write")
            .header(header::CONTENT_TYPE, "application/x-protobuf")
            .header("content-encoding", "snappy")
            .body(Body::from(late.clone()))
            .unwrap();
        assert!(send(app, request).await.is_success());
    }
    differences.extend(compare("with late data").await);
    // And once the summariser has made that hour and its day again.
    summarise(&remote_store, end + 3 * 3_600 * SECOND);
    differences.extend(compare("repaired").await);

    assert!(
        differences.is_empty(),
        "{} of {} answers differ:\n{}",
        differences.len(),
        cases.len() * 3,
        differences.join("\n")
    );
}
