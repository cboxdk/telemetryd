//! Compressed ingest bodies, end to end.
//!
//! gzip is part of OTLP/HTTP, not an optional extra, and it failed in the field in the
//! most misleading way available: `cboxdk/laravel-telemetry` gzips only above a size
//! threshold, so the empty batch a health check sends got a 200 and reported "all
//! checks passed" while the 8.6 KB batch behind it got a 400 and vanished. The
//! operator saw a healthy diagnostic, a successful flush, and no data.
//!
//! So these tests do not stop at the status code. A compressed batch has to be
//! *queryable afterwards* through the same read API the UI uses — the assertion the
//! health check was missing.
//!
//! The other half is that decompressing on an ingest endpoint is a bomb surface. The
//! cap is `server.max_body_bytes`, the same limit the uncompressed path is held to,
//! and it is asserted from both sides: a body that fits is accepted, a few KB of gzip
//! that would expand past it is refused with the same 413 the raw body would have got.

#![allow(clippy::unwrap_used)]

use std::io::Write;
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

const NOW: u64 = 1_750_000_000_000_000_000;
const NOW_SECONDS: u64 = 1_750_000_000;

struct Harness {
    router: axum::Router,
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
            router: router(state),
            _tmp: tmp,
        }
    }

    /// POST a body with whatever `Content-Encoding` the caller names — or none.
    async fn post(
        &self,
        path: &str,
        content_type: &str,
        encoding: Option<&str>,
        body: Vec<u8>,
    ) -> (StatusCode, Value) {
        let mut request = Request::post(path).header(header::CONTENT_TYPE, content_type);
        if let Some(encoding) = encoding {
            request = request.header(header::CONTENT_ENCODING, encoding);
        }
        let (status, text) = self.send(request.body(Body::from(body)).unwrap()).await;
        (status, serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    async fn post_otlp(
        &self,
        path: &str,
        encoding: Option<&str>,
        body: Vec<u8>,
    ) -> (StatusCode, Value) {
        self.post(path, "application/json", encoding, body).await
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let (status, text) = self
            .send(Request::get(path).body(Body::empty()).unwrap())
            .await;
        (status, serde_json::from_str(&text).unwrap_or(Value::Null))
    }

    async fn send(&self, request: Request<Body>) -> (StatusCode, String) {
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }
}

// ---------------------------------------------------------------------------
// compressors — the client side of the contract
// ---------------------------------------------------------------------------

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn zlib(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
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

// ---------------------------------------------------------------------------
// payloads
// ---------------------------------------------------------------------------

fn otlp_logs(line: &str) -> Vec<u8> {
    json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeLogs": [{"logRecords": [{
                "timeUnixNano": NOW.to_string(),
                "severityNumber": 9,
                "severityText": "INFO",
                "body": {"stringValue": line},
                "attributes": [],
            }]}]
        }]
    })
    .to_string()
    .into_bytes()
}

fn otlp_traces() -> Vec<u8> {
    json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeSpans": [{"spans": [{
                "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
                "spanId": "00f067aa0ba902b7",
                "name": "GET /checkout",
                "kind": 2,
                "startTimeUnixNano": NOW.to_string(),
                "endTimeUnixNano": (NOW + 5_000_000).to_string(),
                "attributes": [],
            }]}]
        }]
    })
    .to_string()
    .into_bytes()
}

fn otlp_metrics() -> Vec<u8> {
    json!({
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeMetrics": [{"metrics": [{
                "name": "queue.depth",
                "gauge": {"dataPoints": [{
                    "timeUnixNano": NOW.to_string(),
                    "asDouble": 42.0,
                }]}
            }]}]
        }]
    })
    .to_string()
    .into_bytes()
}

/// A batch big enough that a real SDK would compress it, and compressible enough that
/// the compressed form is nothing like the decompressed one.
fn big_otlp_logs(records: usize) -> Vec<u8> {
    let records: Vec<Value> = (0..records)
        .map(|i| {
            json!({
                "timeUnixNano": (NOW + i as u64).to_string(),
                "severityNumber": 9,
                "severityText": "INFO",
                "body": {"stringValue": format!("order {i} processed {}", "-".repeat(200))},
                "attributes": [],
            })
        })
        .collect();

    json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeLogs": [{"logRecords": records}]
        }]
    })
    .to_string()
    .into_bytes()
}

fn loki_range(query: &str) -> String {
    format!(
        "/loki/api/v1/query_range?query={}&start={}&end={}&limit=5000",
        urlencode(query),
        NOW - 3_600_000_000_000,
        NOW + 3_600_000_000_000
    )
}

// ---------------------------------------------------------------------------
// the thing that was broken
// ---------------------------------------------------------------------------

/// The regression, stated as the product cares about it: a gzipped batch is stored,
/// and it comes back out of the read API.
#[tokio::test]
async fn a_gzipped_log_batch_is_accepted_and_queryable() {
    let harness = Harness::new();

    let (status, body) = harness
        .post_otlp(
            "/v1/logs",
            Some("gzip"),
            gzip(&otlp_logs("payment declined")),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({}), "a clean batch reports no partial success");

    let (status, response) = harness.get(&loki_range(r#"{app="checkout"}"#)).await;
    assert_eq!(status, StatusCode::OK);
    let streams = response["data"]["result"].as_array().unwrap();
    let lines: Vec<&str> = streams
        .iter()
        .flat_map(|s| s["values"].as_array().unwrap())
        .map(|v| v[1].as_str().unwrap())
        .collect();
    assert_eq!(
        lines,
        vec!["payment declined"],
        "the gzipped batch has to be readable, not merely acknowledged"
    );
}

#[tokio::test]
async fn a_gzipped_trace_batch_is_accepted_and_queryable() {
    let harness = Harness::new();

    let (status, body) = harness
        .post_otlp("/v1/traces", Some("gzip"), gzip(&otlp_traces()))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, response) = harness
        .get("/api/traces/4bf92f3577b34da6a3ce929d0e0e4736")
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let spans = response["batches"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .unwrap();
    assert_eq!(spans[0]["name"], "GET /checkout");
}

#[tokio::test]
async fn a_gzipped_metric_batch_is_accepted_and_queryable() {
    let harness = Harness::new();

    let (status, body) = harness
        .post_otlp("/v1/metrics", Some("gzip"), gzip(&otlp_metrics()))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, response) = harness
        .get(&format!(
            "/api/v1/query?query={}&time={}",
            urlencode("queue_depth"),
            NOW_SECONDS + 1
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{response}");
    let result = response["data"]["result"].as_array().unwrap();
    assert_eq!(result.len(), 1, "{response}");
    assert_eq!(result[0]["value"][1], "42");
}

/// The size the field bug actually involved — past the threshold where an SDK switches
/// compression on, which is the whole reason the small case looked healthy.
#[tokio::test]
async fn a_realistically_sized_gzipped_batch_round_trips() {
    let harness = Harness::new();
    let payload = big_otlp_logs(500);
    let compressed = gzip(&payload);

    assert!(payload.len() > 100_000, "got {} bytes", payload.len());
    assert!(
        compressed.len() * 10 < payload.len(),
        "the compressed form should be nothing like the decompressed one"
    );

    let (status, body) = harness
        .post_otlp("/v1/logs", Some("gzip"), compressed)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (_, response) = harness.get(&loki_range(r#"{app="checkout"}"#)).await;
    let stored: usize = response["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["values"].as_array().unwrap().len())
        .sum();
    assert_eq!(stored, 500);
}

// ---------------------------------------------------------------------------
// the other codings, and the ones we do not speak
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_supported_coding_is_accepted_on_every_otlp_endpoint() {
    for (path, payload) in [
        ("/v1/logs", otlp_logs("hello") as Vec<u8>),
        ("/v1/traces", otlp_traces()),
        ("/v1/metrics", otlp_metrics()),
    ] {
        for (encoding, body) in [
            (Some("gzip"), gzip(&payload)),
            (Some("x-gzip"), gzip(&payload)),
            (Some("GZIP"), gzip(&payload)),
            (Some("deflate"), zlib(&payload)),
            (Some("zstd"), zstd::encode_all(&payload[..], 3).unwrap()),
            // `identity` and an absent header have to behave exactly as before.
            (Some("identity"), payload.clone()),
            (None, payload.clone()),
        ] {
            let harness = Harness::new();
            let (status, response) = harness.post_otlp(path, encoding, body).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{path} with Content-Encoding: {encoding:?} — {response}"
            );
        }
    }
}

/// A coding we cannot undo must say which one, not hand back a parse error pointing at
/// the compressed magic bytes. That confusion is what cost the original afternoon.
#[tokio::test]
async fn an_unsupported_coding_is_refused_by_name() {
    let harness = Harness::new();

    for path in ["/v1/logs", "/v1/traces", "/v1/metrics"] {
        let (status, response) = harness
            .post_otlp(path, Some("br"), b"\x1b\x0e\x00\xf8brotli".to_vec())
            .await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {response}");
        assert_eq!(response["error"]["code"], "unsupported_feature");
        assert_eq!(response["error"]["feature"], "Content-Encoding: br");
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("br"),
            "the message has to name the coding: {response}"
        );
        assert!(response["error"]["hint"].is_string());
    }
}

#[tokio::test]
async fn stacked_codings_are_refused_rather_than_half_decoded() {
    let harness = Harness::new();
    let (status, response) = harness
        .post_otlp("/v1/logs", Some("gzip, gzip"), gzip(&gzip(&otlp_logs("x"))))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"]["code"], "unsupported_feature");
}

/// A body that lies about its coding is a bad request naming the coding — and a body
/// that is genuinely bad JSON still gets the message it always did, which is the one
/// that tells you where the JSON went wrong.
#[tokio::test]
async fn the_two_kinds_of_decode_failure_stay_distinguishable() {
    let harness = Harness::new();

    let (status, response) = harness
        .post_otlp("/v1/metrics", Some("gzip"), otlp_metrics())
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"]["code"], "bad_request");
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("gzip"),
        "{response}"
    );

    let (status, response) = harness
        .post_otlp("/v1/metrics", Some("gzip"), gzip(b"not json at all"))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"]["code"], "bad_request");
    let message = response["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("could not decode the OTLP metrics payload"),
        "the JSON error must survive decompression unchanged: {message}"
    );
    assert!(
        message.contains("line 1 column 1"),
        "and keep serde's position, which is the useful part: {message}"
    );
}

// ---------------------------------------------------------------------------
// the bomb
// ---------------------------------------------------------------------------

/// `server.max_body_bytes` bounds the *decompressed* body.
///
/// Without this, 30 KB of gzip is a request for gigabytes of resident memory, on an
/// endpoint that is by definition open to whatever the network sends. The compressed
/// body sails through `RequestBodyLimitLayer` — that layer only ever saw 30 KB.
#[tokio::test]
async fn a_gzip_bomb_is_refused_at_the_same_limit_the_raw_body_would_be() {
    let harness = Harness::configured(|config| {
        config.server.max_body_bytes = bytesize::ByteSize::mib(1);
    });

    let bomb = gzip(&vec![b'{'; 256 * 1024 * 1024]);
    assert!(
        bomb.len() < 512 * 1024,
        "the compressed body has to slip under the request-body limit for the test to \
         mean anything, got {} bytes",
        bomb.len()
    );

    let (status, response) = harness.post_otlp("/v1/logs", Some("gzip"), bomb).await;

    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "a compressed body must hit the same 413 the raw one does: {response}"
    );
    assert_eq!(response["error"]["code"], "limit_exceeded");
    assert!(
        response["error"]["message"]
            .as_str()
            .unwrap()
            .contains("server.max_body_bytes"),
        "the error has to name the setting an operator would change: {response}"
    );
}

#[tokio::test]
async fn the_cap_holds_for_every_coding_and_every_endpoint() {
    let big = vec![b' '; 32 * 1024 * 1024];

    for path in ["/v1/logs", "/v1/traces", "/v1/metrics", "/api/v1/write"] {
        for (encoding, body) in [
            ("gzip", gzip(&big)),
            ("deflate", zlib(&big)),
            ("zstd", zstd::encode_all(&big[..], 3).unwrap()),
        ] {
            let harness = Harness::configured(|config| {
                config.server.max_body_bytes = bytesize::ByteSize::mib(1);
            });
            let (status, response) = harness
                .post(path, "application/json", Some(encoding), body)
                .await;
            assert_eq!(
                status,
                StatusCode::PAYLOAD_TOO_LARGE,
                "{path} with {encoding}: {response}"
            );
        }
    }
}

/// The cap is the configured number, not a hardcoded one: raise it and a body that was
/// refused goes through. A limit that cannot be tuned is the bug this repo already
/// fixed once for uncompressed bodies.
#[tokio::test]
async fn the_cap_tracks_the_configured_value() {
    let payload = big_otlp_logs(4_000);
    assert!(payload.len() > 1024 * 1024, "got {} bytes", payload.len());
    let compressed = gzip(&payload);

    let tight = Harness::configured(|config| {
        config.server.max_body_bytes = bytesize::ByteSize::kib(512);
    });
    let (status, _) = tight
        .post_otlp("/v1/logs", Some("gzip"), compressed.clone())
        .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

    let roomy = Harness::configured(|config| {
        config.server.max_body_bytes = bytesize::ByteSize::mib(16);
    });
    let (status, response) = roomy.post_otlp("/v1/logs", Some("gzip"), compressed).await;
    assert_eq!(status, StatusCode::OK, "{response}");
}

// ---------------------------------------------------------------------------
// remote_write
// ---------------------------------------------------------------------------

/// `Content-Encoding: snappy` on `remote_write` names the payload's own framing, which
/// `remote_write::decode` owns. It must pass through this layer untouched rather than
/// being unwrapped twice or refused as unknown — Prometheus sends it on every request.
#[tokio::test]
async fn remote_write_still_speaks_snappy() {
    let harness = Harness::new();
    let (status, _) = harness
        .post(
            "/api/v1/write",
            "application/x-protobuf",
            Some("snappy"),
            remote_write_payload(),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

/// And a proxy that gzips it on the way in is still readable.
#[tokio::test]
async fn remote_write_survives_a_gzip_wrapper() {
    let harness = Harness::new();
    let (status, _) = harness
        .post(
            "/api/v1/write",
            "application/x-protobuf",
            Some("gzip"),
            gzip(&remote_write_payload()),
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

/// One snappy-framed `WriteRequest`: `queue_depth{app="checkout"} 42`.
fn remote_write_payload() -> Vec<u8> {
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
    fn key(field: u64, wire: u64) -> Vec<u8> {
        varint((field << 3) | wire)
    }
    fn delimited(field: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = key(field, 2);
        out.extend(varint(payload.len() as u64));
        out.extend_from_slice(payload);
        out
    }

    let mut ts = Vec::new();
    for (name, value) in [("__name__", "queue_depth"), ("app", "checkout")] {
        let mut label = delimited(1, name.as_bytes());
        label.extend(delimited(2, value.as_bytes()));
        ts.extend(delimited(1, &label));
    }
    let mut sample = key(1, 1);
    sample.extend_from_slice(&42.0f64.to_bits().to_le_bytes());
    sample.extend(key(2, 0));
    sample.extend(varint(NOW / 1_000_000));
    ts.extend(delimited(2, &sample));

    snap::raw::Encoder::new()
        .compress_vec(&delimited(1, &ts))
        .unwrap()
}
