//! OTLP/gRPC: an `Export` call framed as gRPC, answered through the OTLP/HTTP handlers,
//! with the outcome in `grpc-status` where a gRPC exporter reads it.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use telemetryd_core::Config;
use telemetryd_core::config::StorageConfig;
use telemetryd_server::{AppState, grpc, router};
use telemetryd_store::Store;
use tower::ServiceExt;

const LOGS: &str = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

fn field(number: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![(number << 3) | 2];
    let mut length = payload.len();
    while length >= 0x80 {
        out.push(u8::try_from(length & 0x7f).unwrap() | 0x80);
        length >>= 7;
    }
    out.push(u8::try_from(length).unwrap());
    out.extend_from_slice(payload);
    out
}

/// `ExportLogsServiceRequest` with one record from `grpc-app`.
fn export_logs(line: &str, nanos: u64) -> Vec<u8> {
    let string = |s: &str| field(1, s.as_bytes());
    let attribute = [field(1, b"service.name"), field(2, &string("grpc-app"))].concat();
    let resource = field(1, &attribute);
    let mut record = vec![0x09]; // time_unix_nano, fixed64
    record.extend_from_slice(&nanos.to_le_bytes());
    record.extend(field(3, b"ERROR"));
    record.extend(field(5, &string(line)));
    let scope_logs = field(2, &record);
    let resource_logs = [field(1, &resource), field(2, &scope_logs)].concat();
    field(1, &resource_logs)
}

fn frame(flag: u8, message: &[u8]) -> Vec<u8> {
    let mut out = vec![flag];
    out.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
    out.extend_from_slice(message);
    out
}

struct Harness {
    app: axum::Router,
    grpc: axum::Router,
    _tmp: tempfile::TempDir,
}

impl Harness {
    fn new(token: Option<&str>) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config {
            storage: StorageConfig {
                data_dir: Some(tmp.path().join("data")),
                ..StorageConfig::default()
            },
            ..Config::default()
        };
        if let Some(token) = token {
            config.auth.ingest_token = serde_json::from_value(token.into()).unwrap();
        }
        config.validate().unwrap();
        let store = Arc::new(Store::open(&config).unwrap());
        let state = AppState::new(Arc::new(config), store).unwrap();
        let app = router(state);
        Self {
            grpc: grpc::router(app.clone(), 1 << 20),
            app,
            _tmp: tmp,
        }
    }

    /// `(http status, grpc-status, grpc-message, message bytes)`.
    async fn call(&self, path: &str, body: Vec<u8>, extra: &[(&str, &str)]) -> Answer {
        let mut request = Request::post(path).header(header::CONTENT_TYPE, "application/grpc");
        for (k, v) in extra {
            request = request.header(*k, *v);
        }
        let response = self
            .grpc
            .clone()
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let collected = response.into_body().collect().await.unwrap();
        let trailers = collected.trailers().cloned().unwrap_or_default();
        let grpc_status = trailers
            .get("grpc-status")
            .or_else(|| headers.get("grpc-status"))
            .map(|v| v.to_str().unwrap().to_owned());
        let grpc_message = headers
            .get("grpc-message")
            .map(|v| v.to_str().unwrap().to_owned());
        Answer {
            status,
            grpc_status,
            grpc_message,
            body: collected.to_bytes().to_vec(),
        }
    }

    async fn lines(&self) -> String {
        let request = Request::get(
            "/loki/api/v1/query_range?query=%7Bapp%3D%22grpc-app%22%7D&start=0&end=9999999999000000000",
        )
        .body(Body::empty())
        .unwrap();
        let response = self.app.clone().oneshot(request).await.unwrap();
        String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap()
    }
}

struct Answer {
    status: StatusCode,
    grpc_status: Option<String>,
    grpc_message: Option<String>,
    body: Vec<u8>,
}

const NANOS: u64 = 1_750_000_000_000_000_000;

#[tokio::test]
async fn an_export_is_stored_and_answered_ok() {
    let h = Harness::new(None);
    let answer = h
        .call(LOGS, frame(0, &export_logs("hello over grpc", NANOS)), &[])
        .await;
    assert_eq!(answer.status, StatusCode::OK);
    assert_eq!(answer.grpc_status.as_deref(), Some("0"));
    // A framed, empty ExportLogsServiceResponse: nothing was partially rejected.
    assert_eq!(answer.body, vec![0, 0, 0, 0, 0]);
    assert!(h.lines().await.contains("hello over grpc"));
}

#[tokio::test]
async fn a_compressed_message_is_read() {
    use std::io::Write;
    let h = Harness::new(None);
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(&export_logs("compressed over grpc", NANOS))
        .unwrap();
    let answer = h
        .call(
            LOGS,
            frame(1, &gzip.finish().unwrap()),
            &[("grpc-encoding", "gzip")],
        )
        .await;
    assert_eq!(
        answer.grpc_status.as_deref(),
        Some("0"),
        "{:?}",
        answer.grpc_message
    );
    assert!(h.lines().await.contains("compressed over grpc"));

    let answer = h
        .call(LOGS, frame(1, b"x"), &[("grpc-encoding", "snappy")])
        .await;
    assert_eq!(answer.grpc_status.as_deref(), Some("12"));
}

#[tokio::test]
async fn the_ingest_token_is_metadata() {
    let h = Harness::new(Some("secret"));
    let body = frame(0, &export_logs("guarded", NANOS));
    let answer = h.call(LOGS, body.clone(), &[]).await;
    assert_eq!(answer.grpc_status.as_deref(), Some("16"), "UNAUTHENTICATED");
    let answer = h
        .call(LOGS, body, &[("authorization", "Bearer secret")])
        .await;
    assert_eq!(
        answer.grpc_status.as_deref(),
        Some("0"),
        "{:?}",
        answer.grpc_message
    );
}

#[tokio::test]
async fn what_is_not_an_export_is_refused_in_grpc_terms() {
    let h = Harness::new(None);
    let answer = h
        .call("/grpc.health.v1.Health/Check", frame(0, &[]), &[])
        .await;
    assert_eq!(answer.status, StatusCode::OK, "gRPC errors ride on 200");
    assert_eq!(answer.grpc_status.as_deref(), Some("12"));

    let answer = h.call(LOGS, vec![0, 0, 0, 0, 9, 1], &[]).await;
    assert_eq!(answer.grpc_status.as_deref(), Some("3"));
    // The message streams on to the OTLP handler, which reports the short read.
    let message = answer.grpc_message.unwrap();
    assert!(message.contains("more bytes than followed"), "{message}");

    // A payload the protobuf decoder refuses is INVALID_ARGUMENT, with its reason.
    let answer = h.call(LOGS, frame(0, &[0xff, 0xff]), &[]).await;
    assert_eq!(answer.grpc_status.as_deref(), Some("3"));
}
