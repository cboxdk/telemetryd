//! With ClickHouse as the store and ClickHouse not answering, an instance says so: a
//! write is refused with `503` and the reason, so the sender keeps it and retries, and
//! `/ready` is `503`, so a load balancer sends writes elsewhere. Nothing is accepted
//! that was not stored.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use telemetryd_core::Config;
use telemetryd_core::config::{ClickHouseConfig, StorageConfig};
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use tower::ServiceExt;

/// A port nothing listens on: bound, its number taken, and released.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[tokio::test]
async fn an_unreachable_clickhouse_is_refused_loudly_not_accepted_quietly() {
    let tmp = tempfile::tempdir().unwrap();
    let config = Config {
        storage: StorageConfig {
            data_dir: Some(tmp.path().join("data")),
            ..StorageConfig::default()
        },
        clickhouse: ClickHouseConfig {
            url: format!("http://127.0.0.1:{}", closed_port()),
            // A schema someone else manages: the instance starts without creating one.
            create_schema: false,
            timeout: std::time::Duration::from_secs(2),
            ..ClickHouseConfig::default()
        },
        ..Config::default()
    };
    config.validate().unwrap();
    let store = Arc::new(Store::open(&config).unwrap());
    let app = router(AppState::new(Arc::new(config), store).unwrap());

    let ready = app
        .clone()
        .oneshot(Request::get("/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let reason = ready.into_body().collect().await.unwrap().to_bytes();
    assert!(
        String::from_utf8_lossy(&reason).contains("ClickHouse"),
        "{}",
        String::from_utf8_lossy(&reason)
    );

    let body = serde_json::json!({"resourceLogs": [{
        "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "api"}}]},
        "scopeLogs": [{"logRecords": [{"timeUnixNano": "1790000000000000000", "body": {"stringValue": "hello"}}]}]
    }]});
    let written = app
        .clone()
        .oneshot(
            Request::post("/v1/logs")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(written.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(written.headers().contains_key(header::RETRY_AFTER));

    let read = app
        .clone()
        .oneshot(
            Request::get("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1789999999&end=1790000001")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::SERVICE_UNAVAILABLE);
}
