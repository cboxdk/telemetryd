//! The memory ingest requests in flight share.
//!
//! The queue bounded how many requests were in flight and `max_decoded_bytes` how far
//! one could expand; nothing bounded the two together, and the body was read before the
//! queue was even asked. These drive the pool from outside: hold most of it, and a
//! request that would need more is told to retry, and succeeds once there is room.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytesize::ByteSize;
use http_body_util::BodyExt;
use serde_json::json;
use telemetryd_core::Config;
use telemetryd_core::config::StorageConfig;
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use tower::ServiceExt;

const NOW: u64 = 1_750_000_000_000_000_000;
const KIB: usize = 1024;

struct Harness {
    state: AppState,
    router: axum::Router,
    _tmp: tempfile::TempDir,
}

impl Harness {
    fn with_pool(bytes: u64) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config {
            storage: StorageConfig {
                data_dir: Some(tmp.path().join("data")),
                ..StorageConfig::default()
            },
            ..Config::default()
        };
        config.limits.ingest_memory = ByteSize::b(bytes);
        config.validate().unwrap();
        let store = Arc::new(Store::open(&config).unwrap());
        let state = AppState::new(Arc::new(config), store).unwrap();
        Self {
            router: router(state.clone()),
            state,
            _tmp: tmp,
        }
    }

    async fn post_logs(&self, body: Vec<u8>) -> (StatusCode, Option<String>) {
        let request = Request::post("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, body.len())
            .body(Body::from(body))
            .unwrap();
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let retry_after = response
            .headers()
            .get(header::RETRY_AFTER)
            .map(|value| value.to_str().unwrap().to_owned());
        let _ = response.into_body().collect().await.unwrap();
        (status, retry_after)
    }
}

/// `records` log lines of a kilobyte each.
fn logs(records: usize) -> Vec<u8> {
    let line = "x".repeat(KIB);
    let records: Vec<_> = (0..records)
        .map(|i| {
            json!({
                "timeUnixNano": (NOW + i as u64).to_string(),
                "severityNumber": 9,
                "body": {"stringValue": line},
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

#[tokio::test]
async fn a_request_the_pool_cannot_cover_now_is_told_to_retry() {
    let harness = Harness::with_pool(0);
    let body = logs(200);
    assert!(body.len() > 200 * KIB);

    // Everything but 400 KiB held elsewhere: the body fits, what it decodes to does not.
    let pool = &harness.state.ingest_memory;
    let elsewhere = pool.reserve(pool.capacity() - 400 * KIB).unwrap();
    let (status, retry_after) = harness.post_logs(body.clone()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(retry_after.is_some(), "a 429 says when to come back");
    assert_eq!(
        pool.in_use(),
        pool.capacity() - 400 * KIB,
        "the refusal held nothing"
    );

    // Once there is room, the same request goes through, and gives back what it held.
    drop(elsewhere);
    let (status, _) = harness.post_logs(body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pool.in_use(), 0);
}

#[tokio::test]
async fn a_pool_always_fits_the_largest_request_the_limits_allow() {
    // Left to derive, and set far too small: both come out able to take one request of
    // the largest size, or a batch the other limits accept would be refused for good.
    for configured in [0, 64 * 1024] {
        let harness = Harness::with_pool(configured);
        let config = &harness.state.config;
        let largest =
            config.server.max_body_bytes.as_u64() * 2 + config.limits.max_decoded_bytes.as_u64();
        assert!(harness.state.ingest_memory.capacity() as u64 >= largest);
        let (status, _) = harness.post_logs(logs(10)).await;
        assert_eq!(status, StatusCode::OK);
    }
}
