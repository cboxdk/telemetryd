//! An instance and a ClickHouse that stops answering.
//!
//! - At startup, one that does not answer is a startup error that names it, rather than
//!   an instance that starts and refuses every write.
//! - Once running, one that goes away is said so: a write is refused with `503` and
//!   `Retry-After`, so the sender keeps it and retries; a read is refused; `/ready` is
//!   `503`, so a load balancer sends writes elsewhere. Nothing is accepted that was not
//!   stored.
//!
//! Neither needs a real ClickHouse: the first has none, the second a stand-in that
//! answers the questions an instance asks at startup and is then shut down.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use telemetryd_core::Config;
use telemetryd_core::config::{ClickHouseConfig, StorageConfig};
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

fn config(dir: &std::path::Path, port: u16) -> Config {
    Config {
        storage: StorageConfig {
            data_dir: Some(dir.to_path_buf()),
            ..StorageConfig::default()
        },
        clickhouse: ClickHouseConfig {
            url: format!("http://127.0.0.1:{port}"),
            // A schema someone else manages: startup checks the tables, creates none.
            create_schema: false,
            timeout: std::time::Duration::from_secs(2),
            ..ClickHouseConfig::default()
        },
        ..Config::default()
    }
}

/// A port nothing listens on: bound, its number taken, and released.
fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[test]
fn a_clickhouse_that_does_not_answer_at_startup_stops_the_start() {
    let tmp = tempfile::tempdir().unwrap();
    let config = config(tmp.path(), closed_port());
    config.validate().unwrap();
    let error = Store::open(&config).unwrap_err();
    assert!(
        matches!(error, telemetryd_core::Error::Unavailable(_)),
        "{error}"
    );
    assert!(error.to_string().contains("ClickHouse"), "{error}");
}

/// Enough of ClickHouse's HTTP interface for an instance to start: every table it
/// expects is there, and every other request succeeds with nothing to say.
async fn stand_in(listener: tokio::net::TcpListener, tables: Vec<String>) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            return;
        };
        let tables = tables.clone();
        tokio::spawn(async move {
            let mut request = Vec::new();
            let mut buffer = [0u8; 8192];
            // Headers, then as much body as they announce.
            let body_start = loop {
                let Ok(read) = socket.read(&mut buffer).await else {
                    return;
                };
                if read == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..read]);
                if let Some(at) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    break at + 4;
                }
            };
            let head = String::from_utf8_lossy(&request[..body_start]).to_ascii_lowercase();
            let length: usize = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
            while request.len() < body_start + length {
                let Ok(read) = socket.read(&mut buffer).await else {
                    return;
                };
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            let text = String::from_utf8_lossy(&request);
            let answer: String = if text.contains("system.tables") {
                tables
                    .iter()
                    .flat_map(|name| ["{\"name\":\"", name.as_str(), "\"}\n"])
                    .collect()
            } else {
                String::new()
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                answer.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

#[tokio::test]
async fn a_clickhouse_that_goes_away_is_refused_loudly_not_accepted_quietly() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let tmp = tempfile::tempdir().unwrap();
    let config = config(tmp.path(), port);
    config.validate().unwrap();
    let tables =
        telemetryd_store::clickhouse::schema::objects(&config.clickhouse, &config.retention);
    let server = tokio::spawn(stand_in(listener, tables));

    let store = Arc::new(
        tokio::task::spawn_blocking({
            let config = config.clone();
            move || Store::open(&config)
        })
        .await
        .unwrap()
        .unwrap(),
    );
    let app = router(AppState::new(Arc::new(config), store).unwrap());
    let ready = app
        .clone()
        .oneshot(Request::get("/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        ready.status(),
        StatusCode::OK,
        "ready while ClickHouse answers"
    );

    // ClickHouse goes away.
    server.abort();
    let _ = server.await;
    // Past the second a readiness answer is reused for.
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;

    let ready = app
        .clone()
        .oneshot(Request::get("/ready").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let reason = ready.into_body().collect().await.unwrap().to_bytes();
    let reason = String::from_utf8_lossy(&reason);
    assert!(reason.contains("ClickHouse"), "{reason}");
    // The reason names no host: `/ready` answers anyone.
    assert!(!reason.contains("127.0.0.1"), "{reason}");

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
    let refusal = written.into_body().collect().await.unwrap().to_bytes();
    let refusal = String::from_utf8_lossy(&refusal);
    assert!(!refusal.contains("127.0.0.1"), "{refusal}");
    assert!(!refusal.contains("INSERT"), "{refusal}");

    let read = app
        .clone()
        .oneshot(
            Request::get(
                "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1789999999&end=1790000001",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(read.status(), StatusCode::SERVICE_UNAVAILABLE);
}
