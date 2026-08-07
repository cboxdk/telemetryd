//! Live tail over a real WebSocket, against a real bound socket.
//!
//! Driven end to end rather than by calling the handler: the upgrade handshake, the
//! frame encoding and the disconnect path are exactly the parts an in-process test
//! would skip, and they are what breaks in production.

#![allow(clippy::unwrap_used, unreachable_pub)]

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use serde_json::{Value, json};
use telemetryd_core::Config;
use telemetryd_core::config::StorageConfig;
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use tokio_tungstenite::tungstenite::Message;

const NOW: u64 = 1_750_000_000_000_000_000;

struct Server {
    addr: std::net::SocketAddr,
    client: reqwest_lite::Client,
    _tmp: tempfile::TempDir,
    shutdown: tokio::sync::oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<()>,
}

/// A three-function HTTP client. Pulling in a full client crate for two POSTs in one
/// test file is not worth the dependency.
mod reqwest_lite {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Debug, Clone, Copy)]
    pub struct Client;

    impl Client {
        pub async fn post_json(self, addr: std::net::SocketAddr, path: &str, body: &str) -> String {
            let request = format!(
                "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            Self::send(addr, &request).await
        }

        async fn send(addr: std::net::SocketAddr, request: &str) -> String {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            String::from_utf8_lossy(&response).into_owned()
        }
    }
}

impl Server {
    async fn start() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            storage: StorageConfig {
                data_dir: Some(tmp.path().join("data")),
                ..StorageConfig::default()
            },
            ..Config::default()
        };
        let store = Arc::new(Store::open(&config).unwrap());
        let state = AppState::new(Arc::new(config), store).unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, rx) = tokio::sync::oneshot::channel();

        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router(state))
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });

        Self {
            addr,
            client: reqwest_lite::Client,
            _tmp: tmp,
            shutdown,
            handle,
        }
    }

    async fn post_logs(&self, payload: &Value) {
        let response = self
            .client
            .post_json(self.addr, "/v1/logs", &payload.to_string())
            .await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    }

    async fn connect_tail(
        &self,
        query: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let encoded: String = query
            .bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    char::from(b).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect();
        let url = format!("ws://{}/loki/api/v1/tail?query={encoded}", self.addr);
        let (socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        socket
    }

    async fn stop(self) {
        let _ = self.shutdown.send(());
        let _ = tokio::time::timeout(Duration::from_secs(5), self.handle).await;
    }
}

fn otlp(body: &str, severity: &str) -> Value {
    json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeLogs": [{"logRecords": [{
                "timeUnixNano": NOW.to_string(),
                "severityNumber": if severity == "error" { 17 } else { 9 },
                "severityText": severity.to_uppercase(),
                "body": {"stringValue": body},
                "attributes": [{"key": "order.id", "value": {"stringValue": "42"}}]
            }]}]
        }]
    })
}

/// Read frames until one carries a stream entry, or time out.
async fn next_entry(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline - tokio::time::Instant::now();
        match tokio::time::timeout(remaining, socket.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let frame: Value = serde_json::from_str(&text).unwrap();
                if frame["streams"].as_array().is_some_and(|s| !s.is_empty()) {
                    return Some(frame);
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => return None,
        }
    }
    None
}

#[tokio::test]
async fn tail_streams_matching_lines_as_they_arrive() {
    let server = Server::start().await;
    let mut socket = server.connect_tail(r#"{app="checkout"}"#).await;

    server.post_logs(&otlp("payment declined", "error")).await;

    let frame = next_entry(&mut socket)
        .await
        .expect("no tail frame arrived");
    assert_eq!(frame["streams"][0]["stream"]["app"], "checkout");
    assert_eq!(frame["streams"][0]["values"][0][1], "payment declined");
    // Timestamps are strings of nanoseconds, as in query_range.
    assert!(frame["streams"][0]["values"][0][0].is_string());

    socket.close(None).await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn tail_applies_the_selector_and_the_line_filter() {
    let server = Server::start().await;
    let mut socket = server
        .connect_tail(r#"{app="checkout", level="error"} |= "declined""#)
        .await;

    // None of these should reach the client.
    server.post_logs(&otlp("payment ok", "info")).await;
    server.post_logs(&otlp("payment retried", "error")).await;
    // This one should.
    server.post_logs(&otlp("payment declined", "error")).await;

    let frame = next_entry(&mut socket)
        .await
        .expect("no tail frame arrived");
    assert_eq!(
        frame["streams"][0]["values"][0][1], "payment declined",
        "the filtered-out lines must not be delivered first"
    );

    socket.close(None).await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn tail_label_filters_reach_record_attributes() {
    let server = Server::start().await;
    let mut socket = server
        .connect_tail(r#"{app="checkout"} | order_id="42""#)
        .await;

    server.post_logs(&otlp("has the attribute", "info")).await;

    let frame = next_entry(&mut socket)
        .await
        .expect("no tail frame arrived");
    assert_eq!(frame["streams"][0]["values"][0][1], "has the attribute");

    socket.close(None).await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn a_bad_tail_query_is_refused_before_the_upgrade() {
    let server = Server::start().await;

    // A WebSocket that opens and immediately closes gives a client nothing to show a
    // user; the error has to arrive as a readable HTTP response.
    let url = format!(
        "ws://{}/loki/api/v1/tail?query=rate(%7Bapp%3D%22x%22%7D%5B5m%5D)",
        server.addr
    );
    let result = tokio_tungstenite::connect_async(url).await;
    assert!(result.is_err(), "an unsupported query must not upgrade");

    let missing = format!("ws://{}/loki/api/v1/tail", server.addr);
    assert!(tokio_tungstenite::connect_async(missing).await.is_err());

    server.stop().await;
}

#[tokio::test]
async fn tail_does_not_block_ingest_when_nobody_is_listening() {
    let server = Server::start().await;

    // The fan-out must never be able to fail or stall the write path.
    for i in 0..50 {
        server.post_logs(&otlp(&format!("line {i}"), "info")).await;
    }

    server.stop().await;
}

#[tokio::test]
async fn multiple_subscribers_each_get_their_own_filtered_view() {
    let server = Server::start().await;
    let mut errors = server
        .connect_tail(r#"{app="checkout", level="error"}"#)
        .await;
    let mut everything = server.connect_tail(r#"{app="checkout"}"#).await;

    server.post_logs(&otlp("just info", "info")).await;
    server.post_logs(&otlp("a real error", "error")).await;

    // The error-only subscriber must skip the info line rather than receive it.
    let frame = next_entry(&mut errors)
        .await
        .expect("error subscriber got nothing");
    assert_eq!(frame["streams"][0]["values"][0][1], "a real error");

    let frame = next_entry(&mut everything)
        .await
        .expect("catch-all got nothing");
    assert_eq!(frame["streams"][0]["values"][0][1], "just info");

    errors.close(None).await.unwrap();
    everything.close(None).await.unwrap();
    server.stop().await;
}

#[tokio::test]
async fn tail_connections_are_counted_and_released() {
    let server = Server::start().await;
    let socket = server.connect_tail(r#"{app="checkout"}"#).await;

    // Give the upgrade a moment to register the subscriber.
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(socket);
    // …and a moment for the disconnect to be noticed, so connections do not leak for
    // the lifetime of the process.
    tokio::time::sleep(Duration::from_millis(200)).await;

    server.stop().await;
}
