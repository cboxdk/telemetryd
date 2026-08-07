//! Loki-compatible query handlers, including live tail.

use std::collections::HashMap;

use axum::Json;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use telemetryd_core::{Error, LogRecord};
use telemetryd_query::logql;
use telemetryd_query::loki::{self, QueryRangeParams, QueryRangeRequest, RangeParams};

use crate::error::ApiError;
use crate::state::AppState;

/// `GET /loki/api/v1/query_range`
pub async fn query_range(
    State(state): State<AppState>,
    Query(params): Query<QueryRangeParams>,
) -> Result<Response, ApiError> {
    let request = QueryRangeRequest::from_params(&params, telemetryd_store::now_nanos())?;

    // Reading segments is blocking file I/O; keep it off the async runtime.
    let store = std::sync::Arc::clone(&state.store);
    let response = tokio::task::spawn_blocking(move || loki::query_range(store.logs(), &request))
        .await
        .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

/// `GET /loki/api/v1/labels`
pub async fn labels(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
) -> Result<Response, ApiError> {
    let (start, end) = loki::resolve_range(
        params.start.as_deref(),
        params.end.as_deref(),
        params.since.as_deref(),
        telemetryd_store::now_nanos(),
    )?;

    let store = std::sync::Arc::clone(&state.store);
    let response = tokio::task::spawn_blocking(move || loki::label_names(store.logs(), start, end))
        .await
        .map_err(|e| Error::Config(format!("query task panicked: {e}")))?;

    Ok(Json(response).into_response())
}

/// `GET /loki/api/v1/label/{name}/values`
pub async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<RangeParams>,
) -> Result<Response, ApiError> {
    let (start, end) = loki::resolve_range(
        params.start.as_deref(),
        params.end.as_deref(),
        params.since.as_deref(),
        telemetryd_store::now_nanos(),
    )?;

    let store = std::sync::Arc::clone(&state.store);
    let response =
        tokio::task::spawn_blocking(move || loki::label_values(store.logs(), &name, start, end))
            .await
            .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

/// `GET /loki/api/v1/series`
///
/// The axum extractor fixes the hasher, so the signature cannot be generic over it.
#[allow(clippy::implicit_hasher)]
pub async fn series(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    // `match[]` repeats, which a HashMap flattens; accept the common spellings so a
    // client is not silently given every series when it asked for one.
    let mut selectors: Vec<String> = Vec::new();
    for key in ["match[]", "match", "query"] {
        if let Some(value) = raw.get(key)
            && !value.trim().is_empty()
        {
            selectors.push(value.clone());
        }
    }

    let (start, end) = loki::resolve_range(
        raw.get("start").map(String::as_str),
        raw.get("end").map(String::as_str),
        raw.get("since").map(String::as_str),
        telemetryd_store::now_nanos(),
    )?;

    let store = std::sync::Arc::clone(&state.store);
    let response =
        tokio::task::spawn_blocking(move || loki::series(store.logs(), &selectors, start, end))
            .await
            .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

// ---------------------------------------------------------------------------
// Live tail
// ---------------------------------------------------------------------------

/// `GET /loki/api/v1/tail` (WebSocket)
pub async fn tail(
    State(state): State<AppState>,
    Query(params): Query<TailParams>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    // Parse before upgrading: a bad query should be an HTTP 400 a client can read, not
    // a WebSocket that opens and immediately closes for no stated reason.
    let raw = params
        .query
        .as_deref()
        .filter(|q| !q.trim().is_empty())
        .ok_or_else(|| Error::BadRequest("the `query` parameter is required".to_owned()))?;
    let query = logql::parse(raw)?;

    Ok(upgrade.on_upgrade(move |socket| run_tail(socket, state, query)))
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct TailParams {
    pub query: Option<String>,
    pub limit: Option<String>,
    pub start: Option<String>,
    pub delay_for: Option<String>,
}

/// Loki's tail frame.
#[derive(Debug, Serialize)]
struct TailResponse {
    streams: Vec<TailStream>,
    #[serde(rename = "dropped_entries", skip_serializing_if = "Vec::is_empty")]
    dropped_entries: Vec<DroppedEntry>,
}

#[derive(Debug, Serialize)]
struct TailStream {
    stream: std::collections::BTreeMap<String, String>,
    values: Vec<[String; 2]>,
}

#[derive(Debug, Serialize)]
struct DroppedEntry {
    timestamp: String,
    labels: std::collections::BTreeMap<String, String>,
}

async fn run_tail(mut socket: WebSocket, state: AppState, query: logql::LogQuery) {
    let mut receiver = state.subscribe_tail();
    state.metrics.incr("telemetryd_tail_connections_total", &[]);

    loop {
        tokio::select! {
            // A client that closes or pings must be noticed promptly, or connections
            // accumulate for the lifetime of the process.
            incoming = socket.recv() => {
                // A client that closes, errors, or vanishes must be noticed
                // promptly; anything else it sends is ignored.
                match incoming {
                    Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
            delivery = receiver.recv() => {
                match delivery {
                    Ok(record) => {
                        if !matches_tail(&query, &record) {
                            continue;
                        }
                        let frame = TailResponse {
                            streams: vec![TailStream {
                                stream: record
                                    .stream
                                    .iter()
                                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                                    .collect(),
                                values: vec![[
                                    record.timestamp_nanos.to_string(),
                                    record.body.clone(),
                                ]],
                            }],
                            dropped_entries: Vec::new(),
                        };
                        let Ok(json) = serde_json::to_string(&frame) else { continue };
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                        // A slow client fell behind the buffer. Tell it so, rather than
                        // silently showing an incomplete tail that looks complete.
                        state.metrics.add("telemetryd_tail_dropped_total", &[], missed);
                        let frame = TailResponse {
                            streams: Vec::new(),
                            dropped_entries: vec![DroppedEntry {
                                timestamp: telemetryd_store::now_nanos().to_string(),
                                labels: std::collections::BTreeMap::new(),
                            }],
                        };
                        let Ok(json) = serde_json::to_string(&frame) else { continue };
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    state.metrics.incr("telemetryd_tail_disconnects_total", &[]);
}

fn matches_tail(query: &logql::LogQuery, record: &LogRecord) -> bool {
    if !telemetryd_core::matches_all(&query.matchers, &record.stream) {
        return false;
    }
    let mut base = record.stream.clone();
    for (name, value) in record.attributes.iter() {
        base.insert(name, value);
        let sanitized = telemetryd_core::record::sanitize_label_name(name);
        if sanitized != name {
            base.insert(sanitized, value);
        }
    }
    query.evaluate(&record.body, &base)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use telemetryd_core::{Labels, Severity};

    fn record(app: &str, body: &str) -> LogRecord {
        let mut stream = Labels::new();
        stream.insert("app", app);
        stream.insert("level", "info");
        LogRecord {
            timestamp_nanos: 1_750_000_000_000_000_000,
            stream,
            severity: Severity::Info,
            severity_text: "INFO".to_owned(),
            body: body.to_owned(),
            attributes: Labels::new(),
            trace_id: None,
            span_id: None,
        }
    }

    #[test]
    fn tail_applies_both_the_selector_and_the_pipeline() {
        let query = logql::parse(r#"{app="checkout"} |= "declined""#).unwrap();

        assert!(matches_tail(
            &query,
            &record("checkout", "payment declined")
        ));
        assert!(
            !matches_tail(&query, &record("cart", "payment declined")),
            "wrong app"
        );
        assert!(
            !matches_tail(&query, &record("checkout", "payment ok")),
            "wrong line"
        );
    }

    #[test]
    fn tail_sees_record_attributes_in_label_filters() {
        let query = logql::parse(r#"{app="checkout"} | order_id="42""#).unwrap();
        let mut with_attr = record("checkout", "x");
        with_attr.attributes.insert("order_id", "42");

        assert!(matches_tail(&query, &with_attr));
        assert!(!matches_tail(&query, &record("checkout", "x")));
    }

    #[test]
    fn the_tail_frame_matches_lokis_shape() {
        let frame = TailResponse {
            streams: vec![TailStream {
                stream: [("app".to_owned(), "checkout".to_owned())]
                    .into_iter()
                    .collect(),
                values: vec![["1750000000000000000".to_owned(), "hello".to_owned()]],
            }],
            dropped_entries: Vec::new(),
        };
        let json = serde_json::to_value(&frame).unwrap();

        assert_eq!(json["streams"][0]["stream"]["app"], "checkout");
        assert_eq!(json["streams"][0]["values"][0][0], "1750000000000000000");
        assert_eq!(json["streams"][0]["values"][0][1], "hello");
        // Absent rather than an empty array when nothing was dropped.
        assert!(json.get("dropped_entries").is_none());
    }

    #[test]
    fn a_lagging_client_is_told_it_missed_entries() {
        let frame = TailResponse {
            streams: Vec::new(),
            dropped_entries: vec![DroppedEntry {
                timestamp: "1750000000000000000".to_owned(),
                labels: std::collections::BTreeMap::new(),
            }],
        };
        let json = serde_json::to_value(&frame).unwrap();
        assert_eq!(json["dropped_entries"].as_array().unwrap().len(), 1);
    }
}
