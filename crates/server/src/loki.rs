//! Loki-compatible query handlers, including live tail.

use axum::Json;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use telemetryd_core::{Error, LogRecord};
use telemetryd_query::logql;
use telemetryd_query::loki::{self, QueryRangeParams, QueryRangeRequest};

use crate::error::ApiError;
use crate::state::AppState;

/// Whether the caller asked for Loki's categorised label shape.
///
/// The header is a comma-separated list of flags; Grafana sends `categorize-labels`.
fn wants_categorized(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get_all("x-loki-response-encoding-flags")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|flag| flag.trim().eq_ignore_ascii_case("categorize-labels"))
}

/// `GET /loki/api/v1/query_range`
pub async fn query_range(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(params): Query<QueryRangeParams>,
) -> Result<Response, ApiError> {
    // Parsed before the guard so a malformed selector is recorded too: a query refused
    // at parse time is exactly as invisible to the operator as one refused later.
    let expression = params.query.clone().unwrap_or_default();
    let mut request = QueryRangeRequest::from_params(&params, telemetryd_store::now_nanos())
        .inspect_err(|error| state.refused_query("logql", &expression, error))?;
    request.categorize = wants_categorized(&headers);

    // Reading segments is blocking file I/O; keep it off the async runtime.
    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || loki::query_range(store.logs(), &request))
        .await
        .map_err(|e| Error::Config(format!("query task panicked: {e}")))?
        .inspect_err(|error| state.refused_query("logql", &expression, error))?;

    Ok(Json(response).into_response())
}

/// `GET /loki/api/v1/query` — an instant query.
///
/// Grafana's Loki datasource checks the connection with exactly this, evaluating
/// `vector(1)+vector(1)` and expecting one point with the value 2; the route did not
/// exist, so the datasource could not be added. A log query is answered over the hour
/// before `time`; metric LogQL is refused by name, as on `query_range`.
pub async fn instant(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(params): Query<loki::InstantParams>,
) -> Result<Response, ApiError> {
    let now = telemetryd_store::now_nanos();
    let at = params.at_nanos(now)?;
    let expression = params.query.clone().unwrap_or_default();
    if params.is_literal() {
        let answer = loki::instant_literal(&expression, at)
            .inspect_err(|error| state.refused_query("logql", &expression, error))?;
        return Ok(Json(answer).into_response());
    }

    let mut request = QueryRangeRequest::from_params(&params.as_range(at), now)
        .inspect_err(|error| state.refused_query("logql", &expression, error))?;
    request.categorize = wants_categorized(&headers);
    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || loki::query_range(store.logs(), &request))
        .await
        .map_err(|e| Error::Config(format!("query task panicked: {e}")))?
        .inspect_err(|error| state.refused_query("logql", &expression, error))?;
    Ok(Json(response).into_response())
}

/// `GET /loki/api/v1/labels`
pub async fn labels(
    State(state): State<AppState>,
    pairs: crate::params::Pairs,
) -> Result<Response, ApiError> {
    let (start, end) = range_of(&pairs)?;
    let selectors = pairs.all(&["query"]);
    let store = std::sync::Arc::clone(&state.store);
    let response =
        crate::auth::spawn_read(move || loki::label_names(store.logs(), &selectors, start, end))
            .await
            .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;
    Ok(Json(response).into_response())
}

/// `GET /loki/api/v1/label/{name}/values`
pub async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    pairs: crate::params::Pairs,
) -> Result<Response, ApiError> {
    let (start, end) = range_of(&pairs)?;
    let selectors = pairs.all(&["query"]);
    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || {
        loki::label_values(store.logs(), &name, &selectors, start, end)
    })
    .await
    .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;
    Ok(Json(response).into_response())
}

/// `GET /loki/api/v1/series`
///
/// Every `match[]` counts; they used to be read into a map, which kept only the last.
pub async fn series(
    State(state): State<AppState>,
    pairs: crate::params::Pairs,
) -> Result<Response, ApiError> {
    let (start, end) = range_of(&pairs)?;
    let selectors = pairs.all(&["match[]", "match", "query"]);
    let store = std::sync::Arc::clone(&state.store);
    let response =
        crate::auth::spawn_read(move || loki::series(store.logs(), &selectors, start, end))
            .await
            .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;
    Ok(Json(response).into_response())
}

fn range_of(pairs: &crate::params::Pairs) -> Result<(u64, u64), ApiError> {
    Ok(loki::resolve_range(
        pairs.first("start").as_deref(),
        pairs.first("end").as_deref(),
        pairs.first("since").as_deref(),
        telemetryd_store::now_nanos(),
    )?)
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

    // Claimed before upgrading, so a refusal is a `429` the client can read.
    let Some(slot) = state.tail_slot() else {
        state.metrics.incr(
            "telemetryd_query_rejected_total",
            &[("surface", "query"), ("reason", "tails")],
        );
        return Err(Error::Overloaded.into());
    };

    // The tail sends and never listens beyond a close, so a client has no business
    // sending more than a control frame. The default let one message be 64 MiB.
    Ok(upgrade
        .max_message_size(4 * 1024)
        .max_frame_size(4 * 1024)
        .on_upgrade(move |socket| run_tail(socket, state, std::sync::Arc::new(query), slot)))
}

/// How many ingested records a tail filters in one go on the blocking pool.
const TAIL_BATCH: usize = 256;

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

async fn run_tail(
    mut socket: WebSocket,
    state: AppState,
    query: std::sync::Arc<logql::LogQuery>,
    _slot: tokio::sync::OwnedSemaphorePermit,
) {
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
                        // Whatever else has arrived joins it, and the batch is matched on
                        // the blocking pool. Matching runs a LogQL pipeline per record per
                        // tail; on the async workers it competed with every request.
                        let mut batch = vec![record];
                        let mut missed = 0;
                        while batch.len() < TAIL_BATCH {
                            match receiver.try_recv() {
                                Ok(more) => batch.push(more),
                                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                                    missed += n;
                                }
                                Err(_) => break,
                            }
                        }
                        let query = std::sync::Arc::clone(&query);
                        let Ok(matched) = tokio::task::spawn_blocking(move || {
                            batch
                                .into_iter()
                                .filter(|record| matches_tail(&query, record))
                                .collect::<Vec<_>>()
                        })
                        .await
                        else {
                            break;
                        };
                        let mut gone = false;
                        for record in matched {
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
                                gone = true;
                                break;
                            }
                        }
                        if gone || (missed > 0 && !report_dropped(&mut socket, &state, missed).await) {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                        if !report_dropped(&mut socket, &state, missed).await {
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

/// Tell a client it fell behind the buffer and lost `missed` records, rather than
/// showing it an incomplete tail that looks complete. `false` when it has gone.
async fn report_dropped(socket: &mut WebSocket, state: &AppState, missed: u64) -> bool {
    state
        .metrics
        .add("telemetryd_tail_dropped_total", &[], missed);
    let frame = TailResponse {
        streams: Vec::new(),
        dropped_entries: vec![DroppedEntry {
            timestamp: telemetryd_store::now_nanos().to_string(),
            labels: std::collections::BTreeMap::new(),
        }],
    };
    let Ok(json) = serde_json::to_string(&frame) else {
        return true;
    };
    socket.send(Message::Text(json.into())).await.is_ok()
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
    query.evaluate(&record.body, &base, &record.stream)
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
