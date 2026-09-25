//! Tempo-compatible query handlers.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use telemetryd_core::Error;
use telemetryd_query::tempo::{self, SearchParams, SearchRequest};

use crate::error::ApiError;
use crate::state::AppState;

/// `GET /api/search`
pub async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Response, ApiError> {
    let request = SearchRequest::from_params(&params, telemetryd_store::now_nanos())?;

    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || tempo::search(store.traces(), &request))
        .await
        .map_err(|e| Error::Config(format!("search task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

/// `GET /api/traces/{trace_id}`
pub async fn trace(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(trace_id): Path<String>,
) -> Result<Response, ApiError> {
    let response = find_trace(&state, trace_id).await?;
    if wants_protobuf(&headers) {
        return Ok(protobuf(response.to_protobuf()));
    }
    Ok(Json(response).into_response())
}

/// `GET /api/v2/traces/{trace_id}` — what Grafana asks first.
///
/// Protobuf is a `tempopb.TraceByIDResponse`; JSON puts the resource spans under
/// `trace.resourceSpans`, as Tempo's v2 does.
pub async fn trace_v2(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(trace_id): Path<String>,
) -> Result<Response, ApiError> {
    let response = find_trace(&state, trace_id).await?;
    if wants_protobuf(&headers) {
        return Ok(protobuf(response.to_protobuf_v2()));
    }
    Ok(Json(serde_json::json!({ "trace": { "resourceSpans": response.batches } })).into_response())
}

async fn find_trace(state: &AppState, trace_id: String) -> Result<tempo::TraceResponse, ApiError> {
    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || tempo::trace(store.traces(), &trace_id))
        .await
        .map_err(|e| Error::Config(format!("trace task panicked: {e}")))??;

    // Tempo answers 404 for an unknown trace id, and the UI shows "not found" rather
    // than an empty trace view.
    if response.batches.is_empty() {
        return Err(Error::NotFound("no trace with that id".to_owned()).into());
    }
    Ok(response)
}

/// Grafana sends `Accept: application/protobuf` and decodes whatever comes back as
/// protobuf; other clients — laravel-telemetry-ui — get JSON, as before.
fn wants_protobuf(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get_all(axum::http::header::ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|kind| {
            let kind = kind.split(';').next().unwrap_or_default().trim();
            kind.eq_ignore_ascii_case("application/protobuf")
                || kind.eq_ignore_ascii_case("application/x-protobuf")
        })
}

fn protobuf(body: Vec<u8>) -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "application/protobuf")],
        body,
    )
        .into_response()
}

/// `GET /api/v2/search/tags`
pub async fn tags_v2(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Response, ApiError> {
    let request = SearchRequest::from_params(&params, telemetryd_store::now_nanos())?;
    let scope = params.scope.clone();

    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || {
        tempo::tags_v2(
            store.traces(),
            request.start_nanos,
            request.end_nanos,
            scope.as_deref(),
        )
    })
    .await
    .map_err(|e| Error::Config(format!("tags task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

/// `GET /api/search/tags`
pub async fn tags(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Response, ApiError> {
    let request = SearchRequest::from_params(&params, telemetryd_store::now_nanos())?;

    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || {
        tempo::tags(store.traces(), request.start_nanos, request.end_nanos)
    })
    .await
    .map_err(|e| Error::Config(format!("tags task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

/// `GET /api/v2/search/tag/{name}/values` — and the v1 path, for older clients.
pub async fn tag_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<SearchParams>,
) -> Result<Response, ApiError> {
    let request = SearchRequest::from_params(&params, telemetryd_store::now_nanos())?;

    let store = std::sync::Arc::clone(&state.store);
    let response =
        crate::auth::spawn_read(move || tempo::tag_values(store.traces(), &name, &request))
            .await
            .map_err(|e| Error::Config(format!("tag values task panicked: {e}")))??;

    Ok(Json(response).into_response())
}
