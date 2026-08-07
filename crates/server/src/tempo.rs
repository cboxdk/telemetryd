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
    let response = tokio::task::spawn_blocking(move || tempo::search(store.traces(), &request))
        .await
        .map_err(|e| Error::Config(format!("search task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

/// `GET /api/traces/{trace_id}`
pub async fn trace(
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
) -> Result<Response, ApiError> {
    let store = std::sync::Arc::clone(&state.store);
    let response = tokio::task::spawn_blocking(move || tempo::trace(store.traces(), &trace_id))
        .await
        .map_err(|e| Error::Config(format!("trace task panicked: {e}")))??;

    // Tempo answers 404 for an unknown trace id, and the UI shows "not found" rather
    // than an empty trace view.
    if response.batches.is_empty() {
        return Err(Error::NotFound("no trace with that id".to_owned()).into());
    }

    Ok(Json(response).into_response())
}

/// `GET /api/search/tags`
pub async fn tags(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> Result<Response, ApiError> {
    let request = SearchRequest::from_params(&params, telemetryd_store::now_nanos())?;

    let store = std::sync::Arc::clone(&state.store);
    let response = tokio::task::spawn_blocking(move || {
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
        tokio::task::spawn_blocking(move || tempo::tag_values(store.traces(), &name, &request))
            .await
            .map_err(|e| Error::Config(format!("tag values task panicked: {e}")))??;

    Ok(Json(response).into_response())
}
