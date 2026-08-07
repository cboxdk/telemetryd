//! Prometheus-compatible query handlers.

use std::collections::HashMap;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use telemetryd_core::Error;
use telemetryd_query::prometheus::{self, InstantParams, MetaParams, RangeParams};

use crate::error::ApiError;
use crate::state::AppState;

/// `GET /api/v1/status/buildinfo`
///
/// `PrometheusSource::probe()` calls this first and only falls back to `query=1`.
/// Without it every connection check in the UI shows a degraded backend.
pub async fn build_info() -> Response {
    Json(prometheus::build_info()).into_response()
}

/// `GET,POST /api/v1/query`
pub async fn instant(
    State(state): State<AppState>,
    Query(params): Query<InstantParams>,
) -> Result<Response, ApiError> {
    let store = std::sync::Arc::clone(&state.store);
    let now = telemetryd_store::now_nanos();

    let response =
        tokio::task::spawn_blocking(move || prometheus::instant(store.metrics(), &params, now))
            .await
            .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

/// `GET,POST /api/v1/query_range`
pub async fn range(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
) -> Result<Response, ApiError> {
    let store = std::sync::Arc::clone(&state.store);
    let now = telemetryd_store::now_nanos();

    let response =
        tokio::task::spawn_blocking(move || prometheus::range(store.metrics(), &params, now))
            .await
            .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

/// `GET /api/v1/labels`
pub async fn labels(
    State(state): State<AppState>,
    Query(params): Query<MetaParams>,
) -> Result<Response, ApiError> {
    let (start, end) = prometheus::meta_range(&params, telemetryd_store::now_nanos())?;

    let store = std::sync::Arc::clone(&state.store);
    let response =
        tokio::task::spawn_blocking(move || prometheus::label_names(store.metrics(), start, end))
            .await
            .map_err(|e| Error::Config(format!("query task panicked: {e}")))?;

    Ok(Json(response).into_response())
}

/// `GET /api/v1/label/{name}/values`
pub async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<MetaParams>,
) -> Result<Response, ApiError> {
    let (start, end) = prometheus::meta_range(&params, telemetryd_store::now_nanos())?;

    let store = std::sync::Arc::clone(&state.store);
    let response = tokio::task::spawn_blocking(move || {
        prometheus::label_values(store.metrics(), &name, start, end)
    })
    .await
    .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;

    Ok(Json(response).into_response())
}

/// `GET /api/v1/series`
///
/// The axum extractor fixes the hasher, so the signature cannot be generic over it.
#[allow(clippy::implicit_hasher)]
pub async fn series(
    State(state): State<AppState>,
    Query(raw): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let selectors: Vec<String> = ["match[]", "match"]
        .iter()
        .filter_map(|key| raw.get(*key))
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .collect();

    let params = MetaParams {
        start: raw.get("start").cloned(),
        end: raw.get("end").cloned(),
        matches: None,
    };
    let (start, end) = prometheus::meta_range(&params, telemetryd_store::now_nanos())?;

    let store = std::sync::Arc::clone(&state.store);
    let response = tokio::task::spawn_blocking(move || {
        prometheus::series(store.metrics(), &selectors, start, end)
    })
    .await
    .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;

    Ok(Json(response).into_response())
}
