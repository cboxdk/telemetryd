//! Prometheus-compatible query handlers.

use axum::Json;
use axum::extract::{Path, State};
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

/// Query parameters from the URL and, on a form POST, from the body.
///
/// Prometheus reads both, and Grafana's Prometheus datasource sends POST with the query in
/// the body by default — its "Save & test" is exactly `POST /api/v1/query` with `query=1+1`
/// in the body. Reading only the URL answered that with "the `query` parameter is
/// required", so Grafana could not connect at all. Where both carry a key the body wins,
/// as it does in the Go form parsing Prometheus uses.
#[derive(Debug)]
pub struct Params<T>(pub T);

impl<S, T> axum::extract::FromRequest<S> for Params<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(request: axum::extract::Request, state: &S) -> Result<Self, ApiError> {
        let query = request.uri().query().unwrap_or_default().to_owned();
        let is_form = request
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/x-www-form-urlencoded"));
        let body = if is_form {
            axum::body::Bytes::from_request(request, state)
                .await
                .map_err(|e| Error::BadRequest(format!("could not read the form body: {e}")))?
        } else {
            axum::body::Bytes::new()
        };

        let mut seen = std::collections::HashSet::new();
        let mut pairs: Vec<(String, String)> = Vec::new();
        for pair in [body.as_ref(), query.as_bytes()]
            .into_iter()
            .flat_map(|bytes| {
                serde_urlencoded::from_bytes::<Vec<(String, String)>>(bytes).unwrap_or_default()
            })
        {
            if seen.insert(pair.0.clone()) {
                pairs.push(pair);
            }
        }
        let merged = serde_urlencoded::to_string(&pairs)
            .map_err(|e| Error::BadRequest(format!("invalid parameters: {e}")))?;
        serde_urlencoded::from_str(&merged)
            .map(Params)
            .map_err(|e| Error::BadRequest(format!("invalid parameters: {e}")).into())
    }
}

/// `GET,POST /api/v1/query`
pub async fn instant(
    State(state): State<AppState>,
    Params(params): Params<InstantParams>,
) -> Result<Response, ApiError> {
    let store = std::sync::Arc::clone(&state.store);
    let now = telemetryd_store::now_nanos();
    let max_samples = state.config.limits.resolved_max_query_samples();

    let expression = params.query.clone().unwrap_or_default();
    let response = crate::auth::spawn_read(move || {
        prometheus::instant(store.metrics(), &params, now, max_samples)
    })
    .await
    .map_err(|e| Error::Config(format!("query task panicked: {e}")))?
    .inspect_err(|error| state.refused_query("promql", &expression, error))?;

    Ok(Json(response).into_response())
}

/// `GET,POST /api/v1/query_range`
pub async fn range(
    State(state): State<AppState>,
    Params(params): Params<RangeParams>,
) -> Result<Response, ApiError> {
    let store = std::sync::Arc::clone(&state.store);
    let now = telemetryd_store::now_nanos();
    let max_samples = state.config.limits.resolved_max_query_samples();

    let expression = params.query.clone().unwrap_or_default();
    let response = crate::auth::spawn_read(move || {
        prometheus::range(store.metrics(), &params, now, max_samples)
    })
    .await
    .map_err(|e| Error::Config(format!("query task panicked: {e}")))?
    .inspect_err(|error| state.refused_query("promql", &expression, error))?;

    Ok(Json(response).into_response())
}

/// `GET,POST /api/v1/labels`
pub async fn labels(
    State(state): State<AppState>,
    pairs: crate::params::Pairs,
) -> Result<Response, ApiError> {
    let (start, end, selectors) = meta(&pairs)?;
    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || {
        prometheus::label_names(store.metrics(), &selectors, start, end)
    })
    .await
    .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;
    Ok(Json(response).into_response())
}

/// `GET /api/v1/label/{name}/values`
pub async fn label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    pairs: crate::params::Pairs,
) -> Result<Response, ApiError> {
    let (start, end, selectors) = meta(&pairs)?;
    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || {
        prometheus::label_values(store.metrics(), &name, &selectors, start, end)
    })
    .await
    .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;
    Ok(Json(response).into_response())
}

/// `GET,POST /api/v1/series`
pub async fn series(
    State(state): State<AppState>,
    pairs: crate::params::Pairs,
) -> Result<Response, ApiError> {
    let (start, end, selectors) = meta(&pairs)?;
    let store = std::sync::Arc::clone(&state.store);
    let response = crate::auth::spawn_read(move || {
        prometheus::series(store.metrics(), &selectors, start, end)
    })
    .await
    .map_err(|e| Error::Config(format!("query task panicked: {e}")))??;
    Ok(Json(response).into_response())
}

/// The time range and every `match[]` of a metadata request.
fn meta(pairs: &crate::params::Pairs) -> Result<(u64, u64, Vec<String>), ApiError> {
    let params = MetaParams {
        start: pairs.first("start"),
        end: pairs.first("end"),
        matches: None,
    };
    let (start, end) = prometheus::meta_range(&params, telemetryd_store::now_nanos())?;
    Ok((start, end, pairs.all(&["match[]", "match"])))
}
