//! HTTP mapping for [`telemetryd_core::Error`].
//!
//! The wire shape comes from `core` so it is identical everywhere an error can
//! surface; this module only decides the status code and any extra headers.

use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use telemetryd_core::Error;

#[derive(Debug)]
pub struct ApiError(pub Error);

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::Unauthorized => StatusCode::UNAUTHORIZED,
            Error::Forbidden(_) => StatusCode::FORBIDDEN,

            // A query-language feature outside our subset is a problem with the
            // request, not the server — API clients surface a 400 body to the user,
            // which is where our "unsupported in telemetryd" message needs to end up.
            Error::Unsupported { .. } | Error::BadRequest(_) => StatusCode::BAD_REQUEST,

            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::LimitExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Error::Overloaded => StatusCode::TOO_MANY_REQUESTS,

            // An I/O failure is transient more often than not — a full disk being cleared,
            // a slow volume — and a writer told 500 gives up: the OTLP exporters retry only
            // 429, 502, 503 and 504, so a 500 on ingest was a batch lost for good. 503
            // with `Retry-After` gets it sent again.
            Error::Io { .. } => StatusCode::SERVICE_UNAVAILABLE,

            // Everything else is ours to fix.
            Error::Config(_)
            | Error::ConfigUnreadable { .. }
            | Error::SecretUnreadable { .. }
            | Error::SecretMissing { .. }
            | Error::SecretEmpty
            | Error::StorageVersionMismatch { .. }
            | Error::DataDirLocked { .. }
            | Error::WalCorrupt { .. }
            // Never reaches a client: relay delivery runs on the maintenance loop, not
            // in a request. Listed so adding a variant stays a compile error.
            | Error::RelayDelivery(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let mut body = self.0.to_body();
        if status.is_server_error() {
            // The detail goes to the log and not to the caller: an I/O error names paths
            // inside the data directory, and a panic message names code. The caller gets
            // what to do and a reference that finds the detail.
            let reference = reference();
            tracing::error!(reference, error = %self.0, code = self.0.code(), "request failed");
            body.error.message = if matches!(self.0, Error::Io { .. }) {
                format!(
                    "telemetryd could not read or write its storage just now; retry \
                     shortly. Reference {reference} in the server log has the detail."
                )
            } else {
                format!(
                    "telemetryd could not complete this request. Reference {reference} in \
                     the server log has the detail."
                )
            };
        }

        let mut response = (status, Json(body)).into_response();

        match &self.0 {
            Error::Unauthorized => {
                response.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Bearer realm=\"telemetryd\""),
                );
            }
            // Tell an overloaded client when to come back rather than leaving it to
            // guess and retry immediately, which is how a queue-full turns into a
            // thundering herd.
            Error::Overloaded => {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            }
            Error::Io { .. } => {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
            }
            _ => {}
        }

        response
    }
}

/// A short id tying a response to its log line. Unique enough to find one error in a
/// day's log; it identifies nothing else.
fn reference() -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
    #[allow(clippy::cast_possible_truncation)]
    let clock = telemetryd_store::now_nanos() as u32;
    format!("{:08x}", clock.rotate_left(13) ^ sequence)
}

/// The Prometheus API's own error envelope, on the Prometheus API.
///
/// Prometheus answers `{"status":"error","errorType":"bad_data","error":"…"}`, and
/// Grafana's Prometheus client reads `error` as a string. Ours was an object, so every
/// refusal surfaced in Grafana as a parse error rather than the message — and a 401,
/// 405 or timeout had no body at all. Rewritten here, on the way out, so the rest of the
/// server keeps one error shape; the telemetryd details (`code`, `hint`, `feature`,
/// `docs`) ride along as extra keys, which Prometheus clients ignore.
pub async fn prometheus_envelope(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = request.uri().path();
    let prometheus = path.starts_with("/api/v1/")
        && !path.starts_with("/api/v1/export")
        && !path.starts_with("/api/v1/write");
    let response = next.run(request).await;
    if !prometheus || response.status().is_success() {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, 1 << 20)
        .await
        .unwrap_or_default();
    let ours: Option<serde_json::Value> = serde_json::from_slice(&bytes).ok();
    let detail = ours.as_ref().map(|v| &v["error"]);
    let message = detail
        .and_then(|d| d["message"].as_str())
        .map(str::to_owned)
        .or_else(|| {
            let text = String::from_utf8_lossy(&bytes).trim().to_owned();
            (!text.is_empty()).then_some(text)
        })
        .unwrap_or_else(|| {
            parts
                .status
                .canonical_reason()
                .unwrap_or("request failed")
                .to_lowercase()
        });
    let error_type = match parts.status.as_u16() {
        400 | 413 | 422 => "bad_data",
        404 | 405 | 501 => "not_found",
        408 | 504 => "timeout",
        429 | 503 => "unavailable",
        _ => "internal",
    };
    let mut envelope = serde_json::json!({
        "status": "error",
        "errorType": error_type,
        "error": message,
    });
    if let Some(detail) = detail.and_then(serde_json::Value::as_object) {
        for key in ["code", "feature", "hint", "docs"] {
            if let Some(value) = detail.get(key) {
                envelope[key] = value.clone();
            }
        }
    }
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    parts.headers.remove(header::CONTENT_LENGTH);
    Response::from_parts(parts, axum::body::Body::from(envelope.to_string()))
}

/// What an API path telemetryd does not serve answers: `501` with a body that says so.
///
/// COMPATIBILITY.md promised this and the server sent an empty `404`, which a client
/// shows as a blank error. Under the API prefixes a missing route is a feature this
/// drop-in does not have yet, and saying so is the useful answer; anywhere else it is
/// just a wrong URL.
pub async fn unimplemented(uri: axum::http::Uri) -> Response {
    let path = uri.path();
    let api = ["/api/", "/loki/api/", "/prometheus/", "/otlp/"]
        .iter()
        .any(|prefix| path.starts_with(prefix));
    if !api {
        return ApiError(Error::NotFound(format!("no route for {path}"))).into_response();
    }
    let body = serde_json::json!({
        "error": {
            "code": "not_implemented",
            "message": format!(
                "telemetryd does not implement {path}; see COMPATIBILITY.md for what it serves"
            ),
            "docs": telemetryd_core::COMPATIBILITY_DOC,
        }
    });
    (StatusCode::NOT_IMPLEMENTED, Json(body)).into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A writer told 500 gives up — the OTLP exporters retry only 429, 502, 503 and
    /// 504 — so a transient disk error on ingest was a batch lost for good.
    #[test]
    fn an_io_failure_asks_the_client_to_retry() {
        let error = Error::io(
            "writing the write-ahead log",
            std::io::Error::other("no space left on device"),
        );
        let response = ApiError(error).into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().contains_key(header::RETRY_AFTER));
    }

    /// A 5xx tells the caller what to do and where the detail is, not the detail: an I/O
    /// error names paths inside the data directory and a panic names code.
    #[tokio::test]
    async fn a_server_error_keeps_its_detail_in_the_log() {
        for error in [
            Error::io(
                "writing /var/lib/telemetryd/wal/logs/00000001.wal",
                std::io::Error::other("no space left on device"),
            ),
            Error::Config("query task panicked: index out of bounds at promeval.rs:812".into()),
        ] {
            let response = ApiError(error).into_response();
            assert!(response.status().is_server_error());
            let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap();
            let body = String::from_utf8_lossy(&body);
            assert!(body.contains("Reference"), "{body}");
            for secret in ["/var/lib", "wal", "promeval", "panicked"] {
                assert!(!body.contains(secret), "{secret} leaked: {body}");
            }
        }
    }
}
