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

            // A query-language feature outside our subset is a problem with the
            // request, not the server — API clients surface a 400 body to the user,
            // which is where our "unsupported in telemetryd" message needs to end up.
            Error::Unsupported { .. } | Error::BadRequest(_) => StatusCode::BAD_REQUEST,

            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::LimitExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Error::Overloaded => StatusCode::TOO_MANY_REQUESTS,
            Error::NotImplemented { .. } => StatusCode::NOT_IMPLEMENTED,

            // Everything else is ours to fix.
            Error::Config(_)
            | Error::ConfigUnreadable { .. }
            | Error::SecretUnreadable { .. }
            | Error::SecretMissing { .. }
            | Error::SecretEmpty
            | Error::StorageVersionMismatch { .. }
            | Error::DataDirLocked { .. }
            | Error::Io { .. }
            | Error::WalCorrupt { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // Log genuine faults only. A 501 is a 5xx by status but a deliberate,
        // documented state of an early build — logging it at ERROR would train
        // operators to ignore the level that matters.
        if status.is_server_error() && !matches!(self.0, Error::NotImplemented { .. }) {
            tracing::error!(error = %self.0, code = self.0.code(), "request failed");
        }

        let mut response = (status, Json(self.0.to_body())).into_response();

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
            _ => {}
        }

        response
    }
}
