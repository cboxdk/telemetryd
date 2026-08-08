//! Bearer token middleware for the ingest and query surfaces (ADR-004).
//!
//! Two independent token sets, because the trust boundaries genuinely differ: app
//! servers push, humans and the UI read. An unset token set leaves that surface open,
//! which is safe by construction on loopback and refused at startup otherwise.

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use telemetryd_core::{Error, TokenSet};

use crate::error::ApiError;
use crate::state::AppState;

/// Which set of tokens guards a surface. The value doubles as the `surface` label on
/// `telemetryd_auth_failures_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    Ingest,
    Query,
    Admin,
}

impl Surface {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::Query => "query",
            Self::Admin => "admin",
        }
    }
}

pub async fn require_ingest_token(
    state: State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    guard(
        Surface::Ingest,
        &state.ingest_tokens.clone(),
        state,
        request,
        next,
    )
    .await
}

/// Guards the operational surface: `/status` and `/metrics`.
///
/// These describe the *deployment* rather than the telemetry — app names, per-app
/// series counts and disk share, whether the instance is running unauthenticated —
/// and that is a narrower audience than "may read logs".
pub async fn require_admin_token(
    state: State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    guard(
        Surface::Admin,
        &state.admin_tokens.clone(),
        state,
        request,
        next,
    )
    .await
}

pub async fn require_query_token(
    state: State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    guard(
        Surface::Query,
        &state.query_tokens.clone(),
        state,
        request,
        next,
    )
    .await
}

async fn guard(
    surface: Surface,
    tokens: &TokenSet,
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    if tokens.is_empty() {
        return Ok(next.run(request).await);
    }

    let presented = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer);

    match presented {
        Some(token) if tokens.verify(token) => Ok(next.run(request).await),
        _ => {
            // Counted, but deliberately not logged per-request: a scanner hitting an
            // exposed instance would otherwise write our disk full with our own logs.
            state.metrics.incr(
                "telemetryd_auth_failures_total",
                &[("surface", surface.as_str())],
            );
            Err(Error::Unauthorized.into())
        }
    }
}

/// Extract the credential from an `Authorization` header.
///
/// The scheme match is case-insensitive because RFC 7235 says it is, and clients in
/// the wild send `bearer` in lower case.
fn bearer(header: &str) -> Option<&str> {
    let (scheme, credential) = header.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| credential.trim())
        .filter(|token| !token.is_empty())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parses_bearer_headers_leniently_but_not_wrongly() {
        assert_eq!(bearer("Bearer abc123"), Some("abc123"));
        assert_eq!(bearer("bearer abc123"), Some("abc123"));
        assert_eq!(bearer("BEARER  abc123 "), Some("abc123"));

        assert_eq!(bearer("Basic abc123"), None);
        assert_eq!(bearer("abc123"), None);
        assert_eq!(bearer("Bearer "), None);
        assert_eq!(bearer(""), None);
    }
}
