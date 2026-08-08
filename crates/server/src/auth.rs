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
use crate::oidc::Rejected;
use crate::state::AppState;

/// Who a request turned out to be, decided by the credential rather than the payload.
///
/// Inserted into the request's extensions by the guard, because the identity is
/// established where the credential is checked and needed where the records are
/// decoded. Passing it any other way would mean the ingest handler re-deriving it, and
/// two places deciding who someone is, is one too many.
#[derive(Debug, Clone)]
pub struct ClientIdentity {
    /// The `app` this client is allowed to be.
    pub app: String,
}

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
    // An unguarded surface stays unguarded only while *nothing* guards it. Turning on
    // Cbox ID must not leave a surface open just because its static token is unset.
    // Relay clients are *ingest* credentials, so they guard that surface and no
    // other. Counting them everywhere locked the read API behind a token no client
    // could present — the surface demanded one, and nothing could satisfy it.
    let relay_guards_this = surface == Surface::Ingest && !state.relay_clients.is_empty();
    if tokens.is_empty() && !state.oidc.is_enabled() && !relay_guards_this {
        return Ok(next.run(request).await);
    }

    let presented = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer);

    // A relay client's credential is checked first and identifies it at the same
    // time: one lookup answers both "may you write" and "as whom".
    if surface == Surface::Ingest
        && let Some(token) = presented
        && let Some(app) = state.relay_clients.identify(token)
    {
        let app = app.to_owned();
        let mut request = request;
        request.extensions_mut().insert(ClientIdentity { app });
        return Ok(next.run(request).await);
    }

    match presented {
        Some(token) if tokens.verify(token) => Ok(next.run(request).await),
        // A static token is checked first and in constant time; only something that is
        // not one reaches the token validator, so enabling Cbox ID costs a static
        // deployment nothing.
        Some(token) if state.oidc.is_enabled() => {
            let mut outcome = state.oidc.authorize(token, surface);
            // A key id we have not seen is what a rotation looks like, so refetch once
            // and try again. The refetch talks to the network, and this is an async
            // runtime worker — doing it inline would park the worker for as long as
            // the issuer feels like taking, and an attacker picks when that happens by
            // choosing the `kid`. `refresh_if_due` rate-limits it to once a minute.
            if outcome == Err(Rejected::UnknownKey) {
                let oidc = std::sync::Arc::clone(&state.oidc);
                if tokio::task::spawn_blocking(move || oidc.refresh_if_due())
                    .await
                    .is_ok()
                {
                    outcome = state.oidc.authorize(token, surface);
                }
            }
            match outcome {
                Ok(authorized) => {
                    tracing::debug!(
                        surface = surface.as_str(),
                        subject = %authorized.subject,
                        "authorized by Cbox ID"
                    );
                    let mut request = request;
                    // `client_id` names the application the token was issued to, and
                    // the issuer reserves it against being overwritten. `sub` is a
                    // *user*, which is not what a record's `app` label means.
                    if let Some(client_id) = authorized.client_id {
                        request
                            .extensions_mut()
                            .insert(ClientIdentity { app: client_id });
                    }
                    Ok(next.run(request).await)
                }
                Err(reason) => {
                    // The reason is logged, never returned: a 401 that explains *why* is a
                    // hint to whoever is guessing.
                    tracing::debug!(surface = surface.as_str(), ?reason, "token refused");
                    state.metrics.incr(
                        "telemetryd_auth_failures_total",
                        &[("surface", surface.as_str())],
                    );
                    Err(Error::Unauthorized.into())
                }
            }
        }
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
