//! Bearer token middleware for the ingest and query surfaces.
//!
//! Two independent token sets, because the trust boundaries genuinely differ: app
//! servers push, humans and the UI read. An unset token set leaves that surface open,
//! which is safe by construction on loopback and refused at startup otherwise.

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
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

/// Guards `/status`, which answers everyone — but not with the same document.
///
/// A credential that satisfies [`require_admin_token`] gets the full deployment picture,
/// byte for byte what it got before this existed. Anything else gets
/// [`crate::routes::status_identity`]: what this software is, and nothing about where it
/// runs.
///
/// # Why this branch cannot fail open
///
/// Making an endpoint dual-mode puts one `match` between "no credential" and everything,
/// and that branch must never fail the wrong way. It cannot here, because the full
/// document is not a branch at all: it is only ever produced by `next.run(request)` deep
/// inside `guard`, which is reached solely after a token verified — or after `guard`
/// established that nothing guards this surface, the pre-existing open case this does not
/// touch. Every other path out of `guard`, including any future one, is an `Err`, and
/// every `Err` lands on the identity. The default is the disclosing-nothing answer, and a
/// mistake here loses the deployment picture rather than leaking it.
pub async fn admin_token_or_identity(
    state: State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    match require_admin_token(state, request, next).await {
        Ok(response) => response,
        // Deliberately narrow. A refusal is the case this widens; a 500 from somewhere
        // else must still read as a 500, not as a cheerful 200 that says the server is
        // fine.
        Err(ApiError(Error::Unauthorized)) => crate::routes::status_identity(),
        Err(other) => other.into_response(),
    }
}

/// Hold a read slot for the length of the request, or refuse it.
///
/// A layer rather than a line in each handler: there are fourteen read routes, and the
/// one that gets forgotten is the one that matters. The permit is moved into the response
/// future, so it is released when the response is fully built — including when the
/// handler returns early or panics.
///
/// The refusal is `429` with `Retry-After`, the same answer a full ingest queue gives,
/// because it means the same thing: come back, this is not about your request.
pub async fn limit_query_concurrency(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(permit) = state.query_slot() else {
        state.metrics.incr(
            "telemetryd_query_rejected_total",
            &[("surface", "query"), ("reason", "concurrency")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };
    let response = next.run(request).await;
    drop(permit);
    Ok(response)
}

/// The same, against the smaller budget one export is measured against.
pub async fn limit_export_concurrency(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let Some(permit) = state.export_slot() else {
        state.metrics.incr(
            "telemetryd_query_rejected_total",
            &[("surface", "export"), ("reason", "concurrency")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };
    let response = next.run(request).await;
    drop(permit);
    Ok(response)
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
                    //
                    // But two of these are not guesses, they are incompatible
                    // configuration — and debug is the wrong level for something the
                    // operator caused and must fix. A sender-constrained token means
                    // DPoP is on at the issuer; a wrong media type usually means an id
                    // token was sent instead of an access token. Both produce 401 on
                    // *every* request with an empty body, which is close to
                    // undiagnosable from the outside.
                    //
                    // Said once per process, because an attacker who can pick the
                    // rejection reason must not be able to pick our log volume.
                    match &reason {
                        Rejected::SenderConstrained => {
                            static SAID: std::sync::Once = std::sync::Once::new();
                            SAID.call_once(|| {
                                tracing::warn!(
                                    "refusing a sender-constrained (DPoP) access token: \
                                 telemetryd cannot validate the proof, and accepting it \
                                 as a plain bearer would discard the binding. Issue \
                                 bearer tokens for telemetryd, or see the single sign-on \
                                 guide."
                                );
                            });
                        }
                        Rejected::WrongTokenType(found) => {
                            static SAID: std::sync::Once = std::sync::Once::new();
                            let found = found.clone();
                            SAID.call_once(|| {
                                tracing::warn!(
                                    %found,
                                    "refusing a token whose media type is not at+jwt \
                                     (RFC 9068). An id token carries `JWT` and authorises \
                                     nothing; send the access token instead."
                                );
                            });
                        }
                        _ => {}
                    }
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
