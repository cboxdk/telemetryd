//! Bearer token middleware for the ingest and query surfaces.
//!
//! Two independent token sets, because the trust boundaries genuinely differ: app
//! servers push, humans and the UI read. An unset token set leaves that surface open,
//! which is safe by construction on loopback and refused at startup otherwise.

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use telemetryd_core::Error;

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
    guard(Surface::Ingest, state, request, next).await
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
    guard(Surface::Admin, state, request, next).await
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
/// A layer works for the query surface because those responses are built in full before
/// the handler returns. It does **not** work for a streamed one: `next.run` returns when
/// the head is ready, and the permit would be gone while the body — and the memory behind
/// it — still existed. `export` claims its own permit for that reason.
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
    let waited = std::time::Instant::now();
    let Some(permit) = state.query_slot(state.query_wait()).await else {
        state.metrics.incr(
            "telemetryd_query_rejected_total",
            &[("surface", "query"), ("reason", "concurrency")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };
    // Counted separately from refusals, because queueing is the early warning and a
    // refusal is the event. An instance whose queue counter climbs while nothing is
    // refused is one dashboard away from refusing, and nothing else would say so.
    if waited.elapsed() > std::time::Duration::from_millis(1) {
        state
            .metrics
            .incr("telemetryd_query_queued_total", &[("surface", "query")]);
    }
    // Held by the response *and* by the blocking read it starts. A timeout or a client
    // that hangs up drops the response future, and used to release the slot with it —
    // while the scan it had started kept running on a blocking thread. The concurrency
    // limit then bounded requests, not work: measured at 27 s of CPU after the 408, with
    // `queries_in_flight` reading zero. Reads started through `spawn_read` keep a share
    // of the permit until they finish.
    let permit = std::sync::Arc::new(permit);
    let response = QUERY_PERMIT
        .scope(std::sync::Arc::clone(&permit), next.run(request))
        .await;
    drop(permit);
    Ok(response)
}

tokio::task_local! {
    static QUERY_PERMIT: std::sync::Arc<tokio::sync::OwnedSemaphorePermit>;
}

/// `spawn_blocking` for a read, holding the request's query slot until the read is done.
///
/// Use this, not `spawn_blocking`, for anything a read route runs: the slot has to outlive
/// the request when the request is abandoned, or the limit stops meaning anything.
pub fn spawn_read<F, R>(work: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let held = QUERY_PERMIT.try_with(std::sync::Arc::clone).ok();
    tokio::task::spawn_blocking(move || {
        let _held = held;
        work()
    })
}

pub async fn require_query_token(
    state: State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    guard(Surface::Query, state, request, next).await
}

/// In strict relay mode every writer's `app` comes from its credential. An access
/// token that names no application leaves nothing to stamp, and letting it through
/// would trust the payload's own claim — the hole this mode exists to close, and the
/// one a bare ingest token is refused at startup for. A token cannot be checked at
/// startup, so it is refused here.
fn refuse_unidentified_writer(
    state: &AppState,
    surface: Surface,
    client_id: Option<&str>,
) -> Result<(), ApiError> {
    static SAID: std::sync::Once = std::sync::Once::new();
    if surface != Surface::Ingest
        || !state.config.relay.is_enabled()
        || state.config.relay.trust_client_identity
        || client_id.is_some()
    {
        return Ok(());
    }
    SAID.call_once(|| {
        tracing::warn!(
            "refusing ingest with an access token that names no client: relay mode \
             stamps each record's app from the credential, and this one carries no \
             client_id to stamp"
        );
    });
    state.metrics.incr(
        "telemetryd_auth_failures_total",
        &[("surface", surface.as_str())],
    );
    Err(Error::Forbidden(
        "this access token names no client (client_id), and relay mode identifies every \
         writer by its credential"
            .to_owned(),
    )
    .into())
}

async fn guard(
    surface: Surface,
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let credentials = state.credentials();
    let tokens = match surface {
        Surface::Ingest => &credentials.ingest,
        Surface::Query => &credentials.query,
        Surface::Admin => &credentials.admin,
    };
    // An unguarded surface stays unguarded only while *nothing* guards it. Turning on
    // Cbox ID must not leave a surface open just because its static token is unset.
    // Relay clients are *ingest* credentials, so they guard that surface and no
    // other. Counting them everywhere locked the read API behind a token no client
    // could present — the surface demanded one, and nothing could satisfy it.
    let relay_guards_this = surface == Surface::Ingest && !credentials.relay_clients.is_empty();
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
        && let Some(app) = credentials.relay_clients.identify(token)
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
                    refuse_unidentified_writer(&state, surface, authorized.client_id.as_deref())?;
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

    /// Strict relay mode identifies every writer by its credential. An access token
    /// that names no client leaves nothing to stamp, so it may not write — while it may
    /// still read, and a token that names its client writes as that client.
    #[test]
    fn a_token_without_a_client_cannot_write_through_a_strict_relay() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = telemetryd_core::Config::default();
        config.storage.data_dir = Some(tmp.path().join("data"));
        config.relay.upstream = "http://127.0.0.1:4399".to_owned();
        let store = std::sync::Arc::new(telemetryd_store::Store::open(&config).unwrap());
        let state = AppState::new(std::sync::Arc::new(config), store).unwrap();

        assert!(refuse_unidentified_writer(&state, Surface::Ingest, None).is_err());
        assert!(refuse_unidentified_writer(&state, Surface::Ingest, Some("mobile")).is_ok());
        assert!(refuse_unidentified_writer(&state, Surface::Query, None).is_ok());
    }

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

    /// A request that times out, or whose client hangs up, drops its response future; the
    /// read it started does not stop. The slot has to stay taken until the read finishes,
    /// or the concurrency limit bounds requests while the work piles up behind it.
    #[tokio::test]
    async fn a_read_keeps_its_slot_after_the_request_is_abandoned() {
        use std::sync::Arc;
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::new(Arc::clone(&slots).try_acquire_owned().unwrap());
        let (release, wait) = std::sync::mpsc::channel::<()>();

        let read = QUERY_PERMIT.sync_scope(Arc::clone(&permit), || {
            spawn_read(move || {
                let _ = wait.recv();
            })
        });
        drop(permit); // the request is gone

        assert_eq!(
            slots.available_permits(),
            0,
            "the slot went back while the read ran"
        );
        release.send(()).unwrap();
        read.await.unwrap();
        assert_eq!(
            slots.available_permits(),
            1,
            "and comes back once it is done"
        );
    }
}
