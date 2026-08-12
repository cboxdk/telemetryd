//! telemetryd's HTTP surface — one port for ingest, query and the UI-facing APIs.
//!
//! The router is the composition root, and it is built independently of any listening
//! socket so contract tests can drive it in-process with `tower::ServiceExt::oneshot`
//!.

pub mod auth;
pub mod error;
pub mod export;
pub mod index;
pub mod ingest;
pub mod loki;
pub mod maintenance;
pub mod metrics;
pub mod oidc;
pub mod prometheus;
pub mod relay;
pub mod routes;
pub mod state;
pub mod tempo;
pub mod tls;

use std::sync::Arc;

use axum::Router;
use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::get;
use telemetryd_core::{Config, Result};
use telemetryd_store::Store;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

pub use state::AppState;
pub use tls::Bound;

/// Build the complete router.
pub fn router(state: AppState) -> Router {
    let max_body =
        usize::try_from(state.config.server.max_body_bytes.as_u64()).unwrap_or(usize::MAX);
    let request_timeout = state.config.server.request_timeout;

    // Unauthenticated: liveness, and an index that says what this is. Neither carries
    // telemetry, neither describes the deployment, and load balancers need `/healthz` to
    // work before anyone has configured a token.
    let public = Router::new()
        .route("/", get(index::index))
        .route("/healthz", get(routes::healthz));

    // `/status` and `/metrics` describe the deployment rather than the telemetry:
    // every app name, its series count, its share of the disk, whether the instance is
    // running unauthenticated. That is a narrower audience than "may read logs", so
    // they sit behind the admin token — which falls back to the query token when
    // unset, exactly as they were guarded before the role existed.
    let admin = Router::new()
        .route("/metrics", get(routes::metrics))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_admin_token,
        ));

    // `/status` is the same admin surface with one addition: a caller it refuses gets
    // told what it reached rather than only that it may not have it. The deployment
    // picture still needs the admin token and is unchanged for everyone who holds one;
    // everyone else gets the identity, which is four constants of the build. See
    // `auth::admin_token_or_identity` for why that branch cannot fail the wrong way.
    let status = Router::new()
        .route("/status", get(routes::status))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::admin_token_or_identity,
        ));

    let query = Router::new()
        // Loki-compatible read APIs (M1).
        .route("/loki/api/v1/query_range", get(loki::query_range))
        .route("/loki/api/v1/labels", get(loki::labels))
        .route("/loki/api/v1/label/{name}/values", get(loki::label_values))
        .route("/loki/api/v1/series", get(loki::series))
        // Full-fidelity export, straight from the store rather than through a query
        // language. Behind the query token: it returns telemetry.
        .route("/api/v1/export", get(export::export))
        .route("/loki/api/v1/tail", get(loki::tail))
        // Tempo-compatible read APIs (M2).
        .route("/api/traces/{trace_id}", get(tempo::trace))
        .route("/api/search", get(tempo::search))
        .route("/api/search/tags", get(tempo::tags))
        .route("/api/v2/search/tags", get(tempo::tags))
        // The UI calls the v2 path; v1 is kept for older clients.
        .route("/api/v2/search/tag/{name}/values", get(tempo::tag_values))
        .route("/api/search/tag/{name}/values", get(tempo::tag_values))
        // Prometheus-compatible read APIs (M3). `buildinfo` is the UI's primary probe;
        // without it every connection check shows a degraded backend.
        .route("/api/v1/status/buildinfo", get(prometheus::build_info))
        .route(
            "/api/v1/query",
            get(prometheus::instant).post(prometheus::instant),
        )
        .route(
            "/api/v1/query_range",
            get(prometheus::range).post(prometheus::range),
        )
        .route("/api/v1/labels", get(prometheus::labels))
        .route("/api/v1/label/{name}/values", get(prometheus::label_values))
        .route("/api/v1/series", get(prometheus::series))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_query_token,
        ));

    let ingest = Router::new()
        // OTLP/HTTP JSON is the first-class ingest path; laravel-telemetry sends JSON,
        // so protobuf is never on the client's critical path.
        .route("/v1/logs", axum::routing::post(ingest::otlp_logs))
        .route("/v1/traces", axum::routing::post(ingest::otlp_traces))
        .route("/v1/metrics", axum::routing::post(ingest::otlp_metrics))
        .route("/api/v1/write", axum::routing::post(ingest::remote_write))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_ingest_token,
        ));

    Router::new()
        .merge(public)
        .merge(admin)
        .merge(status)
        .merge(query)
        .merge(ingest)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            record_request,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ))
        .layer(RequestBodyLimitLayer::new(max_body))
        // axum's extractors carry their own 2 MB default, which silently won whatever
        // `server.max_body_bytes` said: a 16 MiB setting rejected anything past 2 MiB,
        // and raising it changed nothing. Disable it so the configured value is the
        // only limit, enforced by the layer above.
        .layer(axum::extract::DefaultBodyLimit::disable())
        // A panic in one handler must not take the process down and lose the WAL's
        // unsynced window along with it.
        .layer(CatchPanicLayer::new())
        .with_state(state)
}

/// Count every request by *matched route*, never by raw path.
///
/// Using the URI would let any caller mint unbounded label cardinality in our own
/// metrics — the exact failure mode telemetryd exists to cap for its users.
async fn record_request(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or_else(|| "<unmatched>".to_owned(), |m| m.as_str().to_owned());
    let method = request.method().clone();

    let response = next.run(request).await;

    state.metrics.incr(
        "telemetryd_http_requests_total",
        &[
            ("route", &route),
            ("method", method.as_str()),
            ("status", response.status().as_str()),
        ],
    );
    response
}

/// Bind the listener and serve until shutdown is signalled.
pub async fn serve(config: Arc<Config>, store: Arc<Store>) -> Result<()> {
    let state = AppState::new(Arc::clone(&config), Arc::clone(&store))?;

    // Two listener types, one serving path. `Bound` exists so everything after this —
    // the key fetch, the maintenance timers, the graceful drain — is identical whether
    // or not we terminate TLS. A second copy of the shutdown logic is how one of them
    // ends up subtly different.
    let bound = if config.server.tls.is_enabled() {
        let (cert_file, key_file) = if config.server.tls.is_self_signed() {
            let dir = store.data_dir().root().join("tls");
            tls::ensure_self_signed(&dir, &config.server.tls.self_signed_names())?
        } else {
            // Validation guarantees both are present together, so a missing one here
            // would be a bug rather than a configuration mistake.
            match (&config.server.tls.cert_file, &config.server.tls.key_file) {
                (Some(cert), Some(key)) => (cert.clone(), key.clone()),
                _ => {
                    return Err(telemetryd_core::Error::Config(
                        "server.tls is enabled without both cert_file and key_file".to_owned(),
                    ));
                }
            }
        };
        let tls_config = tls::server_config(&cert_file, &key_file)?;
        let listener = tls::TlsListener::bind(config.server.listen, tls_config).await?;
        Bound::Tls(Box::new(listener))
    } else {
        let listener = tokio::net::TcpListener::bind(config.server.listen)
            .await
            .map_err(|e| {
                telemetryd_core::Error::io(format!("binding {}", config.server.listen), e)
            })?;
        Bound::Plain(listener)
    };

    let local = bound.local_addr().unwrap_or(config.server.listen);
    tracing::info!(
        listen = %local,
        scheme = if config.server.tls.is_enabled() { "https" } else { "http" },
        data_dir = %store.data_dir().root().display(),
        version = telemetryd_core::VERSION,
        "telemetryd is serving"
    );
    if config.server.insecure {
        tracing::warn!(
            listen = %local,
            "running with --insecure: this instance accepts unauthenticated requests \
             from the network"
        );
    }

    // Fetch the key set before serving, so the first request does not pay for it —
    // but do not *require* it. An identity provider that is down must not stop
    // telemetryd starting: static tokens keep working, and the refresh loop recovers
    // when it returns. Failing closed here would be the coupling this design exists to
    // avoid, in its worst form.
    if state.oidc.is_enabled() {
        let oidc = Arc::clone(&state.oidc);
        match tokio::task::spawn_blocking(move || oidc.refresh()).await {
            Ok(Ok(keys)) => tracing::info!(
                issuer = %config.auth.oidc.issuer,
                keys,
                "accepting OIDC access tokens"
            ),
            Ok(Err(error)) => tracing::warn!(
                issuer = %config.auth.oidc.issuer,
                %error,
                "could not load the OIDC key set at startup; those tokens will be \
                 refused until it can be fetched. Static tokens are unaffected."
            ),
            Err(error) => tracing::error!(%error, "the key fetch task panicked"),
        }
    }

    if let Some(relay) = &state.relay {
        tracing::info!(
            upstream = %config.relay.upstream,
            trust_client_identity = config.relay.trust_client_identity,
            "relay mode: forwarding sealed segments upstream"
        );
        let _ = relay;
    }

    let maintenance = maintenance::Maintenance::start_with(
        &store,
        config.storage.wal_sync_interval,
        Some(Arc::clone(&state.oidc)),
        state.relay.clone(),
        config.relay.interval,
    );

    // `server.shutdown_grace` bounds the drain. Without a bound, one client holding a
    // long tail connection open keeps the process alive indefinitely, and the service
    // manager eventually SIGKILLs it — losing the interval-sync window the graceful
    // path exists to protect. Better to stop waiting and flush.
    let grace = config.server.shutdown_grace;

    // The clock starts when the signal arrives, not when the server does. Wrapping the
    // whole serve future in a timeout would have exited a healthy process after
    // `shutdown_grace` seconds of ordinary uptime.
    let (signalled, mut wait_for_signal) = tokio::sync::watch::channel(false);
    let serving = axum::serve(bound, router(state)).with_graceful_shutdown(async move {
        shutdown_signal().await;
        let _ = signalled.send(true);
    });
    let deadline = async move {
        // Ignore a send error: the sender is dropped once serving finishes, which is
        // exactly the case where the deadline no longer matters.
        let _ = wait_for_signal.wait_for(|started| *started).await;
        tokio::time::sleep(grace).await;
    };

    tokio::select! {
        result = serving => result.map_err(|e| telemetryd_core::Error::io("serving HTTP", e))?,
        () = deadline => tracing::warn!(
            grace_seconds = grace.as_secs(),
            "connections were still open when server.shutdown_grace elapsed; \
             stopping anyway and flushing the write-ahead log"
        ),
    }

    // Stop the timers first, so nothing is mid-seal while we are trying to stop.
    maintenance.stop();

    // In-flight requests are drained by now; this is the last chance to make the
    // interval-sync window durable, so a clean stop never loses records.
    tracing::info!("draining complete, flushing write-ahead log");
    store.sync_all()?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "could not install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => tracing::info!("received interrupt, shutting down"),
        () = terminate => tracing::info!("received SIGTERM, shutting down"),
    }
}
