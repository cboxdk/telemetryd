//! Request handlers.
//!
//! M0 implements the self-observability endpoints. The ingest and query routes are
//! registered but answer `501` — pointing `laravel-telemetry` at an early build should
//! produce "not yet, planned for M1", not a `404` that reads as a wrong URL.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use axum::Json;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use telemetryd_core::{Error, VERSION};
use telemetryd_store::StoreStatus;

use crate::error::ApiError;
use crate::metrics::Sample;
use crate::state::AppState;

/// Liveness. Never authenticated, never touches the store — a health check that fails
/// because the disk is busy tells a load balancer the wrong thing.
pub async fn healthz() -> Response {
    (StatusCode::OK, "ok\n").into_response()
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub version: &'static str,
    pub milestone: &'static str,
    pub storage_format_version: u32,
    pub started_at: String,
    pub uptime_seconds: f64,
    pub listen: String,
    /// True when the operator bypassed the ADR-004 bind check. Surfaced here so
    /// "is this instance exposed without auth?" is answerable from a dashboard.
    pub insecure: bool,
    /// `off`, `certificate` or `self-signed`.
    ///
    /// "Is this connection encrypted, and by what" was answerable only by reading the
    /// configuration file on the box — which is exactly the thing you cannot do from a
    /// dashboard, and exactly what you want to check across a fleet. `self-signed` is
    /// distinguished from `certificate` because they are not the same promise: one
    /// encrypts, the other also authenticates.
    pub tls: &'static str,
    pub auth: AuthStatus,
    pub storage: StoreStatus,
    /// What each app holds, largest first. The answer to "who is filling my disk",
    /// which every other number here is a total of.
    pub apps: Vec<telemetryd_store::AppUsage>,
    pub retention: BTreeMap<&'static str, String>,
    pub limits: LimitsStatus,
    /// Forwarding upstream. Absent unless relay mode is on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relay: Option<crate::relay::RelayStatus>,
}

#[derive(Debug, Serialize)]
pub struct AuthStatus {
    pub ingest: &'static str,
    pub query: &'static str,
    pub admin: &'static str,
    /// Present only when an issuer is configured. `keys: 0` is the shape to alert on:
    /// it means Cbox ID tokens are being refused because the key set never loaded, and
    /// nothing else in the response would say so.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc: Option<OidcStatus>,
}

#[derive(Debug, Serialize)]
pub struct OidcStatus {
    pub issuer: String,
    pub keys: usize,
    pub keys_stale: bool,
}

#[derive(Debug, Serialize)]
pub struct LimitsStatus {
    pub max_series: u64,
    pub max_series_per_app: u64,
    pub max_log_line_bytes: u64,
    pub ingest_queue_depth: u32,
}

pub async fn status(State(state): State<AppState>) -> Result<Json<StatusResponse>, ApiError> {
    let config = &state.config;
    let storage = state.store.snapshot()?;

    let started_at = state
        .started_at
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown".to_owned());

    let retention = BTreeMap::from([
        (
            "logs",
            humantime::format_duration(config.retention.logs.get()).to_string(),
        ),
        (
            "traces",
            humantime::format_duration(config.retention.traces.get()).to_string(),
        ),
        (
            "metrics",
            humantime::format_duration(config.retention.metrics.get()).to_string(),
        ),
    ]);

    Ok(Json(StatusResponse {
        version: VERSION,
        milestone: crate::MILESTONE,
        storage_format_version: telemetryd_core::STORAGE_FORMAT_VERSION,
        started_at,
        uptime_seconds: state.uptime_seconds(),
        listen: config.server.listen.to_string(),
        insecure: config.server.insecure,
        tls: config.server.tls.posture(),
        auth: AuthStatus {
            ingest: enabled(state.ingest_tokens.is_empty()),
            query: enabled(state.query_tokens.is_empty()),
            admin: enabled(state.admin_tokens.is_empty()),
            oidc: state.oidc.is_enabled().then(|| OidcStatus {
                issuer: config.auth.oidc.issuer.clone(),
                keys: state.oidc.key_count(),
                keys_stale: state.oidc.is_stale(),
            }),
        },
        storage,
        retention,
        apps: state.store.app_usage(),
        limits: LimitsStatus {
            max_series: config.limits.max_series,
            max_series_per_app: config.limits.max_series_per_app,
            max_log_line_bytes: config.limits.max_log_line_bytes.as_u64(),
            ingest_queue_depth: config.limits.ingest_queue_depth,
        },
        relay: state.relay.as_ref().map(|relay| relay.status(&state.store)),
    }))
}

fn enabled(is_empty: bool) -> &'static str {
    if is_empty { "disabled" } else { "enabled" }
}

pub async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    let body = state.metrics.render(&gauges(&state)?);
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response())
}

/// Read live gauge values at scrape time. See [`crate::metrics::Metrics::render`] for
/// why these are not cached.
///
/// Series limits and the Cbox ID key set.
///
/// Split out so `gauges` stays readable; both describe the *deployment* rather than
/// the telemetry, which is why they sit behind the admin token.
#[allow(clippy::cast_precision_loss)]
fn push_auth_and_cardinality_gauges(state: &AppState, samples: &mut Vec<Sample>) {
    if state.oidc.is_enabled() {
        samples.push(Sample::new(
            "telemetryd_oidc_keys",
            &[],
            state.oidc.key_count() as f64,
        ));
    }

    if let Some(relay) = &state.relay {
        // The backlog is the number worth alerting on: delivered totals only rise, so
        // they cannot tell you the shipper has stopped, and a backlog that keeps
        // growing is the only thing that says so.
        for (signal, pending) in relay.backlog(&state.store) {
            samples.push(Sample::new(
                "telemetryd_relay_pending_segments",
                &[("signal", signal.as_str())],
                pending as f64,
            ));
        }
        for (name, value) in [
            (
                "telemetryd_relay_segments_delivered_total",
                relay.stats.segments_delivered.load(Ordering::Relaxed),
            ),
            (
                "telemetryd_relay_records_delivered_total",
                relay.stats.records_delivered.load(Ordering::Relaxed),
            ),
            (
                "telemetryd_relay_failures_total",
                relay.stats.failures.load(Ordering::Relaxed),
            ),
        ] {
            samples.push(Sample::new(name, &[], value as f64));
        }
    }

    let (active_series, rejected_series, max_series, _) = state.store.cardinality_status();
    for (name, value) in [
        ("telemetryd_series_active", active_series as f64),
        ("telemetryd_series_rejected_total", rejected_series as f64),
        ("telemetryd_series_limit", max_series as f64),
    ] {
        samples.push(Sample::new(name, &[], value));
    }
}

/// Every sample is `f64` because that is what the Prometheus exposition format is; the
/// counts and byte totals here are far below the 2^53 boundary where that matters.
/// Per-signal storage and write-ahead-log numbers.
///
/// Split out of `gauges` for length, which is also why it is worth naming what these
/// are for: `wal_unsynced_records` is how much a power cut would cost right now, and
/// `segments_scanned` against `segments_pruned` is whether pruning is earning its keep.
#[allow(clippy::cast_precision_loss)]
fn push_per_signal_gauges(snapshot: &telemetryd_store::StoreStatus, samples: &mut Vec<Sample>) {
    for (signal, stats) in [
        ("logs", &snapshot.logs),
        ("traces", &snapshot.traces),
        ("metrics", &snapshot.metrics),
    ] {
        for (name, value) in [
            ("telemetryd_records_buffered", stats.buffered_records as f64),
            (
                "telemetryd_records_appended_total",
                stats.appended_records as f64,
            ),
            ("telemetryd_segments", stats.segments as f64),
            ("telemetryd_segment_rows", stats.segment_rows as f64),
            (
                "telemetryd_segments_sealed_total",
                stats.sealed_segments as f64,
            ),
            // Non-zero means data has been lost to a damaged file. Worth an alert.
            (
                "telemetryd_segments_unreadable_total",
                stats.segments_unreadable as f64,
            ),
            // Whether pruning is doing anything: scanned against pruned is the ratio
            // that says whether the manifests are earning their keep.
            (
                "telemetryd_query_segments_scanned_total",
                stats.segments_scanned as f64,
            ),
            (
                "telemetryd_query_segments_pruned_total",
                stats.segments_pruned as f64,
            ),
            ("telemetryd_wal_segments", stats.wal.segments as f64),
            (
                "telemetryd_wal_records_total",
                stats.wal.appended_records as f64,
            ),
            // How much a power cut would cost right now. All five of these were
            // declared in the descriptor table and never given a value: `/metrics`
            // published HELP and TYPE for numbers that could not exist.
            (
                "telemetryd_wal_unsynced_records",
                stats.wal.unsynced_records as f64,
            ),
        ] {
            samples.push(Sample::new(name, &[("signal", signal)], value));
        }
    }
}

#[allow(clippy::cast_precision_loss)]
fn gauges(state: &AppState) -> Result<Vec<Sample>, Error> {
    let snapshot = state.store.snapshot()?;
    let mut samples = vec![
        // TLS posture as a label rather than its own gauge: it is a property of the
        // deployment, not a measurement, and `build_info` is where Prometheus expects
        // to find those. `off` is a value rather than an absent series, so an alert can
        // say "no instance should be serving plaintext" without guessing.
        Sample::new(
            "telemetryd_build_info",
            &[
                ("version", VERSION),
                ("tls", state.config.server.tls.posture()),
            ],
            1.0,
        ),
        Sample::new("telemetryd_uptime_seconds", &[], state.uptime_seconds()),
        Sample::new(
            "telemetryd_disk_budget_bytes",
            &[],
            snapshot.disk_budget_bytes as f64,
        ),
        Sample::new(
            "telemetryd_storage_over_budget",
            &[],
            f64::from(u8::from(snapshot.over_budget)),
        ),
        Sample::new(
            "telemetryd_wal_truncations_total",
            &[],
            snapshot.wal_truncations.len() as f64,
        ),
    ];

    for (kind, bytes) in [
        ("wal", snapshot.usage.wal_bytes),
        ("segments", snapshot.usage.segment_bytes),
        ("metrics", snapshot.usage.metric_bytes),
        ("tmp", snapshot.usage.tmp_bytes),
    ] {
        samples.push(Sample::new(
            "telemetryd_disk_used_bytes",
            &[("kind", kind)],
            bytes as f64,
        ));
    }

    // All three signals, not just logs. A trace store filling up or a metric segment
    // going unreadable is exactly as worth alerting on, and it used to be invisible
    // here while showing up in /status — two answers to the same question.
    push_per_signal_gauges(&snapshot, &mut samples);

    push_auth_and_cardinality_gauges(state, &mut samples);

    // Per app, so a dashboard can show who is growing rather than only that the total
    // is. Cardinality is capped per app, and until this existed the cap was enforceable
    // but not observable.
    for usage in state.store.app_usage() {
        let app = [("app", usage.app.as_str())];
        samples.push(Sample::new(
            "telemetryd_app_series",
            &app,
            usage.series as f64,
        ));
        samples.push(Sample::new("telemetryd_app_rows", &app, usage.rows as f64));
        samples.push(Sample::new(
            "telemetryd_app_bytes_estimate",
            &app,
            usage.estimated_bytes as f64,
        ));
    }

    // Retention outcomes belong in metrics, not just logs: deleting a user's data is
    // never something they should have to grep for.
    let reaper = &snapshot.retention;
    samples.push(Sample::new(
        "telemetryd_retention_deleted_total",
        &[("reason", "age")],
        reaper.deleted_by_age as f64,
    ));
    samples.push(Sample::new(
        "telemetryd_retention_deleted_total",
        &[("reason", "disk_budget")],
        reaper.deleted_by_budget as f64,
    ));
    samples.push(Sample::new(
        "telemetryd_tail_subscribers",
        &[],
        state.tail_subscribers() as f64,
    ));

    Ok(samples)
}
