//! `GET /api/v1/export` — a time range as OTLP NDJSON, straight from the store.
//!
//! [ADR-012](../../docs/adr/0012-import-and-export.md) built export on top of the Loki
//! API, and for logs that works: telemetryd serves the API it reads, so one path covers
//! its own data and a foreign backend's. For traces and metrics it does not, and the
//! ADR said so rather than shipping something lossy:
//!
//! - a trace search interface answers "which traces match this" and not "every trace in
//!   this window", so an exporter built on it returns a subset while looking complete
//! - a metric range query returns points at whatever `step` was asked for — a rendering
//!   of the series, not the samples that were stored
//!
//! Neither problem is about effort, and neither goes away with more care at the client.
//! They go away by not going through a query language at all: this reads records and
//! hands them to the same encoder relay mode uses, so what comes out is what was stored.
//!
//! It only works against telemetryd. That is the trade — full fidelity for our own data,
//! and the Loki path still there for everyone else's.
//!
//! # Bounded, and walked by the client
//!
//! A range may be far larger than memory, and the store's scan returns a `Vec` — so an
//! unbounded request would load the whole range twice, once as records and once as
//! strings. An earlier draft of this file did exactly that and described itself as
//! streaming, which it was not.
//!
//! So one request returns at most `limit` records, oldest first, and the client advances
//! `start` past the newest timestamp it saw. Same discipline as the Loki path: the
//! cursor is a timestamp rather than an offset, because records arrive while you page
//! and an offset would shift underneath you.

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use telemetryd_core::{Error, Signal};
use telemetryd_ingest::otlp_encode;
use telemetryd_store::{Order, Scan};

use crate::error::ApiError;
use crate::state::AppState;

/// Records per line. Each becomes one OTLP request when replayed, so this is also the
/// batch an importer will post.
const BATCH: usize = 2_000;
/// Records one request will return. The client walks the range in these.
const DEFAULT_LIMIT: usize = 50_000;
const MAX_LIMIT: usize = 200_000;

#[derive(Debug, Deserialize)]
pub struct ExportParams {
    /// `logs`, `traces` or `metrics`.
    pub signal: String,
    /// Unix nanoseconds.
    pub start: u64,
    pub end: u64,
    /// Records per request. Bounded so one call cannot ask the server to hold a whole
    /// store in memory.
    #[serde(default)]
    pub limit: Option<usize>,
}

fn signal_of(name: &str) -> Result<Signal, Error> {
    match name {
        "logs" => Ok(Signal::Logs),
        "traces" => Ok(Signal::Traces),
        "metrics" => Ok(Signal::Metrics),
        other => Err(Error::BadRequest(format!(
            "unknown signal {other:?}; expected logs, traces or metrics"
        ))),
    }
}

/// `GET /api/v1/export?signal=traces&start=…&end=…`
///
/// Ascending, because an export is read start to finish and a reader following along
/// should see time move forwards.
pub async fn export(
    State(state): State<AppState>,
    Query(params): Query<ExportParams>,
) -> Result<Response, ApiError> {
    let signal = signal_of(&params.signal)?;
    if params.end < params.start {
        return Err(Error::BadRequest("end is before start".to_owned()).into());
    }

    let store = std::sync::Arc::clone(&state.store);
    let (start, end) = (params.start, params.end);
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);

    // Read on a blocking thread: scanning opens Parquet files, and doing that on a
    // runtime worker is what parked one in the Cbox ID path.
    let lines = tokio::task::spawn_blocking(move || -> Result<Vec<String>, Error> {
        let scan = Scan {
            start_nanos: start,
            end_nanos: end,
            limit,
            order: Order::Ascending,
            exact_key: None,
            required_text: None,
            // No narrowing: an export wants every column of every row, and a
            // projection here would silently drop whatever it left out.
            columns: None,
        };

        let mut lines = Vec::new();
        match signal {
            Signal::Logs => {
                let records = store.logs().scan(scan, &[], &|_| true)?;
                for batch in records.chunks(BATCH) {
                    lines.push(otlp_encode::encode_logs(batch).to_string());
                }
            }
            Signal::Traces => {
                let records = store.traces().scan(scan, &[], &|_| true)?;
                for batch in records.chunks(BATCH) {
                    lines.push(otlp_encode::encode_spans(batch).to_string());
                }
            }
            Signal::Metrics => {
                let records = store.metrics().scan(scan, &[], &|_| true)?;
                for batch in records.chunks(BATCH) {
                    lines.push(otlp_encode::encode_metrics(batch).to_string());
                }
            }
        }
        Ok(lines)
    })
    .await
    .map_err(|e| Error::Config(format!("export task panicked: {e}")))??;

    let body = lines
        .into_iter()
        .map(|line| line + "\n")
        .collect::<String>();

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/x-ndjson; charset=utf-8",
        )],
        body,
    )
        .into_response())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_signal_name_is_matched_exactly() {
        assert_eq!(signal_of("logs").unwrap(), Signal::Logs);
        assert_eq!(signal_of("traces").unwrap(), Signal::Traces);
        assert_eq!(signal_of("metrics").unwrap(), Signal::Metrics);
        // Named rather than guessed at: a typo should be a 400 that says what was
        // expected, not an empty export that looks like an empty range.
        let error = signal_of("Logs").unwrap_err().to_string();
        assert!(
            error.contains("expected logs, traces or metrics"),
            "{error}"
        );
    }
}
