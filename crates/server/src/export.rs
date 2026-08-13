//! `GET /api/v1/export` — a time range as OTLP NDJSON, straight from the store.
//!
//! Export was first built on top of the Loki
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
use telemetryd_core::Error;
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
    /// Which signal to export.
    ///
    /// Typed rather than a free string, so serde rejects anything else before the
    /// handler runs and the error names what was expected. A `String` here meant a
    /// typo produced an empty export, which is indistinguishable from an empty range.
    pub signal: Signal,
    /// Unix nanoseconds.
    pub start: u64,
    pub end: u64,
    /// Records per request. Bounded so one call cannot ask the server to hold a whole
    /// store in memory.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// The query parameter, as an enum so an unknown value is a 400 rather than an empty
/// answer.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Signal {
    Logs,
    Traces,
    Metrics,
}

impl From<Signal> for telemetryd_core::Signal {
    fn from(signal: Signal) -> Self {
        match signal {
            Signal::Logs => Self::Logs,
            Signal::Traces => Self::Traces,
            Signal::Metrics => Self::Metrics,
        }
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
    // Claimed here rather than by a router layer, and held until the last chunk is
    // written.
    //
    // The layer released it when the handler returned, which for a streaming response is
    // as soon as the *head* is built — while the scanned records are still alive in the
    // task producing the body. Measured with clients that never read: three admitted at
    // 317 MB, then three more, then three more, ending at 689 MB against a limit of 3. A
    // client that stalls is not exotic; it is what a slow network looks like.
    //
    // The permit moves into the emitting task below, so it is released when the body
    // finishes or when the client hangs up, which is when the memory is actually freed.
    let Some(permit) = state.export_slot() else {
        state.metrics.incr(
            "telemetryd_query_rejected_total",
            &[("surface", "export"), ("reason", "concurrency")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };

    let signal: telemetryd_core::Signal = params.signal.into();
    if params.end < params.start {
        return Err(Error::BadRequest("end is before start".to_owned()).into());
    }

    let store = std::sync::Arc::clone(&state.store);
    let (start, end) = (params.start, params.end);
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);

    // Read on a blocking thread: scanning opens Parquet files, and doing that on a
    // runtime worker is what parked one in the Cbox ID path.
    let scanned = tokio::task::spawn_blocking(move || -> Result<Scanned, Error> {
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

        Ok(match signal {
            telemetryd_core::Signal::Logs => {
                Scanned::Logs(store.logs().scan(scan, &[], &|_| true)?)
            }
            telemetryd_core::Signal::Traces => {
                Scanned::Traces(store.traces().scan(scan, &[], &|_| true)?)
            }
            telemetryd_core::Signal::Metrics => {
                Scanned::Metrics(store.metrics().scan(scan, &[], &|_| true)?)
            }
        })
    })
    .await
    .map_err(|e| Error::Config(format!("export task panicked: {e}")))??;

    // Encode and send batch by batch instead of building the whole document first.
    //
    // What this fixes: the response used to exist three times over at its peak — the
    // scanned records, a `Vec<String>` holding every batch already encoded as JSON, and
    // then a single `String` concatenating all of those. Measured at 139 MB for one
    // request against a 120,000-record store, and 32 concurrent requests reached 3.6 GB
    // — enough to end the process on any small VPS, from an endpoint a query token
    // reaches. Each chunk is now dropped as soon as it is written.
    //
    // The scan itself still materialises its records: paging it would mean advancing a
    // cursor past the last timestamp seen, and records sharing a nanosecond would be
    // duplicated or lost at the boundary. Correctness first; this removes the two copies
    // that can be removed without inventing a tiebreaker.
    //
    // The scan runs *before* the first byte goes out, deliberately. A stream that has
    // already sent `200 OK` cannot change its mind and return a `500`, so the fallible
    // half stays on this side of the response and encoding — which cannot fail — is what
    // streams.
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<String, std::io::Error>>(2);

    tokio::task::spawn_blocking(move || {
        // Moved in so it lives exactly as long as the records do.
        let _permit = permit;
        let emit = |line: String| {
            // `blocking_send` is the backpressure: a slow reader parks this thread
            // rather than letting encoded batches pile up in the channel. An error
            // means the client hung up, and there is nobody left to send to.
            sender.blocking_send(Ok(line + "\n")).is_ok()
        };
        match scanned {
            Scanned::Logs(records) => {
                for batch in records.chunks(BATCH) {
                    if !emit(otlp_encode::encode_logs(batch).to_string()) {
                        return;
                    }
                }
            }
            Scanned::Traces(records) => {
                for batch in records.chunks(BATCH) {
                    if !emit(otlp_encode::encode_spans(batch).to_string()) {
                        return;
                    }
                }
            }
            Scanned::Metrics(records) => {
                for batch in records.chunks(BATCH) {
                    if !emit(otlp_encode::encode_metrics(batch).to_string()) {
                        return;
                    }
                }
            }
        }
    });

    let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|item| (item, receiver))
    });

    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "application/x-ndjson; charset=utf-8",
        )],
        axum::body::Body::from_stream(stream),
    )
        .into_response())
}

/// The scan result, kept in one type so the streaming half has a single value to match
/// on rather than three parallel `Option`s.
enum Scanned {
    Logs(Vec<telemetryd_core::LogRecord>),
    Traces(Vec<telemetryd_core::span::SpanRecord>),
    Metrics(Vec<telemetryd_core::metric::MetricSample>),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// An unknown signal must be refused, not answered with nothing — an empty
    /// export is indistinguishable from an empty range.
    #[test]
    fn an_unknown_signal_is_refused_by_the_type() {
        assert!(serde_json::from_str::<Signal>("\"logs\"").is_ok());
        assert!(serde_json::from_str::<Signal>("\"traces\"").is_ok());
        assert!(serde_json::from_str::<Signal>("\"metrics\"").is_ok());
        // Case matters, and so does spelling.
        assert!(serde_json::from_str::<Signal>("\"Logs\"").is_err());
        assert!(serde_json::from_str::<Signal>("\"events\"").is_err());
    }
}
