//! Pulling raw metric samples out of a Prometheus-compatible backend.
//!
//! # Why not a range query
//!
//! `step` is a *resolution*. Timestamps come back on the step grid rather than at the
//! times the samples were stored, so a finer step yields more points from the same
//! underlying samples — repetition and shifted timestamps dressed up as fidelity.
//! Shrinking the step and paginating is the obvious idea and it makes the result worse.
//!
//! Remote read returns, in the specification's own words, "a list of raw samples
//! matching the requested query": original timestamps, no evaluation.
//!
//! # What it costs
//!
//! Prometheus documents remote read as **not part of the stable API**, "subject to
//! change even between non-major version releases". A migration path is exactly the
//! thing that runs rarely and under pressure, so depending on an interface that may move
//! between patch releases of the source is a real risk rather than a theoretical one.
//! Every run says so out loud; see [`WARNING`].
//!
//! The protobuf is written and read by hand, as `remote_write` already is on the ingest
//! side. Field numbers are taken from `prompb/remote.proto` and `prompb/types.proto`
//! rather than from memory:
//!
//! - `ReadRequest`: `queries = 1`, `accepted_response_types = 2`
//! - `Query`: `start_timestamp_ms = 1`, `end_timestamp_ms = 2`, `matchers = 3`
//! - `LabelMatcher`: `type = 1` (EQ 0, NEQ 1, RE 2, NRE 3), `name = 2`, `value = 3`
//! - `ReadResponse`: `results = 1`; `QueryResult`: `timeseries = 1`
//! - `TimeSeries`: `labels = 1`, `samples = 2`
//! - `Label`: `name = 1`, `value = 2`; `Sample`: `value = 1` (double), `timestamp = 2`

use anyhow::{Context, bail};
use serde_json::Value;
use telemetryd_ingest::remote_read::{parse_response, read_request, to_otlp};

/// Printed on every run. The instability is the reason this was left unbuilt for a
/// while, and an operator running a migration deserves to know what it rests on.
pub const WARNING: &str = "warning: remote read is not part of Prometheus' stable API \
                           and may change between minor releases of the source. If this \
                           stops working after an upgrade there, have the source send \
                           OTLP instead.";

/// Fetch one range and return it as an OTLP request, plus the sample count and the
/// oldest timestamp seen.
pub fn fetch(
    base: &str,
    token: Option<&str>,
    start_ms: i64,
    end_ms: i64,
) -> anyhow::Result<Option<(Value, u64, i64)>> {
    let url = format!("{}/api/v1/read", base.trim_end_matches('/'));
    let payload = snap::raw::Encoder::new()
        .compress_vec(&read_request(start_ms, end_ms))
        .context("compressing the read request")?;

    // Headers first, then `config`, matching the other clients in this crate.
    //
    // A note against a wrong conclusion: while getting this working, every request
    // failed with "Can't assign requested address" and the builder order looked like
    // the cause. It was not — the walk was looping and had exhausted the machine's
    // ephemeral ports. The order is kept for consistency, not as a fix.
    let mut request = ureq::post(&url)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-read-version", "0.1.0");
    if let Some(token) = token {
        request = request.header("authorization", &format!("Bearer {token}"));
    }

    let mut response = request
        .config()
        .http_status_as_error(false)
        .build()
        .send(&payload[..])
        .with_context(|| format!("could not reach {url}"))?;
    let status = response.status().as_u16();
    let body = response.body_mut().read_to_vec()?;
    if status != 200 {
        let detail: String = String::from_utf8_lossy(&body).chars().take(400).collect();
        bail!("{url} answered {status}: {detail}");
    }

    let decoded = snap::raw::Decoder::new()
        .decompress_vec(&body)
        .context("the response was not valid snappy")?;
    let series = parse_response(&decoded)?;

    let oldest = series
        .iter()
        .flat_map(|(_, samples)| samples.iter().map(|(timestamp, _)| *timestamp))
        .min();
    let (batch, count) = to_otlp(&series);
    if count == 0 {
        return Ok(None);
    }
    Ok(Some((batch, count, oldest.unwrap_or(start_ms))))
}
