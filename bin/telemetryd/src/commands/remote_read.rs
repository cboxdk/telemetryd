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
use serde_json::{Value, json};
use telemetryd_ingest::protobuf::{Reader, WireType};

/// Printed on every run. The instability is the reason this was left unbuilt for a
/// while, and an operator running a migration deserves to know what it rests on.
pub const WARNING: &str = "warning: remote read is not part of Prometheus' stable API \
                           and may change between minor releases of the source. If this \
                           stops working after an upgrade there, have the source send \
                           OTLP instead.";

/// The metric name label, which becomes the OTLP metric's name rather than one of its
/// attributes.
const NAME_LABEL: &str = "__name__";

fn varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = u8::try_from(value & 0x7f).unwrap_or(0);
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn tag(out: &mut Vec<u8>, field: u32, wire: u8) {
    varint(out, u64::from(field) << 3 | u64::from(wire));
}

fn field_string(out: &mut Vec<u8>, field: u32, value: &str) {
    tag(out, field, 2);
    varint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn field_message(out: &mut Vec<u8>, field: u32, body: &[u8]) {
    tag(out, field, 2);
    varint(out, body.len() as u64);
    out.extend_from_slice(body);
}

/// A `ReadRequest` for everything in a range.
///
/// One matcher is required — a query with none is not a query — so `__name__ =~ ".+"`
/// stands for "every series", which is what a migration wants.
fn read_request(start_ms: i64, end_ms: i64) -> Vec<u8> {
    let mut matcher = Vec::new();
    tag(&mut matcher, 1, 0);
    varint(&mut matcher, 2); // RE
    field_string(&mut matcher, 2, NAME_LABEL);
    field_string(&mut matcher, 3, ".+");

    let mut query = Vec::new();
    tag(&mut query, 1, 0);
    varint(&mut query, start_ms.max(0).cast_unsigned());
    tag(&mut query, 2, 0);
    varint(&mut query, end_ms.max(0).cast_unsigned());
    field_message(&mut query, 3, &matcher);

    let mut request = Vec::new();
    field_message(&mut request, 1, &query);
    // SAMPLES only. The streamed-chunks form is XOR-compressed and would mean carrying
    // a Gorilla decoder for a path that already has a stable alternative.
    tag(&mut request, 2, 0);
    varint(&mut request, 0);
    request
}

/// One series: its labels, and its samples as `(timestamp_ms, value)`.
type Series = (Vec<(String, String)>, Vec<(i64, f64)>);

fn parse_response(body: &[u8]) -> anyhow::Result<Vec<Series>> {
    let mut out = Vec::new();
    let mut response = Reader::new(body);
    while let Some((field, wire)) = response.next_field()? {
        if field != 1 || wire != WireType::LengthDelimited {
            response.skip(wire)?;
            continue;
        }
        let mut result = response.message()?;
        while let Some((field, wire)) = result.next_field()? {
            if field != 1 || wire != WireType::LengthDelimited {
                result.skip(wire)?;
                continue;
            }
            out.push(parse_series(&mut result.message()?)?);
        }
    }
    Ok(out)
}

fn parse_series(series: &mut Reader<'_>) -> anyhow::Result<Series> {
    let mut labels = Vec::new();
    let mut samples = Vec::new();
    while let Some((field, wire)) = series.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut label = series.message()?;
                let (mut name, mut value) = (String::new(), String::new());
                while let Some((field, wire)) = label.next_field()? {
                    match (field, wire) {
                        (1, WireType::LengthDelimited) => label.string()?.clone_into(&mut name),
                        (2, WireType::LengthDelimited) => label.string()?.clone_into(&mut value),
                        _ => label.skip(wire)?,
                    }
                }
                labels.push((name, value));
            }
            (2, WireType::LengthDelimited) => {
                let mut sample = series.message()?;
                let (mut value, mut timestamp) = (0.0, 0i64);
                while let Some((field, wire)) = sample.next_field()? {
                    match (field, wire) {
                        // `double value = 1` — a fixed64 holding IEEE-754 bits.
                        (1, WireType::Fixed64) => value = f64::from_bits(sample.fixed64()?),
                        (2, WireType::Varint) => {
                            timestamp = i64::try_from(sample.varint()?).unwrap_or(0);
                        }
                        _ => sample.skip(wire)?,
                    }
                }
                samples.push((timestamp, value));
            }
            _ => series.skip(wire)?,
        }
    }
    Ok((labels, samples))
}

/// Series to an OTLP metrics request.
///
/// Every sample keeps the timestamp it was stored with, which is the whole point of
/// coming through this API rather than a range query.
fn to_otlp(series: &[Series]) -> (Value, u64) {
    let mut metrics = Vec::new();
    let mut count = 0u64;

    for (labels, samples) in series {
        let name = labels
            .iter()
            .find(|(key, _)| key == NAME_LABEL)
            .map_or("", |(_, value)| value.as_str());
        let attributes: Vec<Value> = labels
            .iter()
            .filter(|(key, _)| key != NAME_LABEL)
            .map(|(key, value)| json!({"key": key, "value": {"stringValue": value}}))
            .collect();

        for (timestamp_ms, value) in samples {
            count += 1;
            metrics.push(json!({
                "name": name,
                "gauge": {"dataPoints": [{
                    // Milliseconds on the wire, nanoseconds in OTLP.
                    "timeUnixNano": (i128::from(*timestamp_ms) * 1_000_000).to_string(),
                    "asDouble": value,
                    "attributes": attributes,
                }]},
            }));
        }
    }

    (
        json!({"resourceMetrics": [{"resource": {"attributes": []},
                                    "scopeMetrics": [{"metrics": metrics}]}]}),
        count,
    )
}

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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_read_request_round_trips_through_our_own_reader() {
        // The encoder is hand-written, so the test is that our decoder — which reads
        // real `remote_write` traffic — can make sense of what it produced.
        let encoded = read_request(1_000, 2_000);
        let mut reader = Reader::new(&encoded);

        let mut saw_query = false;
        while let Some((field, wire)) = reader.next_field().unwrap() {
            if field == 1 {
                let mut query = reader.message().unwrap();
                let mut start = 0;
                let mut end = 0;
                let mut matcher_name = String::new();
                while let Some((field, wire)) = query.next_field().unwrap() {
                    match (field, wire) {
                        (1, WireType::Varint) => start = query.varint().unwrap(),
                        (2, WireType::Varint) => end = query.varint().unwrap(),
                        (3, WireType::LengthDelimited) => {
                            let mut matcher = query.message().unwrap();
                            while let Some((field, wire)) = matcher.next_field().unwrap() {
                                if field == 2 {
                                    matcher_name = matcher.string().unwrap().to_owned();
                                } else {
                                    matcher.skip(wire).unwrap();
                                }
                            }
                        }
                        _ => query.skip(wire).unwrap(),
                    }
                }
                assert_eq!((start, end), (1_000, 2_000));
                assert_eq!(matcher_name, "__name__");
                saw_query = true;
            } else {
                reader.skip(wire).unwrap();
            }
        }
        assert!(saw_query, "the request carried no query");
    }

    #[test]
    fn a_sample_keeps_the_timestamp_it_was_stored_with() {
        // The entire reason for using this API instead of a range query.
        let series = vec![(
            vec![
                (NAME_LABEL.to_owned(), "http_requests_total".to_owned()),
                ("app".to_owned(), "checkout".to_owned()),
            ],
            vec![(1_760_000_000_123, 42.0), (1_760_000_001_456, 43.0)],
        )];
        let (batch, count) = to_otlp(&series);
        assert_eq!(count, 2);

        let points = &batch["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
        assert_eq!(points[0]["name"], "http_requests_total");
        // Milliseconds in, nanoseconds out, and not a step boundary in sight.
        assert_eq!(
            points[0]["gauge"]["dataPoints"][0]["timeUnixNano"],
            "1760000000123000000"
        );
        assert_eq!(
            points[1]["gauge"]["dataPoints"][0]["timeUnixNano"],
            "1760000001456000000"
        );

        // `__name__` is the metric's name, never one of its attributes.
        let attributes = points[0]["gauge"]["dataPoints"][0]["attributes"]
            .as_array()
            .unwrap();
        assert_eq!(attributes.len(), 1);
        assert_eq!(attributes[0]["key"], "app");
    }

    #[test]
    fn an_empty_response_is_none_rather_than_an_empty_batch() {
        // How the walk terminates: no samples means the range is exhausted.
        let (_, count) = to_otlp(&[]);
        assert_eq!(count, 0);
    }
}
