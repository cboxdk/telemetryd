//! Prometheus `remote_write` ingest.
//!
//! The payload is snappy-compressed protobuf. Both are server-side only, so neither is
//! on the client's critical path — the constraint that keeps OTLP to JSON does not
//! apply here.
//!
//! The schema is small enough to decode by hand ([`crate::protobuf`]):
//!
//! ```proto
//! message WriteRequest { repeated TimeSeries timeseries = 1; }
//! message TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
//! message Label        { string name = 1; string value = 2; }
//! message Sample       { double value = 1; int64 timestamp = 2; }  // millis
//! ```

use telemetryd_core::config::LimitsConfig;
use telemetryd_core::metric::{METRIC_NAME_LABEL, MetricKind, MetricSample, is_valid_metric_name};
use telemetryd_core::record::APP_LABEL;
use telemetryd_core::{Error, Labels, Result};

use crate::protobuf::{Reader, WireType};
use crate::{Decoded, RejectReason, Rejection};

/// Everything a `remote_write` decode needs beyond the payload.
#[derive(Debug, Clone, Copy)]
pub struct WriteContext<'a> {
    pub limits: &'a LimitsConfig,
    /// Used when a series carries no `app` label of its own.
    pub default_app: &'a str,
}

/// Decompress and decode a `remote_write` request.
pub fn decode(compressed: &[u8], ctx: WriteContext<'_>) -> Result<Decoded<MetricSample>> {
    // Prometheus always compresses; a few clients send raw protobuf. Falling back
    // rather than failing costs one failed decompression and saves an afternoon of
    // debugging "400 Bad Request" with no explanation.
    let decompressed = match snap::raw::Decoder::new().decompress_vec(compressed) {
        Ok(bytes) => bytes,
        Err(_) if looks_like_protobuf(compressed) => compressed.to_vec(),
        Err(e) => {
            return Err(Error::BadRequest(format!(
                "remote_write payload is not valid snappy: {e}"
            )));
        }
    };

    let mut decoded = Decoded::default();
    let mut reader = Reader::new(&decompressed);

    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            // WriteRequest.timeseries
            (1, WireType::LengthDelimited) => {
                let series = reader.message()?;
                read_timeseries(series, ctx, &mut decoded)?;
            }
            _ => reader.skip(wire)?,
        }
    }

    Ok(decoded)
}

/// A protobuf message plausibly starts with a valid field key.
fn looks_like_protobuf(bytes: &[u8]) -> bool {
    bytes
        .first()
        .is_some_and(|byte| matches!(byte & 0x07, 0 | 1 | 2 | 5))
}

fn read_timeseries(
    mut reader: Reader<'_>,
    ctx: WriteContext<'_>,
    decoded: &mut Decoded<MetricSample>,
) -> Result<()> {
    let mut labels = Labels::new();
    // Millisecond timestamp and value, in wire order.
    let mut samples: Vec<(i64, f64)> = Vec::new();

    while let Some((field, wire)) = reader.next_field()? {
        match (field, wire) {
            // TimeSeries.labels
            (1, WireType::LengthDelimited) => {
                let mut label = reader.message()?;
                let (mut name, mut value) = (String::new(), String::new());
                while let Some((field, wire)) = label.next_field()? {
                    match (field, wire) {
                        (1, WireType::LengthDelimited) => label.string()?.clone_into(&mut name),
                        (2, WireType::LengthDelimited) => label.string()?.clone_into(&mut value),
                        _ => label.skip(wire)?,
                    }
                }
                if !name.is_empty() {
                    labels.insert(name, value);
                }
            }
            // TimeSeries.samples
            (2, WireType::LengthDelimited) => {
                let mut sample = reader.message()?;
                let (mut value, mut timestamp_millis) = (0.0f64, 0i64);
                while let Some((field, wire)) = sample.next_field()? {
                    match (field, wire) {
                        (1, WireType::Fixed64) => value = sample.double()?,
                        (2, WireType::Varint) => {
                            // int64 on the wire is a plain varint in two's complement,
                            // so the reinterpretation is exactly the decoding, not a
                            // lossy cast.
                            #[allow(clippy::cast_possible_wrap)]
                            {
                                timestamp_millis = sample.varint()? as i64;
                            }
                        }
                        _ => sample.skip(wire)?,
                    }
                }
                samples.push((timestamp_millis, value));
            }
            _ => reader.skip(wire)?,
        }
    }

    if samples.is_empty() {
        return Ok(());
    }

    // Validate the series once, not once per sample: a rejected series rejects all of
    // its samples with the same reason.
    let series = match build_series(labels, ctx) {
        Ok(series) => series,
        Err(rejection) => {
            for _ in &samples {
                decoded.rejections.push(rejection.clone());
            }
            return Ok(());
        }
    };

    for (timestamp_millis, value) in samples {
        let Some(timestamp_nanos) = millis_to_nanos(timestamp_millis) else {
            decoded.rejections.push(Rejection::new(
                RejectReason::InvalidTimestamp,
                format!("sample timestamp {timestamp_millis} is not a plausible time"),
            ));
            continue;
        };

        decoded.records.push(MetricSample {
            timestamp_nanos,
            series: series.clone(),
            value,
            // remote_write carries no type information at all.
            kind: MetricKind::Unknown,
        });
    }

    Ok(())
}

fn build_series(
    mut labels: Labels,
    ctx: WriteContext<'_>,
) -> std::result::Result<Labels, Rejection> {
    let Some(name) = labels.get(METRIC_NAME_LABEL).map(str::to_owned) else {
        return Err(Rejection::new(
            RejectReason::MissingMetricName,
            "time series has no __name__ label".to_owned(),
        ));
    };
    if !is_valid_metric_name(&name) {
        return Err(Rejection::new(
            RejectReason::InvalidMetricName,
            format!("{name:?} is not a valid Prometheus metric name"),
        ));
    }

    // Every series belongs to an app, so retention and quotas never have to handle a
    // missing tenant.
    if !labels.contains_key(APP_LABEL) {
        let app = labels
            .get("service_name")
            .or_else(|| labels.get("job"))
            .unwrap_or(ctx.default_app)
            .to_owned();
        labels.insert(APP_LABEL, app);
    }

    if labels.len() > ctx.limits.max_labels_per_series as usize {
        return Err(Rejection::new(
            RejectReason::TooManyLabels,
            format!(
                "{} labels exceeds max_labels_per_series ({})",
                labels.len(),
                ctx.limits.max_labels_per_series
            ),
        ));
    }
    for (name, value) in labels.iter() {
        if name.len() > ctx.limits.max_label_name_bytes as usize {
            return Err(Rejection::new(
                RejectReason::LabelNameTooLong,
                format!("label name {name:?} exceeds max_label_name_bytes"),
            ));
        }
        if value.len() > ctx.limits.max_label_value_bytes as usize {
            return Err(Rejection::new(
                RejectReason::LabelValueTooLong,
                format!("value of label {name:?} exceeds max_label_value_bytes"),
            ));
        }
    }

    Ok(labels)
}

/// Convert a millisecond timestamp, rejecting values outside 2001–2100.
///
/// `remote_write` timestamps are milliseconds, unambiguously — unlike OTLP there is no
/// unit confusion to recover from, so an implausible value is a real problem rather
/// than a unit mistake and is reported instead of guessed at.
fn millis_to_nanos(millis: i64) -> Option<u64> {
    const MIN_MILLIS: i64 = 978_307_200_000;
    const MAX_MILLIS: i64 = 4_102_444_800_000;

    (MIN_MILLIS..MAX_MILLIS)
        .contains(&millis)
        .then(|| u64::try_from(millis).ok().map(|m| m * 1_000_000))
        .flatten()
}

#[cfg(test)]
// Exact float comparison is deliberate here: these are small values that round-trip
// through f64 without loss, and the assertion is that they arrived unchanged.
#[allow(clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::*;

    const NOW_MILLIS: i64 = 1_750_000_000_000;
    const NOW_NANOS: u64 = 1_750_000_000_000_000_000;

    // -- encoding helpers, so tests build real payloads ---------------------

    fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    fn key(number: u32, wire: u8) -> Vec<u8> {
        varint(u64::from(number) << 3 | u64::from(wire))
    }

    fn delimited(number: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = key(number, 2);
        out.extend(varint(payload.len() as u64));
        out.extend_from_slice(payload);
        out
    }

    fn label(name: &str, value: &str) -> Vec<u8> {
        let mut inner = delimited(1, name.as_bytes());
        inner.extend(delimited(2, value.as_bytes()));
        inner
    }

    fn sample(value: f64, timestamp_millis: i64) -> Vec<u8> {
        let mut inner = key(1, 1);
        inner.extend_from_slice(&value.to_bits().to_le_bytes());
        inner.extend(key(2, 0));
        inner.extend(varint(timestamp_millis.cast_unsigned()));
        inner
    }

    /// Labels and `(value, millis)` samples for one series.
    type SeriesSpec<'a> = (Vec<(&'a str, &'a str)>, Vec<(f64, i64)>);

    fn write_request(series: &[SeriesSpec<'_>]) -> Vec<u8> {
        let mut request = Vec::new();
        for (labels, samples) in series {
            let mut ts = Vec::new();
            for (name, value) in labels {
                ts.extend(delimited(1, &label(name, value)));
            }
            for (value, timestamp) in samples {
                ts.extend(delimited(2, &sample(*value, *timestamp)));
            }
            request.extend(delimited(1, &ts));
        }
        snap::raw::Encoder::new().compress_vec(&request).unwrap()
    }

    fn decode_default(payload: &[u8]) -> Decoded<MetricSample> {
        let limits = LimitsConfig::default();
        decode(
            payload,
            WriteContext {
                limits: &limits,
                default_app: "unknown",
            },
        )
        .unwrap()
    }

    // -- tests --------------------------------------------------------------

    #[test]
    fn decodes_a_realistic_write_request() {
        let payload = write_request(&[(
            vec![
                ("__name__", "http_requests_total"),
                ("app", "checkout"),
                ("status", "500"),
            ],
            vec![(42.0, NOW_MILLIS), (43.0, NOW_MILLIS + 15_000)],
        )]);

        let decoded = decode_default(&payload);
        assert_eq!(decoded.records.len(), 2);
        assert!(decoded.rejections.is_empty());

        let first = &decoded.records[0];
        assert_eq!(first.name(), "http_requests_total");
        assert_eq!(first.app(), "checkout");
        assert_eq!(first.series.get("status"), Some("500"));
        assert_eq!(first.value, 42.0);
        // Milliseconds on the wire, nanoseconds in the store.
        assert_eq!(first.timestamp_nanos, NOW_NANOS);
        assert_eq!(
            decoded.records[1].timestamp_nanos,
            NOW_NANOS + 15_000_000_000
        );
    }

    #[test]
    fn several_series_in_one_request_all_decode() {
        let payload = write_request(&[
            (
                vec![("__name__", "up"), ("app", "checkout")],
                vec![(1.0, NOW_MILLIS)],
            ),
            (
                vec![("__name__", "up"), ("app", "cart")],
                vec![(0.0, NOW_MILLIS)],
            ),
        ]);

        let decoded = decode_default(&payload);
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].app(), "checkout");
        assert_eq!(decoded.records[1].app(), "cart");
    }

    #[test]
    fn an_app_label_is_derived_when_absent() {
        // Retention and quotas key off `app`, so it must never be missing.
        let from_job = write_request(&[(
            vec![("__name__", "up"), ("job", "checkout")],
            vec![(1.0, NOW_MILLIS)],
        )]);
        assert_eq!(decode_default(&from_job).records[0].app(), "checkout");

        let from_nothing = write_request(&[(vec![("__name__", "up")], vec![(1.0, NOW_MILLIS)])]);
        assert_eq!(decode_default(&from_nothing).records[0].app(), "unknown");
    }

    #[test]
    fn a_series_without_a_name_is_rejected_with_its_samples() {
        let payload = write_request(&[(
            vec![("app", "checkout")],
            vec![(1.0, NOW_MILLIS), (2.0, NOW_MILLIS)],
        )]);

        let decoded = decode_default(&payload);
        assert!(decoded.records.is_empty());
        assert_eq!(decoded.rejections.len(), 2, "both samples are refused");
        assert_eq!(
            decoded.rejections[0].reason,
            RejectReason::MissingMetricName
        );
    }

    #[test]
    fn an_invalid_metric_name_is_refused_rather_than_rewritten() {
        // Renaming would mean a dashboard queries a series that does not exist under
        // the name the user wrote.
        let payload = write_request(&[(
            vec![("__name__", "has.dots"), ("app", "x")],
            vec![(1.0, NOW_MILLIS)],
        )]);

        let decoded = decode_default(&payload);
        assert!(decoded.records.is_empty());
        assert_eq!(
            decoded.rejections[0].reason,
            RejectReason::InvalidMetricName
        );
    }

    #[test]
    fn an_implausible_timestamp_is_reported_not_guessed_at() {
        // Unlike OTLP, remote_write timestamps are unambiguously milliseconds, so a
        // wild value is a real problem rather than a unit mistake.
        let payload = write_request(&[(
            vec![("__name__", "up"), ("app", "x")],
            vec![(1.0, 0), (1.0, NOW_MILLIS)],
        )]);

        let decoded = decode_default(&payload);
        assert_eq!(decoded.records.len(), 1, "the good sample still lands");
        assert_eq!(decoded.rejections[0].reason, RejectReason::InvalidTimestamp);
    }

    #[test]
    fn a_series_over_the_label_cap_is_rejected() {
        let limits = LimitsConfig {
            max_labels_per_series: 3,
            ..LimitsConfig::default()
        };
        let payload = write_request(&[(
            vec![
                ("__name__", "up"),
                ("app", "x"),
                ("a", "1"),
                ("b", "2"),
                ("c", "3"),
            ],
            vec![(1.0, NOW_MILLIS)],
        )]);

        let decoded = decode(
            &payload,
            WriteContext {
                limits: &limits,
                default_app: "unknown",
            },
        )
        .unwrap();
        assert!(decoded.records.is_empty());
        assert_eq!(decoded.rejections[0].reason, RejectReason::TooManyLabels);
    }

    #[test]
    fn uncompressed_protobuf_is_accepted_too() {
        // A few clients skip snappy; failing on it produces a 400 with nothing useful
        // in it.
        let mut ts = delimited(1, &label("__name__", "up"));
        ts.extend(delimited(1, &label("app", "checkout")));
        ts.extend(delimited(2, &sample(1.0, NOW_MILLIS)));
        let raw = delimited(1, &ts);

        let decoded = decode_default(&raw);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].name(), "up");
    }

    #[test]
    fn an_empty_request_is_valid_and_yields_nothing() {
        let payload = snap::raw::Encoder::new().compress_vec(&[]).unwrap();
        let decoded = decode_default(&payload);
        assert!(decoded.records.is_empty());
        assert!(decoded.rejections.is_empty());
    }

    #[test]
    fn a_series_with_no_samples_is_silently_skipped() {
        let payload = write_request(&[(vec![("__name__", "up"), ("app", "x")], vec![])]);
        let decoded = decode_default(&payload);
        assert!(decoded.records.is_empty());
        assert!(
            decoded.rejections.is_empty(),
            "nothing was refused, there was nothing to refuse"
        );
    }

    #[test]
    fn garbage_is_a_client_error_not_a_panic() {
        for payload in [
            vec![0xff, 0xff, 0xff],
            vec![0x00],
            b"definitely not protobuf".to_vec(),
            vec![],
        ] {
            let limits = LimitsConfig::default();
            let result = decode(
                &payload,
                WriteContext {
                    limits: &limits,
                    default_app: "unknown",
                },
            );
            if let Err(error) = result {
                assert!(matches!(error, Error::BadRequest(_)), "{error:?}");
            }
        }
    }

    #[test]
    fn special_float_values_survive() {
        let payload = write_request(&[(
            vec![("__name__", "up"), ("app", "x")],
            vec![(f64::NAN, NOW_MILLIS), (f64::INFINITY, NOW_MILLIS + 1000)],
        )]);

        let decoded = decode_default(&payload);
        assert!(
            decoded.records[0].value.is_nan(),
            "NaN is a real Prometheus signal"
        );
        assert!(decoded.records[1].value.is_infinite());
    }
}
