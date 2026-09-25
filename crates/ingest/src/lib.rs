//! Ingest decoders.
//!
//! Turns wire formats into the typed records in `telemetryd-core`, applies the
//! configured limits, and reports exactly what it rejected and why.
//!
//! # Rejections are never silent
//!
//! Every rejected record carries a [`RejectReason`], which becomes the `reason` label
//! on `telemetryd_ingest_rejected_total` and is summarised back to the client through
//! OTLP's own `partialSuccess` field. A caller that sends 500 lines and gets 499
//! stored is told so in the response rather than discovering it in a dashboard later.
//!
//! JSON is the first-class OTLP encoding because that is what `cboxdk/laravel-telemetry`
//! emits — no protobuf, no C extension on the client path.
//!
//! [`compression`] sits in front of all of it: bodies arrive compressed more often than
//! not, and undoing that is the first thing done with untrusted bytes on the write path.

pub mod compression;
pub mod logs;
pub mod otlp;
pub mod otlp_encode;
pub mod otlp_metrics;
pub mod otlp_protobuf;
pub mod protobuf;
pub mod remote_read;
pub mod remote_write;
pub mod traces;

/// Why a record was refused. The string form is the metric label, so it is a closed
/// set rather than free text — an operator can alert on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RejectReason {
    BodyTooLarge,
    TooManyAttributes,
    TooManyLabels,
    LabelNameTooLong,
    LabelValueTooLong,
    /// A span with no usable trace id cannot be joined to anything.
    MissingTraceId,
    /// Likewise with no span id: nothing could ever reference it.
    MissingSpanId,
    /// A time series with no `__name__`.
    MissingMetricName,
    /// A metric name that is not a valid Prometheus name. Refused rather than
    /// rewritten — see `telemetryd_core::metric::is_valid_metric_name`.
    InvalidMetricName,
    /// A sample timestamp outside any plausible range.
    InvalidTimestamp,
    /// Storing it would create a new series past the configured cardinality cap.
    ///
    /// Unlike the others this is decided by the *store*, after decoding, because the
    /// series is not known until then.
    SeriesLimit,
    /// A delta-temporality sum or histogram. Stored as if cumulative it would read as a
    /// counter resetting at every point, and `rate` would be wrong by the width of the
    /// window, so it is refused where the sender can see it.
    DeltaTemporality,
    /// A summary or exponential histogram, which telemetryd does not store. It used to be
    /// dropped with a 200 and nothing in `partialSuccess`.
    UnsupportedMetricType,
}

impl RejectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BodyTooLarge => "body_too_large",
            Self::TooManyAttributes => "too_many_attributes",
            Self::TooManyLabels => "too_many_labels",
            Self::LabelNameTooLong => "label_name_too_long",
            Self::LabelValueTooLong => "label_value_too_long",
            Self::MissingTraceId => "missing_trace_id",
            Self::MissingSpanId => "missing_span_id",
            Self::MissingMetricName => "missing_metric_name",
            Self::InvalidMetricName => "invalid_metric_name",
            Self::InvalidTimestamp => "invalid_timestamp",
            Self::SeriesLimit => "series_limit",
            Self::DeltaTemporality => "delta_temporality",
            Self::UnsupportedMetricType => "unsupported_metric_type",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Rejection {
    pub reason: RejectReason,
    /// Human-readable specifics, surfaced in the `partialSuccess` error message.
    pub detail: String,
}

impl Rejection {
    pub fn new(reason: RejectReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }
}

/// The result of decoding one request.
#[derive(Debug)]
pub struct Decoded<T> {
    pub records: Vec<T>,
    pub rejections: Vec<Rejection>,
    /// Records whose timestamp was in the wrong unit and was corrected. Counted so a
    /// producer bug stays visible rather than being papered over.
    pub rescaled_timestamps: u64,
    /// Bodies that exceeded `max_log_line_bytes` and were truncated rather than
    /// dropped.
    pub truncated_bodies: u64,
    /// What this request may still expand to, from `limits.max_decoded_bytes`.
    budget: usize,
    /// What it has expanded to so far.
    used: usize,
}

impl<T> Default for Decoded<T> {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            rejections: Vec::new(),
            rescaled_timestamps: 0,
            truncated_bodies: 0,
            budget: usize::MAX,
            used: 0,
        }
    }
}

/// Refuse a JSON body that would parse into far more than it is.
///
/// `{},` is three bytes and the log record it parses into is nearly three hundred, so a
/// body of empty objects expands a hundredfold before any limit on records can see it.
/// Real OTLP JSON spends twenty bytes or more per object — keys, quotes, values — so
/// allowing one object per eight bytes admits every real payload and refuses the floods.
/// Counted in one pass over the bytes, before anything is allocated.
///
/// # Errors
/// A JSON error naming the count, reported by the caller as an undecodable payload.
pub fn json_objects_within(body: &[u8]) -> Result<(), serde_json::Error> {
    let allowance = (body.len() / 8).clamp(10_000, 900_000);
    let mut objects = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for &byte in body {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => {
                objects += 1;
                if objects > allowance {
                    return Err(<serde_json::Error as serde::de::Error>::custom(format!(
                        "this {}-byte body holds more than {allowance} JSON objects, which \
                         would parse into far more memory than it arrived as; split the batch",
                        body.len()
                    )));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// What a decoded record costs to hold, for [`Decoded::keep`].
pub trait DecodedBytes {
    fn decoded_bytes(&self) -> usize;
}

impl DecodedBytes for telemetryd_core::LogRecord {
    fn decoded_bytes(&self) -> usize {
        self.size_estimate()
    }
}

impl DecodedBytes for telemetryd_core::SpanRecord {
    fn decoded_bytes(&self) -> usize {
        self.size_estimate()
    }
}

impl DecodedBytes for telemetryd_core::MetricSample {
    fn decoded_bytes(&self) -> usize {
        self.size_estimate()
    }
}

impl<T> Decoded<T> {
    /// An empty result that will hold at most `limits.max_decoded_bytes`.
    ///
    /// The body limit bounds what arrives, not what it becomes: resource attributes are
    /// copied into every record they describe, and a histogram bucket carries its own copy
    /// of the series' labels. Measured before this existed, 73 KB of JSON decoded to 138 MB
    /// and 108 KB to 216 MB. Charging as records are produced keeps what is held bounded
    /// even when the request is not.
    #[must_use]
    pub fn bounded(limits: &telemetryd_core::config::LimitsConfig) -> Self {
        Self {
            budget: usize::try_from(limits.max_decoded_bytes.as_u64()).unwrap_or(usize::MAX),
            ..Self::default()
        }
    }

    /// Keep a decoded record, unless the request has outgrown its budget.
    ///
    /// Past the budget records are dropped as they are produced rather than collected and
    /// counted afterwards — collecting them is the allocation the budget exists to stop.
    /// The caller then refuses the whole request; see [`Self::over_budget`].
    pub fn keep(&mut self, record: T)
    where
        T: DecodedBytes,
    {
        if self.charge(record.decoded_bytes()) {
            self.records.push(record);
        }
    }

    /// Record a rejection, charged like a record: a rejection's detail can name the
    /// label that caused it, and one per sample was the costliest shape of all.
    pub fn refuse(&mut self, rejection: Rejection) {
        let size = std::mem::size_of::<Rejection>() + rejection.detail.len();
        if self.charge(size) {
            self.rejections.push(rejection);
        }
    }

    fn charge(&mut self, bytes: usize) -> bool {
        self.used = self.used.saturating_add(bytes);
        self.used <= self.budget
    }

    /// Whether the request decoded to more than it may hold.
    ///
    /// What was kept is then incomplete, so it must not be stored: refusing the request as
    /// a whole is the one answer a client can act on, where storing part of it would be a
    /// silent loss.
    #[must_use]
    pub fn over_budget(&self) -> bool {
        self.used > self.budget
    }

    /// What the request expanded to, for the message that refuses it.
    #[must_use]
    pub fn decoded_bytes(&self) -> usize {
        self.used
    }

    pub fn accepted(&self) -> usize {
        self.records.len()
    }

    pub fn rejected(&self) -> usize {
        self.rejections.len()
    }

    /// Fold in records the *store* refused, after decoding succeeded.
    ///
    /// Cardinality is only knowable once the series is known, which is after decode,
    /// so these arrive separately. They belong in the same `partialSuccess` all the
    /// same: from the producer's side "you sent 500 and I kept 50" is one fact, not
    /// two, and splitting it across a response and a log file is how it gets missed.
    pub fn note_series_rejections(&mut self, rejected: usize, limit: Option<&str>) {
        let Some(limit) = limit else { return };
        for _ in 0..rejected {
            self.rejections.push(Rejection {
                reason: RejectReason::SeriesLimit,
                detail: format!(
                    "would create a new series past {limit}; raise the limit or send \
                     fewer distinct label combinations"
                ),
            });
        }
    }

    /// One-line summary for OTLP `partialSuccess.errorMessage`.
    ///
    /// Names the distinct reasons and gives one concrete example, which is what makes
    /// a partial success actionable instead of just alarming.
    pub fn rejection_summary(&self) -> Option<String> {
        let first = self.rejections.first()?;
        let mut reasons: Vec<&str> = self
            .rejections
            .iter()
            .map(|r| r.reason.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        reasons.sort_unstable();

        Some(format!(
            "{} record(s) rejected ({}); for example: {}",
            self.rejections.len(),
            reasons.join(", "),
            first.detail
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn reject_reasons_are_a_closed_label_set() {
        for reason in [
            RejectReason::BodyTooLarge,
            RejectReason::TooManyAttributes,
            RejectReason::TooManyLabels,
            RejectReason::LabelNameTooLong,
            RejectReason::LabelValueTooLong,
            RejectReason::MissingTraceId,
            RejectReason::MissingSpanId,
            RejectReason::MissingMetricName,
            RejectReason::InvalidMetricName,
            RejectReason::InvalidTimestamp,
        ] {
            let label = reason.as_str();
            assert!(!label.is_empty());
            assert!(
                label.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
                "{label} is not a usable metric label value"
            );
        }
    }

    #[test]
    fn a_clean_decode_has_no_summary() {
        let decoded: Decoded<()> = Decoded::default();
        assert_eq!(decoded.rejection_summary(), None);
        assert_eq!(decoded.accepted(), 0);
    }

    #[test]
    fn the_summary_names_the_reasons_and_gives_a_concrete_example() {
        let decoded = Decoded::<()> {
            rejections: vec![
                Rejection::new(RejectReason::BodyTooLarge, "log body of 900000 bytes"),
                Rejection::new(RejectReason::TooManyLabels, "61 stream labels"),
                Rejection::new(RejectReason::BodyTooLarge, "log body of 800000 bytes"),
            ],
            ..Decoded::default()
        };

        let summary = decoded.rejection_summary().unwrap();
        assert!(summary.contains("3 record(s) rejected"), "{summary}");
        assert!(summary.contains("body_too_large"), "{summary}");
        assert!(summary.contains("too_many_labels"), "{summary}");
        assert!(
            summary.contains("900000"),
            "should include a concrete example: {summary}"
        );
    }

    /// A body of empty objects parses a hundredfold larger than it is; a real payload of
    /// the same size, and braces inside strings, must still pass.
    #[test]
    fn a_json_flood_is_counted_before_it_is_parsed() {
        let flood = format!(r#"{{"resourceLogs":[{}]}}"#, vec!["{}"; 200_000].join(","));
        assert!(crate::json_objects_within(flood.as_bytes()).is_err());

        let record = r#"{"timeUnixNano":"1700000000000000000","body":{"stringValue":"GET /api {id} took 12ms"},"attributes":[{"key":"http.route","value":{"stringValue":"/api/{id}"}}]}"#;
        let real = format!(
            r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[{}]}}]}}]}}"#,
            vec![record; 20_000].join(",")
        );
        assert!(crate::json_objects_within(real.as_bytes()).is_ok());

        let braces = format!(r#"{{"body":"{}"}}"#, "{".repeat(100_000));
        assert!(
            crate::json_objects_within(braces.as_bytes()).is_ok(),
            "braces in a string are not objects"
        );
    }
}
