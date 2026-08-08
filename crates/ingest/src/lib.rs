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
pub mod otlp_metrics;
pub mod protobuf;
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
}

impl<T> Default for Decoded<T> {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            rejections: Vec::new(),
            rescaled_timestamps: 0,
            truncated_bodies: 0,
        }
    }
}

impl<T> Decoded<T> {
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
}
