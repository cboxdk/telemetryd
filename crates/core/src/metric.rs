//! Metric samples.
//!
//! A sample is `(series labels, timestamp, value)`. The series is a [`Labels`] set
//! including the metric name under [`METRIC_NAME_LABEL`], which is how Prometheus
//! models it — `rate(http_requests_total{app="checkout"}[5m])` is a matcher on
//! `__name__` plus a matcher on `app`, and treating the name as just another label
//! keeps selector handling in one place.

use serde::{Deserialize, Serialize};

use crate::record::Labels;

/// The label carrying the metric name, as in Prometheus.
pub const METRIC_NAME_LABEL: &str = "__name__";

/// What kind of series this is.
///
/// Kept because it changes how a value should be *read*, not how it is stored: a
/// counter that resets to zero means a restart, where the same drop in a gauge is just
/// a smaller number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricKind {
    Gauge,
    Counter,
    /// A cumulative histogram bucket, `_bucket` with an `le` label.
    Histogram,
    Summary,
    /// The producer did not say. Prometheus `remote_write` carries no type at all.
    Unknown,
}

impl MetricKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gauge => "gauge",
            Self::Counter => "counter",
            Self::Histogram => "histogram",
            Self::Summary => "summary",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_str_lossy(raw: &str) -> Self {
        match raw {
            "gauge" => Self::Gauge,
            "counter" => Self::Counter,
            "histogram" => Self::Histogram,
            "summary" => Self::Summary,
            _ => Self::Unknown,
        }
    }
}

/// One sample of one series.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    pub timestamp_nanos: u64,
    /// The full series identity, including `__name__`.
    pub series: Labels,
    pub value: f64,
    pub kind: MetricKind,
}

impl MetricSample {
    pub fn name(&self) -> &str {
        self.series.get(METRIC_NAME_LABEL).unwrap_or("")
    }

    pub fn app(&self) -> &str {
        self.series
            .get(crate::record::APP_LABEL)
            .unwrap_or(crate::record::UNKNOWN_APP)
    }

    /// Approximate heap cost. Small and near-constant, which is the point: the series
    /// labels are interned per segment, so the per-sample cost on disk is a `u32`, a
    /// timestamp and a float.
    pub fn size_estimate(&self) -> usize {
        std::mem::size_of::<Self>() + crate::sizing::labels_bytes(&self.series)
    }
}

/// Whether a metric name is a valid Prometheus name.
///
/// Checked rather than rewritten. A metric name is part of the query a user writes, so
/// silently renaming `foo.bar` to `foo_bar` would mean their dashboard queries a series
/// that does not exist under the name they gave it. OTLP names *are* rewritten, because
/// there the dotted form is the convention and the mapping is well known — but that
/// happens once, explicitly, at the OTLP boundary.
pub fn is_valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn sample() -> MetricSample {
        let mut series = Labels::new();
        series.insert(METRIC_NAME_LABEL, "http_requests_total");
        series.insert("app", "checkout");
        series.insert("status", "500");

        MetricSample {
            timestamp_nanos: 1_750_000_000_000_000_000,
            series,
            value: 42.0,
            kind: MetricKind::Counter,
        }
    }

    #[test]
    fn the_metric_name_is_a_label() {
        // Which is what lets a selector be handled exactly like any other matcher.
        assert_eq!(sample().name(), "http_requests_total");
        assert_eq!(sample().series.get("__name__"), Some("http_requests_total"));
    }

    #[test]
    fn a_sample_without_a_name_does_not_panic() {
        let orphan = MetricSample {
            series: Labels::new(),
            ..sample()
        };
        assert_eq!(orphan.name(), "");
        assert_eq!(orphan.app(), "unknown");
    }

    #[test]
    fn metric_names_are_validated_not_rewritten() {
        // Rewriting would mean a dashboard queries a name the user never wrote.
        assert!(is_valid_metric_name("http_requests_total"));
        assert!(is_valid_metric_name("_internal"));
        assert!(is_valid_metric_name("job:rate:5m"));

        assert!(!is_valid_metric_name(""));
        assert!(!is_valid_metric_name("1st_metric"));
        assert!(!is_valid_metric_name("has.dots"));
        assert!(!is_valid_metric_name("has-dashes"));
        assert!(!is_valid_metric_name("has spaces"));
    }

    #[test]
    fn kinds_round_trip_through_their_string_form() {
        for kind in [
            MetricKind::Gauge,
            MetricKind::Counter,
            MetricKind::Histogram,
            MetricKind::Summary,
            MetricKind::Unknown,
        ] {
            assert_eq!(MetricKind::from_str_lossy(kind.as_str()), kind);
        }
        // remote_write carries no type, so anything unrecognised is Unknown.
        assert_eq!(MetricKind::from_str_lossy("nonsense"), MetricKind::Unknown);
    }
}
