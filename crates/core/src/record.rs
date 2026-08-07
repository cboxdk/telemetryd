//! The typed in-memory representation of telemetry.
//!
//! Everything past the decode boundary is a real type — [`Labels`], [`LogRecord`],
//! [`Severity`] — not a loose map threaded through the system. Wire formats parse
//! into these on the way in and serialise out of them at the edge, so the shape of a
//! record is checked once rather than assumed at every use.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// A sorted, deduplicated label set. This is the identity of a stream.
///
/// Sorted because two label sets with the same pairs in a different order are the
/// same stream, and that has to be true structurally rather than by convention —
/// otherwise the same logical stream fragments into several, and cardinality caps
/// count the same thing more than once.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Labels(BTreeMap<String, String>);

impl Labels {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.0.insert(name.into(), value.into());
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    pub fn remove(&mut self, name: &str) -> Option<String> {
        self.0.remove(name)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// Stable identity for this label set, used as a series/stream key.
    ///
    /// FNV-1a over the sorted pairs with an explicit separator so `{ab="c"}` and
    /// `{a="bc"}` cannot collide by concatenation.
    pub fn fingerprint(&self) -> u64 {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut hash = OFFSET;
        let mut feed = |bytes: &[u8]| {
            for byte in bytes {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(PRIME);
            }
        };
        for (name, value) in &self.0 {
            feed(name.as_bytes());
            feed(&[0x01]);
            feed(value.as_bytes());
            feed(&[0x02]);
        }
        hash
    }

    /// Render in the `{a="1", b="2"}` form both LogQL and PromQL use.
    pub fn to_selector(&self) -> String {
        let inner: Vec<String> = self
            .0
            .iter()
            .map(|(name, value)| format!("{name}=\"{}\"", escape_label_value(value)))
            .collect();
        format!("{{{}}}", inner.join(", "))
    }
}

impl FromIterator<(String, String)> for Labels {
    fn from_iter<T: IntoIterator<Item = (String, String)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl fmt::Debug for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_selector())
    }
}

fn escape_label_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Coerce an arbitrary attribute key into a valid label name.
///
/// OTLP attribute keys are dotted (`service.name`, `http.status_code`); Loki and
/// Prometheus label names are `[a-zA-Z_][a-zA-Z0-9_]*`. Rewriting rather than
/// rejecting keeps ordinary OTLP data usable, and doing it in exactly one place means
/// ingest and query cannot disagree about what a label ended up called.
pub fn sanitize_label_name(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for (index, ch) in raw.chars().enumerate() {
        let valid = if index == 0 {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_'
        };
        out.push(if valid { ch } else { '_' });
    }
    if out.is_empty() { "_".to_owned() } else { out }
}

/// Normalised severity. OTLP severity numbers are grouped into the levels operators
/// actually filter on, and exposed as the `level` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
    Unknown,
}

impl Severity {
    /// Map an OTLP `severityNumber` per the OpenTelemetry logs data model.
    pub fn from_otlp_number(number: i32) -> Self {
        match number {
            1..=4 => Self::Trace,
            5..=8 => Self::Debug,
            9..=12 => Self::Info,
            13..=16 => Self::Warn,
            17..=20 => Self::Error,
            21..=24 => Self::Fatal,
            _ => Self::Unknown,
        }
    }

    /// Fall back to the free-text `severityText` when no number was supplied.
    ///
    /// Worth doing: plenty of producers send only text, and a log view where
    /// everything is `unknown` is useless for the one filter people always apply.
    pub fn from_text(text: &str) -> Self {
        match text.trim().to_ascii_lowercase().as_str() {
            "trace" | "trc" | "verbose" => Self::Trace,
            "debug" | "dbg" => Self::Debug,
            "info" | "information" | "inf" | "notice" => Self::Info,
            "warn" | "warning" | "wrn" => Self::Warn,
            "error" | "err" | "severe" => Self::Error,
            "fatal" | "critical" | "crit" | "alert" | "emergency" | "panic" => Self::Fatal,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Fatal => "fatal",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One log line, after decoding and limit enforcement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    /// Event time in Unix nanoseconds, matching OTLP.
    pub timestamp_nanos: u64,
    /// The stream this line belongs to: `app`, `level`, and sanitised resource
    /// attributes. Bounded, because this is what the cardinality cap counts.
    pub stream: Labels,
    pub severity: Severity,
    /// The producer's own severity text, preserved verbatim. `level` is the
    /// normalised form; this is what they actually sent.
    pub severity_text: String,
    pub body: String,
    /// Per-record attributes. Not part of the stream identity — putting these in the
    /// stream is how a log store dies of cardinality — but queryable through label
    /// filters.
    pub attributes: Labels,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

impl LogRecord {
    /// The `app` this record belongs to. Always present: the decoder assigns
    /// [`UNKNOWN_APP`] rather than allowing an unattributed record, so retention,
    /// quotas and queries never have to handle a missing tenant.
    pub fn app(&self) -> &str {
        self.stream.get(APP_LABEL).unwrap_or(UNKNOWN_APP)
    }

    /// Approximate heap cost, used to decide when to seal a segment.
    pub fn size_estimate(&self) -> usize {
        let labels = |labels: &Labels| {
            labels
                .iter()
                .map(|(k, v)| k.len() + v.len() + 2)
                .sum::<usize>()
        };
        self.body.len()
            + self.severity_text.len()
            + labels(&self.stream)
            + labels(&self.attributes)
            + self.trace_id.as_ref().map_or(0, String::len)
            + self.span_id.as_ref().map_or(0, String::len)
            + 32
    }
}

/// The label carrying tenancy. Per ADR-004 this is a query namespace, not a security
/// boundary.
pub const APP_LABEL: &str = "app";
/// The normalised severity label.
pub const LEVEL_LABEL: &str = "level";
/// Assigned when a producer sends neither `app` nor `service.name`.
pub const UNKNOWN_APP: &str = "unknown";

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn label_order_does_not_change_identity() {
        let mut a = Labels::new();
        a.insert("app", "checkout");
        a.insert("level", "error");

        let mut b = Labels::new();
        b.insert("level", "error");
        b.insert("app", "checkout");

        assert_eq!(a, b);
        assert_eq!(a.fingerprint(), b.fingerprint());
        assert_eq!(a.to_selector(), r#"{app="checkout", level="error"}"#);
    }

    #[test]
    fn fingerprints_do_not_collide_across_the_pair_boundary() {
        // Without a separator, {ab="c"} and {a="bc"} would hash the same bytes.
        let a: Labels = [("ab".to_owned(), "c".to_owned())].into_iter().collect();
        let b: Labels = [("a".to_owned(), "bc".to_owned())].into_iter().collect();
        assert_ne!(a.fingerprint(), b.fingerprint());
    }

    #[test]
    fn selector_rendering_escapes_quotes_and_backslashes() {
        let labels: Labels = [("path".to_owned(), r#"C:\a"b"#.to_owned())]
            .into_iter()
            .collect();
        assert_eq!(labels.to_selector(), r#"{path="C:\\a\"b"}"#);
    }

    #[test]
    fn otlp_attribute_keys_become_valid_label_names() {
        assert_eq!(sanitize_label_name("service.name"), "service_name");
        assert_eq!(sanitize_label_name("http.status_code"), "http_status_code");
        assert_eq!(sanitize_label_name("k8s.pod/name"), "k8s_pod_name");
        // A leading digit is not valid as the first character.
        assert_eq!(sanitize_label_name("1st"), "_st");
        assert_eq!(sanitize_label_name(""), "_");
        // Already-valid names pass through untouched.
        assert_eq!(sanitize_label_name("app"), "app");
        assert_eq!(sanitize_label_name("_private"), "_private");
    }

    #[test]
    fn otlp_severity_numbers_map_to_levels() {
        assert_eq!(Severity::from_otlp_number(1), Severity::Trace);
        assert_eq!(Severity::from_otlp_number(9), Severity::Info);
        assert_eq!(Severity::from_otlp_number(12), Severity::Info);
        assert_eq!(Severity::from_otlp_number(13), Severity::Warn);
        assert_eq!(Severity::from_otlp_number(17), Severity::Error);
        assert_eq!(Severity::from_otlp_number(24), Severity::Fatal);
        assert_eq!(Severity::from_otlp_number(0), Severity::Unknown);
        assert_eq!(Severity::from_otlp_number(99), Severity::Unknown);
    }

    #[test]
    fn severity_text_is_a_usable_fallback() {
        // Monolog and friends send text, not numbers.
        assert_eq!(Severity::from_text("ERROR"), Severity::Error);
        assert_eq!(Severity::from_text(" warning "), Severity::Warn);
        assert_eq!(Severity::from_text("critical"), Severity::Fatal);
        assert_eq!(Severity::from_text("notice"), Severity::Info);
        assert_eq!(Severity::from_text("nonsense"), Severity::Unknown);
    }

    #[test]
    fn a_record_always_has_an_app() {
        let record = LogRecord {
            timestamp_nanos: 1,
            stream: Labels::new(),
            severity: Severity::Info,
            severity_text: String::new(),
            body: "hello".to_owned(),
            attributes: Labels::new(),
            trace_id: None,
            span_id: None,
        };
        assert_eq!(record.app(), UNKNOWN_APP);
        assert!(record.size_estimate() >= 5);
    }
}
