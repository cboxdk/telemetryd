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
pub struct Labels(std::sync::Arc<BTreeMap<String, String>>);

impl Labels {
    pub fn new() -> Self {
        Self::default()
    }

    /// Copy-on-write, because the map behind a `Labels` is shared.
    ///
    /// A label set is built once at ingest and read forever after, so the clone this can
    /// trigger costs nothing in practice: while a set is being assembled its refcount is
    /// one and `make_mut` hands back the map it already owns. Sharing is what makes the
    /// same stream appearing in a thousand segments cost one map instead of a thousand.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        std::sync::Arc::make_mut(&mut self.0).insert(name.into(), value.into());
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    /// Look up a key, accepting either the original spelling or the sanitised one.
    ///
    /// Attribute keys are stored exactly as the producer sent them —
    /// `exception.type` stays `exception.type` — because they are *data*, and
    /// rewriting them means a trace view shows a key nobody sent. Stream labels are
    /// different: they have to be valid Loki/Prometheus label names, so those are
    /// sanitised at ingest.
    ///
    /// That leaves queries needing to reach an attribute by either spelling —
    /// TraceQL's `span.exception.type` and LogQL's `| exception_type="x"` mean the
    /// same attribute. The exact match is tried first and is the common case; the
    /// scan only runs when it misses, over a set capped by `max_attrs_per_record`.
    pub fn get_relaxed(&self, name: &str) -> Option<&str> {
        if let Some(value) = self.get(name) {
            return Some(value);
        }
        let wanted = sanitize_label_name(name);
        self.0
            .iter()
            .find(|(key, _)| sanitize_label_name(key) == wanted)
            .map(|(_, value)| value.as_str())
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    pub fn remove(&mut self, name: &str) -> Option<String> {
        std::sync::Arc::make_mut(&mut self.0).remove(name)
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

    /// A 64-bit identity for this label set, for the in-memory tables that key series
    /// by it — the cardinality count above all.
    ///
    /// Keyed SipHash, seeded once per process, over the pairs as `str` hashes them — each
    /// followed by a byte UTF-8 never contains, so no pair can run into the next. It was
    /// unkeyed FNV-1a with `0x01`/`0x02` separators, and those bytes are legal inside a
    /// label value: `{a="x\u{2}b\u{1}y"}` and `{a="x", b="y"}` hashed alike on purpose,
    /// and a producer could mint series the cardinality limit counted as one it had
    /// already seen. With the key unknown outside the process, a collision can no longer
    /// be constructed, only met by chance — and every table that must be exact confirms
    /// equality rather than trusting this.
    ///
    /// Never persisted, which is what lets it be seeded: it means nothing to another
    /// process.
    pub fn fingerprint(&self) -> u64 {
        Self::fingerprint_of(self.iter())
    }

    /// [`Self::fingerprint`] of the set these pairs would make, without making it.
    ///
    /// The pairs must come in name order, as a set iterates them. It lets a table be
    /// asked whether it already holds a set before one is allocated to ask with.
    pub fn fingerprint_of<'a>(pairs: impl Iterator<Item = (&'a str, &'a str)>) -> u64 {
        use std::hash::{BuildHasher, Hash, Hasher};
        static SEED: std::sync::OnceLock<std::collections::hash_map::RandomState> =
            std::sync::OnceLock::new();
        let mut hasher = SEED.get_or_init(Default::default).build_hasher();
        for (name, value) in pairs {
            name.hash(&mut hasher);
            value.hash(&mut hasher);
        }
        hasher.finish()
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
        Self(std::sync::Arc::new(iter.into_iter().collect()))
    }
}

impl Labels {
    /// Share another set's map instead of holding a copy of it.
    ///
    /// Every sealed segment carries a dictionary of the streams it holds, and the same
    /// stream appears in every segment covering the time it was written to. Those
    /// dictionaries are identical label sets held once per segment, and that repetition
    /// is what makes resident memory scale with how much is *stored* rather than with how
    /// much is running — the term that took a 7.5 GiB server down.
    ///
    /// Only useful when the two are already equal; the caller establishes that, which is
    /// why this is not `PartialEq`-checked here.
    #[must_use]
    pub fn shared_with(&self) -> Self {
        Self(std::sync::Arc::clone(&self.0))
    }

    /// Whether nothing else holds this set's map: no other `Labels` shares it.
    ///
    /// For a table that shares sets out, to tell which entries only it still holds.
    #[must_use]
    pub fn is_only_holder(&self) -> bool {
        std::sync::Arc::strong_count(&self.0) == 1
    }

    /// Whether two label sets are backed by the same allocation.
    ///
    /// Public because sharing is a memory *contract*, not an implementation detail: the
    /// store depends on a segment dictionary costing a pointer per repeated stream rather
    /// than a map, and a property nothing can assert is a property that quietly stops
    /// holding. Equality is unaffected either way — this only says whether the saving is
    /// actually being made.
    #[must_use]
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }

    /// An identifier for the shared map behind this label set.
    ///
    /// Two `Labels` with the same id are the same allocation and therefore equal; two with
    /// different ids may still be equal, so this identifies, it does not compare. That is
    /// enough to key a cache on: a query evaluator that hands every step the *same*
    /// label set for a series can then look up what it precomputed about that series
    /// without comparing label values at all — which is otherwise a handful of string
    /// comparisons per sample per step, and the hottest thing in a range query.
    ///
    /// Only meaningful while the label set is alive. A cache keyed on it must hold the
    /// `Labels` it came from, or an address could be reused by a later allocation.
    #[must_use]
    pub fn storage_id(&self) -> usize {
        std::sync::Arc::as_ptr(&self.0) as usize
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
        use crate::sizing::{labels_bytes, optional_string_bytes, string_bytes};

        std::mem::size_of::<Self>()
            + string_bytes(&self.body)
            + string_bytes(&self.severity_text)
            + labels_bytes(&self.stream)
            + labels_bytes(&self.attributes)
            + optional_string_bytes(self.trace_id.as_ref())
            + optional_string_bytes(self.span_id.as_ref())
    }
}

/// The label carrying tenancy. It is a query namespace, not a security
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

        // And the separators cannot be forged from inside a value. These two hashed the
        // same bytes when the separators were 0x01 and 0x02.
        let forged: Labels = [("a".to_owned(), "x\u{2}b\u{1}y".to_owned())]
            .into_iter()
            .collect();
        let honest: Labels = [
            ("a".to_owned(), "x".to_owned()),
            ("b".to_owned(), "y".to_owned()),
        ]
        .into_iter()
        .collect();
        assert_ne!(forged.fingerprint(), honest.fingerprint());
        // Stable within the process, which is all anything relies on.
        assert_eq!(honest.fingerprint(), honest.clone().fingerprint());
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
