//! Trace spans.
//!
//! Same shape of decision as [`crate::record`]: a real type, not a map. Spans carry
//! more structure than log lines — a parent link, a status, a duration, nested events —
//! and every one of those is something a query needs to reason about.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::record::Labels;

/// OTLP `SpanKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpanKind {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    pub fn from_otlp_number(number: i32) -> Self {
        match number {
            1 => Self::Internal,
            2 => Self::Server,
            3 => Self::Client,
            4 => Self::Producer,
            5 => Self::Consumer,
            _ => Self::Unspecified,
        }
    }

    /// Parse the proto enum name, which OTLP JSON may send instead of a number.
    pub fn from_otlp_name(name: &str) -> Option<Self> {
        match name.trim_start_matches("SPAN_KIND_") {
            "INTERNAL" => Some(Self::Internal),
            "SERVER" => Some(Self::Server),
            "CLIENT" => Some(Self::Client),
            "PRODUCER" => Some(Self::Producer),
            "CONSUMER" => Some(Self::Consumer),
            "UNSPECIFIED" => Some(Self::Unspecified),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Internal => "internal",
            Self::Server => "server",
            Self::Client => "client",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }

    /// The numeric form, for round-tripping back out as OTLP JSON.
    pub fn as_otlp_number(self) -> i32 {
        match self {
            Self::Unspecified => 0,
            Self::Internal => 1,
            Self::Server => 2,
            Self::Client => 3,
            Self::Producer => 4,
            Self::Consumer => 5,
        }
    }
}

impl fmt::Display for SpanKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// OTLP span status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpanStatus {
    /// The default. Notably **not** the same as `Ok`: TraceQL's `status = error`
    /// must not match a span nobody set a status on.
    Unset,
    Ok,
    Error,
}

impl SpanStatus {
    pub fn from_otlp_number(number: i32) -> Self {
        match number {
            1 => Self::Ok,
            2 => Self::Error,
            _ => Self::Unset,
        }
    }

    pub fn from_otlp_name(name: &str) -> Option<Self> {
        match name.trim_start_matches("STATUS_CODE_") {
            "OK" => Some(Self::Ok),
            "ERROR" => Some(Self::Error),
            "UNSET" => Some(Self::Unset),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }

    pub fn as_otlp_number(self) -> i32 {
        match self {
            Self::Unset => 0,
            Self::Ok => 1,
            Self::Error => 2,
        }
    }
}

impl fmt::Display for SpanStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A timestamped event attached to a span.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanEvent {
    pub time_nanos: u64,
    pub name: String,
    pub attributes: Labels,
}

/// One span, after decoding and limit enforcement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpanRecord {
    pub trace_id: String,
    pub span_id: String,
    /// `None` for a root span.
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: SpanKind,
    pub start_nanos: u64,
    pub end_nanos: u64,
    pub status: SpanStatus,
    pub status_message: String,
    /// Resource-derived labels: `app`, `service_name`, and the configured promotions.
    /// This is the stream identity, and what segment pruning indexes.
    pub stream: Labels,
    /// Span attributes. High cardinality by nature (`http.url`, `db.statement`), so
    /// deliberately not part of the stream.
    pub attributes: Labels,
    pub events: Vec<SpanEvent>,
}

impl SpanRecord {
    pub fn app(&self) -> &str {
        self.stream
            .get(crate::record::APP_LABEL)
            .unwrap_or(crate::record::UNKNOWN_APP)
    }

    pub fn service_name(&self) -> &str {
        self.stream
            .get("service_name")
            .unwrap_or_else(|| self.app())
    }

    /// Duration in nanoseconds.
    ///
    /// Saturating: a producer with a skewed clock can send `end` before `start`, and a
    /// negative duration would either panic in debug or wrap to something enormous.
    pub fn duration_nanos(&self) -> u64 {
        self.end_nanos.saturating_sub(self.start_nanos)
    }

    pub fn is_root(&self) -> bool {
        self.parent_span_id.is_none()
    }

    pub fn size_estimate(&self) -> usize {
        use crate::sizing::{labels_bytes, optional_string_bytes, string_bytes, vec_bytes};

        std::mem::size_of::<Self>()
            + string_bytes(&self.trace_id)
            + string_bytes(&self.span_id)
            + optional_string_bytes(self.parent_span_id.as_ref())
            + string_bytes(&self.name)
            + string_bytes(&self.status_message)
            + labels_bytes(&self.stream)
            + labels_bytes(&self.attributes)
            + vec_bytes::<SpanEvent>(self.events.len())
            + self
                .events
                .iter()
                .map(|e| string_bytes(&e.name) + labels_bytes(&e.attributes))
                .sum::<usize>()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn span() -> SpanRecord {
        let mut stream = Labels::new();
        stream.insert("app", "checkout");
        stream.insert("service_name", "checkout");

        SpanRecord {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_owned(),
            span_id: "00f067aa0ba902b7".to_owned(),
            parent_span_id: None,
            name: "POST /checkout".to_owned(),
            kind: SpanKind::Server,
            start_nanos: 1_750_000_000_000_000_000,
            end_nanos: 1_750_000_000_150_000_000,
            status: SpanStatus::Error,
            status_message: "payment declined".to_owned(),
            stream,
            attributes: Labels::new(),
            events: Vec::new(),
        }
    }

    #[test]
    fn duration_is_derived_from_the_endpoints() {
        assert_eq!(span().duration_nanos(), 150_000_000);
    }

    #[test]
    fn a_backwards_clock_does_not_produce_a_huge_duration() {
        // Producers with skewed clocks send end < start; wrapping would report
        // something like 584 years.
        let mut span = span();
        span.end_nanos = span.start_nanos - 1_000;
        assert_eq!(span.duration_nanos(), 0);
    }

    #[test]
    fn span_kinds_map_from_numbers_and_names() {
        assert_eq!(SpanKind::from_otlp_number(2), SpanKind::Server);
        assert_eq!(SpanKind::from_otlp_number(99), SpanKind::Unspecified);
        assert_eq!(
            SpanKind::from_otlp_name("SPAN_KIND_CLIENT"),
            Some(SpanKind::Client)
        );
        assert_eq!(SpanKind::from_otlp_name("CLIENT"), Some(SpanKind::Client));
        assert_eq!(SpanKind::from_otlp_name("NONSENSE"), None);

        // Round-trips, so a trace read back is the trace that went in.
        for kind in [
            SpanKind::Unspecified,
            SpanKind::Internal,
            SpanKind::Server,
            SpanKind::Client,
            SpanKind::Producer,
            SpanKind::Consumer,
        ] {
            assert_eq!(SpanKind::from_otlp_number(kind.as_otlp_number()), kind);
        }
    }

    #[test]
    fn unset_status_is_not_ok_and_not_error() {
        // TraceQL's `status = error` must not match a span nobody set a status on,
        // and `status = ok` must not match one either.
        assert_eq!(SpanStatus::from_otlp_number(0), SpanStatus::Unset);
        assert_eq!(SpanStatus::from_otlp_number(1), SpanStatus::Ok);
        assert_eq!(SpanStatus::from_otlp_number(2), SpanStatus::Error);
        assert_ne!(SpanStatus::Unset, SpanStatus::Ok);

        for status in [SpanStatus::Unset, SpanStatus::Ok, SpanStatus::Error] {
            assert_eq!(
                SpanStatus::from_otlp_number(status.as_otlp_number()),
                status
            );
        }
    }

    #[test]
    fn service_name_falls_back_to_app() {
        let mut span = span();
        span.stream.remove("service_name");
        assert_eq!(span.service_name(), "checkout");
    }

    #[test]
    fn a_span_without_a_parent_is_a_root() {
        assert!(span().is_root());
        let mut child = span();
        child.parent_span_id = Some("aaaa".to_owned());
        assert!(!child.is_root());
    }
}
