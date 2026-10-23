//! Spans in `otel_traces`.
//!
//! # Exactly what the local store would return
//!
//! A span's attributes hold its own, the scope attributes no stream label took, and
//! every resource attribute under `resource.`. Written, the `resource.` ones are
//! `ResourceAttributes` without the prefix and the rest are `SpanAttributes` — the
//! Collector's columns, which other readers expect — and read back, they are put
//! together again as they were. `Stream` is the stream ingest computed, as for logs.
//!
//! Kind and status are stored as the store package and the Collector spell them
//! (`Server`, `Error`), so all three read one table the same way.
//!
//! A span whose producer's clock put its end before its start is stored with a duration
//! of zero, which is what its duration already reads as; it comes back ending where it
//! started.
//!
//! # What runs where
//!
//! TraceQL conditions become conditions ClickHouse selects by — each one necessary for
//! the span to match, never sufficient — and the query's own predicate decides every
//! span that comes back. See [`condition`].

use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

use serde::{Deserialize, Serialize};
use telemetryd_core::record::sanitize_label_name;
use telemetryd_core::span::{
    RESOURCE_ATTRIBUTE_PREFIX, SpanEvent, SpanKind, SpanLink, SpanRecord, SpanStatus,
};
use telemetryd_core::{APP_LABEL, Error, Labels, Result, UNKNOWN_APP};

use super::Client;
use super::scan::{Selected, Table, Timed, labels_of};
use super::schema::{TRACE_IDS, TRACE_STREAMS, TRACES};
use super::sql;
use crate::backend::{RecordFilter, SpanCondition, SpanField, SpanOp, SpanStore, SpanValue};
use crate::records::Scan;
use crate::topk::Order;

const SELECT: &str = "toUnixTimestamp64Nano(Timestamp) AS t, TraceId, SpanId, ParentSpanId, \
    SpanName, SpanKind, ServiceName, ResourceAttributes, SpanAttributes, Duration, StatusCode, \
    StatusMessage, arrayMap(x -> toUnixTimestamp64Nano(x), `Events.Timestamp`) AS EventTimes, \
    `Events.Name` AS EventNames, `Events.Attributes` AS EventAttributes, \
    `Links.TraceId` AS LinkTraceIds, `Links.SpanId` AS LinkSpanIds, \
    `Links.TraceState` AS LinkTraceStates, `Links.Attributes` AS LinkAttributes, Stream";

/// The span table, behind the same interface as the local store.
#[derive(Debug, Clone)]
pub struct ClickHouseSpans {
    client: Client,
}

impl ClickHouseSpans {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Write spans. Stored, and readable by every instance, once this returns.
    ///
    /// # Errors
    /// [`Error::Unavailable`] when ClickHouse did not take them.
    pub fn append(&self, spans: &[SpanRecord]) -> Result<()> {
        if spans.is_empty() {
            return Ok(());
        }
        let mut body = Vec::with_capacity(spans.len() * 1024);
        for span in spans {
            serde_json::to_writer(&mut body, &WriteSpan::from(span))
                .map_err(|e| Error::Unavailable(format!("encoding a span row: {e}")))?;
            body.push(b'\n');
        }
        self.client.insert(TRACES, &body)?;
        self.client.stats().count_rows(spans.len());
        Ok(())
    }

    fn table(&self) -> Table<'_> {
        Table {
            client: &self.client,
            name: TRACES,
            streams: TRACE_STREAMS,
            columns: SELECT,
        }
    }

    /// The streams with spans in the range that the conditions leave possible.
    ///
    /// A `resource.x = "v"` or `=~` condition decides a stream that has the label `x`
    /// sanitises to, since the span reads its resource attribute from the stream first;
    /// a stream without the label is kept, because the span may still have the attribute.
    fn select(
        &self,
        start: u64,
        end: u64,
        conditions: &[SpanCondition],
    ) -> Result<Option<Selected>> {
        let Some(mut selected) = self.table().select(start, end, &[])? else {
            return Ok(None);
        };
        let deciding: Vec<(String, StreamTest)> = conditions
            .iter()
            .filter_map(|c| match (&c.field, c.op, &c.value) {
                (SpanField::Resource(name), SpanOp::Equal, SpanValue::Text(text)) => {
                    Some((sanitize_label_name(name), StreamTest::Equal(text.clone())))
                }
                (SpanField::Resource(name), SpanOp::Regex, SpanValue::Text(pattern)) => {
                    telemetryd_core::matcher::compile_regex(&format!("^(?s:{pattern})$"))
                        .ok()
                        .map(|regex| (sanitize_label_name(name), StreamTest::Regex(regex)))
                }
                _ => None,
            })
            .collect();
        if !deciding.is_empty() {
            selected.streams.retain(|_, stream| {
                deciding
                    .iter()
                    .all(|(label, test)| match stream.get(label) {
                        Some(value) => test.matches(value),
                        None => true,
                    })
            });
            let apps: BTreeSet<String> = selected
                .streams
                .values()
                .map(|s| service_of(s).to_owned())
                .collect();
            selected.services.retain(|s| apps.contains(s));
        }
        Ok(Some(selected))
    }
}

enum StreamTest {
    Equal(String),
    Regex(regex::Regex),
}

impl StreamTest {
    fn matches(&self, value: &str) -> bool {
        match self {
            Self::Equal(text) => value == text,
            Self::Regex(regex) => regex.is_match(value),
        }
    }
}

fn service_of(stream: &Labels) -> &str {
    stream.get(APP_LABEL).unwrap_or(UNKNOWN_APP)
}

impl SpanStore for ClickHouseSpans {
    fn scan(
        &self,
        request: Scan<'_>,
        conditions: &[SpanCondition],
        extra: RecordFilter<'_, SpanRecord>,
    ) -> Result<Vec<SpanRecord>> {
        let selected = self.select(request.start_nanos, request.end_nanos, conditions)?;
        let mut base: Vec<String> = vec![
            selected
                .as_ref()
                .map_or_else(|| "1".to_owned(), Selected::condition),
        ];
        base.extend(conditions.iter().filter_map(condition));
        if let Some(key) = request.exact_key {
            base.push(format!("TraceId = {}", sql::quote(key)));
        }
        let (start, end) = (request.start_nanos, request.end_nanos);
        self.table().scan(
            &request,
            &base.join(" AND "),
            selected.as_ref(),
            &Row::into_span,
            &|span: &SpanRecord| {
                span.start_nanos >= start && span.start_nanos <= end && extra(span)
            },
        )
    }

    fn trace(&self, trace_id: &str) -> Result<Vec<SpanRecord>> {
        // The trace-id index first, the Collector's way: it is sorted by id, so finding
        // when a trace happened is a lookup, and the span table is then read for that
        // stretch only rather than probed by Bloom filter across every part it has.
        let mut bounds = None;
        self.client.query_lines(
            &format!(
                "SELECT toUnixTimestamp64Nano(min(Start)) AS low, \
                 toUnixTimestamp64Nano(max(End)) AS high, count() AS n \
                 FROM {TRACE_IDS} WHERE TraceId = {}",
                sql::quote(trace_id)
            ),
            |line| {
                let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
                let number = |name: &str| value.get(name).and_then(serde_json::Value::as_u64);
                if number("n").unwrap_or(0) > 0 {
                    bounds = number("low").zip(number("high"));
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        let Some((low, high)) = bounds else {
            return Ok(Vec::new());
        };
        let mut spans = Vec::new();
        self.table().read(
            &format!(
                "{} AND TraceId = {}",
                super::scan::time_condition((low, true), (high, true)),
                sql::quote(trace_id)
            ),
            Order::Ascending,
            None,
            |row: Row| {
                spans.push(row.into_span());
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(spans)
    }

    fn of_traces(&self, trace_ids: &[&str], request: Scan<'_>) -> Result<Vec<SpanRecord>> {
        if trace_ids.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: std::collections::HashSet<&str> = trace_ids.iter().copied().collect();
        let ids: Vec<String> = trace_ids.iter().map(|id| sql::quote(id)).collect();
        let base = format!("TraceId IN ({})", ids.join(", "));
        let (start, end) = (request.start_nanos, request.end_nanos);
        self.table().scan(
            &request,
            &base,
            None,
            &Row::into_span,
            &|span: &SpanRecord| {
                span.start_nanos >= start
                    && span.start_nanos <= end
                    && wanted.contains(span.trace_id.as_str())
            },
        )
    }
}

/// `replaceRegexpAll(x, '[^a-zA-Z]', '')`: a key's letters. Sanitising never changes an
/// ASCII letter, so two keys a relaxed lookup treats as one always share them.
fn letters_sql(column: &str) -> String {
    format!("replaceRegexpAll({column}, '[^a-zA-Z]', '')")
}

fn letters(text: &str) -> String {
    text.chars().filter(char::is_ascii_alphabetic).collect()
}

/// The condition a value must meet, as SQL over the expression `v`, when one can be
/// stated.
///
/// Only conditions the span must meet to match are stated: `!=`, `!~` and `= nil` are
/// left to the predicate, since an absent attribute and a present one read differently
/// there and a map lookup cannot tell them apart.
fn value_condition(op: SpanOp, value: &SpanValue, v: &str) -> Option<String> {
    match (op, value) {
        (SpanOp::Equal, SpanValue::Text(text)) => Some(format!("{v} = {}", sql::quote(text))),
        (SpanOp::Equal, SpanValue::Number(number)) => Some(format!(
            "abs(toFloat64OrNull({v}) - {}) < 2.220446049250313e-16",
            float(*number)
        )),
        (SpanOp::Regex, SpanValue::Text(pattern)) => Some(format!(
            "match({v}, {})",
            sql::quote(&format!("^(?s:{pattern})$"))
        )),
        (
            SpanOp::Greater | SpanOp::GreaterEqual | SpanOp::Less | SpanOp::LessEqual,
            SpanValue::Number(number),
        ) => Some(format!(
            "toFloat64OrNull({v}) {} {}",
            comparison(op),
            float(*number)
        )),
        _ => None,
    }
}

fn comparison(op: SpanOp) -> &'static str {
    match op {
        SpanOp::Greater => ">",
        SpanOp::GreaterEqual => ">=",
        SpanOp::Less => "<",
        SpanOp::LessEqual => "<=",
        SpanOp::NotEqual => "!=",
        SpanOp::Equal | SpanOp::Regex | SpanOp::NotRegex => "=",
    }
}

fn float(number: f64) -> String {
    if number.is_finite() {
        format!("{number:?}")
    } else if number.is_nan() {
        "nan".to_owned()
    } else if number > 0.0 {
        "inf".to_owned()
    } else {
        "-inf".to_owned()
    }
}

/// Some key of `map` that a relaxed lookup of `name` could find, holding a value that
/// meets the condition. `prefix` is what the span's own attribute key starts with for
/// this map — `resource.` for the resource attributes, which the span holds under it.
fn in_map(map: &str, prefix: &str, name: &str, op: SpanOp, value: &SpanValue) -> Option<String> {
    let key = if prefix.is_empty() {
        letters_sql("k")
    } else {
        format!(
            "concat({}, {})",
            sql::quote(&letters(prefix)),
            letters_sql("k")
        )
    };
    let test = value_condition(op, value, "v")?;
    Some(format!(
        "arrayExists((k, v) -> {key} = {} AND {test}, mapKeys({map}), mapValues({map}))",
        sql::quote(&letters(name))
    ))
}

/// One TraceQL condition as a condition ClickHouse selects by — necessary for the span
/// to match, never sufficient — or `None` where none can be stated.
fn condition(c: &SpanCondition) -> Option<String> {
    match &c.field {
        SpanField::Name => text_column("SpanName", c),
        SpanField::StatusMessage => text_column("StatusMessage", c),
        SpanField::Duration => {
            let nanos = match &c.value {
                #[allow(clippy::cast_precision_loss)]
                SpanValue::Nanos(nanos) => *nanos as f64,
                SpanValue::Number(number) => *number,
                _ => return Some("0".to_owned()),
            };
            Some(match c.op {
                SpanOp::Equal => format!(
                    "abs(toFloat64(Duration) - {}) < 2.220446049250313e-16",
                    float(nanos)
                ),
                SpanOp::NotEqual => format!(
                    "abs(toFloat64(Duration) - {}) >= 2.220446049250313e-16",
                    float(nanos)
                ),
                SpanOp::Regex | SpanOp::NotRegex => "0".to_owned(),
                op => format!("toFloat64(Duration) {} {}", comparison(op), float(nanos)),
            })
        }
        SpanField::Status => {
            let wanted = match &c.value {
                SpanValue::Status(status) => Some(*status),
                SpanValue::Text(text) => [SpanStatus::Unset, SpanStatus::Ok, SpanStatus::Error]
                    .into_iter()
                    .find(|s| s.as_str().eq_ignore_ascii_case(text)),
                _ => return Some("0".to_owned()),
            };
            equality("StatusCode", c.op, wanted.map(status_name))
        }
        SpanField::Kind => {
            let wanted = match &c.value {
                SpanValue::Kind(kind) => Some(*kind),
                SpanValue::Text(text) => KINDS
                    .into_iter()
                    .find(|k| k.as_str().eq_ignore_ascii_case(text)),
                _ => return Some("0".to_owned()),
            };
            equality("SpanKind", c.op, wanted.map(kind_name))
        }
        SpanField::Span(name) => {
            let own = in_map("SpanAttributes", "", name, c.op, &c.value)?;
            let resource = in_map(
                "ResourceAttributes",
                RESOURCE_ATTRIBUTE_PREFIX,
                name,
                c.op,
                &c.value,
            )?;
            Some(format!(
                "({}({own} OR {resource}))",
                present(c.op, &c.value, &["SpanAttributes", "ResourceAttributes"])
            ))
        }
        SpanField::Resource(name) => {
            let stream = in_map("Stream", "", name, c.op, &c.value)?;
            let key = sql::quote(name);
            let own = value_condition(c.op, &c.value, &format!("ResourceAttributes[{key}]"))?;
            Some(format!(
                "({}({stream} OR (mapContains(ResourceAttributes, {key}) AND {own})))",
                present(c.op, &c.value, &["Stream", "ResourceAttributes"])
            ))
        }
        SpanField::Unscoped(name) => {
            let own = in_map("SpanAttributes", "", name, c.op, &c.value)?;
            let resource = in_map(
                "ResourceAttributes",
                RESOURCE_ATTRIBUTE_PREFIX,
                name,
                c.op,
                &c.value,
            )?;
            let stream = in_map("Stream", "", name, c.op, &c.value)?;
            // A resource attribute is reachable unprefixed too, through `resource.x`'s
            // own lookup; letters of the bare name cover it.
            let bare = in_map("ResourceAttributes", "", name, c.op, &c.value)?;
            Some(format!(
                "({}({own} OR {resource} OR {stream} OR {bare}))",
                present(
                    c.op,
                    &c.value,
                    &["SpanAttributes", "ResourceAttributes", "Stream"]
                )
            ))
        }
    }
}

const KINDS: [SpanKind; 6] = [
    SpanKind::Unspecified,
    SpanKind::Internal,
    SpanKind::Server,
    SpanKind::Client,
    SpanKind::Producer,
    SpanKind::Consumer,
];

/// For equality with a text value, that it is among some map's values at all — implied by the
/// lookup it is put in front of, and the form a Bloom filter index on `mapValues` skips
/// granules by — as `"(…) AND "`; nothing for any other value.
fn present(op: SpanOp, value: &SpanValue, maps: &[&str]) -> String {
    let (SpanOp::Equal, SpanValue::Text(text)) = (op, value) else {
        return String::new();
    };
    let any: Vec<String> = maps
        .iter()
        .map(|map| format!("has(mapValues({map}), {})", sql::quote(text)))
        .collect();
    format!("({}) AND ", any.join(" OR "))
}

fn text_column(column: &str, c: &SpanCondition) -> Option<String> {
    match (c.op, &c.value) {
        (SpanOp::NotEqual, SpanValue::Text(text)) => {
            Some(format!("{column} != {}", sql::quote(text)))
        }
        (SpanOp::NotRegex, SpanValue::Text(pattern)) => Some(format!(
            "NOT match({column}, {})",
            sql::quote(&format!("^(?s:{pattern})$"))
        )),
        _ => value_condition(c.op, &c.value, column),
    }
}

fn equality(column: &str, op: SpanOp, wanted: Option<&str>) -> Option<String> {
    match (op, wanted) {
        (SpanOp::Equal, Some(name)) => Some(format!("{column} = {}", sql::quote(name))),
        (SpanOp::NotEqual, Some(name)) => Some(format!("{column} != {}", sql::quote(name))),
        // A status or kind nobody has: equal to nothing, unequal to everything.
        (SpanOp::NotEqual, None) => None,
        _ => Some("0".to_owned()),
    }
}

/// Kind as the Collector and the store package write it.
fn kind_name(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Unspecified => "Unspecified",
        SpanKind::Internal => "Internal",
        SpanKind::Server => "Server",
        SpanKind::Client => "Client",
        SpanKind::Producer => "Producer",
        SpanKind::Consumer => "Consumer",
    }
}

fn status_name(status: SpanStatus) -> &'static str {
    match status {
        SpanStatus::Unset => "Unset",
        SpanStatus::Ok => "Ok",
        SpanStatus::Error => "Error",
    }
}

/// One span as written.
#[derive(Serialize)]
struct WriteSpan<'a> {
    #[serde(rename = "Timestamp")]
    timestamp: u64,
    #[serde(rename = "TraceId")]
    trace_id: &'a str,
    #[serde(rename = "SpanId")]
    span_id: &'a str,
    #[serde(rename = "ParentSpanId")]
    parent_span_id: &'a str,
    #[serde(rename = "SpanName")]
    name: &'a str,
    #[serde(rename = "SpanKind")]
    kind: &'static str,
    #[serde(rename = "ServiceName")]
    service: &'a str,
    #[serde(rename = "ResourceAttributes")]
    resource: BTreeMap<&'a str, &'a str>,
    #[serde(rename = "SpanAttributes")]
    attributes: BTreeMap<&'a str, &'a str>,
    #[serde(rename = "Duration")]
    duration: u64,
    #[serde(rename = "StatusCode")]
    status: &'static str,
    #[serde(rename = "StatusMessage")]
    status_message: &'a str,
    #[serde(rename = "Events.Timestamp")]
    event_times: Vec<u64>,
    #[serde(rename = "Events.Name")]
    event_names: Vec<&'a str>,
    #[serde(rename = "Events.Attributes")]
    event_attributes: Vec<&'a Labels>,
    #[serde(rename = "Links.TraceId")]
    link_trace_ids: Vec<&'a str>,
    #[serde(rename = "Links.SpanId")]
    link_span_ids: Vec<&'a str>,
    #[serde(rename = "Links.TraceState")]
    link_trace_states: Vec<&'a str>,
    #[serde(rename = "Links.Attributes")]
    link_attributes: Vec<&'a Labels>,
    #[serde(rename = "Stream")]
    stream: &'a Labels,
}

impl<'a> From<&'a SpanRecord> for WriteSpan<'a> {
    fn from(span: &'a SpanRecord) -> Self {
        let mut resource = BTreeMap::new();
        let mut attributes = BTreeMap::new();
        for (name, value) in span.attributes.iter() {
            match name.strip_prefix(RESOURCE_ATTRIBUTE_PREFIX) {
                Some(stripped) => resource.insert(stripped, value),
                None => attributes.insert(name, value),
            };
        }
        Self {
            timestamp: span.start_nanos,
            trace_id: &span.trace_id,
            span_id: &span.span_id,
            parent_span_id: span.parent_span_id.as_deref().unwrap_or_default(),
            name: &span.name,
            kind: kind_name(span.kind),
            service: service_of(&span.stream),
            resource,
            attributes,
            duration: span.duration_nanos(),
            status: status_name(span.status),
            status_message: &span.status_message,
            event_times: span.events.iter().map(|e| e.time_nanos).collect(),
            event_names: span.events.iter().map(|e| e.name.as_str()).collect(),
            event_attributes: span.events.iter().map(|e| &e.attributes).collect(),
            link_trace_ids: span.links.iter().map(|l| l.trace_id.as_str()).collect(),
            link_span_ids: span.links.iter().map(|l| l.span_id.as_str()).collect(),
            link_trace_states: span.links.iter().map(|l| l.trace_state.as_str()).collect(),
            link_attributes: span.links.iter().map(|l| &l.attributes).collect(),
            stream: &span.stream,
        }
    }
}

/// One span as read.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Row {
    #[serde(rename = "t")]
    t: u64,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    span_name: String,
    span_kind: String,
    service_name: String,
    resource_attributes: BTreeMap<String, String>,
    span_attributes: BTreeMap<String, String>,
    duration: u64,
    status_code: String,
    status_message: String,
    event_times: Vec<u64>,
    event_names: Vec<String>,
    event_attributes: Vec<BTreeMap<String, String>>,
    link_trace_ids: Vec<String>,
    link_span_ids: Vec<String>,
    #[serde(default)]
    link_trace_states: Vec<String>,
    link_attributes: Vec<BTreeMap<String, String>>,
    #[serde(default)]
    stream: BTreeMap<String, String>,
}

impl Timed for Row {
    fn nanos(&self) -> u64 {
        self.t
    }
}

impl Row {
    fn into_span(self) -> SpanRecord {
        let kind = KINDS
            .into_iter()
            .find(|k| kind_name(*k).eq_ignore_ascii_case(&self.span_kind))
            .unwrap_or(SpanKind::Unspecified);
        let status = [SpanStatus::Unset, SpanStatus::Ok, SpanStatus::Error]
            .into_iter()
            .find(|s| status_name(*s).eq_ignore_ascii_case(&self.status_code))
            .unwrap_or(SpanStatus::Unset);
        let mut attributes = labels_of(self.span_attributes);
        for (name, value) in self.resource_attributes {
            attributes.insert(format!("{RESOURCE_ATTRIBUTE_PREFIX}{name}"), value);
        }
        // A row another writer put here has no stream: `app` is what every stream has.
        let stream = if self.stream.is_empty() {
            let mut stream = Labels::new();
            let app = if self.service_name.is_empty() {
                UNKNOWN_APP.to_owned()
            } else {
                self.service_name
            };
            stream.insert(APP_LABEL, app);
            stream
        } else {
            labels_of(self.stream)
        };
        let events = self
            .event_times
            .into_iter()
            .zip(self.event_names)
            .zip(self.event_attributes)
            .map(|((time_nanos, name), attributes)| SpanEvent {
                time_nanos,
                name,
                attributes: labels_of(attributes),
            })
            .collect();
        let mut trace_states = self.link_trace_states.into_iter();
        let links = self
            .link_trace_ids
            .into_iter()
            .zip(self.link_span_ids)
            .zip(self.link_attributes)
            .map(|((trace_id, span_id), attributes)| SpanLink {
                trace_id,
                span_id,
                trace_state: trace_states.next().unwrap_or_default(),
                attributes: labels_of(attributes),
            })
            .collect();
        SpanRecord {
            trace_id: self.trace_id,
            span_id: self.span_id,
            parent_span_id: Some(self.parent_span_id).filter(|id| !id.is_empty()),
            name: self.span_name,
            kind,
            start_nanos: self.t,
            end_nanos: self.t.saturating_add(self.duration),
            status,
            status_message: self.status_message,
            stream,
            attributes,
            events,
            links,
        }
    }
}
