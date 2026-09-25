//! The Tempo-compatible query API.
//!
//! Shapes match what `TempoSource` in `laravel-telemetry-ui` parses, which
//! differs from a naive reading of the Tempo docs in two ways that matter: search is
//! driven by TraceQL through `q`, and tag values come from the **v2** path.
//!
//! Note the unit change from Loki: Tempo's `start`/`end` are **seconds**. Matching each
//! upstream's own convention is the point of compatibility, however inconsistent that
//! is across the three.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};
use telemetryd_core::span::SpanRecord;
use telemetryd_core::{Error, Result};
use telemetryd_store::RecordStore;
use telemetryd_store::spans::SpanSchema;

use crate::traceql::{self, TraceQuery};

pub const DEFAULT_SEARCH_LIMIT: usize = 20;
pub const MAX_SEARCH_LIMIT: usize = 1_000;

/// Query parameters shared by the search endpoints.
#[derive(Debug, Default, Deserialize)]
pub struct SearchParams {
    /// TraceQL. The UI always sends this, even when it compiles to `{}`.
    pub q: Option<String>,
    /// Unix **seconds**.
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<String>,
    /// Accepted and ignored — Tempo's legacy tag syntax, superseded by `q`.
    pub tags: Option<String>,
    /// `resource`, `span` or `intrinsic` on the v2 tag listing; absent means all.
    pub scope: Option<String>,
    #[serde(rename = "minDuration")]
    pub min_duration: Option<String>,
    #[serde(rename = "maxDuration")]
    pub max_duration: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SearchRequest {
    pub query: TraceQuery,
    pub start_nanos: u64,
    pub end_nanos: u64,
    pub limit: usize,
    pub min_duration_nanos: Option<u64>,
    pub max_duration_nanos: Option<u64>,
}

impl SearchRequest {
    pub fn from_params(params: &SearchParams, now_nanos: u64) -> Result<Self> {
        let query = traceql::parse(params.q.as_deref().unwrap_or_default())?;

        let end_nanos = match params.end.as_deref().filter(|s| !s.is_empty()) {
            Some(raw) => parse_seconds(raw)?,
            None => now_nanos,
        };
        let start_nanos = match params.start.as_deref().filter(|s| !s.is_empty()) {
            Some(raw) => parse_seconds(raw)?,
            // Tempo's default lookback.
            None => end_nanos.saturating_sub(3_600_000_000_000),
        };
        if start_nanos > end_nanos {
            return Err(Error::BadRequest(
                "`start` must not be after `end`".to_owned(),
            ));
        }

        let limit = match params.limit.as_deref().filter(|s| !s.is_empty()) {
            Some(raw) => raw
                .trim()
                .parse::<usize>()
                .map_err(|_| Error::BadRequest(format!("`limit` must be a number, got {raw:?}")))?,
            None => DEFAULT_SEARCH_LIMIT,
        };
        if limit == 0 {
            return Err(Error::BadRequest(
                "`limit` must be greater than zero".to_owned(),
            ));
        }

        Ok(Self {
            query,
            start_nanos,
            end_nanos,
            limit: limit.min(MAX_SEARCH_LIMIT),
            min_duration_nanos: params
                .min_duration
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(parse_duration)
                .transpose()?,
            max_duration_nanos: params
                .max_duration
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(parse_duration)
                .transpose()?,
        })
    }

    /// `minDuration` and `maxDuration` bound the *trace*, as in Tempo. They were applied
    /// to each span, so `minDuration=2s` found every trace holding one slow span — and
    /// missed a slow trace made of many quick ones.
    fn duration_ok(&self, trace_nanos: u64) -> bool {
        self.min_duration_nanos.is_none_or(|min| trace_nanos >= min)
            && self.max_duration_nanos.is_none_or(|max| trace_nanos <= max)
    }
}

/// Tempo timestamps are Unix seconds, but clients occasionally send other units. The
/// same magnitude check as elsewhere keeps a mis-scaled range from returning nothing.
fn parse_seconds(raw: &str) -> Result<u64> {
    let raw = raw.trim();
    if let Ok(seconds) = raw.parse::<f64>()
        && seconds >= 0.0
        && seconds.is_finite()
    {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let as_nanos = (seconds * 1e9) as u64;
        // Anything already far beyond a plausible seconds value was sent in a finer
        // unit; crate::loki::parse_time knows the ranges.
        return Ok(if seconds > 4_102_444_800.0 {
            crate::loki::parse_time(raw)?
        } else {
            as_nanos
        });
    }
    crate::loki::parse_time(raw)
}

fn parse_duration(raw: &str) -> Result<u64> {
    match crate::lexer::tokenize(raw)?.first().map(|s| &s.token) {
        Some(crate::lexer::Token::Duration(nanos)) => Ok(*nanos),
        // A bare number in this position is milliseconds, as Tempo documents.
        Some(crate::lexer::Token::Number(number)) if *number >= 0.0 =>
        {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            Ok((*number * 1e6) as u64)
        }
        _ => Err(Error::BadRequest(format!(
            "{raw:?} is not a duration (try `100ms`, `1.5s`)"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub traces: Vec<TraceSummary>,
    pub metrics: SearchMetrics,
}

#[derive(Debug, Serialize)]
pub struct TraceSummary {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    #[serde(rename = "rootServiceName")]
    pub root_service_name: String,
    #[serde(rename = "rootTraceName")]
    pub root_trace_name: String,
    /// Nanoseconds, as a string — the UI does `intdiv($nano, 1_000_000_000)`.
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    #[serde(rename = "durationMs")]
    pub duration_ms: f64,
    /// The spans the TraceQL expression matched. The UI reads `spanSets` (v2) and
    /// falls back to the singular `spanSet`.
    #[serde(rename = "spanSets")]
    pub span_sets: Vec<SpanSet>,
}

#[derive(Debug, Serialize)]
pub struct SpanSet {
    pub spans: Vec<MatchedSpan>,
    pub matched: u32,
}

#[derive(Debug, Serialize)]
pub struct MatchedSpan {
    #[serde(rename = "spanID")]
    pub span_id: String,
    pub name: String,
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    #[serde(rename = "durationNanos")]
    pub duration_nanos: String,
    pub attributes: Vec<TempoKeyValue>,
}

#[derive(Debug, Serialize)]
pub struct TempoKeyValue {
    pub key: String,
    pub value: TempoValue,
}

#[derive(Debug, Serialize)]
pub struct TempoValue {
    #[serde(rename = "stringValue")]
    pub string_value: String,
}

#[derive(Debug, Default, Serialize)]
pub struct SearchMetrics {
    #[serde(rename = "inspectedTraces")]
    pub inspected_traces: u32,
    #[serde(rename = "inspectedSpans")]
    pub inspected_spans: u32,
}

#[derive(Debug, Serialize)]
pub struct TagsResponse {
    #[serde(rename = "tagNames")]
    pub tag_names: Vec<String>,
}

/// `GET /api/v2/search/tags`.
#[derive(Debug, Serialize)]
pub struct TagScopesResponse {
    pub scopes: Vec<TagScope>,
}

#[derive(Debug, Serialize)]
pub struct TagScope {
    pub name: &'static str,
    pub tags: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct TagValuesResponse {
    #[serde(rename = "tagValues")]
    pub tag_values: Vec<TagValue>,
}

#[derive(Debug, Serialize)]
pub struct TagValue {
    /// Always `"string"`: every attribute value is stored as text.
    #[serde(rename = "type")]
    pub value_type: &'static str,
    pub value: String,
}

/// `GET /api/traces/{traceID}` — OTLP `resourceSpans`, under Tempo's `batches` key.
#[derive(Debug, Serialize)]
pub struct TraceResponse {
    pub batches: Vec<ResourceSpans>,
}

#[derive(Debug, Serialize)]
pub struct ResourceSpans {
    pub resource: ResourceJson,
    #[serde(rename = "scopeSpans")]
    pub scope_spans: Vec<ScopeSpans>,
}

#[derive(Debug, Serialize)]
pub struct ResourceJson {
    pub attributes: Vec<TempoKeyValue>,
}

#[derive(Debug, Serialize)]
pub struct ScopeSpans {
    pub spans: Vec<SpanJson>,
}

#[derive(Debug, Serialize)]
pub struct SpanJson {
    #[serde(rename = "traceId")]
    pub trace_id: String,
    #[serde(rename = "spanId")]
    pub span_id: String,
    #[serde(rename = "parentSpanId", skip_serializing_if = "String::is_empty")]
    pub parent_span_id: String,
    pub name: String,
    pub kind: i32,
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    #[serde(rename = "endTimeUnixNano")]
    pub end_time_unix_nano: String,
    pub attributes: Vec<TempoKeyValue>,
    pub status: StatusJson,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<EventJson>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<LinkJson>,
}

#[derive(Debug, Serialize)]
pub struct StatusJson {
    pub code: i32,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// A link to another span, in the same id spelling as the span itself.
#[derive(Debug, Serialize)]
pub struct LinkJson {
    #[serde(rename = "traceId")]
    pub trace_id: String,
    #[serde(rename = "spanId")]
    pub span_id: String,
    #[serde(rename = "traceState", skip_serializing_if = "String::is_empty")]
    pub trace_state: String,
    pub attributes: Vec<TempoKeyValue>,
}

#[derive(Debug, Serialize)]
pub struct EventJson {
    #[serde(rename = "timeUnixNano")]
    pub time_unix_nano: String,
    pub name: String,
    pub attributes: Vec<TempoKeyValue>,
}

fn key_values<'a>(pairs: impl Iterator<Item = (&'a str, &'a str)>) -> Vec<TempoKeyValue> {
    pairs
        .map(|(key, value)| TempoKeyValue {
            key: key.to_owned(),
            value: TempoValue {
                string_value: value.to_owned(),
            },
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// How many spans one search, tag listing or tag-value listing may read.
///
/// These read every span in the window and kept all of them before trimming to `limit`:
/// `{}` over a day was the whole day's spans in memory for one request, estimated at half
/// a gigabyte an hour on a busy instance. They read newest first now and stop at this
/// many, which is what Tempo does too — search answers with the most recent matches, and a
/// tag list built from the latest spans is the list a person picking a tag wants.
pub const MAX_SPANS_READ: usize = 100_000;

fn newest(start_nanos: u64, end_nanos: u64) -> telemetryd_store::Scan<'static> {
    let mut scan = telemetryd_store::Scan::range(start_nanos, end_nanos);
    scan.limit = MAX_SPANS_READ;
    scan.order = telemetryd_store::Order::Descending;
    scan
}

/// Spans grouped by trace, each trace's spans once apiece and in start order.
///
/// Once apiece: an exporter that retries a batch it was not sure had landed sends the
/// same spans twice, and they used to be counted and drawn twice.
fn by_trace(spans: Vec<SpanRecord>) -> HashMap<String, Vec<SpanRecord>> {
    let mut traces: HashMap<String, Vec<SpanRecord>> = HashMap::new();
    for span in spans {
        traces.entry(span.trace_id.clone()).or_default().push(span);
    }
    for spans in traces.values_mut() {
        *spans = distinct_spans(std::mem::take(spans));
    }
    traces
}

fn distinct_spans(mut spans: Vec<SpanRecord>) -> Vec<SpanRecord> {
    spans.sort_by(|a, b| {
        a.start_nanos
            .cmp(&b.start_nanos)
            .then_with(|| a.span_id.cmp(&b.span_id))
    });
    let mut seen = std::collections::HashSet::with_capacity(spans.len());
    spans.retain(|span| seen.insert(span.span_id.clone()));
    spans
}

/// `GET /api/search`
///
/// Two reads: one for the spans the query matches, which say which traces qualify, and
/// one for those traces whole. A result row describes the *trace* — its root span's
/// name and service, its start, its duration — and those are facts about spans the
/// query need not have matched. They used to come from the matched spans alone, so
/// `{ span.db.system = "mysql" }` listed every trace under the name and duration of
/// its database query.
pub fn search(store: &RecordStore<SpanSchema>, request: &SearchRequest) -> Result<SearchResponse> {
    let inspected = std::sync::atomic::AtomicU32::new(0);
    let filter = |span: &SpanRecord| {
        inspected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        request.query.matches(span)
    };
    let window = || newest(request.start_nanos, request.end_nanos);
    let matched = by_trace(store.scan(window(), &[], &filter)?);

    let whole = if matched.is_empty() {
        HashMap::new()
    } else {
        by_trace(store.scan(window(), &[], &|span: &SpanRecord| {
            matched.contains_key(&span.trace_id)
        })?)
    };

    let mut summaries: Vec<(u64, TraceSummary)> = matched
        .into_iter()
        .filter_map(|(trace_id, spans)| {
            let all = whole.get(&trace_id).map_or(spans.as_slice(), Vec::as_slice);
            let start = all.iter().map(|s| s.start_nanos).min().unwrap_or(0);
            let end = all.iter().map(|s| s.end_nanos).max().unwrap_or(start);
            let duration = end.saturating_sub(start);
            if !request.duration_ok(duration) {
                return None;
            }
            // The root when the window holds it; otherwise the earliest span, so the row
            // still names something rather than looking broken.
            let root = all.iter().find(|s| s.is_root()).unwrap_or(&all[0]);
            #[allow(clippy::cast_precision_loss)]
            let duration_ms = duration as f64 / 1e6;

            let summary = TraceSummary {
                root_service_name: root.service_name().to_owned(),
                root_trace_name: root.name.clone(),
                start_time_unix_nano: start.to_string(),
                duration_ms,
                span_sets: vec![SpanSet {
                    matched: u32::try_from(spans.len()).unwrap_or(u32::MAX),
                    spans: spans
                        .iter()
                        .map(|span| MatchedSpan {
                            span_id: span.span_id.clone(),
                            name: span.name.clone(),
                            start_time_unix_nano: span.start_nanos.to_string(),
                            duration_nanos: span.duration_nanos().to_string(),
                            attributes: key_values(span.attributes.iter()),
                        })
                        .collect(),
                }],
                trace_id,
            };
            Some((start, summary))
        })
        .collect();

    // Newest first: a trace list is read from the top. Ties by id, so equal starts do
    // not reshuffle between refreshes now that grouping is by hash.
    summaries.sort_by(|(a, x), (b, y)| b.cmp(a).then_with(|| x.trace_id.cmp(&y.trace_id)));
    summaries.truncate(request.limit);
    let summaries: Vec<TraceSummary> = summaries.into_iter().map(|(_, s)| s).collect();

    let inspected_spans = inspected.load(std::sync::atomic::Ordering::Relaxed);
    Ok(SearchResponse {
        metrics: SearchMetrics {
            inspected_traces: u32::try_from(summaries.len()).unwrap_or(u32::MAX),
            inspected_spans,
        },
        traces: summaries,
    })
}

/// `GET /api/traces/{traceID}`
///
/// Returns every span in the trace, grouped by resource, in OTLP shape. The whole
/// retention window is searched rather than a time range: a trace id is exact, and a
/// client that has one should not also have to know when it happened.
pub fn trace(store: &RecordStore<SpanSchema>, trace_id: &str) -> Result<TraceResponse> {
    let wanted = trace_id.trim().to_ascii_lowercase();
    if wanted.is_empty() {
        return Err(Error::BadRequest("empty trace id".to_owned()));
    }

    // The whole retention window, but not the whole store: `exact_key` lets each
    // segment's Bloom filter answer "this trace is definitely not here" without any
    // I/O. Without it a trace lookup reads every segment, which is the difference
    // between a millisecond and several seconds on a full disk.
    let spans = store.scan(
        telemetryd_store::Scan {
            abort_over: 0,
            start_nanos: 0,
            end_nanos: u64::MAX,
            limit: 0,
            order: telemetryd_store::Order::Ascending,
            exact_key: Some(&wanted),
            columns: None,
            required_text: None,
        },
        &[],
        &|span: &SpanRecord| span.trace_id == wanted,
    )?;

    // One batch per distinct resource, as OTLP models it; each span once.
    let mut by_resource: BTreeMap<telemetryd_core::Labels, Vec<SpanRecord>> = BTreeMap::new();
    for span in distinct_spans(spans) {
        by_resource
            .entry(span.stream.clone())
            .or_default()
            .push(span);
    }

    let batches = by_resource
        .into_iter()
        .map(|(resource, mut spans)| {
            spans.sort_by_key(|s| s.start_nanos);
            ResourceSpans {
                resource: ResourceJson {
                    // The UI reads `service.name` with a dot, so the dotted spelling is
                    // restored here even though it is stored sanitised.
                    attributes: key_values(resource.iter().map(|(k, v)| {
                        (
                            if k == "service_name" {
                                "service.name"
                            } else {
                                k
                            },
                            v,
                        )
                    })),
                },
                scope_spans: vec![ScopeSpans {
                    spans: spans.iter().map(to_span_json).collect(),
                }],
            }
        })
        .collect();

    Ok(TraceResponse { batches })
}

fn to_span_json(span: &SpanRecord) -> SpanJson {
    SpanJson {
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        parent_span_id: span.parent_span_id.clone().unwrap_or_default(),
        name: span.name.clone(),
        kind: span.kind.as_otlp_number(),
        start_time_unix_nano: span.start_nanos.to_string(),
        end_time_unix_nano: span.end_nanos.to_string(),
        attributes: key_values(span.attributes.iter()),
        status: StatusJson {
            code: span.status.as_otlp_number(),
            message: span.status_message.clone(),
        },
        events: span
            .events
            .iter()
            .map(|event| EventJson {
                time_unix_nano: event.time_nanos.to_string(),
                name: event.name.clone(),
                attributes: key_values(event.attributes.iter()),
            })
            .collect(),
        links: span
            .links
            .iter()
            .map(|link| LinkJson {
                trace_id: link.trace_id.clone(),
                span_id: link.span_id.clone(),
                trace_state: link.trace_state.clone(),
                attributes: key_values(link.attributes.iter()),
            })
            .collect(),
    }
}

/// `GET /api/search/tags`
///
/// Both resource labels and span attributes, since TraceQL can filter on either.
pub fn tags(
    store: &RecordStore<SpanSchema>,
    start_nanos: u64,
    end_nanos: u64,
) -> Result<TagsResponse> {
    let scoped = scoped_tags(store, start_nanos, end_nanos)?;
    let mut names: BTreeSet<String> = BTreeSet::new();
    for (_, tags) in scoped {
        names.extend(tags);
    }
    Ok(TagsResponse {
        tag_names: names.into_iter().collect(),
    })
}

/// `GET /api/v2/search/tags` — the same names, grouped by TraceQL scope.
///
/// Grafana's Tempo datasource reads only `scopes` from this path, and because the call
/// succeeds it never falls back to v1; answering with the v1 shape left its query builder
/// and autocomplete empty. `scope` narrows the answer to one of `resource`, `span` or
/// `intrinsic`, as Tempo's does.
pub fn tags_v2(
    store: &RecordStore<SpanSchema>,
    start_nanos: u64,
    end_nanos: u64,
    scope: Option<&str>,
) -> Result<TagScopesResponse> {
    let wanted = scope
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != "all");
    let scopes = scoped_tags(store, start_nanos, end_nanos)?
        .into_iter()
        .filter(|(name, _)| wanted.is_none_or(|w| w.eq_ignore_ascii_case(name)))
        .map(|(name, tags)| TagScope {
            name,
            tags: tags.into_iter().collect(),
        })
        .collect();
    Ok(TagScopesResponse { scopes })
}

/// Tag names by scope: stream labels are the resource, span attributes are the span,
/// and the intrinsics every span has.
fn scoped_tags(
    store: &RecordStore<SpanSchema>,
    start_nanos: u64,
    end_nanos: u64,
) -> Result<Vec<(&'static str, BTreeSet<String>)>> {
    let resource: BTreeSet<String> = store
        .label_names(start_nanos, end_nanos)
        .into_iter()
        .collect();
    let mut span: BTreeSet<String> = BTreeSet::new();
    for record in store.scan(newest(start_nanos, end_nanos), &[], &|_| true)? {
        span.extend(record.attributes.names().map(str::to_owned));
    }
    // Intrinsics are filterable, so they belong in the tag list a UI offers.
    let intrinsic: BTreeSet<String> = ["name", "status", "duration", "kind"]
        .map(str::to_owned)
        .into_iter()
        .collect();
    Ok(vec![
        ("resource", resource),
        ("span", span),
        ("intrinsic", intrinsic),
    ])
}

/// `GET /api/v2/search/tag/{name}/values`
pub fn tag_values(
    store: &RecordStore<SpanSchema>,
    tag: &str,
    request: &SearchRequest,
) -> Result<TagValuesResponse> {
    // Accept the scoped spellings the UI may send, since TraceQL uses them.
    let name = tag
        .trim()
        .trim_start_matches("resource.")
        .trim_start_matches("span.")
        .trim_start_matches('.');
    let wanted = name.to_owned();

    let mut values: BTreeSet<String> = BTreeSet::new();
    for span in store.scan(
        newest(request.start_nanos, request.end_nanos),
        &[],
        &|span| request.query.matches(span),
    )? {
        match wanted.as_str() {
            "name" => values.insert(span.name.clone()),
            "status" => values.insert(span.status.as_str().to_owned()),
            "kind" => values.insert(span.kind.as_str().to_owned()),
            other => {
                match span
                    .attributes
                    .get_relaxed(other)
                    .or_else(|| span.stream.get(other))
                    .or_else(|| {
                        // Stream labels are stored sanitised, so a dotted query name
                        // has to be sanitised to reach them.
                        span.stream
                            .get(&telemetryd_core::record::sanitize_label_name(other))
                    }) {
                    Some(value) => values.insert(value.to_owned()),
                    None => false,
                }
            }
        };
    }

    Ok(TagValuesResponse {
        tag_values: values
            .into_iter()
            .map(|value| TagValue {
                value_type: "string",
                value,
            })
            .collect(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const NOW: u64 = 1_750_000_000_000_000_000;

    #[test]
    fn tempo_timestamps_are_seconds() {
        let params = SearchParams {
            start: Some("1750000000".to_owned()),
            end: Some("1750003600".to_owned()),
            ..SearchParams::default()
        };
        let request = SearchRequest::from_params(&params, NOW).unwrap();
        assert_eq!(request.start_nanos, NOW);
        assert_eq!(request.end_nanos, NOW + 3_600_000_000_000);
    }

    #[test]
    fn fractional_seconds_are_accepted() {
        let params = SearchParams {
            start: Some("1750000000.5".to_owned()),
            ..SearchParams::default()
        };
        let request = SearchRequest::from_params(&params, NOW + 3_600_000_000_000).unwrap();
        assert_eq!(request.start_nanos, NOW + 500_000_000);
    }

    #[test]
    fn a_nanosecond_timestamp_is_still_understood() {
        // Not what Tempo documents, but harmless to accept and awful to debug if not.
        let params = SearchParams {
            start: Some("1750000000000000000".to_owned()),
            ..SearchParams::default()
        };
        let request = SearchRequest::from_params(&params, NOW + 1).unwrap();
        assert_eq!(request.start_nanos, NOW);
    }

    #[test]
    fn the_range_defaults_to_the_last_hour() {
        let request = SearchRequest::from_params(&SearchParams::default(), NOW).unwrap();
        assert_eq!(request.end_nanos, NOW);
        assert_eq!(request.start_nanos, NOW - 3_600_000_000_000);
    }

    #[test]
    fn an_empty_q_means_every_span() {
        for q in [None, Some(String::new()), Some("{}".to_owned())] {
            let params = SearchParams {
                q,
                ..SearchParams::default()
            };
            let request = SearchRequest::from_params(&params, NOW).unwrap();
            assert!(request.query.is_empty());
        }
    }

    #[test]
    fn min_and_max_duration_parse_both_spellings() {
        let params = SearchParams {
            min_duration: Some("100ms".to_owned()),
            // A bare number here is milliseconds, as Tempo documents.
            max_duration: Some("500".to_owned()),
            ..SearchParams::default()
        };
        let request = SearchRequest::from_params(&params, NOW).unwrap();
        assert_eq!(request.min_duration_nanos, Some(100_000_000));
        assert_eq!(request.max_duration_nanos, Some(500_000_000));
    }

    #[test]
    fn limit_defaults_and_clamps() {
        let base = |limit: Option<&str>| SearchParams {
            limit: limit.map(str::to_owned),
            ..SearchParams::default()
        };
        assert_eq!(
            SearchRequest::from_params(&base(None), NOW).unwrap().limit,
            DEFAULT_SEARCH_LIMIT
        );
        assert_eq!(
            SearchRequest::from_params(&base(Some("999999")), NOW)
                .unwrap()
                .limit,
            MAX_SEARCH_LIMIT
        );
        assert!(SearchRequest::from_params(&base(Some("0")), NOW).is_err());
    }

    #[test]
    fn an_unsupported_traceql_feature_propagates_as_unsupported() {
        let params = SearchParams {
            q: Some("{ a = 1 } || { b = 2 }".to_owned()),
            ..SearchParams::default()
        };
        let err = SearchRequest::from_params(&params, NOW).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
    }

    #[test]
    fn the_tag_values_response_uses_the_v2_object_shape() {
        let response = TagValuesResponse {
            tag_values: vec![TagValue {
                value_type: "string",
                value: "checkout".to_owned(),
            }],
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["tagValues"][0]["type"], "string");
        assert_eq!(json["tagValues"][0]["value"], "checkout");
    }

    #[test]
    fn the_search_summary_matches_what_the_ui_reads() {
        let response = SearchResponse {
            traces: vec![TraceSummary {
                trace_id: "abc".to_owned(),
                root_service_name: "checkout".to_owned(),
                root_trace_name: "POST /checkout".to_owned(),
                start_time_unix_nano: NOW.to_string(),
                duration_ms: 150.0,
                span_sets: vec![SpanSet {
                    spans: Vec::new(),
                    matched: 0,
                }],
            }],
            metrics: SearchMetrics::default(),
        };
        let json = serde_json::to_value(&response).unwrap();

        assert_eq!(json["traces"][0]["traceID"], "abc");
        assert_eq!(json["traces"][0]["rootServiceName"], "checkout");
        assert_eq!(json["traces"][0]["rootTraceName"], "POST /checkout");
        assert_eq!(json["traces"][0]["durationMs"], 150.0);
        // Nanoseconds as a string: the UI does intdiv() on it.
        assert!(json["traces"][0]["startTimeUnixNano"].is_string());
        assert!(json["traces"][0]["spanSets"].is_array());
    }
}

// ---------------------------------------------------------------------------
// Protobuf, for Grafana
// ---------------------------------------------------------------------------

/// Grafana's Tempo datasource asks for traces with `Accept: application/protobuf` and
/// decodes the body with `proto.Unmarshal` whatever its content type — it tries
/// `/api/v2/traces/{id}` first and falls back to `/api/traces/{id}` only on a 404. JSON
/// therefore never worked there: the trace view failed on every trace.
///
/// `tempopb.Trace` is `repeated ResourceSpans resourceSpans = 1`, which is byte for
/// byte OTLP's `TracesData`, so these are the OTLP field numbers the ingest decoder reads.
impl TraceResponse {
    /// `tempopb.Trace`, for `/api/traces/{id}`.
    #[must_use]
    pub fn to_protobuf(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for batch in &self.batches {
            pb::message(&mut out, 1, &batch.to_protobuf());
        }
        out
    }

    /// `tempopb.TraceByIDResponse`, for `/api/v2/traces/{id}`: the trace under field 1.
    #[must_use]
    pub fn to_protobuf_v2(&self) -> Vec<u8> {
        let mut out = Vec::new();
        pb::message(&mut out, 1, &self.to_protobuf());
        out
    }
}

impl ResourceSpans {
    fn to_protobuf(&self) -> Vec<u8> {
        let mut resource = Vec::new();
        for kv in &self.resource.attributes {
            pb::message(&mut resource, 1, &kv.to_protobuf());
        }
        let mut out = Vec::new();
        pb::message(&mut out, 1, &resource);
        for scope in &self.scope_spans {
            // The instrumentation scope, always present even when empty. Grafana reads
            // `scope.Name` without checking for nil, so leaving an empty scope out — which
            // proto3 would allow — panicked its trace transform on every trace. Found by
            // running Grafana 12.2 against this, not by reading.
            let mut encoded = Vec::new();
            pb::message(&mut encoded, 1, &[]);
            for span in &scope.spans {
                pb::message(&mut encoded, 2, &span.to_protobuf());
            }
            pb::message(&mut out, 2, &encoded);
        }
        out
    }
}

impl SpanJson {
    fn to_protobuf(&self) -> Vec<u8> {
        let mut out = Vec::new();
        pb::bytes(&mut out, 1, &pb::hex(&self.trace_id));
        pb::bytes(&mut out, 2, &pb::hex(&self.span_id));
        pb::bytes(&mut out, 4, &pb::hex(&self.parent_span_id));
        pb::string(&mut out, 5, &self.name);
        pb::uint(&mut out, 6, u64::try_from(self.kind).unwrap_or(0));
        pb::fixed64(&mut out, 7, self.start_time_unix_nano.parse().unwrap_or(0));
        pb::fixed64(&mut out, 8, self.end_time_unix_nano.parse().unwrap_or(0));
        for kv in &self.attributes {
            pb::message(&mut out, 9, &kv.to_protobuf());
        }
        for event in &self.events {
            let mut encoded = Vec::new();
            pb::fixed64(&mut encoded, 1, event.time_unix_nano.parse().unwrap_or(0));
            pb::string(&mut encoded, 2, &event.name);
            for kv in &event.attributes {
                pb::message(&mut encoded, 3, &kv.to_protobuf());
            }
            pb::message(&mut out, 11, &encoded);
        }
        for link in &self.links {
            let mut encoded = Vec::new();
            pb::bytes(&mut encoded, 1, &pb::hex(&link.trace_id));
            pb::bytes(&mut encoded, 2, &pb::hex(&link.span_id));
            pb::string(&mut encoded, 3, &link.trace_state);
            for kv in &link.attributes {
                pb::message(&mut encoded, 4, &kv.to_protobuf());
            }
            pb::message(&mut out, 13, &encoded);
        }
        let mut status = Vec::new();
        pb::string(&mut status, 2, &self.status.message);
        pb::uint(&mut status, 3, u64::try_from(self.status.code).unwrap_or(0));
        pb::message(&mut out, 15, &status);
        out
    }
}

impl TempoKeyValue {
    fn to_protobuf(&self) -> Vec<u8> {
        let mut value = Vec::new();
        pb::string(&mut value, 1, &self.value.string_value);
        let mut out = Vec::new();
        pb::string(&mut out, 1, &self.key);
        pb::message(&mut out, 2, &value);
        out
    }
}

/// The few protobuf writers these messages need. Proto3 omits zero and empty scalars, and
/// so do these; a nested message is always written, since its presence carries meaning.
mod pb {
    pub(super) fn varint(out: &mut Vec<u8>, mut value: u64) {
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
        varint(out, (u64::from(field) << 3) | u64::from(wire));
    }

    pub(super) fn message(out: &mut Vec<u8>, field: u32, payload: &[u8]) {
        tag(out, field, 2);
        varint(out, payload.len() as u64);
        out.extend_from_slice(payload);
    }

    pub(super) fn bytes(out: &mut Vec<u8>, field: u32, payload: &[u8]) {
        if !payload.is_empty() {
            message(out, field, payload);
        }
    }

    pub(super) fn string(out: &mut Vec<u8>, field: u32, text: &str) {
        bytes(out, field, text.as_bytes());
    }

    pub(super) fn uint(out: &mut Vec<u8>, field: u32, value: u64) {
        if value != 0 {
            tag(out, field, 0);
            varint(out, value);
        }
    }

    pub(super) fn fixed64(out: &mut Vec<u8>, field: u32, value: u64) {
        if value != 0 {
            tag(out, field, 1);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }

    /// Ids are hex on the JSON side and raw bytes on the wire. Anything that is not hex
    /// becomes an absent id rather than a wrong one.
    pub(super) fn hex(text: &str) -> Vec<u8> {
        if !text.len().is_multiple_of(2) {
            return Vec::new();
        }
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16))
            .collect::<Result<Vec<u8>, _>>()
            .unwrap_or_default()
    }
}
