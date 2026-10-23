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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::ControlFlow;

use serde::{Deserialize, Serialize};
use telemetryd_core::record::sanitize_label_name;
use telemetryd_core::span::{
    RESOURCE_ATTRIBUTE_PREFIX, SpanEvent, SpanKind, SpanLink, SpanRecord, SpanStatus,
};
use telemetryd_core::{APP_LABEL, Error, Labels, Result, UNKNOWN_APP};

use super::Client;
use super::scan::{Selected, Table, Timed, labels_of, time_condition};
use super::schema::{TRACE_IDS, TRACE_KEYS, TRACE_STREAMS, TRACES};
use super::sql;
use super::traceql::{KINDS, Keys, STATUSES, kind_name, status_name};
use crate::backend::{
    RecordFilter, SpanAggregateRequest, SpanAggregateRow, SpanAggregates, SpanAggregation,
    SpanCondition, SpanField, SpanOp, SpanStore, SpanValue,
};
use crate::records::Scan;
use crate::topk::Order;

/// Spans an aggregate may hand back to be decided one by one. Past this the caller
/// scans: holding them all would be the scan's cost and more.
const MAX_UNDECIDED: usize = 100_000;

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
    /// From when `otel_trace_keys` holds every attribute key; `None` without one.
    keys_since: Option<u64>,
}

/// Distinct attribute keys a range's spans may carry before listing them stops paying:
/// past this a relaxed lookup is matched on letters instead.
const MAX_KEYS: usize = 10_000;

impl ClickHouseSpans {
    #[must_use]
    pub fn new(client: Client) -> Self {
        // A database whose schema someone else manages may have no keys summary, and
        // then relaxed lookups are matched on letters.
        let keys_since = super::schema::rollup_start(&client, super::schema::TRACE_KEYS_SINCE)
            .ok()
            .flatten();
        Self { client, keys_since }
    }

    /// The attribute keys and stream labels the selected streams' spans can carry over
    /// the range, when the keys summary holds them all and any field read needs them.
    fn keys(
        &self,
        start: u64,
        end: u64,
        selected: &Selected,
        fields: &[&SpanField],
    ) -> Result<Option<Keys>> {
        const HOUR: u64 = 3_600_000_000_000;
        let needed = fields
            .iter()
            .any(|f| matches!(f, SpanField::Span(_) | SpanField::Unscoped(_)));
        if !needed || self.keys_since.is_none_or(|since| start < since) {
            return Ok(None);
        }
        let services: Vec<String> = selected.services.iter().map(|s| sql::quote(s)).collect();
        if services.is_empty() {
            return Ok(Some(Keys::default()));
        }
        let mut keys = Keys::default();
        let mut listed = 0usize;
        self.client.query_lines(
            &format!(
                "SELECT DISTINCT Scope AS scope, Key AS key FROM {TRACE_KEYS} \
                 WHERE ServiceName IN ({}) AND Hour >= toDateTime({}) AND Hour <= toDateTime({}) \
                 LIMIT {}",
                services.join(", "),
                start / HOUR * 3600,
                end / 1_000_000_000,
                MAX_KEYS + 1
            ),
            |line| {
                let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
                let text = |n: &str| value.get(n).and_then(serde_json::Value::as_str);
                listed += 1;
                match (text("scope"), text("key")) {
                    (Some("span"), Some(key)) => {
                        keys.span.insert(key.to_owned());
                    }
                    (Some("resource"), Some(key)) => {
                        keys.resource.insert(key.to_owned());
                    }
                    _ => {}
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        if listed > MAX_KEYS {
            return Ok(None);
        }
        for stream in selected.streams.values() {
            keys.stream.extend(stream.names().map(str::to_owned));
        }
        Ok(Some(keys))
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

    /// Lower each new trace's start bound to its first span, from the trace-id index,
    /// clipped to the range as the caller's ranking clips it.
    fn lower_bounds(
        &self,
        fresh: &[String],
        starts: &mut HashMap<String, (u64, u64)>,
        start: u64,
    ) -> Result<()> {
        for ids in fresh.chunks(500) {
            let quoted: Vec<String> = ids.iter().map(|id| sql::quote(id)).collect();
            self.client.query_lines(
                &format!(
                    "SELECT TraceId AS id, toUnixTimestamp64Nano(min(Start)) AS first \
                     FROM {TRACE_IDS} WHERE TraceId IN ({}) GROUP BY TraceId",
                    quoted.join(", ")
                ),
                |line| {
                    let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
                    let id = value.get("id").and_then(serde_json::Value::as_str);
                    let first = value.get("first").and_then(serde_json::Value::as_u64);
                    if let (Some(id), Some(first)) = (id, first)
                        && let Some((lower, _)) = starts.get_mut(id)
                    {
                        *lower = (*lower).min(first.max(start));
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
        }
        Ok(())
    }

    /// The first and last span start of any of these traces, from the trace-id index:
    /// quoted ids in, `None` when none of them is there.
    fn trace_bounds(&self, quoted_ids: &[String]) -> Result<Option<(u64, u64)>> {
        let mut bounds = None;
        self.client.query_lines(
            &format!(
                "SELECT toUnixTimestamp64Nano(min(Start)) AS low, \
                 toUnixTimestamp64Nano(max(End)) AS high, count() AS n \
                 FROM {TRACE_IDS} WHERE TraceId IN ({})",
                quoted_ids.join(", ")
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
        Ok(bounds)
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
    /// The query aggregated in ClickHouse: the spans every condition's sufficient form
    /// takes, counted and measured per step and group there; the ones only the
    /// necessary forms take, handed back to be decided one by one. `None` for a field
    /// ClickHouse cannot read as Rust does — a duration as a label, an unscoped name —
    /// and for quantiles when any span is undecided, since quantiles do not add.
    fn aggregate(&self, request: &SpanAggregateRequest<'_>) -> Result<Option<SpanAggregates>> {
        if request
            .group_by
            .iter()
            .any(|f| matches!(f, SpanField::Duration))
        {
            return Ok(None);
        }
        let mut out = SpanAggregates::default();
        let Some(last) = request.end_nanos.checked_sub(1) else {
            return Ok(Some(out));
        };
        if request.start_nanos > last || request.steps == 0 || request.step_nanos == 0 {
            return Ok(Some(out));
        }
        let Some(selected) = self.select(request.start_nanos, last, request.conditions)? else {
            return Ok(None);
        };
        if selected.streams.is_empty() {
            return Ok(Some(out));
        }
        let mut read: Vec<&SpanField> = fields(request.conditions);
        read.extend(request.group_by.iter());
        let keys = self.keys(request.start_nanos, last, &selected, &read)?;
        let mut groups = Vec::new();
        let mut exact = Vec::new();
        for field in request.group_by {
            let Some((expr, decided)) = super::traceql::group_value(field, keys.as_ref()) else {
                return Ok(None);
            };
            groups.push(expr);
            exact.push(decided);
        }
        let stated: Vec<_> = request
            .conditions
            .iter()
            .map(|c| super::traceql::condition(c, keys.as_ref()))
            .collect();
        let mut necessary = vec![
            time_condition((request.start_nanos, true), (last, true)),
            selected.condition(),
        ];
        necessary.extend(stated.iter().map(|s| s.necessary.clone()));
        let necessary = necessary.join(" AND ");
        let mut sufficient: Vec<String> = stated.iter().map(|s| s.sufficient.clone()).collect();
        sufficient.extend(exact);
        let sufficient = if sufficient.is_empty() {
            "1".to_owned()
        } else {
            sufficient.join(" AND ")
        };

        let query = aggregate_query(request, &groups, &necessary, &sufficient);
        let mut index: HashMap<(usize, Vec<Option<String>>), usize> = HashMap::new();
        self.client.query_lines(&query, |line| {
            absorb_aggregate(line, request, groups.len(), &mut index, &mut out)?;
            Ok(ControlFlow::Continue(()))
        })?;

        let mut overflowed = false;
        self.table().read(
            &format!("{necessary} AND NOT ({sufficient})"),
            Order::Ascending,
            Some(MAX_UNDECIDED + 1),
            |row: Row| {
                if out.individual.len() >= MAX_UNDECIDED {
                    overflowed = true;
                    return Ok(ControlFlow::Break(()));
                }
                out.individual.push(row.into_span());
                Ok(ControlFlow::Continue(()))
            },
        )?;
        // Too many to hold for deciding one by one — the query reads an attribute only a
        // relaxed name reaches, on most spans — or quantiles, which do not add: the
        // caller scans instead.
        if overflowed
            || (matches!(request.aggregation, SpanAggregation::Quantiles(_))
                && !out.individual.is_empty())
        {
            return Ok(None);
        }
        Ok(Some(out))
    }

    fn scan(
        &self,
        request: Scan<'_>,
        conditions: &[SpanCondition],
        extra: RecordFilter<'_, SpanRecord>,
    ) -> Result<Vec<SpanRecord>> {
        let selected = self.select(request.start_nanos, request.end_nanos, conditions)?;
        let keys = match &selected {
            Some(selected) => self.keys(
                request.start_nanos,
                request.end_nanos,
                selected,
                &fields(conditions),
            )?,
            None => None,
        };
        let mut base: Vec<String> = vec![
            selected
                .as_ref()
                .map_or_else(|| "1".to_owned(), Selected::condition),
        ];
        base.extend(conditions.iter().map(|c| condition(c, keys.as_ref())));
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
        let Some((low, high)) = self.trace_bounds(&[sql::quote(trace_id)])? else {
            return Ok(Vec::new());
        };
        let mut spans = Vec::new();
        self.table().read(
            &format!(
                "{} AND TraceId = {}",
                time_condition((low, true), (high, true)),
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

    /// The latest-starting traces with a matching span, read newest first in widening
    /// windows, until no trace not yet found could start later than the `want`th found.
    ///
    /// A trace's start is its earliest span, which a matching span is never before. So
    /// once the `want`th latest start among the traces found lies after the earliest
    /// time read so far, every trace still unread starts earlier than all of those, and
    /// reading further cannot change which `want` started last. Starts come from the
    /// trace-id index, clipped to the search's range as the caller's ranking clips them.
    /// The scan's limit still caps how many matching spans are read.
    fn search_traces(
        &self,
        request: Scan<'_>,
        conditions: &[SpanCondition],
        extra: RecordFilter<'_, SpanRecord>,
        want: usize,
    ) -> Result<Vec<String>> {
        let (start, end) = (request.start_nanos, request.end_nanos);
        let Some(selected) = self.select(start, end, conditions)? else {
            let mut ids: Vec<String> = SpanStore::scan(self, request, conditions, extra)?
                .into_iter()
                .map(|span| span.trace_id)
                .collect();
            ids.sort_unstable();
            ids.dedup();
            return Ok(ids);
        };
        if selected.streams.is_empty() || start > end {
            return Ok(Vec::new());
        }
        let keys = self.keys(start, end, &selected, &fields(conditions))?;
        let mut base = vec![selected.condition()];
        base.extend(conditions.iter().map(|c| condition(c, keys.as_ref())));
        let base = base.join(" AND ");
        let table = self.table();
        let cap = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let want = want.max(1);
        let hours = table.hourly_rows(start, end, &selected)?;
        let need = (want as u64).saturating_mul(20).max(2_000);
        let mut width = super::scan::first_window(&hours, Order::Descending, need, end - start);

        // Per trace, `(lower, upper)` bounds on its start: its earliest matching span
        // read bounds it from above, the index's first span, clipped to the range, from
        // below.
        let mut starts: HashMap<String, (u64, u64)> = HashMap::new();
        let mut read = 0usize;
        let mut edge = end;
        loop {
            let low = edge.saturating_sub(width);
            let last = low <= start;
            let window = if last {
                time_condition((start, true), (edge, true))
            } else {
                time_condition((low, false), (edge, true))
            };
            let mut fresh: Vec<String> = Vec::new();
            let mut full = false;
            // Read whole, not to a row limit: the cap counts spans the predicate takes,
            // and a limit on rows would leave the rest of the window — spans the
            // predicate would have taken — unread.
            table.read(
                &format!("{window} AND {base}"),
                Order::Descending,
                None,
                |row: Row| {
                    let span = row.into_span();
                    if span.start_nanos >= start && span.start_nanos <= end && extra(&span) {
                        read += 1;
                        if let Some((lower, upper)) = starts.get_mut(&span.trace_id) {
                            *upper = (*upper).min(span.start_nanos);
                            *lower = (*lower).min(span.start_nanos);
                        } else {
                            starts.insert(
                                span.trace_id.clone(),
                                (span.start_nanos, span.start_nanos),
                            );
                            fresh.push(span.trace_id);
                        }
                        if read >= cap {
                            full = true;
                            return Ok(ControlFlow::Break(()));
                        }
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
            self.lower_bounds(&fresh, &mut starts, start)?;
            let frontier = if last { start } else { low };
            if full || last {
                break;
            }
            if starts.len() >= want && kth_lower(&starts, want) > frontier {
                break;
            }
            edge = low;
            width = width.saturating_mul(4);
        }
        // A trace whose start cannot reach the `want`th surest one is not among the
        // latest `want`, and need not be read whole.
        if starts.len() > want {
            let floor = kth_lower(&starts, want);
            starts.retain(|_, (_, upper)| *upper >= floor);
        }
        Ok(starts.into_keys().collect())
    }

    /// The spans of these traces, each read over the stretch the trace-id index says
    /// it covers: `TraceId = … AND Timestamp BETWEEN …` per trace, a hundred to a
    /// query. A list of ids alone defeats the Bloom filter — five hundred probes pass
    /// nearly every granule — and a range spanning a chunk's traces reads every span in
    /// between.
    ///
    /// The scan's limit holds for all of them together, newest first, as one scan's would.
    fn of_traces(&self, trace_ids: &[&str], request: Scan<'_>) -> Result<Vec<SpanRecord>> {
        let (start, end) = (request.start_nanos, request.end_nanos);
        if trace_ids.is_empty() || start > end {
            return Ok(Vec::new());
        }
        let wanted: std::collections::HashSet<&str> = trace_ids.iter().copied().collect();
        let mut bounds: Vec<(String, u64, u64)> = Vec::new();
        for ids in trace_ids.chunks(500) {
            let quoted: Vec<String> = ids.iter().map(|id| sql::quote(id)).collect();
            self.client.query_lines(
                &format!(
                    "SELECT TraceId AS id, toUnixTimestamp64Nano(min(Start)) AS low, \
                     toUnixTimestamp64Nano(max(End)) AS high FROM {TRACE_IDS} \
                     WHERE TraceId IN ({}) GROUP BY TraceId",
                    quoted.join(", ")
                ),
                |line| {
                    let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
                    let number = |n: &str| value.get(n).and_then(serde_json::Value::as_u64);
                    if let (Some(id), Some(low), Some(high)) = (
                        value.get("id").and_then(serde_json::Value::as_str),
                        number("low"),
                        number("high"),
                    ) {
                        let (low, high) = (low.max(start), high.min(end));
                        if low <= high {
                            bounds.push((id.to_owned(), low, high));
                        }
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
        }
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit
        };
        let newest_first = request.order == Order::Descending;
        let keep = |spans: &mut Vec<SpanRecord>| {
            if spans.len() > limit {
                if newest_first {
                    spans.sort_by_key(|s| std::cmp::Reverse(s.start_nanos));
                } else {
                    spans.sort_by_key(|s| s.start_nanos);
                }
                spans.truncate(limit);
            }
        };
        let mut spans = Vec::new();
        for chunk in bounds.chunks(100) {
            let ranges: Vec<String> = chunk
                .iter()
                .map(|(id, low, high)| {
                    format!(
                        "(TraceId = {} AND {})",
                        sql::quote(id),
                        time_condition((*low, true), (*high, true))
                    )
                })
                .collect();
            self.table().read(
                &ranges.join(" OR "),
                request.order,
                (limit != usize::MAX).then_some(limit),
                |row: Row| {
                    let span = row.into_span();
                    if span.start_nanos >= start
                        && span.start_nanos <= end
                        && wanted.contains(span.trace_id.as_str())
                    {
                        spans.push(span);
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
            keep(&mut spans);
            super::scan::over_ceiling(&request, spans.len())?;
        }
        Ok(spans)
    }
}

/// The aggregate's one query: per step and group — and per histogram bound — what the
/// aggregation needs, over the spans both forms take.
fn aggregate_query(
    request: &SpanAggregateRequest<'_>,
    groups: &[String],
    necessary: &str,
    sufficient: &str,
) -> String {
    let seconds = "(toFloat64(Duration) / 1e9)";
    let (measures, histogram) = match request.aggregation {
        SpanAggregation::Count => ("count() AS n".to_owned(), false),
        SpanAggregation::Duration => (
            format!(
                "count() AS n, sum({seconds}) AS total, min({seconds}) AS low, max({seconds}) AS high"
            ),
            false,
        ),
        SpanAggregation::Quantiles(levels) => (
            format!(
                "count() AS n, quantilesExactInclusive({})({seconds}) AS qs",
                levels
                    .iter()
                    .map(|q| format!("{q:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            false,
        ),
        SpanAggregation::Histogram => ("count() AS n".to_owned(), true),
    };
    let mut select = vec![format!(
        "least(intDiv(toUnixTimestamp64Nano(Timestamp) - {}, {}), {}) AS st",
        request.start_nanos,
        request.step_nanos,
        request.steps - 1
    )];
    let mut keys = vec!["st".to_owned()];
    for (i, expr) in groups.iter().enumerate() {
        select.push(format!("{expr} AS g{i}"));
        keys.push(format!("g{i}"));
    }
    if histogram {
        select.push(format!(
            "if(Duration = 0, 0, pow(2, ceil(log2({seconds})))) AS hb"
        ));
        keys.push("hb".to_owned());
    }
    format!(
        "SELECT {}, {measures} FROM {TRACES} WHERE {necessary} AND ({sufficient}) GROUP BY {}",
        select.join(", "),
        keys.join(", ")
    )
}

/// One row of [`aggregate_query`]'s answer, folded into `out`.
fn absorb_aggregate(
    line: &[u8],
    request: &SpanAggregateRequest<'_>,
    groups: usize,
    index: &mut HashMap<(usize, Vec<Option<String>>), usize>,
    out: &mut SpanAggregates,
) -> Result<()> {
    let value: serde_json::Value = serde_json::from_slice(line).map_err(|e| {
        Error::Unavailable(format!("reading a span aggregate from ClickHouse: {e}"))
    })?;
    let float = |name: &str| {
        value
            .get(name)
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0)
    };
    let step = usize::try_from(
        value
            .get("st")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    )
    .unwrap_or(0);
    let key: Vec<Option<String>> = (0..groups)
        .map(|i| {
            value
                .get(format!("g{i}"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    let count = value
        .get("n")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let next = out.rows.len();
    let slot = *index.entry((step, key.clone())).or_insert(next);
    if slot == next {
        out.rows.push(SpanAggregateRow {
            key,
            step,
            ..SpanAggregateRow::default()
        });
    }
    let row = &mut out.rows[slot];
    row.count += count;
    match request.aggregation {
        SpanAggregation::Duration => {
            row.sum = float("total");
            row.min = float("low");
            row.max = float("high");
        }
        SpanAggregation::Quantiles(_) => {
            row.quantiles = value
                .get("qs")
                .and_then(serde_json::Value::as_array)
                .map(|qs| qs.iter().filter_map(serde_json::Value::as_f64).collect())
                .unwrap_or_default();
        }
        SpanAggregation::Histogram => row.histogram.push((float("hb").to_bits(), count)),
        SpanAggregation::Count => {}
    }
    Ok(())
}

/// One TraceQL condition as a condition ClickHouse selects by — necessary for the span
/// to match, never sufficient.
fn condition(c: &SpanCondition, keys: Option<&Keys>) -> String {
    super::traceql::condition(c, keys).necessary
}

fn fields(conditions: &[SpanCondition]) -> Vec<&SpanField> {
    conditions.iter().map(|c| &c.field).collect()
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
        let status = STATUSES
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

/// The `k`th greatest lower bound on a trace's start, `k` from one.
fn kth_lower(starts: &HashMap<String, (u64, u64)>, k: usize) -> u64 {
    let mut lowers: Vec<u64> = starts.values().map(|(lower, _)| *lower).collect();
    lowers.sort_unstable_by(|a, b| b.cmp(a));
    lowers.get(k.saturating_sub(1)).copied().unwrap_or(0)
}
