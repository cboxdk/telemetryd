//! Logs in `otel_logs`.
//!
//! # Exactly what the local store would return
//!
//! A record is written as the Collector's columns plus `Stream`, and read back into
//! the same [`LogRecord`] ingest produced:
//!
//! - `ResourceAttributes` is the producer's resource and scope attributes, verbatim.
//! - `LogAttributes` is the record's own attributes, without the resource attributes
//!   ingest merged into them — those are in `ResourceAttributes` already.
//! - On the way back the merge is redone by the rule ingest applies
//!   ([`telemetryd_ingest`'s `keep_unpromoted`]): a resource attribute joins the
//!   record's attributes unless it was promoted into the stream or the record has its
//!   own value under that key.
//!
//! # What runs where
//!
//! ClickHouse selects: the time range, the stream matchers and the `|=`/`!=` line
//! filters become the `WHERE`, and the table's sort key on `ServiceName` is what makes
//! an `app` matcher skip every other app unread. Everything LogQL does after that —
//! parsers, label filters, formatting — runs on the rows that come back, in the same
//! code that runs over the local store, which is what keeps the answers the same. The
//! matchers are checked again on those rows, so a condition ClickHouse evaluates
//! slightly more loosely can only ever cost a row read, never a row lost.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::ControlFlow;

use serde::{Deserialize, Serialize};
use telemetryd_core::record::sanitize_label_name;
use telemetryd_core::{
    APP_LABEL, Error, LEVEL_LABEL, LabelMatcher, Labels, LogRecord, Result, Severity, UNKNOWN_APP,
    matches_all,
};

use super::Client;
use super::schema::{LOG_STREAMS, LOGS};
use super::sql;
use crate::backend::{BucketRequest, Buckets, LogStore, RecordFilter};
use crate::records::Scan;
use crate::topk::Order;

const SELECT: &str = "toUnixTimestamp64Nano(Timestamp) AS t, TraceId, SpanId, SeverityText, \
                      SeverityNumber, ServiceName, Body, ResourceAttributes, LogAttributes, Stream";

/// Rows asked for per round trip when a scan has a limit.
///
/// The record predicate runs here, not in ClickHouse, so a limited scan cannot simply
/// ask for `limit` rows: it reads pages until the predicate has accepted enough. Twice
/// the limit covers the common case of a predicate that accepts most of what the
/// matchers selected in one trip.
fn page_size(limit: usize) -> usize {
    limit.saturating_mul(2).clamp(500, 10_000)
}

/// The log table, behind the same interface as the local store.
#[derive(Debug, Clone)]
pub struct ClickHouseLogs {
    client: Client,
}

impl ClickHouseLogs {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Write records. Stored, and readable by every instance, once this returns.
    ///
    /// # Errors
    /// [`Error::Unavailable`] when ClickHouse did not take them; nothing is half
    /// written, and a sender retries the whole batch.
    pub fn append(&self, records: &[LogRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut body = Vec::with_capacity(records.len() * 512);
        for record in records {
            serde_json::to_writer(&mut body, &WriteRow::from(record))
                .map_err(|e| Error::Unavailable(format!("encoding a log row: {e}")))?;
            body.push(b'\n');
        }
        self.client.insert(LOGS, &body)?;
        self.client.stats().count_rows(records.len());
        Ok(())
    }

    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    fn read(
        &self,
        conditions: &str,
        order: Order,
        limit: Option<usize>,
        mut each: impl FnMut(Row) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let direction = match order {
            Order::Descending => "DESC",
            Order::Ascending => "ASC",
        };
        let mut query = format!(
            "SELECT {SELECT} FROM {LOGS} WHERE {conditions} ORDER BY Timestamp {direction}"
        );
        if let Some(limit) = limit {
            query = format!("{query} LIMIT {limit}");
        }
        self.client.query_lines(&query, |line| {
            let row: Row = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!(
                    "ClickHouse returned a log row this cannot read: {e}"
                ))
            })?;
            each(row)
        })
    }

    /// Accept rows from one window into `out`, newest or oldest first, until it holds
    /// the scan's limit; `true` once it does.
    ///
    /// Keyset pagination on the timestamp. A page that ends partway through one timestamp
    /// leaves the rest of it for the next page, which starts at that timestamp again:
    /// dropping the last timestamp's rows from this page and asking from it inclusively
    /// means rows that share a timestamp are neither skipped nor read twice. A page that
    /// is one timestamp throughout reads that timestamp whole.
    fn read_window(
        &self,
        window: &str,
        request: &Scan<'_>,
        accepts: &dyn Fn(&LogRecord) -> bool,
        out: &mut Vec<LogRecord>,
    ) -> Result<bool> {
        let page = page_size(request.limit);
        let mut bound: Option<String> = None;
        loop {
            let conditions = match &bound {
                Some(bound) => format!("{window} AND {bound}"),
                None => window.to_owned(),
            };
            let mut rows = self.rows(&conditions, request.order, Some(page))?;
            let full = rows.len() == page;
            let mut next = None;
            if full {
                let last = rows.last().map_or(0, |r| r.t);
                let first = rows.first().map_or(0, |r| r.t);
                if first == last {
                    rows = self.rows(
                        &format!("{window} AND Timestamp = {}", sql::nanos(last)),
                        request.order,
                        None,
                    )?;
                    next = match request.order {
                        Order::Descending => last.checked_sub(1).map(|t| ("<=", t)),
                        Order::Ascending => last.checked_add(1).map(|t| (">=", t)),
                    };
                } else {
                    rows.retain(|r| r.t != last);
                    next = Some((
                        match request.order {
                            Order::Descending => "<=",
                            Order::Ascending => ">=",
                        },
                        last,
                    ));
                }
            }
            for row in rows {
                let record = row.into_record();
                if accepts(&record) {
                    out.push(record);
                    over_ceiling(request, out.len())?;
                    if out.len() >= request.limit {
                        return Ok(true);
                    }
                }
            }
            match next {
                Some((op, at)) if full => {
                    bound = Some(format!("Timestamp {op} {}", sql::nanos(at)));
                }
                _ => return Ok(false),
            }
        }
    }

    fn rows(&self, conditions: &str, order: Order, limit: Option<usize>) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        self.read(conditions, order, limit, |row| {
            rows.push(row);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(rows)
    }
}

/// More streams than this and listing their hashes stops paying: the matchers go into
/// the query as conditions on the stream map instead.
const MAX_LISTED_STREAMS: usize = 10_000;

/// Candidate rows a limited scan's first window is sized to hold, per row it needs.
const WINDOW_ROWS_PER_RESULT: u64 = 4;

/// The streams a query selects, resolved against `otel_log_streams` by the same
/// matcher code every store uses — exactly, so what reaches `otel_logs` is a list of
/// hashes rather than conditions on a map.
struct Selected {
    streams: HashMap<u64, Labels>,
    services: BTreeSet<String>,
}

impl Selected {
    fn condition(&self) -> String {
        if self.streams.is_empty() {
            return "0".to_owned();
        }
        let services: Vec<String> = self.services.iter().map(|s| sql::quote(s)).collect();
        let hashes: Vec<String> = self.streams.keys().map(u64::to_string).collect();
        format!(
            "ServiceName IN ({}) AND StreamHash IN ({})",
            services.join(", "),
            hashes.join(", ")
        )
    }
}

impl ClickHouseLogs {
    /// The streams with data in the range that the matchers select, or `None` for more
    /// than [`MAX_LISTED_STREAMS`] of them.
    fn select(&self, start: u64, end: u64, matchers: &[LabelMatcher]) -> Result<Option<Selected>> {
        let mut conditions = vec![stream_range(start, end)];
        conditions.extend(
            matchers
                .iter()
                .map(|m| sql::matcher("Stream", "ServiceName", m)),
        );
        let query = format!(
            "SELECT StreamHash AS hash, any(Stream) AS labels, any(ServiceName) AS service \
             FROM {LOG_STREAMS} WHERE {} GROUP BY StreamHash LIMIT {}",
            conditions.join(" AND "),
            MAX_LISTED_STREAMS + 1
        );
        let mut selected = Selected {
            streams: HashMap::new(),
            services: BTreeSet::new(),
        };
        let mut listed = 0usize;
        self.client.query_lines(&query, |line| {
            #[derive(Deserialize)]
            struct StreamRow {
                hash: u64,
                labels: BTreeMap<String, String>,
                service: String,
            }
            let row: StreamRow = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!("reading a stream from ClickHouse: {e}"))
            })?;
            listed += 1;
            let labels = labels_of(row.labels);
            if !labels.is_empty() && matches_all(matchers, &labels) {
                selected.streams.insert(row.hash, labels);
                selected.services.insert(row.service);
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok((listed <= MAX_LISTED_STREAMS).then_some(selected))
    }

    /// Rows the selected streams hold per hour of the range, as `(hour, rows)`.
    fn hourly_rows(&self, start: u64, end: u64, selected: &Selected) -> Result<Vec<(u64, u64)>> {
        let query = format!(
            "SELECT toUInt64(toUnixTimestamp(Hour)) AS hour, sum(Rows) AS rows \
             FROM {LOG_STREAMS} WHERE {} AND {} GROUP BY hour ORDER BY hour",
            stream_range(start, end),
            selected.condition()
        );
        let mut hours = Vec::new();
        self.client.query_lines(&query, |line| {
            let value: serde_json::Value = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!("reading hourly rows from ClickHouse: {e}"))
            })?;
            let number = |name: &str| {
                value
                    .get(name)
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            };
            hours.push((number("hour").saturating_mul(1_000_000_000), number("rows")));
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(hours)
    }
}

/// How the stream matchers reach `otel_logs`: as hashes, or as map conditions.
fn stream_condition(selected: Option<&Selected>, matchers: &[LabelMatcher]) -> String {
    match selected {
        Some(selected) => selected.condition(),
        None if matchers.is_empty() => "1".to_owned(),
        None => matchers
            .iter()
            .map(|m| sql::matcher("Stream", "ServiceName", m))
            .collect::<Vec<_>>()
            .join(" AND "),
    }
}

/// The line filters as conditions on the body.
fn body_conditions(request: &Scan<'_>) -> Vec<String> {
    let mut parts = Vec::new();
    for filter in request.body_filters.iter().filter(|f| !f.text.is_empty()) {
        let not = if filter.contains { "" } else { "NOT " };
        parts.push(format!(
            "Body {not}LIKE {}",
            sql::like_contains(&filter.text)
        ));
    }
    if let Some(text) = request.required_text.filter(|t| {
        !t.is_empty()
            && !request
                .body_filters
                .iter()
                .any(|f| f.contains && f.text == *t)
    }) {
        parts.push(format!("Body LIKE {}", sql::like_contains(text)));
    }
    parts
}

/// `Timestamp` between two bounds, each inclusive or not.
fn time_condition(
    (low, low_inclusive): (u64, bool),
    (high, high_inclusive): (u64, bool),
) -> String {
    format!(
        "Timestamp {} {} AND Timestamp {} {}",
        if low_inclusive { ">=" } else { ">" },
        sql::nanos(low),
        if high_inclusive { "<=" } else { "<" },
        sql::nanos(high),
    )
}

/// The span a limited scan's first window covers, from how densely the selected streams
/// wrote at the end it starts from: wide enough to hold the candidates `need` rows ask
/// for, if the rate held, and never narrower than a second.
fn first_window(hours: &[(u64, u64)], order: Order, need: u64, span: u64) -> u64 {
    const HOUR: u64 = 3_600_000_000_000;
    let busiest_end = match order {
        Order::Descending => hours.iter().rev().find(|(_, rows)| *rows > 0),
        Order::Ascending => hours.iter().find(|(_, rows)| *rows > 0),
    };
    let window = busiest_end.map_or(60_000_000_000, |(_, rows)| {
        u64::try_from(u128::from(need) * u128::from(HOUR) / u128::from(*rows)).unwrap_or(u64::MAX)
    });
    // Not `clamp`: a range shorter than a second would make its bounds cross.
    window.max(1_000_000_000).min(span.max(1))
}

fn over_ceiling(request: &Scan<'_>, collected: usize) -> Result<()> {
    if request.abort_over != 0 && collected > request.abort_over {
        return Err(Error::BadRequest(format!(
            "this query matched more than {} records, which is more than one query is \
             allowed to hold at once",
            request.abort_over
        )));
    }
    Ok(())
}

impl LogStore for ClickHouseLogs {
    fn buckets(&self, request: &BucketRequest<'_>) -> Result<Option<Buckets>> {
        if request.width_nanos == 0 {
            return Ok(None);
        }
        let Some(selected) =
            self.select(request.anchor_nanos, request.until_nanos, request.matchers)?
        else {
            return Ok(None);
        };
        let mut out = Buckets::default();
        if selected.streams.is_empty() {
            return Ok(Some(out));
        }
        let anchor = i64::try_from(request.anchor_nanos).unwrap_or(i64::MAX);
        let width = i64::try_from(request.width_nanos).unwrap_or(i64::MAX);
        let scan = Scan::range(request.anchor_nanos, request.until_nanos)
            .body_filters(request.body_filters);
        let mut base = vec![
            time_condition((request.anchor_nanos, false), (request.until_nanos, true)),
            selected.condition(),
        ];
        base.extend(body_conditions(&scan));
        let base = base.join(" AND ");
        // Rows an attribute could regroup are read whole and grouped by the caller.
        let individual = if request.group_by.is_empty() {
            None
        } else {
            let names: Vec<String> = request.group_by.iter().map(|n| sql::quote(n)).collect();
            Some(format!("hasAny(AttributeLabels, [{}])", names.join(", ")))
        };
        let bytes = if request.bytes {
            "sum(length(Body))"
        } else {
            "0"
        };
        let query = format!(
            "SELECT intDiv(toUnixTimestamp64Nano(Timestamp) - {anchor} - 1, {width}) AS k, \
             StreamHash AS s, count() AS lines, {bytes} AS bytes \
             FROM {LOGS} WHERE {base}{not_individual} GROUP BY k, s",
            not_individual = individual
                .as_ref()
                .map_or(String::new(), |c| format!(" AND NOT {c}")),
        );
        let mut index: HashMap<Vec<String>, usize> = HashMap::new();
        self.client.query_lines(&query, |line| {
            let row: serde_json::Value = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!("reading a bucket from ClickHouse: {e}"))
            })?;
            let number = |name: &str| {
                row.get(name)
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            };
            let (k, lines, bytes) = (number("k"), number("lines"), number("bytes"));
            let Some(stream) = selected.streams.get(&number("s")) else {
                return Ok(ControlFlow::Continue(()));
            };
            let values: Vec<String> = request
                .group_by
                .iter()
                .map(|name| group_value(stream, name).to_owned())
                .collect();
            out.lines += lines;
            let next = out.groups.len();
            let slot = *index.entry(values.clone()).or_insert(next);
            if slot == next {
                out.groups.push((values, Vec::new()));
            }
            out.groups[slot].1.push((k, lines, bytes));
            Ok(ControlFlow::Continue(()))
        })?;

        if let Some(individual) = individual {
            self.read(
                &format!("{base} AND {individual}"),
                Order::Ascending,
                None,
                |row| {
                    let record = row.into_record();
                    if matches_all(request.matchers, &record.stream) {
                        out.lines += 1;
                        out.individual.push(record);
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )?;
        }
        Ok(Some(out))
    }

    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: RecordFilter<'_, LogRecord>,
    ) -> Result<Vec<LogRecord>> {
        let (start, end) = (request.start_nanos, request.end_nanos);
        let mut out = Vec::new();
        if start > end {
            return Ok(out);
        }
        let selected = self.select(start, end, matchers)?;
        if selected.as_ref().is_some_and(|s| s.streams.is_empty()) {
            return Ok(out);
        }
        let mut base = vec![stream_condition(selected.as_ref(), matchers)];
        base.extend(body_conditions(&request));
        let base = base.join(" AND ");
        let accepts = |record: &LogRecord| {
            record.timestamp_nanos >= start
                && record.timestamp_nanos <= end
                && matches_all(matchers, &record.stream)
                && extra(record)
        };

        if request.limit == 0 {
            let mut failed = None;
            let conditions = format!("{} AND {base}", time_condition((start, true), (end, true)));
            self.read(&conditions, request.order, None, |row| {
                let record = row.into_record();
                if accepts(&record) {
                    out.push(record);
                    if let Err(error) = over_ceiling(&request, out.len()) {
                        failed = Some(error);
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            })?;
            return failed.map_or(Ok(out), Err);
        }

        // Windows from the end the scan starts at, each four times the last. The table
        // is sorted by time within an app, so a window costs what it holds — the newest
        // hundred lines of a day need the last few seconds read, not the day. The first
        // is sized from how densely the selected streams wrote there.
        let hours = match &selected {
            Some(selected) => self.hourly_rows(start, end, selected)?,
            None => Vec::new(),
        };
        let need = (request.limit as u64)
            .saturating_mul(WINDOW_ROWS_PER_RESULT)
            .max(1_000);
        let mut width = first_window(&hours, request.order, need, end - start);
        let mut edge = match request.order {
            Order::Descending => end,
            Order::Ascending => start,
        };
        loop {
            let (window, last) = match request.order {
                Order::Descending => {
                    let low = edge.saturating_sub(width);
                    if low <= start {
                        (time_condition((start, true), (edge, true)), true)
                    } else {
                        (time_condition((low, false), (edge, true)), false)
                    }
                }
                Order::Ascending => {
                    let high = edge.saturating_add(width);
                    if high >= end {
                        (time_condition((edge, true), (end, true)), true)
                    } else {
                        (time_condition((edge, true), (high, false)), false)
                    }
                }
            };
            if self.read_window(
                &format!("{window} AND {base}"),
                &request,
                &accepts,
                &mut out,
            )? {
                return Ok(out);
            }
            if last {
                return Ok(out);
            }
            edge = match request.order {
                Order::Descending => edge.saturating_sub(width),
                Order::Ascending => edge.saturating_add(width),
            };
            width = width.saturating_mul(4);
        }
    }

    fn label_names(&self, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>> {
        let query = format!(
            "SELECT DISTINCT arrayJoin(mapKeys(Stream)) AS name FROM {LOG_STREAMS} \
             WHERE {} ORDER BY name",
            stream_range(start_nanos, end_nanos)
        );
        let mut names = Vec::new();
        self.client.query_lines(&query, |line| {
            names.push(field(line, "name")?);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(names)
    }

    fn label_values(&self, name: &str, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>> {
        let query = format!(
            "SELECT DISTINCT Stream[{key}] AS value FROM {LOG_STREAMS} \
             WHERE {range} AND mapContains(Stream, {key}) ORDER BY value",
            key = sql::quote(name),
            range = stream_range(start_nanos, end_nanos)
        );
        let mut values = Vec::new();
        self.client.query_lines(&query, |line| {
            values.push(field(line, "value")?);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(values)
    }

    fn streams(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>> {
        let mut conditions = vec![stream_range(start_nanos, end_nanos)];
        conditions.extend(
            matchers
                .iter()
                .map(|m| sql::matcher("Stream", "ServiceName", m)),
        );
        let query = format!(
            "SELECT any(Stream) AS labels FROM {LOG_STREAMS} WHERE {} GROUP BY StreamHash",
            conditions.join(" AND ")
        );
        let mut seen = std::collections::BTreeSet::new();
        self.client.query_lines(&query, |line| {
            #[derive(Deserialize)]
            struct StreamRow {
                labels: BTreeMap<String, String>,
            }
            let row: StreamRow = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!("reading a stream from ClickHouse: {e}"))
            })?;
            let labels = labels_of(row.labels);
            if !labels.is_empty() && matches_all(matchers, &labels) {
                seen.insert(labels);
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(seen.into_iter().collect())
    }
}

/// A grouped label's value as a stream gives it. `detected_level` is the one label a
/// line carries that is not a stream label but follows from one: it is the record's
/// level, unless the stream names one of its own.
fn group_value<'a>(stream: &'a Labels, name: &str) -> &'a str {
    if name == "detected_level" {
        stream
            .get("detected_level")
            .or_else(|| stream.get(LEVEL_LABEL))
            .unwrap_or_default()
    } else {
        stream.get(name).unwrap_or_default()
    }
}

/// The streams summary's rows that can hold data in the range. Each row records the
/// first and last timestamp it covers, so this is exact to the stream, not the hour.
fn stream_range(start_nanos: u64, end_nanos: u64) -> String {
    format!(
        "Hour >= toStartOfHour({start}) AND Hour <= {end} \
         AND MaxTimestamp >= {start} AND MinTimestamp <= {end}",
        start = sql::nanos(start_nanos),
        end = sql::nanos(end_nanos),
    )
}

fn field(line: &[u8], name: &str) -> Result<String> {
    let value: serde_json::Value = serde_json::from_slice(line)
        .map_err(|e| Error::Unavailable(format!("reading {name} from ClickHouse: {e}")))?;
    Ok(value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned())
}

fn labels_of(map: BTreeMap<String, String>) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in map {
        labels.insert(name, value);
    }
    labels
}

/// The OTLP severity number for a normalised level: the first of its range.
fn severity_number(severity: Severity) -> i32 {
    match severity {
        Severity::Trace => 1,
        Severity::Debug => 5,
        Severity::Info => 9,
        Severity::Warn => 13,
        Severity::Error => 17,
        Severity::Fatal => 21,
        Severity::Unknown => 0,
    }
}

/// One row as written: borrowed from the record, nothing copied but the split maps.
#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct WriteRow<'a> {
    timestamp: u64,
    trace_id: &'a str,
    span_id: &'a str,
    severity_text: &'a str,
    severity_number: i32,
    service_name: &'a str,
    body: &'a str,
    resource_attributes: &'a Labels,
    log_attributes: OwnAttributes<'a>,
    stream: &'a Labels,
    attribute_labels: Vec<String>,
}

impl<'a> From<&'a LogRecord> for WriteRow<'a> {
    fn from(record: &'a LogRecord) -> Self {
        Self {
            timestamp: record.timestamp_nanos,
            trace_id: record.trace_id.as_deref().unwrap_or_default(),
            span_id: record.span_id.as_deref().unwrap_or_default(),
            severity_text: &record.severity_text,
            severity_number: severity_number(record.severity),
            service_name: record.stream.get(APP_LABEL).unwrap_or(UNKNOWN_APP),
            body: &record.body,
            resource_attributes: &record.resource,
            log_attributes: OwnAttributes(record),
            stream: &record.stream,
            attribute_labels: attribute_labels(record),
        }
    }
}

/// Every label name the record's attributes give it, as `LogRecord::filter_labels`
/// does: each attribute under its own name and its sanitised one.
fn attribute_labels(record: &LogRecord) -> Vec<String> {
    let mut names = BTreeSet::new();
    for (name, _) in record.attributes.iter() {
        names.insert(name.to_owned());
        names.insert(sanitize_label_name(name));
    }
    names.into_iter().collect()
}

/// The record's attributes without the resource attributes ingest merged into them.
///
/// An attribute equal to the resource's value under the same key is dropped; on the
/// way back the merge puts it back. One the record set differently stays — it is the
/// record's own, and on the way back it wins over the resource's, as at ingest.
struct OwnAttributes<'a>(&'a LogRecord);

impl Serialize for OwnAttributes<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let record = self.0;
        serializer.collect_map(
            record
                .attributes
                .iter()
                .filter(|(name, value)| record.resource.get(name) != Some(*value)),
        )
    }
}

/// One row as read.
#[derive(Deserialize)]
struct Row {
    t: u64,
    #[serde(rename = "TraceId")]
    trace_id: String,
    #[serde(rename = "SpanId")]
    span_id: String,
    #[serde(rename = "SeverityText")]
    severity_text: String,
    #[serde(rename = "SeverityNumber")]
    severity_number: i32,
    #[serde(rename = "ServiceName")]
    service_name: String,
    #[serde(rename = "Body")]
    body: String,
    #[serde(rename = "ResourceAttributes")]
    resource_attributes: BTreeMap<String, String>,
    #[serde(rename = "LogAttributes")]
    log_attributes: BTreeMap<String, String>,
    #[serde(rename = "Stream", default)]
    stream: BTreeMap<String, String>,
}

impl Row {
    fn into_record(self) -> LogRecord {
        let severity = match Severity::from_otlp_number(self.severity_number) {
            Severity::Unknown => Severity::from_text(&self.severity_text),
            known => known,
        };
        // A row another writer put here — the store package's own ingest — has no
        // stream column. `app` and `level` are what every stream has, and all such a
        // row can be given without guessing at promotions.
        let stream = if self.stream.is_empty() {
            let mut stream = Labels::new();
            let app = if self.service_name.is_empty() {
                UNKNOWN_APP.to_owned()
            } else {
                self.service_name
            };
            stream.insert(APP_LABEL, app);
            stream.insert(LEVEL_LABEL, severity.as_str());
            stream
        } else {
            labels_of(self.stream)
        };
        let resource = labels_of(self.resource_attributes);
        let mut attributes = labels_of(self.log_attributes);
        for (name, value) in resource.iter() {
            if stream.get(&sanitize_label_name(name)).is_some() || attributes.get(name).is_some() {
                continue;
            }
            attributes.insert(name.to_owned(), value.to_owned());
        }
        LogRecord {
            timestamp_nanos: self.t,
            stream,
            severity,
            severity_text: self.severity_text,
            body: self.body,
            attributes,
            trace_id: Some(self.trace_id).filter(|id| !id.is_empty()),
            span_id: Some(self.span_id).filter(|id| !id.is_empty()),
            resource,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3_600_000_000_000;

    #[test]
    fn the_first_window_holds_what_the_last_hour_wrote_at_its_rate() {
        // 3.6 million rows an hour is a thousand a second: 4,000 rows is four seconds.
        let hours = [(0, 7), (HOUR, 3_600_000)];
        assert_eq!(
            first_window(&hours, Order::Descending, 4_000, 2 * HOUR),
            4_000_000_000
        );
        // From the other end the first hour is the one that counts.
        assert_eq!(first_window(&hours, Order::Ascending, 7, 2 * HOUR), HOUR);
    }

    #[test]
    fn the_first_window_never_exceeds_the_range_even_under_a_second() {
        assert_eq!(first_window(&[], Order::Descending, 100, 0), 1);
        assert_eq!(first_window(&[(0, 1)], Order::Descending, 100, 500), 500);
    }
}
