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

use std::collections::BTreeMap;
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
        self.client.insert(LOGS, &body)
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

    fn rows(&self, conditions: &str, order: Order, limit: Option<usize>) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        self.read(conditions, order, limit, |row| {
            rows.push(row);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(rows)
    }
}

/// What a scan selects in SQL: the range, the matchers and the line filters.
fn conditions(request: &Scan<'_>, matchers: &[LabelMatcher]) -> String {
    let mut parts = vec![
        format!("Timestamp >= {}", sql::nanos(request.start_nanos)),
        format!("Timestamp <= {}", sql::nanos(request.end_nanos)),
    ];
    parts.extend(
        matchers
            .iter()
            .map(|m| sql::matcher("Stream", "ServiceName", m)),
    );
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
    parts.join(" AND ")
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
        let anchor = i64::try_from(request.anchor_nanos).unwrap_or(i64::MAX);
        let width = i64::try_from(request.width_nanos).unwrap_or(i64::MAX);
        let scan = Scan::range(request.anchor_nanos, request.until_nanos)
            .body_filters(request.body_filters);
        let base = conditions(&scan, request.matchers);
        // `conditions` reads `>=`; a window's floor is exclusive.
        let base = format!(
            "{base} AND Timestamp > {}",
            sql::nanos(request.anchor_nanos)
        );
        let individual = individual_condition(request.group_by);

        let groups: Vec<String> = request
            .group_by
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{} AS g{i}", group_value(name)))
            .collect();
        let keys: Vec<String> = (0..request.group_by.len())
            .map(|i| format!("g{i}"))
            .collect();
        let query = format!(
            "SELECT intDiv(toUnixTimestamp64Nano(Timestamp) - {anchor} - 1, {width}) AS k, \
             count() AS lines, sum(length(Body)) AS bytes{comma}{groups} \
             FROM {LOGS} WHERE {base} AND NOT ({individual}) GROUP BY k{comma}{keys}",
            comma = if groups.is_empty() { "" } else { ", " },
            groups = groups.join(", "),
            keys = keys.join(", "),
        );
        let mut out = Buckets::default();
        let mut index: std::collections::HashMap<Vec<String>, usize> =
            std::collections::HashMap::new();
        self.client.query_lines(&query, |line| {
            let row: serde_json::Value = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!("reading a bucket from ClickHouse: {e}"))
            })?;
            let values: Vec<String> = (0..request.group_by.len())
                .map(|i| {
                    row.get(format!("g{i}"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned()
                })
                .collect();
            let number = |name: &str| {
                row.get(name)
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            };
            let (k, lines, bytes) = (number("k"), number("lines"), number("bytes"));
            out.lines += lines;
            let next = out.groups.len();
            let slot = *index.entry(values.clone()).or_insert(next);
            if slot == next {
                out.groups.push((values, Vec::new()));
            }
            out.groups[slot].1.push((k, lines, bytes));
            Ok(ControlFlow::Continue(()))
        })?;

        self.read(
            &format!("{base} AND ({individual})"),
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
        Ok(Some(out))
    }

    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: RecordFilter<'_, LogRecord>,
    ) -> Result<Vec<LogRecord>> {
        let base = conditions(&request, matchers);
        let accepts = |record: &LogRecord| {
            record.timestamp_nanos >= request.start_nanos
                && record.timestamp_nanos <= request.end_nanos
                && matches_all(matchers, &record.stream)
                && extra(record)
        };
        let mut out = Vec::new();

        if request.limit == 0 {
            let mut failed = None;
            self.read(&base, request.order, None, |row| {
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

        // Keyset pagination on the timestamp. A page that ends partway through one
        // timestamp leaves the rest of it for the next page, which starts at that
        // timestamp again: dropping the last timestamp's rows from this page and asking
        // from it inclusively means rows that share a timestamp are neither skipped nor
        // read twice. A page that is one timestamp throughout reads that timestamp whole.
        let page = page_size(request.limit);
        let mut bound: Option<String> = None;
        loop {
            let conditions = match &bound {
                Some(bound) => format!("{base} AND {bound}"),
                None => base.clone(),
            };
            let mut rows = self.rows(&conditions, request.order, Some(page))?;
            let full = rows.len() == page;
            let mut next = None;
            if full {
                let last = rows.last().map_or(0, |r| r.t);
                let first = rows.first().map_or(0, |r| r.t);
                if first == last {
                    rows = self.rows(
                        &format!("{base} AND Timestamp = {}", sql::nanos(last)),
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
                    over_ceiling(&request, out.len())?;
                    if out.len() >= request.limit {
                        return Ok(out);
                    }
                }
            }
            match next {
                Some((op, at)) if full => {
                    bound = Some(format!("Timestamp {op} {}", sql::nanos(at)));
                }
                _ => return Ok(out),
            }
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
            "SELECT any(Stream) AS Stream FROM {LOG_STREAMS} WHERE {} GROUP BY StreamHash",
            conditions.join(" AND ")
        );
        let mut seen = std::collections::BTreeSet::new();
        self.client.query_lines(&query, |line| {
            #[derive(Deserialize)]
            struct StreamRow {
                #[serde(rename = "Stream")]
                stream: BTreeMap<String, String>,
            }
            let row: StreamRow = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!("reading a stream from ClickHouse: {e}"))
            })?;
            let labels = labels_of(row.stream);
            if !labels.is_empty() && matches_all(matchers, &labels) {
                seen.insert(labels);
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(seen.into_iter().collect())
    }
}

/// A grouped label's value as its stream gives it. `detected_level` is the one label a
/// line carries that is not a stream label but follows from one: it is the record's
/// level, unless the stream names one of its own.
fn group_value(name: &str) -> String {
    if name == "detected_level" {
        "if(mapContains(Stream, 'detected_level'), Stream['detected_level'], Stream['level'])"
            .to_owned()
    } else {
        format!("Stream[{}]", sql::quote(name))
    }
}

/// Rows whose grouped labels cannot be read off `Stream` alone, and so are grouped by
/// the caller, one record at a time.
///
/// A label a line is grouped by comes from its stream unless an attribute stands in for
/// it: one of the record's own whose name sanitises to the label, or an unpromoted
/// resource attribute that does where the stream has no such label (see
/// `LogRecord::filter_labels`). Sanitising is matched loosely here, on letters alone —
/// sanitising never changes an ASCII letter, so a key that sanitises to the label always
/// has its letters — which can only ever send a row the slow way that did not need it.
/// A row with no stream at all, written by something other than telemetryd, goes the
/// same way, since its stream is reconstructed rather than stored.
fn individual_condition(group_by: &[String]) -> String {
    let letters = |text: &str| {
        text.chars()
            .filter(char::is_ascii_alphabetic)
            .collect::<String>()
    };
    let mut parts = vec!["empty(Stream)".to_owned()];
    if !group_by.is_empty() {
        let names: Vec<String> = group_by.iter().map(|n| sql::quote(&letters(n))).collect();
        parts.push(format!(
            "arrayExists(k -> replaceRegexpAll(k, '[^a-zA-Z]', '') IN ({}), mapKeys(LogAttributes))",
            names.join(", ")
        ));
        let resource: Vec<String> = group_by
            .iter()
            .map(|n| {
                format!(
                    "(replaceRegexpAll(k, '[^a-zA-Z]', '') = {} AND NOT mapContains(Stream, {}))",
                    sql::quote(&letters(n)),
                    sql::quote(n)
                )
            })
            .collect();
        parts.push(format!(
            "arrayExists(k -> {}, mapKeys(ResourceAttributes))",
            resource.join(" OR ")
        ));
    }
    parts.join(" OR ")
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
        }
    }
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
