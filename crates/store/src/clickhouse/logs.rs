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
use super::scan::{Table, Timed, field, labels_of, stream_condition, stream_range, time_condition};
use super::schema::{LOG_STREAMS, LOG_VOLUME, LOG_VOLUME_MINUTES, LOGS};
use super::sql;
use crate::backend::{BucketRequest, Buckets, LogStore, RecordFilter};
use crate::records::Scan;
use crate::topk::Order;

const SELECT: &str = "toUnixTimestamp64Nano(Timestamp) AS t, TraceId, SpanId, SeverityText, \
                      SeverityNumber, ServiceName, Body, ResourceAttributes, LogAttributes, Stream";

/// The log table, behind the same interface as the local store.
#[derive(Debug, Clone)]
pub struct ClickHouseLogs {
    client: Client,
    /// From when `otel_log_volume` holds every line; `None` without one.
    volume_since: Option<u64>,
    /// The same for `otel_log_volume_1m`.
    minutes_since: Option<u64>,
}

impl ClickHouseLogs {
    #[must_use]
    pub fn new(client: Client) -> Self {
        // A database whose schema someone else manages may have no rollup, and then
        // every count reads the lines.
        let since = |name| super::schema::rollup_start(&client, name).ok().flatten();
        let volume_since = since(super::schema::LOG_VOLUME_SINCE);
        let minutes_since = since(super::schema::LOG_VOLUME_MINUTES_SINCE);
        Self {
            client,
            volume_since,
            minutes_since,
        }
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

    fn table(&self) -> Table<'_> {
        Table {
            client: &self.client,
            name: LOGS,
            streams: LOG_STREAMS,
            columns: SELECT,
        }
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

impl LogStore for ClickHouseLogs {
    fn buckets(&self, request: &BucketRequest<'_>) -> Result<Option<Buckets>> {
        if request.width_nanos == 0 {
            return Ok(None);
        }
        let table = self.table();
        let Some(selected) =
            table.select(request.anchor_nanos, request.until_nanos, request.matchers)?
        else {
            return Ok(None);
        };
        let mut grouper = Grouper::new(&selected, request.group_by);
        if selected.streams.is_empty() {
            return Ok(Some(grouper.out));
        }
        let (anchor, width) = (request.anchor_nanos, request.width_nanos);
        let scan = Scan::range(anchor, request.until_nanos).body_filters(request.body_filters);
        let mut base = vec![selected.condition()];
        base.extend(body_conditions(&scan));
        let base = base.join(" AND ");
        // Lines an attribute could regroup are read whole and grouped by the caller.
        let individual = (!request.group_by.is_empty()).then(|| {
            let names: Vec<String> = request.group_by.iter().map(|n| sql::quote(n)).collect();
            format!("hasAny(AttributeLabels, [{}])", names.join(", "))
        });
        let not_individual = individual
            .as_ref()
            .map_or(String::new(), |c| format!(" AND NOT {c}"));
        let whole_range = time_condition((anchor, false), (request.until_nanos, true));

        let plan = self.rollup_plan(request);
        // What is counted from the lines: everything, or only the seconds a bucket
        // bound splits when the rollups have the rest.
        let from_lines = plan.as_ref().map_or_else(
            || vec![whole_range.clone()],
            |plan| plan.split_conditions(anchor, request.until_nanos),
        );
        let bytes = if request.bytes {
            "sum(length(Body))"
        } else {
            "0"
        };
        for time in &from_lines {
            let query = format!(
                "SELECT intDiv(toUnixTimestamp64Nano(Timestamp) - {anchor} - 1, {width}) AS k, \
                 StreamHash AS s, count() AS lines, {bytes} AS bytes \
                 FROM {LOGS} WHERE ({time}) AND {base}{not_individual} GROUP BY k, s",
            );
            self.client
                .query_lines(&query, |line| grouper.absorb(line))?;
        }

        // Lines to group one by one: over the whole range if a rollup says some are in
        // the stretch it covers, otherwise only in the split seconds, which none covers.
        let individual_times = match (&plan, &individual) {
            (_, None) => Vec::new(),
            (None, Some(_)) => vec![whole_range.clone()],
            (Some(plan), Some(individual)) => {
                if self.count_from_rollups(
                    plan,
                    (anchor, width),
                    &selected,
                    Some(individual),
                    &mut grouper,
                )? {
                    vec![whole_range.clone()]
                } else {
                    from_lines
                }
            }
        };
        if individual.is_none()
            && let Some(plan) = &plan
        {
            self.count_from_rollups(plan, (anchor, width), &selected, None, &mut grouper)?;
        }

        let mut out = grouper.out;
        if let Some(individual) = &individual {
            for time in &individual_times {
                table.read(
                    &format!("({time}) AND {base} AND {individual}"),
                    Order::Ascending,
                    None,
                    |row: Row| {
                        let record = row.into_record();
                        if matches_all(request.matchers, &record.stream) {
                            out.lines += 1;
                            out.individual.push(record);
                        }
                        Ok(ControlFlow::Continue(()))
                    },
                )?;
            }
        }
        Ok(Some(out))
    }

    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: RecordFilter<'_, LogRecord>,
    ) -> Result<Vec<LogRecord>> {
        let table = self.table();
        let selected = table.select(request.start_nanos, request.end_nanos, matchers)?;
        let mut base = vec![stream_condition(selected.as_ref(), matchers)];
        base.extend(body_conditions(&request));
        let (start, end) = (request.start_nanos, request.end_nanos);
        table.scan(
            &request,
            &base.join(" AND "),
            selected.as_ref(),
            &Row::into_record,
            &|record: &LogRecord| {
                record.timestamp_nanos >= start
                    && record.timestamp_nanos <= end
                    && matches_all(matchers, &record.stream)
                    && extra(record)
            },
        )
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

const SECOND: u64 = 1_000_000_000;
const MINUTE: u64 = 60 * SECOND;

/// Ranges or split units per query: each is a condition of its own, and ClickHouse
/// refuses a query text past a quarter of a megabyte.
const PER_QUERY: usize = 500;

/// One rollup's part of a count: which of its units, by index, it answers.
struct Tier {
    table: &'static str,
    /// `Second` or `Minute`: the column, and what a unit is.
    unit: &'static str,
    nanos: u64,
    /// Inclusive ranges of unit indexes.
    ranges: Vec<(u64, u64)>,
    /// Units inside the ranges that a bucket bound splits, left to a finer tier.
    excluded: Vec<u64>,
}

impl Tier {
    /// The tier's units as conditions, a few hundred ranges to a query.
    fn conditions(&self) -> Vec<String> {
        self.ranges
            .chunks(PER_QUERY)
            .map(|ranges| {
                let (low, high) = (ranges[0].0, ranges[ranges.len() - 1].1);
                let within: Vec<String> = ranges
                    .iter()
                    .map(|(a, b)| format!("({unit} >= {a} AND {unit} <= {b})", unit = self.unit))
                    .collect();
                let excluded: Vec<String> = self
                    .excluded
                    .iter()
                    .filter(|u| (low..=high).contains(*u))
                    .map(u64::to_string)
                    .collect();
                let mut condition = format!("({})", within.join(" OR "));
                if !excluded.is_empty() {
                    condition = format!(
                        "{condition} AND {} NOT IN ({})",
                        self.unit,
                        excluded.join(", ")
                    );
                }
                condition
            })
            .collect()
    }
}

/// How a bucketed count with no line filter is answered.
///
/// A unit — `(s, s + 1]` of a second, `(m, m + 60]` of a minute — lies wholly in one
/// bucket unless a bucket bound falls strictly inside it. Whole minutes are counted from
/// the minute rollup; the whole seconds of minutes a bound splits, from the second
/// rollup; the seconds a bound splits, from the lines, whose timestamps put each line on
/// its side of the bound. With bounds on whole minutes the lines are not read at all.
struct RollupPlan {
    tiers: Vec<Tier>,
    split_seconds: Vec<u64>,
}

impl RollupPlan {
    /// The split seconds as conditions on the lines, clipped to the range.
    fn split_conditions(&self, anchor: u64, until: u64) -> Vec<String> {
        self.split_seconds
            .chunks(PER_QUERY)
            .map(|seconds| {
                seconds
                    .iter()
                    .map(|&second| {
                        let low = (second * SECOND).max(anchor);
                        let high = ((second + 1) * SECOND).min(until);
                        format!("({})", time_condition((low, false), (high, true)))
                    })
                    .collect::<Vec<_>>()
                    .join(" OR ")
            })
            .collect()
    }
}

/// Bucket counts, keyed by stream hash, folded into groups by their labels' values.
struct Grouper<'a> {
    selected: &'a super::scan::Selected,
    group_by: &'a [String],
    index: HashMap<Vec<String>, usize>,
    out: Buckets,
}

impl<'a> Grouper<'a> {
    fn new(selected: &'a super::scan::Selected, group_by: &'a [String]) -> Self {
        Self {
            selected,
            group_by,
            index: HashMap::new(),
            out: Buckets::default(),
        }
    }

    /// One `{k, s, lines, bytes}` row.
    fn absorb(&mut self, line: &[u8]) -> Result<ControlFlow<()>> {
        let row: serde_json::Value = serde_json::from_slice(line)
            .map_err(|e| Error::Unavailable(format!("reading a bucket from ClickHouse: {e}")))?;
        let number = |name: &str| {
            row.get(name)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        let (k, lines, bytes) = (number("k"), number("lines"), number("bytes"));
        let Some(stream) = self.selected.streams.get(&number("s")) else {
            return Ok(ControlFlow::Continue(()));
        };
        let values: Vec<String> = self
            .group_by
            .iter()
            .map(|name| group_value(stream, name).to_owned())
            .collect();
        self.out.lines += lines;
        let next = self.out.groups.len();
        let slot = *self.index.entry(values.clone()).or_insert(next);
        if slot == next {
            self.out.groups.push((values, Vec::new()));
        }
        self.out.groups[slot].1.push((k, lines, bytes));
        Ok(ControlFlow::Continue(()))
    }
}

impl ClickHouseLogs {
    /// Count each tier's units from its rollup, all but the lines `individual`
    /// selects; `true` if any of those is in a stretch a rollup covered.
    fn count_from_rollups(
        &self,
        plan: &RollupPlan,
        (anchor, width): (u64, u64),
        selected: &super::scan::Selected,
        individual: Option<&str>,
        grouper: &mut Grouper<'_>,
    ) -> Result<bool> {
        let not_individual = individual.map_or(String::new(), |c| format!(" AND NOT {c}"));
        let mut any = false;
        for tier in &plan.tiers {
            for units in tier.conditions() {
                let base = format!("{} AND {units}", selected.condition());
                let query = format!(
                    "SELECT intDiv((toUInt64({unit}) + 1) * {nanos} - {anchor} - 1, {width}) AS k, \
                     StreamHash AS s, sum(Lines) AS lines, sum(Bytes) AS bytes \
                     FROM {table} WHERE {base}{not_individual} GROUP BY k, s",
                    unit = tier.unit,
                    nanos = tier.nanos,
                    table = tier.table,
                );
                self.client
                    .query_lines(&query, |line| grouper.absorb(line))?;
                if let (Some(individual), false) = (individual, any) {
                    self.client.query_lines(
                        &format!(
                            "SELECT 1 AS one FROM {} WHERE {base} AND {individual} LIMIT 1",
                            tier.table
                        ),
                        |_| {
                            any = true;
                            Ok(ControlFlow::Break(()))
                        },
                    )?;
                }
            }
        }
        Ok(any)
    }

    /// Which rollups can count this request, and for which units.
    ///
    /// Only with no line filter — a rollup counts every line — and only over a range the
    /// rollups held every line of. The minute rollup only helps buckets of a minute or
    /// more; under that nearly every minute has a bound inside it.
    fn rollup_plan(&self, request: &BucketRequest<'_>) -> Option<RollupPlan> {
        let (anchor, until, width) = (
            request.anchor_nanos,
            request.until_nanos,
            request.width_nanos,
        );
        let seconds_since = self.volume_since?;
        if !request.body_filters.is_empty() || anchor < seconds_since || width == 0 {
            return None;
        }
        let first_second = anchor.div_ceil(SECOND);
        let last_second = (until / SECOND).checked_sub(1)?;
        if first_second > last_second {
            return None;
        }
        let buckets = (until - anchor) / width;
        let edges = || (0..=buckets).map(|k| anchor + k * width);
        let split_units = |unit: u64| -> Vec<u64> {
            let mut split: Vec<u64> = edges()
                .filter(|edge| edge % unit != 0)
                .map(|edge| edge / unit)
                .collect();
            split.dedup();
            split
        };
        let split_seconds = split_units(SECOND);

        let minutes = self
            .minutes_since
            .is_some_and(|since| since <= anchor && width >= MINUTE);
        let mut tiers = Vec::new();
        if minutes {
            let split_minutes = split_units(MINUTE);
            let first_minute = anchor.div_ceil(MINUTE);
            if let Some(last_minute) = (until / MINUTE).checked_sub(1)
                && first_minute <= last_minute
            {
                tiers.push(Tier {
                    table: LOG_VOLUME_MINUTES,
                    unit: "Minute",
                    nanos: MINUTE,
                    ranges: vec![(first_minute, last_minute)],
                    excluded: split_minutes.clone(),
                });
            }
            // The seconds no whole minute holds: those of the minutes a bound splits —
            // which, since the range's own ends are bounds, include its ragged ends.
            let ranges: Vec<(u64, u64)> = split_minutes
                .iter()
                .map(|m| ((m * 60).max(first_second), (m * 60 + 59).min(last_second)))
                .filter(|(low, high)| low <= high)
                .collect();
            if !ranges.is_empty() {
                tiers.push(Tier {
                    table: LOG_VOLUME,
                    unit: "Second",
                    nanos: SECOND,
                    ranges,
                    excluded: split_seconds.clone(),
                });
            }
        } else {
            tiers.push(Tier {
                table: LOG_VOLUME,
                unit: "Second",
                nanos: SECOND,
                ranges: vec![(first_second, last_second)],
                excluded: split_seconds.clone(),
            });
        }
        Some(RollupPlan {
            tiers,
            split_seconds,
        })
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

impl Timed for Row {
    fn nanos(&self) -> u64 {
        self.t
    }
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
