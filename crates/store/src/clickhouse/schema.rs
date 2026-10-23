//! The tables, and the statements that create them.
//!
//! # Whose layout
//!
//! `otel_logs` is the OpenTelemetry Collector's table, column for column, as
//! `cboxdk/laravel-telemetry-store` creates it — so that package's readers, and
//! Grafana's ClickHouse plugin, read what telemetryd writes. Created by either side
//! first, it ends up the same: every statement here is `IF NOT EXISTS`, and the columns
//! telemetryd adds are added to a table the other side created.
//!
//! # What telemetryd adds
//!
//! - **`Stream`**, the stream labels telemetryd computed at ingest: `app`, `level` and
//!   the promoted resource attributes, sanitised. They cannot be recomputed from
//!   `ResourceAttributes` at read time — which attributes are promoted is
//!   configuration, and configuration changes — and a stream that changed identity
//!   under an existing row would change what every query returns.
//! - **`StreamHash`**, the stream as one number. A query's stream matchers are
//!   resolved against `otel_log_streams` first, exactly, by the same matcher code the
//!   local store uses, and what reaches `otel_logs` is `StreamHash IN (…)`: a `UInt64`
//!   column read instead of a map, five times faster over a billion rows, and the key
//!   log volume is grouped by without reading the stream at all.
//! - **`AttributeLabels`**, the label names a record's attributes give it. An attribute
//!   can stand in for a stream label in what a query groups by; this is what lets a
//!   grouped count tell the rare row where one does without reading the attributes.
//! - **`otel_log_streams`**, every stream seen per hour, kept current by a
//!   materialized view. Label names, label values and series answer from it: the same
//!   questions asked of `otel_logs` would read the stream column of every row in the
//!   range, which at billions of rows a day is the difference between milliseconds and
//!   minutes.

use telemetryd_core::Result;
use telemetryd_core::config::ClickHouseConfig;

use super::Client;

pub const LOGS: &str = "otel_logs";
pub const LOG_STREAMS: &str = "otel_log_streams";
const LOG_STREAMS_VIEW: &str = "otel_log_streams_mv";
pub const LOG_VOLUME: &str = "otel_log_volume";
const LOG_VOLUME_VIEW: &str = "otel_log_volume_mv";
pub const META: &str = "telemetryd_meta";
/// The key in [`META`] saying from when `otel_log_volume` counts every line.
pub const LOG_VOLUME_SINCE: &str = "log_volume_since";
pub const TRACES: &str = "otel_traces";
pub const TRACE_IDS: &str = "otel_traces_trace_id_ts";
const TRACE_IDS_VIEW: &str = "otel_traces_trace_id_ts_mv";
pub const TRACE_STREAMS: &str = "otel_trace_streams";
const TRACE_STREAMS_VIEW: &str = "otel_trace_streams_mv";
pub const TRACE_KEYS: &str = "otel_trace_attribute_keys";
const TRACE_KEYS_VIEW: &str = "otel_trace_attribute_keys_mv";

/// The table engine: plain on one server, replicated on a cluster.
pub(super) struct Engine<'a> {
    pub(super) cluster: &'a str,
}

impl Engine<'_> {
    pub(super) fn on_cluster(&self) -> String {
        if self.cluster.is_empty() {
            String::new()
        } else {
            format!(" ON CLUSTER {}", super::sql::quote(self.cluster))
        }
    }

    pub(super) fn engine(&self, table: &str, kind: &str) -> String {
        if self.cluster.is_empty() {
            format!("{kind}()")
        } else {
            format!(
                "Replicated{kind}('/clickhouse/tables/{{shard}}/{{database}}/{table}', '{{replica}}')"
            )
        }
    }
}

/// Every statement, in the order they must run.
#[must_use]
pub fn statements(config: &ClickHouseConfig) -> Vec<String> {
    let engine = Engine {
        cluster: config.cluster.trim(),
    };
    let on = engine.on_cluster();
    let days = config.retention_days.max(1);
    let ttl = format!("TTL toDateTime(Timestamp) + INTERVAL {days} DAY");
    let settings = "SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1";

    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {LOGS}{on} (
    Timestamp DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    SeverityText LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber Int32 CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    Body String CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Stream Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StreamHash UInt64 MATERIALIZED cityHash64(Stream) CODEC(ZSTD(1)),
    AttributeLabels Array(LowCardinality(String)) CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = {}
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, toDateTime(Timestamp))
{ttl}
{settings}",
            engine.engine(LOGS, "MergeTree")
        ),
        // A table the store package created has none of them.
        format!(
            "ALTER TABLE {LOGS}{on} ADD COLUMN IF NOT EXISTS \
             Stream Map(LowCardinality(String), String) CODEC(ZSTD(1))"
        ),
        format!(
            "ALTER TABLE {LOGS}{on} ADD COLUMN IF NOT EXISTS \
             StreamHash UInt64 MATERIALIZED cityHash64(Stream) CODEC(ZSTD(1))"
        ),
        format!(
            "ALTER TABLE {LOGS}{on} ADD COLUMN IF NOT EXISTS \
             AttributeLabels Array(LowCardinality(String)) CODEC(ZSTD(1))"
        ),
    ]
    .into_iter()
    .chain(streams_summary(
        &engine,
        LOGS,
        LOG_STREAMS,
        LOG_STREAMS_VIEW,
        "Timestamp",
        days,
    ))
    .chain(log_volume(&engine, days))
    .chain(traces(&engine, &ttl, settings, days))
    .chain(super::metrics_schema::statements(config))
    .collect()
}

/// A table of every stream a signal's table holds, per hour, kept current by a
/// materialized view: what stream matchers resolve against and label names and values
/// answer from.
fn streams_summary(
    engine: &Engine<'_>,
    source: &str,
    summary: &str,
    view: &str,
    time: &str,
    days: u32,
) -> Vec<String> {
    let on = engine.on_cluster();
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {summary}{on} (
    Hour DateTime CODEC(Delta(4), ZSTD(1)),
    ServiceName LowCardinality(String),
    StreamHash UInt64,
    Stream SimpleAggregateFunction(any, Map(String, String)),
    Rows SimpleAggregateFunction(sum, UInt64),
    MinTimestamp SimpleAggregateFunction(min, DateTime64(9, 'UTC')),
    MaxTimestamp SimpleAggregateFunction(max, DateTime64(9, 'UTC'))
) ENGINE = {}
PARTITION BY toDate(Hour)
ORDER BY (Hour, ServiceName, StreamHash)
TTL Hour + INTERVAL {days} DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
            engine.engine(summary, "AggregatingMergeTree")
        ),
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {view}{on} TO {summary} AS
SELECT
    toStartOfHour({time}) AS Hour,
    ServiceName,
    cityHash64(S) AS StreamHash,
    any(S) AS Stream,
    count() AS Rows,
    min({time}) AS MinTimestamp,
    max({time}) AS MaxTimestamp
FROM (SELECT {time}, ServiceName, Stream AS S FROM {source})
GROUP BY Hour, ServiceName, StreamHash"
        ),
    ]
}

/// Lines and bytes per stream per second, for log volume without reading the lines.
///
/// A second here is `(s, s + 1]`, half open the way a LogQL window is, so a second lies
/// wholly inside every bucket whose bounds fall on whole seconds. Kept per set of
/// attribute label names as well, so a count grouped by a label an attribute could
/// stand in for can tell from here alone whether any line in the range needs reading.
///
/// [`META`] records when it was created: only from then does it hold every line, and
/// a query reaching back before that counts from the lines.
fn log_volume(engine: &Engine<'_>, days: u32) -> Vec<String> {
    let on = engine.on_cluster();
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {LOG_VOLUME}{on} (
    ServiceName LowCardinality(String),
    Second UInt32 CODEC(Delta(4), ZSTD(1)),
    StreamHash UInt64,
    LabelsHash UInt64,
    AttributeLabels SimpleAggregateFunction(any, Array(String)),
    Lines SimpleAggregateFunction(sum, UInt64),
    Bytes SimpleAggregateFunction(sum, UInt64)
) ENGINE = {}
PARTITION BY toDate(toDateTime(Second))
ORDER BY (ServiceName, Second, StreamHash, LabelsHash)
TTL toDateTime(Second) + INTERVAL {days} DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
            engine.engine(LOG_VOLUME, "AggregatingMergeTree")
        ),
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {LOG_VOLUME_VIEW}{on} TO {LOG_VOLUME} AS
SELECT
    ServiceName,
    toUInt32(intDiv(toUnixTimestamp64Nano(Timestamp) - 1, 1000000000)) AS Second,
    cityHash64(S) AS StreamHash,
    cityHash64(A) AS LabelsHash,
    any(A) AS AttributeLabels,
    count() AS Lines,
    sum(length(Body)) AS Bytes
FROM (SELECT Timestamp, ServiceName, Stream AS S, arrayMap(x -> toString(x), AttributeLabels) AS A, Body FROM {LOGS})
GROUP BY ServiceName, Second, StreamHash, LabelsHash"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {META}{on} (
    Name String,
    Value String,
    Updated DateTime64(9, 'UTC')
) ENGINE = {}
ORDER BY Name",
            engine.engine(META, "ReplacingMergeTree")
        ),
    ]
}

/// `otel_traces`, as the Collector and the store package lay it out, with the stream
/// columns logs have; the trace-id index the Collector keeps beside it; the streams
/// summary; and every attribute key per hour, for the tag lists Grafana offers.
///
/// Sorted by app and time, not by app, span name and time as the Collector sorts it:
/// a search reads newest first, and with the name ahead of the time in the key every
/// name's newest spans are in a different place.
fn traces(engine: &Engine<'_>, ttl: &str, settings: &str, days: u32) -> Vec<String> {
    let on = engine.on_cluster();
    let mut statements = vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {TRACES}{on} (
    Timestamp DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    ParentSpanId String CODEC(ZSTD(1)),
    TraceState String CODEC(ZSTD(1)),
    SpanName LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration UInt64 CODEC(ZSTD(1)),
    StatusCode LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage String CODEC(ZSTD(1)),
    Events Nested (Timestamp DateTime64(9, 'UTC'), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    Stream Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StreamHash UInt64 MATERIALIZED cityHash64(Stream) CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_attr_value mapValues(SpanAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1
) ENGINE = {}
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, toDateTime(Timestamp))
{ttl}
{settings}",
            engine.engine(TRACES, "MergeTree")
        ),
    ];
    // A table the store package created has none of these.
    for column in [
        "TraceState String CODEC(ZSTD(1))",
        "`Links.TraceState` Array(String) CODEC(ZSTD(1))",
        "Stream Map(LowCardinality(String), String) CODEC(ZSTD(1))",
        "StreamHash UInt64 MATERIALIZED cityHash64(Stream) CODEC(ZSTD(1))",
    ] {
        statements.push(format!(
            "ALTER TABLE {TRACES}{on} ADD COLUMN IF NOT EXISTS {column}"
        ));
    }
    statements.push(format!(
        "CREATE TABLE IF NOT EXISTS {TRACE_IDS}{on} (
    TraceId String CODEC(ZSTD(1)),
    Start DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    End DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1))
) ENGINE = {}
PARTITION BY toDate(Start)
ORDER BY (TraceId, toUnixTimestamp(Start))
TTL toDateTime(Start) + INTERVAL {days} DAY
{settings}",
        engine.engine(TRACE_IDS, "MergeTree")
    ));
    statements.push(format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {TRACE_IDS_VIEW}{on} TO {TRACE_IDS} AS
SELECT TraceId, min(Timestamp) AS Start, max(Timestamp) AS End
FROM {TRACES}
WHERE TraceId != ''
GROUP BY TraceId"
    ));
    statements.extend(streams_summary(
        engine,
        TRACES,
        TRACE_STREAMS,
        TRACE_STREAMS_VIEW,
        "Timestamp",
        days,
    ));
    statements.push(format!(
        "CREATE TABLE IF NOT EXISTS {TRACE_KEYS}{on} (
    Hour DateTime CODEC(Delta(4), ZSTD(1)),
    ServiceName LowCardinality(String),
    Scope LowCardinality(String),
    Key LowCardinality(String),
    Spans SimpleAggregateFunction(sum, UInt64)
) ENGINE = {}
PARTITION BY toDate(Hour)
ORDER BY (Hour, ServiceName, Scope, Key)
TTL Hour + INTERVAL {days} DAY
{settings}",
        engine.engine(TRACE_KEYS, "AggregatingMergeTree")
    ));
    statements.push(format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {TRACE_KEYS_VIEW}{on} TO {TRACE_KEYS} AS
SELECT toStartOfHour(Timestamp) AS Hour, ServiceName, scope.1 AS Scope, scope.2 AS Key, count() AS Spans
FROM {TRACES}
ARRAY JOIN arrayConcat(
    arrayMap(k -> ('span', k), mapKeys(SpanAttributes)),
    arrayMap(k -> ('resource', k), mapKeys(ResourceAttributes))
) AS scope
GROUP BY Hour, ServiceName, Scope, Key"
    ));
    statements
}

/// Create whatever is missing.
///
/// # Errors
/// [`telemetryd_core::Error::Unavailable`] when ClickHouse refuses a statement, naming
/// it.
pub fn ensure(client: &Client, config: &ClickHouseConfig) -> Result<()> {
    // From the server's default database: the configured one may not exist yet, and a
    // request naming a database that does not exist is refused before it runs.
    let on = Engine {
        cluster: config.cluster.trim(),
    }
    .on_cluster();
    client.in_database("default").execute(&format!(
        "CREATE DATABASE IF NOT EXISTS {}{on}",
        identifier(client.database())
    ))?;
    for statement in statements(config) {
        client.execute(&statement)?;
    }
    record_rollup_start(client, LOG_VOLUME_SINCE, LOGS)
}

/// How far back a rollup can be trusted, recorded once, when it is first created.
///
/// A table with no rows yet has the rollup complete from the start. One that already
/// held rows — written before this view existed, by an older telemetryd or another
/// writer — has it complete only from now, and ten minutes later than now so that no
/// line already written with a timestamp a little ahead of the clock is counted as if
/// the rollup held it.
fn record_rollup_start(client: &Client, name: &str, table: &str) -> Result<()> {
    if rollup_start(client, name)?.is_some() {
        return Ok(());
    }
    let mut empty = true;
    client.query_lines(&format!("SELECT 1 AS one FROM {table} LIMIT 1"), |_| {
        empty = false;
        Ok(std::ops::ControlFlow::Break(()))
    })?;
    let since = if empty {
        "'0'".to_owned()
    } else {
        "toString(toUnixTimestamp64Nano(now64(9)) + 600000000000)".to_owned()
    };
    client.execute(&format!(
        "INSERT INTO {META} SELECT {}, {since}, now64(9)",
        super::sql::quote(name)
    ))
}

/// From when a rollup holds every row, in nanoseconds, if it was ever created.
///
/// The most recently written value wins: two instances starting together on a table
/// that already held rows record moments seconds apart, and a backfill that has filled
/// the rollup in for older rows writes an earlier one on purpose.
///
/// # Errors
/// When ClickHouse cannot be read.
pub fn rollup_start(client: &Client, name: &str) -> Result<Option<u64>> {
    let mut since = None;
    client.query_lines(
        &format!(
            "SELECT toUInt64OrZero(argMax(Value, Updated)) AS since, count() AS n FROM {META} WHERE Name = {}",
            super::sql::quote(name)
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            if value.get("n").and_then(serde_json::Value::as_u64).unwrap_or(0) > 0 {
                since = value.get("since").and_then(serde_json::Value::as_u64);
            }
            Ok(std::ops::ControlFlow::Continue(()))
        },
    )?;
    Ok(since)
}

/// A database name as an identifier, backquoted.
fn identifier(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}
