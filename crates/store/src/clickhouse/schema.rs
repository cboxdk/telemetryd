//! The tables, and the statements that create them.
//!
//! # Whose layout
//!
//! `otel_logs` and `otel_traces` are the OpenTelemetry Collector's tables, column for
//! column, as `cboxdk/laravel-telemetry-store` creates them — so that package's
//! readers, and Grafana's ClickHouse plugin, read what telemetryd writes. Created by
//! either side first, they end up the same: every statement here is `IF NOT EXISTS`,
//! and the columns telemetryd adds are added to a table the other side created.
//!
//! # What telemetryd adds
//!
//! - **`Stream`**, the stream labels telemetryd computed at ingest: `app`, `level` and
//!   the promoted resource attributes, sanitised. They cannot be recomputed from
//!   `ResourceAttributes` at read time — which attributes are promoted is
//!   configuration, and configuration changes — and a stream that changed identity
//!   under an existing row would change what every query returns.
//! - **`StreamHash`**, the stream as one number. A query's stream matchers are
//!   resolved against the streams summary first, exactly, by the same matcher code the
//!   local store uses, and what reaches the table is `StreamHash IN (…)`: a `UInt64`
//!   column read instead of a map, five times faster over a billion rows, and the key
//!   log volume is grouped by without reading the stream at all.
//! - **`AttributeLabels`**, the label names a log record's attributes give it. An
//!   attribute can stand in for a stream label in what a query groups by; this is what
//!   lets a grouped count tell the rare row where one does without reading the
//!   attributes.
//! - **Streams summaries**, every stream seen per hour, kept current by materialized
//!   views. Label names, label values and series answer from them: the same questions
//!   asked of the tables would read the stream column of every row in the range.
//! - **Log volume rollups**, lines and bytes per stream per second and per minute, so a
//!   log volume chart over ninety days reads thousands of rows rather than hundreds of
//!   billions. See [`log_volume`].
//!
//! # Retention and tiers
//!
//! Every table keeps rows for its signal's `retention.*`, as a TTL ClickHouse applies by
//! dropping whole day partitions. With `clickhouse.storage_policy` and
//! `clickhouse.cold_after` set, the two large tables move their older days to the
//! policy's cold volume first — object storage or slower disks — and keep them
//! queryable there. Summaries and rollups are small and stay on the hot volume, so the
//! questions they answer stay fast however old the data. Retention changed in the
//! configuration reaches the tables on the next start or reload, without rewriting
//! data that exists: see [`ttl_statements`].

use std::time::Duration;

use telemetryd_core::Result;
use telemetryd_core::config::{ClickHouseConfig, RetentionConfig};

use super::Client;
use super::sql::quote;

pub const LOGS: &str = "otel_logs";
pub const LOG_STREAMS: &str = "otel_log_streams";
const LOG_STREAMS_VIEW: &str = "otel_log_streams_mv";
pub const LOG_VOLUME: &str = "otel_log_volume";
const LOG_VOLUME_VIEW: &str = "otel_log_volume_mv";
pub const LOG_VOLUME_MINUTES: &str = "otel_log_volume_1m";
const LOG_VOLUME_MINUTES_VIEW: &str = "otel_log_volume_1m_mv";
pub const META: &str = "telemetryd_meta";
/// The key in [`META`] saying from when `otel_log_volume` counts every line.
pub const LOG_VOLUME_SINCE: &str = "log_volume_since";
/// The same for `otel_log_volume_1m`.
pub const LOG_VOLUME_MINUTES_SINCE: &str = "log_volume_1m_since";
pub const TRACES: &str = "otel_traces";
pub const TRACE_IDS: &str = "otel_traces_trace_id_ts";
const TRACE_IDS_VIEW: &str = "otel_traces_trace_id_ts_mv";
pub const TRACE_STREAMS: &str = "otel_trace_streams";
const TRACE_STREAMS_VIEW: &str = "otel_trace_streams_mv";

/// Which retention a table keeps rows for.
#[derive(Debug, Clone, Copy)]
pub(super) enum Signal {
    Logs,
    Traces,
    /// For the metric tables, which age by `retention.metrics`.
    #[allow(dead_code)]
    Metrics,
}

/// How a table's rows age: by which expression, for which signal, and whether its old
/// days move to the cold volume or stay where they are.
#[derive(Debug, Clone, Copy)]
pub(super) struct Aging {
    pub(super) table: &'static str,
    /// A `DateTime` expression over the table's columns.
    pub(super) time: &'static str,
    pub(super) signal: Signal,
    pub(super) moves: bool,
}

const AGING: &[Aging] = &[
    Aging {
        table: LOGS,
        time: "toDateTime(Timestamp)",
        signal: Signal::Logs,
        moves: true,
    },
    Aging {
        table: LOG_STREAMS,
        time: "Hour",
        signal: Signal::Logs,
        moves: false,
    },
    Aging {
        table: LOG_VOLUME,
        time: "toDateTime(Second)",
        signal: Signal::Logs,
        moves: false,
    },
    Aging {
        table: LOG_VOLUME_MINUTES,
        time: "toDateTime(Minute * 60)",
        signal: Signal::Logs,
        moves: false,
    },
    Aging {
        table: TRACES,
        time: "toDateTime(Timestamp)",
        signal: Signal::Traces,
        moves: true,
    },
    Aging {
        table: TRACE_IDS,
        time: "toDateTime(Start)",
        signal: Signal::Traces,
        moves: false,
    },
    Aging {
        table: TRACE_STREAMS,
        time: "Hour",
        signal: Signal::Traces,
        moves: false,
    },
];

/// What every statement is shaped by: the cluster, the tiers, the retention.
pub(super) struct Layout<'a> {
    pub(super) config: &'a ClickHouseConfig,
    pub(super) retention: &'a RetentionConfig,
}

impl Layout<'_> {
    pub(super) fn on_cluster(&self) -> String {
        let cluster = self.config.cluster.trim();
        if cluster.is_empty() {
            String::new()
        } else {
            format!(" ON CLUSTER {}", quote(cluster))
        }
    }

    /// A `MergeTree` family engine: plain on one server, replicated on a cluster.
    pub(super) fn engine(&self, table: &str, kind: &str) -> String {
        if self.config.cluster.trim().is_empty() {
            format!("{kind}()")
        } else {
            format!(
                "Replicated{kind}('/clickhouse/tables/{{shard}}/{{database}}/{table}', '{{replica}}')"
            )
        }
    }

    pub(super) fn keep(&self, signal: Signal) -> Duration {
        match signal {
            Signal::Logs => self.retention.logs.0,
            Signal::Traces => self.retention.traces.0,
            Signal::Metrics => self.retention.metrics.0,
        }
    }

    /// The `TTL` clause: delete after the signal's retention, and for a table whose old
    /// days move, to the cold volume after `clickhouse.cold_after` first.
    pub(super) fn ttl(&self, time: &str, signal: Signal, moves: bool) -> String {
        let keep = self.keep(signal).as_secs().max(1);
        let cold = self.config.cold_after.as_secs();
        if moves && cold > 0 && cold < keep {
            format!(
                "TTL {time} + INTERVAL {cold} SECOND TO VOLUME {}, {time} + INTERVAL {keep} SECOND DELETE",
                quote(&self.config.cold_volume)
            )
        } else {
            format!("TTL {time} + INTERVAL {keep} SECOND DELETE")
        }
    }

    /// The `SETTINGS` clause every table shares.
    pub(super) fn settings(&self) -> String {
        let policy = self.config.storage_policy.trim();
        if policy.is_empty() {
            "SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1".to_owned()
        } else {
            format!(
                "SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1, storage_policy = {}",
                quote(policy)
            )
        }
    }

    fn aging(&self, table: &str) -> String {
        let aging = AGING
            .iter()
            .find(|a| a.table == table)
            .copied()
            .unwrap_or(Aging {
                table: "",
                time: "toDateTime(Timestamp)",
                signal: Signal::Logs,
                moves: false,
            });
        self.ttl(aging.time, aging.signal, aging.moves)
    }
}

/// Every statement, in the order they must run.
#[must_use]
pub fn statements(config: &ClickHouseConfig, retention: &RetentionConfig) -> Vec<String> {
    let layout = Layout { config, retention };
    let on = layout.on_cluster();
    let mut statements = vec![format!(
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
{}
{}",
        layout.engine(LOGS, "MergeTree"),
        layout.aging(LOGS),
        layout.settings(),
    )];
    // A table the store package created has none of them.
    for column in [
        "Stream Map(LowCardinality(String), String) CODEC(ZSTD(1))",
        "StreamHash UInt64 MATERIALIZED cityHash64(Stream) CODEC(ZSTD(1))",
        "AttributeLabels Array(LowCardinality(String)) CODEC(ZSTD(1))",
    ] {
        statements.push(format!(
            "ALTER TABLE {LOGS}{on} ADD COLUMN IF NOT EXISTS {column}"
        ));
    }
    statements.extend(streams_summary(
        &layout,
        LOGS,
        LOG_STREAMS,
        LOG_STREAMS_VIEW,
    ));
    statements.extend(log_volume(&layout));
    statements.extend(traces(&layout));
    statements.push(format!(
        "CREATE TABLE IF NOT EXISTS {META}{on} (
    Name String,
    Value String,
    Updated DateTime64(9, 'UTC')
) ENGINE = {}
ORDER BY Name",
        layout.engine(META, "ReplacingMergeTree")
    ));
    statements
}

/// The TTL every table should have under this configuration, for tables that exist:
/// what a changed `retention.*` or `clickhouse.cold_after` takes effect through.
///
/// Run with `materialize_ttl_after_modify = 0`, so existing parts are not rewritten:
/// a part's rows age by the new rule as ClickHouse next merges or drops it, and a day
/// partition past the new retention is dropped whole.
#[must_use]
pub fn ttl_statements(config: &ClickHouseConfig, retention: &RetentionConfig) -> Vec<String> {
    let layout = Layout { config, retention };
    let on = layout.on_cluster();
    AGING
        .iter()
        .map(|aging| {
            format!(
                "ALTER TABLE {}{on} MODIFY {}",
                aging.table,
                layout.ttl(aging.time, aging.signal, aging.moves)
            )
        })
        .collect()
}

/// A table of every stream a signal's table holds, per hour, kept current by a
/// materialized view: what stream matchers resolve against and label names and values
/// answer from.
fn streams_summary(layout: &Layout<'_>, source: &str, summary: &str, view: &str) -> Vec<String> {
    let on = layout.on_cluster();
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
{}
{}",
            layout.engine(summary, "AggregatingMergeTree"),
            layout.aging(summary),
            layout.settings(),
        ),
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {view}{on} TO {summary} AS
SELECT
    toStartOfHour(Timestamp) AS Hour,
    ServiceName,
    cityHash64(S) AS StreamHash,
    any(S) AS Stream,
    count() AS Rows,
    min(Timestamp) AS MinTimestamp,
    max(Timestamp) AS MaxTimestamp
FROM (SELECT Timestamp, ServiceName, Stream AS S FROM {source})
GROUP BY Hour, ServiceName, StreamHash"
        ),
    ]
}

/// Lines and bytes per stream per second, and per minute, for log volume without
/// reading the lines.
///
/// A second here is `(s, s + 1]` and a minute `(m, m + 60]`, half open the way a LogQL
/// window is, so either lies wholly inside every bucket whose bounds fall on its own
/// bounds. Kept per set of attribute label names as well, so a count grouped by a label
/// an attribute could stand in for can tell from here alone whether any line in the
/// range needs reading.
///
/// [`META`] records when each was created: only from then does it hold every line, and
/// a query reaching back before that counts from the lines.
fn log_volume(layout: &Layout<'_>) -> Vec<String> {
    let on = layout.on_cluster();
    let mut statements = Vec::new();
    for (table, view, unit, nanos) in [
        (LOG_VOLUME, LOG_VOLUME_VIEW, "Second", 1_000_000_000_u64),
        (
            LOG_VOLUME_MINUTES,
            LOG_VOLUME_MINUTES_VIEW,
            "Minute",
            60_000_000_000,
        ),
    ] {
        statements.push(format!(
            "CREATE TABLE IF NOT EXISTS {table}{on} (
    ServiceName LowCardinality(String),
    {unit} UInt32 CODEC(Delta(4), ZSTD(1)),
    StreamHash UInt64,
    LabelsHash UInt64,
    AttributeLabels SimpleAggregateFunction(any, Array(String)),
    Lines SimpleAggregateFunction(sum, UInt64),
    Bytes SimpleAggregateFunction(sum, UInt64)
) ENGINE = {}
PARTITION BY toDate({partition})
ORDER BY (ServiceName, {unit}, StreamHash, LabelsHash)
{}
{}",
            layout.engine(table, "AggregatingMergeTree"),
            layout.aging(table),
            layout.settings(),
            partition = if unit == "Second" {
                "toDateTime(Second)"
            } else {
                "toDateTime(Minute * 60)"
            },
        ));
        statements.push(format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {view}{on} TO {table} AS
SELECT
    ServiceName,
    toUInt32(intDiv(toUnixTimestamp64Nano(Timestamp) - 1, {nanos})) AS {unit},
    cityHash64(S) AS StreamHash,
    cityHash64(A) AS LabelsHash,
    any(A) AS AttributeLabels,
    count() AS Lines,
    sum(length(Body)) AS Bytes
FROM (SELECT Timestamp, ServiceName, Stream AS S, arrayMap(x -> toString(x), AttributeLabels) AS A, Body FROM {LOGS})
GROUP BY ServiceName, {unit}, StreamHash, LabelsHash"
        ));
    }
    statements
}

/// `otel_traces`, as the Collector and the store package lay it out, with the stream
/// columns logs have; the trace-id index the Collector keeps beside it; and the streams
/// summary.
///
/// Sorted by app and time, not by app, span name and time as the Collector sorts it:
/// a search reads newest first, and with the name ahead of the time in the key every
/// name's newest spans are in a different place.
fn traces(layout: &Layout<'_>) -> Vec<String> {
    let on = layout.on_cluster();
    let mut statements = vec![format!(
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
{}
{}",
        layout.engine(TRACES, "MergeTree"),
        layout.aging(TRACES),
        layout.settings(),
    )];
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
{}
{}",
        layout.engine(TRACE_IDS, "MergeTree"),
        layout.aging(TRACE_IDS),
        layout.settings(),
    ));
    statements.push(format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {TRACE_IDS_VIEW}{on} TO {TRACE_IDS} AS
SELECT TraceId, min(Timestamp) AS Start, max(Timestamp) AS End
FROM {TRACES}
WHERE TraceId != ''
GROUP BY TraceId"
    ));
    statements.extend(streams_summary(
        layout,
        TRACES,
        TRACE_STREAMS,
        TRACE_STREAMS_VIEW,
    ));
    statements
}

/// Create whatever is missing, give every table the TTL the configuration asks for, and
/// record from when each rollup is complete.
///
/// # Errors
/// [`telemetryd_core::Error::Unavailable`] when ClickHouse refuses a statement, naming
/// it.
pub fn ensure(
    client: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
) -> Result<()> {
    // From the server's default database: the configured one may not exist yet, and a
    // request naming a database that does not exist is refused before it runs.
    let on = Layout { config, retention }.on_cluster();
    client.in_database("default").execute(&format!(
        "CREATE DATABASE IF NOT EXISTS {}{on}",
        identifier(client.database())
    ))?;
    for statement in statements(config, retention) {
        client.execute(&statement)?;
    }
    apply_ttl(client, config, retention)?;
    record_rollup_start(client, LOG_VOLUME_SINCE, LOGS)?;
    record_rollup_start(client, LOG_VOLUME_MINUTES_SINCE, LOGS)
}

/// Bring every table's TTL to the configuration's, without rewriting existing parts.
///
/// # Errors
/// When ClickHouse refuses — a cold volume the table's storage policy does not have,
/// most likely.
pub fn apply_ttl(
    client: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
) -> Result<()> {
    for statement in ttl_statements(config, retention) {
        client.execute_with(&statement, &[("materialize_ttl_after_modify", "0")])?;
    }
    Ok(())
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
        quote(name)
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
            quote(name)
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
