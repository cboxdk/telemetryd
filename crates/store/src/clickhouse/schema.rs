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
//!   billions. See `log_volume`.
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
use super::metrics_schema;
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
pub const TRACE_KEYS: &str = "otel_trace_keys";
const TRACE_KEYS_VIEW: &str = "otel_trace_keys_mv";
/// The key in [`META`] saying from when `otel_trace_keys` holds every key.
pub const TRACE_KEYS_SINCE: &str = "trace_keys_since";

/// Which retention a table keeps rows for.
#[derive(Debug, Clone, Copy)]
pub(super) enum Signal {
    Logs,
    Traces,
    /// For the metric tables, which age by `retention.metrics`.
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
        table: TRACE_KEYS,
        time: "Hour",
        signal: Signal::Traces,
        moves: false,
    },
    Aging {
        table: TRACE_STREAMS,
        time: "Hour",
        signal: Signal::Traces,
        moves: false,
    },
    Aging {
        table: metrics_schema::SAMPLES,
        time: "toDateTime(Timestamp)",
        signal: Signal::Metrics,
        moves: true,
    },
    Aging {
        table: metrics_schema::SERIES,
        time: "toDateTime(Day)",
        signal: Signal::Metrics,
        moves: false,
    },
    Aging {
        table: metrics_schema::COUNTS,
        time: "toDateTime(toUInt64(Hour) * 3600)",
        signal: Signal::Metrics,
        moves: false,
    },
    Aging {
        table: metrics_schema::FOLDS,
        time: "toDateTime(Starts)",
        signal: Signal::Metrics,
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
    ///
    /// Replicated, a table would by default drop a block identical to one it took
    /// recently, as a retried insert. A rollup's blocks are computed: two different
    /// batches can count the same second of the same stream to the same number, and the
    /// second would vanish. Nothing here relies on the deduplication, so it is off.
    pub(super) fn settings(&self) -> String {
        let mut settings = vec![
            "index_granularity = 8192".to_owned(),
            "ttl_only_drop_parts = 1".to_owned(),
        ];
        let policy = self.config.storage_policy.trim();
        if !policy.is_empty() {
            settings.push(format!("storage_policy = {}", quote(policy)));
        }
        if !self.config.cluster.trim().is_empty() {
            settings.push("replicated_deduplication_window = 0".to_owned());
        }
        format!("SETTINGS {}", settings.join(", "))
    }

    pub(super) fn aging(&self, table: &str) -> String {
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
    INDEX idx_body_ngram Body TYPE {BODY_NGRAMS} GRANULARITY 1,
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
    // A table created before the n-gram index has it on the parts written from now
    // on; older parts are read without skipping until `telemetryd clickhouse migrate`
    // materializes it on them.
    statements.push(format!(
        "ALTER TABLE {LOGS}{on} ADD INDEX IF NOT EXISTS idx_body_ngram Body TYPE {BODY_NGRAMS} GRANULARITY 1"
    ));
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
    statements.extend(super::metrics_schema::statements(&layout));
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
        // One row per trace once merged, not one per batch its spans came in: a trace's
        // first and last span are a min and a max, and reads take them as such, so a
        // part not merged yet answers the same.
        "CREATE TABLE IF NOT EXISTS {TRACE_IDS}{on} (
    TraceId String CODEC(ZSTD(1)),
    Start SimpleAggregateFunction(min, DateTime64(9, 'UTC')) CODEC(Delta(8), ZSTD(1)),
    End SimpleAggregateFunction(max, DateTime64(9, 'UTC')) CODEC(Delta(8), ZSTD(1))
) ENGINE = {}
PARTITION BY toDate(Start)
ORDER BY TraceId
{}
{}",
        layout.engine(TRACE_IDS, "AggregatingMergeTree"),
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
    // Every attribute key per app per hour: what turns a relaxed attribute lookup into a
    // list of the keys it could find, instead of a pattern run on every key of every span.
    statements.push(format!(
        "CREATE TABLE IF NOT EXISTS {TRACE_KEYS}{on} (
    Hour DateTime CODEC(Delta(4), ZSTD(1)),
    ServiceName LowCardinality(String),
    Scope LowCardinality(String),
    Key String CODEC(ZSTD(1)),
    Spans SimpleAggregateFunction(sum, UInt64)
) ENGINE = {}
PARTITION BY toDate(Hour)
ORDER BY (ServiceName, Hour, Scope, Key)
{}
{}",
        layout.engine(TRACE_KEYS, "AggregatingMergeTree"),
        layout.aging(TRACE_KEYS),
        layout.settings(),
    ));
    statements.push(format!(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS {TRACE_KEYS_VIEW}{on} TO {TRACE_KEYS} AS
SELECT toStartOfHour(Timestamp) AS Hour, ServiceName, key.1 AS Scope, key.2 AS Key, count() AS Spans
FROM {TRACES}
ARRAY JOIN arrayConcat(
    arrayMap(k -> ('span', k), mapKeys(SpanAttributes)),
    arrayMap(k -> ('resource', k), mapKeys(ResourceAttributes))
) AS key
GROUP BY Hour, ServiceName, Scope, Key"
    ));
    statements
}

/// Every table and view the statements create, by name, in order: what a database whose
/// schema someone else manages must hold.
#[must_use]
pub fn objects(config: &ClickHouseConfig, retention: &RetentionConfig) -> Vec<String> {
    statements(config, retention)
        .iter()
        .filter_map(|statement| {
            let rest = statement
                .strip_prefix("CREATE TABLE IF NOT EXISTS ")
                .or_else(|| statement.strip_prefix("CREATE MATERIALIZED VIEW IF NOT EXISTS "))?;
            let name: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            (!name.is_empty()).then_some(name)
        })
        .collect()
}

/// Which of [`objects`] the configured database does not have.
///
/// # Errors
/// When ClickHouse cannot be read.
pub fn missing(
    client: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
) -> Result<Vec<String>> {
    let mut present = std::collections::BTreeSet::new();
    client.query_lines(
        "SELECT name FROM system.tables WHERE database = currentDatabase()",
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            if let Some(name) = value.get("name").and_then(serde_json::Value::as_str) {
                present.insert(name.to_owned());
            }
            Ok(std::ops::ControlFlow::Continue(()))
        },
    )?;
    Ok(objects(config, retention)
        .into_iter()
        .filter(|name| !present.contains(name))
        .collect())
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
    let server = client.in_database("default");
    one_shard(&server, config)?;
    let database_there = || -> Result<bool> {
        let mut there = false;
        server.query_lines(
            &format!(
                "SELECT 1 AS one FROM system.databases WHERE name = {}",
                quote(client.database())
            ),
            |_| {
                there = true;
                Ok(std::ops::ControlFlow::Break(()))
            },
        )?;
        Ok(there)
    };
    if !database_there()? {
        server.execute(&format!(
            "CREATE DATABASE IF NOT EXISTS {}{on}",
            identifier(client.database())
        ))?;
        // On a cluster the statement can return before this replica has run it —
        // another instance's identical statement, queued first, is the one this replica
        // ran — and a table created in a database not there yet is refused. Wait until
        // it is there.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !database_there()? {
            if std::time::Instant::now() >= deadline {
                return Err(telemetryd_core::Error::Unavailable(format!(
                    "database {:?} was created on the cluster but has not appeared on this \
                     replica within a minute; check the cluster's distributed DDL queue",
                    client.database()
                )));
            }
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }
    // What is there already is left alone: on a cluster every statement is a task for
    // every replica through Keeper, and a fleet restarting one instance at a time would
    // queue the whole schema once per instance for nothing.
    let existing = existing_columns(client)?;
    for statement in statements(config, retention) {
        if already_there(&statement, &existing) {
            continue;
        }
        client.execute(&statement)?;
    }
    let ttl = ttl_statements(config, retention).join(";\n");
    let applied = setting(client, TTL_APPLIED)?;
    if applied.as_deref() != Some(ttl.as_str()) {
        apply_ttl(client, config, retention)?;
    }
    record_rollup_start(client, LOG_VOLUME_SINCE, LOGS)?;
    record_rollup_start(client, LOG_VOLUME_MINUTES_SINCE, LOGS)?;
    record_rollup_start(client, TRACE_KEYS_SINCE, TRACES)
}

/// The body index: 4-grams, so a line filter's every substring of four characters or
/// more can skip granules, a bare id included. A token index skips only on tokens the
/// pattern holds whole, which `|= "9f8e7d6c"` does not — it read every granule — and
/// measured on 20M lines, this one is smaller (58 MB against 63) and skipped 2541 of
/// 2544 granules for an id that the token index skipped none of.
pub(crate) const BODY_NGRAMS: &str = "ngrambf_v1(4, 32768, 2, 0)";

/// The key in [`META`] holding the TTL statements last applied, so a start with the
/// same retention runs none of them.
const TTL_APPLIED: &str = "ttl_applied";

/// Refuse a cluster of more than one shard, and one the server does not know.
///
/// The tables are replicated, not distributed: on a cluster of several shards each
/// instance would read and write only its own replica's shard, and every answer would
/// be part of one. Shards need `Distributed` tables, which telemetryd does not create.
fn one_shard(server: &Client, config: &ClickHouseConfig) -> Result<()> {
    let cluster = config.cluster.trim();
    if cluster.is_empty() {
        return Ok(());
    }
    let mut shards = None;
    server.query_lines(
        &format!(
            "SELECT uniqExact(shard_num) AS shards FROM system.clusters WHERE cluster = {}",
            quote(cluster)
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            shards = value.get("shards").and_then(serde_json::Value::as_u64);
            Ok(std::ops::ControlFlow::Continue(()))
        },
    )?;
    match shards.unwrap_or(0) {
        0 => Err(telemetryd_core::Error::Config(format!(
            "clickhouse.cluster is {cluster:?}, which the server's remote_servers does not define"
        ))),
        1 => Ok(()),
        n => Err(telemetryd_core::Error::Config(format!(
            "clickhouse.cluster {cluster:?} has {n} shards. telemetryd's tables are replicated, \
             not distributed: on more than one shard each instance would see one shard's \
             data. Use a cluster of one shard and as many replicas as you like"
        ))),
    }
}

/// Every table in the database, and its columns and, as `index <name>`, its skipping
/// indexes.
fn existing_columns(
    client: &Client,
) -> Result<std::collections::BTreeMap<String, std::collections::BTreeSet<String>>> {
    let mut tables: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        std::collections::BTreeMap::new();
    // `c.name`, not `name`: the first column's alias `name` would otherwise shadow
    // the column, and every table would seem to have a column named after itself.
    client.query_lines(
        "SELECT name, '' AS column FROM system.tables WHERE database = currentDatabase() \
         UNION ALL \
         SELECT c.table AS name, c.name AS column FROM system.columns AS c \
         WHERE c.database = currentDatabase() \
         UNION ALL \
         SELECT i.table AS name, concat('index ', i.name) AS column \
         FROM system.data_skipping_indices AS i WHERE i.database = currentDatabase()",
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            let text = |key: &str| {
                value
                    .get(key)
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            let columns = tables.entry(text("name")).or_default();
            let column = text("column");
            if !column.is_empty() {
                columns.insert(column);
            }
            Ok(std::ops::ControlFlow::Continue(()))
        },
    )?;
    Ok(tables)
}

/// Whether a statement would change nothing: a `CREATE … IF NOT EXISTS` of something
/// there, an `ADD COLUMN IF NOT EXISTS` of a column there.
fn already_there(
    statement: &str,
    existing: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
) -> bool {
    let name = |rest: &str| -> String {
        rest.chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect()
    };
    if let Some(rest) = statement
        .strip_prefix("CREATE TABLE IF NOT EXISTS ")
        .or_else(|| statement.strip_prefix("CREATE MATERIALIZED VIEW IF NOT EXISTS "))
    {
        return existing.contains_key(&name(rest));
    }
    if let Some(rest) = statement.strip_prefix("ALTER TABLE ")
        && let Some((_, index)) = rest.split_once(" ADD INDEX IF NOT EXISTS ")
    {
        let index = format!("index {}", name(index));
        return existing
            .get(&name(rest))
            .is_some_and(|columns| columns.contains(&index));
    }
    if let Some(rest) = statement.strip_prefix("ALTER TABLE ")
        && let Some((_, column)) = rest.split_once(" ADD COLUMN IF NOT EXISTS ")
    {
        let column: String = column
            .trim_start_matches('`')
            .chars()
            .take_while(|c| *c != '`' && *c != ' ')
            .collect();
        return existing
            .get(&name(rest))
            .is_some_and(|columns| columns.contains(&column));
    }
    false
}

/// A value in [`META`], the latest written.
fn setting(client: &Client, name: &str) -> Result<Option<String>> {
    let mut value = None;
    client.query_lines(
        &format!(
            "SELECT argMax(Value, Updated) AS value, count() AS n FROM {META} WHERE Name = {}",
            quote(name)
        ),
        |line| {
            let row: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            if row
                .get("n")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
                > 0
            {
                value = row
                    .get("value")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
            }
            Ok(std::ops::ControlFlow::Continue(()))
        },
    )?;
    Ok(value)
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
    let statements = ttl_statements(config, retention);
    for statement in &statements {
        client.execute_with(statement, &[("materialize_ttl_after_modify", "0")])?;
    }
    client.execute(&format!(
        "INSERT INTO {META} SELECT {}, {}, now64(9)",
        quote(TTL_APPLIED),
        quote(&statements.join(";\n"))
    ))
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    fn existing(table: &str, entries: &[&str]) -> BTreeMap<String, BTreeSet<String>> {
        BTreeMap::from([(
            table.to_owned(),
            entries.iter().map(|e| (*e).to_owned()).collect(),
        )])
    }

    #[test]
    fn a_statement_for_what_is_there_is_skipped_and_one_for_what_is_not_runs() {
        let there = existing("otel_logs", &["Stream", "index idx_body_ngram"]);
        for (statement, skipped) in [
            ("CREATE TABLE IF NOT EXISTS otel_logs (x UInt8)", true),
            ("CREATE TABLE IF NOT EXISTS otel_traces (x UInt8)", false),
            (
                "ALTER TABLE otel_logs ADD COLUMN IF NOT EXISTS Stream Map(String, String)",
                true,
            ),
            (
                "ALTER TABLE otel_logs ADD COLUMN IF NOT EXISTS `StreamHash` UInt64",
                false,
            ),
            (
                "ALTER TABLE otel_logs ON CLUSTER td ADD INDEX IF NOT EXISTS idx_body_ngram Body",
                true,
            ),
            (
                "ALTER TABLE otel_logs ADD INDEX IF NOT EXISTS idx_other Body",
                false,
            ),
            // A column named like an index is not the index.
            (
                "ALTER TABLE otel_logs ADD COLUMN IF NOT EXISTS idx_body_ngram String",
                false,
            ),
        ] {
            assert_eq!(already_there(statement, &there), skipped, "{statement}");
        }
    }
}
