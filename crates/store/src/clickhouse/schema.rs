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

/// The table engine: plain on one server, replicated on a cluster.
struct Engine<'a> {
    cluster: &'a str,
}

impl Engine<'_> {
    fn on_cluster(&self) -> String {
        if self.cluster.is_empty() {
            String::new()
        } else {
            format!(" ON CLUSTER {}", super::sql::quote(self.cluster))
        }
    }

    fn engine(&self, table: &str, kind: &str) -> String {
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
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_stream_value mapValues(Stream) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = {}
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, toDateTime(Timestamp))
{ttl}
{settings}",
            engine.engine(LOGS, "MergeTree")
        ),
        // A table the store package created has neither.
        format!(
            "ALTER TABLE {LOGS}{on} ADD COLUMN IF NOT EXISTS \
             Stream Map(LowCardinality(String), String) CODEC(ZSTD(1))"
        ),
        format!(
            "ALTER TABLE {LOGS}{on} ADD INDEX IF NOT EXISTS \
             idx_stream_value mapValues(Stream) TYPE bloom_filter(0.01) GRANULARITY 1"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {LOG_STREAMS}{on} (
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
{settings}",
            engine.engine(LOG_STREAMS, "AggregatingMergeTree")
        ),
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {LOG_STREAMS_VIEW}{on} TO {LOG_STREAMS} AS
SELECT
    toStartOfHour(Timestamp) AS Hour,
    ServiceName,
    cityHash64(S) AS StreamHash,
    any(S) AS Stream,
    count() AS Rows,
    min(Timestamp) AS MinTimestamp,
    max(Timestamp) AS MaxTimestamp
FROM (SELECT Timestamp, ServiceName, Stream AS S FROM {LOGS})
GROUP BY Hour, ServiceName, StreamHash"
        ),
    ]
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
    Ok(())
}

/// A database name as an identifier, backquoted.
fn identifier(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}
