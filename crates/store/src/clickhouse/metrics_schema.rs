//! The metric tables.
//!
//! Not the Collector's `otel_metrics_*`: telemetryd keeps metrics as Prometheus does —
//! samples of label sets, classic histograms already expanded into `_bucket` series,
//! staleness markers, `remote_write` samples with no type at all — and PromQL reads
//! them that way. Turning them back into OTLP points to store and back into samples to
//! read would be two lossy translations for nothing.
//!
//! # The tables
//!
//! - **`telemetryd_metrics_ingest`** is a `Null` table: what telemetryd inserts into,
//!   holding nothing. Two materialized views take each insert from it, so a sample is
//!   written with its full label set once and stored without it.
//! - **`telemetryd_samples`**: one row per sample, sorted by metric name, series and
//!   time, so a PromQL selector reads its series' runs in order and nothing else. The
//!   label set is a hash here; the value is a `Float64` the Gorilla codec compresses,
//!   and `Special` says when the value is one JSON cannot carry — a staleness marker,
//!   another NaN, an infinity — so every value reads back bit for bit.
//! - **`telemetryd_series`**: every series per day, its labels, app, first and last
//!   sample. Matchers are resolved against it, exactly, by the matcher code every store
//!   uses; label names, values and `/series` answer from it.

use telemetryd_core::config::ClickHouseConfig;

use super::schema::Engine;

pub const INGEST: &str = "telemetryd_metrics_ingest";
pub const SAMPLES: &str = "telemetryd_samples";
const SAMPLES_VIEW: &str = "telemetryd_samples_mv";
pub const SERIES: &str = "telemetryd_series";
const SERIES_VIEW: &str = "telemetryd_series_mv";

/// `Special`: the value column holds the value itself.
pub const VALUE: u8 = 0;
/// `Special`: a staleness marker, `STALE_MARKER_BITS`.
pub const STALE: u8 = 1;
/// `Special`: any other NaN.
pub const NAN: u8 = 2;
pub const POSITIVE_INFINITY: u8 = 3;
pub const NEGATIVE_INFINITY: u8 = 4;

/// Every statement, in the order they must run.
#[must_use]
pub fn statements(config: &ClickHouseConfig) -> Vec<String> {
    let engine = Engine {
        cluster: config.cluster.trim(),
    };
    let on = engine.on_cluster();
    let days = config.retention_days.max(1);
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {INGEST}{on} (
    Timestamp DateTime64(9, 'UTC'),
    MetricName LowCardinality(String),
    ServiceName LowCardinality(String),
    Series Map(LowCardinality(String), String),
    Kind UInt8,
    Value Float64,
    Special UInt8
) ENGINE = Null"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {SAMPLES}{on} (
    MetricName LowCardinality(String),
    SeriesHash UInt64,
    Timestamp DateTime64(9, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    Value Float64 CODEC(Gorilla, ZSTD(1)),
    Special UInt8 CODEC(ZSTD(1)),
    Kind UInt8 CODEC(ZSTD(1))
) ENGINE = {}
PARTITION BY toDate(Timestamp)
ORDER BY (MetricName, SeriesHash, Timestamp)
TTL toDateTime(Timestamp) + INTERVAL {days} DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
            engine.engine(SAMPLES, "MergeTree")
        ),
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {SAMPLES_VIEW}{on} TO {SAMPLES} AS
SELECT MetricName, cityHash64(Series) AS SeriesHash, Timestamp, Value, Special, Kind
FROM {INGEST}"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {SERIES}{on} (
    Day Date,
    MetricName LowCardinality(String),
    ServiceName LowCardinality(String),
    SeriesHash UInt64,
    Series SimpleAggregateFunction(any, Map(String, String)),
    Samples SimpleAggregateFunction(sum, UInt64),
    MinTimestamp SimpleAggregateFunction(min, DateTime64(9, 'UTC')),
    MaxTimestamp SimpleAggregateFunction(max, DateTime64(9, 'UTC'))
) ENGINE = {}
PARTITION BY Day
ORDER BY (MetricName, Day, SeriesHash)
TTL Day + INTERVAL {days} DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1",
            engine.engine(SERIES, "AggregatingMergeTree")
        ),
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {SERIES_VIEW}{on} TO {SERIES} AS
SELECT
    toDate(Timestamp) AS Day,
    MetricName,
    ServiceName,
    cityHash64(S) AS SeriesHash,
    any(S) AS Series,
    count() AS Samples,
    min(Timestamp) AS MinTimestamp,
    max(Timestamp) AS MaxTimestamp
FROM (SELECT Timestamp, MetricName, ServiceName, Series AS S FROM {INGEST})
GROUP BY Day, MetricName, ServiceName, SeriesHash"
        ),
    ]
}
