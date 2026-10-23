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
//! - **`telemetryd_sample_counts`**: samples per series per hour, staleness markers left
//!   out as a fold leaves them out, kept by a view. What says whether a fold below still
//!   describes every sample of its hour, or late data has arrived since it was made.
//! - **`telemetryd_folds`**: a counter-and-gauge summary per series per hour (tier 1)
//!   and per day (tier 2), the same summary a sealed local segment carries, made by
//!   telemetryd from the samples with the local store's own arithmetic. A window of
//!   whole hours or days is answered from these instead of from every sample in it.
//!
//! Every table is partitioned by day, the summaries and counts as much as the samples:
//! with `ttl_only_drop_parts` a row leaves when its whole partition has expired, so a
//! coarser partition would keep a month's summaries past the retention they belong to.
//! Times are UTC throughout, whatever the server's own timezone is.
//!
//! Hours and days here are half open the way a PromQL window is: bucket `k` holds the
//! samples in `(k·width, (k+1)·width]`, so a window whose bounds fall on bucket bounds
//! is exactly the buckets inside it.

use super::schema::Layout;

pub const INGEST: &str = "telemetryd_metrics_ingest";
pub const SAMPLES: &str = "telemetryd_samples";
const SAMPLES_VIEW: &str = "telemetryd_samples_mv";
pub const SERIES: &str = "telemetryd_series";
const SERIES_VIEW: &str = "telemetryd_series_mv";
pub const COUNTS: &str = "telemetryd_sample_counts";
const COUNTS_VIEW: &str = "telemetryd_sample_counts_mv";
pub const FOLDS: &str = "telemetryd_folds";

/// `Special`: the value column holds the value itself.
pub const VALUE: u8 = 0;
/// `Special`: a staleness marker, `STALE_MARKER_BITS`.
pub const STALE: u8 = 1;
/// `Special`: any other NaN.
pub const NAN: u8 = 2;
pub const POSITIVE_INFINITY: u8 = 3;
pub const NEGATIVE_INFINITY: u8 = 4;

pub const HOUR_NANOS: u64 = 3_600_000_000_000;
pub const DAY_NANOS: u64 = 24 * HOUR_NANOS;
/// `Tier` of an hourly fold.
pub const HOURLY: u8 = 1;
/// `Tier` of a daily fold.
pub const DAILY: u8 = 2;

/// The bucket of `width` holding a sample at `nanos`: `(k·width, (k+1)·width]`.
#[must_use]
pub fn bucket_of(nanos: u64, width: u64) -> u64 {
    nanos.saturating_sub(1) / width
}

/// Every statement, in the order they must run, keeping `retention` of metrics.
#[must_use]
pub(super) fn statements(layout: &Layout<'_>) -> Vec<String> {
    let engine = layout;
    let on = engine.on_cluster();
    let (samples_ttl, series_ttl) = (layout.aging(SAMPLES), layout.aging(SERIES));
    let (counts_ttl, folds_ttl) = (layout.aging(COUNTS), layout.aging(FOLDS));
    let settings = layout.settings();
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
{samples_ttl}
{settings}",
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
{series_ttl}
{settings}",
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
    .into_iter()
    .chain(summaries(engine, &counts_ttl, &folds_ttl))
    .collect()
}

/// The sample counts and the summaries checked against them.
fn summaries(engine: &Layout<'_>, counts_ttl: &str, folds_ttl: &str) -> Vec<String> {
    let on = engine.on_cluster();
    let settings = engine.settings();
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {COUNTS}{on} (
    MetricName LowCardinality(String),
    Hour UInt32,
    SeriesHash UInt64,
    Samples SimpleAggregateFunction(sum, UInt64)
) ENGINE = {}
PARTITION BY intDiv(Hour, 24)
ORDER BY (MetricName, Hour, SeriesHash)
{counts_ttl}
{settings}",
            engine.engine(COUNTS, "AggregatingMergeTree")
        ),
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {COUNTS_VIEW}{on} TO {COUNTS} AS
SELECT
    MetricName,
    toUInt32(intDiv(toUnixTimestamp64Nano(Timestamp) - 1, {HOUR_NANOS})) AS Hour,
    cityHash64(S) AS SeriesHash,
    countIf(Special != {STALE}) AS Samples
FROM (SELECT Timestamp, MetricName, Series AS S, Special FROM {INGEST})
GROUP BY MetricName, Hour, SeriesHash"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {FOLDS}{on} (
    MetricName LowCardinality(String),
    Tier UInt8,
    Bucket UInt32,
    SeriesHash UInt64,
    Starts DateTime('UTC') CODEC(Delta(4), ZSTD(1)),
    Seen UInt64,
    FirstNanos UInt64,
    LastNanos UInt64,
    FirstValue UInt64,
    LastValue UInt64,
    Increase UInt64,
    Sum UInt64,
    Compensation UInt64,
    Min UInt64,
    Max UInt64,
    Version UInt64
) ENGINE = {}
PARTITION BY (Tier, toDate(Starts))
ORDER BY (MetricName, Tier, Bucket, SeriesHash)
{folds_ttl}
{settings}",
            versioned(&engine.engine(FOLDS, "ReplacingMergeTree"))
        ),
    ]
}

/// A `ReplacingMergeTree` engine clause that keeps the highest `Version`: the engine
/// takes it as its last argument, after a replicated one's path and replica.
fn versioned(engine: &str) -> String {
    match engine.strip_suffix("()") {
        Some(name) => format!("{name}(Version)"),
        None => match engine.strip_suffix(')') {
            Some(open) => format!("{open}, Version)"),
            None => engine.to_owned(),
        },
    }
}
