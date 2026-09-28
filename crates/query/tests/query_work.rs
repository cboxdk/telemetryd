//! How much of the store a dashboard's queries read, against how much they need.
//!
//! A day of one app's request histogram is 850 series — 50 route and method pairs, each
//! with 15 buckets, a count and a sum — and a dashboard's cheap panels ask about the 50
//! counts. They used to read all 850: every row of every segment in the window, and every
//! buffered record, to keep one row in seventeen. Time budgets make flaky tests, so this
//! counts rows instead: what the store handed a query, against what the query needed.
//! If a change makes these queries read everything again, it fails here, on any machine.

#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use telemetryd_core::config::Config;
use telemetryd_core::{Labels, MetricKind, MetricSample};
use telemetryd_query::prometheus::{InstantParams, RangeParams, instant, range};
use telemetryd_store::Store;

const SECOND: u64 = 1_000_000_000;
const T0: u64 = 1_790_000_000;
/// Six hours at thirty seconds, sealed hourly for five and the last left buffered.
const TICKS: u64 = 720;
const PAIRS: u64 = 50;
const BOUNDS: [&str; 15] = [
    "0.005", "0.01", "0.025", "0.05", "0.075", "0.1", "0.25", "0.5", "0.75", "1", "2.5", "5",
    "7.5", "10", "+Inf",
];

fn series(name: &str, pair: u64, le: Option<&str>) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    labels.insert("service_name", "geocodio-api");
    labels.insert("http_route", format!("/api/{}", pair / 2));
    labels.insert(
        "http_request_method",
        if pair.is_multiple_of(2) {
            "GET"
        } else {
            "POST"
        },
    );
    if let Some(le) = le {
        labels.insert("le", le);
    }
    labels
}

fn filled() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    for tick in 1..=TICKS {
        let at = (T0 + tick * 30) * SECOND;
        let mut step = Vec::new();
        for pair in 0..PAIRS {
            let total = (tick * (pair + 5)) as f64;
            let mut sample = |series: Labels, value: f64| {
                step.push(MetricSample {
                    series,
                    timestamp_nanos: at,
                    value,
                    kind: MetricKind::Counter,
                });
            };
            for (i, le) in BOUNDS.iter().enumerate() {
                sample(
                    series("latency_bucket", pair, Some(le)),
                    total * (i + 1) as f64 / 15.0,
                );
            }
            sample(series("latency_count", pair, None), total);
            sample(series("latency_sum", pair, None), total * 0.25);
        }
        store.metrics().append(&step).unwrap();
        if tick % 120 == 0 && tick < TICKS {
            store.metrics().seal_now().unwrap();
        }
    }
    (dir, store)
}

/// Rows the store handed out while `query` ran.
fn rows_read(store: &Store, query: impl FnOnce()) -> u64 {
    let before = store.metrics().status().rows_read;
    query();
    store.metrics().status().rows_read - before
}

fn chart(store: &Store, query: &str) {
    let params = RangeParams {
        query: Some(query.to_owned()),
        start: Some((T0 + 30).to_string()),
        end: Some((T0 + TICKS * 30).to_string()),
        step: Some("86".to_owned()),
        timeout: None,
    };
    let answer = range(store.metrics(), &params, 0, 0).unwrap();
    assert!(!answer.data.result.is_empty(), "{query}");
}

fn at_end(store: &Store, query: &str) {
    let params = InstantParams {
        query: Some(query.to_owned()),
        time: Some((T0 + TICKS * 30).to_string()),
        timeout: None,
    };
    let answer = instant(store.metrics(), &params, 0, 0).unwrap();
    assert!(!answer.data.result.samples().is_empty(), "{query}");
}

/// Everything in the store: what reading all of it would cost.
const ALL_ROWS: u64 = TICKS * PAIRS * 17;
/// One metric name's rows: what a query about the counts needs.
const COUNT_ROWS: u64 = TICKS * PAIRS;

#[test]
fn a_chart_of_one_metric_reads_that_metric() {
    let (_dir, store) = filled();
    for query in [
        "sum(rate(latency_count[15m]))",
        "sum by (http_route, http_request_method) (rate(latency_count[15m])) * 60",
        "sum(rate(latency_sum[15m]))",
    ] {
        let read = rows_read(&store, || chart(&store, query));
        assert!(
            read <= COUNT_ROWS * 11 / 10,
            "{query} read {read} rows for {COUNT_ROWS} it needed, of {ALL_ROWS} in the store"
        );
    }
}

#[test]
fn a_total_over_the_whole_window_reads_little_more_than_its_edges() {
    let (_dir, store) = filled();
    // Five sealed hours inside the window answer from their summaries; only the buffered
    // hour and the segment straddling the start are read.
    let read = rows_read(&store, || {
        at_end(&store, "sum(increase(latency_count[5h30m]))");
    });
    assert!(
        read <= COUNT_ROWS / 3,
        "read {read} rows; the counts are {COUNT_ROWS}, the store {ALL_ROWS}"
    );
}

#[test]
fn a_quantile_chart_reads_the_buckets_and_nothing_else() {
    let (_dir, store) = filled();
    let read = rows_read(&store, || {
        chart(
            &store,
            "histogram_quantile(0.95, sum by (le) (rate(latency_bucket[15m])))",
        );
    });
    let buckets = TICKS * PAIRS * 15;
    assert!(
        read <= buckets * 11 / 10,
        "read {read} rows for {buckets} buckets, of {ALL_ROWS} in the store"
    );
}
