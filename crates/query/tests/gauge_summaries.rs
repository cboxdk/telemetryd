//! Gauge functions from segment summaries, and selectors read only where they look.
//!
//! Two shortcuts answer from less than every sample: `sum_over_time`, `count_over_time`,
//! `avg_over_time`, `min_over_time` and `max_over_time` over a window holding whole
//! segments take those segments' summaries, and a range query reads only the lookback
//! window in front of each point. Neither is allowed to change an answer. So every case
//! here is answered twice — by the store, through whichever path it picks, and by the
//! evaluator over every sample held in memory — on generated data built to hit the edges:
//! negative values, NaN, staleness markers, gaps, segments cut anywhere, and late data
//! landing in a segment that overlaps an older one.

#![allow(
    clippy::unwrap_used,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]

use proptest::prelude::*;
use telemetryd_core::config::Config;
use telemetryd_core::metric::STALE_MARKER_BITS;
use telemetryd_core::{Labels, MetricKind, MetricSample};
use telemetryd_query::promeval::{Snapshot, Value};
use telemetryd_query::promql;
use telemetryd_store::Store;

const SECOND: u64 = 1_000_000_000;
const T0: u64 = 1_790_000_000;
/// Six hours at a minute.
const TICKS: u64 = 360;

fn labels(name: &str, instance: usize) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    labels.insert("instance", instance.to_string());
    labels
}

fn at(tick: u64) -> u64 {
    (T0 + tick * 60) * SECOND
}

/// What one generated sample is.
#[derive(Debug, Clone, Copy)]
enum Draw {
    Missing,
    Value(i16),
    Nan,
    Stale,
}

fn draw() -> impl Strategy<Value = Draw> {
    prop_oneof![
        3 => Just(Draw::Missing),
        30 => any::<i16>().prop_map(Draw::Value),
        1 => Just(Draw::Nan),
        1 => Just(Draw::Stale),
    ]
}

#[derive(Debug, Clone)]
struct Case {
    /// Per series, per tick.
    series: Vec<Vec<Draw>>,
    /// Ticks after which the buffer is sealed into a segment.
    seals: Vec<u64>,
    /// Samples appended after everything else, at earlier times: `(series, tick)`.
    late: Vec<(usize, u64)>,
}

fn case() -> impl Strategy<Value = Case> {
    (
        proptest::collection::vec(proptest::collection::vec(draw(), TICKS as usize), 1..4),
        proptest::collection::btree_set(1..TICKS, 2..12),
        proptest::collection::vec((0usize..3, 0..TICKS), 0..3),
    )
        .prop_map(|(series, seals, late)| Case {
            late: late
                .into_iter()
                .filter(|(s, _)| *s < series.len())
                .collect(),
            series,
            seals: seals.into_iter().collect(),
        })
}

fn sample(series: usize, tick: u64, draw: Draw, offset_nanos: u64) -> Option<MetricSample> {
    let value = match draw {
        Draw::Missing => return None,
        Draw::Value(v) => f64::from(v) / 8.0,
        Draw::Nan => f64::NAN,
        Draw::Stale => f64::from_bits(STALE_MARKER_BITS),
    };
    Some(MetricSample {
        series: labels("gauge", series),
        timestamp_nanos: at(tick) + offset_nanos,
        value,
        kind: MetricKind::Gauge,
    })
}

/// The store filled as the case says, and every sample it was given.
fn filled(case: &Case) -> (tempfile::TempDir, Store, Vec<MetricSample>) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    let mut all = Vec::new();
    for tick in 0..TICKS {
        let step: Vec<MetricSample> = case
            .series
            .iter()
            .enumerate()
            .filter_map(|(series, draws)| sample(series, tick, draws[tick as usize], 0))
            .collect();
        store.metrics().append(&step).unwrap();
        all.extend(step);
        if case.seals.contains(&tick) {
            store.metrics().seal_now().unwrap();
        }
    }
    store.metrics().seal_now().unwrap();
    // Late data: half a minute after a tick that has already been sealed, so it never
    // shares a timestamp with a sample already stored.
    let late: Vec<MetricSample> = case
        .late
        .iter()
        .filter_map(|(series, tick)| sample(*series, *tick, Draw::Value(77), 30 * SECOND))
        .collect();
    if !late.is_empty() {
        store.metrics().append(&late).unwrap();
        all.extend(late);
    }
    (dir, store, all)
}

fn answer(value: Value) -> Vec<(Labels, f64)> {
    let mut out = match value {
        Value::Scalar(value) => vec![(Labels::new(), value)],
        Value::Vector(vector) => vector.samples,
    };
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn same(x: f64, y: f64) -> bool {
    (x.is_nan() && y.is_nan())
        || x.to_bits() == y.to_bits()
        || (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0)
}

fn agree(query: &str, points: &[u64], store: &Store, all: &[MetricSample]) -> Result<(), String> {
    let expr = promql::parse(query).unwrap();
    let mut stored = Snapshot::load_at(store.metrics(), &expr, points, 0).unwrap();
    stored.prepare(&expr, points);
    let mut held = Snapshot::from_samples(all.to_vec());
    held.prepare(&expr, points);
    for point in points {
        let got = answer(stored.eval(&expr, *point).unwrap());
        let want = answer(held.eval(&expr, *point).unwrap());
        let equal = got.len() == want.len()
            && got
                .iter()
                .zip(&want)
                .all(|((a, x), (b, y))| a == b && same(*x, *y));
        if !equal {
            return Err(format!(
                "{query} at {point}: store {got:?}, samples {want:?}"
            ));
        }
    }
    Ok(())
}

const GAUGE_FUNCTIONS: [&str; 5] = [
    "sum_over_time",
    "count_over_time",
    "avg_over_time",
    "min_over_time",
    "max_over_time",
];

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, .. ProptestConfig::default() })]

    /// A window holding the whole store, at one instant: the summaries' case.
    #[test]
    fn whole_window_gauge_functions_agree_with_the_samples(case in case()) {
        let (_dir, store, all) = filled(&case);
        let end = at(TICKS) ;
        for function in GAUGE_FUNCTIONS {
            for query in [
                format!("{function}(gauge[7h])"),
                format!("sum({function}(gauge[7h]))"),
                format!("{function}(gauge[3h] offset 90m)"),
            ] {
                agree(&query, &[end], &store, &all).map_err(TestCaseError::fail)?;
            }
        }
        agree(
            "max_over_time(gauge[7h]) - min_over_time(gauge[7h]) + count_over_time(gauge[7h])",
            &[end],
            &store,
            &all,
        )
        .map_err(TestCaseError::fail)?;
    }

    /// Charts: a bare selector and a narrow window at points spread over the span — the
    /// windowed read's case — and a wide window at a few points.
    #[test]
    fn charts_agree_with_the_samples(case in case(), step in 300u64..2_500) {
        let (_dir, store, all) = filled(&case);
        let first = at(10);
        let last = at(TICKS) + 600 * SECOND;
        let points: Vec<u64> = (first..=last).step_by((step * SECOND) as usize).collect();
        for query in [
            "gauge",
            "gauge offset 7m",
            "sum(gauge)",
            "avg_over_time(gauge[10m])",
            "max_over_time(gauge[4m]) - gauge",
            "timestamp(gauge)",
        ] {
            agree(query, &points, &store, &all).map_err(TestCaseError::fail)?;
        }
        let few: Vec<u64> = points.iter().copied().step_by(points.len().div_ceil(3)).collect();
        for function in GAUGE_FUNCTIONS {
            agree(&format!("{function}(gauge[3h])"), &few, &store, &all)
                .map_err(TestCaseError::fail)?;
        }
    }
}

/// Older stores hold summaries in format 1, which carry no sums. A gauge function over
/// them must read the rows rather than trust numbers that are not there, and the backfill
/// must bring them up to the current format.
#[test]
fn a_summary_from_before_gauges_is_not_trusted_and_is_rewritten() {
    let case = Case {
        series: vec![
            (0..TICKS)
                .map(|t| Draw::Value((t % 50) as i16 - 20))
                .collect(),
        ],
        seals: vec![60, 120, 180, 240, 300],
        late: Vec::new(),
    };
    let (dir, store, all) = filled(&case);
    let segments = dir.path().join("segments").join("metrics");
    // Rewrite every summary as format 1: the six counter numbers per stream, nothing more.
    let mut rewritten = 0;
    for entry in std::fs::read_dir(&segments).unwrap() {
        let path = entry.unwrap().path().join("folds.bin");
        let Ok(raw) = std::fs::read(&path) else {
            continue;
        };
        let body = &raw[..raw.len() - 8];
        let count = u64::from_le_bytes(body[8..16].try_into().unwrap()) as usize;
        let mut old = Vec::new();
        old.extend_from_slice(b"TFLD");
        old.extend_from_slice(&1u32.to_le_bytes());
        old.extend_from_slice(&body[8..16]);
        for stream in 0..count {
            let at = 16 + stream * 80;
            old.extend_from_slice(&body[at..at + 48]);
        }
        std::fs::write(&path, &old).unwrap();
        rewritten += 1;
    }
    assert!(rewritten >= 5, "only {rewritten} summaries found to age");
    drop(store);

    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    for query in [
        "max_over_time(gauge[7h])",
        "avg_over_time(gauge[7h])",
        "rate(gauge[7h])",
    ] {
        agree(query, &[at(TICKS)], &store, &all).unwrap();
    }
    let written = store.metrics().backfill_folds(100).unwrap();
    assert!(
        written >= 5,
        "the backfill rewrote {written} of {rewritten}"
    );
    for query in ["max_over_time(gauge[7h])", "avg_over_time(gauge[7h])"] {
        agree(query, &[at(TICKS)], &store, &all).unwrap();
    }
}
