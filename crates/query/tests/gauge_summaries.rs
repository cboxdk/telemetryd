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
    /// `±1e308`: two of the same sign overflow, which is where adding a window up in
    /// segment order and in time order part ways.
    Huge(bool),
}

fn draw() -> impl Strategy<Value = Draw> {
    prop_oneof![
        3 => Just(Draw::Missing),
        30 => any::<i16>().prop_map(Draw::Value),
        1 => Just(Draw::Nan),
        1 => Just(Draw::Stale),
        1 => any::<bool>().prop_map(Draw::Huge),
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
        Draw::Huge(positive) => {
            if positive {
                1e308
            } else {
                -1e308
            }
        }
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

/// The case that a finite join of finite segment sums got wrong: in time order the
/// window's sum overflows and stays infinite, as Prometheus's does; segment by segment
/// each part is finite, and so was their join.
#[test]
fn a_sum_that_overflows_in_time_order_is_not_taken_from_summaries() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    let point = |tick: u64, value: f64| MetricSample {
        series: labels("gauge", 0),
        timestamp_nanos: at(tick),
        value,
        kind: MetricKind::Gauge,
    };
    let all = vec![
        point(1, 1e308),
        point(2, 1e308),
        point(3, -1e308),
        point(4, 1.0),
    ];
    store.metrics().append(&all[..1]).unwrap();
    store.metrics().seal_now().unwrap();
    store.metrics().append(&all[1..3]).unwrap();
    store.metrics().seal_now().unwrap();
    store.metrics().append(&all[3..]).unwrap();

    for query in ["sum_over_time(gauge[3h])", "avg_over_time(gauge[3h])"] {
        agree(query, &[at(10)], &store, &all).unwrap();
    }
    let expr = promql::parse("sum_over_time(gauge[3h])").unwrap();
    let snapshot = Snapshot::load_at(store.metrics(), &expr, &[at(10)], 0).unwrap();
    let got = answer(snapshot.eval(&expr, at(10)).unwrap());
    assert!(got[0].1.is_infinite() && got[0].1 > 0.0, "{got:?}");
}

fn rows_read(store: &Store, query: impl FnOnce()) -> u64 {
    let before = store.metrics().status().rows_read;
    query();
    store.metrics().status().rows_read - before
}

/// A gauge that ever held a NaN — momentarily undefined, not ended — still takes its
/// segments' summaries for the count, least and greatest. The check against NaN in a
/// summary is for counter summaries written before staleness markers were skipped.
#[test]
fn a_gauge_that_held_a_nan_still_takes_its_summaries() {
    let case = Case {
        series: vec![
            (0..TICKS)
                .map(|t| {
                    if t % 7 == 3 {
                        Draw::Nan
                    } else {
                        Draw::Value((t % 50) as i16 - 20)
                    }
                })
                .collect(),
        ],
        seals: (1..12).map(|i| i * 30).collect(),
        late: Vec::new(),
    };
    let (_dir, store, all) = filled(&case);
    for function in ["max_over_time", "min_over_time", "count_over_time"] {
        let query = format!("{function}(gauge[7h])");
        let read = rows_read(&store, || {
            agree(&query, &[at(TICKS)], &store, &all).unwrap();
        });
        // `agree` loads once from the store; reading every row would be all 360.
        assert!(read < TICKS / 4, "{query} read {read} of {TICKS} rows");
    }
}

/// A chart keeps only the samples in front of its points. Held to the query sample
/// allowance, which counts what a read keeps: a bare gauge chart whose span holds three
/// times the allowance and whose lookback windows hold half of it is answered — and is
/// refused, as it used to be, if the whole span is read again.
#[test]
fn a_chart_holds_only_the_samples_its_points_look_at() {
    let case = Case {
        series: (0..3)
            .map(|s| {
                (0..TICKS)
                    .map(|t| Draw::Value((t * (s + 1)) as i16))
                    .collect()
            })
            .collect(),
        seals: vec![120, 240],
        late: Vec::new(),
    };
    let (_dir, store, all) = filled(&case);
    // 3 series × 360 samples in the span; a point every twelve minutes looks back five,
    // which holds 3 × 5 × 30 = 450 of them.
    let points: Vec<u64> = (1..=30).map(|i| at(i * 12)).collect();
    let allowance = 600;
    assert!(all.len() as u64 > allowance);
    for query in ["gauge", "max_over_time(gauge[4m])"] {
        let expr = promql::parse(query).unwrap();
        let mut stored = Snapshot::load_at(store.metrics(), &expr, &points, allowance)
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        stored.prepare(&expr, &points);
        agree(query, &points, &store, &all).unwrap();
    }
}

/// A current-format summary that fails its checksum is forgotten when a query finds it,
/// so the backfill writes it again instead of every query paying for the miss.
#[test]
fn a_damaged_summary_is_forgotten_and_rewritten() {
    let case = Case {
        series: vec![(0..TICKS).map(|t| Draw::Value((t % 50) as i16)).collect()],
        seals: vec![60, 120, 180, 240, 300],
        late: Vec::new(),
    };
    let (dir, store, all) = filled(&case);
    let mut damaged = 0;
    for entry in std::fs::read_dir(dir.path().join("segments").join("metrics")).unwrap() {
        let path = entry.unwrap().path().join("folds.bin");
        if let Ok(mut raw) = std::fs::read(&path) {
            // Past the header, which still says "format 2"; the checksum catches it.
            raw[20] ^= 0xff;
            std::fs::write(&path, &raw).unwrap();
            damaged += 1;
        }
    }
    assert!(damaged >= 5);
    drop(store);
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    agree("max_over_time(gauge[7h])", &[at(TICKS)], &store, &all).unwrap();
    let written = store.metrics().backfill_folds(100).unwrap();
    assert!(written >= 5, "rewrote {written} of {damaged}");
    agree("max_over_time(gauge[7h])", &[at(TICKS)], &store, &all).unwrap();
}
