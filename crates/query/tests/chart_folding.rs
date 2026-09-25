//! A chart is many points over a narrow window, spread across a wide span.
//!
//! That shape used to be refused: the span was read in one piece, so a day of histogram
//! buckets exceeded what one query may hold and the panel showed a 400 while the same
//! panel at one hour was fine. It is folded now, and folding is only allowed to answer a
//! query if it gives the same numbers the ordinary read gives.

#![allow(clippy::unwrap_used)]

use telemetryd_core::config::Config;
use telemetryd_core::{Labels, MetricKind, MetricSample};
use telemetryd_query::promeval::{Snapshot, Value};
use telemetryd_query::promql;
use telemetryd_store::Store;

const SECOND: u64 = 1_000_000_000;

fn config(dir: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.to_path_buf());
    // Several segments across the span, so the read really is sliced.
    config.storage.segment_duration =
        telemetryd_core::config::DurationSetting(std::time::Duration::from_secs(600));
    config
}

/// Six hours of two counters at half-minute resolution, one of them restarting partway
/// through — the span is wide enough to fold and the window is not.
fn samples() -> Vec<MetricSample> {
    let mut out = Vec::new();
    for tick in 1..=720u64 {
        for (route, scale) in [("/a", 3.0), ("/b", 11.0)] {
            let mut series = Labels::new();
            series.insert("__name__", "probe");
            series.insert("route", route);
            #[allow(clippy::cast_precision_loss)]
            let raw = tick as f64 * scale;
            // `/b` restarts at the halfway mark, which is what a deploy looks like.
            let value = if route == "/b" && tick > 360 {
                raw - 360.0 * scale
            } else {
                raw
            };
            out.push(MetricSample {
                series,
                timestamp_nanos: tick * 30 * SECOND,
                value,
                kind: MetricKind::Counter,
            });
        }
    }
    out
}

#[test]
fn a_chart_over_a_wide_span_agrees_with_the_ordinary_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();

    let rows = samples();
    for sample in &rows {
        store
            .metrics()
            .append(std::slice::from_ref(sample))
            .unwrap();
    }
    store.metrics().seal_now().ok();

    // What the UI asks for: 250 points across the whole span, a fifteen-minute rate
    // window, and a span of six hours.
    let expr = promql::parse("sum by (route) (rate(probe[15m]))").unwrap();
    let first = 30 * SECOND;
    let last = 720 * 30 * SECOND;
    let step = (last - first) / 249;
    let points: Vec<u64> = (0..250).map(|i| first + i * step).collect();

    // An allowance the ordinary read cannot meet and the fold can. 1,440 samples lie in
    // the span, so the ordinary read is refused. The fold holds one call by two series by
    // 250 points — 500 cells, more than the allowance counted in items, and well under it
    // counted in memory, which is what the allowance is actually about. Answering here
    // proves both that the folded path was taken and that a cell is charged for what it
    // costs rather than for what a sample costs.
    let allowance = 300;
    let folded = Snapshot::load_at(store.metrics(), &expr, &points, allowance).unwrap();

    let mut walked = Snapshot::from_samples(rows);
    walked.prepare(&expr, &points);

    let mut compared = 0;
    for at in &points {
        let from_fold = folded.eval(&expr, *at).unwrap();
        let from_rows = walked.eval(&expr, *at).unwrap();
        let (Value::Vector(a), Value::Vector(b)) = (from_fold, from_rows) else {
            panic!("a rate aggregation evaluates to a vector");
        };
        let (mut a, mut b) = (a.samples, b.samples);
        a.sort_by(|x, y| x.0.cmp(&y.0));
        b.sort_by(|x, y| x.0.cmp(&y.0));
        assert_eq!(a.len(), b.len(), "series count at {at}: {a:?} vs {b:?}");
        for (one, two) in a.iter().zip(b.iter()) {
            assert_eq!(one.0, two.0, "labels at {at}");
            let (x, y) = (one.1, two.1);
            assert!(
                (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0),
                "at {at}: folded {x} vs walked {y}"
            );
            compared += 1;
        }
    }
    assert!(compared > 400, "only {compared} values compared");
}

/// Folding answers `rate` and `increase`, and a bare selector is neither.
///
/// A selector standing on its own wants the newest sample inside a lookback, which no
/// fold carries. Folding it would quietly drop the operand, so a wide span is not on its
/// own a reason to fold — the expression has to be one a fold can answer.
#[test]
fn a_bare_selector_is_read_the_ordinary_way_however_wide_the_span() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();

    let rows = samples();
    for sample in &rows {
        store
            .metrics()
            .append(std::slice::from_ref(sample))
            .unwrap();
    }
    store.metrics().seal_now().ok();

    let expr = promql::parse("probe").unwrap();
    let first = 30 * SECOND;
    let last = 720 * 30 * SECOND;
    let step = (last - first) / 249;
    let points: Vec<u64> = (0..250).map(|i| first + i * step).collect();

    let loaded = Snapshot::load_at(store.metrics(), &expr, &points, 0).unwrap();
    let mut walked = Snapshot::from_samples(rows);
    walked.prepare(&expr, &points);

    let at = points[249];
    let (Value::Vector(from_store), Value::Vector(from_rows)) = (
        loaded.eval(&expr, at).unwrap(),
        walked.eval(&expr, at).unwrap(),
    ) else {
        panic!("a selector evaluates to a vector");
    };

    // Both series, with the value each last held. Folding this expression would have
    // dropped the operand and returned nothing at all, which is the failure being kept out.
    let (mut a, mut b) = (from_store.samples, from_rows.samples);
    a.sort_by(|x, y| x.0.cmp(&y.0));
    b.sort_by(|x, y| x.0.cmp(&y.0));
    assert_eq!(a.len(), 2, "got {a:?}");
    assert_eq!(a, b, "the store and the samples disagree");
}
