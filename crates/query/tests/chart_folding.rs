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

/// Two counters under one name, told apart only by a label.
fn by_code() -> Vec<MetricSample> {
    let mut out = Vec::new();
    for tick in 1..=720u64 {
        for (code, per_tick) in [("500", 1.0), ("200", 10.0)] {
            let mut series = Labels::new();
            series.insert("__name__", "reqs");
            series.insert("code", code);
            #[allow(clippy::cast_precision_loss)]
            let value = tick as f64 * per_tick;
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

fn compare(store: &Store, rows: Vec<MetricSample>, query: &str, points: &[u64]) -> usize {
    let expr = promql::parse(query).unwrap();
    let loaded = Snapshot::load_at(store.metrics(), &expr, points, 0).unwrap();
    let mut walked = Snapshot::from_samples(rows);
    walked.prepare(&expr, points);
    let mut compared = 0;
    for at in points {
        let (Value::Vector(a), Value::Vector(b)) = (
            loaded.eval(&expr, *at).unwrap(),
            walked.eval(&expr, *at).unwrap(),
        ) else {
            panic!("{query} evaluates to a vector");
        };
        let (mut a, mut b) = (a.samples, b.samples);
        a.sort_by(|x, y| x.0.cmp(&y.0));
        b.sort_by(|x, y| x.0.cmp(&y.0));
        assert_eq!(a.len(), b.len(), "{query} at {at}: {a:?} vs {b:?}");
        for (one, two) in a.iter().zip(b.iter()) {
            assert_eq!(one.0, two.0, "{query} labels at {at}");
            assert!(
                (one.1 - two.1).abs() <= 1e-9 * one.1.abs().max(two.1.abs()).max(1.0),
                "{query} at {at}: store {} vs rows {}",
                one.1,
                two.1
            );
            compared += 1;
        }
    }
    compared
}

/// Selectors that share a name and differ by a label have to stay apart when folded.
///
/// The read uses only the matchers every selector shares, so both sides of an error
/// ratio arrive together. Folding used to add every sample to every call, which made
/// `errors / all` read as exactly 1 on any chart over two hours and on any window wider
/// than that — the standard error-rate panel, confidently wrong.
#[test]
fn an_error_ratio_is_folded_per_selector() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();
    let rows = by_code();
    for sample in &rows {
        store
            .metrics()
            .append(std::slice::from_ref(sample))
            .unwrap();
    }
    store.metrics().seal_now().ok();

    let first = 30 * SECOND;
    let last = 720 * 30 * SECOND;
    let step = (last - first) / 249;
    let chart: Vec<u64> = (0..250).map(|i| first + i * step).collect();

    // A chart: 250 points across six hours, so the sliced fold answers it.
    let ratio = r#"sum(rate(reqs{code="500"}[15m])) / sum(rate(reqs[15m]))"#;
    assert!(compare(&store, rows.clone(), ratio, &chart) > 200);

    // One point over a three-hour window, so the per-segment summaries answer it.
    let wide = r#"sum(rate(reqs{code="500"}[3h])) / sum(rate(reqs[3h]))"#;
    assert_eq!(compare(&store, rows.clone(), wide, &[last]), 1);

    // And the number itself, not only agreement: one error in every eleven requests.
    let expr = promql::parse(wide).unwrap();
    let loaded = Snapshot::load_at(store.metrics(), &expr, &[last], 0).unwrap();
    let Value::Vector(vector) = loaded.eval(&expr, last).unwrap() else {
        panic!("a ratio evaluates to a vector");
    };
    let value = vector.samples[0].1;
    assert!((value - 1.0 / 11.0).abs() < 1e-9, "ratio {value}");
}

/// Late data lands in a newer segment whose time range overlaps an older one.
///
/// Joining per-segment summaries in segment order then reads the step back to the older
/// value as a counter reset, and the total comes out more than twice too large. The
/// store has to notice the overlap and let the query read the rows, which it sorts.
#[test]
fn late_data_is_not_read_as_a_counter_reset() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();

    let point = |minute: u64| {
        let mut series = Labels::new();
        series.insert("__name__", "late_total");
        #[allow(clippy::cast_precision_loss)]
        let value = minute as f64 * 3.0;
        MetricSample {
            series,
            timestamp_nanos: minute * 60 * SECOND,
            value,
            kind: MetricKind::Counter,
        }
    };

    // Minutes 1–60 and 121–180 arrive on time; 61–120 were buffered by an agent during
    // an outage and arrive last, into a segment of their own.
    let mut rows = Vec::new();
    for minute in (1..=60u64).chain(121..=180) {
        rows.push(point(minute));
        store.metrics().append(&[point(minute)]).unwrap();
    }
    store.metrics().seal_now().ok();
    for minute in 61..=120u64 {
        rows.push(point(minute));
        store.metrics().append(&[point(minute)]).unwrap();
    }
    store.metrics().seal_now().ok();

    let end = 180 * 60 * SECOND;
    assert_eq!(compare(&store, rows, "increase(late_total[3h])", &[end]), 1);
}

/// What a range query returns is bounded like what it reads. Two series scraped every
/// thirty seconds are fourteen hundred samples to load, and at a three-second step they
/// are fourteen thousand points to return — each held, formatted and serialised.
#[test]
fn the_answer_is_bounded_like_the_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();
    for sample in &samples() {
        store
            .metrics()
            .append(std::slice::from_ref(sample))
            .unwrap();
    }
    store.metrics().seal_now().ok();

    let params = |step: &str| -> telemetryd_query::prometheus::RangeParams {
        serde_json::from_value(serde_json::json!({
            "query": "probe",
            "start": "30",
            "end": "21600",
            "step": step,
        }))
        .unwrap()
    };
    let now = 21_600 * SECOND;

    let err = telemetryd_query::prometheus::range(store.metrics(), &params("3"), now, 5_000)
        .expect_err("14,400 points against an allowance of 5,000");
    assert!(err.to_string().contains("would return more than"), "{err}");

    telemetryd_query::prometheus::range(store.metrics(), &params("60"), now, 5_000)
        .expect("720 points fit");
}

/// Three counters at one-second resolution, so a segment spans several Parquet row groups and a sparse chart can skip some of them.
fn dense(ticks: u64) -> Vec<MetricSample> {
    let mut out = Vec::new();
    for tick in 1..=ticks {
        for (pod, per_tick) in [("a", 1.0), ("b", 2.5), ("c", 7.0)] {
            let mut series = Labels::new();
            series.insert("__name__", "dense_total");
            series.insert("pod", pod);
            #[allow(clippy::cast_precision_loss)]
            let raw = tick as f64 * per_tick;
            // `c` restarts every ten thousand ticks, when its value passes 70,000.
            let value = if pod == "c" { raw % 70_000.0 } else { raw };
            out.push(MetricSample {
                series,
                timestamp_nanos: tick * SECOND,
                value,
                kind: MetricKind::Counter,
            });
        }
    }
    out
}

/// A chart whose windows are far apart reads only what they cover — and gives the
/// answer reading everything gives, over sealed row groups and the unsealed buffer.
#[test]
fn a_sparse_chart_agrees_with_the_ordinary_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();
    let rows = dense(40_000);
    let (sealed, buffered) = rows.split_at(90_000);
    store.metrics().append(sealed).unwrap();
    store.metrics().seal_now().unwrap();
    store.metrics().append(buffered).unwrap();

    // A point every twenty minutes, each wanting one minute: a twentieth of the span.
    let chart: Vec<u64> = (1..=33).map(|i| i * 1_200 * SECOND).collect();
    for query in [
        "rate(dense_total[1m])",
        "sum(increase(dense_total[1m]))",
        r#"sum(rate(dense_total{pod="c"}[1m])) / sum(rate(dense_total[1m]))"#,
    ] {
        assert!(
            compare(&store, rows.clone(), query, &chart) >= 33,
            "{query}"
        );
    }
}

/// Late data in a chart: segments overlap in time, so a series' samples arrive out of
/// order across them, and the fold has to notice and read them sorted.
#[test]
fn a_chart_over_late_data_agrees_with_the_ordinary_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();
    let rows = samples();
    // The second half arrives first, then the first half, then a tail left unsealed.
    let (early, late) = rows.split_at(720);
    let (late, tail) = late.split_at(late.len() - 40);
    store.metrics().append(late).unwrap();
    store.metrics().seal_now().unwrap();
    store.metrics().append(early).unwrap();
    store.metrics().seal_now().unwrap();
    store.metrics().append(tail).unwrap();

    let first = 30 * SECOND;
    let last = 720 * 30 * SECOND;
    let step = (last - first) / 249;
    let chart: Vec<u64> = (0..250).map(|i| first + i * step).collect();
    assert!(
        compare(
            &store,
            rows.clone(),
            "sum by (route) (rate(probe[15m]))",
            &chart
        ) > 400
    );
}
