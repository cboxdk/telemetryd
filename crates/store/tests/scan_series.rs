//! `scan_series` hands out a series at a time what the materialising read hands out a row
//! at a time, and `fold_window` folds the same rows whichever way it reaches them.
//!
//! Both read the store a way that skips work: series not asked for are never looked at,
//! buffered records are found through a per-series index, and a segment straddling a
//! window's edge is folded straight from its columns. A mistake in any of those drops or
//! repeats samples, and the number that comes out still looks like a rate.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use telemetryd_core::config::Config;
use telemetryd_core::{LabelMatcher, Labels, MatchOp, MetricKind, MetricSample};
use telemetryd_store::Store;

const SECOND: u64 = 1_000_000_000;

fn config(dir: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.to_path_buf());
    config
}

fn series(name: &str, route: usize) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    labels.insert("route", format!("/r{route}"));
    labels
}

/// A hundred and twenty series across two metric names, every one at every step, the way
/// a histogram's buckets arrive: interleaved, a fresh label set per sample.
fn step(at_secs: u64) -> Vec<MetricSample> {
    (0..120)
        .map(|i| MetricSample {
            series: series(if i % 4 == 0 { "count" } else { "bucket" }, i),
            timestamp_nanos: at_secs * SECOND,
            #[allow(clippy::cast_precision_loss)]
            value: (at_secs * (i as u64 + 1)) as f64,
            kind: MetricKind::Counter,
        })
        .collect()
}

/// Two sealed stretches and a buffered one of six thousand records — more than one
/// chunk — so every path is taken.
fn filled() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();
    let metrics = store.metrics();
    for at in 1..=150 {
        metrics.append(&step(at * 10)).unwrap();
        if at == 50 || at == 100 {
            metrics.seal_now().unwrap();
        }
    }
    (dir, store)
}

type BySeries = BTreeMap<Labels, Vec<(u64, f64)>>;

fn by_scan(store: &Store, windows: &[(u64, u64)], matchers: &[LabelMatcher]) -> BySeries {
    let mut out = BySeries::new();
    let complete = store
        .metrics()
        .scan_series(windows, matchers, &mut |run| {
            assert!(
                run.timestamps.windows(2).all(|pair| pair[0] <= pair[1]),
                "a run is in time order"
            );
            out.entry(run.series.clone()).or_default().extend(
                run.timestamps
                    .iter()
                    .copied()
                    .zip(run.values.iter().copied()),
            );
            std::ops::ControlFlow::Continue(())
        })
        .unwrap();
    assert!(complete);
    out
}

fn by_rows(store: &Store, (start, end): (u64, u64), matchers: &[LabelMatcher]) -> BySeries {
    let mut out = BySeries::new();
    for sample in store
        .metrics()
        .query(start, end, matchers, &|_| true)
        .unwrap()
    {
        out.entry(sample.series)
            .or_default()
            .push((sample.timestamp_nanos, sample.value));
    }
    for samples in out.values_mut() {
        samples.sort_by_key(|(at, _)| *at);
    }
    out
}

fn name_is(name: &str) -> Vec<LabelMatcher> {
    vec![LabelMatcher::new("__name__", MatchOp::Equal, name).unwrap()]
}

#[test]
fn series_runs_hold_exactly_the_rows() {
    let (_dir, store) = filled();
    // Across both seals and into the buffer, starting and ending mid-stretch.
    let span = (205 * SECOND, 1_395 * SECOND);
    for matchers in [
        vec![],
        name_is("count"),
        name_is("bucket"),
        name_is("absent"),
    ] {
        let scanned = by_scan(&store, &[span], &matchers);
        let rows = by_rows(&store, span, &matchers);
        assert_eq!(scanned, rows, "matchers {matchers:?}");
    }
    assert_eq!(by_scan(&store, &[span], &name_is("count")).len(), 30);
}

#[test]
fn a_run_starts_and_ends_at_the_span_it_was_asked_for() {
    let (_dir, store) = filled();
    // The very edges are samples: both ends are included.
    let span = (500 * SECOND, 1_010 * SECOND);
    let scanned = by_scan(&store, &[span], &name_is("count"));
    for samples in scanned.values() {
        assert_eq!(samples.first().unwrap().0, 500 * SECOND);
        assert_eq!(samples.last().unwrap().0, 1_010 * SECOND);
        assert_eq!(samples.len(), 52);
    }
}

#[test]
fn folding_a_window_agrees_with_folding_its_rows() {
    let (_dir, store) = filled();
    // Every segment either straddles an edge or lies inside, and the buffer is in too.
    let mut answered = 0;
    for (start, end) in [
        (0, 2_000),
        (205, 1_395),
        (5, 1_200),
        (400, 1_495),
        (505, 995),
    ] {
        let (start, end) = (start * SECOND, end * SECOND);
        let folded = store
            .metrics()
            .fold_window(start, end, &name_is("count"), 4)
            .unwrap();
        let Some(folded) = folded else {
            // Declined is allowed; a different answer is not.
            continue;
        };
        answered += 1;
        let rows = by_rows(&store, (start + 1, end), &name_is("count"));
        assert_eq!(folded.len(), rows.len());
        for (labels, fold) in folded {
            let samples = &rows[&labels];
            let increase: f64 = samples.windows(2).map(|pair| pair[1].1 - pair[0].1).sum();
            assert_eq!(fold.seen, samples.len() as u64, "{labels:?}");
            assert_eq!(fold.first_nanos, samples[0].0);
            assert_eq!(fold.last_nanos, samples.last().unwrap().0);
            assert!((fold.increase - increase).abs() < 1e-9, "{labels:?}");
        }
    }
    assert!(
        answered >= 3,
        "the shortcut answered only {answered} of the windows"
    );
}

/// Late data: a series' buffered samples reach back before its last sealed one. A fold
/// cannot take that in, so the shortcut must decline rather than read the step back as a
/// counter reset.
#[test]
fn late_buffered_samples_decline_the_fold() {
    let (_dir, store) = filled();
    store.metrics().append(&step(600)).unwrap();
    let folded = store
        .metrics()
        .fold_window(0, 2_000 * SECOND, &name_is("count"), 4)
        .unwrap();
    assert!(folded.is_none(), "{folded:?}");
}

/// A sealed metric segment lies series by series: streams in label-set order, each
/// stream's rows one stretch in time order, and the per-stream counts locating them.
#[test]
fn a_sealed_metric_segment_lies_series_by_series() {
    let (_dir, store) = filled();
    let segments = store.metrics().segments();
    assert_eq!(segments.len(), 2);
    for segment in segments {
        let manifest = &segment.manifest;
        assert!(manifest.stream_major);
        assert!(manifest.streams.windows(2).all(|pair| pair[0] < pair[1]));

        let rows = segment.read::<telemetryd_store::MetricSchema>().unwrap();
        let mut at = 0usize;
        for (stream, count) in manifest.streams.iter().zip(&manifest.stream_rows) {
            let run = &rows[at..at + *count as usize];
            assert!(run.iter().all(|sample| &sample.series == stream));
            assert!(
                run.windows(2)
                    .all(|pair| pair[0].timestamp_nanos < pair[1].timestamp_nanos)
            );
            at += *count as usize;
        }
        assert_eq!(at, rows.len());
    }
}

/// Segments sealed before the series-by-series layout carry no flag and are read by time
/// range. A store holding such segments beside new ones answers exactly the same.
#[test]
fn a_segment_without_the_layout_flag_is_read_by_time_range() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(&config(dir.path())).unwrap();
        for at in 1..=100 {
            store.metrics().append(&step(at * 10)).unwrap();
            if at == 50 {
                store.metrics().seal_now().unwrap();
            }
        }
        store.metrics().seal_now().unwrap();
    }
    let segments = dir.path().join("segments").join("metrics");
    let first = std::fs::read_dir(&segments)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .min()
        .unwrap();
    let manifest = first.join("manifest.json");
    let written = std::fs::read_to_string(&manifest).unwrap();
    let flagged = "\"stream_major\": true";
    assert!(written.contains(flagged), "{written}");
    std::fs::write(
        &manifest,
        written.replace(flagged, "\"stream_major\": false"),
    )
    .unwrap();

    let store = Store::open(&config(dir.path())).unwrap();
    let flags: Vec<bool> = store
        .metrics()
        .segments()
        .iter()
        .map(|segment| segment.manifest.stream_major)
        .collect();
    assert_eq!(flags.iter().filter(|flag| !**flag).count(), 1, "{flags:?}");
    let span = (105 * SECOND, 995 * SECOND);
    for matchers in [vec![], name_is("count")] {
        assert_eq!(
            by_scan(&store, &[span], &matchers),
            by_rows(&store, span, &matchers)
        );
    }
}

/// One series out of the middle of a segment laid out series by series: a few rows the
/// row selection has to find inside pages holding others.
#[test]
fn one_series_from_the_middle_of_a_segment() {
    let (_dir, store) = filled();
    let span = (205 * SECOND, 1_395 * SECOND);
    for route in [0, 7, 57, 119] {
        let matchers =
            vec![LabelMatcher::new("route", MatchOp::Equal, format!("/r{route}")).unwrap()];
        let scanned = by_scan(&store, &[span], &matchers);
        assert_eq!(scanned.len(), 1, "route {route}");
        assert_eq!(scanned, by_rows(&store, span, &matchers), "route {route}");
    }
}
