//! `fold_window` has to give the same answer as walking every row.
//!
//! It is the one place a wrong number would look entirely plausible: a counter total over
//! a week, computed from per-segment summaries, has nothing about it that says whether the
//! segments were joined correctly.

#![allow(clippy::unwrap_used)]

use telemetryd_core::config::Config;
use telemetryd_core::{Labels, MetricKind, MetricSample};
use telemetryd_store::Store;

fn config(dir: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.to_path_buf());
    // Small enough that the samples below land in several segments.
    config.storage.segment_duration =
        telemetryd_core::config::DurationSetting(std::time::Duration::from_secs(300));
    config
}

fn sample(route: &str, at_secs: u64, value: f64) -> MetricSample {
    let mut series = Labels::new();
    series.insert("__name__", "probe");
    series.insert("route", route);
    MetricSample {
        series,
        timestamp_nanos: at_secs * 1_000_000_000,
        value,
        kind: MetricKind::Counter,
    }
}

#[test]
fn summaries_agree_with_walking_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();

    // Half an hour at a minute's resolution, across several five-minute segments, with a
    // counter reset partway through — the case a naive join gets wrong.
    let mut expected_increase = 0.0;
    let mut previous: Option<f64> = None;
    for minute in 1..=30u64 {
        let value = if minute <= 17 {
            f64::from(u32::try_from(minute).unwrap()) * 7.0
        } else {
            f64::from(u32::try_from(minute).unwrap())
        };
        if let Some(previous) = previous {
            expected_increase += if value < previous {
                value
            } else {
                value - previous
            };
        }
        previous = Some(value);
        store
            .metrics()
            .append(&[sample("/a", minute * 60, value)])
            .unwrap();
        // Sealed every few minutes, so the window really spans several segments — which
        // is the whole thing under test.
        if minute % 5 == 0 {
            store.metrics().seal_now().unwrap();
        }
    }
    store.metrics().seal_now().ok();

    let folded = store
        .metrics()
        .fold_window(0, 31 * 60 * 1_000_000_000, &[])
        .unwrap()
        .expect("six segments is within the read-whole limit, so summaries answer this");
    assert_eq!(folded.len(), 1, "one stream: {folded:?}");
    let (_, fold) = &folded[0];

    assert_eq!(fold.seen, 30, "every sample counted");
    assert_eq!(
        fold.first_nanos,
        60 * 1_000_000_000,
        "span starts at the first"
    );
    assert_eq!(
        fold.last_nanos,
        30 * 60 * 1_000_000_000,
        "and ends at the last"
    );
    assert!(
        (fold.increase - expected_increase).abs() < 1e-9,
        "increase {} vs expected {expected_increase}",
        fold.increase
    );
}
