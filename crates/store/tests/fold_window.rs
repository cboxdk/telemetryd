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
        .fold_window(0, 31 * 60 * 1_000_000_000, &[], 4)
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

/// Three rules decide whether the shortcut is worth taking, and all three matter.
///
/// A window narrower than a segment contains none, so summaries have nothing to add and
/// taking the shortcut would mean reading whole segments to answer a quarter of an hour.
/// A window that does contain segments still has two edges that must be read, and a chart
/// pays for those at every one of its points. Getting either wrong turned a dashboard
/// panel from a refusal into a thirty-second timeout.
#[test]
fn the_shortcut_is_taken_only_when_it_saves_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();

    for minute in 1..=30u64 {
        #[allow(clippy::cast_precision_loss)]
        let value = minute as f64 * 5.0;
        store
            .metrics()
            .append(&[sample("/a", minute * 60, value)])
            .unwrap();
        if minute % 5 == 0 {
            store.metrics().seal_now().unwrap();
        }
    }
    store.metrics().seal_now().ok();
    let folds = &store.metrics();

    // Three minutes inside one five-minute segment: nothing is contained whole, so the
    // shortcut is declined however much budget it is given.
    let (narrow_from, narrow_to) = (11 * 60 * 1_000_000_000, 14 * 60 * 1_000_000_000);
    assert!(
        folds
            .fold_window(narrow_from, narrow_to, &[], 4)
            .unwrap()
            .is_none(),
        "a window narrower than a segment has nothing to take from summaries"
    );

    // Most of the span, with both ends landing mid-segment: those two must be read.
    let (wide_from, wide_to) = (150 * 1_000_000_000, 28 * 60 * 1_000_000_000);
    assert!(
        folds
            .fold_window(wide_from, wide_to, &[], 4)
            .unwrap()
            .is_some(),
        "one evaluation point can afford to read the two edge segments"
    );
    assert!(
        folds
            .fold_window(wide_from, wide_to, &[], 0)
            .unwrap()
            .is_none(),
        "a chart pays for each edge segment at every point, so it can afford none"
    );
}

/// A staleness marker is a NaN with one particular bit pattern, and that pattern is the
/// whole of its meaning. It has to survive sealing into Parquet as itself — an ordinary
/// NaN would read back as a value — and a segment's summary must not fold it in, or its
/// NaN would make every increase across that segment NaN.
#[test]
fn a_staleness_marker_survives_sealing_and_stays_out_of_the_summary() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&config(dir.path())).unwrap();
    let stale = f64::from_bits(telemetryd_core::metric::STALE_MARKER_BITS);

    for minute in 1..=4u64 {
        #[allow(clippy::cast_precision_loss)]
        let value = minute as f64 * 10.0;
        store
            .metrics()
            .append(&[sample("/a", minute * 60, value)])
            .unwrap();
    }
    store
        .metrics()
        .append(&[sample("/a", 5 * 60, stale)])
        .unwrap();
    let segment = store.metrics().seal_now().unwrap().expect("a segment");

    let rows = segment
        .read::<telemetryd_store::metrics::MetricSchema>()
        .unwrap();
    let marker = rows
        .iter()
        .find(|row| row.timestamp_nanos == 300 * 1_000_000_000);
    assert_eq!(
        marker.map(|row| row.value.to_bits()),
        Some(telemetryd_core::metric::STALE_MARKER_BITS),
        "the marker came back as itself"
    );

    let folded = store
        .metrics()
        .fold_window(0, 10 * 60 * 1_000_000_000, &[], 4)
        .unwrap()
        .expect("one summarised segment");
    let (_, fold) = &folded[0];
    assert_eq!(fold.seen, 4, "the marker is not a sample");
    assert!((fold.increase - 30.0).abs() < 1e-9, "got {}", fold.increase);
}
