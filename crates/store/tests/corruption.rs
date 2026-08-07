//! A damaged segment must cost you that segment, not the store.
//!
//! telemetryd is the thing you look at when everything else is broken, so the failure
//! mode that matters is a query returning nothing at all. Before this was handled, a
//! single corrupt Parquet file made every query over its time range fail outright —
//! sixty healthy segments denied because of one bad sector.
//!
//! Damaged manifests and missing files were already skipped at load. These cover the
//! case that is only discovered at *read* time, which is the one that reaches a user.

#![allow(clippy::unwrap_used)]

use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::Duration;

use telemetryd_core::config::{Compression, WalSync};
use telemetryd_core::{LabelMatcher, Labels, LogRecord, Severity};
use telemetryd_store::logs::LogSchema;
use telemetryd_store::records::{RecordStore, Scan, StoreSettings};
use telemetryd_store::topk::Order;

const BASE: u64 = 1_750_000_000_000_000_000;

struct Harness {
    root: PathBuf,
    _tmp: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        for sub in ["wal", "segments", "tmp"] {
            std::fs::create_dir_all(root.join(sub)).unwrap();
        }
        Self { root, _tmp: tmp }
    }

    fn open(&self) -> RecordStore<LogSchema> {
        RecordStore::<LogSchema>::open(
            &self.root.join("wal"),
            self.root.join("segments"),
            self.root.join("tmp"),
            StoreSettings {
                segment_duration: Duration::from_secs(3600),
                max_segment_bytes: 1 << 30,
                wal_sync: WalSync::Never,
                wal_sync_interval: Duration::ZERO,
                compression: Compression::Zstd,
                query_parallelism: 1,
            },
        )
        .unwrap()
    }
}

fn record(i: u64) -> LogRecord {
    let mut stream = Labels::new();
    stream.insert("app", "checkout");
    LogRecord {
        timestamp_nanos: BASE + i * 1_000_000,
        stream,
        severity: Severity::Info,
        severity_text: "INFO".to_owned(),
        body: format!("line {i}"),
        attributes: Labels::new(),
        trace_id: None,
        span_id: None,
    }
}

fn everything() -> Scan<'static> {
    Scan {
        start_nanos: 0,
        end_nanos: u64::MAX,
        limit: 0,
        order: Order::Ascending,
        exact_key: None,
        columns: None,
        required_text: None,
    }
}

/// Build three sealed segments and return the middle one's Parquet file.
fn store_with_three_segments(harness: &Harness) -> (RecordStore<LogSchema>, PathBuf) {
    let store = harness.open();
    for segment in 0..3u64 {
        let records: Vec<LogRecord> = (0..500).map(|i| record(segment * 1000 + i)).collect();
        store.append(&records).unwrap();
        store.seal_now().unwrap();
    }

    let mut segments = store.segments();
    segments.sort_by_key(|s| s.manifest.min_time_nanos);
    let victim = segments[1].data_path();
    assert!(
        victim.exists(),
        "expected a Parquet file at {}",
        victim.display()
    );
    (store, victim)
}

fn count(store: &RecordStore<LogSchema>) -> usize {
    let matchers = [LabelMatcher::equal("app", "checkout")];
    store
        .scan(everything(), &matchers, &|_| true)
        .unwrap()
        .len()
}

#[test]
fn a_truncated_segment_costs_only_its_own_rows() {
    let harness = Harness::new();
    let (store, victim) = store_with_three_segments(&harness);
    assert_eq!(count(&store), 1500);

    // A truncated file is what a full disk or a torn write leaves behind.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&victim)
        .unwrap();
    file.set_len(128).unwrap();
    drop(file);

    let store = harness.open();
    let survivors = count(&store);
    assert_eq!(
        survivors, 1000,
        "expected the two healthy segments to answer, got {survivors} rows"
    );
    assert!(
        store.status().segments_unreadable > 0,
        "a skipped segment has to be reported, not silently dropped"
    );
}

#[test]
fn garbage_in_the_middle_of_a_segment_does_not_fail_the_query() {
    let harness = Harness::new();
    let (store, victim) = store_with_three_segments(&harness);
    let before = count(&store);

    let length = std::fs::metadata(&victim).unwrap().len();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&victim)
        .unwrap();
    file.seek(SeekFrom::Start(length / 2)).unwrap();
    file.write_all(&[0xA5; 2048]).unwrap();
    drop(file);

    let store = harness.open();
    // The exact survivor count depends on where the damage lands relative to the row
    // groups, so this asserts the property that matters: the query answers, and it
    // answers with less than it did.
    let survivors = count(&store);
    assert!(
        survivors < before,
        "the damaged segment should have cost something, got {survivors} of {before}"
    );
    assert!(
        survivors >= 1000,
        "the two healthy segments must still answer, got {survivors}"
    );
}

#[test]
fn a_zero_length_segment_is_skipped() {
    let harness = Harness::new();
    let (_store, victim) = store_with_three_segments(&harness);
    std::fs::write(&victim, b"").unwrap();

    let store = harness.open();
    assert_eq!(count(&store), 1000);
}

/// The flag is per segment, not per read: a store with one bad file should not do the
/// failing work again on every query for the rest of the process's life.
#[test]
fn a_damaged_segment_is_only_diagnosed_once() {
    let harness = Harness::new();
    let (_store, victim) = store_with_three_segments(&harness);
    std::fs::write(&victim, b"not parquet").unwrap();

    let store = harness.open();
    for _ in 0..5 {
        assert_eq!(count(&store), 1000);
    }

    let mut segments = store.segments();
    segments.sort_by_key(|s| s.manifest.min_time_nanos);
    assert!(
        segments[1].is_unreadable(),
        "the segment should stay marked so later queries skip it cheaply"
    );
}
