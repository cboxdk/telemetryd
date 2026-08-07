//! Integration tests for the record store: durability, recovery, pruning, and the
//! crash windows that produce wrong *answers* rather than obvious failures.

#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use telemetryd_core::config::{Compression, WalSync};
use telemetryd_core::{LabelMatcher, Labels, LogRecord, MatchOp, Severity};
use telemetryd_store::logs::LogSchema;
use telemetryd_store::records::{RecordStore, StoreSettings};

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

    fn wal_dir(&self) -> PathBuf {
        self.root.join("wal")
    }

    fn open(&self) -> RecordStore<LogSchema> {
        self.open_with(settings())
    }

    fn open_with(&self, settings: StoreSettings) -> RecordStore<LogSchema> {
        RecordStore::<LogSchema>::open(
            &self.wal_dir(),
            self.root.join("segments"),
            self.root.join("tmp"),
            settings,
        )
        .unwrap()
    }
}

fn settings() -> StoreSettings {
    StoreSettings {
        segment_duration: Duration::from_secs(3600),
        max_segment_bytes: 1 << 30,
        wal_sync: WalSync::Always,
        wal_sync_interval: Duration::ZERO,
        compression: Compression::Zstd,
        query_parallelism: 1,
    }
}

fn record(i: u64, app: &str, level: Severity, body: &str) -> LogRecord {
    let mut stream = Labels::new();
    stream.insert("app", app);
    stream.insert("level", level.as_str());

    let mut attributes = Labels::new();
    attributes.insert("seq", i.to_string());

    LogRecord {
        timestamp_nanos: BASE + i * 1_000_000,
        stream,
        severity: level,
        severity_text: level.as_str().to_uppercase(),
        body: body.to_owned(),
        attributes,
        trace_id: None,
        span_id: None,
    }
}

fn all(store: &RecordStore<LogSchema>) -> Vec<LogRecord> {
    let mut out = store.query(0, u64::MAX, &[], &|_| true).unwrap();
    out.sort_by_key(|r| r.timestamp_nanos);
    out
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

// ---------------------------------------------------------------------------
// basic round trip
// ---------------------------------------------------------------------------

#[test]
fn buffered_records_are_queryable_immediately() {
    let harness = Harness::new();
    let store = harness.open();

    let records: Vec<LogRecord> = (0..50)
        .map(|i| record(i, "checkout", Severity::Info, &format!("line {i}")))
        .collect();
    store.append(&records).unwrap();

    // Not sealed yet: the point is that data is visible before it hits a segment.
    assert_eq!(store.status().segments, 0);
    assert_eq!(all(&store), records);
}

#[test]
fn sealed_and_buffered_records_are_both_returned_exactly_once() {
    let harness = Harness::new();
    let store = harness.open();

    let first: Vec<LogRecord> = (0..30)
        .map(|i| record(i, "a", Severity::Info, "old"))
        .collect();
    store.append(&first).unwrap();
    store.seal_now().unwrap();

    let second: Vec<LogRecord> = (30..60)
        .map(|i| record(i, "a", Severity::Info, "new"))
        .collect();
    store.append(&second).unwrap();

    let got = all(&store);
    assert_eq!(
        got.len(),
        60,
        "every record exactly once across the seal boundary"
    );
    assert_eq!(got, [first, second].concat());
    assert_eq!(store.status().segments, 1);
}

#[test]
fn sealing_an_empty_buffer_is_a_no_op() {
    let harness = Harness::new();
    let store = harness.open();
    assert!(store.seal_now().unwrap().is_none());
    assert_eq!(store.status().segments, 0);
}

// ---------------------------------------------------------------------------
// durability and recovery
// ---------------------------------------------------------------------------

#[test]
fn unsealed_records_survive_a_restart() {
    let harness = Harness::new();
    let records: Vec<LogRecord> = (0..25)
        .map(|i| record(i, "checkout", Severity::Error, &format!("boom {i}")))
        .collect();

    {
        let store = harness.open();
        store.append(&records).unwrap();
        store.sync().unwrap();
    }

    let reopened = harness.open();
    assert_eq!(
        all(&reopened),
        records,
        "the WAL must replay into the buffer"
    );
    assert_eq!(reopened.status().recovered_records, 25);
}

#[test]
fn sealed_records_are_not_replayed_again_after_a_restart() {
    let harness = Harness::new();
    let records: Vec<LogRecord> = (0..40)
        .map(|i| record(i, "a", Severity::Info, "x"))
        .collect();

    {
        let store = harness.open();
        store.append(&records).unwrap();
        store.seal_now().unwrap();
    }

    let reopened = harness.open();
    assert_eq!(reopened.status().recovered_records, 0);
    assert_eq!(
        all(&reopened).len(),
        40,
        "read from the segment, not the log"
    );
}

/// The window that produces wrong answers rather than obvious failures.
///
/// If the process dies after a segment is published but before the write-ahead log
/// that fed it is deleted, a naive replay puts those records back into the buffer and
/// they are stored twice. Nothing errors — the same log line simply appears twice in
/// every query, with no indication why.
#[test]
fn a_crash_between_sealing_and_truncating_the_log_does_not_duplicate_records() {
    let harness = Harness::new();
    let records: Vec<LogRecord> = (0..40)
        .map(|i| record(i, "checkout", Severity::Info, &format!("line {i}")))
        .collect();

    let wal_backup = harness.root.join("wal-backup");
    {
        let store = harness.open();
        store.append(&records).unwrap();
        store.sync().unwrap();

        // Snapshot the log as it stands *before* the seal deletes it.
        copy_dir(&harness.wal_dir(), &wal_backup);

        store.seal_now().unwrap();
        assert_eq!(store.status().segments, 1);
    }

    // Restore the log: the segment is published, but the truncation never happened.
    copy_dir(&wal_backup, &harness.wal_dir());

    let reopened = harness.open();
    let got = all(&reopened);
    assert_eq!(
        got.len(),
        40,
        "expected 40 records, got {} — duplicated",
        got.len()
    );
    assert_eq!(got, records);
    assert_eq!(
        reopened.status().recovered_records,
        0,
        "records already durable in a segment must not be replayed"
    );
}

#[test]
fn a_crash_before_sealing_recovers_everything_from_the_log() {
    let harness = Harness::new();
    let first: Vec<LogRecord> = (0..20)
        .map(|i| record(i, "a", Severity::Info, "sealed"))
        .collect();
    let second: Vec<LogRecord> = (20..35)
        .map(|i| record(i, "a", Severity::Info, "buffered"))
        .collect();

    {
        let store = harness.open();
        store.append(&first).unwrap();
        store.seal_now().unwrap();
        // Appended after the seal, so these live only in the log.
        store.append(&second).unwrap();
        store.sync().unwrap();
        // No clean shutdown: just drop.
    }

    let reopened = harness.open();
    assert_eq!(all(&reopened), [first, second.clone()].concat());
    assert_eq!(reopened.status().recovered_records, second.len() as u64);
}

#[test]
fn an_incomplete_segment_directory_is_ignored_rather_than_fatal() {
    let harness = Harness::new();
    {
        let store = harness.open();
        store
            .append(&[record(1, "a", Severity::Info, "x")])
            .unwrap();
        store.seal_now().unwrap();
    }

    // A crash mid-publish can leave a directory with no manifest.
    let orphan = harness
        .root
        .join("segments")
        .join("00000000000000000000-99999999");
    std::fs::create_dir_all(&orphan).unwrap();
    std::fs::write(orphan.join("data.parquet"), b"partial").unwrap();

    let reopened = harness.open();
    assert_eq!(
        reopened.status().segments,
        1,
        "the good segment still loads"
    );
    assert_eq!(all(&reopened).len(), 1);
}

// ---------------------------------------------------------------------------
// querying
// ---------------------------------------------------------------------------

#[test]
fn time_range_filtering_is_inclusive_on_both_ends() {
    let harness = Harness::new();
    let store = harness.open();
    let records: Vec<LogRecord> = (0..10)
        .map(|i| record(i, "a", Severity::Info, "x"))
        .collect();
    store.append(&records).unwrap();

    let start = records[2].timestamp_nanos;
    let end = records[5].timestamp_nanos;
    let got = store.query(start, end, &[], &|_| true).unwrap();
    assert_eq!(got.len(), 4, "records 2..=5 inclusive");
}

#[test]
fn label_matchers_apply_across_segments_and_buffer_identically() {
    let harness = Harness::new();
    let store = harness.open();

    store
        .append(&[
            record(0, "checkout", Severity::Error, "sealed error"),
            record(1, "cart", Severity::Info, "sealed info"),
        ])
        .unwrap();
    store.seal_now().unwrap();
    store
        .append(&[
            record(2, "checkout", Severity::Error, "live error"),
            record(3, "cart", Severity::Info, "live info"),
        ])
        .unwrap();

    let matchers = vec![
        LabelMatcher::equal("app", "checkout"),
        LabelMatcher::equal("level", "error"),
    ];
    let got = store.query(0, u64::MAX, &matchers, &|_| true).unwrap();
    assert_eq!(got.len(), 2, "one from the segment, one from the buffer");
    assert!(got.iter().all(|r| r.app() == "checkout"));
}

#[test]
fn pruning_never_hides_a_matching_segment() {
    let harness = Harness::new();
    let store = harness.open();

    // Three segments, each holding a distinct app.
    for (i, app) in ["alpha", "beta", "gamma"].iter().enumerate() {
        store
            .append(&[record(i as u64, app, Severity::Info, "x")])
            .unwrap();
        store.seal_now().unwrap();
    }
    assert_eq!(store.status().segments, 3);

    for app in ["alpha", "beta", "gamma"] {
        let got = store
            .query(0, u64::MAX, &[LabelMatcher::equal("app", app)], &|_| true)
            .unwrap();
        assert_eq!(got.len(), 1, "pruning skipped the segment holding {app}");
        assert_eq!(got[0].app(), app);
    }

    // A negative matcher is satisfied by streams lacking the label, so it must not
    // prune anything away.
    let negative = LabelMatcher::new("nonexistent", MatchOp::NotEqual, "x").unwrap();
    assert_eq!(
        store
            .query(0, u64::MAX, &[negative], &|_| true)
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn the_extra_filter_runs_after_label_matching() {
    let harness = Harness::new();
    let store = harness.open();
    store
        .append(&[
            record(0, "a", Severity::Info, "contains needle"),
            record(1, "a", Severity::Info, "does not"),
        ])
        .unwrap();

    let got = store
        .query(0, u64::MAX, &[], &|r: &LogRecord| r.body.contains("needle"))
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].body, "contains needle");
}

#[test]
fn out_of_order_timestamps_are_stored_and_found() {
    let harness = Harness::new();
    let store = harness.open();

    // Late arrivals are normal: batching clients, retries, clock skew.
    let mut records = vec![
        record(500, "a", Severity::Info, "newest"),
        record(1, "a", Severity::Info, "oldest"),
        record(250, "a", Severity::Info, "middle"),
    ];
    store.append(&records).unwrap();
    store.seal_now().unwrap();

    records.sort_by_key(|r| r.timestamp_nanos);
    assert_eq!(all(&store), records);

    // The manifest must span the true event-time range, not the arrival order.
    let segment = &store.segments()[0];
    assert_eq!(segment.manifest.min_time_nanos, records[0].timestamp_nanos);
    assert_eq!(segment.manifest.max_time_nanos, records[2].timestamp_nanos);
}

// ---------------------------------------------------------------------------
// label discovery
// ---------------------------------------------------------------------------

#[test]
fn label_names_and_values_cover_segments_and_buffer() {
    let harness = Harness::new();
    let store = harness.open();

    let mut sealed = record(0, "checkout", Severity::Error, "x");
    sealed.stream.insert("region", "eu-west");
    store.append(&[sealed]).unwrap();
    store.seal_now().unwrap();

    let mut live = record(1, "cart", Severity::Info, "y");
    live.stream.insert("tier", "free");
    store.append(&[live]).unwrap();

    let names = store.label_names(0, u64::MAX);
    for expected in ["app", "level", "region", "tier"] {
        assert!(
            names.contains(&expected.to_owned()),
            "missing {expected} in {names:?}"
        );
    }

    let apps = store.label_values("app", 0, u64::MAX).unwrap();
    assert_eq!(apps, vec!["cart".to_owned(), "checkout".to_owned()]);

    let levels = store.label_values("level", 0, u64::MAX).unwrap();
    assert_eq!(levels, vec!["error".to_owned(), "info".to_owned()]);
}

#[test]
fn label_values_are_complete_even_for_an_unbounded_label() {
    let harness = Harness::new();
    let store = harness.open();

    // Past the manifest's tracking cap the values are not recorded, so the store has
    // to read the segment rather than under-report and make a UI dropdown wrong.
    let records: Vec<LogRecord> = (0..400)
        .map(|i| {
            let mut r = record(i, "a", Severity::Info, "x");
            r.stream.insert("request_id", format!("req-{i}"));
            r
        })
        .collect();
    store.append(&records).unwrap();
    store.seal_now().unwrap();

    let values = store.label_values("request_id", 0, u64::MAX).unwrap();
    assert_eq!(
        values.len(),
        400,
        "unbounded labels must still enumerate fully"
    );
}

#[test]
fn streams_returns_distinct_label_sets() {
    let harness = Harness::new();
    let store = harness.open();
    store
        .append(&[
            record(0, "a", Severity::Info, "1"),
            record(1, "a", Severity::Info, "2"),
            record(2, "a", Severity::Error, "3"),
            record(3, "b", Severity::Info, "4"),
        ])
        .unwrap();

    let streams = store.streams(0, u64::MAX, &[]).unwrap();
    assert_eq!(streams.len(), 3, "a/info, a/error, b/info");
}

// ---------------------------------------------------------------------------
// sealing behaviour
// ---------------------------------------------------------------------------

#[test]
fn a_full_buffer_seals_itself() {
    let harness = Harness::new();
    let store = harness.open_with(StoreSettings {
        max_segment_bytes: 4096,
        ..settings()
    });

    for i in 0..200 {
        store
            .append(&[record(i, "a", Severity::Info, &"x".repeat(200))])
            .unwrap();
    }

    assert!(
        store.status().segments > 0,
        "size-based sealing never fired"
    );
    assert_eq!(
        all(&store).len(),
        200,
        "no record lost while sealing under load"
    );
}

#[test]
fn a_window_that_has_not_elapsed_does_not_seal() {
    let harness = Harness::new();
    // Deliberately long: this asserts a *negative*, so the window must not be able to
    // elapse accidentally on a loaded test runner.
    let store = harness.open_with(StoreSettings {
        segment_duration: Duration::from_secs(3600),
        ..settings()
    });

    store
        .append(&[record(0, "a", Severity::Info, "x")])
        .unwrap();
    assert!(store.maybe_seal().unwrap().is_none());
    assert_eq!(store.status().segments, 0);
}

#[test]
fn time_based_sealing_fires_once_the_window_elapses() {
    let harness = Harness::new();
    let store = harness.open_with(StoreSettings {
        segment_duration: Duration::from_millis(20),
        ..settings()
    });

    store
        .append(&[record(0, "a", Severity::Info, "x")])
        .unwrap();

    // Poll rather than sleeping exactly once. The claim is "the window causes a seal",
    // not "a seal happens within one scheduler quantum"; asserting the latter is how a
    // test becomes flaky under parallel load, and a flaky test trains people to
    // re-run rather than to read.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if store.maybe_seal().unwrap().is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        store.status().segments,
        1,
        "the window never triggered a seal"
    );

    // An empty buffer must not produce an empty segment on every subsequent tick.
    std::thread::sleep(Duration::from_millis(40));
    assert!(store.maybe_seal().unwrap().is_none());
    assert_eq!(store.status().segments, 1);
}

#[test]
fn removing_a_segment_takes_it_out_of_queries_and_off_disk() {
    let harness = Harness::new();
    let store = harness.open();
    store
        .append(&[record(0, "a", Severity::Info, "x")])
        .unwrap();
    let segment = store.seal_now().unwrap().unwrap();
    let dir = segment.dir.clone();

    assert!(store.remove_segment(&segment.manifest.id).unwrap());
    assert!(!dir.exists(), "retention must actually free the disk");
    assert!(all(&store).is_empty());
    assert!(
        !store.remove_segment(&segment.manifest.id).unwrap(),
        "idempotent"
    );
}

// ---------------------------------------------------------------------------
// concurrency
// ---------------------------------------------------------------------------

#[test]
fn concurrent_appends_and_seals_lose_nothing() {
    const WRITERS: u64 = 8;
    const PER_WRITER: u64 = 250;

    let harness = Harness::new();
    let store = Arc::new(harness.open_with(StoreSettings {
        max_segment_bytes: 8192,
        ..settings()
    }));

    std::thread::scope(|scope| {
        for writer in 0..WRITERS {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                for i in 0..PER_WRITER {
                    let index = writer * PER_WRITER + i;
                    store
                        .append(&[record(
                            index,
                            "concurrent",
                            Severity::Info,
                            &format!("w{writer}"),
                        )])
                        .unwrap();
                }
            });
        }
        // A sealer racing the writers is the interesting part: seal takes the buffer
        // and rotates the log while appends are still arriving.
        let sealer = Arc::clone(&store);
        scope.spawn(move || {
            for _ in 0..20 {
                sealer.seal_now().unwrap();
                std::thread::sleep(Duration::from_millis(2));
            }
        });
    });

    let got = all(&store);
    assert_eq!(
        got.len() as u64,
        WRITERS * PER_WRITER,
        "records were lost or duplicated under concurrent append and seal"
    );

    let mut seqs: Vec<&str> = got.iter().filter_map(|r| r.attributes.get("seq")).collect();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs.len() as u64, WRITERS * PER_WRITER, "duplicate records");
}

#[test]
fn everything_survives_a_restart_after_concurrent_load() {
    let harness = Harness::new();
    {
        let store = Arc::new(harness.open_with(StoreSettings {
            max_segment_bytes: 8192,
            ..settings()
        }));
        std::thread::scope(|scope| {
            for writer in 0..4u64 {
                let store = Arc::clone(&store);
                scope.spawn(move || {
                    for i in 0..200u64 {
                        store
                            .append(&[record(writer * 200 + i, "a", Severity::Info, "x")])
                            .unwrap();
                    }
                });
            }
        });
        store.sync().unwrap();
    }

    let reopened = harness.open();
    assert_eq!(all(&reopened).len(), 800);
}
