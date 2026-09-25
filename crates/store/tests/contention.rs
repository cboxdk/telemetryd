//! Queries must not stop writes.
//!
//! Two production defects lived behind a fully green suite because every test either
//! wrote or read, never both at once:
//!
//! - `scan` walked the whole live buffer while holding the lock that `append` needs, so
//!   a single reader cost 45% of ingest throughput and query latency was 777 ms against
//!   a benchmark of 1.4 ms.
//! - a limited query examined every buffered record, because nothing recorded which
//!   ones could not possibly be in the newest hundred.
//!
//! These assert the properties rather than the timings. Wall-clock thresholds on a
//! shared CI runner are how a suite earns a reputation for flaking — one here did — so
//! what is checked is structural: an append completing while a query is held inside
//! its scan, and how many records a limited query examines.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use telemetryd_core::config::{Compression, WalSync};
use telemetryd_core::{LabelMatcher, Labels, LogRecord, Severity};
use telemetryd_store::logs::LogSchema;
use telemetryd_store::records::{RecordStore, Scan, StoreSettings};
use telemetryd_store::topk::Order;

const BASE: u64 = 1_750_000_000_000_000_000;

fn settings() -> StoreSettings {
    StoreSettings {
        segment_duration: Duration::from_secs(3600),
        // Large enough that nothing seals mid-test: this is about the live buffer, and
        // a seal in the middle would quietly change what is being measured.
        max_segment_bytes: 1 << 30,
        wal_sync: WalSync::Never,
        wal_sync_interval: Duration::ZERO,
        compression: Compression::Zstd,
        query_parallelism: 1,
    }
}

fn store(dir: &std::path::Path) -> RecordStore<LogSchema> {
    for sub in ["wal", "segments", "tmp"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    RecordStore::<LogSchema>::open(
        &dir.join("wal"),
        dir.join("segments"),
        dir.join("tmp"),
        settings(),
    )
    .unwrap()
}

fn record(i: u64) -> LogRecord {
    let mut stream = Labels::new();
    stream.insert("app", "checkout");
    stream.insert("level", "info");
    let mut attributes = Labels::new();
    attributes.insert("exception.type", "TimeoutError");
    LogRecord {
        timestamp_nanos: BASE + i * 1_000,
        stream,
        severity: Severity::Info,
        severity_text: "INFO".to_owned(),
        body: format!("payment attempt {i} for order {}", 1000 + i),
        attributes,
        trace_id: None,
        span_id: None,
    }
}

fn newest_hundred() -> Scan<'static> {
    Scan {
        abort_over: 0,
        start_nanos: 0,
        end_nanos: u64::MAX,
        limit: 100,
        order: Order::Descending,
        exact_key: None,
        columns: None,
        required_text: None,
    }
}

/// An append must not wait for a query to finish scanning.
///
/// Checked by holding a query *inside* its scan — its record predicate parks until told
/// to go on — and appending meanwhile. If the scan held the lock appends need, the
/// append cannot finish until the query does, and the query does not go on until the
/// append has finished or the wait gives up.
///
/// It used to be measured: the worst time one append blocked while a reader scanned,
/// against 20 ms. That was right about the defect and wrong about CI, where a busy
/// runner can stall any thread for 20 ms, so the gate failed now and then for nothing.
/// Holding the scan open answers the same question with no clock in it.
#[test]
fn an_append_does_not_wait_for_a_query() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(store(tmp.path()));
    store
        .append(&(0..10_000).map(record).collect::<Vec<_>>())
        .unwrap();

    let (inside, in_scan) = std::sync::mpsc::channel::<()>();
    let (appended, append_done) = std::sync::mpsc::channel::<()>();
    let append_done = std::sync::Mutex::new(append_done);
    let finished_while_scanning = Arc::new(AtomicBool::new(false));
    let reader = {
        let (store, finished) = (Arc::clone(&store), Arc::clone(&finished_while_scanning));
        std::thread::spawn(move || {
            let parked = AtomicBool::new(false);
            let predicate = |_: &LogRecord| {
                if !parked.swap(true, Ordering::Relaxed) {
                    inside.send(()).unwrap();
                    let waited = append_done
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(10));
                    finished.store(waited.is_ok(), Ordering::Relaxed);
                }
                true
            };
            store
                .scan(Scan::range(0, u64::MAX), &[], &predicate)
                .unwrap();
        })
    };

    in_scan.recv().unwrap();
    store.append(&[record(10_000)]).unwrap();
    // Only sent once the append is back. If it could not get the lock, the reader gave
    // up waiting first, and the flag says so.
    let _ = appended.send(());
    reader.join().unwrap();
    assert!(
        finished_while_scanning.load(Ordering::Relaxed),
        "an append waited for a query to finish scanning — queries are holding the append \
         lock again"
    );
}

/// A limited query must not examine more of the buffer as the buffer fills.
///
/// Without time bounds on the buffer chunks its cost was linear in everything buffered.
/// Counted as the records it looks at, rather than timed: a count cannot flake, and it
/// is the thing the chunk bounds exist to keep small.
#[test]
fn a_limited_query_does_not_scale_with_the_buffer() {
    let tmp = tempfile::tempdir().unwrap();
    let store = store(tmp.path());
    let matchers = [LabelMatcher::equal("app", "checkout")];

    let batch: Vec<LogRecord> = (0..20_000).map(record).collect();
    store.append(&batch).unwrap();
    let small = examined(&store, &matchers);

    let batch: Vec<LogRecord> = (20_000..400_000).map(record).collect();
    store.append(&batch).unwrap();
    let large = examined(&store, &matchers);

    // Twenty times the data. A walk of everything examines twenty times as many; the
    // newest hundred sit in the newest chunk or two whatever the size.
    assert!(
        large <= small * 2,
        "a limited query looked at {small} records of 20k and {large} of 400k"
    );
    assert!(
        large < 20_000,
        "{large} records examined for the newest hundred"
    );
}

fn examined(store: &RecordStore<LogSchema>, matchers: &[LabelMatcher]) -> u64 {
    let seen = AtomicU64::new(0);
    let found = store
        .scan(newest_hundred(), matchers, &|_| {
            seen.fetch_add(1, Ordering::Relaxed);
            true
        })
        .unwrap();
    assert_eq!(found.len(), 100);
    seen.load(Ordering::Relaxed)
}

/// Whatever the buffer reports having is what a query can actually see.
///
/// Chunking split the buffer across an immutable list and a mutable tail, and a reader
/// that forgot to freeze the tail would silently miss the newest records — the failure
/// mode most likely to look fine in a functional test and lose data in production.
#[test]
fn a_query_sees_records_appended_a_moment_ago() {
    let tmp = tempfile::tempdir().unwrap();
    let store = store(tmp.path());
    let matchers = [LabelMatcher::equal("app", "checkout")];

    // Deliberately not a multiple of the chunk size, so the last chunk is partial.
    for i in 0..37 {
        store.append(&[record(i)]).unwrap();

        let found = store
            .scan(
                Scan {
                    abort_over: 0,
                    start_nanos: 0,
                    end_nanos: u64::MAX,
                    limit: 0,
                    order: Order::Ascending,
                    exact_key: None,
                    columns: None,
                    required_text: None,
                },
                &matchers,
                &|_| true,
            )
            .unwrap();
        assert_eq!(
            found.len() as u64,
            i + 1,
            "after appending {} records the query saw {}",
            i + 1,
            found.len()
        );
    }

    assert_eq!(store.status().buffered_records, 37);
}
