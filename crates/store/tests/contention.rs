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
//! shared CI runner are how a suite earns a reputation for flaking, so what is checked
//! is the *ratio* between doing the work alone and doing it under concurrent queries,
//! with wide margins — a regression to the old behaviour is a factor of two, not a few
//! percent.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
/// Measured as the *worst* time a single append blocks, not as average throughput.
/// Throughput conflates two different things — the reader competing for CPU, which is
/// unavoidable and small, and the reader holding the append lock, which is the defect.
/// A ratio test cannot tell them apart, and would have passed on the 45% regression
/// this exists to catch.
///
/// So the reader sleeps between queries. It uses almost no CPU, and any append delay
/// that remains is the lock.
#[test]
fn an_append_does_not_wait_for_a_query() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(store(tmp.path()));

    // A buffer big enough that scanning all of it is clearly measurable — which is
    // exactly what the old implementation did on every query, while holding the lock.
    let batch: Vec<LogRecord> = (0..300_000).map(record).collect();
    store.append(&batch).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let queries = Arc::new(AtomicU64::new(0));
    let reader = {
        let (store, stop, queries) = (store.clone(), stop.clone(), queries.clone());
        std::thread::spawn(move || {
            let matchers = [LabelMatcher::equal("app", "checkout")];
            while !stop.load(Ordering::Relaxed) {
                store.scan(newest_hundred(), &matchers, &|_| true).unwrap();
                queries.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    };

    let mut worst = Duration::ZERO;
    for i in 0..400u64 {
        let started = Instant::now();
        store.append(&[record(300_000 + i)]).unwrap();
        worst = worst.max(started.elapsed());
        std::thread::sleep(Duration::from_millis(1));
    }

    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    assert!(
        queries.load(Ordering::Relaxed) > 10,
        "the reader barely ran ({} queries), so this measured nothing",
        queries.load(Ordering::Relaxed)
    );

    // Appending one record is microseconds of work. Scanning 300k buffered records is
    // tens of milliseconds, and under the old buffer an append that landed during a
    // scan waited for all of it. 20 ms is far above the real cost and far below that.
    assert!(
        worst < Duration::from_millis(20),
        "an append blocked for {worst:?} while a query was running —          queries are holding the append lock again"
    );
}

/// A limited query must not get slower as the buffer fills.
///
/// Without time bounds on the buffer chunks its cost was linear in everything buffered,
/// so this compares a small buffer against one twenty times the size.
#[test]
fn a_limited_query_does_not_scale_with_the_buffer() {
    let tmp = tempfile::tempdir().unwrap();
    let store = store(tmp.path());
    let matchers = [LabelMatcher::equal("app", "checkout")];

    let batch: Vec<LogRecord> = (0..20_000).map(record).collect();
    store.append(&batch).unwrap();
    let small = time_queries(&store, &matchers);

    let batch: Vec<LogRecord> = (20_000..400_000).map(record).collect();
    store.append(&batch).unwrap();
    let large = time_queries(&store, &matchers);

    // Twenty times the data. A linear scan would be ~20x slower; pruning should keep
    // this near flat, so 5x is a generous ceiling that still catches the regression.
    assert!(
        large < small * 5 + Duration::from_millis(5),
        "query cost scales with buffer size: {small:?} at 20k records, {large:?} at 400k"
    );
}

fn time_queries(store: &RecordStore<LogSchema>, matchers: &[LabelMatcher]) -> Duration {
    // Warm once so the first call's allocations are not attributed to the measurement.
    store.scan(newest_hundred(), matchers, &|_| true).unwrap();

    let started = Instant::now();
    for _ in 0..20 {
        let found = store.scan(newest_hundred(), matchers, &|_| true).unwrap();
        assert_eq!(found.len(), 100);
    }
    started.elapsed() / 20
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
