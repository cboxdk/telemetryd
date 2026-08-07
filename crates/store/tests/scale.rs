//! Scale properties of the query path.
//!
//! These assert *asymptotics*, not wall-clock times — a timing assertion on a shared
//! CI runner is a coin flip, but "how many segments did this query have to open?" is
//! deterministic and is the thing that actually decides whether a query is fast.
//!
//! Each test here corresponds to an optimisation that is easy to regress silently: the
//! query would still return the right answer, just by reading the whole store.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::time::Duration;

use telemetryd_core::config::{Compression, WalSync};
use telemetryd_core::span::{SpanKind, SpanRecord, SpanStatus};
use telemetryd_core::{LabelMatcher, Labels, LogRecord, Severity};
use telemetryd_store::logs::LogSchema;
use telemetryd_store::records::{RecordStore, Scan, StoreSettings};
use telemetryd_store::spans::SpanSchema;
use telemetryd_store::topk::Order;

const BASE: u64 = 1_750_000_000_000_000_000;
const SECOND: u64 = 1_000_000_000;

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

    fn logs(&self) -> RecordStore<LogSchema> {
        self.logs_with_parallelism(1)
    }

    fn logs_with_parallelism(&self, workers: usize) -> RecordStore<LogSchema> {
        RecordStore::<LogSchema>::open(
            &self.root.join("wal"),
            self.root.join("segments"),
            self.root.join("tmp"),
            settings_with_parallelism(workers),
        )
        .unwrap()
    }

    fn spans(&self) -> RecordStore<SpanSchema> {
        RecordStore::<SpanSchema>::open(
            &self.root.join("wal"),
            self.root.join("segments"),
            self.root.join("tmp"),
            settings(),
        )
        .unwrap()
    }
}

fn settings() -> StoreSettings {
    settings_with_parallelism(1)
}

fn settings_with_parallelism(query_parallelism: usize) -> StoreSettings {
    StoreSettings {
        segment_duration: Duration::from_secs(3600),
        max_segment_bytes: 1 << 30,
        wal_sync: WalSync::Never,
        wal_sync_interval: Duration::ZERO,
        compression: Compression::Zstd,
        query_parallelism,
    }
}

fn log(ts: u64, app: &str, body: &str) -> LogRecord {
    let mut stream = Labels::new();
    stream.insert("app", app);
    stream.insert("level", "info");
    LogRecord {
        timestamp_nanos: ts,
        stream,
        severity: Severity::Info,
        severity_text: "INFO".to_owned(),
        body: body.to_owned(),
        attributes: Labels::new(),
        trace_id: None,
        span_id: None,
    }
}

fn span(ts: u64, trace_id: &str) -> SpanRecord {
    let mut stream = Labels::new();
    stream.insert("app", "checkout");
    SpanRecord {
        trace_id: trace_id.to_owned(),
        span_id: format!("{ts:016x}"),
        parent_span_id: None,
        name: "op".to_owned(),
        kind: SpanKind::Server,
        start_nanos: ts,
        end_nanos: ts + 1_000_000,
        status: SpanStatus::Ok,
        status_message: String::new(),
        stream,
        attributes: Labels::new(),
        events: Vec::new(),
    }
}

/// Build `count` sealed segments, each holding `per_segment` records one second apart.
fn build_log_segments(store: &RecordStore<LogSchema>, count: u64, per_segment: u64) {
    for segment in 0..count {
        let records: Vec<LogRecord> = (0..per_segment)
            .map(|i| {
                let ts = BASE + (segment * per_segment + i) * SECOND;
                log(ts, "checkout", &format!("line {segment}-{i}"))
            })
            .collect();
        store.append(&records).unwrap();
        store.seal_now().unwrap();
    }
}

// ---------------------------------------------------------------------------
// limit push-down
// ---------------------------------------------------------------------------

#[test]
fn a_limited_query_does_not_open_every_segment() {
    let harness = Harness::new();
    let store = harness.logs();
    build_log_segments(&store, 30, 100);
    assert_eq!(store.status().segments, 30);

    let before = store.status();
    let results = store
        .scan(
            Scan {
                start_nanos: 0,
                end_nanos: u64::MAX,
                limit: 10,
                order: Order::Descending,
                exact_key: None,
                columns: None,
            },
            &[],
            &|_| true,
        )
        .unwrap();

    assert_eq!(results.len(), 10);
    let scanned = store.status().segments_scanned - before.segments_scanned;
    assert!(
        scanned <= 3,
        "a limit=10 query opened {scanned} of 30 segments; the limit cutoff is not pruning"
    );
}

#[test]
fn a_limited_query_still_returns_the_correct_records() {
    // The optimisation must not change the answer, only the work.
    let harness = Harness::new();
    let store = harness.logs();
    build_log_segments(&store, 10, 50);

    let newest = store
        .scan(
            Scan {
                start_nanos: 0,
                end_nanos: u64::MAX,
                limit: 5,
                order: Order::Descending,
                exact_key: None,
                columns: None,
            },
            &[],
            &|_| true,
        )
        .unwrap();

    let everything = store.query(0, u64::MAX, &[], &|_| true).unwrap();
    let mut expected = everything;
    expected.sort_by_key(|r| std::cmp::Reverse(r.timestamp_nanos));
    expected.truncate(5);

    assert_eq!(
        newest, expected,
        "the bounded scan disagreed with a full scan"
    );
}

#[test]
fn an_ascending_limited_query_takes_the_oldest_and_prunes_from_the_other_end() {
    let harness = Harness::new();
    let store = harness.logs();
    build_log_segments(&store, 30, 100);

    let before = store.status();
    let results = store
        .scan(
            Scan {
                start_nanos: 0,
                end_nanos: u64::MAX,
                limit: 10,
                order: Order::Ascending,
                exact_key: None,
                columns: None,
            },
            &[],
            &|_| true,
        )
        .unwrap();

    assert_eq!(results.len(), 10);
    assert_eq!(
        results[0].timestamp_nanos, BASE,
        "should start at the oldest record"
    );
    let scanned = store.status().segments_scanned - before.segments_scanned;
    assert!(
        scanned <= 3,
        "ascending scan opened {scanned} of 30 segments"
    );
}

#[test]
fn an_unbounded_query_still_reads_what_it_must() {
    // The counterpart: no limit means no cutoff, so every matching segment is read.
    // Asserted so a future "optimisation" cannot silently truncate an unbounded query.
    let harness = Harness::new();
    let store = harness.logs();
    build_log_segments(&store, 10, 20);

    let results = store.query(0, u64::MAX, &[], &|_| true).unwrap();
    assert_eq!(results.len(), 200);
}

// ---------------------------------------------------------------------------
// time-range and label pruning
// ---------------------------------------------------------------------------

#[test]
fn a_narrow_time_range_opens_only_the_segments_it_overlaps() {
    let harness = Harness::new();
    let store = harness.logs();
    build_log_segments(&store, 20, 100);

    let before = store.status();
    // A window inside a single segment's span.
    let start = BASE + 250 * SECOND;
    let end = BASE + 260 * SECOND;
    let results = store.query(start, end, &[], &|_| true).unwrap();

    assert_eq!(results.len(), 11);
    let scanned = store.status().segments_scanned - before.segments_scanned;
    assert!(
        scanned <= 2,
        "a 10-second window opened {scanned} of 20 segments"
    );
}

#[test]
fn a_label_matcher_prunes_segments_that_never_saw_the_value() {
    let harness = Harness::new();
    let store = harness.logs();

    // 20 segments, only one of which contains the app we will ask for.
    for i in 0..20u64 {
        let app = if i == 7 { "needle" } else { "haystack" };
        store.append(&[log(BASE + i * SECOND, app, "x")]).unwrap();
        store.seal_now().unwrap();
    }

    let before = store.status();
    let results = store
        .query(
            0,
            u64::MAX,
            &[LabelMatcher::equal("app", "needle")],
            &|_| true,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    let scanned = store.status().segments_scanned - before.segments_scanned;
    assert_eq!(
        scanned, 1,
        "the label index should have skipped the other 19"
    );
}

// ---------------------------------------------------------------------------
// exact-key (Bloom filter) lookup
// ---------------------------------------------------------------------------

#[test]
fn a_trace_lookup_does_not_read_the_whole_retention_window() {
    let harness = Harness::new();
    let store = harness.spans();

    // 40 segments of unrelated traces, with the one we want buried in the middle.
    for i in 0..40u64 {
        let trace_id = if i == 17 {
            "4bf92f3577b34da6a3ce929d0e0e4736".to_owned()
        } else {
            format!("{i:032x}")
        };
        store.append(&[span(BASE + i * SECOND, &trace_id)]).unwrap();
        store.seal_now().unwrap();
    }

    let before = store.status();
    let results = store
        .scan(
            Scan {
                start_nanos: 0,
                end_nanos: u64::MAX,
                limit: 0,
                order: Order::Ascending,
                exact_key: Some("4bf92f3577b34da6a3ce929d0e0e4736"),
                columns: None,
            },
            &[],
            &|s: &SpanRecord| s.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736",
        )
        .unwrap();

    assert_eq!(results.len(), 1, "the trace must still be found");
    let scanned = store.status().segments_scanned - before.segments_scanned;
    // ~1% false positives at this sizing, so a couple of extra reads are expected;
    // reading all 40 would mean the filter is not consulted at all.
    assert!(
        scanned <= 4,
        "a trace lookup opened {scanned} of 40 segments; the Bloom filter is not pruning"
    );
}

#[test]
fn the_bloom_filter_never_hides_a_trace_that_exists() {
    // A false positive costs a segment read. A false negative returns an incomplete
    // trace and looks like a correct answer, so it must be impossible.
    let harness = Harness::new();
    let store = harness.spans();

    let ids: Vec<String> = (0..200).map(|i| format!("{i:032x}")).collect();
    for (i, id) in ids.iter().enumerate() {
        store.append(&[span(BASE + i as u64 * SECOND, id)]).unwrap();
        if i % 20 == 19 {
            store.seal_now().unwrap();
        }
    }
    store.seal_now().unwrap();

    for id in &ids {
        let found = store
            .scan(
                Scan {
                    start_nanos: 0,
                    end_nanos: u64::MAX,
                    limit: 0,
                    order: Order::Ascending,
                    exact_key: Some(id),
                    columns: None,
                },
                &[],
                &|s: &SpanRecord| &s.trace_id == id,
            )
            .unwrap();
        assert_eq!(found.len(), 1, "Bloom filter hid trace {id}");
    }
}

// ---------------------------------------------------------------------------
// memory
// ---------------------------------------------------------------------------

#[test]
fn a_limited_query_over_a_large_store_stays_bounded() {
    // 50k records, asking for 20. If the collector were unbounded this would hold all
    // 50k in memory before truncating; the assertion here is that the returned set is
    // exactly the bound, and the scan counters show it did not read everything.
    let harness = Harness::new();
    let store = harness.logs();
    build_log_segments(&store, 50, 1000);

    let before = store.status();
    let results = store
        .scan(
            Scan {
                start_nanos: 0,
                end_nanos: u64::MAX,
                limit: 20,
                order: Order::Descending,
                exact_key: None,
                columns: None,
            },
            &[],
            &|_| true,
        )
        .unwrap();

    assert_eq!(results.len(), 20);
    let after = store.status();
    assert!(
        after.segments_pruned > before.segments_pruned,
        "nothing was pruned over a 50-segment store"
    );
    assert!(
        after.segments_scanned - before.segments_scanned <= 3,
        "opened {} segments for a limit=20 query",
        after.segments_scanned - before.segments_scanned
    );
}

/// Parallel scanning must return *the same* answer, not merely an equally good one.
///
/// The risk is at the limit boundary. Timestamps tie constantly — plenty of producers
/// emit millisecond precision into a nanosecond field — and if ties broke on which
/// thread got there first, the same query over unchanged data would return a different
/// hundred lines each run, and a paging UI would show a line twice or skip it.
///
/// So this seeds heavy ties deliberately and compares element by element.
#[test]
fn parallel_scanning_returns_exactly_the_sequential_answer() {
    let harness = Harness::new();

    // 24 segments so the worker pool is actually used, with only 8 distinct
    // timestamps per segment: ~40 records share every timestamp.
    {
        let store = harness.logs();
        for segment in 0..24u64 {
            let records: Vec<LogRecord> = (0..320)
                .map(|i| {
                    let ts = BASE + segment * 3600 * SECOND + (i % 8) * SECOND;
                    log(
                        ts,
                        if i % 3 == 0 { "checkout" } else { "cart" },
                        &format!("line {i}"),
                    )
                })
                .collect();
            store.append(&records).unwrap();
            store.seal_now().unwrap();
        }
    }

    let sequential = harness.logs();
    let parallel = harness.logs_with_parallelism(8);

    let matchers = vec![LabelMatcher::equal("app", "checkout")];
    for (order, limit) in [
        (Order::Descending, 100),
        (Order::Ascending, 100),
        (Order::Descending, 7),
        // Unbounded: no cutoff to prune with, so every segment is read by both.
        (Order::Descending, 0),
    ] {
        let request = || Scan {
            start_nanos: 0,
            end_nanos: u64::MAX,
            limit,
            order,
            exact_key: None,
            columns: None,
        };
        let expected = sequential.scan(request(), &matchers, &|_| true).unwrap();
        let actual = parallel.scan(request(), &matchers, &|_| true).unwrap();

        assert_eq!(
            expected.len(),
            actual.len(),
            "{order:?} limit={limit}: length"
        );
        for (i, (want, got)) in expected.iter().zip(&actual).enumerate() {
            assert_eq!(
                (want.timestamp_nanos, &want.body),
                (got.timestamp_nanos, &got.body),
                "{order:?} limit={limit}: record {i} differs"
            );
        }
    }
}

/// Repeat the same parallel query and require an identical answer every time.
///
/// The equivalence test above could pass by luck if a race resolved the same way twice.
/// This one fails if the result depends on scheduling at all.
#[test]
fn a_parallel_query_is_deterministic_across_runs() {
    let harness = Harness::new();
    {
        let store = harness.logs();
        for segment in 0..16u64 {
            let records: Vec<LogRecord> = (0..200)
                .map(|i| {
                    log(
                        BASE + segment * 3600 * SECOND + (i % 4) * SECOND,
                        "checkout",
                        &format!("line {i}"),
                    )
                })
                .collect();
            store.append(&records).unwrap();
            store.seal_now().unwrap();
        }
    }

    let store = harness.logs_with_parallelism(8);
    let request = || Scan {
        start_nanos: 0,
        end_nanos: u64::MAX,
        limit: 50,
        order: Order::Descending,
        exact_key: None,
        columns: None,
    };

    let first: Vec<_> = store
        .scan(request(), &[], &|_| true)
        .unwrap()
        .into_iter()
        .map(|r| (r.timestamp_nanos, r.body))
        .collect();

    for run in 1..12 {
        let again: Vec<_> = store
            .scan(request(), &[], &|_| true)
            .unwrap()
            .into_iter()
            .map(|r| (r.timestamp_nanos, r.body))
            .collect();
        assert_eq!(first, again, "run {run} disagreed with the first");
    }
}

/// A segment's overall time range is useless for pruning once one producer's clock
/// sits away from the others'.
///
/// Concurrent producers land in the same segments, so a segment holding a backfill job
/// alongside live traffic spans both. A `limit=100` query for the older producer then
/// finds its cutoff below every segment's `max_time_nanos`, so nothing is skippable and
/// every segment is opened. Measured at 111 ms across 90 segments, and seven seconds
/// across 5,500 — degrading as the store grew, which is the shape that matters.
///
/// Per-stream bounds let the cutoff apply to the streams the query actually selected.
#[test]
fn a_stream_is_pruned_on_its_own_time_range_not_the_segments() {
    let harness = Harness::new();
    {
        let store = harness.logs();
        // Three apps, interleaved into the same segments, an hour apart in event time.
        for round in 0..12u64 {
            for app in 0..3u64 {
                let records: Vec<LogRecord> = (0..400)
                    .map(|i| {
                        log(
                            BASE + app * 3600 * SECOND + (round * 400 + i) * 1000,
                            &format!("svc-{app}"),
                            &format!("line {i}"),
                        )
                    })
                    .collect();
                store.append(&records).unwrap();
            }
            store.seal_now().unwrap();
        }
    }

    let store = harness.logs();
    let segments = store.segments().len();
    assert!(segments >= 10, "expected several segments, got {segments}");

    let newest_hundred = |app: &str| {
        let before = store.status().segments_scanned;
        let found = store
            .scan(
                Scan {
                    start_nanos: 0,
                    end_nanos: u64::MAX,
                    limit: 100,
                    order: Order::Descending,
                    exact_key: None,
                    columns: None,
                },
                &[LabelMatcher::equal("app", app)],
                &|_| true,
            )
            .unwrap();
        assert_eq!(found.len(), 100, "{app} should still return its newest 100");
        store.status().segments_scanned - before
    };

    // The newest app was always fine — its cutoff sits at the top of the range.
    let newest = newest_hundred("svc-2");
    // The oldest is the case that used to open every segment.
    let oldest = newest_hundred("svc-0");

    assert!(
        oldest <= newest * 2 + 2,
        "the oldest app opened {oldest} segments against the newest app's {newest}; \
         pruning is falling back to the whole-segment range again"
    );
    assert!(
        oldest < segments as u64,
        "opening {oldest} of {segments} segments means nothing was pruned"
    );
}
