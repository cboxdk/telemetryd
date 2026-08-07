//! Storage benchmarks.
//!
//! These exist so a change that quietly makes ingest or query an order of magnitude
//! slower shows up as a number rather than as a support ticket. The scale *tests* in
//! `tests/scale.rs` assert the asymptotics; these measure the constants.
//!
//! Run with `cargo bench -p telemetryd-store`.

#![allow(clippy::unwrap_used)]

use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use telemetryd_core::config::{Compression, WalSync};
use telemetryd_core::{LabelMatcher, Labels, LogRecord, Severity};
use telemetryd_store::logs::LogSchema;
use telemetryd_store::records::{RecordStore, Scan, StoreSettings};
use telemetryd_store::schema::RecordSchema;
use telemetryd_store::topk::Order;

const BASE: u64 = 1_750_000_000_000_000_000;

fn settings() -> StoreSettings {
    StoreSettings {
        segment_duration: Duration::from_secs(3600),
        max_segment_bytes: 1 << 30,
        // Benchmarking the device's fsync latency tells us nothing about our own code;
        // durability is covered by the tests.
        wal_sync: WalSync::Never,
        wal_sync_interval: Duration::ZERO,
        compression: Compression::Zstd,
    }
}

fn record(i: u64) -> LogRecord {
    let mut stream = Labels::new();
    stream.insert(
        "app",
        if i.is_multiple_of(3) {
            "checkout"
        } else {
            "cart"
        },
    );
    stream.insert("level", if i.is_multiple_of(7) { "error" } else { "info" });
    stream.insert("service_name", "checkout");

    let mut attributes = Labels::new();
    attributes.insert("order_id", i.to_string());
    attributes.insert("http_method", "POST");

    LogRecord {
        timestamp_nanos: BASE + i * 1_000_000,
        stream,
        severity: Severity::Info,
        severity_text: "INFO".to_owned(),
        body: format!("request {i} completed in {}ms with status 200", i % 500),
        attributes,
        trace_id: Some(format!("{i:032x}")),
        span_id: Some(format!("{i:016x}")),
    }
}

fn open(dir: &std::path::Path) -> RecordStore<LogSchema> {
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

/// Arrow encode/decode, isolated from any I/O.
fn bench_arrow(c: &mut Criterion) {
    let records: Vec<LogRecord> = (0..10_000).map(record).collect();

    let mut group = c.benchmark_group("arrow");
    group.throughput(Throughput::Elements(records.len() as u64));

    group.bench_function("to_batch", |b| {
        b.iter(|| LogSchema::to_batch(std::hint::black_box(&records)).unwrap());
    });

    let (batch, streams) = LogSchema::to_batch(&records).unwrap();
    let all_rows: telemetryd_store::schema::Rows =
        (0..u32::try_from(batch.num_rows()).unwrap()).collect();

    group.bench_function("materialize_all", |b| {
        b.iter(|| {
            LogSchema::materialize(std::hint::black_box(&batch), &all_rows, &streams).unwrap()
        });
    });

    // The shape the query path actually produces: select on columns, decode the few
    // survivors. This is the number that decides how a `limit` query feels.
    group.bench_function("select_rows_only", |b| {
        b.iter(|| LogSchema::select_rows(std::hint::black_box(&batch), 0, u64::MAX, &[]).unwrap());
    });

    let hundred: telemetryd_store::schema::Rows = (0..100).collect();
    group.bench_function("materialize_100_of_10000", |b| {
        b.iter(|| {
            LogSchema::materialize(std::hint::black_box(&batch), &hundred, &streams).unwrap()
        });
    });

    group.finish();
}

/// Append through the WAL into the buffer — the ingest hot path.
fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("append");
    group.throughput(Throughput::Elements(1_000));

    group.bench_function("1000_records_batched", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let store = open(tmp.path());
                let records: Vec<LogRecord> = (0..1_000).map(record).collect();
                (tmp, store, records)
            },
            |(tmp, store, records)| {
                store.append(&records).unwrap();
                drop(tmp);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("1000_records_one_at_a_time", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let store = open(tmp.path());
                let records: Vec<LogRecord> = (0..1_000).map(record).collect();
                (tmp, store, records)
            },
            |(tmp, store, records)| {
                // What a chatty client produces: one request per line. The gap
                // between this and the batched case is the cost of lock acquisition
                // and per-call overhead, and is worth watching.
                for r in &records {
                    store.append(std::slice::from_ref(r)).unwrap();
                }
                drop(tmp);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

/// Sealing a buffer into a Parquet segment.
fn bench_seal(c: &mut Criterion) {
    let mut group = c.benchmark_group("seal");
    group.sample_size(20);
    group.throughput(Throughput::Elements(20_000));

    group.bench_function("20000_records", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::tempdir().unwrap();
                let store = open(tmp.path());
                let records: Vec<LogRecord> = (0..20_000).map(record).collect();
                store.append(&records).unwrap();
                (tmp, store)
            },
            |(tmp, store)| {
                store.seal_now().unwrap();
                drop(tmp);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

/// The query path, over a store big enough for pruning to matter.
fn bench_query(c: &mut Criterion) {
    let tmp = tempfile::tempdir().unwrap();
    let store = open(tmp.path());

    // 20 segments × 5000 records = 100k records on disk.
    for segment in 0..20u64 {
        let records: Vec<LogRecord> = (0..5_000).map(|i| record(segment * 5_000 + i)).collect();
        store.append(&records).unwrap();
        store.seal_now().unwrap();
    }

    let mut group = c.benchmark_group("query_100k_records");
    group.sample_size(20);

    // The shape a log viewer sends: newest 100, one app.
    group.bench_function("limit_100_backward", |b| {
        b.iter(|| {
            store
                .scan(
                    Scan {
                        start_nanos: 0,
                        end_nanos: u64::MAX,
                        limit: 100,
                        order: Order::Descending,
                        exact_key: None,
                        columns: None,
                    },
                    &[LabelMatcher::equal("app", "checkout")],
                    &|_| true,
                )
                .unwrap()
        });
    });

    // Same answer size, but with a line filter that rejects almost everything — this
    // is where streaming decode earns its keep.
    group.bench_function("limit_100_with_line_filter", |b| {
        b.iter(|| {
            store
                .scan(
                    Scan {
                        start_nanos: 0,
                        end_nanos: u64::MAX,
                        limit: 100,
                        order: Order::Descending,
                        exact_key: None,
                        columns: None,
                    },
                    &[],
                    &|r: &LogRecord| r.body.contains("status 200"),
                )
                .unwrap()
        });
    });

    // A narrow window: should touch one or two segments regardless of store size.
    group.bench_function("narrow_time_window", |b| {
        b.iter(|| {
            store
                .query(BASE + 50_000_000_000, BASE + 51_000_000_000, &[], &|_| true)
                .unwrap()
        });
    });

    // The pathological case, kept honest: no limit, whole store.
    group.bench_function("unbounded_full_scan", |b| {
        b.iter(|| store.query(0, u64::MAX, &[], &|_| true).unwrap());
    });

    group.finish();
}

/// Label matching, which runs once per candidate record.
fn bench_matching(c: &mut Criterion) {
    let labels = record(1).stream;
    let matchers = vec![
        LabelMatcher::equal("app", "cart"),
        LabelMatcher::new("level", telemetryd_core::MatchOp::Regex, "err.*").unwrap(),
    ];

    c.bench_function("matches_all_2_matchers", |b| {
        b.iter(|| telemetryd_core::matches_all(std::hint::black_box(&matchers), &labels));
    });
}

criterion_group!(
    benches,
    bench_arrow,
    bench_append,
    bench_seal,
    bench_query,
    bench_matching
);
criterion_main!(benches);
