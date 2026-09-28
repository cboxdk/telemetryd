//! How long a restart spends replaying the write-ahead log, with a metric buffer shaped
//! like a busy host's: `cargo run --release -p telemetryd-store --example replay_timing`.

#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use telemetryd_core::config::Config;
use telemetryd_core::{Labels, MetricKind, MetricSample};
use telemetryd_store::Store;

fn main() {
    let records: u64 = std::env::args()
        .nth(1)
        .and_then(|n| n.parse().ok())
        .unwrap_or(800_000);
    let dir = tempfile::tempdir().unwrap();
    let copy_dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    let series: Vec<Labels> = (0..2_000)
        .map(|i| {
            let mut labels = Labels::new();
            labels.insert("__name__", "http_server_request_duration_seconds_bucket");
            for (k, v) in [
                ("app", "cbox-web"),
                ("service_name", "cbox-web"),
                ("deployment_environment_name", "production"),
                ("http_request_method", "GET"),
                ("host_name", "web01"),
                ("process_runtime_name", "php"),
            ] {
                labels.insert(k, v);
            }
            labels.insert("http_route", format!("/route/{}", i / 20));
            labels.insert("le", format!("{}", i % 20));
            labels.insert("http_response_status_code", "200");
            labels
        })
        .collect();
    {
        let store = Store::open(&config).unwrap();
        let start = 1_790_000_000_000_000_000u64;
        let batch: Vec<MetricSample> = (0..records)
            .map(|i| MetricSample {
                timestamp_nanos: start + (i / 2_000) * 15_000_000_000,
                series: series[(i % 2_000) as usize].clone(),
                value: i as f64,
                kind: MetricKind::Counter,
            })
            .collect();
        for chunk in batch.chunks(10_000) {
            store.metrics().append(chunk).unwrap();
        }
        store.sync_all().unwrap();
        // A copy of the directory as a crash would leave it: the log synced, the
        // buffer never sealed, and no lock held by anyone.
        copy(dir.path(), copy_dir.path());
        std::mem::forget(store);
    }
    config.storage.data_dir = Some(copy_dir.path().to_path_buf());
    let started = std::time::Instant::now();
    let store = Store::open(&config).unwrap();
    println!(
        "replayed {} records in {:?}",
        store.metrics().status().buffered_records,
        started.elapsed()
    );
}

fn copy(from: &std::path::Path, to: &std::path::Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            std::fs::create_dir_all(&target).unwrap();
            copy(&entry.path(), &target);
        } else if entry.file_name() != "LOCK" && entry.file_name() != ".lock" {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}
