//! How long opening a store with many sealed metric segments takes — the part of a
//! restart that is not the log: `cargo run --release -p telemetryd-store --example
//! open_timing -- <segments> <series>`.

#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use telemetryd_core::config::Config;
use telemetryd_core::{Labels, MetricKind, MetricSample};
use telemetryd_store::Store;

fn main() {
    let mut args = std::env::args().skip(1);
    let segments: u64 = args.next().and_then(|n| n.parse().ok()).unwrap_or(100);
    let series: u64 = args.next().and_then(|n| n.parse().ok()).unwrap_or(15_000);
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    config.limits.max_series = 0;
    let labels: Vec<Labels> = (0..series)
        .map(|i| {
            let mut labels = Labels::new();
            labels.insert("__name__", format!("metric_{}", i % 40));
            for (k, v) in [
                ("app", "cbox-web"),
                ("service_name", "cbox-web"),
                ("deployment_environment_name", "production"),
                ("host_name", "web01"),
                ("process_runtime_name", "php"),
                ("http_request_method", "GET"),
            ] {
                labels.insert(k, v);
            }
            labels.insert("http_route", format!("/route/{}", i / 40 % 60));
            labels.insert("le", format!("{}", i % 7));
            labels
        })
        .collect();
    {
        let store = Store::open(&config).unwrap();
        let start = 1_790_000_000_000_000_000u64;
        for s in 0..segments {
            let batch: Vec<MetricSample> = labels
                .iter()
                .enumerate()
                .map(|(i, series)| MetricSample {
                    timestamp_nanos: start + s * 3_600_000_000_000 + i as u64,
                    series: series.clone(),
                    value: i as f64,
                    kind: MetricKind::Counter,
                })
                .collect();
            store.metrics().append(&batch).unwrap();
            store.metrics().seal_now().unwrap();
        }
    }
    let rounds: usize = std::env::var("ROUNDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(3);
    for _ in 0..rounds {
        let started = std::time::Instant::now();
        let store = Store::open(&config).unwrap();
        println!(
            "opened {} segments of {series} series in {:?}",
            store.metrics().status().segments,
            started.elapsed()
        );
    }
}
