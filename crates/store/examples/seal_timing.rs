//! How long sealing a buffer of metric samples takes, and its batch-building part alone:
//! `cargo run --release -p telemetryd-store --example seal_timing -- <records> <series>`.
//! With `SEAL_ONLY` set it only seals, so the process's peak memory is the seal's.

#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use std::time::Instant;

use telemetryd_core::config::Config;
use telemetryd_core::{Labels, MetricKind, MetricSample};
use telemetryd_store::schema::RecordSchema;
use telemetryd_store::{MetricSchema, Store};

fn main() {
    let mut args = std::env::args().skip(1);
    let records: usize = args
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(5_000_000);
    let series: usize = args.next().and_then(|n| n.parse().ok()).unwrap_or(850);
    let labels: Vec<Labels> = (0..series)
        .map(|i| {
            let mut labels = Labels::new();
            labels.insert("__name__", format!("metric_{}", i % 17));
            labels.insert("app", "bench");
            labels.insert("http_route", format!("/route/{}", i / 17));
            labels.insert("le", format!("{}", i % 15));
            labels
        })
        .collect();
    // Interleaved, as a buffer holds them: every series at each instant in turn.
    let samples: Vec<MetricSample> = (0..records)
        .map(|i| MetricSample {
            series: labels[i % series].clone(),
            timestamp_nanos: 1_790_000_000_000_000_000 + (i / series) as u64 * 30_000_000_000,
            value: i as f64,
            kind: MetricKind::Counter,
        })
        .collect();

    if std::env::var("SEAL_ONLY").is_err() {
        let at = Instant::now();
        let (batch, _) = MetricSchema::to_batch(&samples).unwrap();
        println!(
            "to_batch (time order)  {:?}  {} rows",
            at.elapsed(),
            batch.num_rows()
        );
        drop(batch);
        let at = Instant::now();
        let (batch, _) = MetricSchema::to_batch_by_stream(&samples).unwrap();
        println!(
            "to_batch_by_stream     {:?}  {} rows",
            at.elapsed(),
            batch.num_rows()
        );
        drop(batch);
    }

    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    config.limits.max_series = 0;
    let store = Store::open(&config).unwrap();
    for chunk in samples.chunks(50_000) {
        store.metrics().append(chunk).unwrap();
    }
    drop(samples);
    let at = Instant::now();
    store.metrics().seal_now().unwrap();
    println!("seal_now               {:?}", at.elapsed());
}
