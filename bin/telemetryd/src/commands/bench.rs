//! `telemetryd bench` — what this machine can actually do.
//!
//! Sizing a self-hosted service is guesswork without a number, and the numbers in our
//! documentation came from our hardware, not yours. This drives synthetic telemetry
//! into a throwaway store while querying it, and reports what the machine managed.
//!
//! It measures the **storage engine**, not the HTTP surface: no sockets, no JSON
//! decode. That is deliberate — those costs are real but they scale with cores and a
//! reverse proxy, while the storage engine is the part that is hard to reason about
//! and the part that decides how much a single box can hold.
//!
//! Every run uses a temporary directory and deletes it afterwards. It will never touch
//! a real data directory, because a benchmark that can destroy production data is a
//! benchmark nobody should run.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use telemetryd_core::config::{Compression, WalSync};
use telemetryd_core::{LabelMatcher, Labels, LogRecord, Severity};
use telemetryd_store::logs::LogSchema;
use telemetryd_store::records::{RecordStore, Scan, StoreSettings};
use telemetryd_store::topk::Order;

#[derive(Debug, clap::Args)]
pub struct BenchArgs {
    /// How long to run for.
    #[arg(long, value_name = "DURATION", default_value = "20s")]
    duration: humantime::Duration,

    /// Concurrent writers. Roughly "how many app servers are sending to me".
    #[arg(long, default_value_t = 4)]
    writers: usize,

    /// Records per append, as a client's batch would arrive.
    #[arg(long, default_value_t = 500)]
    batch: usize,

    /// Buffer size before a segment is sealed. The main lever on memory.
    #[arg(long, value_name = "SIZE", default_value = "64MiB")]
    segment_bytes: bytesize::ByteSize,
}

pub fn run(args: &BenchArgs) -> anyhow::Result<()> {
    let duration: Duration = args.duration.into();
    // Made with std rather than a crate: `tempfile` is a dev-dependency here, and a
    // hidden benchmark is not a good reason to add a runtime one.
    let dir = scratch_dir()?;
    for sub in ["wal", "segments", "tmp"] {
        std::fs::create_dir_all(dir.join(sub))?;
    }
    let cleanup = CleanupOnDrop(dir.clone());

    let store = Arc::new(
        RecordStore::<LogSchema>::open(
            &dir.join("wal"),
            dir.join("segments"),
            dir.join("tmp"),
            StoreSettings {
                segment_duration: Duration::from_secs(3600),
                max_segment_bytes: args.segment_bytes.as_u64(),
                // The benchmark is about our own code. Measuring this disk's fsync
                // latency tells you about the disk, which you can measure directly.
                wal_sync: WalSync::Never,
                wal_sync_interval: Duration::ZERO,
                compression: Compression::Zstd,
                query_parallelism: 1,
            },
        )
        .context("opening the benchmark store")?,
    );

    crate::out::outln!(
        "benchmarking for {} with {} writer(s), batches of {}, sealing at {}\n",
        args.duration,
        args.writers,
        args.batch,
        args.segment_bytes
    );

    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(Mutex::new(Vec::<f64>::new()));

    let started = Instant::now();
    let mut threads = Vec::new();

    for writer in 0..args.writers {
        let (store, stop, written) = (Arc::clone(&store), Arc::clone(&stop), Arc::clone(&written));
        let batch_size = args.batch;
        threads.push(std::thread::spawn(move || -> anyhow::Result<()> {
            let base = 1_760_000_000_000_000_000 + (writer as u64) * 1_000_000_000_000;
            let mut offset = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let batch: Vec<LogRecord> = (0..batch_size as u64)
                    .map(|i| record(writer, base + (offset + i) * 1_000))
                    .collect();
                store.append(&batch)?;
                written.fetch_add(batch_size as u64, Ordering::Relaxed);
                offset += batch_size as u64;
            }
            Ok(())
        }));
    }

    // One reader, the shape a log viewer sends: newest hundred for one app.
    {
        let (store, stop, latencies) = (
            Arc::clone(&store),
            Arc::clone(&stop),
            Arc::clone(&latencies),
        );
        threads.push(std::thread::spawn(move || -> anyhow::Result<()> {
            let matchers = [LabelMatcher::equal("app", "app-0")];
            while !stop.load(Ordering::Relaxed) {
                let at = Instant::now();
                store.scan(
                    Scan {
                        start_nanos: 0,
                        end_nanos: u64::MAX,
                        limit: 100,
                        order: Order::Descending,
                        exact_key: None,
                        columns: None,
                        required_text: None,
                    },
                    &matchers,
                    &|_| true,
                )?;
                let elapsed = at.elapsed().as_secs_f64() * 1000.0;
                lock(&latencies).push(elapsed);
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(())
        }));
    }

    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    for thread in threads {
        thread
            .join()
            .map_err(|_| anyhow::anyhow!("a benchmark thread panicked"))??;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let total = written.load(Ordering::Relaxed);
    report(&store.status(), total, elapsed, &lock(&latencies));
    drop(cleanup);
    Ok(())
}

/// Print the result. Counts are exact; the derived figures are approximate by nature,
/// so the `f64` conversions below lose nothing anyone would notice.
#[allow(clippy::cast_precision_loss)]
fn report(
    status: &telemetryd_store::RecordStoreStatus,
    total: u64,
    elapsed: f64,
    latencies: &[f64],
) {
    crate::out::outln!("ingest");
    crate::out::outln!("  records          {total:>12}");
    crate::out::outln!("  rate             {:>12.0} /s", total as f64 / elapsed);
    crate::out::outln!("  segments sealed  {:>12}", status.sealed_segments);
    crate::out::outln!(
        "  buffered         {:>12}  ({})",
        status.buffered_records,
        bytesize::ByteSize::b(status.buffered_bytes)
    );
    crate::out::outln!(
        "  on disk          {:>12}  ({} per record)",
        bytesize::ByteSize::b(status.segment_bytes).to_string(),
        if status.segment_rows == 0 {
            "n/a".to_owned()
        } else {
            format!(
                "{:.1} B",
                status.segment_bytes as f64 / status.segment_rows as f64
            )
        }
    );

    let mut samples = latencies.to_vec();
    if samples.is_empty() {
        crate::out::outln!("\nno queries completed — try a longer --duration");
        return;
    }
    samples.sort_by(f64::total_cmp);
    crate::out::outln!("\nquery, newest 100 for one app, while all of that was being written");
    crate::out::outln!("  (writers run flat out with no pauses, which no real client does — treat");
    crate::out::outln!("   these as the worst case, not the typical one)");
    crate::out::outln!("  count            {:>12}", samples.len());
    for (label, value) in [
        ("p50", percentile(&samples, 50)),
        ("p95", percentile(&samples, 95)),
        ("p99", percentile(&samples, 99)),
        ("max", samples[samples.len() - 1]),
    ] {
        crate::out::outln!("  {label}              {value:>12.1} ms");
    }

    crate::out::outln!(
        "\nMemory is roughly 80 MB plus about 1.3x --segment-bytes; lower it if this \n\
         machine is tight, at the cost of more, smaller segments."
    );
}

/// A unique directory under the platform temp dir.
fn scratch_dir() -> anyhow::Result<std::path::PathBuf> {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let dir =
        std::env::temp_dir().join(format!("telemetryd-bench-{}-{unique}", std::process::id()));
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

/// Removes the scratch directory however the benchmark ends, including on a panic in
/// one of the threads — a benchmark that leaves gigabytes behind gets run once.
struct CleanupOnDrop(std::path::PathBuf);

impl Drop for CleanupOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Nearest-rank percentile, in integer arithmetic.
///
/// `fraction` is given in hundredths so the index never round-trips through a float:
/// with a handful of samples the interpolated variants mostly report their own
/// arithmetic anyway, and this keeps the index exact.
fn percentile(sorted: &[f64], hundredths: usize) -> f64 {
    let index = (sorted.len() * hundredths / 100).min(sorted.len() - 1);
    sorted[index]
}

fn record(writer: usize, timestamp_nanos: u64) -> LogRecord {
    let mut stream = Labels::new();
    stream.insert("app", format!("app-{writer}"));
    stream.insert("level", "info");
    let mut attributes = Labels::new();
    attributes.insert("exception.type", "TimeoutError");
    LogRecord {
        timestamp_nanos,
        stream,
        severity: Severity::Info,
        severity_text: "INFO".to_owned(),
        body: format!("payment attempt {timestamp_nanos} for order 1000 took 42ms"),
        attributes,
        trace_id: None,
        span_id: None,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
