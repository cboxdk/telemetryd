//! telemetryd's storage engine.
//!
//! Two engines, one lifecycle (ADR-001). The record store handles logs, spans and
//! events — identical machinery, different Arrow schema. Metrics get their own chunk
//! store in M3. Both register in one retention pass, so the disk budget means the same
//! thing everywhere.

pub mod bloom;
pub mod datadir;
pub mod logs;
pub mod metrics;
pub mod records;
pub mod retention;
pub mod schema;
pub mod segment;
pub mod spans;
pub mod topk;
pub mod wal;

use std::collections::BTreeMap;
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use telemetryd_core::MetricSample;
use telemetryd_core::config::{Config, RetentionConfig};
use telemetryd_core::span::SpanRecord;
use telemetryd_core::{LogRecord, Result, Signal};

pub use datadir::{DataDir, DiskUsage};
pub use logs::LogSchema;
pub use metrics::MetricSchema;
pub use records::{RecordStore, RecordStoreStatus, Scan, StoreSettings};
pub use retention::{Candidate, Plan, ReaperReport};
pub use segment::{Segment, SegmentManifest};
pub use spans::SpanSchema;
pub use topk::Order;
pub use wal::{Truncation, TruncationReason, Wal, WalStats};

/// Wall-clock now, in Unix nanoseconds.
pub fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// The storage engine handle. One per process — [`DataDir`] enforces that.
#[derive(Debug)]
pub struct Store {
    data_dir: DataDir,
    logs: RecordStore<LogSchema>,
    traces: RecordStore<SpanSchema>,
    metrics: RecordStore<MetricSchema>,
    disk_budget: u64,
    retention: RetentionConfig,
    reaper: Mutex<ReaperReport>,
    /// Kept for the process lifetime rather than only logged at startup — a crash
    /// that cost records should stay visible in `/status`.
    wal_truncations: RwLock<Vec<Truncation>>,
}

impl Store {
    /// Open (or create) the store, replaying whatever the write-ahead log still holds.
    pub fn open(config: &Config) -> Result<Self> {
        let root = config.storage.resolve_data_dir();
        let data_dir = DataDir::open(&root)?;
        data_dir.clean_tmp()?;

        let settings = StoreSettings::from(&config.storage);
        let logs = RecordStore::<LogSchema>::open(
            &data_dir.wal_dir(Signal::Logs),
            data_dir.segments_dir(Signal::Logs),
            data_dir.tmp_dir(),
            settings,
        )?;

        let traces = RecordStore::<SpanSchema>::open(
            &data_dir.wal_dir(Signal::Traces),
            data_dir.segments_dir(Signal::Traces),
            data_dir.tmp_dir(),
            settings,
        )?;

        let metrics = RecordStore::<MetricSchema>::open(
            &data_dir.wal_dir(Signal::Metrics),
            data_dir.segments_dir(Signal::Metrics),
            data_dir.tmp_dir(),
            settings,
        )?;

        Ok(Self {
            data_dir,
            logs,
            traces,
            metrics,
            disk_budget: config.storage.disk_budget.as_u64(),
            retention: config.retention.clone(),
            reaper: Mutex::new(ReaperReport::default()),
            wal_truncations: RwLock::new(Vec::new()),
        })
    }

    pub fn logs(&self) -> &RecordStore<LogSchema> {
        &self.logs
    }

    pub fn traces(&self) -> &RecordStore<SpanSchema> {
        &self.traces
    }

    /// Append trace spans. Durable before this returns.
    pub fn append_spans(&self, records: &[SpanRecord]) -> Result<()> {
        self.traces.append(records)
    }

    pub fn metrics(&self) -> &RecordStore<MetricSchema> {
        &self.metrics
    }

    /// Append metric samples. Durable before this returns.
    pub fn append_samples(&self, records: &[MetricSample]) -> Result<()> {
        self.metrics.append(records)
    }

    pub fn data_dir(&self) -> &DataDir {
        &self.data_dir
    }

    /// Append log records. Durable before this returns.
    pub fn append_logs(&self, records: &[LogRecord]) -> Result<()> {
        self.logs.append(records)
    }

    /// Flush and fsync every log. Called on graceful shutdown so a clean stop never
    /// loses the interval-sync window.
    pub fn sync_all(&self) -> Result<()> {
        self.logs.sync()?;
        self.traces.sync()?;
        self.metrics.sync()
    }

    /// Apply the configured sync policy without forcing a flush.
    pub fn maybe_sync(&self) -> Result<()> {
        self.logs.maybe_sync()?;
        self.traces.maybe_sync()?;
        self.metrics.maybe_sync()
    }

    /// Seal any buffer whose window has elapsed.
    pub fn maybe_seal(&self) -> Result<()> {
        self.logs.maybe_seal()?;
        self.traces.maybe_seal()?;
        self.metrics.maybe_seal()?;
        Ok(())
    }

    /// Seal everything, regardless of window. Used on shutdown and by tests.
    pub fn seal_all(&self) -> Result<()> {
        self.logs.seal_now()?;
        self.traces.seal_now()?;
        self.metrics.seal_now()?;
        Ok(())
    }

    /// Run one retention pass: expire by age, then enforce the disk budget.
    ///
    /// Every deletion is reported — logged, counted, and reflected in `/status`.
    /// Silently discarding a user's telemetry to stay under a budget would be the
    /// single most damaging thing this process could do quietly.
    pub fn run_retention(&self) -> Result<ReaperReport> {
        let usage = self.data_dir.usage()?;

        // Every signal contributes candidates to one plan, because the disk budget is
        // global — the oldest data anywhere is what goes.
        let candidates: Vec<Candidate> = self
            .logs
            .segments()
            .iter()
            .chain(self.traces.segments().iter())
            .chain(self.metrics.segments().iter())
            .map(|segment| Candidate {
                signal: segment.manifest.signal,
                id: segment.manifest.id.clone(),
                max_time_nanos: segment.manifest.max_time_nanos,
                bytes: segment.manifest.bytes,
            })
            .collect();

        let segment_bytes: u64 = candidates.iter().map(|c| c.bytes).sum();
        let non_segment_bytes = usage.total().saturating_sub(segment_bytes);

        let plan = retention::plan(
            &candidates,
            now_nanos(),
            &self.retention_windows(),
            self.disk_budget,
            non_segment_bytes,
        );

        let mut report = ReaperReport {
            last_run_unix_nanos: now_nanos(),
            ..ReaperReport::default()
        };

        for candidate in &plan.by_age {
            if self.delete_segment(candidate)? {
                report.deleted_by_age += 1;
                report.bytes_freed += candidate.bytes;
            }
        }
        for candidate in &plan.by_budget {
            if self.delete_segment(candidate)? {
                report.deleted_by_budget += 1;
                report.bytes_freed += candidate.bytes;
            }
        }

        if report.deleted_by_age > 0 {
            tracing::info!(
                segments = report.deleted_by_age,
                bytes = report.bytes_freed,
                "expired segments past their retention window"
            );
        }
        if report.deleted_by_budget > 0 {
            // Deleting data the operator asked to keep is a WARN, always. It means the
            // budget and the retention window are in conflict and one of them is wrong.
            tracing::warn!(
                segments = report.deleted_by_budget,
                budget_bytes = self.disk_budget,
                "deleted segments that were still inside their retention window to \
                 stay under storage.disk_budget — raise the budget or shorten retention"
            );
        }

        let after = self.data_dir.usage()?.total();
        report.still_over_budget = after > self.disk_budget;
        if report.still_over_budget {
            tracing::error!(
                used_bytes = after,
                budget_bytes = self.disk_budget,
                "still over the disk budget after deleting everything eligible"
            );
        }

        *lock(&self.reaper) = report.clone();
        Ok(report)
    }

    fn delete_segment(&self, candidate: &Candidate) -> Result<bool> {
        match candidate.signal {
            Signal::Logs => self.logs.remove_segment(&candidate.id),
            Signal::Traces => self.traces.remove_segment(&candidate.id),
            Signal::Metrics => self.metrics.remove_segment(&candidate.id),
            // Events land later. Nothing else produces candidates yet, so this is a
            // loud no-op rather than a silent skip.
            Signal::Events => {
                tracing::warn!(signal = %Signal::Events, "retention has no store for events yet");
                Ok(false)
            }
        }
    }

    fn retention_windows(&self) -> BTreeMap<Signal, Duration> {
        BTreeMap::from([
            (Signal::Logs, self.retention.logs.get()),
            (Signal::Traces, self.retention.traces.get()),
            (Signal::Events, self.retention.events.get()),
            (Signal::Metrics, self.retention.metrics.get()),
        ])
    }

    pub fn record_wal_truncation(&self, truncation: Truncation) {
        lock_write(&self.wal_truncations).push(truncation);
    }

    /// A point-in-time view for `/status`.
    pub fn snapshot(&self) -> Result<StoreStatus> {
        let usage = self.data_dir.usage()?;
        let used = usage.total();

        Ok(StoreStatus {
            data_dir: self.data_dir.root().display().to_string(),
            disk_budget_bytes: self.disk_budget,
            disk_used_bytes: used,
            // Reported, not clamped: a ratio above 1.0 is exactly the signal an
            // operator needs to see, and hiding it would defeat the point.
            #[allow(clippy::cast_precision_loss)]
            disk_used_ratio: if self.disk_budget == 0 {
                0.0
            } else {
                used as f64 / self.disk_budget as f64
            },
            over_budget: used > self.disk_budget,
            usage,
            logs: self.logs.status(),
            traces: self.traces.status(),
            metrics: self.metrics.status(),
            retention: lock(&self.reaper).clone(),
            wal_truncations: lock_read(&self.wal_truncations).clone(),
        })
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StoreStatus {
    pub data_dir: String,
    pub disk_budget_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_used_ratio: f64,
    pub over_budget: bool,
    pub usage: DiskUsage,
    pub logs: RecordStoreStatus,
    pub traces: RecordStoreStatus,
    pub metrics: RecordStoreStatus,
    pub retention: ReaperReport,
    /// Non-empty when a crash cost us records. "Degrade loudly."
    pub wal_truncations: Vec<Truncation>,
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn lock_write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytesize::ByteSize;
    use telemetryd_core::config::{DurationSetting, StorageConfig};
    use telemetryd_core::{Labels, Severity};

    fn config(dir: &std::path::Path) -> Config {
        Config {
            storage: StorageConfig {
                data_dir: Some(dir.to_path_buf()),
                ..StorageConfig::default()
            },
            ..Config::default()
        }
    }

    fn record(ts: u64, body: &str) -> LogRecord {
        let mut stream = Labels::new();
        stream.insert("app", "checkout");
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

    #[test]
    fn opens_appends_and_recovers_across_restarts() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config(&tmp.path().join("data"));

        {
            let store = Store::open(&config).unwrap();
            let records: Vec<LogRecord> =
                (0..25).map(|i| record(now_nanos() + i, "hello")).collect();
            store.append_logs(&records).unwrap();
            store.sync_all().unwrap();
        }

        let reopened = Store::open(&config).unwrap();
        assert_eq!(reopened.logs().status().recovered_records, 25);
        assert_eq!(
            reopened
                .logs()
                .query(0, u64::MAX, &[], &|_| true)
                .unwrap()
                .len(),
            25
        );
    }

    #[test]
    fn a_second_store_on_the_same_directory_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config(&tmp.path().join("data"));

        let _first = Store::open(&config).unwrap();
        let err = Store::open(&config).unwrap_err();
        assert!(
            matches!(err, telemetryd_core::Error::DataDirLocked { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn retention_expires_segments_past_the_window() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = config(&tmp.path().join("data"));
        config.retention.logs = DurationSetting(Duration::from_secs(3600));
        config.storage.segment_duration = DurationSetting(Duration::from_secs(60));

        let store = Store::open(&config).unwrap();
        let now = now_nanos();

        // One segment well past the window, one inside it.
        store
            .append_logs(&[record(now - 10 * 3_600_000_000_000, "ancient")])
            .unwrap();
        store.seal_all().unwrap();
        store.append_logs(&[record(now, "recent")]).unwrap();
        store.seal_all().unwrap();
        assert_eq!(store.logs().segments().len(), 2);

        let report = store.run_retention().unwrap();
        assert_eq!(report.deleted_by_age, 1);
        assert_eq!(report.deleted_by_budget, 0);

        let remaining = store.logs().query(0, u64::MAX, &[], &|_| true).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].body, "recent");
    }

    #[test]
    fn retention_enforces_the_disk_budget_and_says_so() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = config(&tmp.path().join("data"));
        config.storage.disk_budget = ByteSize::kib(8);

        let store = Store::open(&config).unwrap();
        let now = now_nanos();
        for i in 0..12u64 {
            store
                .append_logs(&[record(now + i * 1_000_000, &"payload ".repeat(200))])
                .unwrap();
            store.seal_all().unwrap();
        }

        let before = store.snapshot().unwrap();
        assert!(
            before.over_budget,
            "test needs to actually exceed the budget"
        );

        let report = store.run_retention().unwrap();
        assert!(
            report.deleted_by_budget > 0,
            "budget enforcement never fired"
        );
        assert!(report.bytes_freed > 0);

        // And the outcome is visible without reading logs.
        let after = store.snapshot().unwrap();
        assert_eq!(after.retention.deleted_by_budget, report.deleted_by_budget);
    }

    #[test]
    fn a_healthy_store_deletes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&config(&tmp.path().join("data"))).unwrap();
        store.append_logs(&[record(now_nanos(), "x")]).unwrap();
        store.seal_all().unwrap();

        let report = store.run_retention().unwrap();
        assert_eq!(report.deleted_by_age, 0);
        assert_eq!(report.deleted_by_budget, 0);
        assert!(!report.still_over_budget);
        assert_eq!(store.logs().segments().len(), 1);
    }

    #[test]
    fn status_reports_budget_usage_and_log_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&config(&tmp.path().join("data"))).unwrap();

        let empty = store.snapshot().unwrap();
        assert!(!empty.over_budget);
        assert_eq!(empty.logs.segments, 0);

        store.append_logs(&[record(now_nanos(), "x")]).unwrap();
        store.seal_all().unwrap();

        let after = store.snapshot().unwrap();
        assert_eq!(after.logs.segments, 1);
        assert_eq!(after.logs.segment_rows, 1);
        assert!(after.disk_used_bytes > 0);
    }
}
