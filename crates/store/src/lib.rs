//! telemetryd's storage engine.
//!
//! Two engines, one lifecycle. The record store handles logs, spans and
//! events — identical machinery, different Arrow schema. Metrics get their own chunk
//! store in M3. Both register in one retention pass, so the disk budget means the same
//! thing everywhere.

pub mod bloom;
pub mod cardinality;
pub mod datadir;
pub mod logs;
pub mod metrics;
pub mod records;
pub mod retention;
pub mod schema;
pub mod segment;
pub mod spans;
pub mod topk;
pub mod trigram;
pub mod wal;

use std::collections::BTreeMap;

use serde::Serialize;
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use telemetryd_core::MetricSample;
use telemetryd_core::config::{Config, RetentionConfig};
use telemetryd_core::span::SpanRecord;

use crate::schema::RecordSchema;
use telemetryd_core::{Labels, LogRecord, Result, Signal};

pub use cardinality::{Admission, Admitted, Cardinality};
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
    /// The two settings an operator most often needs to change *now* — usually
    /// because a disk is filling — so they live behind a lock and can be replaced
    /// without a restart. Read only by the reaper, which is a cold path, so the lock
    /// costs nothing on ingest or query.
    policy: RwLock<RetentionPolicy>,
    /// The ceiling on distinct series, enforced at ingest.
    cardinality: Cardinality,
    reaper: Mutex<ReaperReport>,
    /// Kept for the process lifetime rather than only logged at startup — a crash
    /// that cost records should stay visible in `/status`.
    wal_truncations: RwLock<Vec<Truncation>>,
}

/// What one app holds in the store.
#[derive(Debug, Default, Clone, Serialize)]
pub struct AppUsage {
    pub app: String,
    /// Distinct series. Exact.
    pub series: u64,
    /// Rows in sealed segments. Exact.
    pub rows: u64,
    /// Apportioned by row share, because a segment mixes apps. An estimate, named as
    /// one so nobody builds billing on it.
    pub estimated_bytes: u64,
}

fn app_of(labels: &Labels) -> &str {
    labels
        .get(telemetryd_core::APP_LABEL)
        .unwrap_or(telemetryd_core::UNKNOWN_APP)
}

/// The reloadable half of the storage configuration.
#[derive(Debug, Clone)]
struct RetentionPolicy {
    disk_budget: u64,
    retention: RetentionConfig,
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
            cardinality: Cardinality::new(
                config.limits.max_series,
                config.limits.max_series_per_app,
            ),
            policy: RwLock::new(RetentionPolicy {
                disk_budget: config.storage.disk_budget.as_u64(),
                retention: config.retention.clone(),
            }),
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
    pub fn append_spans(&self, records: &[SpanRecord]) -> Result<Admitted> {
        self.append_within_limits(records, SpanSchema::index_labels, |kept| {
            self.traces.append(kept)
        })
    }

    pub fn metrics(&self) -> &RecordStore<MetricSchema> {
        &self.metrics
    }

    /// Append metric samples. Durable before this returns.
    pub fn append_samples(&self, records: &[MetricSample]) -> Result<Admitted> {
        self.append_within_limits(records, MetricSchema::index_labels, |kept| {
            self.metrics.append(kept)
        })
    }

    pub fn data_dir(&self) -> &DataDir {
        &self.data_dir
    }

    /// Append log records. Durable before this returns.
    pub fn append_logs(&self, records: &[LogRecord]) -> Result<Admitted> {
        self.append_within_limits(records, LogSchema::index_labels, |kept| {
            self.logs.append(kept)
        })
    }

    /// Store the records whose series fit inside the cardinality caps, and report the
    /// rest.
    ///
    /// Only *new* series are ever refused. An app already at its limit keeps working;
    /// what stops is its ability to invent more series. Turning a labelling mistake
    /// into an outage of the telemetry you still have would be the wrong trade.
    ///
    /// The accepted records are passed through untouched when nothing was rejected,
    /// which is the overwhelmingly common case and worth not copying a batch for.
    fn append_within_limits<T: Clone>(
        &self,
        records: &[T],
        labels: impl Fn(&T) -> &Labels,
        store: impl FnOnce(&[T]) -> Result<()>,
    ) -> Result<Admitted> {
        let mut kept: Option<Vec<T>> = None;
        let mut admitted = Admitted {
            stored: records.len(),
            rejected: 0,
            reason: None,
        };

        for (index, record) in records.iter().enumerate() {
            let series = labels(record);
            let app = series.get(telemetryd_core::APP_LABEL).unwrap_or("unknown");
            let verdict = self.cardinality.admit(app, series);
            if verdict.is_accepted() {
                if let Some(kept) = &mut kept {
                    kept.push(record.clone());
                }
            } else {
                let kept = kept.get_or_insert_with(|| records[..index].to_vec());
                let _ = kept;
                admitted.rejected += 1;
                admitted.reason.get_or_insert(verdict.reason());
            }
        }

        let to_store = kept.as_deref().unwrap_or(records);
        admitted.stored = to_store.len();
        store(to_store)?;

        if let Some(reason) = admitted.reason {
            // Loud, every time: a cap that silently drops data is indistinguishable
            // from a bug in the producer, and someone will spend a day on it.
            tracing::warn!(
                rejected = admitted.rejected,
                stored = admitted.stored,
                limit = reason,
                active_series = self.cardinality.active_series(),
                "refused records that would have created new series past the \
                 cardinality limit; raise the limit or reduce the labels being sent"
            );
        }
        Ok(admitted)
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
    /// Total segments sealed across all signals, for cheap change detection.
    ///
    /// Sealing is the only thing that grows segment bytes, so this changing is the
    /// signal that the disk budget might now be exceeded. Three atomic loads, versus a
    /// filesystem walk to answer the same question.
    #[must_use]
    pub fn sealed_count(&self) -> u64 {
        self.logs.sealed_count() + self.traces.sealed_count() + self.metrics.sealed_count()
    }

    /// Reap with nothing held back. What a store that forwards nowhere does.
    pub fn run_retention(&self) -> Result<ReaperReport> {
        self.run_retention_protecting(retention::Undelivered::default())
    }

    /// Reap while holding back segments a relay has not forwarded yet.
    ///
    /// The store does not know what a relay is, and should not: it is handed the ids
    /// to protect. That keeps delivery policy in the layer that owns delivery, and
    /// keeps this function testable without a network.
    pub fn run_retention_protecting(
        &self,
        undelivered: retention::Undelivered<'_>,
    ) -> Result<ReaperReport> {
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
            self.disk_budget(),
            non_segment_bytes,
            undelivered,
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
        for candidate in &plan.undelivered_dropped {
            if self.delete_segment(candidate)? {
                report.dropped_undelivered += 1;
                report.bytes_freed += candidate.bytes;
            }
        }
        report.blocked_by_undelivered = plan.blocked_by_undelivered;

        if report.dropped_undelivered > 0 {
            // Telemetry that never reached its destination and now never will. Louder
            // than the budget warning below, because the budget one costs a copy and
            // this one costs the only copy.
            tracing::error!(
                segments = report.dropped_undelivered,
                budget_bytes = self.disk_budget(),
                "deleted segments that were never forwarded upstream: the disk budget \
                 could not be held any other way. Raise storage.disk_budget, or set \
                 relay.when_full = \"reject\" to push back on clients instead of \
                 losing their telemetry."
            );
        }
        if report.blocked_by_undelivered {
            tracing::error!(
                budget_bytes = self.disk_budget(),
                "the disk budget is full of telemetry that has not been forwarded \
                 upstream, and relay.when_full = \"reject\", so ingest is being \
                 refused. Fix the upstream, or raise storage.disk_budget."
            );
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
                budget_bytes = self.disk_budget(),
                "deleted segments that were still inside their retention window to \
                 stay under storage.disk_budget — raise the budget or shorten retention"
            );
        }

        let after = self.data_dir.usage()?.total();
        // Retention just deleted series along with their segments. Recount from what
        // survived, or the limiter would keep refusing new series against a budget
        // held by data that no longer exists.
        self.refresh_cardinality();

        report.still_over_budget = after > self.disk_budget();
        if report.still_over_budget {
            tracing::error!(
                used_bytes = after,
                budget_bytes = self.disk_budget(),
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
        }
    }

    fn retention_windows(&self) -> BTreeMap<Signal, Duration> {
        let policy = lock_read(&self.policy);
        BTreeMap::from([
            (Signal::Logs, policy.retention.logs.get()),
            (Signal::Traces, policy.retention.traces.get()),
            (Signal::Metrics, policy.retention.metrics.get()),
        ])
    }

    /// Replace the retention windows and disk budget without restarting.
    ///
    /// Returns a description of what changed, for the operator to see in the log. An
    /// empty result means the reloaded configuration asked for nothing new, which is
    /// worth saying out loud — "reloaded" and "changed something" are different facts.
    /// The retention windows and disk budget currently in force.
    ///
    /// The store is the authority: `apply_retention_policy` writes here on `SIGHUP`,
    /// and the reaper reads here. `/status` used to report the configuration captured
    /// at startup instead, so after any reload it told an operator the old window while
    /// the reaper enforced the new one — on the one field people check when asking
    /// where their data went.
    #[must_use]
    pub fn retention_in_force(&self) -> (RetentionConfig, u64) {
        let policy = lock_read(&self.policy);
        (policy.retention.clone(), policy.disk_budget)
    }

    pub fn apply_retention_policy(&self, config: &Config) -> Vec<String> {
        let mut policy = lock_write(&self.policy);
        let mut changes = Vec::new();

        let budget = config.storage.disk_budget.as_u64();
        if budget != policy.disk_budget {
            changes.push(format!(
                "storage.disk_budget {} -> {}",
                bytesize::ByteSize::b(policy.disk_budget),
                bytesize::ByteSize::b(budget)
            ));
            policy.disk_budget = budget;
        }

        for (name, old, new) in [
            (
                "logs",
                policy.retention.logs.get(),
                config.retention.logs.get(),
            ),
            (
                "traces",
                policy.retention.traces.get(),
                config.retention.traces.get(),
            ),
            (
                "metrics",
                policy.retention.metrics.get(),
                config.retention.metrics.get(),
            ),
        ] {
            if old != new {
                changes.push(format!(
                    "retention.{name} {}s -> {}s",
                    old.as_secs(),
                    new.as_secs()
                ));
            }
        }
        policy.retention = config.retention.clone();

        changes
    }

    /// What each app is costing, from segment manifests alone — no file is opened.
    ///
    /// The question an operator has when the disk budget alarm fires is "which app is
    /// filling it", and until this existed there was no way to answer it: `app` is the
    /// whole tenancy model, `max_series_per_app` is enforced per app, and yet every
    /// number reported was a total.
    ///
    /// Rows and series are exact. **Bytes are apportioned by row share** and labelled
    /// as an estimate, because a segment holds every app that was writing when it
    /// sealed and Parquet does not record who owns which compressed page. An app
    /// writing unusually long lines will be under-counted, and there is no honest way
    /// to fix that short of one segment per app.
    #[must_use]
    pub fn app_usage(&self) -> Vec<AppUsage> {
        let mut apps: BTreeMap<String, AppUsage> = BTreeMap::new();

        for (signal, segments) in [
            (Signal::Logs, self.logs.segments()),
            (Signal::Traces, self.traces.segments()),
            (Signal::Metrics, self.metrics.segments()),
        ] {
            let _ = signal;
            for segment in &segments {
                let manifest = &segment.manifest;
                if manifest.stream_rows.len() != manifest.streams.len() {
                    // Written before per-stream rows existed. Counting its series is
                    // still right; attributing its bytes would be a guess.
                    for labels in &manifest.streams {
                        apps.entry(app_of(labels).to_owned()).or_default().series += 1;
                    }
                    continue;
                }
                for (labels, rows) in manifest.streams.iter().zip(&manifest.stream_rows) {
                    let usage = apps.entry(app_of(labels).to_owned()).or_default();
                    usage.series += 1;
                    usage.rows += rows;
                    if let Some(share) = manifest
                        .bytes
                        .saturating_mul(*rows)
                        .checked_div(manifest.rows)
                    {
                        usage.estimated_bytes += share;
                    }
                }
            }
        }

        let mut usage: Vec<AppUsage> = apps
            .into_iter()
            .map(|(app, mut counts)| {
                counts.app = app;
                counts
            })
            .collect();
        // Largest first: the answer to "who is filling my disk" belongs at the top.
        usage.sort_by(|a, b| {
            b.estimated_bytes
                .cmp(&a.estimated_bytes)
                .then(a.app.cmp(&b.app))
        });
        usage
    }

    /// Recount active series from the segments that still exist plus what is buffered.
    fn refresh_cardinality(&self) {
        let logs = self.logs.segments();
        let traces = self.traces.segments();
        let metrics = self.metrics.segments();
        let streams = logs
            .iter()
            .chain(traces.iter())
            .chain(metrics.iter())
            .flat_map(|segment| segment.manifest.streams.iter())
            .map(|labels| {
                (
                    labels
                        .get(telemetryd_core::APP_LABEL)
                        .unwrap_or(telemetryd_core::UNKNOWN_APP),
                    labels,
                )
            });
        self.cardinality.refresh(streams);
    }

    /// Series counted right now, and the caps they are counted against.
    #[must_use]
    pub fn cardinality_status(&self) -> (u64, u64, u64, u64) {
        let (max_series, max_per_app) = self.cardinality.limits();
        (
            self.cardinality.active_series(),
            self.cardinality.rejected_records(),
            max_series,
            max_per_app,
        )
    }

    #[must_use]
    pub fn disk_budget(&self) -> u64 {
        lock_read(&self.policy).disk_budget
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
            disk_budget_bytes: self.disk_budget(),
            disk_used_bytes: used,
            // Reported, not clamped: a ratio above 1.0 is exactly the signal an
            // operator needs to see, and hiding it would defeat the point.
            #[allow(clippy::cast_precision_loss)]
            disk_used_ratio: if self.disk_budget() == 0 {
                0.0
            } else {
                used as f64 / self.disk_budget() as f64
            },
            over_budget: used > self.disk_budget(),
            usage,
            logs: self.logs.status(),
            traces: self.traces.status(),
            metrics: self.metrics.status(),
            retention: lock(&self.reaper).clone(),
            series_active: self.cardinality.active_series(),
            series_by_app: self.cardinality.series_by_app(),
            series_rejected: self.cardinality.rejected_records(),
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
    /// Distinct series admitted right now, against `limits.max_series`.
    ///
    /// The disk figures were here from the first day and this was not, which had it
    /// exactly backwards: a full disk is visible from three other directions and reaps
    /// itself, while a full series limit is silent. An instance can sit at its cap
    /// refusing every new stream while reporting 0.3% of the budget used, and nothing
    /// else in this document says so.
    pub series_active: u64,
    /// Records refused because a cardinality cap was full. Monotonic.
    pub series_rejected: u64,
    /// The same count split by app, against `limits.max_series_per_app`.
    ///
    /// Deliberately not folded into `apps`, which reports what is *stored* — rows, bytes
    /// and the series found in sealed segments. This reports what is *admitted*, and the
    /// two differ by however long sealing takes. `apps` is empty for the first minutes of
    /// an instance's life; the limit is enforced from the first record.
    pub series_by_app: std::collections::BTreeMap<String, u64>,
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
