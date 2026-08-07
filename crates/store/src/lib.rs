//! telemetryd's storage engine.
//!
//! M0 provides the durable foundation the later milestones build on: the data
//! directory and its single-writer lock, the write-ahead log with crash recovery, and
//! the status surface that reports what is actually on disk. Segments, compaction and
//! the retention reaper arrive with M1 (see ADR-001).

pub mod datadir;
pub mod wal;

use std::collections::BTreeMap;
use std::sync::Mutex;

use telemetryd_core::config::StorageConfig;
use telemetryd_core::{Error, Result, Signal};

pub use datadir::{DataDir, DiskUsage};
pub use wal::{Truncation, TruncationReason, Wal, WalStats};

/// The storage engine handle. One per process — [`DataDir`] enforces that.
#[derive(Debug)]
pub struct Store {
    data_dir: DataDir,
    wals: BTreeMap<Signal, Mutex<Wal>>,
    disk_budget: u64,
    recovery: RecoveryReport,
}

impl Store {
    /// Open (or create) the store, replaying every write-ahead log.
    ///
    /// Replay happens before any log is opened for append, so a torn tail from a crash
    /// is repaired rather than being appended after.
    pub fn open(config: &StorageConfig) -> Result<Self> {
        let root = config.resolve_data_dir();
        let data_dir = DataDir::open(&root)?;
        data_dir.clean_tmp()?;

        let mut recovery = RecoveryReport::default();
        let mut wals = BTreeMap::new();

        for signal in Signal::ALL {
            let dir = data_dir.wal_dir(signal);

            // M0 counts what it recovers; M1 replaces the closure with the record
            // decoder that rebuilds the in-memory segment buffer.
            let replayed = wal::replay(&dir, |_payload| Ok(()))?;
            recovery.records += replayed.records;
            recovery.bytes += replayed.bytes;
            if let Some(truncation) = replayed.truncated {
                recovery.truncations.push(truncation);
            }

            let wal = Wal::open(
                &dir,
                config.wal_sync,
                config.wal_sync_interval,
                config.max_segment_bytes.as_u64(),
            )?;
            wals.insert(signal, Mutex::new(wal));
        }

        if recovery.records > 0 {
            tracing::info!(
                records = recovery.records,
                bytes = recovery.bytes,
                "replayed write-ahead log"
            );
        }

        Ok(Self {
            data_dir,
            wals,
            disk_budget: config.disk_budget.as_u64(),
            recovery,
        })
    }

    /// Append a raw record to a signal's write-ahead log.
    ///
    /// Blocking, and called from async handlers only via `spawn_blocking` until M1
    /// introduces the dedicated writer task with group commit.
    pub fn append(&self, signal: Signal, payload: &[u8]) -> Result<()> {
        self.with_wal(signal, |wal| wal.append(payload))
    }

    /// Flush and fsync every log. Called on graceful shutdown so a clean stop never
    /// loses the interval-sync window.
    pub fn sync_all(&self) -> Result<()> {
        for signal in Signal::ALL {
            self.with_wal(signal, Wal::sync)?;
        }
        Ok(())
    }

    pub fn data_dir(&self) -> &DataDir {
        &self.data_dir
    }

    pub fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// A point-in-time view for `/status`.
    pub fn snapshot(&self) -> Result<StoreStatus> {
        let usage = self.data_dir.usage()?;
        let mut wal_stats = BTreeMap::new();
        for signal in Signal::ALL {
            wal_stats.insert(signal, self.with_wal(signal, |wal| Ok(wal.stats()))?);
        }

        let used = usage.total();
        Ok(StoreStatus {
            data_dir: self.data_dir.root().display().to_string(),
            disk_budget_bytes: self.disk_budget,
            disk_used_bytes: used,
            // Reported, not clamped: a ratio above 1.0 is exactly the signal an
            // operator needs to see, and hiding it would defeat the point.
            //
            // The f64 conversion loses precision above 2^53 bytes (8 PiB). This is a
            // single-node store with a default 10 GiB budget; a ratio displayed to one
            // decimal place does not care.
            #[allow(clippy::cast_precision_loss)]
            disk_used_ratio: if self.disk_budget == 0 {
                0.0
            } else {
                used as f64 / self.disk_budget as f64
            },
            over_budget: used > self.disk_budget,
            usage,
            wal: wal_stats,
            recovered_records: self.recovery.records,
            wal_truncations: self.recovery.truncations.clone(),
        })
    }

    fn with_wal<T>(&self, signal: Signal, f: impl FnOnce(&mut Wal) -> Result<T>) -> Result<T> {
        let cell = self
            .wals
            .get(&signal)
            .ok_or_else(|| Error::BadRequest(format!("no write-ahead log for signal {signal}")))?;
        // A poisoned lock means a previous writer panicked mid-append. The WAL is
        // still structurally sound — a torn frame is exactly what replay repairs — so
        // recovering is safer than propagating a panic to every later request.
        let mut wal = cell.lock().unwrap_or_else(|poisoned| {
            tracing::error!(%signal, "write-ahead log mutex was poisoned by a panicking writer");
            poisoned.into_inner()
        });
        f(&mut wal)
    }
}

#[derive(Debug, Default, Clone)]
pub struct RecoveryReport {
    pub records: u64,
    pub bytes: u64,
    pub truncations: Vec<Truncation>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StoreStatus {
    pub data_dir: String,
    pub disk_budget_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_used_ratio: f64,
    pub over_budget: bool,
    pub usage: DiskUsage,
    pub wal: BTreeMap<Signal, WalStats>,
    pub recovered_records: u64,
    /// Non-empty when a crash cost us records. Kept in `/status` for the process
    /// lifetime rather than only logged at startup — "degrade loudly".
    pub wal_truncations: Vec<Truncation>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bytesize::ByteSize;

    fn config(dir: &std::path::Path) -> StorageConfig {
        StorageConfig {
            data_dir: Some(dir.to_path_buf()),
            ..StorageConfig::default()
        }
    }

    #[test]
    fn opens_appends_and_recovers_across_restarts() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config(&tmp.path().join("data"));

        let store = Store::open(&config).unwrap();
        assert_eq!(store.recovery().records, 0);
        for i in 0..25u32 {
            store
                .append(Signal::Logs, format!("log-{i}").as_bytes())
                .unwrap();
        }
        store.append(Signal::Traces, b"span").unwrap();
        store.sync_all().unwrap();
        drop(store);

        let reopened = Store::open(&config).unwrap();
        assert_eq!(reopened.recovery().records, 26);
        assert!(reopened.recovery().truncations.is_empty());
    }

    #[test]
    fn status_reports_budget_and_usage() {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = config(&tmp.path().join("data"));
        config.disk_budget = ByteSize::kib(1);

        let store = Store::open(&config).unwrap();
        let before = store.snapshot().unwrap();
        assert!(!before.over_budget);
        assert_eq!(before.wal.len(), 4);

        for _ in 0..200 {
            store.append(Signal::Logs, &[0u8; 64]).unwrap();
        }
        store.sync_all().unwrap();

        let after = store.snapshot().unwrap();
        assert!(after.disk_used_bytes > before.disk_used_bytes);
        // Over-budget must be visible rather than clamped away.
        assert!(after.over_budget);
        assert!(after.disk_used_ratio > 1.0);
    }

    #[test]
    fn a_crash_torn_tail_surfaces_in_status_after_restart() {
        use std::io::Write;

        let tmp = tempfile::tempdir().unwrap();
        let config = config(&tmp.path().join("data"));

        let store = Store::open(&config).unwrap();
        store.append(Signal::Logs, b"durable").unwrap();
        store.sync_all().unwrap();
        let wal_path = store.data_dir().wal_dir(Signal::Logs).join("00000001.wal");
        drop(store);

        // Half a frame, as a `kill -9` mid-append would leave.
        let mut file = std::fs::File::options()
            .append(true)
            .open(&wal_path)
            .unwrap();
        file.write_all(&[32, 0, 0, 0, 9, 9, 9, 9]).unwrap();
        file.write_all(b"partial").unwrap();
        drop(file);

        let reopened = Store::open(&config).unwrap();
        assert_eq!(reopened.recovery().records, 1);

        let status = reopened.snapshot().unwrap();
        assert_eq!(status.wal_truncations.len(), 1);
        assert_eq!(
            status.wal_truncations[0].reason,
            TruncationReason::PartialFrame
        );

        // And the store is fully usable afterwards.
        reopened.append(Signal::Logs, b"after-recovery").unwrap();
        reopened.sync_all().unwrap();
    }

    #[test]
    fn a_second_store_on_the_same_directory_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let config = config(&tmp.path().join("data"));

        let _first = Store::open(&config).unwrap();
        let err = Store::open(&config).unwrap_err();
        assert!(matches!(err, Error::DataDirLocked { .. }), "got {err:?}");
    }
}
