//! Background maintenance: sealing, syncing and retention.
//!
//! All three are timers rather than reactions to ingest, because all three must happen
//! on an *idle* instance too. A store that received its last record five minutes before
//! a power cut should still have fsynced it; a store nobody is writing to should still
//! expire yesterday's logs.

use std::sync::Arc;
use std::time::Duration;

use telemetryd_store::Store;
use tokio::task::JoinHandle;

/// How often to check whether the open buffer's window has elapsed.
///
/// Independent of `segment_duration`: the check is cheap, and a coarse tick would mean
/// a segment configured to seal every minute actually sealing every tick instead.
const SEAL_TICK: Duration = Duration::from_secs(5);

/// How often retention runs. Segment-granular deletion means running it more often
/// than segments are produced achieves nothing.
const RETENTION_TICK: Duration = Duration::from_secs(60);

/// Handles for the background tasks, so shutdown can stop them deterministically.
#[derive(Debug)]
pub struct Maintenance {
    tasks: Vec<JoinHandle<()>>,
}

impl Maintenance {
    /// Start the maintenance tasks against `store`.
    pub fn start(store: &Arc<Store>, wal_sync_interval: Duration) -> Self {
        let mut tasks = Vec::new();

        // Sync: honour `wal_sync = "interval"` even when no request is in flight.
        // Without this, "at most 100ms of loss" would silently become "at most 100ms
        // after the last write, then unbounded".
        {
            let store = Arc::clone(store);
            let period = wal_sync_interval.max(Duration::from_millis(10));
            tasks.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(period);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    let store = Arc::clone(&store);
                    let result = tokio::task::spawn_blocking(move || store.maybe_sync()).await;
                    if let Ok(Err(e)) = result {
                        tracing::warn!(error = %e, "background write-ahead log sync failed");
                    }
                }
            }));
        }

        // Seal: close the open buffer once its window elapses.
        {
            let store = Arc::clone(store);
            tasks.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(SEAL_TICK);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    let store = Arc::clone(&store);
                    let result = tokio::task::spawn_blocking(move || store.maybe_seal()).await;
                    if let Ok(Err(e)) = result {
                        // Sealing failing is serious but not fatal: the records are
                        // still in the WAL and still queryable from the buffer.
                        tracing::error!(error = %e, "sealing a segment failed; records remain buffered");
                    }
                }
            }));
        }

        // Retention: expire by age, then enforce the disk budget.
        {
            let store = Arc::clone(store);
            tasks.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(RETENTION_TICK);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    let store = Arc::clone(&store);
                    let result = tokio::task::spawn_blocking(move || store.run_retention()).await;
                    match result {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => tracing::error!(error = %e, "retention pass failed"),
                        Err(e) => tracing::error!(error = %e, "retention task panicked"),
                    }
                }
            }));
        }

        Self { tasks }
    }

    /// Stop every task. Called before the final flush so nothing is mid-seal while the
    /// process is trying to shut down cleanly.
    pub fn stop(self) {
        for task in self.tasks {
            task.abort();
        }
    }
}
