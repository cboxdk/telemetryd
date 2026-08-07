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

/// How often retention *looks*, which is not how often it works.
///
/// Segment bytes only grow when a segment is sealed, so the tick reads a counter and
/// does nothing unless that changed. Ticking often is therefore nearly free, and it is
/// what bounds how far usage can pass the disk budget: on a fixed 60-second tick a
/// fast writer overshot the ceiling by 65% and the peak grew run over run, because a
/// minute of writes at full rate is a great deal of data.
const RETENTION_TICK: Duration = Duration::from_secs(1);

/// Run retention at least this often even when nothing sealed.
///
/// Age-based expiry depends on the clock, not on writes: a store that stopped
/// receiving data still has records that fall out of the retention window, and they
/// should leave on schedule rather than when traffic happens to resume.
const RETENTION_FLOOR: Duration = Duration::from_secs(60);

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
                let mut seen_seals = store.sealed_count();
                let mut last_run = tokio::time::Instant::now();
                loop {
                    ticker.tick().await;

                    // A full pass walks the data directory. Doing that every second
                    // would be wasteful, and skipping it until a fixed minute has
                    // passed is how the budget got overshot — so the trigger is the
                    // thing that actually changes usage: a segment being sealed.
                    let seals = store.sealed_count();
                    let due = seals != seen_seals || last_run.elapsed() >= RETENTION_FLOOR;
                    if !due {
                        continue;
                    }
                    seen_seals = seals;
                    last_run = tokio::time::Instant::now();

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
