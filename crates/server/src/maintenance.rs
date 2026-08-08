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
    /// Expiry by age, the disk budget, and whatever a relay still owes upstream.
    fn spawn_retention(
        tasks: &mut Vec<tokio::task::JoinHandle<()>>,
        store: &Arc<Store>,
        relay: Option<Arc<crate::relay::Relay>>,
    ) {
        let store = Arc::clone(store);
        let reaper_relay = relay;
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
                // What the relay has not forwarded is held back from the reaper.
                // Deleting it would lose telemetry that never reached anywhere,
                // and nothing downstream would ever know it existed.
                let protected = reaper_relay.clone();
                let result = tokio::task::spawn_blocking(move || match protected {
                    Some(relay) => {
                        let mut ids = std::collections::BTreeSet::new();
                        for signal in [
                            telemetryd_core::Signal::Logs,
                            telemetryd_core::Signal::Traces,
                            telemetryd_core::Signal::Metrics,
                        ] {
                            ids.extend(relay.undelivered(&store, signal));
                        }
                        store.run_retention_protecting(telemetryd_store::retention::Undelivered {
                            ids: Some(&ids),
                            drop_when_full: relay.drops_when_full(),
                        })
                    }
                    None => store.run_retention(),
                })
                .await;
                match result {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => tracing::error!(error = %e, "retention pass failed"),
                    Err(e) => tracing::error!(error = %e, "retention task panicked"),
                }
            }
        }));
    }

    /// The forwarding loop, split out so `start_with` stays readable.
    fn spawn_relay(
        tasks: &mut Vec<tokio::task::JoinHandle<()>>,
        store: &Arc<Store>,
        relay: Option<Arc<crate::relay::Relay>>,
        relay_interval: Duration,
    ) {
        let Some(relay) = relay else {
            return;
        };

        let store = Arc::clone(store);
        let interval = if relay_interval.is_zero() {
            Duration::from_secs(30)
        } else {
            relay_interval
        };
        tasks.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let (relay, store) = (Arc::clone(&relay), Arc::clone(&store));
                // Bounded per pass so one enormous backlog cannot hold the
                // blocking pool for minutes; the next tick continues from the
                // cursor, which is exactly what the cursor is for.
                let result = tokio::task::spawn_blocking(move || relay.ship(&store, 32)).await;
                match result {
                    Ok(Ok(0)) => {}
                    Ok(Ok(shipped)) => {
                        tracing::debug!(segments = shipped, "forwarded upstream");
                    }
                    Ok(Err(e)) => tracing::warn!(error = %e, "relay pass failed"),
                    Err(e) => tracing::error!(error = %e, "relay task panicked"),
                }
            }
        }));
    }

    /// Start the maintenance tasks against `store`.
    pub fn start(store: &Arc<Store>, wal_sync_interval: Duration) -> Self {
        Self::start_with(store, wal_sync_interval, None, None, Duration::ZERO)
    }

    /// As [`Self::start`], plus refreshing the Cbox ID key set when one is configured.
    pub fn start_with(
        store: &Arc<Store>,
        wal_sync_interval: Duration,
        oidc: Option<Arc<crate::oidc::Oidc>>,
        relay: Option<Arc<crate::relay::Relay>>,
        relay_interval: Duration,
    ) -> Self {
        let mut tasks = Vec::new();

        // Keys, on a timer. A rotation is also picked up on demand when a token
        // arrives with an unseen key id, so this is the ceiling on staleness rather
        // than the mechanism — and it is what recovers after the issuer has been down.
        if let Some(oidc) = oidc.filter(|oidc| oidc.is_enabled()) {
            tasks.push(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(60));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    if !oidc.is_stale() {
                        continue;
                    }
                    let oidc = Arc::clone(&oidc);
                    let result = tokio::task::spawn_blocking(move || oidc.refresh()).await;
                    match result {
                        Ok(Ok(count)) => {
                            tracing::debug!(keys = count, "refreshed the Cbox ID key set");
                        }
                        // Keeps serving the keys it already has: a provider that goes
                        // down must not take authentication with it.
                        Ok(Err(error)) => tracing::warn!(
                            %error,
                            "could not refresh the Cbox ID key set; continuing with the cached one"
                        ),
                        Err(e) => tracing::error!(error = %e, "key refresh task panicked"),
                    }
                }
            }));
        }

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

        Self::spawn_retention(&mut tasks, store, relay.clone());

        Self::spawn_relay(&mut tasks, store, relay, relay_interval);

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
