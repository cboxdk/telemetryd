//! The limit on how many distinct series a store will hold.
//!
//! Unbounded cardinality is how a log store dies: one label carrying a request id or a
//! pod name turns a handful of streams into millions, and every one of them costs
//! memory in the stream dictionary, a column in every segment, and a matcher
//! evaluation on every query. By the time it is visible in latency it is usually too
//! late to query your way out of it.
//!
//! `limits.max_series` and `limits.max_series_per_app` existed as configuration,
//! validation and documentation for some time before they existed as behaviour: 400
//! distinct series were accepted against a configured cap of 50, with nothing rejected
//! and nothing logged.
//!
//! # The budget counts what is live, not what has been seen
//!
//! It used to count history. Every series in every sealed segment held a slot until
//! retention deleted the segment, which for metrics is thirty days — so a series that
//! nothing had written to since last month was still holding budget against the series
//! arriving now. Renaming a set of metrics therefore cost double for a month: the old
//! names kept their slots long after the last sample, and on one deployment 93 dead
//! names held half the budget while every new log stream was refused.
//!
//! That was a category error. The cap exists to bound what the process is *carrying* —
//! stream dictionaries, buffers, per-series work on every query. Data at rest in sealed
//! segments is bounded by `storage.disk_budget` and by retention, which are different
//! controls with their own reaper. Counting the same series twice, in two budgets, made
//! the tighter one fire for a reason that had nothing to do with load.
//!
//! So each series carries the time it was last written, and one that has been silent
//! for `limits.series_idle_after` gives its slot back. A rename now heals in an hour
//! rather than in a month, and `series_active` means what the name says.
//!
//! # Why a full budget still refuses, rather than evicting the oldest
//!
//! Reclaiming idle series is eviction where eviction helps. When the budget is full of
//! genuinely *active* series, evicting the least recently used one to admit a new one is
//! worse than refusing, for three reasons:
//!
//! - **It thrashes.** 120,000 active series against a 100,000 cap would evict and
//!   re-admit continuously, and every series would end up with holes. Complete data for
//!   a bounded set is worth more to someone debugging than 83% of everything with gaps
//!   in unpredictable places.
//! - **It frees nothing.** The evicted series' stream is still in the writer's
//!   dictionary and its records are still buffered or sealed. Re-admitting it a second
//!   later allocates again. The memory the cap is protecting does not come back.
//! - **It makes dashboards flicker**, because the admitted set churns rather than
//!   settling.
//!
//! A refusal here is honest: the budget is full of things that are actually running, and
//! the answer is a bigger budget or fewer label combinations. Two properties make that
//! survivable, and they matter more than the exact accounting:
//!
//! - **Only *new* series are refused.** An app already over the limit keeps working;
//!   what stops is its ability to invent more. Rejecting existing series would turn a
//!   labelling mistake into an outage of the telemetry you still have.
//! - **Refusals are loud.** They are counted, reported to the producer in the OTLP
//!   response, exposed as a metric, logged, and shown against the limit in
//!   `telemetryd status`.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use telemetryd_core::Labels;
use telemetryd_core::config::WhenSeriesFull;

/// How often a full budget will sweep for idle series, at most.
///
/// The sweep walks every counted series under the write lock, so it is not something to
/// do per refused record — a budget that is full stays full, and would otherwise sweep on
/// every single one. Ten seconds is far below the shortest sensible idle window and far
/// above the cost of the walk.
const RECLAIM_MIN_INTERVAL_SECS: u64 = 10;

/// What happened to a record offered to the limiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// A series already counted, or room to count a new one.
    Accepted,
    /// Would exceed `max_series_per_app`.
    AppLimit,
    /// Would exceed `max_series` across all apps.
    GlobalLimit,
}

impl Admission {
    #[must_use]
    pub fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted)
    }

    /// Wording for the OTLP `partialSuccess` message and the log line.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::AppLimit => "limits.max_series_per_app",
            Self::GlobalLimit => "limits.max_series",
        }
    }
}

/// The outcome of offering a batch to the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Admitted {
    /// Records actually written.
    pub stored: usize,
    /// Records refused because their series would have exceeded a cap.
    pub rejected: usize,
    /// Which limit was hit first, for the message the producer sees.
    pub reason: Option<&'static str>,
}

#[derive(Debug, Default)]
struct State {
    /// Every counted series, and the second it was last written.
    ///
    /// The timestamp is an atomic so that touching a series the store already knows
    /// about — overwhelmingly the common case — needs only a read lock. Making it a
    /// plain `u64` would put every ingested record behind the write lock.
    series: HashMap<u64, AtomicU64>,
    /// The same fingerprints split by app, so one noisy app cannot consume the global
    /// budget.
    per_app: HashMap<String, HashSet<u64>>,
}

/// Tracks distinct series and decides what may be added.
#[derive(Debug)]
pub struct Cardinality {
    max_series: u64,
    max_series_per_app: u64,
    /// Silence after which a series gives its slot back. `0` disables reclaiming.
    idle_after_secs: u64,
    when_full: WhenSeriesFull,
    state: RwLock<State>,
    rejected: AtomicU64,
    reclaimed: AtomicU64,
    evicted: AtomicU64,
    last_reclaim_secs: AtomicU64,
}

impl Cardinality {
    #[must_use]
    pub fn new(
        max_series: u64,
        max_series_per_app: u64,
        idle_after: std::time::Duration,
        when_full: WhenSeriesFull,
    ) -> Self {
        Self {
            max_series,
            max_series_per_app,
            idle_after_secs: idle_after.as_secs(),
            when_full,
            state: RwLock::new(State::default()),
            rejected: AtomicU64::new(0),
            reclaimed: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
            last_reclaim_secs: AtomicU64::new(0),
        }
    }

    /// Decide whether a record's series may be stored.
    ///
    /// Takes a write lock only when the series is new, so the common case — a stream
    /// that already exists — is a read lock, a hash lookup and an atomic store.
    pub fn admit(&self, app: &str, labels: &Labels) -> Admission {
        self.admit_at(app, labels, now_secs())
    }

    /// `admit`, with the clock passed in. Tests drive the idle window through this.
    pub fn admit_at(&self, app: &str, labels: &Labels, now: u64) -> Admission {
        let fingerprint = labels.fingerprint();

        {
            let state = read(&self.state);
            if let Some(seen) = state.series.get(&fingerprint) {
                seen.store(now, Ordering::Relaxed);
                return Admission::Accepted;
            }
        }

        let mut state = write(&self.state);
        // Re-check: another thread may have added it between the two locks.
        if let Some(seen) = state.series.get(&fingerprint) {
            seen.store(now, Ordering::Relaxed);
            return Admission::Accepted;
        }

        if self.global_full(&state) || self.app_full(&state, app) {
            self.sweep_if_due(&mut state, now);
        }
        if let Some(refusal) = self.make_room(&mut state, app, now) {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return refusal;
        }

        state
            .per_app
            .entry(app.to_owned())
            .or_default()
            .insert(fingerprint);
        state.series.insert(fingerprint, AtomicU64::new(now));
        Admission::Accepted
    }

    /// Free a slot for one new series, or name the limit that stopped us.
    fn make_room(&self, state: &mut State, app: &str, now: u64) -> Option<Admission> {
        for _ in 0..2 {
            let full = if self.global_full(state) {
                Admission::GlobalLimit
            } else if self.app_full(state, app) {
                Admission::AppLimit
            } else {
                return None;
            };
            // Only the app's own series may be evicted for an app that is at *its* cap;
            // taking another app's slot is the starvation the per-app limit exists to
            // prevent.
            let scope = (full == Admission::AppLimit).then_some(app);
            if self.when_full != WhenSeriesFull::EvictOldest
                || !self.evict_oldest(state, scope, now)
            {
                return Some(full);
            }
        }
        None
    }

    /// Drop the least recently written series, so a new one can take its place.
    ///
    /// Returns false when there is nothing to evict, which is what turns this back into a
    /// refusal rather than an endless loop.
    fn evict_oldest(&self, state: &mut State, app: Option<&str>, now: u64) -> bool {
        let candidates: Option<&HashSet<u64>> = match app {
            // An app at its cap with no recorded series cannot happen, but a missing
            // entry must not silently widen the search to every other app's slots.
            Some(app) => match state.per_app.get(app) {
                Some(set) => Some(set),
                None => return false,
            },
            None => None,
        };
        let oldest = match candidates {
            Some(set) => set
                .iter()
                .filter_map(|fp| Some((*fp, state.series.get(fp)?.load(Ordering::Relaxed))))
                .min_by_key(|(_, seen)| *seen),
            None => state
                .series
                .iter()
                .map(|(fp, seen)| (*fp, seen.load(Ordering::Relaxed)))
                .min_by_key(|(_, seen)| *seen),
        };
        let Some((fingerprint, seen)) = oldest else {
            return false;
        };
        // Never evict something written this same second: that is not a ring buffer
        // making room, it is two series trading one slot back and forth within a single
        // batch, and neither of them ends up with usable data.
        if seen >= now {
            return false;
        }
        forget(state, &std::iter::once(fingerprint).collect());
        self.evicted.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn global_full(&self, state: &State) -> bool {
        self.max_series != 0 && state.series.len() as u64 >= self.max_series
    }

    fn app_full(&self, state: &State, app: &str) -> bool {
        self.max_series_per_app != 0
            && state.per_app.get(app).map_or(0, HashSet::len) as u64 >= self.max_series_per_app
    }

    fn sweep_if_due(&self, state: &mut State, now: u64) {
        let last = self.last_reclaim_secs.load(Ordering::Relaxed);
        if now.saturating_sub(last) < RECLAIM_MIN_INTERVAL_SECS {
            return;
        }
        self.last_reclaim_secs.store(now, Ordering::Relaxed);
        self.reclaim_locked(state, now);
    }

    /// Give back the slots held by series nothing has written to for a while.
    ///
    /// Called on the ingest path when the budget is full, and periodically by
    /// maintenance so that `series_active` describes the present even on an instance
    /// nobody is pushing against its limit.
    pub fn reclaim_idle(&self) -> usize {
        self.reclaim_idle_at(now_secs())
    }

    pub fn reclaim_idle_at(&self, now: u64) -> usize {
        if self.idle_after_secs == 0 {
            return 0;
        }
        let mut state = write(&self.state);
        self.reclaim_locked(&mut state, now)
    }

    fn reclaim_locked(&self, state: &mut State, now: u64) -> usize {
        if self.idle_after_secs == 0 {
            return 0;
        }
        let cutoff = now.saturating_sub(self.idle_after_secs);
        let idle: HashSet<u64> = state
            .series
            .iter()
            .filter(|(_, seen)| seen.load(Ordering::Relaxed) <= cutoff)
            .map(|(fingerprint, _)| *fingerprint)
            .collect();
        if idle.is_empty() {
            return 0;
        }
        forget(state, &idle);
        self.reclaimed
            .fetch_add(idle.len() as u64, Ordering::Relaxed);
        idle.len()
    }

    #[must_use]
    pub fn active_series(&self) -> u64 {
        read(&self.state).series.len() as u64
    }

    /// Live series per app, which is not the same thing as the per-app figures derived
    /// from sealed segments.
    ///
    /// This is what the per-app *limit* is enforced against, so it is what a report on
    /// that limit has to read. The segment-derived numbers lag by however long a segment
    /// takes to seal and are empty on a fresh instance — which is precisely the window in
    /// which someone is finding out that their app has hit a cap.
    #[must_use]
    pub fn series_by_app(&self) -> std::collections::BTreeMap<String, u64> {
        read(&self.state)
            .per_app
            .iter()
            .map(|(app, series)| (app.clone(), series.len() as u64))
            .collect()
    }

    #[must_use]
    pub fn rejected_records(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Series that gave their slot back after falling silent. Monotonic.
    #[must_use]
    pub fn reclaimed_series(&self) -> u64 {
        self.reclaimed.load(Ordering::Relaxed)
    }

    /// Series dropped to make room for a new one under `WhenFull::EvictOldest`.
    ///
    /// Separate from `reclaimed_series` because they mean opposite things: reclaiming is
    /// the budget correcting itself and costs nothing, while evicting is the budget being
    /// genuinely too small and costs whatever the evicted series would have recorded.
    /// A number that climbs here is the signal to raise the limit.
    #[must_use]
    pub fn evicted_series(&self) -> u64 {
        self.evicted.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn limits(&self) -> (u64, u64) {
        (self.max_series, self.max_series_per_app)
    }
}

/// Remove a set of series from both indexes, dropping apps left with nothing.
fn forget(state: &mut State, gone: &HashSet<u64>) {
    state
        .series
        .retain(|fingerprint, _| !gone.contains(fingerprint));
    state.per_app.retain(|_, series| {
        series.retain(|fingerprint| !gone.contains(fingerprint));
        !series.is_empty()
    });
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

fn read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const HOUR: std::time::Duration = std::time::Duration::from_secs(3600);

    fn limiter(max: u64, per_app: u64) -> Cardinality {
        Cardinality::new(max, per_app, HOUR, WhenSeriesFull::Refuse)
    }

    fn evicting(max: u64, per_app: u64) -> Cardinality {
        Cardinality::new(max, per_app, HOUR, WhenSeriesFull::EvictOldest)
    }

    fn series(app: &str, n: usize) -> Labels {
        let mut labels = Labels::new();
        labels.insert("app", app);
        labels.insert("pod", format!("pod-{n}"));
        labels
    }

    #[test]
    fn a_series_already_counted_is_always_admitted() {
        let limiter = limiter(1, 1);
        let labels = series("checkout", 0);

        assert!(limiter.admit("checkout", &labels).is_accepted());
        // Same series, and the cap is one — this must still pass, or a busy app would
        // stop working the moment it reached its limit.
        for _ in 0..10 {
            assert!(limiter.admit("checkout", &labels).is_accepted());
        }
        assert_eq!(limiter.active_series(), 1);
    }

    #[test]
    fn a_new_series_past_the_global_cap_is_refused() {
        let limiter = limiter(3, 100);
        for i in 0..3 {
            assert!(
                limiter
                    .admit("checkout", &series("checkout", i))
                    .is_accepted()
            );
        }
        assert_eq!(
            limiter.admit("checkout", &series("checkout", 99)),
            Admission::GlobalLimit
        );
        assert_eq!(limiter.active_series(), 3);
        assert_eq!(limiter.rejected_records(), 1);
    }

    #[test]
    fn one_app_cannot_consume_the_whole_budget() {
        let limiter = limiter(100, 2);
        for i in 0..2 {
            assert!(limiter.admit("noisy", &series("noisy", i)).is_accepted());
        }
        assert_eq!(
            limiter.admit("noisy", &series("noisy", 9)),
            Admission::AppLimit
        );

        // The quiet app is unaffected: that is the whole point of the per-app cap.
        assert!(limiter.admit("quiet", &series("quiet", 0)).is_accepted());
    }

    /// Zero still means unlimited *here*. Configuration no longer lets one through —
    /// `resolved_max_series` turns a configured `0` into a derived number — but the
    /// tracker keeps the meaning so a caller that genuinely wants no ceiling can say so.
    #[test]
    fn zero_means_unlimited() {
        let limiter = limiter(0, 0);
        for i in 0..1000 {
            assert!(
                limiter
                    .admit("checkout", &series("checkout", i))
                    .is_accepted()
            );
        }
        assert_eq!(limiter.rejected_records(), 0);
    }

    #[test]
    fn a_series_that_falls_silent_gives_its_slot_back() {
        let limiter = limiter(2, 2);
        // Two series at t=0, then only the first keeps being written.
        assert!(
            limiter
                .admit_at("checkout", &series("checkout", 0), 0)
                .is_accepted()
        );
        assert!(
            limiter
                .admit_at("checkout", &series("checkout", 1), 0)
                .is_accepted()
        );
        assert_eq!(
            limiter.admit_at("checkout", &series("checkout", 2), 0),
            Admission::GlobalLimit
        );

        // Two hours later series 1 has been silent the whole time. Its slot is the one
        // the newcomer should get — this is the case that used to hold budget for a
        // month because a sealed segment still mentioned it.
        let later = 2 * 3600;
        assert!(
            limiter
                .admit_at("checkout", &series("checkout", 0), later)
                .is_accepted()
        );
        assert!(
            limiter
                .admit_at("checkout", &series("checkout", 2), later)
                .is_accepted()
        );
        assert_eq!(limiter.active_series(), 2);
        assert_eq!(limiter.reclaimed_series(), 1);
        assert_eq!(limiter.evicted_series(), 0);
    }

    #[test]
    fn a_budget_full_of_busy_series_refuses_rather_than_evicting() {
        let limiter = limiter(2, 2);
        for i in 0..2 {
            assert!(
                limiter
                    .admit_at("checkout", &series("checkout", i), 0)
                    .is_accepted()
            );
        }
        // Everything is still being written, so nothing is idle and nothing may go.
        // Refusing keeps the two admitted series complete; the alternative churns all
        // three and leaves each with holes.
        for step in 1..5 {
            assert_eq!(
                limiter.admit_at("checkout", &series("checkout", 9), step),
                Admission::GlobalLimit
            );
        }
        assert_eq!(limiter.active_series(), 2);
        assert_eq!(limiter.evicted_series(), 0);
        assert_eq!(limiter.rejected_records(), 4);
    }

    #[test]
    fn evict_oldest_makes_room_by_dropping_the_least_recently_written() {
        let limiter = evicting(2, 2);
        assert!(
            limiter
                .admit_at("checkout", &series("checkout", 0), 10)
                .is_accepted()
        );
        assert!(
            limiter
                .admit_at("checkout", &series("checkout", 1), 20)
                .is_accepted()
        );

        // Series 0 is the oldest write, so it is the one that gives way.
        assert!(
            limiter
                .admit_at("checkout", &series("checkout", 2), 30)
                .is_accepted()
        );
        assert_eq!(limiter.active_series(), 2);
        assert_eq!(limiter.evicted_series(), 1);

        // And it really was series 0: re-offering it counts as new.
        assert!(
            limiter
                .admit_at("checkout", &series("checkout", 0), 40)
                .is_accepted()
        );
        assert_eq!(limiter.evicted_series(), 2);
    }

    /// Eviction must not trade slots inside one batch, where every series carries the
    /// same timestamp — that admits nobody and drops somebody on every record.
    #[test]
    fn evict_oldest_will_not_churn_within_a_single_instant() {
        let limiter = evicting(2, 2);
        for i in 0..2 {
            assert!(
                limiter
                    .admit_at("checkout", &series("checkout", i), 100)
                    .is_accepted()
            );
        }
        assert_eq!(
            limiter.admit_at("checkout", &series("checkout", 5), 100),
            Admission::GlobalLimit
        );
        assert_eq!(limiter.evicted_series(), 0);
    }

    /// An app at its own cap may only take back its own slots. Letting it evict across
    /// apps would undo the one thing the per-app limit is for.
    #[test]
    fn evicting_for_a_full_app_never_takes_another_apps_slot() {
        let limiter = evicting(100, 1);
        assert!(
            limiter
                .admit_at("quiet", &series("quiet", 0), 10)
                .is_accepted()
        );
        assert!(
            limiter
                .admit_at("noisy", &series("noisy", 0), 20)
                .is_accepted()
        );

        // `quiet` holds the globally oldest series, and `noisy` is the one that is full.
        assert!(
            limiter
                .admit_at("noisy", &series("noisy", 1), 30)
                .is_accepted()
        );

        let by_app = limiter.series_by_app();
        assert_eq!(
            by_app.get("quiet"),
            Some(&1),
            "the other app kept its series"
        );
        assert_eq!(by_app.get("noisy"), Some(&1));
    }

    #[test]
    fn an_app_that_loses_every_series_stops_being_counted() {
        let limiter = limiter(10, 10);
        assert!(
            limiter
                .admit_at("gone", &series("gone", 0), 0)
                .is_accepted()
        );
        assert_eq!(limiter.reclaim_idle_at(2 * 3600), 1);
        assert!(limiter.series_by_app().is_empty());
        assert_eq!(limiter.active_series(), 0);
    }

    #[test]
    fn a_zero_idle_window_turns_reclaiming_off() {
        let limiter = Cardinality::new(1, 1, std::time::Duration::ZERO, WhenSeriesFull::Refuse);
        assert!(
            limiter
                .admit_at("checkout", &series("checkout", 0), 0)
                .is_accepted()
        );
        assert_eq!(limiter.reclaim_idle_at(10 * 365 * 24 * 3600), 0);
        assert_eq!(limiter.active_series(), 1);
    }
}
