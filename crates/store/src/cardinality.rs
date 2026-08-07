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
//! Two properties matter more than the exact accounting:
//!
//! - **Only *new* series are refused.** An app already over the limit keeps working;
//!   what stops is its ability to invent more. Rejecting existing series would turn a
//!   labelling mistake into an outage of the telemetry you still have.
//! - **Refusals are loud.** They are counted, reported to the producer in the OTLP
//!   response, exposed as a metric and logged. A cap that silently drops data is
//!   indistinguishable from a bug in the producer, and someone will spend a day on it.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use telemetryd_core::Labels;

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
    /// Fingerprints of every series currently counted.
    global: HashSet<u64>,
    /// The same, split by app, so one noisy app cannot consume the global budget.
    per_app: HashMap<String, HashSet<u64>>,
}

/// Tracks distinct series and decides what may be added.
#[derive(Debug)]
pub struct Cardinality {
    max_series: u64,
    max_series_per_app: u64,
    state: RwLock<State>,
    rejected: std::sync::atomic::AtomicU64,
}

impl Cardinality {
    #[must_use]
    pub fn new(max_series: u64, max_series_per_app: u64) -> Self {
        Self {
            max_series,
            max_series_per_app,
            state: RwLock::new(State::default()),
            rejected: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Decide whether a record's series may be stored.
    ///
    /// Takes a write lock only when the series is new, so the common case — a stream
    /// that already exists — is a read lock and a hash lookup.
    pub fn admit(&self, app: &str, labels: &Labels) -> Admission {
        let fingerprint = labels.fingerprint();

        {
            let state = read(&self.state);
            if state.global.contains(&fingerprint) {
                return Admission::Accepted;
            }
        }

        let mut state = write(&self.state);
        // Re-check: another thread may have added it between the two locks.
        if state.global.contains(&fingerprint) {
            return Admission::Accepted;
        }

        // Unlimited when set to zero, which is how every other limit here spells it.
        if self.max_series != 0 && state.global.len() as u64 >= self.max_series {
            self.reject();
            return Admission::GlobalLimit;
        }
        let for_app = state.per_app.entry(app.to_owned()).or_default();
        if self.max_series_per_app != 0 && for_app.len() as u64 >= self.max_series_per_app {
            self.reject();
            return Admission::AppLimit;
        }

        for_app.insert(fingerprint);
        state.global.insert(fingerprint);
        Admission::Accepted
    }

    fn reject(&self) {
        self.rejected
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Replace the counted set with the series that actually exist.
    ///
    /// Called after retention, because a cap that only ever counts upward would refuse
    /// new series long after the old ones expired — the store would still be under its
    /// limit while the limiter believed otherwise. Rebuilding from what is on disk is
    /// what makes the cap track reality rather than history.
    pub fn refresh<'a>(&self, active: impl Iterator<Item = (&'a str, &'a Labels)>) {
        let mut rebuilt = State::default();
        for (app, labels) in active {
            let fingerprint = labels.fingerprint();
            rebuilt.global.insert(fingerprint);
            rebuilt
                .per_app
                .entry(app.to_owned())
                .or_default()
                .insert(fingerprint);
        }
        *write(&self.state) = rebuilt;
    }

    #[must_use]
    pub fn active_series(&self) -> u64 {
        read(&self.state).global.len() as u64
    }

    #[must_use]
    pub fn rejected_records(&self) -> u64 {
        self.rejected.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[must_use]
    pub fn limits(&self) -> (u64, u64) {
        (self.max_series, self.max_series_per_app)
    }
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

    fn series(app: &str, n: usize) -> Labels {
        let mut labels = Labels::new();
        labels.insert("app", app);
        labels.insert("pod", format!("pod-{n}"));
        labels
    }

    #[test]
    fn a_series_already_counted_is_always_admitted() {
        let limiter = Cardinality::new(1, 1);
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
        let limiter = Cardinality::new(3, 100);
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
        let limiter = Cardinality::new(100, 2);
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

    #[test]
    fn zero_means_unlimited() {
        let limiter = Cardinality::new(0, 0);
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
    fn refreshing_frees_the_budget_that_expired_data_was_holding() {
        let limiter = Cardinality::new(2, 2);
        assert!(
            limiter
                .admit("checkout", &series("checkout", 0))
                .is_accepted()
        );
        assert!(
            limiter
                .admit("checkout", &series("checkout", 1))
                .is_accepted()
        );
        assert_eq!(
            limiter.admit("checkout", &series("checkout", 2)),
            Admission::GlobalLimit
        );

        // Retention removed one of them; the budget it held has to come back, or the
        // limiter would keep refusing against series that no longer exist.
        let surviving = series("checkout", 0);
        limiter.refresh(std::iter::once(("checkout", &surviving)));
        assert_eq!(limiter.active_series(), 1);
        assert!(
            limiter
                .admit("checkout", &series("checkout", 2))
                .is_accepted()
        );
    }
}
