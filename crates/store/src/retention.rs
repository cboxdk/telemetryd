//! Retention: time-based expiry and disk-budget enforcement.
//!
//! Both work by deleting whole segments — there are no row-level deletes and no
//! tombstones. That is why the budget is a *soft ceiling with a hard alarm*
//! rather than a hard cap: usage can overshoot by up to one segment before the reaper
//! catches it, and pretending otherwise would be a lie in the documentation.
//!
//! The decision is a pure function ([`plan`]) separated from the deletion, because the
//! failure mode here is deleting the wrong thing, and that is much easier to test when
//! nothing has to touch a disk.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde::Serialize;
use telemetryd_core::Signal;

/// A segment the reaper may delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub signal: Signal,
    pub id: String,
    /// Newest event in the segment. Retention is judged on this: a segment is only
    /// expired once *everything* in it is past the window.
    pub max_time_nanos: u64,
    pub bytes: u64,
}

/// What the reaper decided to do, and why.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Plan {
    /// Expired by the configured retention window.
    pub by_age: Vec<Candidate>,
    /// Deleted early because the disk budget was exceeded.
    pub by_budget: Vec<Candidate>,
    /// Deleted despite not having been forwarded yet, because the budget could not be
    /// held any other way and the policy is `drop_oldest`.
    ///
    /// Always data loss. Separated from `by_budget` so it can be counted, logged and
    /// alerted on as its own thing — "we deleted expired data" and "we deleted data
    /// that never reached its destination" should never share a number.
    pub undelivered_dropped: Vec<Candidate>,
    /// The budget could not be held without deleting undelivered data, and the policy
    /// said not to.
    pub blocked_by_undelivered: bool,
}

impl Plan {
    pub fn is_empty(&self) -> bool {
        self.by_age.is_empty() && self.by_budget.is_empty() && self.undelivered_dropped.is_empty()
    }

    pub fn bytes_freed(&self) -> u64 {
        self.all().map(|c| c.bytes).sum()
    }

    pub fn all(&self) -> impl Iterator<Item = &Candidate> {
        self.by_age
            .iter()
            .chain(&self.by_budget)
            .chain(&self.undelivered_dropped)
    }
}

/// Decide which segments to delete.
///
/// Age first, then budget on whatever is left — so a run that is over budget deletes
/// genuinely expired data before it starts eating into data the operator asked to
/// keep.
/// Segments held back from the reaper because a relay has not forwarded them yet, and
/// what to do when the budget cannot be held without them.
#[derive(Debug, Clone, Copy, Default)]
pub struct Undelivered<'a> {
    pub ids: Option<&'a BTreeSet<String>>,
    pub drop_when_full: bool,
}

impl Undelivered<'_> {
    fn protects(&self, id: &str) -> bool {
        self.ids.is_some_and(|ids| ids.contains(id))
    }
}

pub fn plan(
    candidates: &[Candidate],
    now_nanos: u64,
    retention: &BTreeMap<Signal, Duration>,
    disk_budget: u64,
    non_segment_bytes: u64,
    undelivered: Undelivered<'_>,
) -> Plan {
    let mut plan = Plan::default();
    let mut surviving: Vec<&Candidate> = Vec::new();

    for candidate in candidates {
        let Some(window) = retention.get(&candidate.signal) else {
            surviving.push(candidate);
            continue;
        };
        let cutoff = now_nanos.saturating_sub(u64::try_from(window.as_nanos()).unwrap_or(u64::MAX));
        // Strictly older: a segment whose newest record sits exactly on the cutoff is
        // still inside the window.
        // Expired *and* undelivered still survives. Retention answers "how long do we
        // keep this", not "may we lose it before it arrives" — and a segment the
        // reaper removes before it ships is gone with no trace anywhere.
        if candidate.max_time_nanos < cutoff && !undelivered.protects(&candidate.id) {
            plan.by_age.push(candidate.clone());
        } else {
            surviving.push(candidate);
        }
    }

    // Budget covers everything on disk, not just segments — the write-ahead log and
    // any staging directory count too, or the ceiling is not really a ceiling.
    let mut used: u64 = non_segment_bytes + surviving.iter().map(|c| c.bytes).sum::<u64>();
    if used <= disk_budget {
        return plan;
    }

    // Oldest first, across every signal: the budget is global, so the oldest data
    // anywhere is what goes, regardless of which signal produced it.
    surviving.sort_by(|a, b| {
        a.max_time_nanos
            .cmp(&b.max_time_nanos)
            .then_with(|| a.id.cmp(&b.id))
    });

    // Delivered data first: given a choice between losing something upstream already
    // has and something it has never seen, the copy upstream holds is the cheaper one.
    let (protected, free): (Vec<&Candidate>, Vec<&Candidate>) = surviving
        .into_iter()
        .partition(|candidate| undelivered.protects(&candidate.id));

    for candidate in free {
        if used <= disk_budget {
            return plan;
        }
        used = used.saturating_sub(candidate.bytes);
        plan.by_budget.push(candidate.clone());
    }

    if used <= disk_budget {
        return plan;
    }

    if !undelivered.drop_when_full {
        // Nothing left that may be deleted. The caller stops accepting writes; the
        // alternative is deleting telemetry that never reached its destination, and
        // that has to be an explicit choice rather than what happens by default.
        plan.blocked_by_undelivered = true;
        return plan;
    }

    for candidate in protected {
        if used <= disk_budget {
            break;
        }
        used = used.saturating_sub(candidate.bytes);
        plan.undelivered_dropped.push(candidate.clone());
    }

    plan
}

/// What a reaper run actually did. Reported in logs, `/status` and self-metrics —
/// deleting a user's data is never something they should have to infer.
#[derive(Debug, Default, Clone, Serialize)]
pub struct ReaperReport {
    pub deleted_by_age: u64,
    pub deleted_by_budget: u64,
    pub bytes_freed: u64,
    /// True when the run finished still over budget — the reaper freed everything it
    /// could and it was not enough.
    pub still_over_budget: bool,
    /// Segments deleted although a relay had not forwarded them. Always data loss, and
    /// counted apart from the budget deletions because it is a different event.
    #[serde(default)]
    pub dropped_undelivered: u64,
    /// The budget is full of unforwarded data and the policy forbids deleting it, so
    /// ingest is being refused until upstream drains.
    #[serde(default)]
    pub blocked_by_undelivered: bool,
    pub last_run_unix_nanos: u64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const HOUR: u64 = 3_600_000_000_000;
    const DAY: u64 = 24 * HOUR;
    const NOW: u64 = 1_750_000_000_000_000_000;

    fn candidate(id: &str, signal: Signal, age_nanos: u64, bytes: u64) -> Candidate {
        Candidate {
            signal,
            id: id.to_owned(),
            max_time_nanos: NOW - age_nanos,
            bytes,
        }
    }

    fn retention(days: &[(Signal, u64)]) -> BTreeMap<Signal, Duration> {
        days.iter()
            .map(|(signal, d)| (*signal, Duration::from_secs(d * 24 * 3600)))
            .collect()
    }

    #[test]
    fn segments_past_the_window_are_expired() {
        let candidates = vec![
            candidate("old", Signal::Logs, 8 * DAY, 100),
            candidate("fresh", Signal::Logs, DAY, 100),
        ];
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[(Signal::Logs, 7)]),
            u64::MAX,
            0,
            Undelivered::default(),
        );

        assert_eq!(plan.by_age.len(), 1);
        assert_eq!(plan.by_age[0].id, "old");
        assert!(plan.by_budget.is_empty());
    }

    #[test]
    fn a_segment_is_kept_until_its_newest_record_expires() {
        // Judging on the newest record means a segment spanning the cutoff survives —
        // deleting it would take still-in-window data with it.
        let straddling = Candidate {
            signal: Signal::Logs,
            id: "straddling".to_owned(),
            max_time_nanos: NOW - 7 * DAY + HOUR,
            bytes: 100,
        };
        let plan = plan(
            &[straddling],
            NOW,
            &retention(&[(Signal::Logs, 7)]),
            u64::MAX,
            0,
            Undelivered::default(),
        );
        assert!(plan.is_empty());
    }

    #[test]
    fn each_signal_uses_its_own_window() {
        let candidates = vec![
            candidate("log", Signal::Logs, 10 * DAY, 100),
            candidate("metric", Signal::Metrics, 10 * DAY, 100),
        ];
        let policy = retention(&[(Signal::Logs, 7), (Signal::Metrics, 30)]);
        let plan = plan(
            &candidates,
            NOW,
            &policy,
            u64::MAX,
            0,
            Undelivered::default(),
        );

        assert_eq!(plan.by_age.len(), 1);
        assert_eq!(plan.by_age[0].signal, Signal::Logs);
    }

    #[test]
    fn the_budget_deletes_oldest_first_across_every_signal() {
        let candidates = vec![
            candidate("newest", Signal::Logs, HOUR, 400),
            candidate("oldest", Signal::Metrics, 5 * HOUR, 400),
            candidate("middle", Signal::Logs, 3 * HOUR, 400),
        ];
        // 1200 bytes in use, budget 900 -> must free at least 300.
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[]),
            900,
            0,
            Undelivered::default(),
        );

        assert!(plan.by_age.is_empty());
        assert_eq!(plan.by_budget.len(), 1);
        assert_eq!(
            plan.by_budget[0].id, "oldest",
            "the budget ignores signal boundaries"
        );
    }

    #[test]
    fn age_is_applied_before_the_budget_bites_into_wanted_data() {
        let candidates = vec![
            candidate("expired", Signal::Logs, 30 * DAY, 500),
            candidate("wanted", Signal::Logs, HOUR, 400),
        ];
        // Deleting the expired segment alone brings usage to 400, under the budget.
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[(Signal::Logs, 7)]),
            450,
            0,
            Undelivered::default(),
        );

        assert_eq!(plan.by_age.len(), 1);
        assert!(
            plan.by_budget.is_empty(),
            "expired data should satisfy the budget before in-window data is touched"
        );
    }

    #[test]
    fn non_segment_bytes_count_against_the_budget() {
        let candidates = vec![candidate("only", Signal::Logs, HOUR, 100)];

        // Segments alone fit; with the WAL they do not.
        assert!(
            plan(
                &candidates,
                NOW,
                &retention(&[]),
                500,
                0,
                Undelivered::default()
            )
            .is_empty()
        );
        let with_wal = plan(
            &candidates,
            NOW,
            &retention(&[]),
            500,
            450,
            Undelivered::default(),
        );
        assert_eq!(with_wal.by_budget.len(), 1);
    }

    #[test]
    fn a_run_that_is_within_budget_deletes_nothing() {
        let candidates = vec![candidate("a", Signal::Logs, HOUR, 100)];
        assert!(
            plan(
                &candidates,
                NOW,
                &retention(&[(Signal::Logs, 7)]),
                10_000,
                0,
                Undelivered::default(),
            )
            .is_empty()
        );
    }

    #[test]
    fn an_unmeetable_budget_frees_everything_it_can_rather_than_giving_up() {
        let candidates = vec![
            candidate("a", Signal::Logs, 3 * HOUR, 100),
            candidate("b", Signal::Logs, 2 * HOUR, 100),
        ];
        // Non-segment bytes alone blow the budget; deleting segments cannot fix it,
        // but the reaper must still free what it can and let the caller alarm.
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[]),
            50,
            1000,
            Undelivered::default(),
        );
        assert_eq!(plan.by_budget.len(), 2);
        assert_eq!(plan.bytes_freed(), 200);
    }

    #[test]
    fn a_signal_with_no_configured_window_never_expires_by_age() {
        let candidates = vec![candidate("ancient", Signal::Metrics, 3650 * DAY, 100)];
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[(Signal::Logs, 7)]),
            u64::MAX,
            0,
            Undelivered::default(),
        );
        assert!(
            plan.is_empty(),
            "an unconfigured signal must not be silently dropped"
        );
    }

    #[test]
    fn nothing_is_ever_planned_twice() {
        let candidates = vec![
            candidate("expired", Signal::Logs, 30 * DAY, 500),
            candidate("fresh", Signal::Logs, HOUR, 500),
        ];
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[(Signal::Logs, 7)]),
            1,
            0,
            Undelivered::default(),
        );

        let mut ids: Vec<&str> = plan.all().map(|c| c.id.as_str()).collect();
        let before = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), before, "a segment appeared in both delete lists");
    }

    fn protected(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|id| (*id).to_owned()).collect()
    }

    /// The silent-data-loss guard.
    ///
    /// Retention answers "how long do we keep this", not "may we lose it before it
    /// arrives". A segment the reaper deletes before a relay ships it is gone with no
    /// trace at either end — nothing upstream ever saw it, and nothing here remembers
    /// it existed.
    #[test]
    fn expired_but_undelivered_data_is_not_deleted() {
        let candidates = vec![
            candidate("old-and-sent", Signal::Logs, 8 * 24 * HOUR, 100),
            candidate("old-and-unsent", Signal::Logs, 8 * 24 * HOUR, 100),
        ];
        let held = protected(&["old-and-unsent"]);
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[(Signal::Logs, 7)]),
            u64::MAX,
            0,
            Undelivered {
                ids: Some(&held),
                drop_when_full: true,
            },
        );

        assert_eq!(plan.by_age.len(), 1);
        assert_eq!(plan.by_age[0].id, "old-and-sent");
        assert!(plan.undelivered_dropped.is_empty());
    }

    /// Given a choice, lose the copy that exists in two places.
    #[test]
    fn the_budget_eats_delivered_data_before_undelivered() {
        let candidates = vec![
            candidate("sent", Signal::Logs, HOUR, 100),
            candidate("unsent", Signal::Logs, 2 * HOUR, 100),
        ];
        let held = protected(&["unsent"]);
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[]),
            150,
            0,
            Undelivered {
                ids: Some(&held),
                drop_when_full: true,
            },
        );

        // "unsent" is older, so an ordering-only reaper would take it first.
        assert_eq!(plan.by_budget.len(), 1);
        assert_eq!(plan.by_budget[0].id, "sent");
        assert!(plan.undelivered_dropped.is_empty());
    }

    #[test]
    fn drop_oldest_loses_undelivered_data_only_as_a_last_resort() {
        let candidates = vec![candidate("unsent", Signal::Logs, HOUR, 500)];
        let held = protected(&["unsent"]);
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[]),
            100,
            0,
            Undelivered {
                ids: Some(&held),
                drop_when_full: true,
            },
        );

        assert_eq!(plan.undelivered_dropped.len(), 1);
        assert!(!plan.blocked_by_undelivered);
    }

    #[test]
    fn reject_keeps_the_data_and_says_so_instead() {
        let candidates = vec![candidate("unsent", Signal::Logs, HOUR, 500)];
        let held = protected(&["unsent"]);
        let plan = plan(
            &candidates,
            NOW,
            &retention(&[]),
            100,
            0,
            Undelivered {
                ids: Some(&held),
                drop_when_full: false,
            },
        );

        // Nothing deleted, and the caller is told why so it can stop accepting writes.
        assert!(plan.is_empty());
        assert!(plan.blocked_by_undelivered);
    }
}
