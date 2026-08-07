---
title: "ADR-008: Parallel segment scanning, for unbounded queries only"
weight: 8
description: "Why only unbounded queries use more than one thread, and what the measurements actually showed."
---

# ADR-008: Parallel segment scanning, for unbounded queries only

- **Status:** Accepted
- **Date:** 2026-08-07
- **Supersedes:** the "single-threaded per query" gap in `BUILD-STATUS.md`

## Context

Sealed segments are independent files. Scanning them on one thread was always a
deliberate deferral rather than an oversight — the stated reason being that a process
also accepting writes should not hand every core to one query.

The benchmark store (20 segments, 100k records) put numbers on what that was costing:

| Query shape | Time |
|---|---|
| `limit=100`, newest first | 1.44 ms |
| `limit=100` with a line filter | 3.00 ms |
| narrow time window | 0.99 ms |
| **unbounded, whole store** | **72.7 ms** |

One shape is fifty times the rest. That is the one worth dividing.

## What the first attempt showed

Parallelising the segment loop across four workers, unconditionally:

| | Sequential | 4 workers | |
|---|---|---|---|
| unbounded scan | 72.7 ms | 56.7 ms | 1.28× faster |
| `limit=100` | 1.45 ms | 2.33 ms | **1.6× slower** |

The regression is the interesting half, and it is not a tuning problem — it is what
the design does when you split it.

A limited scan is not fast because it reads quickly. It is fast because the collector's
cutoff tightens on the first segment, and the remaining nineteen are then rejected from
their manifests without ever being opened. That is a *sequential* dependency: segment
20 is cheap precisely because segment 1 already ran. Four workers start before any
cutoff exists, so they open and decode four segments' worth of data that the cutoff
would have discarded. The parallelism is real; the work is waste.

## Decision

**Only unbounded queries are scanned in parallel.** With `limit == 0` there is no
cutoff to lose, so the work divides cleanly. Any query with a limit stays sequential
and keeps the pruning that makes it fast.

`storage.query_parallelism` controls the ceiling: `0` picks a conservative fraction of
the machine (half the cores, capped at four), `1` disables it.

Two supporting pieces:

**A shared cutoff.** Workers publish their local cutoff to one atomic, and check it
before opening a segment. The merged top-k is the top-k of the union, and the union
contains every worker's k entries — so the merged cutoff is at least the tightest
individual one, and publishing the extreme is always a sound bound. It can only ever
skip *less* than the truth allows, never more. (This currently only benefits unbounded
queries, which have no cutoff; it exists so the mechanism is correct if the limit
restriction is ever relaxed.)

**Ties break on position, not on arrival.** `TopK` previously ordered equal timestamps
by a per-collector insertion counter. Under parallelism that counter reflects thread
scheduling, so the same query over unchanged data could return a different hundred
lines each run, and a paging UI would show a line twice or skip one. Timestamps tie
constantly in practice — plenty of producers emit millisecond precision into a
nanosecond field — so this is the common case, not an edge one. The key is now
`(scan unit, row)`: which part of the store the record came from and where within it.
That is a property of the data, so one thread and eight produce the identical answer.

## What it actually buys

| | Time | vs sequential |
|---|---|---|
| unbounded, sequential | 71.8 ms | — |
| unbounded, 4 workers | 56.5 ms | 1.27× |
| unbounded, 8 workers | 57.3 ms | 1.25× |
| `limit=100`, 4 workers configured | 1.43 ms | unchanged (runs sequentially) |

**Eight workers are no faster than four.** That is the honest headline: the remaining
cost is serial — materialising and sorting a hundred thousand records is bound by
allocation, not by segment decode — so this is an Amdahl ceiling, not a tuning knob
that was left too low. Raising the default would spend cores for nothing, which is why
the automatic value caps at four.

1.27× is a modest return for concurrency in the hot path. It is kept because the cost
is contained: limited queries are untouched, the behaviour is off by one config value,
and equivalence with the sequential path is asserted rather than assumed.

## How it is kept honest

Two tests, both in `crates/store/tests/scale.rs`:

- `parallel_scanning_returns_exactly_the_sequential_answer` seeds heavy timestamp
  ties (~40 records per distinct timestamp) and compares the two paths element by
  element across four order/limit combinations. Not "same length" — same records, same
  order.
- `a_parallel_query_is_deterministic_across_runs` repeats one parallel query a dozen
  times and requires an identical result each time, so a race that happened to resolve
  consistently twice does not pass.

## Revisit if

- the unbounded path stops being allocation-bound — streaming results to the caller
  instead of collecting them would move the ceiling, and then more workers would pay
- a profile shows segment decode, rather than materialisation, dominating a wide query

## Consequences

The `limit == 0` path is the only concurrent one, so the code that a log viewer
exercises on every keystroke is still single-threaded and still deterministic by
construction rather than by test.

`ColumnFilter` and the record predicate gained a `Sync` bound. They were already
`Send + Sync` types in practice, so no caller changed.
