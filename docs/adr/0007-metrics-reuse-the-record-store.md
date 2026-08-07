---
title: "ADR-007: Metrics reuse the record store instead of a bespoke chunk store"
weight: 7
description: "Why metrics reuse the record store instead of the bespoke chunk store ADR-001 planned."
---

# ADR-007: Metrics reuse the record store instead of a bespoke chunk store

- **Status:** Accepted
- **Date:** 2026-08-07
- **Milestone:** M3
- **Supersedes:** the metric-store half of [ADR-001](0001-storage-architecture.md)

## Context

ADR-001 specified two engines: the record store for logs, spans and events, and a
separate metric store with Gorilla-style chunks — delta-of-delta timestamps, XOR-encoded
values, its own label postings index, its own retention.

That was the right call *at the time it was written*, when the record store was a
sketch. It is not the right call now. Between then and M3 the record store acquired:

- per-segment **stream interning** — a dictionary of distinct label sets, referenced by
  a `u32` (ADR-006)
- **late materialisation** and predicate pushdown into Parquet
- a **bounded collector** with segment-level cutoffs
- per-segment **Bloom filters** for exact-key lookups
- manifest-based **pruning**, atomic sealing, crash recovery, and a retention reaper
  that already enforces one global disk budget

A metric sample is `(labels, timestamp, value)`. The record store's stream dictionary
*is* a series index — the exact thing the bespoke design was going to build. Writing a
second engine now means reimplementing all of the above, and every future improvement
to one would have to be ported to the other or silently not apply.

## Decision

**Metrics are a `RecordSchema` over the existing record store.** A sample is a row:
interned `stream_id` (the series), `timestamp_nanos`, `value: f64`.

Concretely this gives us, for free and already tested:

- series identity, deduplicated per segment, with matchers evaluated once per series
  rather than once per sample
- `/api/v1/labels`, `/api/v1/label/{name}/values` and `/api/v1/series` answered from
  segment metadata with **no file I/O**
- one retention pass and one disk budget across all four signals
- the same crash-recovery, atomic-seal and single-writer guarantees
- the same query pushdown, so a range query reads two columns

## What this costs

Honest accounting, because this is a real trade and not a free win.

**Compression.** Gorilla is very good at exactly this shape: ~1.3 bytes/sample on
regular series. Parquet with zstd, a dictionary-encoded `stream_id`, sorted timestamps
(delta-friendly) and `f64` values lands closer to 2–4 bytes/sample. So roughly **2–3×
more disk per sample** than the bespoke design would have used.

That matters less than it sounds. The default disk budget is 10 GiB across all signals,
and logs dominate it by an order of magnitude — a busy app writes far more log bytes
than metric bytes. Trading 2× on the smallest signal to avoid a second storage engine
is a good trade at this scale. **It would not be at a hundred times this scale**, which
is precisely the scale telemetryd declines to serve.

**No specialised chunk-level operations.** A Gorilla store can iterate a series without
touching other series' bytes. Ours reads a row group containing many series and filters.
Mitigated by interning (the filter is an integer compare) and pushdown (only two
columns decompress), but it is not free.

## Revisit if

- metric bytes approach log bytes in a real deployment's `/status`, or
- a range query over a month of one series shows up as a latency problem in the
  benchmarks

Either would mean the assumption above stopped holding. The schema is versioned and the
`RecordSchema` boundary is narrow, so a chunk store can be slotted in behind it later
without touching ingest or query.

## Consequences

**M3 is a schema plus a query layer, not a storage engine.** That is a large reduction
in new, untested code on the most numerous signal we store.

**ADR-001's "two engines, one lifecycle" becomes one engine.** The lifecycle argument it
made was the important half and still holds; the second engine turned out to be
unnecessary to achieve it.

**Cardinality caps still apply at ingest**, unchanged: they are a limit on how many
distinct label sets we accept, not a property of how samples are stored.
