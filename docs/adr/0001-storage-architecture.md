---
title: "ADR-001: Storage architecture"
weight: 1
description: "WAL, immutable Parquet segments, and four deviations from the original storage plan."
---

# ADR-001: Storage architecture

- **Status:** Accepted
- **Date:** 2026-08-07
- **Milestone:** M0 (design), realised across M1–M3

## Context

telemetryd stores four signals — logs, trace spans, events, metrics — for a single
team, a handful of apps, on one node, under a hard disk budget (default 10 GiB) and a
short retention window (default 7 days). It must be a single static binary with no
runtime dependencies and no sidecars.

That scale is the design input that matters most. We are not building a system that
must survive a 100-node fan-in or a 500 GiB working set. Single-node is a feature, and
it lets us delete entire categories of machinery (distributed consensus, sharding,
object-store tiering, a query scheduler, a compactor service).

## Decision

Two engines, one lifecycle.

### 1. Record store (logs, spans, events)

Identical machinery for all three; they differ only in their Arrow schema.

```
ingest → validate/cap → WAL (durable) → in-memory Arrow buffer (queryable)
                                              │
                                     seal on time or size
                                              ▼
                              immutable segment on disk (Parquet + manifest)
                                              │
                                    retention reaper deletes whole segments
```

- **WAL** is per-signal, segmented, append-only. Framing is
  `[u32 len][u32 crc32][payload]`. It is the durability boundary and nothing more —
  not a replication log, not a query source beyond crash replay.
- **In-memory buffer** holds the un-sealed window as Arrow `RecordBatch`es. The query
  path reads *sealed segments ∪ live buffer*, so freshly ingested data is queryable
  immediately. On boot the buffer is reconstructed by replaying the WAL tail.
- **Segment** = one directory: `data.parquet` + `manifest.json` (+ optional
  `index/` sidecar). Sealed atomically via write-to-`tmp/` then `rename(2)`.
- **Retention** deletes whole segment directories. No row-level deletes, no
  tombstones, no compaction of live data.

Schema shape: hot labels (`app`, `service`, `severity`, `trace_id`, timestamp) are
hoisted into dedicated typed columns so Parquet statistics give us row-group pruning.
Everything else lives in a `attributes` map column. Rows are sorted by
`(app, timestamp)` within a segment so range scans stay contiguous.

### 2. Metric store

Prometheus `remote_write` (snappy + protobuf — server-side only, so protobuf is fine)
and OTLP metrics both normalise into the same internal sample stream.

- Series identified by a hash of the sorted label set; a `series_id → labels` table
  plus an inverted `label=value → series_id` postings index, both memory-resident
  with an on-disk snapshot + WAL.
- Samples land in per-series chunks: **delta-of-delta timestamps + XOR-encoded
  float values** (Gorilla). Chunks seal at a fixed sample count (default 120) and are
  appended to a per-window chunk file.
- Cardinality caps enforced at write time, per app.

### 3. Shared lifecycle

Both engines register sealed artefacts as segments in the *same* manifest space, so
one reaper enforces both time-based retention and the disk budget. **The byte budget
wins**: when usage exceeds it, the reaper drops oldest-first across all signals until
under budget, and says so loudly (log + self-metric + `/status`).

### 4. On-disk layout

```
telemetryd-data/
├── VERSION                  # storage format version; refuse to start on mismatch
├── LOCK                     # advisory flock — enforces single writer
├── wal/{logs,traces,events,metrics}/NNNNNNNN.wal
├── segments/{logs,traces,events}/<start>-<end>-<ulid>/
│   ├── data.parquet
│   ├── manifest.json
│   └── index/               # optional tantivy sidecar (see D1)
├── metrics/{chunks,labels}/
└── tmp/                     # staging for atomic rename
```

**No embedded KV store** (sled/RocksDB/redb). The segment catalogue is rebuilt at boot
by scanning `manifest.json` files. At 7 d × 1 h × 4 signals that is ~700 small file
reads — single-digit milliseconds — and it is self-healing: a half-written segment
directory without a valid manifest is garbage-collected rather than corrupting an
index. This buys us one fewer dependency, one fewer corruption surface, and one fewer
thing to explain in the README.

## Deviations from the brief

These are the places I am proposing something different from the kickstart spec, with
reasoning. Each is reversible.

### D1 — tantivy becomes an optional sidecar; M1 ships without it

> **Update: the trigger fired.** Measured over 63 million records (405 MB, 877
> segments) on the musl build, a line filter matching *nothing* takes 3.9 s — about
> 10 s per gigabyte scanned, extrapolating to ~98 s over a full 10 GiB store against a
> stated threshold of 1 s. Bounding the time range still works (397 ms over the newest
> 10%, 45 ms over the newest 1%), so the mitigation is real and is what a UI does by
> default. The remaining decision is whether to pay trigram indexing at seal time —
> tokens will not do, because `|=` is a substring match — and that trade is recorded in
> `BUILD-STATUS.md` rather than settled here.


**Brief:** full-text + term lookup via an embedded tantivy index per segment.

**Proposed:** keep tantivy in the design and reserve the `index/` slot in the segment
format from day one, but build it in **M1.5**, behind a config flag, once benches show
where the scan path falls over.

*Why.* The LogQL subset we owe the UI is `{selector} |= "foo"`. The selector resolves
against segment manifest label statistics — it never needs a term dictionary. The line
filter then runs over *one hour of one app's logs*, which under a 10 GiB/7 d budget is
on the order of tens of MB of compressed Parquet: a vectorised substring scan is
sub-100 ms without an index. Tantivy earns its keep for high-selectivity term search
across the *full* retention window, which is a real use case but not the demo path.

Putting it on the M1 critical path means paying for a heavy dependency, a
document-schema mapping, an index-lifecycle problem (build cost at seal time, open-file
pressure at query time — 168 open indices for a 7-day query), and a second corruption
surface, *before* we have a single measurement telling us we need it. The segment
format is versioned and the manifest has the slot, so adding it later is additive.

**Falsifiable trigger:** if the M1 bench shows a 7-day, single-term query over a full
10 GiB dataset exceeding 1 s, tantivy lands in M1.5 as specified.

### D2 — DataFusion is opt-in per query shape, not the default engine

**Brief:** "queries via DataFusion where it buys us something; hand-rolled where it is
overkill" — so this is an interpretation, not a contradiction, but I want it recorded.

**Default is hand-rolled** Arrow predicate evaluation. DataFusion is introduced only
when a concrete query shape demands a planner (likely: Tempo search with mixed tag +
duration predicates, and PromQL aggregation over many series). It is confined to a
single module behind a Cargo feature so we can measure its binary-size and
compile-time cost and drop it if it does not pay.

### D3 — metrics default retention is 30 d, not 7 d

**Brief:** 7-day retention across the board.

Metrics are two orders of magnitude cheaper per unit time than logs — a Gorilla chunk
holds ~1.3 bytes/sample. Defaulting metrics to 7 d throws away the one signal we can
afford to keep, and dashboards are far less useful without week-over-week comparison.
Logs, traces and events stay at 7 d. The disk budget remains the hard cap over all
signals, so this cannot cause an unbounded footprint.

### D4 — segments seal on time **or** size, whichever comes first

A pure 1 h window makes memory unbounded under a burst. Segments seal at
`min(segment_duration, max_segment_bytes)` (default 1 h / 256 MiB buffered). This makes
worst-case memory a configured number rather than a function of traffic.

### D5 — WAL fsync defaults to interval, not per-write

Default `wal_sync = "interval"` at 100 ms, giving a documented worst-case loss window
of 100 ms on hard power loss. `always` (fsync per batch) and `never` are available.
Per-write fsync would cap ingest at the device's sync rate — a few hundred batches/s on
typical VPS storage — which is the wrong default for an observability store where the
data is already a lossy sample of reality. **This is documented in the README, not
buried:** losing 100 ms of telemetry on a power cut is a trade the operator should make
knowingly.

## Consequences

**Good.** No background compaction of live data. Retention is `rm -rf` of a directory.
Crash recovery is WAL replay of one open window. The entire store is inspectable with
`ls` and `parquet-tools`. Deleting an app's data is a directory filter.

**Bad.** Segment-granular retention means the disk budget is enforced in ~1 h
quanta — we overshoot slightly before the reaper catches up; the budget is therefore a
soft ceiling with a hard alarm, and we size the default headroom accordingly. Sorting
by `(app, timestamp)` means a cross-app query at high app-count scans more row groups
than a global time sort would; acceptable at "a handful of apps", revisit if it isn't.

**Accepted limit.** Single writer, enforced by `LOCK`. Two `telemetryd serve`
processes on one data directory is an error, not a race.

## Alternatives rejected

- **ClickHouse/DuckDB embedded** — DuckDB is a compelling single-file analytics engine,
  but it is a C++ dependency that fights the static-musl requirement and gives us no
  story for streaming ingest or live tail.
- **A general TSDB for everything** — writing one store for logs and metrics means
  optimising for neither. The brief is right to split them.
- **Object storage tiering** — irrelevant at one node and 10 GiB.
