# ADR-006: Query performance comes from knowing the queries

- **Status:** Accepted
- **Date:** 2026-08-07
- **Milestone:** M2 (applies to every signal)

## Context

The question that prompted this: can a purpose-built store outperform a general
analytical engine on a single host?

For arbitrary analytical SQL, no — and it would be a bad use of our time to try.
Vectorised aggregation over billions of rows, arbitrary joins, and codec engineering
are the product of years of specialised work.

But that is not the workload. telemetryd serves a **closed set of query shapes from a
known client**, which we read out of that client's source rather than guessing
(ADR-005). There are about eight of them:

1. newest *N* log lines for a stream selector, optionally with a line filter
2. one trace by id
3. span search over a bounded window with attribute predicates
4. label names / label values / series
5. Prometheus range and instant queries over a step grid

A general engine must plan for a query it has not seen. We know ours in advance, and
that is a structural advantage rather than a clever one — it lets us build exactly the
index each shape wants and skip the machinery that exists to handle the shapes we do
not have.

## The measurement that set the direction

The first honest benchmark said the query path cost **526 ns per record**, and it was
almost entirely decode: every row re-parsed two JSON label maps and allocated seven
strings *before* any filter ran. A `limit=100` query over 100k rows cost 3.2 ms to
return 100 rows. The store was doing work proportional to the data, not to the answer.

## Decision

Five changes, each following from a known query shape.

### 1. Stream interning — matchers run per stream, not per row

Stream label sets repeat massively: a segment holding a million rows typically has a
few dozen distinct ones. They are interned into a per-segment dictionary held in the
manifest; the data carries a `u32` `stream_id`.

This does two things. Decoding a stream stops being a JSON parse and becomes an array
index. And a selector like `{app="checkout", level="error"}` is evaluated **once per
distinct stream** — a few dozen times — instead of once per row.

It also makes label discovery free: `/loki/api/v1/labels`, `label/{name}/values` and
`/series` are answered from the dictionary with **no file I/O at all**, and exactly,
with no cardinality cutoff to under-report from.

### 2. Late materialisation — decode only what survives

Row selection reads two columns (timestamp, stream id) and produces a row-index list.
Only those rows are decoded. `select_rows` over 10k rows measures at **8.9 µs** —
0.89 ns/row, against 526 ns/row to decode one.

### 3. Predicate pushdown into Parquet

The selection predicate is handed to arrow-rs as a `RowFilter` over a projection of
just the filter columns. The wide columns — bodies, attribute maps, span events — are
never *decompressed* for rows that will be discarded, not merely never decoded.

### 4. A bounded collector with a segment cutoff

Queries carry their limit and direction all the way down. A top-K collector holds
`limit` records rather than every match, so peak memory is set by what was asked for
rather than by what happened to match. Once full, its cutoff timestamp lets whole
segments be skipped without opening them: a `limit=10` query over a 30-segment store
opens at most three.

### 5. Segments sorted by time; metadata parsed once

Rows are sorted by timestamp at seal, which gives Parquet row-group statistics
something real to prune with. Segments are immutable, so the Parquet footer is parsed
once per segment and reused — on a selective query that fixed cost was a large share of
the total.

Sorting is skipped when the buffer is already ordered, which it normally is; the
unconditional `to_vec()` it replaced measured at ~20% of seal time.

## Results

Same benchmark, before and after:

| | before | after |
|---|---|---|
| filter 10k rows | 5.26 ms (full decode) | **8.9 µs** (`select_rows`) |
| `limit=100`, newest first, 100k rows | 3.18 ms | ~1.4 ms |
| narrow time window | 3.14 ms | ~0.97 ms |
| seal 20k records | 40.4 ms | 40.0 ms |
| label values | full segment read | **no I/O** |
| trace by id | every segment | ~1 segment (Bloom) |

The remaining `limit=100` cost is dominated by opening segments and decompressing the
filter columns — the floor for actually touching data on disk.

## Where the structural advantages are

Honest accounting of what specialisation buys, and what it does not.

**Genuine, durable advantages on one host:**

- **No merge amplification.** A general LSM/MergeTree store rewrites data several times
  through background merges. We seal a segment once and never rewrite it. Less disk I/O
  per ingested byte is the single biggest lever on single-host ingest.
- **Point lookups.** A trace id has no useful ordering and cardinality equal to the row
  count. Our per-segment Bloom filter answers "definitely not here" exactly; a
  scan-oriented engine needs a skip index to approximate it.
- **Metadata-only answers.** Label and series queries touch no data files.
- **Fixed per-query cost.** No SQL parse, no planner, no optimiser — the shapes are
  known at compile time.
- **Footprint.** Tens of MB rather than hundreds, which decides whether this runs on a
  small VPS at all.

**Where a general engine stays ahead, and we do not compete:**

- large aggregations over billions of rows (vectorised execution, SIMD, codegen)
- arbitrary `GROUP BY` and joins
- compression ratios from specialised per-type codecs
- mature parallel query execution

This is not a limitation to fix later. It is the trade that makes the rest possible: a
single binary with no planner, no merge scheduler and no coordinator has fewer moving
parts precisely because it declined those problems.

## Consequences

**The segment format is v2** and not backward compatible. `SEGMENT_FORMAT_VERSION` is
bumped and mismatched segments are refused rather than misread. The read path still
handles a missing dictionary so a future format change has an escape hatch.

**Correctness is layered.** The Bloom filter, the manifest label index, the Parquet
pushdown and the columnar line pre-filter may all **over**-select; none may
under-select. The record-level predicate remains the sole authority on what is
returned. This is what makes the optimisations safe to add and safe to remove: getting
one subtly wrong costs speed, never correctness. `tests/scale.rs` asserts both halves —
that pruning happens, and that the answer is identical to a full scan.

**Benchmarks are part of the build.** `cargo bench -p telemetryd-store` measures the
constants; `tests/scale.rs` asserts the asymptotics as segment-open counts rather than
wall-clock times, so they are meaningful on a shared CI runner.

## Not done yet

Named so they are choices rather than oversights:

- **Attribute maps are still per-row JSON.** They are genuinely high cardinality, so
  interning does not obviously apply; a dictionary-encoded Arrow `Map` is the likely
  next step. Only paid for rows that survive filtering, so it no longer dominates.
- **No text index.** Line filters scan the body column. A per-segment trigram index
  would let a selective `|=` skip row groups entirely; ADR-001 D1 already reserves the
  slot and set the trigger condition.
- **Single-threaded per query.** Segments are independent and could be scanned in
  parallel. Deliberately deferred: on a machine also serving ingest, spending every
  core on one query is not obviously the right default.
