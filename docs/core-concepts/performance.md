---
title: "Query performance"
weight: 24
description: "Why queries cost the size of the answer rather than the size of the data."
---

# Query performance

The design goal is that a query costs the size of its **answer**, not the size of the
**data**. Five mechanisms get there, and each follows from knowing the query shapes in
advance. Full reasoning and measurements are in


## What happens to a query

**Segments are pruned before they are opened.** Each manifest carries the segment's time
bounds, a per-label value index, and the distinct label sets it contains. A query that
cannot match is skipped with no file I/O at all.

**Matchers run once per stream, not once per row.** Label sets are interned into a
per-segment dictionary, so a segment with a million rows across fifty streams evaluates
a selector fifty times.

**Only surviving rows are decoded.** Row selection reads two columns — a timestamp and a
`u32` stream id — and produces a row-index list. Decoding a record allocates strings and
maps; doing that before filtering means a `limit=100` query pays for every row it scans.

**The predicate is pushed into Parquet.** Wide columns — bodies, attribute maps, span
events — are never *decompressed* for rows that will be discarded.

**Limits stop the scan.** A bounded collector holds `limit` records rather than every
match, so peak memory is set by what was asked for. Once full, its cutoff lets whole
segments be skipped: a `limit=10` query over a 30-segment store opens at most three.

## Counter summaries

A `rate` or `increase` over a long window is the one shape the mechanisms above cannot
help with: it is not selective. `rate(http_requests_total[90d])` matches everything in
the window by construction, so there is nothing to prune and nothing to stop early — the
query reads every row it covers. That is why a wide dashboard panel used to cost seconds
while a `limit=100` log search cost milliseconds.

So a segment carries the answer next to it. When a segment is sealed, telemetryd folds
each stream's samples once and writes a small `folds.bin` beside the Parquet file: how
many samples, the first and last timestamp and value, and the accumulated increase with
counter resets already applied. Forty-eight bytes per stream.

A window that wholly contains a segment can then take that stream's total without opening
the segment at all. Only the two segments straddling the window's edges are read, plus
whatever has not been sealed yet. **Measured on a 30-million-row store: a 24-hour window
went from 724 ms to 234 ms, a 7-day window from 6.4 s to 555 ms, and 90 days from 7.1 s
to 316 ms.** Answers agreed with the row-by-row path on every one of 2 456 values, the
largest disagreement being 1.07e-15 — floating-point addition in a different order.

Three properties are worth stating, because each was a decision:

**It is exact, not approximate.** The summary is a fold over the same rows in the same
order, not a sample or a sketch. A query answered from summaries and one answered from
rows differ only by the order the additions happen in.

**It is a shortcut, not a replacement.** When enough segments in a window lack a summary
that reading them would cost more than the ordinary scan, the store declines and the
query takes exactly the path it took before — the same code, unchanged. A store that has
never summarised anything is no slower than one that never had the feature.

**Existing segments are summarised in the background.** Upgrading does not rewrite the
store on startup. A maintenance task folds a few segments at a time, so an upgraded
instance reaches full speed within the hour while staying answerable throughout. Nothing
about the on-disk format changed: `folds.bin` is additive, and an older telemetryd
ignores it.

## Queries that touch no data at all

`/loki/api/v1/labels`, `/api/v1/label/{name}/values` and `/series` are answered from
segment metadata. No Parquet file is opened, and the answer is exact — there is no
cardinality cutoff to under-report from.

## Trace by id

A trace id has no useful ordering and cardinality equal to the row count, so nothing
statistical can prune it. Each segment carries a Bloom filter over its trace ids, which
answers "definitely not here" exactly. A lookup reads about one segment instead of every
segment in the retention window.

## Correctness is layered

Every one of these mechanisms may **over**-select. None may under-select. The
record-level predicate remains the sole authority on what is returned.

That is what makes the optimisations safe to add and safe to remove: getting one subtly
wrong costs speed, never correctness. The test suite asserts both halves — that pruning
happens, *and* that the result equals a full scan.

## Watching it

```bash
curl -s http://127.0.0.1:4319/metrics | grep query_segments
```

`telemetryd_query_segments_scanned_total` against
`telemetryd_query_segments_pruned_total` is the ratio to watch when queries feel slow.
A high scan count means something is defeating pruning — usually a query with no
selective matcher.

## Where a general engine stays ahead

Named so the trade is explicit: large aggregations over billions of rows, arbitrary
`GROUP BY` and joins, specialised compression codecs, and mature parallel query
execution. telemetryd does not compete on any of them, and that is the trade that makes
a single binary with no planner and no merge scheduler possible.
