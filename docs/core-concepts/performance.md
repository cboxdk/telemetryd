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
