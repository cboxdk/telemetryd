---
title: "Architecture"
weight: 21
description: "Six crates, a strict dependency DAG, and the path a record takes end to end."
---

# Architecture

## The path a record takes

```
POST /v1/logs
      │
      ▼
  decode + limits          telemetryd-ingest
      │                    rejections counted, never silent
      ▼
  write-ahead log          telemetryd-store
      │                    durable before the response returns
      ▼
  in-memory buffer         queryable immediately
      │
      │ sealed on time or size
      ▼
  Parquet segment          immutable, never rewritten
      │
      ▼
  retention reaper         deletes whole segments
```

The two properties worth noticing: a record is **durable before the client is told it
was accepted**, and it is **queryable before it is sealed**. A telemetry store where the
last hour is invisible would fail at the thing people actually use it for.

## Crates

```
core ← store ← ingest ─┐
             ↖ query ──┴→ server → telemetryd (bin)
```

| Crate | Holds |
|---|---|
| `telemetryd-core` | Config, the error taxonomy, signal types, label matchers |
| `telemetryd-store` | Data directory, WAL, segments, retention, the query engine |
| `telemetryd-ingest` | OTLP JSON and `remote_write` decoders, limit enforcement |
| `telemetryd-query` | LogQL, TraceQL and PromQL subsets, and the three API shapes |
| `telemetryd-server` | axum router, auth, self-observability |
| `telemetryd` | CLI: `serve`, `status`, `validate`, `service`, `version` |

A strict DAG, no crate depending on a sibling above it. `core` is transport-agnostic —
it knows nothing about axum or HTTP — so the mapping from an error to a status code
lives in `server` and can change without touching the domain.

`server` is separate from the binary so the router can be driven in-process by contract
tests, with no socket and no argument parsing. That is why the test suite runs in
seconds. See [ADR-002](../adr/0002-workspace-layout.md).

## Deliberate absences

Each of these is a thing telemetryd does not have, and the reason:

- **No embedded key-value store.** The segment catalogue is rebuilt at boot by scanning
  manifests — a few hundred small reads. One fewer dependency and one fewer corruption
  surface.
- **No background compaction.** Segments are sealed once and never rewritten. That is
  most of the single-host ingest advantage: no merge amplification.
- **No query planner.** The query shapes are known at compile time.
- **No C dependencies at all.** Which is what makes the static musl builds a
  straightforward link rather than a source of surprises.
