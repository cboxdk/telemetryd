---
title: "telemetryd"
weight: 1
description: "Single-binary observability backend: OTLP and Prometheus in, Loki/Tempo/Prometheus APIs out."
---

# telemetryd

telemetryd stores logs, traces and metrics in one directory and serves them back
through the Loki, Tempo and Prometheus HTTP APIs that `cboxdk/laravel-telemetry-ui`
already speaks.

**One binary, one port, one data directory, no sidecars.** Your telemetry never leaves
your infrastructure.

## The mental model

Four ideas explain almost everything else.

**One process, one directory.** Ingest, storage and query all live in the same binary,
behind the same port. There is no collector to run, no object store to configure and no
second service to keep alive. A data directory has exactly one writer, enforced by a
lock rather than by convention.

**Records become immutable segments.** A record is written to a write-ahead log, held
in memory where it is immediately queryable, and eventually sealed into a Parquet
segment that is never rewritten. Retention deletes whole segments. There is no
compaction of live data and no merge amplification — which is most of why a single host
goes as far as it does.

**Bounded by design.** Retention, a disk budget, cardinality caps, body sizes and queue
depth are all configured numbers. When one is hit, telemetryd rejects loudly — a
labelled counter, a structured error, an entry in `/status` — and never drops data
quietly.

**The API subset is the specification.** telemetryd implements exactly what the UI
calls, read out of that client's source rather than from the upstream API references.
Anything outside the subset returns a structured error naming the feature.

## Where to go next

| | |
|---|---|
| [Quickstart](quickstart.md) | Running and querying in one read |
| [Requirements](requirements.md) | What it needs to run |
| [Getting started](getting-started/_index.md) | Installing, sending data, testing |
| [Core concepts](core-concepts/_index.md) | Architecture, signals, storage, performance |
| [Cookbook](cookbook/_index.md) | Task-shaped recipes |
| [Configuration](configuration/_index.md) | Every option, with defaults |
| [Extension points](extension-points/_index.md) | What you can change, and how |
| [Security](security/_index.md) | Threat model, hardening, honest scope |
| [Decision records](adr/_index.md) | Why it is built the way it is |

## Scope

telemetryd is scoped deliberately: one team, a handful of apps, one VPS or a dev
laptop. It targets that case completely rather than scaling to a large fleet.

Single-node is a design choice, not a limitation waiting to be removed. It is what lets
us delete sharding, consensus, object-store tiering and a query scheduler. If you
outgrow one node you have outgrown telemetryd, and we would rather say so than pretend
otherwise.
