---
title: "Extension points"
weight: 50
description: "What you can change about telemetryd's behaviour, and what you cannot."
---

# Extension points

telemetryd is a binary rather than a library, so extension happens through configuration
and through the APIs — not through plugins. This page is the honest list of what is
adjustable.

## What you can change

**Which attributes become stream labels.** The cardinality contract.
[`ingest.stream_labels`](../configuration/reference.md) decides which resource
attributes create new streams. Everything else is still stored and still queryable.

**Every limit.** Cardinality, label lengths, body size, attributes per record, spans per
trace, ingest queue depth. All configured, all enforced at ingest with a labelled
counter when hit.

**Durability against throughput.** `storage.wal_sync` chooses between `always`,
`interval` and `never`.

**How data is bounded.** Retention per signal, plus a global disk budget that overrides
it.

**Where things live.** Data directory, listen address, log level and format.

**Whether over-long bodies are truncated or refused.**
`ingest.truncate_oversized_bodies` — the default truncates, because a 300 KiB stack
trace is usually the most interesting line of the day.

## What you cannot change

Stated plainly, because an unstated limit is indistinguishable from a bug:

- **No plugins.** No scripting, no WASM, no dynamic loading. A single static binary is
  the product.
- **No custom storage backends.** Files in one directory, by design.
- **No write-path transformations.** No relabelling rules, no sampling, no drop filters.
  Shape the data in your instrumentation, where you have the context to do it well.
- **No TLS.** Terminate at a reverse proxy.
- **No multi-tenancy.** `app` is a namespace, not a boundary.
- **No clustering or replication.** Single node is the design.

## Extending around it

**Query it from anything.** The Loki, Tempo and Prometheus APIs are the extension point
that matters — anything speaking them works within the documented subset.

**Scrape its self-metrics.** `/metrics` is Prometheus exposition; `/status` is JSON with
disk usage, retention activity, per-signal counts and recovery events.

**Fork it.** MIT, roughly ten thousand lines, no C dependencies. The doc comments
explain why each piece is the way it is, including where the original plan turned out
to be wrong.
