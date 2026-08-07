---
title: "Signals"
weight: 22
description: "Logs, traces and metrics — what differs, and what is deliberately shared."
---

# Signals

telemetryd stores three signals. They share one storage engine and differ only in their
Arrow schema — which is the whole reason adding traces and metrics was a schema and a
query layer rather than two more storage engines.

| | Logs | Traces | Metrics |
|---|---|---|---|
| Ingest | `POST /v1/logs` | `POST /v1/traces` | `POST /v1/metrics`, `POST /api/v1/write` |
| Query API | Loki | Tempo | Prometheus |
| Query language | LogQL subset | TraceQL subset | PromQL subset |
| Default retention | 7 days | 7 days | 30 days |
| Indexed by | event time | span **start** time | sample time |

## Streams and series are the same idea

Every record belongs to a **stream** — a bounded set of labels that identifies it. In
Prometheus terms that is a series; in Loki terms a stream. telemetryd treats them
identically, which is why label discovery works the same way for all three signals and
why the metric name is stored as just another label (`__name__`).

A stream is deliberately small: `app`, `level`, and a configured list of promoted
resource attributes. Everything else is an **attribute** — stored, queryable, but not
part of the stream's identity. Putting high-cardinality values in the stream is how a
telemetry store dies, so the line is drawn in configuration rather than left to chance.

## `app` is always present

Every record carries an `app` label. If a producer sends neither `app` nor
`service.name`, telemetryd assigns `unknown` rather than storing an unattributed record
— so retention, quotas and queries never need a special case for a missing tenant.

`app` is a query namespace, **not a security boundary**. A holder of the ingest token
can write any `app` value. See [Security](../security/_index.md).

## Metrics retention is longer, on purpose

Metrics default to 30 days where logs and traces default to 7. They cost far less per
unit of time, and week-over-week comparison is most of what dashboards are for. The disk
budget is still the hard cap across all three, so this cannot cause an unbounded
footprint.
