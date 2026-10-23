---
title: "Scale out on ClickHouse"
weight: 95
description: "Run any number of telemetryd instances in front of one ClickHouse, keep ninety days or more, and change nothing in your apps or dashboards."
---

# Scale out on ClickHouse

One telemetryd on one machine is the default, and needs nothing else. When one machine
is no longer enough — more data than its disk holds for the retention you need, more
ingest than one process takes, or no single machine you are willing to lose — the same
binary runs as many instances as you like in front of one ClickHouse:

```
  apps (laravel-telemetry, OTLP)        Grafana, laravel-telemetry-ui
              │                                     │
              ▼                                     ▼
        ┌──────────────── load balancer ──────────────────┐
        │  telemetryd   telemetryd   telemetryd   …       │   stateless: any one
        └───────────────────────┬─────────────────────────┘   answers for all data
                                ▼
                    ClickHouse (one server, or a replicated cluster)
```

Nothing in front of it changes. Apps send the same OTLP to the same paths; Grafana and
laravel-telemetry-ui ask the same Loki, Tempo and Prometheus APIs and get the same
answers — the test suite holds ClickHouse's answers to the local store's and to Loki's
and Tempo's own, query for query.

```toml
[clickhouse]
url      = "https://clickhouse.internal:8443"   # the HTTP interface
database = "telemetry"
user     = "telemetryd"
password = "file:/run/secrets/clickhouse"

[retention]
logs    = "90d"
traces  = "90d"
metrics = "395d"
```

Start one instance with that, and it creates the database and tables it needs. Start
more with the same file and put them behind a load balancer; `/ready` answers `503`
when an instance cannot reach ClickHouse, so the balancer stops sending it writes.

## What lives where

Every instance writes to and reads from the same tables, so it holds no telemetry of
its own: a batch is acknowledged once ClickHouse has it — `async_insert` with
`wait_for_async_insert`, which turns many small batches from many instances into few
parts — and from then on every instance reads it. An instance that is restarted or
replaced loses nothing. `storage.data_dir` still holds its TLS material and nothing
else of consequence.

The tables are the OpenTelemetry Collector's ClickHouse layout — `otel_logs`,
`otel_traces` — as `cboxdk/laravel-telemetry-store` creates them, so that package's
readers and Grafana's ClickHouse plugin read what telemetryd writes, and SQL you write
by hand finds the columns where the Collector's documentation says they are. telemetryd
adds columns of its own beside them (`Stream`, `StreamHash`, `AttributeLabels`) and
small tables kept by materialized views:

| Table | Holds | Answers |
|---|---|---|
| `otel_logs`, `otel_traces` | every line and span | everything else |
| `otel_log_streams`, `otel_trace_streams` | each stream per hour | label names and values, series, which streams a selector means |
| `otel_log_volume`, `otel_log_volume_1m` | lines and bytes per stream per second, per minute | log volume: `count_over_time`, `rate`, `bytes_*` with no line filter |
| `otel_traces_trace_id_ts` | each trace's first and last span | a trace by id, without probing every part |
| `telemetryd_samples` | every metric sample, by name, series and time | PromQL |
| `telemetryd_series` | each series per day, with its labels | label names and values, series, which series a selector means |
| `telemetryd_folds`, `telemetryd_sample_counts` | each series' counter and gauge summary per hour and per day, and the samples it covers | `rate`, `increase`, `*_over_time` over long windows |
| `telemetryd_meta` | from when each rollup is complete | — |

Metrics are not the Collector's `otel_metrics_*`. telemetryd keeps them as Prometheus
does — label sets, classic histograms already expanded into `_bucket` series,
staleness markers, `remote_write` samples with no type — and PromQL reads them that
way; converting each one to an OTLP point and back would be two lossy translations.
Every value reads back bit for bit, staleness markers and infinities included. The
hourly and daily folds are made by each instance in the background from the samples,
with the local store's own arithmetic; one whose hour has received late samples since
is not used, and is made again.

Set `create_schema = false` for a database whose schema someone else manages;
`telemetryd` then expects the tables to exist.

## Ninety days, and more

Retention is `retention.logs`, `retention.traces` and `retention.metrics`, as on one
node, applied as each table's TTL. ClickHouse enforces it by dropping whole day
partitions, which costs nothing. Changing it in the configuration takes effect on the
next start or reload, without rewriting what is stored. Up to ten years
(ClickHouse's `DateTime` ends in 2106).

At volume most of those days should not sit on the fastest disks. With a [storage
policy](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes)
that has a hot and a cold volume — local NVMe and object storage, say — telemetryd moves
older days of logs and spans to the cold one and keeps them queryable there:

```toml
[clickhouse]
storage_policy = "hot_cold"   # defined in ClickHouse's server config
cold_after     = "7d"         # logs and spans older than this move…
cold_volume    = "cold"       # …to this volume of the policy
```

The summaries and rollups stay on the hot volume, so label lists and log volume charts
stay fast over the whole retention. A storage policy applies when a table is created;
ClickHouse cannot move an existing table to another.

### How much space

Measured on synthetic API request logs shaped like a geocoding service's — a few dozen
streams, 120-byte lines, attributes on every line — ClickHouse held **17.4 bytes a line**
after compression (2 billion lines in 34.9 GiB, from 483 GiB raw). At sixty billion
lines a month that is about a terabyte a month, three for ninety days. Your lines will
compress differently; `telemetryd_app_bytes_estimate` and ClickHouse's `system.parts`
say what yours cost.

## How queries stay fast

A query is answered by ClickHouse doing the selecting and telemetryd doing the
language: LogQL, TraceQL and PromQL run in the same code as on one node, over what
ClickHouse hands back, which is what keeps the answers the same.

- **Streams first.** A selector is resolved against the streams summary — exactly, by
  the matcher code the local store uses — into a list of stream hashes, and the table is
  read with `ServiceName IN (…) AND StreamHash IN (…)`: a `UInt64` column rather than a
  map, and an app the sort key prunes on even when the query named none.
- **Newest first, in windows.** `{app="api", level="error"}` with a limit of 100 over a
  day reads the last few seconds of that day, sized from how densely the stream writes,
  widening only if they do not hold enough.
- **Line filters, TraceQL conditions in SQL.** `|= "timeout"` becomes a `LIKE` the body's
  token index can skip granules by; `{ span.http.route = "/geocode" && duration > 1s }`
  becomes conditions on the maps and the duration column. Each is a condition every
  match meets, never sufficient on its own: the query's own evaluation still decides.
- **Log volume from rollups.** A count with no line filter is answered from the per-minute
  rollup for whole minutes, the per-second one for the seconds of minutes a bucket bound
  splits, and the lines only for the seconds a bound splits — exactly, line for line.

Measured through telemetryd's Loki API, on one ClickHouse in Docker on a laptop
(12 cores), against a day of 2 billion lines:

| Query | Time |
|---|---|
| `{app="api", level="error"}`, limit 100, 24 h | 0.04 s |
| `{app="api"} \|= "status=503"`, limit 100, 24 h | 0.05 s |
| `{app="api"} \| logfmt \| status >= 500`, limit 100, 1 h | 0.3 s |
| `sum by (level) (count_over_time({app="api"}[5m]))`, 24 h | 0.01 s |
| `sum(count_over_time({level="error"}[24h]))` | 0.01 s |
| labels, label values, series, 24 h | < 0.01 s |
| `sum(rate({app="api"} \|= "/geocode" [1m]))`, 6 h, a third of all lines match | 9 s |

The last is the shape that stays expensive: a line filter that most lines pass, over
hours, reads every line it covers. It scales with ClickHouse's cores, not telemetryd's.

And through the Tempo API, against a day of 600 million spans (200 million traces):

| Query | Time |
|---|---|
| search `{}`, limit 20, 24 h | 0.15 s |
| search `{ status = error }`, 24 h | 0.07 s |
| search `{ span.http.route = "/places" && duration > 390ms }`, 24 h | 0.06 s |
| search for one attribute value among 100,000, 24 h | 0.23 s |
| trace by id | 0.01 s |
| tag values, tag names, 24 h | 0.9 s |
| `{ kind = server } \| quantile_over_time(duration, 0.95)`, 1 h at 5 m | 0.12 s |
| `{ status = error } \| rate() by (span.http.route)`, 1 h at 1 m | 0.26 s |
| the same, 24 h at 1 h | 6 s |

And through the Prometheus API, against 42,500 series sampled every 30 seconds for 30
days (3.7 billion samples, 3.1 GiB), with Grafana's step-aligned ranges:

| Query | 24 h | 7 d | 30 d |
|---|---|---|---|
| p95 of a request histogram over the range, instant | 0.6 s | 0.9 s | 1.5 s |
| `increase` per route over the range, instant | 0.07 s | 0.10 s | 0.15 s |
| `rate` per route, charted (5 m / 1 h / 1 h step) | 0.18 s | 0.39 s | 1.4 s |
| p95 per route, charted (1 d step) | — | — | 2.4 s |
| label values, series | < 0.07 s | | |

What stays expensive is a quantile per series at a fine step over a long range — p95
per route at a one-hour step over thirty days, 45 s here: each of the histogram's
series is evaluated at every step in telemetryd. Chart such panels at a coarser step,
or record them as their own series.

A search reads matching spans newest first and stops as soon as no trace it has not
seen could start later than the ones it has; only those are read whole. TraceQL
metrics are counted in ClickHouse, by intrinsics, resource and span attributes, with
the rare span ClickHouse cannot decide on — an attribute only a relaxed name reaches,
a number written unusually — read and counted by telemetryd, so the answer is the one
the local store gives.

## A replicated cluster

For a ClickHouse you can lose a server of, name its cluster:

```toml
[clickhouse]
cluster = "telemetry"   # from ClickHouse's remote_servers, with {shard}/{replica} macros
```

Every table is then created `ON CLUSTER` with a `Replicated` engine — the instances can
start together; the statements are idempotent — and each instance can talk to a
different replica. The test suite runs two instances in front of two replicas and holds
one's answers to the other's. Deduplication of identical inserted blocks is turned off
for these tables: a rollup's blocks are computed, and two different batches can count
one second of one stream to the same number.

## What is per instance

Some things are decided where a request arrives, and with several instances that is
each one on its own:

- **Series limits.** `limits.max_series` and `max_series_per_app` count the series each
  instance has admitted. Behind a load balancer spreading one app over four instances,
  the effective ceiling is up to four times the setting. Size them per instance.
- **Span metrics and service graphs.** `metrics_generator` pairs a client span with its
  server span in memory. Spans of one trace that reach different instances do not pair;
  route by trace id (most OTLP load balancers can) if you use service graphs.
- **Folds.** Every instance makes the hourly and daily metric folds; it is cheap and
  idempotent, so several doing it at once only repeat one another. Samples arriving
  more than 72 hours late are not folded in again: those hours are read from the samples.
- **`/status` and `/metrics`** describe the instance you asked, and its requests to
  ClickHouse (`telemetryd_clickhouse_*`); what each app holds comes from ClickHouse and
  is the same on every instance.

Live tail is not per instance: with ClickHouse as the store, a tail asks ClickHouse each
second for what matched, so it sees every instance's lines, a second or so behind.

## Delivery

A batch is acknowledged once ClickHouse holds it. If ClickHouse does not answer, the
instance answers `503` and the sender retries — at least once, as everywhere in OTLP: a
batch whose acknowledgement was lost can be stored twice. Two lines that share a
timestamp come back in no defined order, as in Loki; on one node they come back in the
order they arrived, which has no meaning across instances.

## Rows other writers left

telemetryd reads the rows it wrote. A row in `otel_logs` written by something else —
the store package's own ingest, a Collector — has no `Stream` column, so no stream
selector matches it; telemetryd's APIs do not see it. The other readers see everything.
The rollups are complete from the moment they were created, which telemetryd records in
`telemetryd_meta`; a range reaching back before that is counted from the lines.

## Relay

`relay.upstream` and `[clickhouse]` together are refused: a relay forwards what it
stores on its own disk, and here there is none. A relay in front of instances on
ClickHouse is a relay without `[clickhouse]`, forwarding to the load balancer.
