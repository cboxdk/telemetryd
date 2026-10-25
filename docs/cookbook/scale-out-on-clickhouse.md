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

Give every instance the same `[clickhouse]` and `[retention]`. The tables are shared,
so their TTLs are too: whichever instance starts or reloads last sets them, and two
instances with different retention would undo each other on every restart.

A ClickHouse that does not answer at startup stops the start, with an error that names
the URL, the account and the database — an instance that came up and refused every
write would look healthy until the first sender gave up. Before a deployment, ask
instead:

```bash
telemetryd clickhouse check  --config /etc/telemetryd/telemetryd.toml
telemetryd clickhouse schema --config /etc/telemetryd/telemetryd.toml > schema.sql
```

`check` connects with the configured account and checks, changing nothing, each thing
an instance needs: the password can be read, the server answers, the database and every
table and view are there (or can be made), `system.parts` is readable, and the cluster
and storage policy named exist. `schema` prints every statement an instance would run —
database, tables, views and TTLs, in order, each idempotent — for a DBA to review or run
by hand.

Tested on ClickHouse 25.8, the current LTS line, on one server and on a replicated
cluster of two replicas behind a Keeper.

## What lives where

Every instance writes to and reads from the same tables, so it holds no telemetry of
its own: a batch is acknowledged once ClickHouse has it — `async_insert` with
`wait_for_async_insert`, which turns many small batches from many instances into few
parts — and from then on every instance reads it. An instance that is restarted or
replaced loses nothing. `storage.data_dir` still holds the instance's lock, its
self-signed TLS certificate if `tls` is `self-signed`, and empty write-ahead log and
segment directories; give every instance a directory of its own, because one directory
still has one writer.

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
| `otel_trace_keys` | each span and resource attribute key per app per hour | which keys a relaxed or unscoped TraceQL field means |
| `telemetryd_metrics_ingest` | nothing (`Null` engine) | what metrics are inserted into; the views below spread each batch |
| `telemetryd_samples` | every metric sample, by name, series and time | PromQL |
| `telemetryd_series` | each series per day, with its labels | label names and values, series, which series a selector means |
| `telemetryd_folds`, `telemetryd_sample_counts` | each series' counter and gauge summary per hour and per day, and the samples it covers | `rate`, `increase`, `*_over_time` over long windows |
| `telemetryd_meta` | from when each rollup is complete, the TTLs last applied, the fold lease | — |

Each summary is kept by a materialized view named after it with `_mv` —
`otel_log_streams_mv`, `otel_log_volume_mv`, `otel_log_volume_1m_mv`,
`otel_trace_streams_mv`, `otel_traces_trace_id_ts_mv`, `otel_trace_keys_mv`,
`telemetryd_samples_mv`, `telemetryd_series_mv`, `telemetryd_sample_counts_mv` — so it
is current the moment an insert returns. `telemetryd_folds` has no view: instances
write it.

Every table but `telemetryd_meta` is partitioned by day, summaries and folds as much as
the rows, so a TTL drops whole partitions and nothing outlives its retention by more
than a day.

Metrics are not the Collector's `otel_metrics_*`. telemetryd keeps them as Prometheus
does — label sets, classic histograms already expanded into `_bucket` series,
staleness markers, `remote_write` samples with no type — and PromQL reads them that
way; converting each one to an OTLP point and back would be two lossy translations.
Every value reads back bit for bit, staleness markers and infinities included. The
hourly and daily folds are made in the background from the samples, with the local
store's own arithmetic, by whichever instance holds a lease in `telemetryd_meta`; one
whose hour has received late samples since is not used, and is made again.

Set `create_schema = false` for a database whose schema someone else manages:
telemetryd then creates nothing, checks at startup that every table and view is there,
and refuses to start, naming what is missing, when one is not. `telemetryd clickhouse
schema` prints what to create.

### Upgrading a deployment

At startup an instance creates what is missing — a new table, a new column — and skips
what is there, so a restart of an unchanged fleet runs no DDL at all. It never alters an
existing table's engine, sort key or partitioning: a table created by an older version
keeps the layout it was created with. Two cases where that matters:

- `otel_traces_trace_id_ts` is an `AggregatingMergeTree` of each trace's earliest and
  latest span on a new deployment, and stays the plain `MergeTree` of earlier versions
  on an existing one. Both answer the same; the older one holds more rows.
- `otel_trace_keys` starts empty, and telemetryd records in `telemetryd_meta` the moment
  it was created. A TraceQL search over a range before then matches relaxed field names
  the slower way, against every key of every span. To fill it in for older spans:

  ```sql
  INSERT INTO otel_trace_keys
  SELECT toStartOfHour(Timestamp) AS Hour, ServiceName, key.1 AS Scope, key.2 AS Key,
         count() AS Spans
  FROM otel_traces
  ARRAY JOIN arrayConcat(
      arrayMap(k -> ('span', k), mapKeys(SpanAttributes)),
      arrayMap(k -> ('resource', k), mapKeys(ResourceAttributes))) AS key
  WHERE Timestamp < (SELECT fromUnixTimestamp64Nano(toInt64(argMax(Value, Updated)))
                     FROM telemetryd_meta WHERE Name = 'trace_keys_since')
  GROUP BY Hour, ServiceName, Scope, Key;

  INSERT INTO telemetryd_meta VALUES ('trace_keys_since', '0', now64(9));
  ```

### Moving from one node

`telemetryd export --url http://old:4319 --to http://new-balancer:4319 --signal logs`,
and the same for `traces` and `metrics`, copies a single node's data into the
ClickHouse fleet through the APIs both ends serve — see
[Getting data out, and bringing it in](export-and-import.md). Run it once per range:
the copy is not deduplicated.

### Backing up

The telemetry is ClickHouse's to back up, with its own `BACKUP DATABASE telemetry TO
Disk('backups', 'telemetry.zip')` or `TO S3(…)`, and `RESTORE` the same way. The
instances hold nothing a backup needs.

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

Metric samples move too. The summaries, rollups and folds stay on the hot volume, so
label lists, log volume charts and long `rate`s stay fast over the whole retention.

`storage_policy` is set when telemetryd creates a table. On a table that exists,
ClickHouse changes it only to a policy that keeps every disk of the old one —
`ALTER TABLE otel_logs MODIFY SETTING storage_policy = 'hot_cold'` from `default`, if
`hot_cold`'s volumes include the `default` disk — and telemetryd does not try: run it
yourself on each table, then set `cold_after`. ClickHouse also moves parts to the next
volume when the first is fuller than the policy's `move_factor`, whatever their age;
size the hot volume for `cold_after` of data with room to spare, or recent days land on
the cold one.

### How much space

Measured on synthetic API request logs shaped like a geocoding service's — a few dozen
streams, 120-byte lines, attributes on every line — ClickHouse held **18.7 bytes a line**
after compression, summaries included (2 billion lines in 34.9 GiB, from 483 GiB raw).
At sixty billion lines a month that is about 1.1 TB a month, 3.4 TB for ninety days.
Your lines will compress differently; `telemetryd_app_bytes_estimate` and ClickHouse's
`system.parts` say what yours cost.

### How much ingest

Four instances in front of one ClickHouse — all of it in Docker on one 12-core laptop
— took OTLP/HTTP JSON log batches from 64 concurrent senders at **232,000 records a
second** in batches of 500, and **440,000** in batches of 2,000, with no request
refused. Most of a request's latency is ClickHouse gathering concurrent inserts into
one part (`async_insert`), so throughput grows with how many senders write at once,
and the number of parts stays small whatever the fleet.

### Sizing

Sixty billion lines a month is 23,000 a second on average. Ingest is cheap for
telemetryd — it parses, labels and forwards — so two instances carry that with room for
peaks several times higher, and a third lets one be lost or restarted. Queries are where
the cores go, and they go to ClickHouse: the expensive shapes below scale with its
cores and disks, not with how many instances there are. Start ClickHouse with what the
storage above needs on fast disks for the hot days, and add cores when the slow
queries are the ones you run.

Shard when one server is no longer enough: when the hot days no longer fit its disks,
or when the scans — a line filter over hours, an error rate over a day — are slow on
all its cores at once. A shard adds its disks and cores to every scan, since lines and
spans spread over all of them. Until then, one shard of two or three replicas is
simpler and loses nothing: at sixty billion lines a month, ninety days is 3.4 TB, which
one server holds. See [Several shards](#several-shards).

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

`ON CLUSTER` statements are answered once every server has run them, so the first
start of a fleet on an empty database takes a while — half a minute on four servers in
the test cluster — and later starts run none.

## Several shards

When one server's disks or cores are no longer enough, give the cluster more shards.
telemetryd asks the server how many `clickhouse.cluster` has at startup, and on more
than one lays every table out twice: `<name>_local` on each shard, holding that
shard's rows, and `<name>` as a `Distributed` table over all of them. Every read and
write names `<name>` as it always has, so nothing above the store changes; the
materialized views are on the local tables, and each shard rolls up its own rows.

Where a row goes is chosen so that what must meet does:

| tables | spread by | why |
|---|---|---|
| `otel_logs`, the log summaries and volume rollups | at random | lines are only ever counted, summed and sorted across shards: every shard's disks share every scan |
| `otel_traces`, `otel_traces_trace_id_ts` | trace id | a trace's spans and its row in the trace-id index are on one shard, and a lookup by trace id asks only that one |
| `otel_trace_streams`, `otel_trace_keys` | at random | summed across shards |
| `telemetryd_metrics_ingest` and every metric table | series | a series' samples, its series and count rows and its folds share a shard, so the reads that join them do so on each shard, and a fold's versions replace one another where they meet |
| `telemetryd_meta` | — | one copy on every server, shards and all: the settings, markers and fold lease are the same wherever they are read |

What a shard cluster needs, and telemetryd refuses to start without:

- `{shard}` and `{replica}` macros on every server, as for one shard;
- `<internal_replication>true</internal_replication>` on every shard with several
  replicas — without it a `Distributed` table writes each row to every replica, and
  replication copies it to each again;
- a database laid out for this many shards. One created on a cluster of one shard
  keeps its replicated tables when the cluster grows; their rows are not where the
  sharded layout looks, and telemetryd says so rather than start. Start the larger
  cluster on a new database, and copy the old one across with `telemetryd export --to`
  while it is still served (see [Moving from one node](#moving-from-one-node));
- as many shards as the database was laid out for, which it records. A row is on the
  shard its key picks out of that many: with a shard more, new rows of a key go
  elsewhere than its old ones, a metric fold made again no longer replaces the one
  before it, and a read by trace id asks the wrong shard. A cluster that grows needs a
  new database the same way.

An insert is acknowledged once every shard it went to holds its rows
(`distributed_foreground_insert`), so a `200` still means stored; a request that fails
on one shard after another took its part is refused, and the sender's retry can store
that part twice. Reads and writes across shards go to each shard's first replica that
answers, in the order `remote_servers` lists them, and to the server's own for its own
shard — so an instance reads what it just wrote. The other replicas take over when the
first does not answer; to spread reads over them, list replicas in a different order
on different servers.

`telemetryd clickhouse schema` asks the server which layout to print;
`--shards one` or `--shards many` prints one without connecting.

## What is per instance

Some things are decided where a request arrives, and with several instances that is
each one on its own:

- **Series limits.** `limits.max_series` and `max_series_per_app` count the series each
  instance has admitted. Behind a load balancer spreading one app over four instances,
  the effective ceiling is up to four times the setting. Size them per instance.
- **Span metrics and service graphs.** `metrics_generator` pairs a client span with its
  server span in memory. Spans of one trace that reach different instances do not pair;
  route by trace id (most OTLP load balancers can) if you use service graphs.
- **Folds.** One instance at a time makes the hourly and daily metric folds, holding a
  ten-minute lease in `telemetryd_meta` that it renews as it goes; another takes over
  when it stops. Should two overlap, a fold made twice is the same fold, kept once.
  Samples arriving more than 72 hours late are not folded in again: those hours are
  read from the samples.
- **`/status` and `/metrics`** describe the instance you asked, and its requests to
  ClickHouse; what each app holds comes from ClickHouse, is read at most once a minute,
  and is the same on every instance.

Live tail is not per instance: with ClickHouse as the store, a tail asks ClickHouse each
second for what matched, so it sees every instance's lines, a second or so behind. The
load balancer needs no stickiness — any instance answers any request — but it must pass
WebSocket upgrades through for `/loki/api/v1/tail`.

## Watching it

`/ready` is the load balancer's question; `/status` and `/metrics` are yours.
`telemetryd status` names the store, the requests made to it per operation, and the
last one that failed if that was within the hour. `/status` has the same under
`storage.clickhouse`:

```json
"clickhouse": {
  "url": "https://clickhouse.internal:8443", "database": "telemetry",
  "requests": [{"operation": "insert", "succeeded": 81234, "failed": 0, "seconds": 912.4}, …],
  "bytes_sent": 4096000000, "rows_written": 40000000,
  "last_error": {"at": 1790000000, "message": "…"}
}
```

`/metrics` carries it as `telemetryd_clickhouse_requests_total{operation, outcome}`
(`operation` is `insert`, `query` or `schema`; `outcome` is `ok` or `error`),
`telemetryd_clickhouse_request_seconds_total{operation}`,
`telemetryd_clickhouse_rows_written_total` and `telemetryd_clickhouse_bytes_sent_total`.
Two alerts cover most of it:

```yaml
- alert: TelemetrydClickHouseWritesFailing
  expr: sum by (instance) (rate(telemetryd_clickhouse_requests_total{operation="insert", outcome="error"}[5m])) > 0
  for: 5m
- alert: TelemetrydClickHouseSlowInserts
  expr: |
    rate(telemetryd_clickhouse_request_seconds_total{operation="insert"}[5m])
      / rate(telemetryd_clickhouse_requests_total{operation="insert", outcome="ok"}[5m]) > 2
  for: 10m
```

## Access

The account in `clickhouse.user` needs, in its database:

```sql
GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW ON telemetry.* TO telemetryd;
GRANT SELECT ON system.parts TO telemetryd;          -- what each app holds
GRANT CREATE DATABASE ON telemetry.* TO telemetryd;  -- only if it creates the database
GRANT CLUSTER ON *.* TO telemetryd;                  -- only with clickhouse.cluster
```

or simply `GRANT ALL ON telemetry.*` plus the last three. With `create_schema = false`
`SELECT, INSERT ON telemetry.*` and `system.parts` are enough. Its settings profile must
not be `readonly`: inserts set `async_insert` per request. Reads set `readonly = 2`
themselves, so a query runs with read rights only whatever the account may do, and
`max_execution_time` to `clickhouse.timeout`, so ClickHouse stops a query telemetryd
has given up on rather than letting it go on loading the shared server; a query that
outlives it is refused as unavailable.

TLS to ClickHouse trusts the same authorities as every other outbound connection:
`tls.ca_file` for a private CA, the built-in roots otherwise, and `telemetryd
clickhouse check` trusts what `serve` would.
There are no client certificates; ClickHouse authenticates the user and password,
which `http://` would send in clear — so it is refused to anything but loopback unless
`allow_plaintext = true`.

## Delivery

A batch is acknowledged once ClickHouse holds it. If ClickHouse does not answer, or
refuses the insert for any reason — rows it would not parse included — the instance
answers `503` with `Retry-After` and the sender keeps the batch and retries: at least
once, as everywhere in OTLP, and a batch whose acknowledgement was lost can be stored
twice. The reason in the response is generic; the server log has ClickHouse's own words. Two lines that share a
timestamp come back in no defined order, as in Loki; on one node they come back in the
order they arrived, which has no meaning across instances.

## Rows other writers left

telemetryd reads the rows it wrote. A row in `otel_logs` written by something else —
the store package's own ingest, a Collector — has an empty `Stream`, so no stream
selector matches it; telemetryd's APIs do not see it. The other readers see everything.
The rollups are complete from the moment they were created, which telemetryd records in
`telemetryd_meta`; a range reaching back before that is counted from the lines.

## Relay

`relay.upstream` and `[clickhouse]` together are refused: a relay forwards what it
stores on its own disk, and here there is none. A relay in front of instances on
ClickHouse is a relay without `[clickhouse]`, forwarding to the load balancer.
