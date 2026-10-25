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

At startup an instance creates what is missing — a new table, view, column or index —
and skips what is there, so a restart of an unchanged fleet runs no DDL at all. It never
alters an existing table's engine, sort key or partitioning: a table created by an older
version keeps the layout it was created with, and a rollup created after the rows it
summarises holds them only from then on. Answers are correct either way; some read more
than they would. `telemetryd clickhouse migrate` brings the database up to this
version's layout, while the fleet keeps writing:

```bash
telemetryd clickhouse migrate --config /etc/telemetryd/telemetryd.toml --dry-run
telemetryd clickhouse migrate --config /etc/telemetryd/telemetryd.toml
telemetryd clickhouse migrate --config /etc/telemetryd/telemetryd.toml --backfill --materialize-indexes
```

`--dry-run` lists what it would do and changes nothing; it creates and drops a probe of
each table to learn what this version's statements give on your server. Without
`--dry-run`, it does what an instance's startup does, and then:

- **Rebuilds a table laid out differently.** It creates the new layout under a
  temporary name and swaps it in with `EXCHANGE TABLES`: writes and materialized views
  name the table, so from that moment they land in the new one. The old rows are then
  copied in a partition at a time — into a staging table, checked against the old
  partition (distinct keys, and the total of every summed column), and moved in with
  `MOVE PARTITION`, which adds parts without an insert, so no view counts them again.
  Each partition is recorded in `telemetryd_meta` as staged and then moved: a run that
  is interrupted is run again and continues without copying any partition twice. A
  table whose rows merge to one answer however often they arrive — the trace-id index,
  the folds — is copied once before the swap as well, so nothing is missing from it
  while the rest is copied. The old table is dropped at the end; `--keep-old` keeps it.
  The database must be `Atomic`, ClickHouse's default.
- **Drops what earlier versions created and this one does not**, by name — the
  `otel_trace_attribute_keys` table and its view, the `idx_stream_value` index — and
  nothing else.
- **Adds an index a table lacks.** An index added to a table with rows covers only
  parts written from then on; `--materialize-indexes` indexes the older partitions, one
  `MATERIALIZE INDEX` per partition, and then drops an index it replaces.
- **Fills in rollups** with `--backfill`: the log volume rollups and `otel_trace_keys`,
  for rows older than the rollup. A day at a time, newest first, it adds per key what
  the rows hold and the rollup does not — counted by the view's own query — so a day done
  twice adds nothing, and rows with old timestamps the rollup already counted are not
  counted again. The moment the rollup is complete from moves back a day at a time, so
  queries use what is filled in as it is. It reads the rows once: on two billion lines,
  budget minutes per day.

One run at a time: a lease in `telemetryd_meta` refuses a second. One that died holds it
for half an hour after its last step, or until a run with `--take-over`. `otel_logs` and
`otel_traces` are the Collector's tables, which other writers may own: one laid out
differently is reported, never rebuilt. A statement may take up to
`--statement-timeout` (six hours); `--json` writes each step as an NDJSON event.

Rebuilds it finds on databases created by pre-release builds of ClickHouse support:
`otel_traces_trace_id_ts` from a plain `MergeTree` to the `AggregatingMergeTree` of each
trace's first and last span, and `telemetryd_folds` and `telemetryd_sample_counts` from
monthly to daily partitions. While the sample counts are copied, metric queries over
the hours not yet copied read samples instead of summaries: slower, never wrong.

### Moving from one node

A single node's data directory moves into ClickHouse directly, offline:

```bash
systemctl stop telemetryd                      # the node: one user of a directory at a time
telemetryd clickhouse import --config /etc/telemetryd/clickhouse.toml \
    --from-data-dir /var/lib/telemetryd
```

It opens the directory as the store it was — sealed segments and the write-ahead log —
and writes every record through the same path an instance in front of ClickHouse uses:
the records themselves, not an OTLP rendering of them. Each signal is read oldest first
in windows of time, never by count, so records sharing a timestamp — a scrape writes
thousands of samples at one — are never split across a boundary. After each window,
`telemetryd_meta` records how far it has come, and a run that is interrupted resumes
there; at most the window in flight is written twice. Records older than the
destination's retention are left out, since ClickHouse would drop them at once;
`--since` narrows it further, `--signal` picks signals, `--restart` starts over.
Samples written into hours the fleet has already summarised are summarised again by
whichever instance holds the fold lease next.

Between two running instances, `telemetryd export --url http://old:4319 --to
http://new-balancer:4319 --signal logs` (and `traces`, `metrics`) copies through the
APIs both ends serve — see [Getting data out, and bringing it in](export-and-import.md).
It is not resumable: run it once per range.

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
