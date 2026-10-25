---
title: "Operate telemetryd on ClickHouse"
weight: 96
description: "What happens when ClickHouse, a replica or Keeper fails, a disk fills or parts pile up, and how to upgrade, rotate, back up and watch a fleet."
---

# Operate telemetryd on ClickHouse

[Scale out on ClickHouse](scale-out-on-clickhouse.md) sets a fleet up. This page is for
running one: what each part does when something fails, what to do about it, and the
routine work — upgrades, credentials, backups, and watching ClickHouse itself.

One rule underlies all of it: **an instance holds no telemetry.** A batch is
acknowledged once ClickHouse holds it, and refused with `503` and `Retry-After: 30`
when it cannot be stored. So nothing an instance does can lose an acknowledged batch,
and what happens to a refused one is the sender's business — see
[Where refused batches go](#where-refused-batches-go).

## When ClickHouse stops answering

| | What happens |
|---|---|
| Writes | `503` with `Retry-After: 30`, for every signal. Nothing is buffered on the instance |
| Queries | `503`, with a reason that says ClickHouse is not usable; the server log has ClickHouse's own words |
| `/ready` | `503 not ready`, so the load balancer stops sending the instance anything |
| `/healthz` | `ok`: the process is fine, and restarting it changes nothing |
| `/status`, `telemetryd status` | the failures per operation, and the last error with its time |
| A starting instance | stops, naming the URL, the account and the database; a service unit's `Restart=on-failure` tries again every five seconds |
| Folds | paused; queries over the hours not folded read the samples, slower but the same answers |

Readiness asks ClickHouse a `SELECT 1` with the instance's own account and database,
within two seconds, and reuses the answer for a second. A wrong password or a dropped
database is therefore not ready, where ClickHouse's own `/ping` would say it was.

When ClickHouse is back, nothing needs doing: instances become ready within a second,
senders retry, and the next fold catches up on the hours it missed.

### Where refused batches go

What survives an outage is decided by the senders:

- **laravel-telemetry without the spool** exports at the end of each request and does
  not retry: a batch refused with `503` is dropped, by design — telemetry never holds up
  a request.
- **laravel-telemetry with the spool** (`TELEMETRY_OTLP_SPOOL=true` and
  `telemetry:flush --daemon`) keeps refused batches in Redis and retries them,
  honouring `Retry-After`. The spool holds `otlp.spool.max_items` batches (20,000 by
  default) and then drops the oldest: size it for the outage you want to ride out, at
  your batch rate.
- **An OpenTelemetry SDK or Collector** retries `503` with backoff for as long as its
  retry settings say, from memory.
- **A [relay](relay-mode.md) in front of the load balancer** — a telemetryd without
  `[clickhouse]` — keeps what it accepted on its own disk and forwards it when the
  fleet answers again, up to `storage.disk_budget`. It is the only one of these that
  survives a long outage without memory to spare.

## One replica down

Each instance talks to the replica in its `clickhouse.url`. When that replica stops
answering, those instances are not ready and the load balancer sends their traffic to
the others; the replica catches up from Keeper when it returns. Point instances at
different replicas — or at a TCP load balancer over all of them — so that losing one
replica never takes every instance with it.

On several shards, reads and writes across shards go to each shard's first replica
that answers, so a replica down costs the shard nothing while another replica of it
is up. A whole shard down refuses writes that have rows for it and queries that need
it: logs are spread over every shard, so in practice all of them.

## Keeper down

Without Keeper, replicated tables are read-only. Reads keep working; an insert waits
for Keeper until `clickhouse.timeout` (30 seconds) and is then refused with `503` and
`Retry-After`. Readiness only reads, so **instances stay ready**: the load balancer
keeps sending them writes, and each is refused. A sender whose own timeout is shorter —
laravel-telemetry's is three seconds — gives up first and treats it as a failed export. Watch
`telemetryd_clickhouse_requests_total{operation="insert", outcome="error"}`
([alerts](scale-out-on-clickhouse.md#watching-it)), not `/ready`, for this one.

Folds pause, since their lease lives in a replicated table. An instance restarted on
an unchanged database still starts, since it runs no DDL; a first start, or one with
something to create, waits on `ON CLUSTER` statements that do not finish without
Keeper. Restarting fixes nothing here: when Keeper is back, inserts succeed again
within seconds.

Run three Keeper nodes, or five: Keeper needs a majority to answer.

## A disk filling

ClickHouse refuses inserts on a full disk, so writes are `503` as above. Shorten
retention to free space: change `retention.*` in the configuration of **every**
instance (they share the tables, and the last one to start sets the TTL), and send one
of them `SIGHUP`. The new TTL is applied without rewriting stored parts, and ClickHouse
drops whole day partitions past it — but on its own schedule, which can take hours
(`merge_with_ttl_timeout`). To free space now, drop the oldest days yourself:

```sql
SELECT table, partition, formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts WHERE database = 'telemetry' AND active
GROUP BY table, partition ORDER BY partition LIMIT 20;

ALTER TABLE telemetry.otel_logs DROP PARTITION '2026-07-01';   -- ON CLUSTER telemetry, on a cluster
```

Every table but `telemetryd_meta` is partitioned by day. Nearly all the bytes are in
`otel_logs`, `otel_traces` and `telemetryd_samples`, so drop days from those; on
several shards, from their `_local` tables. The rollups and folds of a dropped day stay
until their own TTL, so log volume charts and long `rate`s still show it while its lines
and spans are gone.

With a hot and a cold volume, also check `move_factor`: when the hot volume is fuller
than it, ClickHouse moves the oldest parts to the cold one whatever `cold_after` says.

## Too many parts

ClickHouse refuses an insert into a partition with more active parts than
`parts_to_throw_insert` (3,000 by default), and slows inserts down from
`parts_to_delay_insert`. telemetryd inserts with `async_insert`, which gathers many
small batches into few parts, so this means merges are not keeping up, not that
batches are too small:

```sql
SELECT table, partition, count() AS parts
FROM system.parts WHERE database = 'telemetry' AND active
GROUP BY table, partition ORDER BY parts DESC LIMIT 10;

SELECT table, elapsed, progress, num_parts FROM system.merges;
```

The usual causes are a disk that is too slow for the write rate, too few background
merge threads (`background_pool_size`), or something else writing to the tables in
small inserts without `async_insert`.

## A slow query

Every read telemetryd makes runs with `max_execution_time` set to `clickhouse.timeout`
(30 seconds by default), so ClickHouse stops a query telemetryd has given up on, and a
client that disconnects cancels its query. The expensive shapes are the ones
[How queries stay fast](scale-out-on-clickhouse.md#how-queries-stay-fast) names; to
find which ones you run:

```sql
SELECT event_time, query_duration_ms, read_rows, formatReadableSize(read_bytes), query
FROM system.query_log
WHERE type = 'QueryFinish' AND user = 'telemetryd' AND event_time > now() - INTERVAL 1 HOUR
ORDER BY query_duration_ms DESC LIMIT 10;
```

## Upgrading telemetryd

1. **Replace instances one at a time.** A new version's startup adds what it needs —
   tables, views, columns, indexes — and changes nothing an older instance uses, so old
   and new run side by side while you roll. Wait for `/ready` before the next one.
2. **Then run `telemetryd clickhouse migrate`**, once every instance runs the new
   version: start with `--dry-run`. Not before: an older instance that restarts creates
   what its version expects, including objects `migrate` has just dropped.
3. **Read the release notes** for anything that needs `--backfill` or
   `--materialize-indexes`; both read old data once, and can run while the fleet serves.

To roll back, replace instances with the older version the same way. Before `migrate`
that is always safe; after it, the release notes say whether the older version reads
the newer layout.

## Upgrading ClickHouse

Upgrade one replica at a time, as ClickHouse documents, staying within a release line
telemetryd is tested on (25.8 LTS today). Instances pointed at the replica being
upgraded are not ready while it is down. Run `telemetryd clickhouse check` against the
upgraded server before moving to the next.

## Rotating the password

The connection is made at startup, so a new password takes a restart. Rotate without
an outage through a second account:

1. Create `telemetryd_2` with the same grants as `telemetryd` ([Access](scale-out-on-clickhouse.md#access)).
2. Set `clickhouse.user` and `clickhouse.password` to it on each instance, and restart
   them one at a time.
3. Drop the old account once `system.processes` shows nothing from it.

A `SIGHUP` with a changed `[clickhouse]` is refused, naming each setting that needs a
restart; the running instance keeps its connection.

## Back up and restore

The instances hold nothing a backup needs: back up the database, including
`telemetryd_meta`, which records the TTLs applied, from when each rollup is complete,
the shard count and `migrate`'s progress.

```sql
BACKUP DATABASE telemetry ON CLUSTER telemetry TO S3('https://…/telemetry/2026-10-25', '…', '…');
RESTORE DATABASE telemetry ON CLUSTER telemetry FROM S3('https://…/telemetry/2026-10-25', '…', '…');
```

On a cluster, `ON CLUSTER` backs up every shard once and restores each shard's rows to
the shard they came from. Restore into a cluster with the same number of shards: a row
is on the shard its key picks out of that many, and telemetryd refuses to start on a
database laid out for another count.

The replicated tables' Keeper paths name the database
(`/clickhouse/tables/{shard}/{database}/{table}`), so two databases of one name cannot
share a Keeper. To restore over the live one, drop it with `DROP DATABASE telemetry ON
CLUSTER telemetry SYNC` first, which removes its Keeper entries. Test restores on a
separate cluster with its own Keeper, and point one instance at it: `telemetryd
clickhouse check`, then a few queries you know the answers to.

A backup is a copy of up to ninety days: at sixty billion lines a month, a few
terabytes. ClickHouse's incremental backups (`SETTINGS base_backup = …`) copy only parts
that are new since the last one, and old days never change.

## Setting up the cluster

telemetryd needs, on every ClickHouse server: a cluster in `remote_servers`, `{shard}`
and `{replica}` macros, Keeper, and on several shards `internal_replication`. A two-shard
cluster of two replicas each:

```xml
<clickhouse>
  <zookeeper>
    <node><host>keeper-1</host><port>9181</port></node>
    <node><host>keeper-2</host><port>9181</port></node>
    <node><host>keeper-3</host><port>9181</port></node>
  </zookeeper>
  <remote_servers>
    <telemetry>
      <shard>
        <internal_replication>true</internal_replication>
        <replica><host>ch-1a</host><port>9000</port></replica>
        <replica><host>ch-1b</host><port>9000</port></replica>
      </shard>
      <shard>
        <internal_replication>true</internal_replication>
        <replica><host>ch-2a</host><port>9000</port></replica>
        <replica><host>ch-2b</host><port>9000</port></replica>
      </shard>
    </telemetry>
  </remote_servers>
  <!-- different on each server -->
  <macros><shard>01</shard><replica>ch-1a</replica></macros>
</clickhouse>
```

A cluster of one shard is the same with one `<shard>`. The servers talk to each other
on the native port as the cluster's `<user>` (`default` when unset): give that account
a password, or `<secret>` the cluster, before the servers are reachable from anywhere
else. `scripts/clickhouse/` in the repository has the cluster the test suite runs on, as
Docker Compose files.

## Watching ClickHouse itself

telemetryd's [alerts](scale-out-on-clickhouse.md#watching-it) catch what a sender would
notice: inserts failing or slow. Before that, ClickHouse shows the trouble coming. Its
built-in Prometheus endpoint (`<prometheus>` in its server config) exports what these
need; the same numbers are in its system tables:

| Watch | Where | Trouble when |
|---|---|---|
| Free disk | `system.disks` | under what `cold_after` days or a day of growth needs |
| Parts per partition | `system.parts` | over a few hundred, and rising |
| Replication queue | `system.replicas` (`queue_size`, `absolute_delay`) | a delay of minutes that keeps growing |
| Read-only replicas | `system.replicas` (`is_readonly`) | any — usually Keeper |
| Keeper | `system.zookeeper_connection` | no connection |
| Failed distributed DDL | `system.distributed_ddl_queue` | entries that do not finish |

`telemetryd_app_bytes_estimate` on `/metrics` shows what each app's telemetry costs on
disk, read from `system.parts` at most once a minute.
