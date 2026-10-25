---
title: "Command-line reference"
weight: 42
description: "Every telemetryd command and flag, with its default."
---

# Command-line reference

Every command exits `0` on success and `1` on failure, with the reason on stderr; a
flag it does not know, or a missing one, exits `2`.
`telemetryd <command> --help` prints what is here, for the binary you have.

Commands that read the configuration take `-c, --config <PATH>` (discovered when
omitted) and `--data-dir <DIR>` (`./telemetryd-data` if it exists, else the platform
data directory). Commands that talk to a running instance take `--url` (default
`http://127.0.0.1:4319`) and a token; pass tokens through the environment variable
named, since an argument shows in `ps` and in shell history.

| Command | Does |
|---|---|
| [`serve`](#serve) | run the server |
| [`status`](#status) | print a running instance's status |
| [`query`](#query) | run a LogQL query against a running instance |
| [`export`](#export) | write a time range out as OTLP, or post it to another instance |
| [`import`](#import) | read a time range in, from another instance or an export file |
| [`validate`](#validate) | check a configuration and show where each value came from |
| [`clickhouse schema`](#clickhouse-schema) | print the ClickHouse statements an instance would run |
| [`clickhouse check`](#clickhouse-check) | check the ClickHouse account can do what an instance needs |
| [`clickhouse migrate`](#clickhouse-migrate) | bring a ClickHouse database up to this version's layout |
| [`clickhouse import`](#clickhouse-import) | copy a stopped single node's data directory into ClickHouse |
| [`init`](#init) | write a configuration with generated tokens |
| [`service`](#service) | print, install or remove a service unit |
| [`version`](#version) | print version and build information |

## serve

Runs the server. Works with no arguments at all.

| Flag | Default | |
|---|---|---|
| `-c, --config <PATH>` | discovered | `telemetryd.toml` |
| `--data-dir <DIR>` | see above | where to store data |
| `--listen <ADDR>` | `server.listen` | one port serves ingest, query and the UI APIs |
| `--insecure` | off | allow a non-loopback address with no token configured; refused otherwise |
| `--log-level <LEVEL>` | `log.level` | `trace`, `debug`, `info`, `warn` or `error` |

`SIGHUP` re-reads the configuration: credentials, `retention.*`,
`storage.disk_budget` and `log.level` change in place, and anything else is refused by
name. See the [configuration reference](reference.md).

## status

| Flag | Default | |
|---|---|---|
| `--url <URL>` | `http://127.0.0.1:4319` | the instance |
| `--token <TOKEN>` | `TELEMETRYD_AUTH_ADMIN_TOKEN` | admin token; the query token on an instance with none |
| `--json` | off | the raw `/status` JSON instead of the summary |

## query

`telemetryd query [LOGQL]`. Without a query, everything from the last hour.

| Flag | Default | |
|---|---|---|
| `--since <DURATION>` | `1h` | how far back |
| `--limit <N>` | `100` | most recent first, up to this many |
| `--forward` | off | oldest first |
| `--output <text\|json>` | `text` | `json` is one object per line |
| `--url <URL>` | `http://127.0.0.1:4319` | the instance |
| `--token <TOKEN>` | `TELEMETRYD_AUTH_QUERY_TOKEN` | query token |

A metric query (`sum by (app) (count_over_time({level="error"}[1h]))`) is answered at
now, one line per series. See [Query from the shell](../cookbook/query-from-the-shell.md).

## export

| Flag | Default | |
|---|---|---|
| `--since <DURATION>` | `1h` | how far back |
| `--query <LOGQL>` | `{app=~".+"}` | what to export; a selector must require a value |
| `--signal <logs\|traces\|metrics>` | `logs` | anything but `logs` needs the source to be a telemetryd |
| `--url <URL>` | `http://127.0.0.1:4319` | the instance to read from |
| `--token <TOKEN>` | `TELEMETRYD_AUTH_QUERY_TOKEN` | token for `--url` |
| `--output <PATH>` | stdout | where to write |
| `--to <URL>` | — | post to another instance instead, through `/api/v1/export`: all three signals at full fidelity |
| `--to-token <TOKEN>` | `TELEMETRYD_AUTH_INGEST_TOKEN` | ingest token for `--to` |
| `--progress <auto\|tty\|plain\|json\|none>` | `auto` | a live meter on a terminal, lines otherwise; `json` is NDJSON on stderr |

See [Getting data out, and bringing it in](../cookbook/export-and-import.md).

## import

| Flag | Default | |
|---|---|---|
| `--from <URL>` | — | a Loki-compatible API to read from |
| `--file <PATH>` | — | a file `telemetryd export` wrote; `-` is stdin |
| `--since <DURATION>` | `1h` | how far back |
| `--query <LOGQL>` | `{app=~".+"}` | what to import |
| `--from-token <TOKEN>` | — | token for `--from` |
| `--signal <logs\|traces\|metrics>` | `logs` | metrics come through Prometheus remote read, with their own timestamps |
| `--url <URL>` | `http://127.0.0.1:4319` | the instance to write into |
| `--token <TOKEN>` | `TELEMETRYD_AUTH_INGEST_TOKEN` | ingest token for `--url` |
| `--progress <…>` | `auto` | as for `export` |
| `--allow-expiring` | off | import a range older than the destination keeps; refused otherwise, since it would be deleted |

## validate

Takes `--config` and `--data-dir`. Type-checks the configuration, runs the cross-field
rules, and prints every resolved value with the layer it came from.

## clickhouse schema

Prints every statement an instance would run — database, tables, views, TTLs, in order,
each idempotent — for a DBA to review or run.

| Flag | Default | |
|---|---|---|
| `--shards <auto\|one\|many>` | `auto` | `auto` asks the server how many shards `clickhouse.cluster` has; `one` (replicated tables) and `many` (each shard's tables, `Distributed` over them) print without connecting |

## clickhouse check

Takes `--config` and `--data-dir`. Connects with the configured account and checks,
changing nothing: the password can be read, the server answers, the database and every
table and view are there or can be made, `system.parts` is readable, and the cluster and
storage policy named exist.

## clickhouse migrate

Brings a database an older telemetryd created up to this version's layout while
instances keep writing. Every step can be repeated, and an interrupted run continues
where it stopped. See [Upgrading a deployment](../cookbook/scale-out-on-clickhouse.md#upgrading-a-deployment).

| Flag | Default | |
|---|---|---|
| `--dry-run` | off | say what would be done; change nothing |
| `--backfill` | off | fill in rollups created after the rows they summarise; reads those rows once |
| `--materialize-indexes` | off | index partitions written before an index existed, then drop the indexes it replaces |
| `--keep-old` | off | keep a rebuilt table's old copy under its temporary name |
| `--take-over` | off | go ahead although another run holds the lease — for a run that died, never one still running |
| `--statement-timeout <DURATION>` | `6h` | how long one statement may take |
| `--json` | off | NDJSON events on stdout |

## clickhouse import

Copies a stopped single node's data directory — sealed segments and the write-ahead
log — into ClickHouse, writing records as an instance would. Resumes where an
interrupted run stopped. See [Moving from one node](../cookbook/scale-out-on-clickhouse.md#moving-from-one-node).

| Flag | Default | |
|---|---|---|
| `--from-data-dir <DIR>` | required | the single node's data directory; it must be stopped |
| `--signal <logs\|traces\|metrics>` | all three | which signals |
| `--since <DURATION>` | the destination's retention | only records this recent |
| `--batch <N>` | `50000` | records per insert |
| `--restart` | off | start from the beginning |
| `--json` | off | NDJSON events on stdout |

## init

| Flag | Default | |
|---|---|---|
| `--dir <DIR>` | `/etc/telemetryd` | where the configuration and token files go |
| `--force` | off | overwrite an existing configuration, which rotates every token |

## service

`telemetryd service print`, `install` and `uninstall`. `print` and `install` take
`--user <USER>` (default `telemetryd`), the system user the service runs as; it is
created only when it is the default and does not exist. See
[Run it as a service](../cookbook/run-as-a-service.md).

## version

`--check` also asks GitHub whether a newer release exists — the only thing telemetryd
ever contacts outside your infrastructure, and only when asked.
