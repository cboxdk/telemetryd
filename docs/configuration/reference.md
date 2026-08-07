---
title: "Configuration reference"
weight: 41
description: "Every configuration option, with its default and the reasoning behind it."
---

# Configuration

telemetryd starts with no configuration at all. Everything below has a default;
`telemetryd serve` with no flags is a supported, complete setup.

**Precedence:** defaults → `telemetryd.toml` → environment → CLI flags.
**Env naming:** `TELEMETRYD_<SECTION>_<KEY>`, e.g. `storage.data_dir` →
`TELEMETRYD_STORAGE_DATA_DIR`.

Durations are humantime strings (`500ms`, `30s`, `7d`). Sizes are byte strings
(`64MiB`, `10GiB`). Unknown keys are a startup error. See [ADR-003](../adr/0003-configuration-model.md).

## Full schema with defaults

```toml
[server]
listen           = "127.0.0.1:4319"  # one port: ingest + query + UI APIs
insecure         = false             # allow non-loopback bind with no token
max_body_bytes   = "16MiB"           # per ingest request
request_timeout  = "30s"
shutdown_grace   = "15s"             # drain in-flight requests, then flush WAL

[auth]
# Omit or leave empty to disable auth on that surface.
# Accepts a string or a list of strings (rotation).
# Indirection: "file:/run/secrets/tok" or "env:MY_VAR".
ingest_token = []                    # guards /v1/*, /api/v1/write
query_token  = []                    # guards Prometheus/Loki/Tempo reads, /status, /metrics

[storage]
data_dir          = ""               # "" = auto (see resolution order below)
disk_budget       = "10GiB"          # hard cap across all signals; reaper enforces
segment_duration  = "1h"             # seal window
max_segment_bytes = "256MiB"         # seal early if buffer exceeds this
wal_sync          = "interval"       # always | interval | never
wal_sync_interval = "100ms"          # worst-case loss window when "interval"
compression       = "zstd"           # zstd | snappy | none  (Parquet page compression)
query_parallelism = 1                # threads per query; 1 = off (default), 0 = auto

[retention]
logs    = "7d"
traces  = "7d"
events  = "7d"
metrics = "30d"                      # cheaper per unit time — see ADR-001 D3

[limits]
# Exceeding any of these rejects the record and increments a labelled
# self-metric; it is never a silent drop. See /status for live counts.
max_series              = 100_000    # global metric series cardinality
max_series_per_app      = 20_000
max_labels_per_series   = 60
max_label_name_bytes    = 128
max_label_value_bytes   = 2048
max_log_line_bytes      = "256KiB"
max_attrs_per_record    = 128
ingest_queue_depth      = 8192       # backpressure: full queue → 429 with Retry-After

[log]                                # telemetryd's own logging
level  = "info"                      # trace | debug | info | warn | error
format = "text"                      # text | json



## Data directory resolution

1. `--data-dir` or `TELEMETRYD_STORAGE_DATA_DIR`
2. `./telemetryd-data` **if it already exists**
3. `$XDG_DATA_HOME/telemetryd` — on macOS `~/Library/Application Support/telemetryd`

The resolved path is logged at startup and reported by `/status`. Rationale in
[ADR-003](../adr/0003-configuration-model.md).

## Config file discovery

Without `--config`, first match wins; absence is fine, a malformed file is an error.

1. `./telemetryd.toml`
2. `$XDG_CONFIG_HOME/telemetryd/telemetryd.toml`
3. `/etc/telemetryd/telemetryd.toml`

## Validating

```bash
telemetryd validate
```

Type-checks, runs cross-field rules, and prints every resolved value with its origin
(default / file / env / flag). Tokens print as `set` or `unset`, never their value.
