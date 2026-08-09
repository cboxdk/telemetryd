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
max_body_bytes   = "16MiB"           # per ingest request, before *and* after decompression
request_timeout  = "30s"
shutdown_grace   = "15s"             # drain in-flight requests, then flush WAL

[auth]
# Omit or leave empty to disable auth on that surface.
# Accepts a string or a list of strings (rotation).
# Indirection: "file:/run/secrets/tok" or "env:MY_VAR".
ingest_token = []                    # guards /v1/*, /api/v1/write
query_token  = []                    # guards the Prometheus/Loki/Tempo read APIs
admin_token  = []                    # guards /status and /metrics

[auth.oidc]
# Accept Cbox ID access tokens alongside the static ones. Unset = static only.
# Tokens are validated locally against the issuer's published keys; telemetryd
# never asks the provider about a token. See ADR-011.
issuer           = ""                # https:// (or loopback); the only required key
audience         = ""                # "" = accept the issuer's own value
scope_write      = "telemetry:write"
scope_read       = "telemetry:read"
scope_admin      = "telemetry:admin"
refresh_interval = "1h"              # how often the key set is refetched
clock_skew       = "1m"              # allowance on exp/nbf

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

[relay]
# Forward what this instance accepts to a central one, as a safe front door for
# clients you do not trust (ADR-013). Unset = store locally and send nothing on.
# upstream  = "https://telemetry.internal"   # https, or loopback for testing
# token     = "file:/run/secrets/upstream"   # the credential clients must never hold
trust_client_identity = false        # take `app` from the payload instead of the credential
when_full             = "drop_oldest"  # drop_oldest | reject, when undelivered data fills the budget
interval              = "30s"        # how often to look for sealed segments to ship
max_queue_share       = 0.5          # most of limits.ingest_queue_depth one client may hold; 1.0 = off
max_request_bytes     = "4MiB"       # segments are split into requests this size; a 413 halves and retries

# One entry per client application. The token identifies it; the app is stamped onto
# every record it sends, replacing whatever the payload claimed.
# [[relay.client]]
# app   = "mobile-ios"
# token = "file:/run/secrets/mobile-ios"

[log]                                # telemetryd's own logging
level  = "info"                      # trace | debug | info | warn | error
format = "text"                      # text | json



## What `auth.oidc.issuer` changes about the other keys

**Setting it guards every surface, including ones whose static token is empty.**

An empty `query_token` means "unguarded" only while nothing else guards that surface.
Once Cbox ID is on, a request to the read API must present *something* valid — and a
token carrying `telemetry:admin` is not valid there, because the scopes are not a
hierarchy.

This is the safe direction, but it is a behaviour change: a surface you were leaving
open on purpose stops being open the moment you set an issuer. Decide per surface,
before enabling it, and check with the tokens you expect people to hold.

Static tokens are checked first and in constant time, so turning this on costs a purely
static deployment nothing.

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
