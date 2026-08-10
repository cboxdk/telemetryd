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
`TELEMETRYD_STORAGE_DATA_DIR`. Nested sections keep the same shape, so
`auth.oidc.issuer` is `TELEMETRYD_AUTH_OIDC_ISSUER`. Every scalar key is covered,
which is what makes a container configurable with no file at all — see
[Docker](../getting-started/docker.md).

The one exception is `[[relay.client]]`, a list of tables with no sensible flat
spelling. Mount a config file for it; its values are credentials, so that is where they
belong. The variable names are a table rather than a mechanical transformation, so a
misspelled `TELEMETRYD_*` variable is a startup error instead of a setting that silently
does nothing.

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
# Accept OIDC access tokens alongside the static ones. Unset = static only.
# Tokens are validated locally against the issuer's published keys; telemetryd
# never asks the provider about a token. See ADR-011.
issuer           = ""                # https:// (or loopback); the only required key
audience         = ""                # "" = accept the issuer's own value
jwks_url         = ""                # "" = {issuer}/.well-known/jwks.json
scope_claim      = "scope"           # the claim carrying granted scopes
scope_write      = "telemetry:write"
scope_read       = "telemetry:read"
scope_admin      = "telemetry:admin"
refresh_interval = "1h"              # how often the key set is refetched
clock_skew       = "1m"              # allowance on exp/nbf

[server.tls]
# Terminate TLS here rather than at a proxy. Unset = plain HTTP.
cert_file   = ""                     # PEM chain, leaf first
key_file    = ""                     # PEM key, unencrypted
self_signed = ""                     # or: the hostnames to generate a certificate for

[tls]
# Trust for connections telemetryd *makes* — the OIDC key fetch, relay shipping,
# transfer. Inbound TLS is still a reverse proxy's job (ADR-004).
ca_file = ""                         # "" = the roots compiled into the binary

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

## Serving HTTPS

Unset, telemetryd speaks plain HTTP and something in front holds the certificate. That
is still the right shape at a public edge, where an ingress already terminates TLS for
several services. It is the wrong shape on an internal network with no proxy and nobody
about to add one — and bearer tokens and log lines in clear are no better internally
than externally.

```toml
[server.tls]
cert_file = "/etc/telemetryd/tls/server.pem"   # chain, leaf first
key_file  = "/etc/telemetryd/tls/server.key"   # unencrypted
```

Both or neither: half a TLS configuration would leave telemetryd serving plain HTTP
while looking configured for it, so it is refused at startup. The key must be
unencrypted, because there is nobody to prompt for a passphrase when a service manager
starts the process.

**Bring a certificate rather than generating one.** A self-signed certificate the
clients do not trust gives you encryption without authentication: it stops passive
capture and not an active attacker, which is the threat that motivates encrypting an
internal network in the first place. In practice it ends with `insecure_skip_verify` on
every SDK, which looks secure and is not. Issue from your internal CA — and point the
clients at it. telemetryd's own outbound side does exactly that with
[`tls.ca_file`](#trusting-a-private-ca), so a relay and its upstream can share one
authority.

Renewal is the issuer's job, not telemetryd's. Replace the files and restart; the
certificate is read at startup.

### Generating one, when you have no CA to hand

```bash
TELEMETRYD_SERVER_TLS_SELF_SIGNED=telemetry.internal telemetryd serve
```

One variable. It generates a certificate for those names into `<data_dir>/tls/` on
first start, keeps the key at `0600`, and reuses it on every restart — a server that
minted a fresh certificate each time would look, to anything that pins or caches, like
an attacker.

The value is the **names clients will connect as**, comma-separated, rather than a
plain on/off switch: a certificate valid only for `localhost` is useless the moment a
client uses a hostname, and that failure arrives as an opaque verification error long
after the setting that caused it. `localhost`, `127.0.0.1` and `::1` are always
included.

**Be clear-eyed about what it buys.** It encrypts, so passive capture — a network tap, a
mirrored port, another host on the same network — stops working, and that is a real
threat worth closing. It does *not* authenticate: clients cannot tell the certificate is
yours, so they must be configured to skip verification, and an active attacker who can
intercept the connection is no worse off than with plain HTTP.

The lasting cost is that instruction. `insecure_skip_verify` tends to stay in client
configuration long after a proper certificate is installed, and then the deployment is
still interceptable while looking encrypted. So this is the answer while you have
nothing better, not the destination. It cannot be combined with `cert_file`/`key_file`;
telemetryd refuses rather than guessing which you meant.

## Checking what is on

`/status` carries `"tls"` — `off`, `certificate` or `self-signed` — and the same value
rides on the `telemetryd_build_info` metric as a label:

```
telemetryd_build_info{tls="self-signed",version="0.24.2"} 1
```

`self-signed` is reported separately from `certificate` because they are not the same
promise: one encrypts, the other also authenticates. And `off` is a value rather than an
absent series, so an alert can say "no instance should be serving plaintext" without
having to interpret a missing metric.

## Trusting a private CA

`tls.ca_file` takes a PEM bundle and is what you set when the issuer or the relay
upstream is behind an internal authority — the deployment
[ADR-013](../adr/0013-relay-mode.md) describes, where the upstream is your own
infrastructure rather than a public host.

```toml
[tls]
ca_file = "/etc/ssl/certs/internal-ca.pem"
```

**It replaces the built-in roots rather than adding to them.** Trusting exactly the
authority that signs your internal hosts is tighter than trusting it *and* every public
CA, and an instance configured this way is usually talking only to internal
infrastructure. If you genuinely need both, the file is a bundle — concatenate the
public roots you want alongside your own.

A file that cannot be read, or that contains no certificates, stops startup. Falling
back to the public roots would hand an operator who asked for a private CA the opposite
of what they configured, which is worse than not starting.

For CLI commands, which read no configuration file, the same value comes from
`TELEMETRYD_TLS_CA_FILE`.

The alternative is the `platform-verifier` build feature, which verifies against the
host's own trust store instead. It is not the default because that store is empty in
most containers.

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
