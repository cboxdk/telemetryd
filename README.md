# telemetryd

Single-binary, zero-config observability backend for the [cboxdk](https://github.com/cboxdk)
Laravel ecosystem. Logs, traces and metrics in one directory, served back
through the Loki, Tempo and Prometheus APIs that `laravel-telemetry-ui` already speaks.

**Your telemetry never leaves your infrastructure: one binary, one port, one data
directory, no sidecars.**

telemetryd is scoped deliberately: one team, a handful of apps, one VPS or a dev
laptop. It targets that case completely rather than scaling to a large fleet.

**Single-node is a design choice, not a limitation we plan to remove.** It is what
lets us delete sharding, consensus, object-store tiering and a query scheduler — and
ship one 2.6 MB binary instead of a stack of services to operate. If you outgrow one
node, you have outgrown telemetryd, and we would rather say so than pretend
otherwise.

> **Status: feature complete.** All three signals go in as OTLP/HTTP JSON (plus
> Prometheus `remote_write`), land in Parquet segments, and come back out through the
> Loki, Tempo and Prometheus query APIs — with live tail, and retention enforcing both
> time and a disk budget. Nothing in the contract answers `501`.
>
> [BUILD-STATUS.md](BUILD-STATUS.md) is the honest, current list of what works, what
> the known gaps are, and what is deliberately absent.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/cboxdk/telemetryd/main/install.sh | sh
```

Detects the platform, verifies the release signature when `cosign` is present and the
checksum always, and refuses to install on a mismatch. On macOS:

```bash
brew install cboxdk/tap/telemetryd
```

A `.deb` and the raw binaries are on the [releases page](https://github.com/cboxdk/telemetryd/releases),
and there is a container image at `ghcr.io/cboxdk/telemetryd` — see [Docker](docs/getting-started/docker.md).

On a server, write a configuration with generated tokens and install the service unit —
a hardened systemd unit on Linux, a launchd plist on macOS:

```bash
sudo telemetryd init           # config + one token per surface, printed once
telemetryd service print       # read the unit before installing it
sudo telemetryd service install
```

Putting it on a server behind TLS is [a page of its own](docs/cookbook/deploy-on-forge.md).

## Quickstart

```bash
telemetryd serve
```

That is the whole setup. It listens on `127.0.0.1:4319`, stores data in
`./telemetryd-data` (or your platform data directory), keeps 7 days of logs and traces
and 30 days of metrics, and stays under a 10 GiB disk budget.

Or as a container, with the same amount of configuration:

```bash
docker run -d -p 4319:4319 -v telemetryd-data:/var/lib/telemetryd \
  ghcr.io/cboxdk/telemetryd:latest
```

A container binds `0.0.0.0`, which telemetryd refuses to do unauthenticated, so the
image generates tokens on first start and prints them once — `docker logs` has them.
Every setting is an environment variable, so nothing needs a file. See
[Docker](docs/getting-started/docker.md).

```bash
telemetryd status
```

Point `cboxdk/laravel-telemetry` at `http://127.0.0.1:4319` and query it back:

```bash
# logs
curl -G http://127.0.0.1:4319/loki/api/v1/query_range \
  --data-urlencode 'query={app="checkout", level="error"} |= "declined"'

# metrics
curl -G http://127.0.0.1:4319/api/v1/query \
  --data-urlencode 'query=rate(http_requests_total[5m])'

# a trace
curl http://127.0.0.1:4319/api/traces/4bf92f3577b34da6a3ce929d0e0e4736
```

Point all three of `laravel-telemetry-ui`'s connectors — Loki, Tempo and
Prometheus — at that one base URL.

## Documentation

Full documentation is in [docs/](docs/index.md) — start with the
[quickstart](docs/quickstart.md).

| | |
|---|---|
| [Getting started](docs/getting-started/_index.md) | Installing, sending data, testing |
| [Core concepts](docs/core-concepts/_index.md) | Architecture, signals, storage, performance |
| [Cookbook](docs/cookbook/_index.md) | Running as a service, exposing it safely, sizing the budget |
| [Configuration](docs/configuration/reference.md) | Every option, with defaults |
| [Extension points](docs/extension-points/_index.md) | What is adjustable, and what is not |
| [Security](docs/security/_index.md) | Threat model and honest scope |

## Design

The reasoning lives next to the code it explains: why a decision was made, and where the
first attempt was wrong, are in the doc comments of the module that carries it. What is
built, what is not, and what is deliberately absent is in
[BUILD-STATUS.md](BUILD-STATUS.md).

Pointing an AI agent at this? [llms.txt](llms.txt) is the short version, and the
installation, sending-data and service pages each carry a copy-paste brief. The part
worth reading first is which query constructs are refused — an agent that knows Loki and
Prometheus will otherwise write valid expressions this does not run.

API contract: [COMPATIBILITY.md](COMPATIBILITY.md) — derived from `laravel-telemetry-ui`'s
actual connector source, not from the upstream API references.

## Your data can leave

A self-hosted tool that can only be read through its own interface has quietly become a
place data goes in. Every signal moves in every direction, as OTLP — the format every
other backend already accepts.

```bash
telemetryd export --since 24h > dump.ndjson          # to a file
telemetryd export --to http://new-host:4319          # straight to another instance
telemetryd import --from https://logs.internal       # in from somewhere else
```

Between two telemetryds, records are read straight from the store rather than
re-derived from a query language, so all three signals come across as stored. From a
foreign backend it goes through the read APIs each speaks — logs, traces, and metrics
through Prometheus remote read, which is the only one of the three that returns stored
samples rather than points on a `step` grid.

`--progress` writes to stderr while the data goes to stdout, so `export | gzip` keeps
its meter. See the [transfer guide](docs/cookbook/export-and-import.md).

## Security

telemetryd **refuses to start** on a non-loopback address with no token configured —
including `0.0.0.0`, which is the bind that actually exposes people. Telemetry
routinely contains emails, tokens and stack traces, so this fails closed. The error
tells you the three ways to fix it and generates a token to paste.

```toml
[auth]
ingest_token = "file:/run/secrets/ingest"   # guards /v1/*, /api/v1/write
query_token  = ["old-token", "new-token"]   # guards the read APIs; a list rotates
admin_token  = "env:TELEMETRYD_ADMIN"       # guards /status and /metrics
```

Three independent tokens, because app servers push, humans read, and dashboards
scrape — different credentials, different rotation. They are not a hierarchy: an admin
token does not grant reads. Comparison is constant-time over SHA-256, so token length
does not leak. Token values never reach a log line, `/status`, or `telemetryd validate`
output; that is enforced by the type, not by discipline.

Point it at Cbox ID and it will accept access tokens too, validated against the
issuer's published keys rather than by calling the provider — so an identity provider
that is down never stops you reading the logs that would explain why:

```toml
[auth.oidc]
issuer = "https://acme.cboxid.com"
```

Scopes map to the same three roles. See the [single sign-on
guide](docs/cookbook/single-sign-on.md).

**Deliberately out of scope in v1**, and stated rather than silently absent:

- **Automatic certificate issuance.** telemetryd *can* terminate TLS — set
  `server.tls.cert_file` and `key_file` — but it does not obtain or renew certificates.
  Bring one from your CA, from certbot, or from whatever issues them where you run. A
  proxy in front remains a fine answer at a public edge, where one usually exists
  already; the built-in path is for internal deployments that have nowhere to put one.
- **Per-app tokens.** The `app` label is a namespace, not a security boundary.
- **mTLS, user accounts.** Out of frame for a single-team tool.

## Durability

The write-ahead log defaults to `wal_sync = "interval"` at 100 ms. **On hard power
loss you can lose up to 100 ms of telemetry.** That is a deliberate default — per-write
fsync would cap ingest at the device's sync rate — but it is your call:

```toml
[storage]
wal_sync = "always"   # fsync every batch
```

A crash that tears the log tail is detected at startup, repaired, and reported three
ways: a `WARN` log, `wal_truncations` in `/status`, and
`telemetryd_wal_truncations_total`. Lost records are never silent.

The same principle covers limits generally. When a cap is hit — cardinality, body
size, queue depth — telemetryd rejects loudly with a labelled counter and a structured
error, rather than dropping data quietly.

## Self-observability

| Endpoint | Auth | Purpose |
|----------|------|---------|
| `/` | always open | What this server is and which routes it answers. Names no version and no contents, because it is open. |
| `/healthz` | always open | Liveness. Touches nothing, so it cannot fail for the wrong reason. |
| `/status` | admin token | Disk usage vs budget, WAL stats, recovery events, limits, retention. Falls back to the query token when no admin token is set. |
| `/metrics` | admin token | Prometheus exposition of telemetryd's own metrics. |

## Development

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo deny check                          # advisories, licenses, bans, sources
python3 scripts/generate-sbom.py --check  # SBOM is not stale
python3 scripts/check-docs.py             # docs structure, frontmatter, links

cargo bench -p telemetryd-store                         # measures the constants
cargo test -p telemetryd-store --test scale --release   # asserts the asymptotics
```

Performance is held in place from two directions: benchmarks measure the constants,
and `tests/scale.rs` asserts the asymptotics as *segment-open counts* rather than
wall-clock times — so they mean something on a shared CI runner. See


CI additionally cross-compiles all four release targets
(`{x86_64,aarch64}-unknown-linux-musl`, `{x86_64,aarch64}-apple-darwin`) and asserts
the musl builds are statically linked — "no glibc surprises" is a constraint, not an
aspiration.

## License

MIT
