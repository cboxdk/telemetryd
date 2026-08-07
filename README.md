# telemetryd

Single-binary, zero-config observability backend for the [cboxdk](https://github.com/cboxdk)
Laravel ecosystem. Logs, traces, events and metrics in one directory, served back
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

> **Status: M3.** All three signals work end to end. Logs, traces and metrics go in as
> OTLP/HTTP JSON (plus Prometheus `remote_write`), land in Parquet segments, and come
> back out through the Loki, Tempo and Prometheus query APIs — with live tail, and
> retention enforcing both time and a disk budget. Nothing in the contract answers
> `501` any more. See [Milestones](#milestones).

## Quickstart

```bash
telemetryd serve
```

That is the whole setup. It listens on `127.0.0.1:4319`, stores data in
`./telemetryd-data` (or your platform data directory), keeps 7 days of logs and traces
and 30 days of metrics, and stays under a 10 GiB disk budget.

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

## Design

Four documents cover the decisions and the reasoning behind them:

| ADR | Subject |
|-----|---------|
| [ADR-001](docs/adr/0001-storage-architecture.md) | Storage architecture — WAL, segments, the two engines, and four proposed deviations from the original plan |
| [ADR-002](docs/adr/0002-workspace-layout.md) | Workspace layout |
| [ADR-003](docs/adr/0003-configuration-model.md) | Configuration model and layering |
| [ADR-004](docs/adr/0004-auth-and-network-binding.md) | Auth, network binding, and what is deliberately out of scope |
| [ADR-005](docs/adr/0005-compatibility-audit.md) | Why the compatibility subset is derived from the client, not the upstream spec |
| [ADR-006](docs/adr/0006-query-performance-architecture.md) | Query performance: what knowing the queries in advance buys, and where a general engine stays ahead |
| [ADR-007](docs/adr/0007-metrics-reuse-the-record-store.md) | Why metrics reuse the record store instead of the bespoke chunk store ADR-001 planned |

Configuration reference: [docs/CONFIGURATION.md](docs/CONFIGURATION.md).
API contract: [COMPATIBILITY.md](COMPATIBILITY.md) — derived from `laravel-telemetry-ui`'s
actual connector source, not from the upstream API references.

## Security

telemetryd **refuses to start** on a non-loopback address with no token configured —
including `0.0.0.0`, which is the bind that actually exposes people. Telemetry
routinely contains emails, tokens and stack traces, so this fails closed. The error
tells you the three ways to fix it and generates a token to paste.

```toml
[auth]
ingest_token = "file:/run/secrets/ingest"   # guards /v1/*, /api/v1/write
query_token  = ["old-token", "new-token"]   # guards the read APIs; a list rotates
```

Two independent tokens, because app servers push and humans read — different
credentials, different rotation. Comparison is constant-time over SHA-256, so token
length does not leak. Token values never reach a log line, `/status`, or
`telemetryd validate` output; that is enforced by the type, not by discipline.

**Deliberately out of scope in v1**, and stated rather than silently absent:

- **TLS.** Terminate it at a reverse proxy. Shipping TLS means shipping certificate
  lifecycle management, and the deployment story does not need it.
- **Per-app tokens.** The `app` label is a namespace, not a security boundary.
- **mTLS, OIDC, user accounts.** Out of frame for a single-team tool.

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
| `/healthz` | always open | Liveness. Touches nothing, so it cannot fail for the wrong reason. |
| `/status` | query token | Disk usage vs budget, WAL stats, recovery events, limits, retention. |
| `/metrics` | query token | Prometheus exposition of telemetryd's own metrics. |

## Milestones

| | Scope | Status |
|---|---|---|
| **M0** | Workspace, config, data dir, WAL, HTTP surface, self-observability, CI | **done** |
| **M1** | OTLP JSON logs → Parquet segments → Loki `query_range`, labels, series, live tail; retention reaper | **done** |
| **M2** | OTLP traces → Tempo trace-by-id, TraceQL search, tags; query performance architecture | **done** |
| **M3** | Prometheus `remote_write` + OTLP metrics → PromQL subset, Prometheus query APIs | **done** |
| M4 | Freeze `COMPATIBILITY.md` and contract-test every row (audit already done — [ADR-005](docs/adr/0005-compatibility-audit.md)) | next |
| M5 | cargo-dist, install script, brew tap, `.deb`, `service install` | |

## Development

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo deny check
cargo bench -p telemetryd-store          # measures the constants
cargo test -p telemetryd-store --test scale --release   # asserts the asymptotics
```

Performance is held in place from two directions: benchmarks measure the constants,
and `tests/scale.rs` asserts the asymptotics as *segment-open counts* rather than
wall-clock times — so they mean something on a shared CI runner. See
[ADR-006](docs/adr/0006-query-performance-architecture.md).

CI additionally cross-compiles all four release targets
(`{x86_64,aarch64}-unknown-linux-musl`, `{x86_64,aarch64}-apple-darwin`) and asserts
the musl builds are statically linked — "no glibc surprises" is a constraint, not an
aspiration.

## License

Apache-2.0
