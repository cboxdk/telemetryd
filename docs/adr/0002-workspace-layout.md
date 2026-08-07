# ADR-002: Workspace layout

- **Status:** Accepted
- **Date:** 2026-08-07
- **Milestone:** M0

## Context

The brief specifies `crates/ingest`, `crates/store`, `crates/query`, `bin/telemetryd`,
with "the HTTP surface in one crate, storage engine in another", and a workspace from
day one.

Two crates are needed that the brief does not name explicitly: config and shared signal
types have no home in that list without creating a cycle (`ingest` and `query` both
need them, neither should depend on the other), and "the HTTP surface in one crate"
implies a crate distinct from the CLI binary.

## Decision

```
telemetryd/
├── Cargo.toml                  # workspace root; shared [workspace.dependencies]
├── rust-toolchain.toml         # pinned stable, edition 2024
├── deny.toml                   # cargo-deny: licenses + advisories
├── COMPATIBILITY.md            # the query-API contract (frozen in M4)
├── docs/adr/                   # architecture decision records
├── crates/
│   ├── core/     telemetryd-core     config, errors, signal types, limits, units
│   ├── store/    telemetryd-store    data dir, WAL, segments, retention, metric store
│   ├── ingest/   telemetryd-ingest   OTLP/HTTP JSON + remote_write decode → store
│   ├── query/    telemetryd-query    PromQL / LogQL / Tempo subsets → store
│   └── server/   telemetryd-server   axum router, auth, self-observability
└── bin/telemetryd/                   clap CLI: serve, status, validate, service, …
```

Dependency graph is a strict DAG, no crate depends on a sibling above it:

```
core ← store ← ingest ─┐
              ↖ query ─┴→ server → telemetryd (bin)
```

## Rationale for the two additions

**`telemetryd-core`** exists because config, the error taxonomy (including the
structured "unsupported in telemetryd" error that every subset boundary returns), and
the in-memory signal types are shared by ingest, query, store and server alike. The
alternative — putting them in `store` — would make `store` a dependency of things that
have no business touching storage, and would put the HTTP error type in the storage
engine.

**`telemetryd-server`** is separate from `bin/telemetryd` so the HTTP surface is
independently testable: contract tests (M4) drive the router in-process via
`tower::ServiceExt::oneshot` without binding a port or going through `clap`. The binary
crate stays thin — argument parsing, config resolution, process lifecycle, and the
non-`serve` subcommands.

## Conventions

- Package names are `telemetryd-*`; the binary is `telemetryd`.
- All third-party versions are declared once in `[workspace.dependencies]`; member
  crates use `dep.workspace = true`. One place to bump, one place for `cargo deny` to
  reason about.
- Shared lint configuration lives in `[workspace.lints]` and is inherited by every
  member, so `-D warnings` means the same thing everywhere.
- Integration tests that cross crate boundaries live in `crates/server/tests/`, since
  the router is the composition root.
