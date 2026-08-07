# Build status

The living record of what is built, what is not, and what is deliberately absent. Kept
current so scope and gaps are always auditable without reading the source.

Last updated at **M5**.

## Milestones

| | Scope | Status |
|---|---|---|
| M0 | Workspace, config, data dir, WAL, HTTP surface, self-observability, CI | **done** |
| M1 | OTLP JSON logs → Parquet segments → Loki APIs, live tail, retention | **done** |
| M2 | OTLP traces → Tempo APIs, TraceQL subset; query performance architecture | **done** |
| M3 | `remote_write` + OTLP metrics → PromQL subset, Prometheus APIs | **done** |
| M4 | Compatibility audit and contract tests | **done** — brought forward to after M1 (ADR-005) |
| M5 | Packaging, SBOM, docs, release CI | **done** |

## What works

**Ingest.** OTLP/HTTP JSON for logs, traces and metrics; Prometheus `remote_write`
(snappy + protobuf, decoded by hand — no `protoc` at build time). Per-record rejection
with OTLP `partialSuccess`. Timestamps in the wrong unit are detected and corrected,
and counted so the producer bug stays visible.

**Storage.** Write-ahead log with crash recovery, immutable Parquet segments, per-segment
stream dictionary, Bloom filters for exact-key lookup, retention by age and by a global
disk budget. One engine serves all three signals.

**Query.** LogQL, TraceQL and PromQL subsets, each parsed in full and lowered so an
unsupported construct reports itself by name. Live tail over WebSocket. Label and series
queries answered from metadata with no file I/O.

**Operations.** `/healthz`, `/status`, `/metrics`. `telemetryd validate` prints every
resolved value with its origin. `telemetryd service install` writes a hardened unit.

**Packaging.** Four cross-compiled targets, install script with checksum *and*
signature verification, Homebrew formula, `.deb`, deterministic CycloneDX SBOM with a
CI drift check.

**Release signing.** `SHA256SUMS` carries a keyless Sigstore signature, made by the
release workflow's own OIDC identity and recorded in the public transparency log.
There is no private key to store or rotate. The release job verifies its own signature
before publishing, and `install.sh` verifies it when `cosign` is present — pinning
`--certificate-identity`, without which any valid Sigstore signature would pass.

## Gates

**The toolchain is pinned** in `rust-toolchain.toml` (currently 1.97.1), so the gate
gives the same answer on a laptop as in CI. This was a real defect, not a precaution:
development ran 1.95 while CI resolved `stable` to 1.97.1, and a lint added in between
failed CI on code the local gate had called clean. Bumping the pin is a deliberate
commit that also fixes whatever new lints surface.

Every one of these is green, and CI enforces all of them:

```
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
cargo deny check                      # advisories, licenses, bans, sources
python3 scripts/generate-sbom.py --check
python3 scripts/check-docs.py
python3 scripts/soak.py target/debug/telemetryd   # the binary, over HTTP
```

The soak is the one that runs the product rather than its parts: it starts the real
binary, pushes all three signals at it, queries them back through the Loki, Tempo and
Prometheus APIs, restarts the process and checks the data survived. It exists because
two defects reached a tagged release with the whole unit suite green — a `.deb` whose
systemd unit pointed at the build runner's filesystem, and a TraceQL endpoint that
rejected its own documented `.attribute` syntax. It also runs in the release workflow,
against the exact artifact being published, before anything is uploaded.

Plus: all four release targets cross-compile, and the musl builds are asserted to be
statically linked.

## Known gaps

Named rather than left to be discovered.

**No hosted apt repository.** Deliberate — running one means running signing
infrastructure and keeping it available. `dpkg -i` from a release asset is documented
instead.

**Events are a signal we do not store.** `Signal::Events` exists in the type system and
the retention reaper handles it, but nothing ingests events yet. The record store would
need only a schema.

**Attribute maps are still per-row JSON.** They are genuinely high cardinality, so
interning does not obviously apply. Only paid for rows that survive filtering, so it no
longer dominates a query (ADR-006).

**No text index.** Line filters scan the body column. ADR-001 D1 reserves the segment
slot and sets the falsifiable trigger for adding tantivy: a 7-day single-term query over
a full 10 GiB dataset exceeding one second.

**Limited queries are single-threaded, deliberately** (ADR-008). Unbounded scans use
several workers and gain about 1.27×; queries *with* a limit stay on one thread because
parallelising them measured 60% **slower** — their speed comes from a cutoff that
tightens as it goes, which is a sequential dependency. Eight workers are no faster than
four, so the remaining cost is serial, not a knob set too low.

**No config reload.** Restart to apply. Startup is fast and the WAL is durable, so
SIGHUP reload would be ongoing tax on every future option for little gain (ADR-003).

**`telemetryd bench` is not built.** `cargo bench -p telemetryd-store` covers the same
ground for now; the hidden subcommand exits with a message saying so rather than
pretending.

## Deliberately absent

Not gaps. These are decisions, each with an ADR:

- **TLS** — terminate at a reverse proxy (ADR-004)
- **Multi-tenancy** — `app` is a query namespace, not a security boundary (ADR-004)
- **Clustering, replication, object-store tiering** — single node is the design (ADR-001)
- **Plugins, relabelling rules, write-path transformations** — shape data in the
  instrumentation
- **OTLP/gRPC** — JSON is first-class because that is what the client emits
- **A bespoke metric chunk store** — superseded by ADR-007, with the cost stated

## Where the reasoning lives

`docs/adr/` holds eight decision records. Two are worth reading before changing
anything:

- **ADR-005** — the compatibility subset is derived from the client's source, not the
  upstream API references. Four requirements the published specs do not imply; two were
  already costing correctness.
- **ADR-006** — query performance, with before-and-after measurements and an honest
  account of where a general analytical engine stays ahead.
