---
title: "Security"
weight: 60
description: "Threat model, what telemetryd protects, and what is deliberately out of scope."
---

# Security

telemetryd stores telemetry, and telemetry routinely contains personal data — email
addresses, IP addresses, session identifiers, API tokens in stack traces, request
bodies. The threat model treats stored data as sensitive by default.

- [Threat model](threat-model.md) — what is defended, and against whom
- [Reporting a vulnerability](https://github.com/cboxdk/telemetryd/blob/main/SECURITY.md)

## The short version

**It fails closed on exposure.** telemetryd refuses to start on a non-loopback address
with no token configured — including `0.0.0.0` and `[::]`, which are the binds that
actually expose people. Overridable with `--insecure`, which warns on every start and
shows in `/status`.

**Two independent bearer tokens.** `ingest_token` guards writes, `query_token` guards
reads plus `/status` and `/metrics`. Both accept a list so a token can be rotated
without a rejection window.

**Constant-time comparison.** Tokens are SHA-256'd and compared with
`subtle::ConstantTimeEq`, so token length does not leak through timing.

**Secrets cannot leak through logging.** The `Secret` type has no `Display`, its `Debug`
renders `Secret(<redacted>)`, and its `Serialize` emits `"set"`/`"unset"`. A stray
`{:?}` or a config dump cannot expose a token — enforced by the type, with tests
asserting it.

**Bounded by construction.** Body size, queue depth, cardinality, label lengths and
attribute counts are all capped and configurable. Exceeding one is a structured error
and a counter, never unbounded memory growth driven by a caller.

**Self-metrics use matched routes.** A caller cannot mint unbounded label cardinality
inside telemetryd's own metrics by varying the URL.

## Deliberately out of scope

Absent by decision, not oversight:

- **TLS termination** — use a reverse proxy
- **Per-app authorization** — `app` is a query namespace, not a boundary
- **mTLS, OIDC, user accounts, query audit logging**
- **Encryption at rest** — use full-disk or filesystem encryption
- **Multi-tenant isolation** — separate instances for parties who must not see each
  other's data

## Cryptography

telemetryd implements no cryptographic primitives. Token hashing uses `sha2` and
constant-time comparison uses `subtle`, both from RustCrypto. There is no bespoke
crypto, no custom protocol verification and no signature validation code in the
repository.

## Supply chain

`cargo deny` gates advisories, licenses, banned crates and source registries in CI. A
deterministic CycloneDX SBOM is committed as `sbom.json` and CI fails on drift. There
are **no C-toolchain dependencies at all**, which is both why the static musl builds are
straightforward and one fewer class of vulnerability to inherit.
