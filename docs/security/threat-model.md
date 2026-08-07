---
title: "Threat model"
weight: 61
description: "Who telemetryd defends against, what it does not, and where the boundary is."
---

# Threat model

## What is being protected

The telemetry itself. Logs carry request bodies and stack traces; traces carry URLs and
database statements; metrics carry label values derived from user input. Treat a
telemetryd data directory as containing personal data.

## Defended

**An operator accidentally exposing the instance.** The most likely real incident: someone
binds `0.0.0.0` to "just try it" and publishes their logs. telemetryd refuses to start,
and the error names three fixes and generates a token to paste. This is the one case
worth a long error message.

**An unauthenticated network attacker.** With a token configured, every ingest and query
surface requires it. `/healthz` is the only unauthenticated endpoint, and it exposes
nothing.

**Timing attacks on the token.** Compared in constant time over a fixed-width hash.

**Resource exhaustion by a caller.** Body size, queue depth, cardinality, label lengths
and attribute counts are all capped. A caller cannot drive telemetryd out of memory or
mint unbounded cardinality — including in telemetryd's own metrics, which are keyed by
matched route rather than raw path.

**Malformed input.** The parsers are fuzzed with property tests asserting they never
panic and that every failure is attributable to the client rather than reported as an
internal error. That testing found a real reachable panic during development, in the
lexer's handling of a backslash before a multi-byte character.

**Corrupted storage.** Checksums on every log record; a torn tail is repaired and
reported rather than read as valid. A segment with an unreadable manifest is skipped, not
misread. A format-version mismatch refuses to start rather than guessing.

**Two processes on one data directory.** Refused by an advisory lock.

## Not defended

**Anyone with read access to the data directory.** There is no encryption at rest and no
access control below the process. Use filesystem permissions and full-disk encryption.

**A holder of the ingest token writing another app's data.** `app` is a query namespace.
Any ingest-token holder can write any `app` value.

**A holder of the query token reading everything.** There is no per-app read
restriction.

**Network eavesdropping.** telemetryd speaks plain HTTP. Terminate TLS at a proxy.

**A malicious operator.** `--insecure` exists and disables the bind check. It warns
loudly and is visible in `/status`, but it is not prevented.

**Supply-chain compromise of a dependency.** `cargo deny` gates known advisories and the
SBOM makes the graph auditable, but neither stops a novel compromise.

## Trust boundaries

```
  application  ──ingest token──▶  telemetryd  ◀──query token──  UI / operator
                                       │
                                       ▼
                              data directory
                        (filesystem permissions only)
```

The ingest and query boundaries are separate on purpose: app servers push, humans read,
and those are different credentials with different rotation cadences. The boundary
between telemetryd and its data directory is the filesystem's, not telemetryd's.

## Reporting

Through GitHub's [Private Vulnerability Reporting](https://github.com/cboxdk/telemetryd/security/advisories/new).

There is no security mailbox, PGP key or response-time commitment, because advertising a
process we do not operate would be worse than saying so. Reports are read and acted on
by the maintainers on a best-effort basis.
