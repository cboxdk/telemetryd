---
title: "Decision records"
weight: 70
description: "Why telemetryd is built the way it is, including where the original plan was wrong."
---

# Decision records

Each record states the context, the decision, and the consequences — including the cost.
Several document places the original plan turned out to be wrong; those are kept rather
than quietly corrected, because the reasoning is the useful part.

| | |
|---|---|
| [ADR-001](0001-storage-architecture.md) | Storage architecture, and four deviations from the original plan |
| [ADR-002](0002-workspace-layout.md) | Workspace layout and the crate DAG |
| [ADR-003](0003-configuration-model.md) | Configuration layering |
| [ADR-004](0004-auth-and-network-binding.md) | Auth, network binding, and what is out of scope |
| [ADR-005](0005-compatibility-audit.md) | Why the API subset comes from the client, not the upstream spec |
| [ADR-006](0006-query-performance-architecture.md) | Query performance, measured |
| [ADR-007](0007-metrics-reuse-the-record-store.md) | Why metrics reuse the record store |
| [ADR-008](0008-parallel-segment-scanning.md) | Parallel scanning, and why only for unbounded queries |
| [ADR-009](0009-allocator.md) | Why the binary ships mimalloc, and the musl numbers that forced it |
| [ADR-010](0010-trigram-index.md) | Trigrams for substring search, and why not tokens |
| [ADR-011](0011-cbox-id-integration.md) | **Proposed** — validating Cbox ID tokens locally, never by asking the provider |

## Two worth reading first

**[ADR-005](0005-compatibility-audit.md)** — the compatibility subset was derived by
reading the client's connector source rather than the upstream API references. That
turned up four requirements the published specs do not imply, two of which were already
costing correctness in shipped code.

**[ADR-006](0006-query-performance-architecture.md)** — what knowing the query shapes in
advance actually buys, with before-and-after measurements, and an honest account of
where a general analytical engine stays ahead.
