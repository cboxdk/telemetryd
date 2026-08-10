---
title: "Core concepts"
weight: 20
description: "How telemetryd is put together, and why each piece is shaped the way it is."
---

# Core concepts

- [Architecture](architecture.md) — the crates, and the path a record takes
- [Signals](signals.md) — logs, traces and metrics, and what they share
- [Storage](storage.md) — WAL, segments, retention, and what survives a crash
- [Query performance](performance.md) — what knowing the queries in advance buys

The doc comments on each module carry the reasoning in full, including the places the
original plan turned out to be wrong.
