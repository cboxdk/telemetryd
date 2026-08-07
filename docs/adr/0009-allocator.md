---
title: "ADR-009: telemetryd ships its own allocator"
weight: 9
description: "Why the binary sets mimalloc as its global allocator, and the musl measurements that forced it."
---

# ADR-009: telemetryd ships its own allocator

- **Status:** Accepted
- **Date:** 2026-08-07

## Context

telemetryd's primary targets are `x86_64-unknown-linux-musl` and
`aarch64-unknown-linux-musl` — that is what "one static binary, no glibc surprises"
means in practice. Every performance number the project had was measured on macOS.

Measuring the musl build changed the picture completely.

The workload is allocation-heavy by construction. A log record is a handful of small
`String`s and two `BTreeMap`s; ingesting means allocating them, and querying means
allocating them again on the way out. musl's allocator serialises on a single global
lock, so under any concurrency that lock — not the disk, not our own mutexes — is what
the process is waiting on.

Over the same 100k-record store, release build, same machine:

| | musl allocator | mimalloc |
|---|---|---|
| unbounded scan, one thread | 130 ms | **65 ms** |
| unbounded scan, four threads | 432 ms | **61 ms** |

Four threads being **3.3× slower than one** is the diagnostic. That is not our code
failing to scale; that is four threads queueing for `malloc`. With mimalloc the same
scan gets *faster* with more threads, which is what independent segment files should
do.

End to end, driving the real binary over HTTP with four writers and one reader:

| | musl allocator | mimalloc |
|---|---|---|
| ingest, no queries | 61,617 rec/s | **448,193 rec/s** |
| ingest, one concurrent reader | 34,917 rec/s | **438,716 rec/s** |
| query p50 under that load | 1,175 ms | **47 ms** |

A 7.3× throughput difference from a dependency that changes no logic.

## Decision

**The binary sets mimalloc as its global allocator, on every target.**

Not only on musl. It is worth 20–30% on macOS as well (unbounded scan 72 ms → 57 ms,
`limit=100` 1.42 ms → 1.00 ms), and more importantly one allocator everywhere means
the benchmarks, the tests and the shipped binary are all measuring the same program.
A benchmark on a different allocator than the one that ships is a benchmark of
something we do not distribute.

The benchmark harness sets it too, for exactly that reason.

## On taking the dependency

The project's standing rule is that a new third-party **runtime** dependency has to
clear a high bar and be confirmed rather than assumed. Recorded here so the trade is
visible:

- **What it costs.** Two crates — `mimalloc` and `libmimalloc-sys` — plus `cc` as a
  build dependency. It compiles C, so the build needs a C toolchain; the release
  images already have one, and `cargo deny` passes on licences, advisories, bans and
  sources. MIT licensed.
- **Why not write it ourselves.** An allocator is exactly the sort of thing the
  honest-crypto rule is about: widely deployed, heavily tested, and catastrophic to
  get subtly wrong.
- **Why not just avoid the allocations.** Worth doing, and partly done — stream
  interning and late materialisation both exist to allocate less. But the remaining
  allocations are the record contents themselves, and removing those means an arena
  or a bespoke string representation threaded through every crate. That is a large,
  invasive change to buy what a build-time dependency buys outright.
- **Why not only on musl.** A `cfg` on target libc would mean the allocator differs
  between the machine a developer profiles on and the machine that runs it, which is
  how this whole class of problem stays hidden.

## Consequences

**Parallel query scanning is now barely worth anything** (ADR-008). It measured 1.27×
when the allocator was the bottleneck; with that fixed, four workers buy about 7% on
an unbounded scan. It is now **off by default** — spending three extra cores for 7%
on a box that is also accepting writes is not a good default.

That is the more interesting lesson: the parallelism was partly compensating for the
allocator. Fixing the real bottleneck made the elaborate fix mostly unnecessary.

**Memory became predictable.** Resident size on musl is now roughly
`80 MB + 1.3 × storage.max_segment_bytes`, stable across many seal cycles, where the
platform allocator drifted upward by ~180 MB regardless of the configured cap.

**Every performance claim in this repository is a musl claim from here on**, or says
which platform it came from. Darwin numbers were misleading in both directions: they
hid the allocator problem and they overstated what parallel scanning was worth.
