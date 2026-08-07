---
title: "ADR-010: A trigram index for substring search"
weight: 10
description: "Why line filters index trigrams rather than tokens, and what the index costs."
---

# ADR-010: A trigram index for substring search

- **Status:** Accepted
- **Date:** 2026-08-08
- **Resolves:** the trigger set by [ADR-001](0001-storage-architecture.md) D1

## Context

ADR-001 D1 deferred a text index and set a falsifiable condition for revisiting it: a
single-term query over a full 10 GiB dataset exceeding one second. That condition was
never measured until now. It had fired, by a factor of a hundred.

Measured on the musl build over 63 million records (405 MB, 877 segments):

| Line filter over the whole store | |
|---|---|
| a term that matches often | 5 ms |
| **a term that matches nothing** | **3.9 s** |

About ten seconds per gigabyte scanned, so roughly **98 seconds** over a full store.

The shape is specific: a filter that matches *nothing* is the worst case, because
nothing fills the bounded collector, so no segment can be skipped on its time range and
every body column has to be decompressed. A filter that matches plenty is fast — the
collector fills from the newest segment and the rest are pruned.

## Decision

**Index trigrams, not tokens.**

A token index would be smaller and cheaper, and it would be **wrong**. `|=` is a
substring match: `TimeoutError` occurs inside `MyTimeoutError` as a substring but not as
a token, so a token filter would report the segment as not containing it and the line
would silently disappear from the results. Wrong answers that look like empty results
are the worst failure this system could have.

Trigrams do not have that problem. If a record contains a pattern as a substring it
contains every three-byte window of that pattern, so "every trigram of the pattern is
present" is a *necessary* condition, and its negation is a sound reason to skip a
segment unread.

Everything is biased towards false positives, which cost one wasted segment read, and
away from false negatives, which lose data:

- a pattern shorter than three bytes has no trigram and prunes nothing
- only a positive `|=` contributes a required substring — `!=` and `!~` are satisfied by
  lines that lack the pattern, and `|~` is a regular expression whose literal text need
  not appear in what it matches
- a filter that fails to load is treated as absent, and an absent filter prunes nothing,
  which is exactly the behaviour that existed before this index

## What it costs

| | before | after |
|---|---|---|
| term matching nothing, 405 MB | 3.9 s | **6 ms** |
| the same, extrapolated to 10 GiB | 98 s | **0.2 s** |
| ingest, no queries | 448k rec/s | **389k rec/s** |
| disk, sidecar vs Parquet | — | under 1% |

**Thirteen percent of ingest throughput.** That is the real price, and it is paid on
every write whether or not anyone ever searches. It buys a query going from "times out"
to "instant", and 389k records per second is still far beyond what the deployments this
is built for produce.

Most of that cost was avoidable and was avoided: hashing each three-byte window with
byte-wise FNV cost nearly a fifth of throughput, and packing the trigram into an integer
and mixing it with two multiplies brought it back to thirteen percent.

## What it does not fix

**A pattern made of common trigrams still prunes poorly.** Searching for one specific
order number costs about 3 seconds over the same store, because its digit trigrams
appear in nearly every segment. The index answers "maybe" and every segment is read.

Bounding the time range still works and remains the general answer: the same query over
the newest 1% of the store is milliseconds. This is a real limitation, not a rounding
error, and it is why the index is a filter rather than a search engine.

## Revisit if

- searches for high-entropy identifiers become common enough that 3 seconds hurts, in
  which case the answer is a positional index rather than a bigger filter
- the ingest cost starts to matter for a real deployment, in which case the index should
  become optional rather than being made cheaper — the cheap wins are already taken
