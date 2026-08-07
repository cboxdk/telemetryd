---
title: "Testing against telemetryd"
weight: 13
description: "Running a real telemetryd in your own test suite, and what to assert."
---

# Testing against telemetryd

telemetryd is a single binary with no dependencies and zero required configuration,
which makes it unusually easy to run inside a test suite. There is no fake to maintain
because there is nothing to fake — you run the real thing.

## Run one per test run

```bash
telemetryd serve --listen 127.0.0.1:0 --data-dir "$(mktemp -d)"
```

A fresh data directory per run means no state leaks between runs. A data directory has
exactly one writer, so parallel test suites need one directory each — telemetryd
refuses the second opener rather than corrupting anything.

For a faster suite, disable fsync:

```bash
TELEMETRYD_STORAGE_WAL_SYNC=never telemetryd serve
```

That is a test-only setting. It trades the durability guarantee for speed, which is the
right trade when the data is thrown away at the end of the run.

## Wait for readiness

```bash
until curl -sf http://127.0.0.1:4319/healthz >/dev/null; do sleep 0.05; done
```

`/healthz` never touches storage, so it cannot fail for the wrong reason.

## What to assert

Data is queryable the moment ingest returns, so a test does not need to wait for a
flush or force a seal.

The most useful assertion is usually not "did my log arrive" but **"did telemetryd
refuse anything"**:

```bash
curl -s http://127.0.0.1:4319/metrics | grep telemetryd_ingest_rejected_total
```

A non-zero counter there means your instrumentation is sending something telemetryd
would not store — an oversized body, too many labels, a bad metric name — and catching
that in CI is much cheaper than finding it in a dashboard.

Also worth asserting in an integration suite:

- `telemetryd_ingest_timestamps_rescaled_total` is zero — non-zero means your producer
  is sending the wrong time unit and telemetryd is compensating
- `/status` reports `over_budget: false`
- `wal_truncations` is empty, meaning no unclean shutdown lost records
