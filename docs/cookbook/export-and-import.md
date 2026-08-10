---
title: "Getting data out, and bringing it in"
weight: 85
description: "Move a time range between instances, onto a laptop, or off telemetryd entirely — as OTLP, through APIs both ends already speak."
---

# Getting data out, and bringing it in

```bash
telemetryd export --since 24h > dump.ndjson
telemetryd import --file dump.ndjson --url http://other-host:4319
```

Neither command reads the other side's storage. Both work through the Loki API
telemetryd serves and the OTLP endpoint it accepts, so the same pair covers moving to a
new instance, pulling production onto a laptop, and leaving telemetryd altogether.

## Moving between two instances

```bash
telemetryd export --url http://old:4319 --to http://new:4319 --signal logs
telemetryd export --url http://old:4319 --to http://new:4319 --signal traces
telemetryd export --url http://old:4319 --to http://new:4319 --signal metrics
```

No file in between, and the highest fidelity available: records are read through
telemetryd's own export endpoint rather than re-derived from a query language, so all
three signals come across as they were stored.

This is the direction to use when both ends are telemetryd. `import --from` is for when
the source is not.

## Pulling an incident onto your machine

The case this gets used for weekly. Point `--from` at production and write into a local
instance you can throw away afterwards:

```bash
telemetryd import --from https://logs.internal --from-token "$PROD_TOKEN" \
  --since 6h --query '{app="checkout"}'
```

Query it offline as often as you like, without putting load on production and without
production being reachable.

**Import into a fresh data directory.** telemetryd is append-only with no deduplication,
so importing an overlapping range twice stores the records twice. For the debugging case
a throwaway directory is what you wanted anyway — and it is also how you tell two sources
apart, which is why there is no `--label` for stamping provenance. Where you do want them
in one store, `deployment_environment` is already a stream label; if your telemetry does
not carry one, that is worth fixing at the source rather than at the import.

## Retention will refuse before it deletes

Ingest applies no age limit, so old records go in perfectly well — and then the reaper
removes anything past `retention.logs`, possibly while the import is still running.

`import` therefore checks the destination's retention against the range and **refuses**
rather than producing an import that appears to succeed and silently leaves nothing
behind. Raise `retention.logs` there, or say `--allow-expiring` if that really is what
you meant.

## The format

One OTLP request per line. NDJSON streams, survives being cut in half, and is what
every other backend already ingests — so `dump.ndjson` is not a telemetryd file, it is
a file anything speaking OTLP can read.

Which is also why `import --file` is one line at a time rather than one parse: a 4 GB
dump should not need 4 GB of memory.

## Progress

**stdout is data, stderr is progress.** That is what lets a live meter run while the
output goes somewhere else:

```bash
telemetryd export --since 7d | gzip > week.ndjson.gz
```

| `--progress` | |
|---|---|
| `auto` (default) | a live meter on a terminal, periodic lines when it is not one |
| `tty` | force the meter |
| `plain` | one line every few seconds — a log file, not a redraw |
| `json` | NDJSON events on stderr, for a program that is watching |
| `none` | silence |

`auto` is what makes this behave under `systemd`, in CI and in a pipe without anyone
passing a flag.

The `json` form ends with a `done` or `failed` event, and both carry
`high_water_nanos` — the oldest record reached. A transfer that dies at 60% tells you
exactly where to start again.

## All three signals

```bash
telemetryd export --signal traces  --since 24h > traces.ndjson
telemetryd export --signal metrics --since 24h > metrics.ndjson
```

There are two paths underneath, and which one runs depends on what you asked for.

**Against telemetryd**, export reads records straight from the store and encodes them —
no query language in the middle, so what comes out is what was stored, and it is one
request per window rather than one per trace.

**Against a foreign backend**, or when you pass a `--query` to take a subset, it goes
through the read APIs instead:

```bash
telemetryd import --from https://tempo.internal --signal traces --since 6h
```

Traces cost N+1 requests per window — search enumerates the window, then each id is
fetched for its spans — so this is much slower than the native path. Correct, though,
which is what matters when the source is not a telemetryd.

**Metrics come through remote read**, not a range query:

```bash
telemetryd import --from https://prometheus.internal --signal metrics --since 24h
```

A range query would return points on the `step` grid rather than the samples that were
stored — shrinking the step gives more points from the same samples, not more fidelity.
Remote read returns the stored samples with their own timestamps.

It prints a warning every run, and means it: Prometheus documents remote read as outside
its stable API, "subject to change even between non-major version releases". If a source
upgrade breaks this, have it send OTLP to telemetryd instead — that path is stable and
already works.

An export file names its signal in every line, so `import --file` routes each line to the
right endpoint by content. A file holding all three just works, and pointing a traces
file at the wrong flag is not a mistake you can make.
