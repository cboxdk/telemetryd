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
a throwaway directory is what you wanted anyway.

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

## What this does and does not carry

Logs come back identical: timestamps, bodies, severity, stream labels and per-record
attributes all survive, and that round trip is asserted against a running pair of
instances rather than asserted in prose.

**Traces and metrics are not exported yet.** Not an oversight — a trace API is a search
interface rather than an enumeration one, so "every trace in this window" is not a
question it answers, and metrics come back resampled at whatever `step` you ask for
rather than as the samples that were stored. Both deserve to be built deliberately
instead of shipped as something that quietly returns a subset. Until then the whole data
directory is still the way to move everything at once — see the backup guide.
