---
title: "Storage"
weight: 23
description: "WAL, immutable segments, retention, and exactly what survives a crash."
---

# Storage

## On disk

```
telemetryd-data/
├── VERSION      # format version; startup refuses a mismatch rather than guessing
├── LOCK         # advisory lock — one writer per directory, enforced
├── wal/{logs,traces,metrics}/NNNNNNNN.wal
├── segments/{logs,traces,metrics}/<id>/
│   ├── data.parquet     # rows, sorted by time, in row groups of 65,536
│   ├── manifest.json    # id, time bounds, label index
│   ├── streams.bin      # stream dictionary: each series' labels, bounds and rows
│   ├── folds.bin        # counter summaries (metrics)
│   ├── text.bloom       # trigram index (logs)
│   └── keys.bloom       # exact-match filter (traces)
└── tmp/         # staging; segments become visible via rename(2)
```

Deliberately inspectable. You can `ls` your way around it and read a segment with any
Parquet tool. Deleting an app's data is a directory filter, not a migration.

## Durability

A record is written to the write-ahead log **before** the client is told it was
accepted. The log is the durability boundary and nothing else — not a replication log,
not a query source beyond crash recovery.

Before the answer goes back, the batch is also handed to the kernel. So a crash of the
*process* — a panic, `SIGKILL`, the kernel stopping it at the unit's `MemoryMax` — loses
nothing that was acknowledged; before 0.64.1 it could lose the last 100 ms, which sat in
telemetryd's own write buffer until the next sync.

`wal_sync` defaults to `interval` at 100 ms. **On hard power loss you can lose up to
100 ms of telemetry.** That is a deliberate default — per-write fsync caps ingest at the
device's sync rate — and it is your call to change:

```toml
[storage]
wal_sync = "always"
```

## What survives a crash

A process killed mid-write leaves a partial frame at the end of the log. On restart
telemetryd detects it, truncates to the last good record, and reports it **three ways**:
a `WARN` log, `wal_truncations` in `/status` for the process lifetime, and
`telemetryd_wal_truncations_total`. Records lost to a crash are never silent.

There is one crash window that would produce wrong *answers* rather than an obvious
failure: dying after a segment is published but before the log that fed it is deleted.
A naive replay would store those records twice, and the same log line would simply
appear twice in every query with nothing to explain it. Each sealed segment records the
log sequence it consumed, so replay skips what is already durable.

## Sealing

A buffer is sealed into a segment when either its time window elapses or it exceeds
`max_segment_bytes` — whichever comes first, so worst-case memory is a configured number
rather than a function of traffic.

Sealing is atomic: written to `tmp/`, then moved with `rename(2)`. A reader never sees a
partial segment, and a crash mid-seal leaves only a `tmp/` directory the janitor
removes.

## Counter summaries

Sealing also writes `folds.bin` next to the Parquet file: one 48-byte record per stream
holding the sample count, the first and last timestamp and value, and the increase over
the segment with counter resets applied. It is what lets a `rate` over a long window skip
the segment entirely — see [Query performance](performance.md).

The file is additive and optional. Segments written before it exists have none, an older
telemetryd ignores it, and deleting it costs speed rather than correctness: a background
task writes it again. It is deliberately a separate file rather than part of the
manifest, so that it is read when a query needs the numbers and stays off the heap
otherwise.

## Segment format

Segments carry a format version in their manifest, and a build refuses to open a store
holding a version it does not know rather than read it wrongly.

Format 3, from 0.64.0, moved the stream dictionary out of `manifest.json` into
`streams.bin`: every distinct label name and value written once, each stream a list of
indices into them, zstd-compressed and checksummed. The manifest used to spell out every
stream's labels in full; on a metric segment with twelve thousand series that was five
megabytes of JSON beside six hundred kilobytes of data. The same segment is now 54 KB of
manifest and dictionary together, and loads in half the time.

0.64.0 reads format 2 as before and rewrites those segments as format 3 in the
background, sixteen every thirty seconds, logging each batch. **This makes the upgrade
one-way:** an earlier build refuses to start on a store holding a format 3 segment, with
an error naming the version it found. To move back to an earlier release, restore the
data directory from a backup taken before upgrading — see
[Back up and restore](../cookbook/back-up-and-restore.md).

## Retention

Two limits, one pass:

1. **Age** — a segment is deleted once its *newest* record is past the window, so a
   segment straddling the cutoff is kept rather than taking in-window data with it.
2. **Disk budget** — if usage still exceeds `storage.disk_budget`, the oldest segments
   go, oldest-first across every signal, because the budget is global.

Deleting data that is still inside its retention window is always a `WARN`: it means the
budget and the window are in conflict and one of them is wrong.

Because deletion works in whole segments, the budget is a **soft ceiling with a hard
alarm** — usage can overshoot by up to one segment before the reaper catches up.
Documented rather than rounded off.
