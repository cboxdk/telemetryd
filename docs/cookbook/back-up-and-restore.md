---
title: "Back up and restore"
weight: 60
description: "Copy the directory. Why that is safe while telemetryd is running, and how to put it back."
---

# Back up and restore

**Copy the data directory. That is the whole procedure**, and it is safe to do while
telemetryd is running.

```bash
# Back up
tar -czf telemetryd-$(date +%F).tar.gz -C /var/lib telemetryd

# Restore
systemctl stop telemetryd
rm -rf /var/lib/telemetryd
tar -xzf telemetryd-2026-08-08.tar.gz -C /var/lib
chown -R telemetryd:telemetryd /var/lib/telemetryd
systemctl start telemetryd
```

There is no `telemetryd backup` command, because there is nothing for it to do that
`cp` does not already do correctly.

## Why a live copy is safe

Not by luck — it follows from how the store is written, and each part of that was
built for crash recovery rather than for backups. Backups get it for free.

**Sealed segments are immutable.** Once published, a segment's Parquet file and
manifest never change. A copy either catches a segment or does not; it cannot catch
one half-updated.

**Segments are published by atomic rename.** A segment is written to `tmp/` and moved
into place in one step. A copy taken mid-seal sees the staging directory, which has no
manifest, and a directory without a readable manifest is skipped on load and cleaned up
later. It never becomes a corrupt segment.

**The write-ahead log is framed and checksummed.** Every record carries a length and a
CRC. A copy taken mid-append catches a torn final record, which is exactly what a power
cut leaves behind, and the recovery path repairs it on open — the same code, exercised
the same way.

**The lock is held by a process, not by a file.** `LOCK` is copied along with
everything else and does not stop the restored copy from starting, because the lock is
an advisory lock on an open file descriptor rather than the file's existence.

## What it was measured doing

A recursive copy taken while a writer was pushing 4,000-record batches:

| | |
|---|---|
| the copy starts | yes |
| write-ahead log replayed on open | yes, buffered records recovered |
| records kept, of those written when the copy began | all of them |
| corruption reported in the log | none |

## What you lose

**Whatever had not been written when the copy started**, and nothing else. If
`storage.wal_sync` is `interval` — the default — the last sync window's worth of
records may also be missing from the copy, in the same way a power cut would lose them.
Set `wal_sync = "always"` if that matters more than throughput.

## Restoring to a different machine

Nothing in the directory is host-specific: no absolute paths, no machine identifiers.
Copy it anywhere and point `--data-dir` at it.

`VERSION` records the storage format. A restore into a *newer* telemetryd is fine —
formats are read forwards, and a manifest missing a field an older build did not write
falls back to the older behaviour. Restoring into an *older* binary than the one that
wrote the data is not supported, and the format check refuses it rather than reading it
wrongly.

## Verifying a backup

The only test of a backup is restoring it, so restore it somewhere harmless and ask it
what it has:

```bash
telemetryd serve --data-dir ./restored --listen 127.0.0.1:4399 &
curl -s localhost:4399/status | jq '.storage.logs'
```

`segment_rows` and `oldest_record_nanos` tell you what actually survived, and
`segments_unreadable` is non-zero if any segment file is damaged.
