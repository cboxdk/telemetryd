---
title: "Size the disk budget"
weight: 34
description: "Choosing a budget and retention that do not fight each other."
---

# Size the disk budget

Two limits govern how much disk telemetryd uses, and they can contradict each other:

```toml
[storage]
disk_budget = "10GiB"

[retention]
logs    = "7d"
traces  = "7d"
metrics = "30d"
```

**The budget always wins.** If keeping 7 days would exceed it, telemetryd deletes
in-window data to stay under — and logs a `WARN` every time, because that means one of
the two numbers is wrong.

## Working out what you need

The honest way is to measure rather than estimate. Run for a day at a representative
load and read it off:

```bash
telemetryd status
```

Then multiply by your retention window and add headroom. Logs dominate by an order of
magnitude on almost every deployment; metrics are cheap enough that their longer default
retention costs little.

## When the two are fighting

`/status` tells you directly:

```json
{
  "storage": {
    "over_budget": true,
    "disk_used_ratio": 1.04,
    "retention": { "deleted_by_budget": 12 }
  }
}
```

**`deleted_by_budget` is the field to watch, not `over_budget`.** They rarely show what
the example above shows at the same moment: `over_budget` is true only in the window
between crossing the ceiling and the reaper running, which is seconds. Afterwards it
reads `false` while data is still being deleted on every pass — measured at
`over_budget: false`, `disk_used_ratio: 0.98`, `deleted_by_budget: 7`. A dashboard
built on `over_budget` will almost always look calm.

`deleted_by_budget` above zero means data you asked to keep is being deleted. Either
raise `disk_budget` or shorten retention — leaving it is choosing to silently lose the
oldest data, which is at least a choice worth making on purpose.

Alert on it:

```promql
telemetryd_storage_over_budget == 1
rate(telemetryd_retention_deleted_total{reason="disk_budget"}[1h]) > 0
```

## Why the budget can be exceeded at all

Retention deletes whole segments, so usage overshoots by up to one segment before the
reaper catches up. It is a soft ceiling with a hard alarm, not a hard cap. Leave
headroom of at least a few times `max_segment_bytes` — configuration validation enforces
a floor of 4×, which is a sanity check rather than a recommendation.
