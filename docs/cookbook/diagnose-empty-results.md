---
title: "Diagnose a query that returns nothing"
weight: 35
description: "Working outwards from ingest to find where the data actually went."
---

# Diagnose a query that returns nothing

An empty result has exactly four causes. Check them in this order — it goes from most to
least common.

## 1. Nothing was stored

```bash
telemetryd status
```

If records are zero for that signal, the problem is on the emitting side. Nothing about
the query will fix it.

## 2. Something was refused

```bash
curl -s localhost:4319/metrics | grep telemetryd_ingest_rejected_total
```

Every rejection is counted with a reason. `body_too_large`, `too_many_labels`,
`invalid_metric_name`, `missing_trace_id` each point somewhere specific. telemetryd
never drops data quietly, so a zero here genuinely means nothing was refused.

## 3. The timestamps are wrong

```bash
curl -s localhost:4319/metrics | grep timestamps_rescaled
```

The most common integration bug is sending seconds or milliseconds where OTLP specifies
nanoseconds. telemetryd detects and corrects it — the counter above tells you it is
happening — but a producer sending something outside any plausible range gets rejected,
and the records land nowhere.

Check what time range you actually have:

```bash
curl -s localhost:4319/status | grep -A3 oldest
```

## 4. The query does not match

Work backwards from the least selective query:

```bash
# Does anything exist at all?
curl -G localhost:4319/loki/api/v1/query_range \
  --data-urlencode 'query={service_name=~".+"}'

# What labels are there?
curl -s localhost:4319/loki/api/v1/labels

# What values does one have?
curl -s localhost:4319/loki/api/v1/label/app/values
```

Label discovery is answered from metadata, so it is fast and exact — if a value is not
listed, nothing carries it.

Two things that surprise people:

- **Label matchers are fully anchored.** `{job=~"api"}` does *not* match `api-gateway`.
  Write `api.*`.
- **Line filters are not anchored.** `|~ "err"` matches anywhere in the line.

## Not an empty result: a subset boundary

If the query used something telemetryd does not implement, the response is HTTP **400**
with the feature named — not an empty result:

```json
{
  "error": {
    "code": "unsupported_feature",
    "feature": "PromQL function `topk`",
    "docs": "https://github.com/cboxdk/telemetryd/blob/main/COMPATIBILITY.md"
  }
}
```

Read the body before concluding there is no data.

## Retention already took it

```bash
curl -s localhost:4319/status | python3 -m json.tool | grep -A5 retention
```

`deleted_by_budget` above zero means the disk budget deleted data still inside its
retention window. See [sizing the disk budget](size-the-disk-budget.md).
