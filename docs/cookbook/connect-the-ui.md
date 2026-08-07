---
title: "Connect laravel-telemetry-ui"
weight: 33
description: "Point all three connectors at one base URL."
---

# Connect laravel-telemetry-ui

`cboxdk/laravel-telemetry-ui` has three connectors — Loki for logs, Tempo for traces,
Prometheus for metrics. Against telemetryd all three point at the **same base URL**,
because one process serves all three APIs on one port.

```env
TELEMETRY_UI_LOKI_URL=http://127.0.0.1:4319
TELEMETRY_UI_TEMPO_URL=http://127.0.0.1:4319
TELEMETRY_UI_METRICS_URL=http://127.0.0.1:4319

# One token covers all three; it maps onto telemetryd's query_token.
TELEMETRY_UI_TOKEN=your-query-token
```

And point the emitting side at the same place:

```env
TELEMETRY_OTLP_ENDPOINT=http://127.0.0.1:4319
```

## Checking the connection

The UI probes each backend before using it, and each probe is a different endpoint:

| Connector | Probe | Expects |
|---|---|---|
| Loki | `GET /loki/api/v1/labels` | `{"status":"success"}` |
| Tempo | `GET /api/search/tags` | a `tagNames` key |
| Prometheus | `GET /api/v1/status/buildinfo` | a build-info envelope |

All three answer. You can check them by hand:

```bash
curl -s http://127.0.0.1:4319/loki/api/v1/labels
curl -s http://127.0.0.1:4319/api/search/tags
curl -s http://127.0.0.1:4319/api/v1/status/buildinfo
```

## If a card is empty

Work outwards from ingest:

1. **Is anything stored?** `telemetryd status` reports records per signal. Zero means
   the problem is on the emitting side, not the query side.
2. **Was anything refused?**
   `curl -s localhost:4319/metrics | grep ingest_rejected` — a non-zero counter names
   the reason.
3. **Are the timestamps right?**
   `telemetryd_ingest_timestamps_rescaled_total` above zero means the producer is
   sending the wrong unit.
4. **Did the query hit a subset boundary?** An unsupported feature returns HTTP 400 with
   the feature named — check the response body rather than assuming there is no data.

More in [diagnosing empty results](diagnose-empty-results.md).

## X-Scope-OrgID

The UI sends it when a tenant is configured. telemetryd ignores it: it is single-tenant,
and `app` is a query namespace rather than a security boundary.
