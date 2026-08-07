---
title: "Quickstart"
weight: 2
description: "From nothing to storing and querying telemetry, in one read."
---

# Quickstart

## 1. Install

```bash
curl -fsSL https://raw.githubusercontent.com/cboxdk/telemetryd/main/install.sh | sh
```

The installer verifies the release checksum and refuses to install on a mismatch.

## 2. Run it

```bash
telemetryd serve
```

That is the whole setup. No configuration file, no flags. It listens on
`127.0.0.1:4319`, stores data in `./telemetryd-data` (or your platform data directory),
keeps 7 days of logs and traces and 30 days of metrics, and stays under a 10 GiB disk
budget.

## 3. Send something

telemetryd accepts OTLP/HTTP with **JSON** encoding — no protobuf and no C extension on
the client side, which is what `cboxdk/laravel-telemetry` emits.

```bash
curl -X POST http://127.0.0.1:4319/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{
    "resourceLogs": [{
      "resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": "checkout"}}
      ]},
      "scopeLogs": [{"logRecords": [{
        "timeUnixNano": "'"$(date +%s)000000000"'",
        "severityNumber": 17,
        "severityText": "ERROR",
        "body": {"stringValue": "payment declined for order 1002"}
      }]}]
    }]
  }'
```

## 4. Query it back

Data is queryable the moment it is accepted — there is no wait for a flush.

```bash
# Logs, through the Loki API
curl -G http://127.0.0.1:4319/loki/api/v1/query_range \
  --data-urlencode 'query={app="checkout", level="error"} |= "declined"'

# Metrics, through the Prometheus API
curl -G http://127.0.0.1:4319/api/v1/query \
  --data-urlencode 'query=rate(http_requests_total[5m])'

# A trace, through the Tempo API
curl http://127.0.0.1:4319/api/traces/4bf92f3577b34da6a3ce929d0e0e4736
```

## 5. Watch it live

```bash
websocat 'ws://127.0.0.1:4319/loki/api/v1/tail?query={app="checkout"}'
```

## 6. Point the UI at it

`cboxdk/laravel-telemetry-ui` has three connectors — Loki, Tempo and Prometheus. All
three point at the same telemetryd base URL:

```env
TELEMETRY_UI_LOKI_URL=http://127.0.0.1:4319
TELEMETRY_UI_TEMPO_URL=http://127.0.0.1:4319
TELEMETRY_UI_METRICS_URL=http://127.0.0.1:4319
TELEMETRY_UI_TOKEN=your-query-token
```

## 7. Check on it

```bash
telemetryd status
```

Disk usage against the budget, records stored per signal, retention activity, and
anything that needs attention.

## Next

- Exposing it beyond localhost needs a token — telemetryd
  [refuses to start without one](security/_index.md), on purpose.
- [Run it as a service](cookbook/run-as-a-service.md) so it survives a reboot.
- [Every configuration option](configuration/reference.md), with defaults.
