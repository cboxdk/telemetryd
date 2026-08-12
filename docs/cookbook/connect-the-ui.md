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

## Is there a telemetryd at this address?

Ask before you hold a token. `GET /` is open, and with `Accept: application/json` it
returns what the server is rather than what it holds:

```bash
curl -s -H 'Accept: application/json' https://telemetry.example.com/
```

```json
{
  "product": "telemetryd",
  "version": "0.35.0",
  "storage_format_version": 1,
  "signals": ["logs", "metrics", "traces"],
  "surfaces": [
    { "title": "Read it back", "auth": "query token", "routes": [ … ] },
    { "title": "Always open", "auth": null, "routes": [ … ] }
  ],
  "docs": "https://github.com/cboxdk/telemetryd/blob/main/docs/quickstart.md"
}
```

This exists because a client that probes only guarded endpoints cannot tell a telemetryd
that wants a token from an address with nothing behind it — both look like a refusal.
Match on `product`; use `storage_format_version` to decide whether you can read its data
at all; use `surfaces[].auth` to know which credential to ask a person for.

`auth` is `null` for the surface that needs none, so a client tests a field rather than
parsing the words "no token".

The document names no app, no count, no disk figure and no retention window. Those are
`/status`, behind the admin token.

### `/status` answers the same question

Since 0.35.0 you get the first four fields from `/status` too, with no credential:

```bash
curl -s https://telemetry.example.com/status
```

```json
{"product":"telemetryd","version":"0.35.0","storage_format_version":1,
 "signals":["logs","metrics","traces"]}
```

Two doors, one document, because a client does not always get to choose which it walks
through. Probing `/` is the right first call, but a client that already had `/status` in
its list — and every operational client does — no longer has to be taught a second URL to
stop reporting "nothing answered" about a server that is answering.

Send the admin token to the same path and you get the deployment picture instead: same
`version`, same `storage_format_version`, plus everything this instance holds. **The
absence of `storage` is how a client knows which of the two it received.** A health check
that must confirm it is authenticated should assert on `storage`, not on the status code —
both are `200`.

A guarded endpoint is a weaker but still real signal: every `401` from telemetryd
carries `WWW-Authenticate: Bearer realm="telemetryd"`. Refusal is identification, not
silence — treat it as "found it, need a credential" rather than moving on to the next
candidate address.

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
