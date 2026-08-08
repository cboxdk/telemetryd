---
title: "Sending data"
weight: 12
description: "What each ingest endpoint accepts, and what it does with awkward input."
---

# Sending data

| Endpoint | Format |
|---|---|
| `POST /v1/logs` | OTLP/HTTP JSON |
| `POST /v1/traces` | OTLP/HTTP JSON |
| `POST /v1/metrics` | OTLP/HTTP JSON |
| `POST /api/v1/write` | Prometheus `remote_write` (snappy + protobuf) |

## JSON is first-class

OTLP/HTTP with **JSON** encoding is the supported path, because that is what
`cboxdk/laravel-telemetry` emits — no protobuf library and no C extension on the client,
which is what makes it work under PHP-FPM.

A protobuf `Content-Type` on the OTLP endpoints is refused with a named
`unsupported_feature` error rather than a parse failure full of binary. OTLP/gRPC is out
of scope.

`remote_write` is protobuf, and that is fine: it is server-side only.

## Compressed bodies

Set `Content-Encoding` and telemetryd will undo it before decoding:

| `Content-Encoding` | |
|---|---|
| absent, or `identity` | the body is used as-is |
| `gzip` (or `x-gzip`) | what every OpenTelemetry SDK sends once a batch passes its compression threshold |
| `deflate` | zlib-wrapped, per the HTTP spec; a bare deflate stream is accepted too |
| `zstd` | |
| anything else | refused with a named `unsupported_feature` error |

Matching is case-insensitive, and a `Content-Encoding` naming two codings at once is
refused rather than half-decoded. On `POST /api/v1/write`, `Content-Encoding: snappy`
means the `remote_write` payload's own framing and passes through untouched — that is
what Prometheus sends.

**This is worth getting right at the client end**, because getting it wrong used to be
invisible: an SDK that compresses only above a size threshold sends the empty batch of a
health check uncompressed, and only the batches that carry data compressed. A server
that ignored the header would answer 200 to the diagnostic and 400 to everything real.

### The size limit applies after decompression

`server.max_body_bytes` (default 16 MiB) bounds the **decompressed** body, not just the
bytes on the wire. Otherwise 30 KB of gzip would be a request for gigabytes of memory on
an endpoint open to whatever the network sends. A body that expands past the limit is
refused with `413` and a `limit_exceeded` error naming the setting — the same answer an
oversized uncompressed body gets, so the number means one thing:

```json
{
  "error": {
    "code": "limit_exceeded",
    "message": "server.max_body_bytes exceeded: the gzip request body expands past the 16777216 byte limit (30104 compressed bytes received)"
  }
}
```

If you legitimately send batches bigger than that, raise `server.max_body_bytes` — or,
better, batch smaller, since the whole body has to be buffered and parsed either way.

## What telemetryd accepts beyond the strict spec

Real producers send these, so refusing them would be pedantry:

- `int64` fields as JSON numbers as well as strings
- both proto3 field spellings — `timeUnixNano` and `time_unix_nano`
- `severityNumber` as a number or as its proto name (`SEVERITY_NUMBER_ERROR`)
- timestamps in seconds, milliseconds or microseconds instead of nanoseconds

That last one is the most common integration mistake there is, and it is silent: every
record lands in 1970 and the data looks lost. The magnitudes do not overlap for any date
between 2001 and 2100, so the intended unit is recoverable rather than guessed at. Every
correction increments `telemetryd_ingest_timestamps_rescaled_total`, so the producer bug
stays visible instead of being papered over.

## Partial success

Rejections are per record, not per request. A batch containing one 2 MB log body stores
the other 499 records and reports the refusal through OTLP's own `partialSuccess` field:

```json
{
  "partialSuccess": {
    "rejectedLogRecords": "1",
    "errorMessage": "1 record(s) rejected (body_too_large); for example: log body of 2097152 bytes exceeds max_log_line_bytes"
  }
}
```

Every rejection also increments `telemetryd_ingest_rejected_total{signal,reason}`.
Nothing is ever dropped quietly.

## What becomes a label

Only a bounded, configured set of resource attributes become **stream labels** — the
thing cardinality is counted in. Everything else is stored and queryable, it just does
not create a new stream.

Promoting every attribute would be the friendly-looking default and a trap: `host.id`,
`process.pid` and `container.id` change per deploy or per process, and would multiply
streams without bound. See [`ingest.stream_labels`](../configuration/reference.md).

Per-record attributes keep the producer's own key spelling — `exception.type` stays
`exception.type`, because a trace view should show what was sent. Queries reach them by
either spelling.
