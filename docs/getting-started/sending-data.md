---
title: "Sending data"
weight: 12
description: "What each ingest endpoint accepts, and what it does with awkward input."
---

# Sending data

| Endpoint | Format |
|---|---|
| `POST /v1/logs` | OTLP/HTTP — JSON or protobuf |
| `POST /v1/traces` | OTLP/HTTP — JSON or protobuf |
| `POST /v1/metrics` | OTLP/HTTP — JSON or protobuf |
| `POST /api/v1/write` | Prometheus `remote_write` (snappy + protobuf) |
| `POST /loki/api/v1/push` | Loki push — snappy protobuf or JSON, from promtail, Alloy, Fluent Bit, Vector |

## Either encoding, nothing to configure

A stock OpenTelemetry SDK works as it comes. The official SDKs default to
`OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`, and that is served; `http/json` is served
too. The `Content-Type` picks the parser and decides nothing else — both decode into the
same structures and share one conversion, so limits, rejections and `partialSuccess`
cannot differ by encoding. A request with no `Content-Type` is read as JSON.

JSON is what `cboxdk/laravel-telemetry` emits, which is why it exists at all: no protobuf
library and no C extension on the client, which is what makes it work under PHP-FPM.

**OTLP/gRPC** is served on a port of its own once you ask for one:

```toml
[server]
grpc_listen = "127.0.0.1:4317"
```

Then `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` and `OTEL_EXPORTER_OTLP_ENDPOINT=http://<host>:4317`
work, as does a collector's `otlp` exporter. The ingest token goes in as metadata —
`OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer <token>"`. Behind the scenes a call is
stored exactly as the matching `POST /v1/…` would be.

## Log shippers

Anything that ships to Loki ships here unchanged: point promtail, Grafana Alloy, Fluent
Bit's `loki` output or Vector's `loki` sink at `http://<host>:4319/loki/api/v1/push`, with
the ingest token as a bearer token if one is set. Both of Loki's encodings are served:
snappy-compressed protobuf, every shipper's default, and JSON.

A pushed stream keeps its labels as its stream labels — `job`, `host`, `filename`, as the
shipper set them — under the same limits OTLP streams meet. Two things are added the way
Loki adds them:

- **`service_name`**, when the stream has none, from the first of `service`, `app`,
  `application`, `name`, `container`, `component`, `workload` or `job` it carries; the
  same value becomes `app`.
- **`level`**, from a `level`, `detected_level` or `severity` label, else from the line:
  a `level=` or `"level":` field, else the first level word in it (`ERROR`, `warn`, …).

A line's structured metadata is kept beside it, and a `trace_id` or `span_id` there links
the line to its trace. A push is answered `204`; one with lines that could not be stored
is answered `400` naming them, and the rest of it is stored all the same.

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

## Hand this to an agent

A self-contained brief. It names only commands and endpoints that exist, so an agent can
execute it without reading the rest of this page — and without inventing the parts of the
Loki and Prometheus APIs telemetryd deliberately does not implement.

````markdown
# Task: send this application's telemetry to telemetryd

telemetryd accepts **OTLP over HTTP** in either encoding — the SDK default
(`http/protobuf`) and `http/json` both work, so there is nothing to set. Configure the
application's existing OpenTelemetry SDK to point at it. Do not add a collector. Prefer
the HTTP exporters: OTLP/gRPC is served only where the operator set
`server.grpc_listen`, so do not assume port 4317 is open.

## Endpoints

| Endpoint | Payload |
|---|---|
| `POST /v1/logs` | OTLP/HTTP — JSON or protobuf |
| `POST /v1/traces` | OTLP/HTTP — JSON or protobuf |
| `POST /v1/metrics` | OTLP/HTTP — JSON or protobuf |
| `POST /api/v1/write` | Prometheus `remote_write` (snappy + protobuf) |
| `POST /loki/api/v1/push` | Loki push — snappy protobuf or JSON, from promtail, Alloy, Fluent Bit, Vector |

Base URL is the instance, e.g. `http://127.0.0.1:4319`. If an ingest token is
configured, send `Authorization: Bearer <token>`; without one the write returns `401`.

`Content-Encoding: gzip`, `deflate` and `zstd` are accepted and decompressed.

## What becomes queryable

The resource attribute `service.name` becomes the `app` label, and severity becomes
`level`. Those two are always present. Any other resource attribute is stored but is
**not** a stream label unless it is listed in `ingest.stream_labels` — that list is the
cardinality contract, so do not add anything that changes per request, per process or
per deploy.

Stored, in this case, means as a **record attribute** under the spelling you sent. A
resource attribute no stream label claimed — `k8s.pod.name`, `host.name`,
`cloud.region`, `container.id` — comes back as structured metadata alongside the
record's own attributes (under `structuredMetadata` in the shape Grafana asks for, among
the stream's labels in Loki's flat shape), and in `/api/v1/export`:

```json
["1700000000000000000", "checkout failed", {
  "order.id": "A-99", "k8s.pod.name": "pod-7f", "cloud.region": "eu-north-1"
}]
```

Two rules where they meet. A record attribute of the same name wins, because it is
closer to the data. And a name already promoted to a stream label is not stored twice —
`service.version` is the label `service_version`, one attribute under two spellings.

Before v0.35.0 those attributes were read only to build stream labels and then
discarded, so anything outside the five promoted names never reached the store at all.
If you are on an older build, that is why a pod name you know you sent cannot be found.

## Verify, do not assume

After wiring the exporter, send real traffic and confirm it arrives:

```bash
curl -G <base>/loki/api/v1/query_range --data-urlencode 'query={app="<service.name>"}'
```

A `200` from the exporter is not proof: telemetryd rejects **per record** and reports
what it dropped in the OTLP `partialSuccess` field of the response body. Read it.

## Do not

- Do not batch above the instance's `server.max_body_bytes` (16 MiB by default); the
  request is refused with `413` rather than truncated.
- Do not retry a `400` unchanged. It names what was wrong in the response body.

````
