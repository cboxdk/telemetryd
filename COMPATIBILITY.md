# Compatibility

What telemetryd implements of the Loki, Tempo and Prometheus HTTP APIs, and what it
deliberately does not.

## How this document was produced

Not from the upstream API references. Every requirement below was read out of
`cboxdk/laravel-telemetry-ui`'s connector source — `LokiSource`, `TempoSource`,
`PrometheusSource` and the three query compilers — because **the subset the UI actually
calls is the specification**, and it differs from the published APIs in ways that
matter. The audit is recorded in [ADR-005](docs/adr/0005-compatibility-audit.md), along
with the four places our first guess was wrong.

Each row below is covered by a contract test. Where a requirement came from reading the
UI rather than from the upstream spec, the test says so.

## The rule

telemetryd does not aim at API completeness. It aims at the exact surface
`laravel-telemetry-ui` needs, plus a small, documented superset where the UI benefits.

Anything outside the subset returns a structured error naming the feature, with HTTP
400:

```json
{
  "error": {
    "code": "unsupported_feature",
    "feature": "LogQL metric query `rate`",
    "message": "LogQL metric query `rate` is not supported by telemetryd",
    "hint": "aggregate the results client-side, or use the Prometheus API for pre-aggregated metrics",
    "docs": "https://github.com/cboxdk/telemetryd/blob/main/COMPATIBILITY.md"
  }
}
```

Each query language is **parsed in full** and then lowered to what telemetryd can
execute. That is why the error can name the function: pattern-matching only the
supported grammar would have produced "syntax error" and sent you hunting for a typo
that is not there.

Endpoints that are contracted but not yet built return `501` with
`code: "not_implemented"` and the milestone they land in — never a `404`, which would
read as a wrong URL.

## Authentication

The UI sends `Authorization: Bearer <token>` on every backend request, and supports one
shared token across all three connectors (`TELEMETRY_UI_TOKEN`). Point all three at the
same telemetryd base URL and set `auth.query_token` to that value.

`X-Scope-OrgID` is sent when a tenant is configured. telemetryd **ignores** it: it is
single-tenant, and the `app` label is a query namespace rather than a security boundary
(see [ADR-004](docs/adr/0004-auth-and-network-binding.md)).

---

## Ingest

| Endpoint | Format | Status |
|---|---|---|
| `POST /v1/logs` | OTLP/HTTP JSON | **M1 — done** |
| `POST /v1/traces` | OTLP/HTTP JSON | M2 |
| `POST /v1/metrics` | OTLP/HTTP JSON | M3 |
| `POST /api/v1/write` | Prometheus `remote_write` (snappy + protobuf) | M3 |

**OTLP/HTTP JSON is first-class.** `cboxdk/laravel-telemetry` emits JSON — no protobuf,
no C extension on the client path. A protobuf `Content-Type` is refused with a named
`unsupported_feature` rather than a parse error full of binary. **OTLP/gRPC is out of
scope for v1.**

### Content-Encoding

| Value | Behaviour |
|---|---|
| absent, `identity` | body used as-is |
| `gzip`, `x-gzip` | decompressed — this is the one every OTLP SDK sends |
| `deflate` | decompressed; zlib-wrapped per RFC 9110, bare deflate accepted too |
| `zstd` | decompressed |
| `snappy` on `/api/v1/write` only | passes through; it names the `remote_write` payload's own framing |
| anything else | `400` with `unsupported_feature`, naming the coding |

gzip is part of OTLP/HTTP rather than an optional extra, and every SDK turns it on
above some batch size — so ignoring the header does not lose an optimisation, it loses
every batch that carries data while still answering `200` to the empty one a health
check sends.

**`server.max_body_bytes` bounds the decompressed body**, not only the bytes received.
A body that expands past it is refused with `413` and `limit_exceeded` naming the
setting — identical to what an oversized uncompressed body gets, because a compressed
request must not be able to buy more memory than an uncompressed one.

Accepted beyond the strict spec, because real producers send it:

- `int64` fields as JSON numbers as well as strings.
- Both proto3 field spellings (`timeUnixNano` and `time_unix_nano`).
- `severityNumber` as a number or as its proto name (`SEVERITY_NUMBER_ERROR`).
- Timestamps in seconds, milliseconds or microseconds instead of nanoseconds. The unit
  is recovered by magnitude — the ranges do not overlap for any date between 2001 and
  2100 — and every correction increments
  `telemetryd_ingest_timestamps_rescaled_total`, so the producer bug stays visible
  rather than being papered over.

Rejections are per record, not per request. A batch with one oversized body stores the
rest and reports the refusal through OTLP's own `partialSuccess` field.

---

## Loki — M1, done

| Endpoint | Used by | Notes |
|---|---|---|
| `GET /loki/api/v1/labels` | `LokiSource::probe()`, label discovery | Must answer `{"status":"success"}` — the UI uses this to decide the URL *is* a log backend |
| `GET /loki/api/v1/query_range` | `LokiSource::query()` | `query`, `start`/`end` in **nanoseconds**, `limit`, `direction=backward` |
| `GET /loki/api/v1/label/{name}/values` | `LokiSource::labelValues()` | optional `start`/`end` |
| `GET /loki/api/v1/series` | — | Not used by the UI; implemented anyway, it is cheap and useful |
| `GET /loki/api/v1/tail` | — | WebSocket live tail. **telemetryd's own feature**, not a UI requirement |

### Response shape

`query_range` returns `{"status":"success","data":{"resultType":"streams","result":[…],"stats":{…}}}`.
The UI checks `status`, checks `resultType == "streams"`, and reads
`result[].stream` plus `result[].values[]`.

**Entries carry structured metadata.** Each `values` element is
`[timestamp, line]` or `[timestamp, line, {…}]`. The third element is where
per-record OTLP attributes go — `order.id`, `session.id`, `trace_id`. The UI merges it
over the stream labels. Promoting those attributes to stream labels instead would
explode cardinality; omitting them entirely would make them invisible in the UI even
though telemetryd stored them. Timestamps are **strings** of nanoseconds, because a
JSON number cannot hold them without loss in JavaScript.

### LogQL subset

Supported:

- stream selectors with `=`, `!=`, `=~`, `!~`
- line filters `|=`, `!=`, `|~`, `!~` (regexes here are **unanchored**, unlike label
  matchers, which are fully anchored)
- the `json` and `logfmt` parsers
- label filters, including `and` / `or` chains — `| status="500" or status="503"` is
  what the UI's compiler emits, so a single bare matcher would not have been enough

**telemetryd superset:** label filters can read per-record attributes without a parser
stage. OTLP records are already structured, so requiring `| json` to reach them would
be theatre.

The selector must contain at least one matcher that requires a value. `{}` and
`{app!="x"}` are refused, because both select every stream in the store. The UI's own
default, `{service_name=~".+"}`, satisfies this.

*Not supported:* metric queries (`rate`, `count_over_time`, `sum by …`), `unwrap`,
`pattern`, `regexp`, `line_format`, `label_format`, `drop`, `keep`, `decolorize`,
`distinct`, `ip`, and numeric label filters (`| status > 400`).

---

## Tempo — M2

| Endpoint | Used by | Notes |
|---|---|---|
| `GET /api/search/tags` | `TempoSource::probe()`, tag discovery | Must return `tagNames` (or `scopes`) — the UI's backend-recognition check |
| `GET /api/search` | `TempoSource::search()` | `q` = **TraceQL**, `start`/`end` in **seconds**, `limit` |
| `GET /api/traces/{traceID}` | `TempoSource::trace()` | Returns `{"batches":[…]}` in OTLP `resourceSpans` shape |
| `GET /api/v2/search/tag/{name}/values` | `TempoSource::tagValues()` | **v2**, with an optional `q` filter. Returns `{"tagValues":[{"type","value"}]}` |

Two corrections our first plan got wrong, both found by reading the connector:

- Search is driven by **TraceQL** through `q`, not by a `tags` parameter. A TraceQL
  subset is therefore required, not optional.
- Tag values come from the **v2** path. The v1 path is not called.

### TraceQL subset

The UI's `TraceqlCompiler` emits a single spanset of `&&`-joined conditions, optionally
followed by `| select(...)`:

```
{ resource.service.name = "checkout" && status = error && duration > 100ms }
```

Supported: one spanset; `&&`-joined conditions; the operators `=`, `!=`, `=~`, `>`,
`<`, `>=`, `<=`; `resource.*`, `span.*`, unscoped `.attribute` and intrinsic fields
(`name`, `status`, `duration`, `kind`); string, number, duration and `nil` literals;
`| select(...)` is accepted and ignored (telemetryd always returns the matched spans).

An unscoped `.attribute` searches span attributes first, then resource labels — the
narrower scope wins, as in TraceQL. The leading dot is significant: `name` is the
span's name, `.name` is an attribute a producer happened to call `name`. Because
ingest sanitizes stream labels but leaves attribute keys in the producer's own
spelling, `.service.name` still reaches the `service_name` label.

*Not supported:* multiple spansets, `||` between conditions, structural operators
(`>>`, `~`), aggregates (`count()`, `avg()`), and `by()`.

---

## Prometheus — M3

| Endpoint | Used by | Notes |
|---|---|---|
| `GET /api/v1/status/buildinfo` | `PrometheusSource::probe()` | Primary probe; must answer with a build-info envelope |
| `GET /api/v1/query` | `PrometheusSource::instant()`, probe fallback (`query=1`) | |
| `GET /api/v1/query_range` | `PrometheusSource::range()` | `start`/`end` in **seconds**, `step` |
| `GET /api/v1/label/{name}/values` | `PrometheusSource::labelValues()` | optional `match[]`, `start`, `end` |
| `GET /api/v1/labels`, `GET /api/v1/series` | — | Not used by the UI; implemented for completeness |

Note the probe: the UI calls `/api/v1/status/buildinfo` first and only falls back to
`query=1`. Without the former, every connection check shows a degraded backend.

### PromQL subset

Driven by what `PromqlCompiler` actually generates:

- selectors: `name{label="value", …}` with `=`, `!=`, `=~`, `!~`
- `rate(sel[5m])`, `increase(sel[5m])`
- aggregations `sum`, `avg`, `min`, `max`, `count`, with `by (…)` / `without (…)`
- `histogram_quantile(0.95, sum by (le, …) (rate(sel[5m])))`
- arithmetic against scalars: `expr * 60`
- **`offset`**, **`or` between vectors**, and **`clamp_min`** — required by the
  compiler's counter-increase form:
  `clamp_min(sel - (sel offset 5m or sel * 0), 0)`. Our first plan listed all three as
  out of scope; they are not.

*Not supported:* subqueries, the `@` modifier, `topk`/`bottomk`/`quantile`,
`predict_linear`, `holt_winters`, recording and alerting rules, `/api/v1/rules`,
`/api/v1/alerts`, exemplars, native histograms, and vector-to-vector matching beyond
the `or` form above (`on`/`ignoring`/`group_left`).

---

## Non-API notes

- **Multi-tenancy** is the `app` label. No `X-Scope-OrgID` handling; the label is a
  query namespace, not a security boundary.
- **Timestamps on ingest** are Unix nanoseconds, matching OTLP. Query APIs use each
  upstream's own convention: Loki nanoseconds, Tempo and Prometheus seconds. That
  inconsistency is upstream's, and matching it is the point.
- **Limits** (cardinality, body size, queue depth) reject with a labelled counter and a
  structured error rather than dropping silently. See `/status` and
  `telemetryd_ingest_rejected_total`.
