# Compatibility

What telemetryd implements of the Loki, Tempo and Prometheus HTTP APIs, and what it
deliberately does not.

## How this document was produced

Not from the upstream API references. Every requirement below was read out of
`cboxdk/laravel-telemetry-ui`'s connector source — `LokiSource`, `TempoSource`,
`PrometheusSource` and the three query compilers — because **the subset the UI actually
calls is the specification**, and it differs from the published APIs in ways that
matter. The audit is recorded alongside this document, along
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
`code: "not_implemented"` — never a `404`, which would
read as a wrong URL.

## Authentication

The UI sends `Authorization: Bearer <token>` on every backend request, and supports one
shared token across all three connectors (`TELEMETRY_UI_TOKEN`). Point all three at the
same telemetryd base URL and set `auth.query_token` to that value.

`X-Scope-OrgID` is sent when a tenant is configured. telemetryd **ignores** it: it is
single-tenant, and the `app` label is a query namespace rather than a security boundary.

## Time ranges are inclusive at both ends

`start` and `end` both include a record landing exactly on them, on every read endpoint
and for all three signals — the store compares `ts >= start && ts <= end`.

That matches Prometheus, whose range queries evaluate at both edges. It differs from
Loki, where `end` is exclusive. The consequence is one record: two adjacent windows
`[t0, t1]` and `[t1, t2]` both return whatever sits exactly at `t1`.

So page by advancing `start` **past** the newest timestamp you received rather than to it.
Verified by querying single-nanosecond windows at each edge of a known set: `[t, t]`
returns exactly the record at `t`, and the record at a shared boundary appears in both
halves — 2501 + 3500 records over a 6000-record set, union 6000, no gaps.

---

## Ingest

| Endpoint | Format | Status |
|---|---|---|
| `POST /v1/logs` | OTLP/HTTP — JSON or protobuf | **done** |
| `POST /v1/traces` | OTLP/HTTP — JSON or protobuf | done |
| `POST /v1/metrics` | OTLP/HTTP — JSON or protobuf | done |
| `POST /api/v1/write` | Prometheus `remote_write` (snappy + protobuf) | done |

**Both OTLP/HTTP encodings are served.** JSON is what `cboxdk/laravel-telemetry` emits —
no protobuf library and no C extension on the client path — and protobuf is what every
official OpenTelemetry SDK sends by default. The `Content-Type` selects the parser and
decides nothing else: both decode into the same structures and share one conversion, so
limits, rejections and `partialSuccess` cannot differ by encoding. A request with no
`Content-Type` is read as JSON.

**OTLP/gRPC is out of scope for v1.** gRPC needs HTTP/2 with trailers and a second
server; the HTTP endpoints above carry the same payloads.

### Sending from something other than laravel-telemetry

Everything below works from any OTLP producer, as it comes.

**Nothing to configure.** The official SDKs default to `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`
and that works. `http/json` works too. Verified by pointing a stock Python
`opentelemetry-sdk` at telemetryd with default settings and reading all three signals
back out.

**`service.name` is optional.** Without it the `app` label is `unknown` and the record is
kept rather than dropped, because a log line with no service name is still a log line.

**Resource attributes survive.** The five names in `ingest.stream_labels` become stream
labels; every other resource and scope attribute is stored as a record attribute under
the spelling it was sent with. So `k8s.pod.name`, `host.name` and `cloud.region` are
queryable and appear in `/api/v1/export`, without adding to stream cardinality.

One fidelity note for round trips: an attribute that arrived in `resource` comes back out
of `/api/v1/export` on the record rather than nested under `resource`. The attribute and
its value are preserved; its OTLP nesting is not.

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

**It bounds the body, not the memory the body costs.** An empty container is cheap to
encode and not free to represent: measured, 16 MiB of empty protobuf messages reached
801 MB resident, and the same shape in JSON reached 111 MB, because two bytes on the wire
become a struct in a vector. Any repeated field past 100,000 elements is now refused with
a `400` naming the field and telling you to split the batch — far above a real batch, far
below what it takes to hurt. Refused rather than truncated, so a shortened batch is never
mistaken for a complete one.

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

## Loki

| Endpoint | Used by | Notes |
|---|---|---|
| `GET /loki/api/v1/labels` | `LokiSource::probe()`, label discovery | Must answer `{"status":"success"}` — the UI uses this to decide the URL *is* a log backend |
| `GET /loki/api/v1/query_range` | `LokiSource::query()` | `query`, `start`/`end` in **nanoseconds**, `limit`, `direction=backward`. `limit` defaults to 100 and is **clamped to 5,000** |
| `GET /loki/api/v1/label/{name}/values` | `LokiSource::labelValues()` | optional `start`/`end` |
| `GET /loki/api/v1/series` | — | Not used by the UI; implemented anyway, it is cheap and useful |
| `GET /loki/api/v1/tail` | — | WebSocket live tail. **telemetryd's own feature**, not a UI requirement |

### Response shape

`query_range` returns `{"status":"success","data":{"resultType":"streams","result":[…],"stats":{…}}}`.
The UI checks `status`, checks `resultType == "streams"`, and reads
`result[].stream` plus `result[].values[]`.

**Metadata keys are sanitised; trace attributes are not.** A Loki structured-metadata key
must be a valid label name, and a label name cannot contain a dot — so `db.query.text`
leaves this API as `db_query_text`, and a client reads `labels['db_query_text']`. Span
attributes keep the OTLP spelling, because TraceQL addresses `span.db.query.text` and a
trace view reads `attributes['db.query.text']`. The two formats genuinely differ and
matching each is the point; the same client uses both spellings, one per signal.

Label *filters* accept either spelling, so `| db_query_text="x"` and `| db.query.text="x"`
reach the same attribute. That leniency is why this was invisible for so long: the filter
matched and the read came back empty.

Two attributes that sanitise to one key — `order.id` and `order_id` on one record — keep
the first, deterministically.

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

## Tempo

| Endpoint | Used by | Notes |
|---|---|---|
| `GET /api/search/tags` | `TempoSource::probe()`, tag discovery | Must return `tagNames` (or `scopes`) — the UI's backend-recognition check |
| `GET /api/search` | `TempoSource::search()` | `q` = **TraceQL**, `start`/`end` in **seconds**, `limit`, **clamped to 1,000** |
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

## Prometheus

| Endpoint | Used by | Notes |
|---|---|---|
| `GET /api/v1/status/buildinfo` | `PrometheusSource::probe()` | Primary probe; must answer with a build-info envelope |
| `GET /api/v1/query` | `PrometheusSource::instant()`, probe fallback (`query=1`) | |
| `GET /api/v1/query_range` | `PrometheusSource::range()` | `start`/`end` in **seconds**, `step` |
| `GET /api/v1/label/{name}/values` | `PrometheusSource::labelValues()` | optional `match[]`, `start`, `end` |
| `GET /api/v1/labels`, `GET /api/v1/series` | — | Not used by the UI; implemented for completeness |

Note the probe: the UI calls `/api/v1/status/buildinfo` first and only falls back to
`query=1`. Without the former, every connection check shows a degraded backend.

### Metric names carry their unit, and counters say `_total`

OTLP names a metric `http.server.request.duration` and puts `ms` in a separate `unit`
field. Prometheus puts both in the name. telemetryd applies the OpenTelemetry-to-Prometheus
naming rules at ingest, so that metric is stored and queried as
`http_server_request_duration_milliseconds` — with `_count`, `_sum` and `_bucket` after
that for a histogram.

| OTLP name | unit | kind | stored as |
|---|---|---|---|
| `http.server.request.duration` | `ms` | histogram | `http_server_request_duration_milliseconds_{count,sum,bucket}` |
| `cache.operations` | `1` | monotonic sum | `cache_operations_total` |
| `worker.memory.rss` | `By` | gauge | `worker_memory_rss_bytes` |
| `job.time` | `s` | monotonic sum | `job_time_seconds_total` |

`1` is dimensionless and adds nothing. A unit already spelled in the name is not repeated,
so a producer that follows the convention itself does not get `_seconds_seconds`. An
unrecognised unit is left off rather than appended verbatim, because a name nobody queries
is the failure this exists to prevent.

**This changed in v0.43.0.** Before it, the unit was read and discarded and monotonic sums
were stored bare, so `http_server_request_duration_count` was the stored name while every
client written to the convention asked for
`http_server_request_duration_milliseconds_count`. The query succeeded and matched nothing
— a dashboard showed `0` rather than an error. Measured against a real deployment, 60 of
the 64 metric names its UI asked for did not exist.

Names written before the upgrade keep their old spelling until retention removes them, so
a range that straddles it has both. Nothing needs to be done about that beyond waiting.

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

#### `rate` and `increase` do not extrapolate

The one place a supported function answers differently from Prometheus, and it is
deliberate rather than missing.

telemetryd divides the counter's delta by the time actually spanned by the samples.
Prometheus divides by the whole range, after extrapolating the observed rate outwards
towards the window edges. When the window is well covered the two agree exactly; when it
is not, they do not. Measured against a counter rising by a true 10/s:

| Samples in a `[5m]` window | telemetryd `rate` | Prometheus `rate` | telemetryd `increase` | Prometheus `increase` |
|---|---|---|---|---|
| 21, spanning the full 300 s | 10.00/s | 10.00/s | 3000 | 3000 |
| 7, spanning only 60 s | 10.00/s | 2.17/s | 3000 | 650 |

Where you meet this is a **series that has not existed for the whole window** — the first
minutes after a deploy, or a metric that only appears under load. telemetryd reports the
rate the counter was actually moving at; Prometheus reports the average across a window
the series did not exist for, which is why `increase` there famously returns fractions of
a request.

Neither is wrong, and telemetryd's answer is usually the one a person wanted. It is
recorded here because a dashboard built against Prometheus and pointed at telemetryd will
show different numbers for a new series, and that difference should not have to be
discovered.

`histogram_quantile` matches Prometheus exactly, including returning the highest finite
bound when the quantile falls in the `+Inf` bucket. Verified against a known distribution.

---

## Result caps, and which ones tell you

Three endpoints bound how much one request may return. Two of them do it silently,
which is worth knowing before you conclude that data is missing:

| Endpoint | Cap | Behaviour when exceeded |
|---|---|---|
| `GET /loki/api/v1/query_range` | `limit` clamped to **5,000** | silent — ask for more and you get 5,000 |
| `GET /api/search` | `limit` clamped to **1,000** | silent |
| `GET /api/v1/query_range` | **11,000** points per series | `400`, naming the number and telling you to widen `step` |

The Prometheus one is the behaviour to copy and the other two match what the upstream
APIs do, which is why they were left alone rather than made to error: a client that
passes a large limit expecting the server to clamp is doing the normal thing.

Page with the timestamp cursor rather than a larger limit — advance `start` past the
newest record you received. `telemetryd query` does this for you, and
`GET /api/v1/export` exists for bulk reads, where the ceiling is 200,000 records per
request rather than 5,000.

## Discovery

Not part of any upstream API, and telemetryd's own.

| Endpoint | Auth | Returns |
|---|---|---|
| `GET /` | none | `Accept: application/json` → an identity document; `text/html` → a page; otherwise plain text |
| `GET /status` | none | the same identity document, and nothing else |
| `GET /status` | admin token | the deployment picture — unchanged, see below |
| `GET /healthz` | none | `ok` |

The identity document is stable and safe to match on: `product` is the constant
`"telemetryd"`, `storage_format_version` is what a client must agree with to read this
instance's data, and `signals` lists `logs`, `metrics` and `traces`. On `/` it carries
two more fields — `surfaces[].auth`, naming the credential each group of routes wants
(`null` where none is needed), and `docs`.

```json
{"product":"telemetryd","version":"0.34.0","storage_format_version":1,
 "signals":["logs","metrics","traces"]}
```

It describes the product, never the deployment. App names, record counts, disk usage,
retention windows, the listen address, relay configuration and per-surface auth state
are on `/status` **with** the admin token.

### `/status` answers both callers, on one schema

**Since 0.34.0.** Before it, every identifying route including `/status` answered `401`
to a client with no credential, so a client could not tell a telemetryd that wants a
token from a host with nothing on it, and reported the second about the first.

- **With the admin token**: exactly what it has always returned, field for field. This
  is a widening, not a replacement; an existing client sees no change.
- **Without one, or with one that is refused**: the four identity fields, `200`.

The two documents share `version` and `storage_format_version` — same name, same type,
same meaning, so there is one schema to parse rather than two. **Tell them apart by the
absence of `storage`, not by the status code**: both are `200`. A monitoring check that
must confirm it authenticated should assert a field only the full document has.

`Cache-Control: no-store` and `Vary: Authorization` on the identity response. One URL
with two bodies chosen by a request header must not be cached by anything shared, and a
cached identity would survive a restart onto a different build — a client would then
check compatibility against a version that is no longer running.

Version disclosure is deliberate and not configurable; the reasoning is in
[the configuration reference](docs/configuration/reference.md).

`/metrics` is unchanged: it still answers `401`, because a Prometheus exposition of
telemetryd itself is all deployment and no identity.

Every `401` carries `WWW-Authenticate: Bearer realm="telemetryd"`, so a refusal
identifies the product too.

## Non-API notes

- **Multi-tenancy** is the `app` label. No `X-Scope-OrgID` handling; the label is a
  query namespace, not a security boundary.
- **Timestamps on ingest** are Unix nanoseconds, matching OTLP. Query APIs use each
  upstream's own convention: Loki nanoseconds, Tempo and Prometheus seconds. That
  inconsistency is upstream's, and matching it is the point.
- **Limits** (cardinality, body size, queue depth) reject with a labelled counter and a
  structured error rather than dropping silently. See `/status` and
  `telemetryd_ingest_rejected_total`.
