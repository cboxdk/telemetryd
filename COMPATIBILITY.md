# Compatibility

What telemetryd implements of the Loki, Tempo and Prometheus HTTP APIs, and what it
deliberately does not.

> **Status: not yet frozen.** This document becomes the specification in **M4**, after
> auditing the actual HTTP calls `cboxdk/laravel-telemetry-ui` makes and running it
> against telemetryd. Until then the tables below are the *intended* subset, derived
> from the endpoints the UI is known to use. Every row becomes a contract test.

## The rule

**The subset is the spec.** telemetryd does not aim at API completeness — it aims at
the exact surface `laravel-telemetry-ui` needs, tested by golden-file contract tests
and round-trip tests (OTLP JSON in → query API out).

Anything outside the subset returns a structured error naming the feature and linking
back here, with HTTP 400:

```json
{
  "error": {
    "code": "unsupported_feature",
    "feature": "PromQL function `predict_linear`",
    "message": "PromQL function `predict_linear` is not supported by telemetryd",
    "hint": "use `deriv` over a range selector",
    "docs": "https://github.com/cboxdk/telemetryd/blob/main/COMPATIBILITY.md"
  }
}
```

Each query language is **parsed in full** and then lowered to what telemetryd can
execute. That is why the error above can name the function: pattern-matching only the
supported grammar would have produced "syntax error" and sent you hunting for a typo
that is not there.

Endpoints that are contracted but not yet built return `501` with
`code: "not_implemented"` and the milestone they land in — never a `404`, which would
read as a wrong URL.

## Ingest

| Endpoint | Format | Milestone |
|---|---|---|
| `POST /v1/logs` | OTLP/HTTP JSON | M1 |
| `POST /v1/traces` | OTLP/HTTP JSON | M2 |
| `POST /v1/metrics` | OTLP/HTTP JSON | M3 |
| `POST /api/v1/write` | Prometheus `remote_write` (snappy + protobuf) | M3 |

**OTLP/HTTP JSON is first-class.** `cboxdk/laravel-telemetry` emits JSON — no protobuf,
no C extension on the client path. OTLP/HTTP protobuf is nice-to-have; **OTLP/gRPC is
out of scope for v1.**

## Loki — M1

| Endpoint | Notes |
|---|---|
| `GET /loki/api/v1/query_range` | |
| `GET /loki/api/v1/labels` | |
| `GET /loki/api/v1/label/{name}/values` | |
| `GET /loki/api/v1/tail` | WebSocket live tail |

**LogQL subset:** stream selectors; line filters `|=`, `!=`, `|~`, `!~`; the `json` and
`logfmt` parsers; label filters.

*Not planned for v1:* metric queries over logs (`rate`, `count_over_time`), `unwrap`,
`pattern`, `line_format`/`label_format`, `drop`/`keep`.

## Tempo — M2

| Endpoint | Notes |
|---|---|
| `GET /api/traces/{traceID}` | |
| `GET /api/search` | tag, duration and time filters |
| `GET /api/search/tags` | |
| `GET /api/search/tag/{name}/values` | |

*Not planned for v1:* TraceQL, `/api/v2/*`, service graphs, metrics-generator output.

## Prometheus — M3

| Endpoint | Notes |
|---|---|
| `GET,POST /api/v1/query` | instant |
| `GET,POST /api/v1/query_range` | |
| `GET /api/v1/labels` | |
| `GET /api/v1/label/{name}/values` | |
| `GET /api/v1/series` | |

**PromQL subset:**

- selectors, including `=`, `!=`, `=~`, `!~` matchers
- `rate`, `irate`, `increase`
- `sum`, `avg`, `min`, `max`, `count`, with `by` / `without`
- `histogram_quantile`
- arithmetic between vectors and scalars

*Not planned for v1:* subqueries, `offset`/`@` modifiers, binary operations between two
vectors with matching (`on`/`ignoring`/`group_left`), `topk`/`bottomk`/`quantile`,
`predict_linear`, `holt_winters`, recording and alerting rules, `/api/v1/rules`,
`/api/v1/alerts`, exemplars, native histograms.

## Non-API compatibility notes

- **Multi-tenancy** is the `app` label. There is no `X-Scope-OrgID` handling; the
  `app` label is a namespace for querying, not a security boundary.
- **Timestamps** are Unix nanoseconds on ingest, matching OTLP.
- **Limits** (cardinality, body size, queue depth) reject with a labelled counter and a
  structured error rather than dropping silently. See `/status`.
