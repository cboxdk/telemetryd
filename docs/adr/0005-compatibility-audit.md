---
title: "ADR-005: The compatibility subset comes from the client, not the upstream spec"
weight: 5
description: "Why the compatibility subset is read out of the client's source rather than the upstream spec."
---

# ADR-005: The compatibility subset comes from the client, not the upstream spec

- **Status:** Accepted
- **Date:** 2026-08-07
- **Milestone:** M4 (brought forward — see Consequences)

## Context

The plan put the compatibility audit in M4, after logs, traces and metrics were built.
The intent was to freeze `COMPATIBILITY.md` once there was something to freeze.

Running the audit early, immediately after M1, changed that ordering. Reading the
actual connector source in `cboxdk/laravel-telemetry-ui` — `LokiSource`,
`TempoSource`, `PrometheusSource` and the three query compilers — turned up four
requirements that the published upstream APIs do not imply, two of which would have
sent M2 and M3 in the wrong direction.

## Decision

**The specification is the client, not the upstream API reference.**
`COMPATIBILITY.md` is derived from what `laravel-telemetry-ui` actually calls, and each
row is backed by a contract test. Where a requirement exists only because the client
depends on it, the test says so, so nobody later "simplifies" it away.

**The audit runs before the milestone it constrains, not after.** M4 remains the
freeze, but the reading happens first.

## What the audit found

### 1. Loki entries need structured metadata (M1 — fixed)

`LokiSource::query()` reads a **third element** from each `values` tuple and merges it
over the stream labels:

```php
$metadata = isset($value[2]) && is_array($value[2]) ? … : [];
```

This is where per-record OTLP attributes belong — `order.id`, `session.id`, `trace_id`.
telemetryd emitted two-element tuples, so it stored those attributes and then made them
invisible to the UI. Nothing would have errored; the data would simply not have been
there, which is the failure mode that takes longest to notice.

Fixed in M1: entries now carry a third element when the record has attributes, and are
left at two when it does not, matching Loki.

### 2. LogQL label filters need `and` / `or` (M1 — fixed)

`LogqlCompiler` emits combined filters in one stage:

```php
' | '.implode($stage->or ? ' or ' : ' and ', array_map($this->matcher(...), $stage->matchers))
```

Our parser accepted one matcher per `|`. An ordinary UI query would have come back as a
syntax error. Fixed, with `and` binding tighter than `or` as LogQL specifies.

### 3. Tempo search is TraceQL, and tag values are v2 (M2)

Two assumptions in the original plan were wrong:

- `COMPATIBILITY.md` said search took "tags/duration/time filters" and listed TraceQL
  as *not planned*. The connector compiles TraceQL into a `q` parameter. A TraceQL
  subset is a requirement, not a nice-to-have.
- Tag values come from `/api/v2/search/tag/{name}/values`, not the v1 path we listed.
  The v1 path is never called.

Also: Tempo's `start`/`end` are **seconds**, where Loki's are nanoseconds. Matching each
upstream's own convention is the whole point of compatibility, so telemetryd matches
both.

### 4. PromQL needs `offset`, `or` and `clamp_min` (M3)

`PromqlCompiler`'s counter-increase form is:

```
clamp_min(sel - (sel offset 5m or sel * 0), 0)
```

The original plan listed the `offset` modifier, vector-to-vector `or`, and `clamp_min`
as out of scope. All three are on the UI's default path. The `or sel * 0` idiom exists
to make the expression yield zero rather than nothing when a series has no older
sample, so dropping it does not degrade a chart — it empties it.

Separately, `PrometheusSource::probe()` calls `/api/v1/status/buildinfo` first and only
falls back to `query=1`. Without that endpoint every connection check would show a
degraded backend even though queries worked.

## Consequences

**Good.** M2 and M3 now have a specification derived from real calls rather than from
our reading of three upstream projects. Two of the four findings were already costing
correctness in shipped M1 code, and both are fixed with tests that name the client.

**The subset is larger than planned.** TraceQL, `offset`, vector `or` and `clamp_min`
all move from "not planned" to required. That is the honest cost of the compatibility
promise; the alternative is a product that technically implements a subset and does not
actually work with the one client it exists for.

**A standing rule.** Before implementing any query-API milestone, read the connector
that will call it. The upstream reference describes what a server *may* do; the client
describes what this server *must* do.

## Notes

The UI authenticates with `Authorization: Bearer <token>` and supports one shared token
across all three connectors, which maps cleanly onto telemetryd's single
`auth.query_token`. It sends `X-Scope-OrgID` when a tenant is configured; telemetryd
ignores it deliberately (ADR-004).

Two endpoints in our contract are **not** used by the UI: `/loki/api/v1/series` and
`/loki/api/v1/tail`. Series is cheap and stays. Live tail is telemetryd's own headline
feature rather than a compatibility obligation, and is documented as such so nobody
mistakes it for a UI requirement later.
