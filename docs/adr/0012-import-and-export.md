---
title: "ADR-012: Import and export"
weight: 12
description: "Portability in both directions, why it goes through query APIs rather than storage formats, and the three things it cannot promise."
---

# ADR-012: Import and export

- **Status:** **Accepted and built, for all three signals.**
- **Date:** 2026-08-08
- **Builds on:** [ADR-005](0005-compatibility-audit.md), [ADR-004](0004-auth-and-network-binding.md)

## Context

telemetryd asks for a commitment: point your instrumentation at it, and your telemetry
lives in its data directory. That is a reasonable thing to ask only if leaving is as
easy as arriving.

Today half of that exists, and it is worth being precise about which half, because it
changes how much is left to build.

**Reading telemetryd from elsewhere already works.** It serves the Loki, Tempo and
Prometheus read APIs, so anything that queries those can query it. Moving a dashboard
onto telemetryd is a URL change, and moving it back off is the same change in reverse.

**Writing live data into telemetryd already works.** It accepts OTLP and Prometheus
`remote_write`, which is what collectors already emit.

What is missing is everything to do with *data that already exists*:

- No bulk export of a time range. `telemetryd query --output json` is logs only, goes
  through LogQL, and is bounded by `--limit`. Traces and metrics cannot come out in
  bulk at all.
- No way to bring an existing store's history in. Not for migration, and — the case
  that actually recurs — not for pulling a slice of production onto a laptop to
  reproduce something.

The second is the one that gets used weekly. Migration happens once; debugging happens
on Tuesday.

## Decision

**Two commands, and neither one ever reads or writes another system's storage.**

```
telemetryd export --since 24h --signal logs,traces > dump.ndjson
telemetryd import --from https://logs.internal --since 24h
telemetryd import --file dump.ndjson
```

Everything crosses the boundary as HTTP, over interfaces that are already a documented
contract for both sides.

### Export emits OTLP

Not a bespoke format, and not Parquet. OTLP is what every other backend already
ingests, which makes it nearly free for whoever receives it.

An earlier draft of this ADR claimed it was nearly free to *implement* too, "because
telemetryd already has the encoders". It does not. It has decoders; writing records
back out is code that did not exist, and was built for relay mode
([ADR-013](0013-relay-mode.md)) in `crates/ingest/src/otlp_encode.rs`. The work is
mechanical rather than hard, but it is work, and the round trip through our own decoder
is what keeps it honest — the first version of that test failed.

It also makes **import from our own export free**: OTLP JSON out is exactly what
`/v1/logs`, `/v1/traces` and `/v1/metrics` take in. `telemetryd export | telemetryd
import --file -` is a complete instance-to-instance copy with no new code path. The
`--file` source exists anyway because a 4 GB dump wants chunking and progress rather
than a single request.

One request-shaped JSON object per line. NDJSON streams, survives being cut in half,
and every tool on the machine can read it.

### Import speaks read APIs, not storage

An importer that parsed another backend's on-disk format would be a maintenance
treadmill against a format nobody promised us. Their read APIs, by contrast, are a
published contract — and we are unusually well placed to consume them, because we
already parse LogQL, TraceQL and PromQL, and already *emit* those exact response
shapes. Both ends of the wire are code we own and test.

The consequence worth noticing: because telemetryd serves those same APIs, **one
importer also copies from another telemetryd**. Migration in, migration out, and
instance-to-instance are one code path, not three.

### Never write to a store you were only asked to read from

The first version of this section said something broader and wrong: that telemetryd
never writes to a foreign store, so export would produce a file and there would be no
`--to <url>`. It borrowed the desktop app's posture — *"This app never writes to a
connected store"* — and over-applied it.

**Relay mode had been posting OTLP to a remote since the day it shipped.** The rule as
stated was already false in our own code, and nobody noticed until someone asked why
export could not do the same thing relay does.

The rule that survives is narrower and still worth keeping: never write into a store you
were only asked to *read* from. That is what protects a connection someone configured
for querying production. A destination named on the command line is not that — it is the
operator saying where their data should go.

So `export --to <url>` exists, and it is the full-fidelity path between two instances:
records read through `/api/v1/export` rather than re-derived from a query language, all
three signals, no file in between. `import --from` remains the direction that works
against a foreign backend, through the read APIs, which cannot carry metrics.

## Progress

A bulk transfer that prints nothing for forty minutes is indistinguishable from one
that has hung. Progress is part of the feature, not decoration on it.

**stdout is data. stderr is progress.** That single rule is what lets
`telemetryd export | gzip > dump.gz` show a live progress meter without corrupting a
byte of the output.

`--progress` takes:

| | |
|---|---|
| `auto` (default) | a live meter when stderr is a terminal, periodic plain lines when it is not |
| `tty` | force the live meter |
| `plain` | one line every few seconds — a log file, not a redraw |
| `json` | NDJSON progress events on stderr |
| `none` | silence |

`auto` is what makes this behave under `systemd`, in CI, and in a pipe without anyone
passing a flag. A redraw with carriage returns and ANSI escapes is right in a terminal
and is garbage in a log file; detecting which one is present costs nothing and guessing
wrong is the difference between readable output and a smear.

`json` exists for the desktop app, and it is why progress is structured rather than
formatted. Each event carries the signal, the window boundaries reached, records
transferred, bytes, rejections so far, and the high-water timestamp:

```json
{"event":"progress","signal":"logs","window_end":"2026-08-01T12:00:00Z","records":184320,"rejected":12,"high_water":"2026-08-01T12:00:00Z"}
```

The final event is `{"event":"done", ...}` or `{"event":"failed","error":"..."}`, and
both carry the high-water mark. That last field is the whole resumability story: an
import that dies at 60% tells you exactly where to start again, and resuming is the
same command with a different `--start`.

Interrupting with Ctrl-C is a clean stop that reports the high-water mark, not a
stack trace.

## The desktop app

The desktop app already has the two things this needs. Profiles hold connections
(`ActiveProfile`, `ConnectionMapper`, `ProfileSwitcher`), and a local sidecar
telemetryd is provisioned as its own profile (`LocalProfileProvisioner`,
`TelemetrydSupervisor`). It also already runs the binary as a supervised subprocess and
listens to its output (`SidecarOutputListener`, `SidecarJournal`).

So "copy the last six hours from *production* into my local store" is, in the app's own
vocabulary, a transfer from a connection in the active profile into the sidecar's
profile — and `--progress=json` on stderr lands in a listener that already exists.

Two things follow that the CLI must get right for the app's sake:

- **Every knob is a flag.** No interactive prompts, no confirmation that requires a
  keypress. The app cannot answer a question typed at a terminal it does not have.
  Where a run should be refused (see retention, below) it is refused with a distinct
  exit code and a machine-readable reason, not a `[y/N]`.
- **The connection's credentials come from the app**, through the same
  `--token` / `TELEMETRYD_AUTH_QUERY_TOKEN` path the CLI already uses, so secrets stay
  in the app's encrypted store and never reach a command line where `ps` can read them.
  This is the rule `telemetryd query` already documents; import inherits it rather than
  inventing a second one.

With more than one connection configured, the interesting operation is not "export
everything" but "bring *this* connection's last N hours local". Scoping import to one
source at a time keeps provenance answerable, which matters once the local store holds
data from three places at once — see `--label`, below.

## Use cases, walked through

| | |
|---|---|
| **Evaluate telemetryd** without moving production | import a day from the existing store, query it locally, keep the original untouched |
| **Adopt telemetryd** | run both in parallel, import the history that predates the switch |
| **Leave telemetryd** | export the range, feed it to whatever ingests OTLP |
| **Debug an incident** | pull the incident window onto a laptop; query it offline, repeatedly, without load on production |
| **Move instances** | export from one, import to the other; no storage-format coupling |
| **Share a reproduction** | an NDJSON file is a bug report someone else can load |
| **Back up a window** | the whole data directory is still the better answer for a full backup; export is for a slice |

The debugging case is the one that shapes the defaults. It wants a bounded time range,
a single connection, an obvious progress display, and a local store it can throw away
afterwards.

## What it costs, honestly

**Retention will delete what you just imported, and this is the trap.** Ingest applies
no age limit, so old records go in perfectly well — and then the reaper removes
anything past `retention.logs`, seven days by default. Import thirty days into a
default configuration and you watch it evaporate, possibly while the import is still
running. `import` therefore compares the requested range against the configured
retention and **refuses** when the range reaches further back, naming the setting to
raise. Refusing is right here: the alternative is a command that appears to succeed and
silently produces nothing.

**Metrics come back resampled, not raw.** A range query over the Prometheus API returns
points at a `step` — a rendering of the series, not the series. Logs and traces round
trip faithfully; imported metrics are an approximation unless `step` matches the
original interval, and they should not be treated as a source of truth for anything
that cares about exact sample times. Stated here rather than buried in a footnote,
because it is the one place the round trip loses information.

**Import is not idempotent.** telemetryd is append-only with no upsert, so importing an
overlapping range twice stores the records twice. There is no dedup, and adding one
would mean a key and an index that the ingest path deliberately does not carry. The
mitigation is the workflow: import into a fresh data directory, which is what the
debugging case wants anyway. Documented, not solved.

**Pagination is where the work is.** Read APIs cap results per request, so a range is
walked in windows with a cutoff that moves, and export streams rather than buffering a
range in memory. This is the engineering; the format choices above are comparatively
easy.

**Production-shaped data will hit the cardinality limits.** Those already reject with
labelled counters rather than dropping silently, but a transfer that quietly rejected
9% of its records has failed at its job. The summary at the end reports rejections by
reason, and a non-zero count is visible in the exit status.

## Deliberately not built

**Continuous forwarding or mirroring to another backend.** That is an outbound agent —
durable queue, retries, backpressure, its own failure modes — and a different product
from a one-shot transfer. It also contradicts the single-binary posture in a way a
`export`/`import` pair does not. If a copy needs to be continuous, the collector that
is already fanning out to telemetryd is the right place to fan out again.

> **Narrowed by [ADR-013](0013-relay-mode.md).** Half of the reasoning above is wrong on
> the facts: telemetryd already durably stores everything it accepts, so the WAL and the
> sealed segments *are* the outbound queue, and what is missing is a cursor rather than a
> buffer. The rest holds for the purpose argued here — mirroring as a preference is not
> worth a delivery subsystem. It does not hold when forwarding is the point of the
> deployment, as it is for a relay in front of untrusted clients.

**A parser for any other backend's on-disk format.** Covered above: their read APIs are
a contract, their storage layout is not.

**`export --to <url>`.** Covered above: writing to a foreign store is a line worth not
crossing.

## How it will be kept honest

The round trip is the test: ingest a known corpus, export it, import it into a second
instance, and assert the second instance answers the same queries as the first. That
one test covers the format, the pagination, the label handling and the timestamp
handling at once, and it fails loudly if any of them drifts.

Beyond it: a soak phase that imports from a stand-in remote serving the Loki and
Prometheus APIs; an assertion that a range exceeding retention is refused rather than
half-performed; and an assertion that `--progress=json` emits parseable NDJSON on
stderr while stdout stays byte-exact, since that separation is what the desktop app
depends on.

## What was built, and what was not

Logs, both directions, verified by the round trip this ADR called for — and it earned
its place on the first run. The exporter produced matching record *counts* and different
content: `level` came back as `unknown`, because ingest derives it from `severityNumber`
and the exporter carried no severity at all. Counting is not comparing, and a test that
only counts would have passed.

**Two paths, and one of the reasons given for them was wrong.**

`GET /api/v1/export` reads records and hands them to the encoder relay mode already
uses, so what comes out is what was stored. That is new API surface this ADR set out to
avoid, and it earns it on fidelity alone: nothing passes through a query language, so
nothing is re-derived from a rendering of itself. It is the right default against
telemetryd.

The justification originally given for it was not. Two releases claimed that "a trace
search API answers which traces match this, not every trace in this window", and used
that to explain why traces could not be imported from a foreign backend. **It is false.**
Search without a query enumerates the window — that is what Tempo does, and what
telemetryd copied. Checked only after someone asked whether the claim was really true.

So trace import from a foreign backend is built: search enumerates, and each id is
fetched for its spans. It costs N+1 requests per window and is far slower than logs,
which is a reason to prefer the native path and not a reason to pretend the option did
not exist.

**Metrics stay logs-and-traces only from a foreign backend, and that limit is real.** A
range query returns points at whatever `step` was asked for rather than the samples that
were stored. There is no read API for raw samples to fall back on, so this one is a
property of the format rather than of our effort. Have the source send OTLP instead.

## Metrics from a foreign backend

Built, with the instability accepted deliberately and announced on every run.

A range query cannot do it. `step` is a *resolution*, so the timestamps come back on the
step grid rather than as the times the samples were stored, and a finer step yields more
points from the same underlying samples — repetition and shifted timestamps dressed as
fidelity. Shrinking the step and paginating is the obvious idea and it makes the output
worse, not better.

**`/api/v1/read` is the mechanism.** Prometheus' remote read returns "a list of raw
samples matching the requested query" — original timestamps, no evaluation — as
snappy-compressed protobuf, the same family as the `remote_write` this project already
decodes by hand.

Prometheus documents remote read as **not part of the stable API, "subject to change
even between non-major version releases"**. That was the argument for leaving it out, and
it is a real cost rather than a theoretical one — but it is a cost to state, not a reason
to withhold the capability from someone whose source cannot be made to push. So
`import --from --signal metrics` exists, and prints the caveat on every run.

The walk needed a guard the first version did not have. A source that does not narrow its
answer to the requested range makes no progress, and the cursor advances a millisecond at
a time: measured at 15,600 rounds and 46,941 duplicated records before it exhausted the
machine's sockets. A window that returns the same oldest sample as the last one now ends
the walk. This is the second walk in this feature to need that said out loud — a cursor
is only a cursor if it is guaranteed to move.

## Provenance labels on import: no

Whether `import` should accept `--label name=value` to stamp where data came from. It
looked finely balanced when this ADR was written. It is not.

**The case it was meant to serve is better served by a separate data directory.** The
motivating picture was one local store holding data from three connections, with
`source="prod"` to tell them apart. But import is not idempotent — telemetryd is
append-only with no upsert — so the guide already tells you to import into a fresh
directory, and the debugging workflow wants a throwaway store anyway. Provenance is then
the directory, at no cost.

**Where you genuinely want two sources side by side, the label already exists.**
`deployment_environment` and `deployment_environment_name` are default stream labels. If
the telemetry does not carry one, that is an instrumentation gap — which is precisely
what [ADR-001](0001-storage-architecture.md) says to fix at the source rather than paper
over at the boundary.

**And it is not free.** `ingest.stream_labels` is documented as the cardinality
contract; a label on every imported record multiplies streams by the number of sources.
Paying that to avoid a second data directory is the wrong way round.

**Relay mode's stamp is not a precedent for this**, though it looks like one. That
replaces a label the *client* controls with an authenticated fact, because a client that
picks its own `app` can impersonate any other. This would add a label for convenience.
Same mechanism, different justification, and the justification is what the exception
rested on.

The narrow case this does not serve: telemetry that carries no environment, from a
source nobody can change, that must be queried beside another in one store. That is
real, and rare enough to reconsider on evidence rather than to design for now.
