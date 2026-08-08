---
title: "ADR-013: Relay mode"
weight: 13
description: "A safe front door for untrusted clients: why the value is stamping identity rather than forwarding bytes, and what the store already gives us for free."
---

# ADR-013: Relay mode

- **Status:** Proposed
- **Date:** 2026-08-08
- **Amends:** [ADR-012](0012-import-and-export.md), which ruled forwarding out
- **Builds on:** [ADR-004](0004-auth-and-network-binding.md), [ADR-011](0011-cbox-id-integration.md)

## Context

Telemetry from a mobile app has a problem that telemetry from an app server does not:
**there is nowhere to keep the credential.**

An app server holds a token in a file only root can read. A mobile binary holds it in
the binary, where anyone with the app and twenty minutes can extract it. So the choices
have been to expose the central backend to the internet behind a shared secret that
will leak, or to build a gateway. Everyone shipping mobile telemetry arrives here.

telemetryd is closer to being that gateway than it looks. It already accepts OTLP over
HTTP, already caps body size and request time, already sheds load with `429` and
`Retry-After` when the ingest queue fills, already bounds cardinality and rejects per
record rather than per request, and — since [ADR-011](0011-cbox-id-integration.md) —
already validates access tokens locally against an issuer's keys.

What it does not do is send anything onward, and one thing it does is actively wrong
for this: **it believes what clients tell it about who they are.**

## The thing that is actually broken

`app` comes out of the client's own payload. `crates/ingest/src/logs.rs` reads it from
the resource attributes, and [ADR-004](0004-auth-and-network-binding.md) says plainly
that the label is a query namespace and not a security boundary.

On a VPS behind a firewall, where every writer is something you deployed, that is a
reasonable trade and the ADR is right. Point it at a fleet of phones and it stops
being reasonable: any holder of any client credential can write records claiming to be
`app="payments"`, and nothing downstream can tell. Dashboards, alerts and retention
policies are all keyed on a label the least trusted party controls.

**So the feature is not forwarding. The feature is that the relay decides who the
client is, and the client does not get a vote.** Everything else here is transport.

## Decision

**A relay mode: accept, store, stamp, ship onward. The same binary, with a shipper
enabled.**

```toml
[relay]
upstream = "https://telemetry.internal"
trust_client_identity = false          # the default in relay mode
```

Not a proxy, and the word matters. A proxy forwards a request while the client waits,
which is precisely wrong for a phone on a train: the client should be acknowledged as
soon as the record is durable locally, and delivery should be somebody else's problem
after that. This is store-and-forward.

Because it is the same binary rather than a separate agent, the edge stays queryable.
Debugging the relay does not mean going to the central store and filtering for one
device — the last few hours are sitting on the relay, answerable through the same Loki,
Tempo and Prometheus APIs as anywhere else.

### Why this reverses ADR-012

ADR-012 ruled out continuous forwarding, and the reasoning there was that it needs a
durable outbound queue with retries and backpressure — a different product from a
one-shot transfer.

The first half of that was wrong on the facts. **telemetryd already durably stores
everything it accepts.** The WAL and the sealed segments *are* the queue; they were
built for crash recovery, and a queue is what crash recovery leaves behind. What is
missing is a cursor and a shipper, not a buffer.

The second half was right about the *old* purpose and does not apply to this one. "Mirror
my data to another backend as well" is a preference, and paying for a delivery
subsystem to satisfy a preference is a bad trade. "Be the only thing my phones are
allowed to talk to" is a security boundary, and that is worth a delivery subsystem.

Same mechanism, different justification, different answer. ADR-012's "deliberately not
built" section should be read as narrowed to the case it actually argued.

### Shipping is per segment, and that is not an accident

The cursor is the last segment delivered, not a timestamp.

A timestamp high-water mark is the obvious design and it is wrong here. Mobile clients
buffer while offline and send when they reconnect, so records routinely arrive with
event times well behind everything already shipped. A cursor over event time would skip
exactly the records that offline devices produce — which, on a mobile fleet, is not an
edge case.

Segments are sealed in **arrival** order, so a late record lands in whichever segment is
open when it turns up, and a cursor over segments delivers it. The property is already
there; it only has to be used.

The cost is latency: nothing ships until a segment seals, and `storage.segment_duration`
defaults to an hour. Relay mode should therefore seal far more often, and the defaults
should say so rather than leaving an operator to discover an hour of lag.

### Identity comes from the credential

With `trust_client_identity = false`, the relay sets `app` from whatever authenticated
the request and discards what the payload claimed.

- A **static token** maps to a configured app name. One token per client application,
  revocable independently, which is the whole reason not to share one.
- A **Cbox ID token** maps from a claim. The relay never has to trust the payload
  because the token was signed by the issuer and validated locally.

This is a write-path transformation, which [ADR-001](0001-storage-architecture.md)
refuses on the grounds that data should be shaped in the instrumentation. That refusal
assumes the instrumentation is yours. Here it is running on a stranger's phone, and
"shape it at the source" is advice to the attacker. The exception is narrow — one
label, set from an authenticated identity, only in relay mode — and it is the same
exception ADR-012 left open for provenance on import, arriving with a much better
argument.

## What has to be decided, not discovered

**Retention must not delete what has not shipped.** The reaper works on age and disk
budget and knows nothing about delivery. Reap an undelivered segment and the data is
gone silently, which is the same failure mode as the retention trap in ADR-012 and just
as easy to ship without noticing. The reaper has to consult the cursor.

**Which then forces the real question: what happens when upstream has been down longer
than the disk budget allows?** Once retention cannot free space, there are exactly two
honest answers, and they are a policy, not an implementation detail:

| | |
|---|---|
| `drop_oldest` | keep accepting, lose the oldest undelivered telemetry, stay available for clients |
| `reject` | stop accepting with `429`, push the problem back to clients that can buffer |

Neither is correct in general. `drop_oldest` suits a mobile fleet, where clients have
little storage and losing old crash reports beats losing new ones. `reject` suits a
relay in front of something that must not lose records. It needs a setting, a metric,
and a loud log line either way — and choosing not to choose means whichever one falls
out of the code becomes the behaviour.

**Delivery is at-least-once, so upstream will see duplicates.** There is no dedup
anywhere in telemetryd, and adding one means a key and an index the ingest path
deliberately does not carry. A shipper that retries after a timeout it cannot
distinguish from a success will re-send. Stated here rather than discovered in a
dashboard showing double the traffic.

**Per-credential rate limiting does not exist.** `limits.ingest_queue_depth` is global:
it sheds load, but it cannot tell one client from another. Against trusted writers that
is fine. Against a fleet, one bad app version — a retry loop shipped to a million
devices — starves every other client through a mechanism that is working exactly as
designed. A relay exposed to untrusted clients needs a per-credential bound, and this
is the largest genuinely new piece of work in this ADR.

**Internet exposure changes the TLS posture.** ADR-004 recommends terminating TLS at a
reverse proxy. A relay taking mobile traffic is internet-facing by definition, so for
this deployment that recommendation is a requirement, and the documentation should not
leave it as a suggestion.

## Deliberately not built

**Fan-out to several upstreams.** One destination. Two destinations means partial
delivery states, per-destination cursors, and a policy for what "delivered" means when
one succeeded and one did not. If a copy needs to go two places, the thing that already
fans out is the right place to do it.

**Transforming, filtering or sampling on the way through.** The identity stamp is one
label set from an authenticated fact, and that is the exception, not the opening of a
pipeline. Sampling belongs in the SDK, where the context to sample well exists.

**Querying upstream through the relay.** The relay answers from what it holds. Making it
a read proxy as well means merging local and remote results, reconciling two retention
windows, and inheriting upstream's availability into every query — which is the coupling
[ADR-011](0011-cbox-id-integration.md) exists to avoid, in a new place.

## How it will be kept honest

The delivery guarantee is the thing to test, and the tests are the unhappy paths:

- upstream refuses connections for the whole run, then recovers: everything accepted is
  eventually delivered, in order, exactly once per shipped segment
- upstream returns `5xx`, then `200`: retried, not dropped, not skipped
- a record arrives with an event time behind the cursor: delivered anyway, which is the
  case a timestamp cursor would silently lose
- retention runs with undelivered segments present: they survive, and the disk-budget
  policy fires with its metric when it cannot free space
- a client claims `app="payments"` with a credential mapped to something else: the
  stamp wins, and this is asserted, because it is the entire security claim
- `kill -9` mid-shipment: the cursor does not advance past what was delivered

The last one is the one to write first. A cursor that advances before delivery is
confirmed turns a crash into silent data loss, and it is the kind of bug that only
appears under a crash nobody arranged.

## Open questions

**Which claim identifies a client application in a Cbox ID token.** `sub` is a user, not
an app, and the relay wants the app. `laravel-id`'s access tokens may carry a client
identifier suitable for this; ADR-011 was written from that source but did not need this
field, so it has not been checked. **This ADR should not be accepted until it has
been** — the mapping is the security boundary, and inventing a claim name that turns out
not to exist would leave a config key that quietly matches nothing.

**Whether relay mode should also lower the retention defaults.** A relay is a waypoint
rather than a store, and keeping seven days of everything on an edge box is probably not
what anyone wants. But a relay that is also the local debugging surface wants *some*
history, and the right number is a guess until someone runs one.
