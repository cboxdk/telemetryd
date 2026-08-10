---
title: "ADR-013: Relay mode"
weight: 13
description: "A safe front door for untrusted clients: why the value is stamping identity rather than forwarding bytes, and what the store already gives us for free."
---

# ADR-013: Relay mode

- **Status:** **Accepted and built.**
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
- A **Cbox ID token** maps from its **`client_id`** claim. Read out of
  `JwtTokenIssuer`: every access token carries it, both grants funnel through the one
  `issue()` that sets it from the registered client, and it is on the reserved list
  that `applyEnrichment()` refuses to overwrite — so no hook can forge it. RFC 9068
  names the same claim, so this is the standard field rather than a local invention.

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

**One client could starve the others — since fixed, and not the way this ADR expected.**
`limits.ingest_queue_depth` is global: it sheds load but cannot tell one client from
another, so a retry loop shipped to a fleet takes the whole queue.

This ADR called for a per-credential *rate* limit and called it the largest piece of
work here. Building it showed the shape was wrong. The identity is the **application**,
not the device — a million phones present one credential — so a requests-per-second
ceiling would have to be guessed against fleet size, and would throttle the entire fleet
to contain one bad version of it.

`relay.max_queue_share` caps how much of the queue any one identity holds at once
instead. It needs no guessed number, it scales with `ingest_queue_depth` on its own, and
the map of active clients is bounded by construction: an entry exists only while that
client has a request in flight, and there can never be more of those than there are
permits.

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

## What one request per segment cost

Shipping a whole segment in one request was the first design, and it could not work.
`storage.max_segment_bytes` defaults to 256 MiB while a receiving telemetryd's
`server.max_body_bytes` defaults to 16 MiB, and the OTLP encoding of a segment is larger
than the Parquet it came from — so the first sealed segment would be refused, the cursor
would never advance, and the relay would retry it forever while the backlog grew until
the disk budget began discarding telemetry.

It was measured rather than reasoned about: 712 KB against a 50 KB ceiling gave twelve
failures, zero deliveries, and a backlog that never drained. Segments are now split into
requests bounded by `relay.max_request_bytes`, and a `413` halves the batch and retries —
so a receiver whose limit is tighter than ours self-corrects instead of wedging. The
accepted size is remembered, because the first attempt at that cost 45 rejected requests
against 48 useful ones.

The lesson worth keeping: the failure needed no unusual configuration. It needed the
defaults.

## How it is kept honest

Built as described. The delivery guarantee is the thing to test, and the tests are the
unhappy paths — the soak runs them against the real binary with a stand-in upstream it
can switch off:

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

The soak found one defect immediately, and it was an interaction no unit test could
see: registering relay clients closed the *read* API, because the check for "is this
surface unguarded" counted ingest credentials against every surface. The read API
demanded a token, and nothing in the configuration could satisfy it.

## Sender-constrained tokens change the premise

The context above says a mobile binary cannot keep a credential. That is true of a
bearer token and **not** true of what Cbox ID can already issue: `JwtTokenIssuer`
supports DPoP (RFC 9449), binding a token to a key the client generates and holds, and
stamping the thumbprint into a `cnf` claim. A token lifted out of a binary is then
useless without the key that never left the device.

That does not remove the need for the identity stamp — a client with a perfectly valid,
perfectly bound token can still lie about `app` — but it changes how bad a leaked
credential is, and it makes per-client credentials genuinely enforceable.

It also surfaced a defect in what is already shipped, fixed alongside this ADR rather
than waiting for relay mode: telemetryd ignored `cnf` entirely, so it accepted a
sender-constrained token as a plain bearer and silently handed back the exact property
the binding was bought to provide. It now refuses such tokens instead. Validating DPoP
proofs — and thereby *accepting* bound tokens — is work relay mode should do, and is
the strongest available answer to the credential problem this ADR opens with.

## Retention defaults in relay mode: unchanged

A relay is a waypoint rather than a store, so keeping seven days of everything on an
edge box looks wasteful, and lowering the defaults when `relay.upstream` is set is the
obvious move.

**It is the wrong one, because it makes a setting mean different things depending on
another setting.** `retention.logs = 7d` should be seven days whether or not this
instance forwards anything. A default that silently changes underneath a second,
unrelated switch is how "why is my data gone" happens, and the operator who most needs
to trust that number is the one running an edge box they cannot easily inspect.

Two things also make the saving smaller than it looks. Undelivered segments are held
back from the reaper regardless of the window, so retention does not govern the path
that matters. And `storage.disk_budget` with `relay.when_full` is the real bound on an
edge box — a ceiling in bytes, which is what a small disk actually has, rather than a
window in days.

So: unchanged, and the guide says plainly that a relay operator will usually want to
lower them by hand. The ADR's own note said the right number was a guess until someone
ran one; guessing it into a default was the part worth declining.
