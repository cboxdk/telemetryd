---
title: "A safe front door for mobile clients"
weight: 90
description: "Accept telemetry from clients you do not trust, decide who they are from their credential, and forward it on."
---

# A safe front door for mobile clients

Telemetry from a mobile app has a problem an app server does not: **there is nowhere to
keep the credential.** A server holds a token in a file only root can read; a mobile
binary holds it in the binary, where anyone with the app and twenty minutes can extract
it.

Relay mode is telemetryd standing in front of your central instance, deciding who each
client is and forwarding what it accepts.

```toml
[relay]
upstream = "https://telemetry.internal"
token    = "file:/run/secrets/upstream"

[[relay.client]]
app   = "mobile-ios"
token = "file:/run/secrets/mobile-ios"

[[relay.client]]
app   = "mobile-android"
token = "file:/run/secrets/mobile-android"
```

## The client does not get a vote

Normally `app` comes out of the payload. Point telemetryd at a fleet of phones and that
stops being acceptable: whoever extracts one token can write records claiming to be
`app="payments"`, and every alert, dashboard and retention rule downstream is keyed on
that label.

In relay mode the label is set from **whatever authenticated the request**, and what
the payload claimed is discarded. A `[[relay.client]]` credential is stamped with its
configured `app`. A Cbox ID token is stamped with its `client_id` claim, which the
issuer reserves against being overwritten.

Each client gets its own credential, so one can be revoked without touching the others —
which is the entire reason not to share one.

`telemetryd_relay_identity_overridden_total` counts records whose claim was overwritten.
A client that keeps claiming someone else's app is either misconfigured or probing, and
either way it is worth seeing before it becomes a support ticket.

If you genuinely trust every writer, `trust_client_identity = true` turns the stamp off.

## A bare ingest token is refused

`auth.ingest_token` carries no identity, so there would be nothing to stamp with.
telemetryd refuses to start on that combination rather than quietly trusting those
writers — that would be a hole in exactly the boundary this mode draws. Move them to
`[[relay.client]]` entries, or say `trust_client_identity = true` and mean it.

## What happens when upstream is down

Nothing is lost. Records are stored locally the moment they are accepted — the client is
answered as soon as they are durable here, not when they reach the far end — and the
shipper retries from where it left off.

Forwarding is per **segment**, in the order they sealed, and the cursor advances only
after upstream confirms. That ordering matters more than it looks: a phone that was
offline sends records with event times well behind everything already forwarded, and a
cursor over *time* would skip exactly the data offline devices produce. Segments seal in
arrival order, so a late record is delivered like any other.

The trade is that delivery is **at-least-once**. A response lost after upstream
committed means the segment is sent again, and telemetryd has no deduplication, so
upstream sees duplicates. Losing data is the worse failure.

## When the disk fills

Retention will not delete what has not been forwarded — that would lose telemetry
nothing anywhere has a copy of. So a long enough outage eventually fills
`storage.disk_budget`, and there are two honest answers:

```toml
[relay]
when_full = "drop_oldest"   # the default
```

| | |
|---|---|
| `drop_oldest` | keep accepting; lose the oldest unforwarded telemetry. Right for phones, which have nowhere to buffer what you refuse. |
| `reject` | stop accepting with `429`; let clients that *can* buffer hold it. Right in front of something that must not lose records. |

Both log an error when they fire, and `drop_oldest` counts what it lost separately from
ordinary budget deletions — "we deleted expired data" and "we deleted data that never
arrived anywhere" are not the same event.

## Watching it

```bash
curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" localhost:4319/status | jq .relay
```

```json
{
  "upstream": "https://telemetry.internal",
  "trust_client_identity": false,
  "when_full": "drop_oldest",
  "pending": { "logs": 2, "traces": 0, "metrics": 0 },
  "segments_delivered": 1184,
  "records_delivered": 9433021,
  "failures": 3,
  "delivered_through": { "logs": "01786226724746643968-00000001" }
}
```

**`telemetryd_relay_pending_segments` is the one to alert on.** Delivered totals only
ever rise, so they cannot tell you the shipper has stopped; a backlog that keeps growing
is the only thing that says so. A few pending segments is normal — that is the seal
interval. A number that climbs for an hour is not.

`failures` counts delivery attempts that were retried, not records lost. Some is normal
on a flaky link; a rate that matches your tick interval means every attempt is failing.

After a restart the counters start again from zero, which is what a counter should do.
`delivered_through` is the field that survives: it is the cursor, read from disk, and it
is how you answer "did this actually get there" without trusting an in-memory number.

The two that mean data was lost live under `retention` rather than here:
`dropped_undelivered` and `blocked_by_undelivered`. Neither should ever be non-zero on a
healthy relay.

## The relay is still queryable

It is the same binary, so the last few hours are sitting right there, answerable through
the same Loki, Tempo and Prometheus APIs. Debugging what a client sent does not mean
going to the central store and filtering for one device.

## Put TLS in front of it

telemetryd speaks plain HTTP and terminates no TLS ([ADR-004](../adr/0004-auth-and-network-binding.md)).
Everywhere else that is a recommendation; a relay taking traffic from phones is
internet-facing by definition, so here it is a requirement. `relay.upstream` must itself
be `https` — telemetryd refuses to start otherwise, because everything it accepts goes
there, along with the upstream credential.
