---
title: "ADR-011: Integrating with Cbox ID"
weight: 11
description: "Validate tokens locally against a key set, never by asking the provider — and why that distinction matters more here than elsewhere."
---

# ADR-011: Integrating with Cbox ID

- **Status:** **Proposed.** The design is settled; taking the dependency is not.
- **Date:** 2026-08-08
- **Builds on:** [ADR-004](0004-auth-and-network-binding.md)

## Context

telemetryd authenticates with static bearer tokens in three roles: write, read, admin.
That is right for one team on one VPS, and it stops being right the moment there are
people rather than services — no per-user identity, no revocation short of rotating a
shared secret, no audit of who read what.

[Cbox ID](https://github.com/cboxdk/laravel-id) is a self-hostable identity platform
and an OAuth/OIDC provider. Its source carries `JwtTokenIssuer` and
`JwtTokenIntrospector`, so it issues JWTs and supports [RFC 7662][introspection]
introspection. Either could authenticate a telemetryd request.

[introspection]: https://www.rfc-editor.org/rfc/rfc7662

## Decision

**Validate tokens locally against the provider's public keys. Do not call the provider
per request.**

The two options are not equivalent here, and the difference is not performance.

**Introspection makes the identity provider a hard runtime dependency of reading your
telemetry.** telemetryd is the thing you open when something is broken. If Cbox ID is
down, or slow, or unreachable from the telemetryd host, introspection means you cannot
read the logs that would tell you why. That is precisely backwards for an observability
backend, and it is the kind of coupling that is invisible until the day it matters.

Local validation inverts it: telemetryd fetches a key set periodically and validates
signatures itself. The provider being down stops new tokens being *issued* — correct,
that is its job — but every token already in a browser or a config file keeps working.
An observability backend should degrade toward remaining readable.

## What it would look like

- `auth.oidc.issuer` — discovery via `/.well-known/openid-configuration`, from which
  the key set URL is read rather than configured separately.
- Keys cached, refreshed on a timer, and refreshed once on an unknown key id, so key
  rotation does not need a restart. A refresh failure keeps serving the previous keys:
  see above.
- **Algorithms pinned to an allow-list.** Accepting whatever the token's header asks
  for is how `alg: none` and RS256-verified-as-HS256 confusion happen. This is not
  optional and it is not configurable.
- `iss`, `aud` and `exp` all checked, with a small fixed clock skew allowance.
- Scopes map to the roles that already exist: `telemetry:write`, `telemetry:read`,
  `telemetry:admin`. The role split is already built and tested, so this is a second
  way of arriving at a role rather than a new authorisation model.
- Static tokens keep working alongside it. Machine-to-machine ingest from an app server
  does not want an OIDC flow, and forcing one would be worse than the problem.

## What it costs, honestly

**A JWT library, as a runtime dependency.** The project rule is that those are
confirmed rather than assumed, and this one is the reason this ADR is Proposed rather
than Accepted. It is also not negotiable in the other direction: signature and claim
verification is exactly the code you must not hand-roll, so the choice is "take a
vetted dependency" or "do not build this".

**A second authentication path to keep correct.** Every route already has a role; this
adds a second way to satisfy it. That is the smallest version of this feature, and it
is still a real increase in the surface where a mistake means letting the wrong person
in.

**Operational surface.** Key fetching, cache expiry, clock skew, and a new class of
failure that produces 401s for reasons outside telemetryd. All of it needs to be
visible in `/status` and `/metrics`, or the first incident is spent guessing.

## What is not decided

Whether to build it at all. Static tokens with three roles is a complete answer for the
deployment this is designed for — one team, one host. This becomes worth its cost when
there are named humans whose access should be revoked individually, or when an audit
trail of *who queried what* is required.

## What would be needed from Cbox ID

Recorded so this is not designed against assumptions, the way the UI compatibility work
was not ([ADR-005](0005-compatibility-audit.md)):

- a reachable discovery document, and confirmation that the key set is published there
- the signing algorithm actually used, so the allow-list pins the real one
- the scope names it issues, and whether they can be shaped to the three roles above
- whether tokens carry an `aud` that telemetryd can require

Until those are read from the source rather than guessed, this stays Proposed.
