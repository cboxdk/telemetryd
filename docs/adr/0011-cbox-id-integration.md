---
title: "ADR-011: Integrating with Cbox ID"
weight: 11
description: "Validate tokens locally against a key set, never by asking the provider — and why that distinction matters more here than elsewhere."
---

# ADR-011: Integrating with Cbox ID

- **Status:** **Accepted and built.**
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

## The shape

- `auth.oidc.issuer` is the only required setting. The key set is read from
  `{issuer}/.well-known/jwks.json`, which is where Cbox ID's `KeyManager` publishes it.
  **No discovery document is fetched.** An earlier draft of this ADR proposed reading
  `jwks_uri` out of `/.well-known/openid-configuration`; against one known issuer that
  is a second network dependency and a second thing to be down, to learn a path this
  document already records.
- Keys cached, refreshed on a timer, and refreshed once on an unknown key id, so key
  rotation does not need a restart. A refresh failure keeps serving the previous keys.
- **The algorithm is never taken from the token.** The header supplies a key id; the
  algorithm comes from the key that id selects. A token asking for `alg: none`, or for
  HS256 against an RSA key, finds no key that agrees. This is not configurable.
- `iss`, `aud` and `exp` all checked, with a configurable clock skew allowance.
- Scopes map to the roles that already exist: `telemetry:write`, `telemetry:read`,
  `telemetry:admin`. The role split is already built and tested, so this is a second
  way of arriving at a role rather than a new authorisation model.
- Static tokens keep working alongside it. Machine-to-machine ingest from an app server
  does not want an OIDC flow, and forcing one would be worse than the problem.

## What it costs, honestly

**A JWT library, as a runtime dependency.** `jsonwebtoken`, and through it `ring` for
signature verification: thirteen crates, 252 to 265 in the SBOM. Confirmed rather than
assumed, per the project rule. It was also not negotiable in the other direction —
signature and claim verification is exactly the code you must not hand-roll, so the
choice was "take a vetted dependency" or "do not build this".

`ring` brings ISC, which the licence allow-list did not carry. ISC is permissive and
OSI-approved, and the project's own policy already names it; the list was incomplete
rather than the dependency wrong.

**A second authentication path to keep correct.** Every route already has a role; this
adds a second way to satisfy it. That is the smallest version of this feature, and it
is still a real increase in the surface where a mistake means letting the wrong person
in.

**Operational surface.** Key fetching, cache expiry, clock skew, and a new class of
failure that produces 401s for reasons outside telemetryd. All of it needs to be
visible in `/status` and `/metrics`, or the first incident is spent guessing.

## What Cbox ID actually does

Read from its source before anything was written, the way the UI compatibility work
was not ([ADR-005](0005-compatibility-audit.md)). Every one of these changed the
design:

| | |
|---|---|
| Signing | `JwtTokenIssuer` mints **RS256** by default; `SigningAlg` also offers ES256 and EdDSA |
| Key set | `KeyManager::jwks()` publishes at **`/.well-known/jwks.json`** |
| Key id | The `kid` is in the token header, so a key is *selected* rather than searched for |
| `aud` | **Always present** — the requested resource, or the issuer itself, because RFC 9068 §2.2 requires it |
| `scope` | A space-separated string claim |
| Lifetime | 900 seconds by default — short, because the token carries authorization |

Two of those shaped the implementation directly. Because `aud` is always set,
telemetryd can *require* it rather than treating it as optional, which closes the
confused-deputy case where a token minted for another resource server on the same
issuer is replayed here. And because `kid` is always present, key selection never has
to fall back to trying every key — which is the fallback that makes algorithm
confusion possible.

Cbox ID's own verifier builds its candidate key set from keys whose algorithm is
allowed, and never trusts the algorithm named in the token. telemetryd does the same
thing from the other side. That the two arrived at the same rule independently is worth
noting: it is the rule.

## What was built

Roughly 300 lines in `crates/server/src/oidc.rs`, plus configuration and wiring.

- Keys fetched from the well-known path, cached, refreshed on a timer, and refreshed
  once on an unseen `kid` — rate-limited to a minute, so a stream of forged key ids
  cannot become a stream of outbound requests.
- **A fetch failure keeps the cached keys.** A provider that goes down stops new tokens
  being issued; it does not stop telemetryd answering.
- **Startup does not require the provider.** If the key set cannot be loaded, telemetryd
  warns and serves anyway. Refusing to start would be the coupling this ADR exists to
  avoid, in its worst form.
- Scopes are matched *whole* against the space-separated claim, so `telemetry:readonly`
  does not satisfy `telemetry:read`.
- `auth.oidc.issuer` must be `https`, or loopback for testing. Keys fetched over
  plaintext can be substituted in flight, and whoever substitutes them mints their own
  admin tokens.
- Static tokens are checked first and in constant time, so enabling this costs a
  static deployment nothing.
- `/status` reports the issuer and key count; `telemetryd_oidc_keys` is the metric to
  alert on. Zero keys means every Cbox ID token is being refused, and nothing else in
  the response would say so.

## How it is kept honest

Fourteen tests, the important ones signing genuine tokens with a real key and running
them through the real path — key parsing, algorithm selection, claim validation and
scope mapping all have to agree, and each is easy to get individually right and jointly
wrong. Among them:

- an HS256 token presented against an RSA key: the algorithm-confusion attack, refused
  because the algorithm comes from the key
- a token from another issuer, and one minted for another audience
- an expired token, and one carrying no `aud` at all
- `telemetry:readonly` not satisfying `telemetry:read`

Verified end to end against a stand-in issuer publishing a real JWKS: `telemetry:read`
opens the query API and not `/status`, `telemetry:admin` the reverse, expired tokens
nothing.
