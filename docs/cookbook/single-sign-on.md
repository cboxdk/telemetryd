---
title: "Single sign-on with Cbox ID"
weight: 80
description: "Accept Cbox ID access tokens, and why the identity provider going down does not take telemetryd with it."
---

# Single sign-on with Cbox ID

```toml
[auth.oidc]
issuer = "https://id.example.com"
```

That is the whole configuration. telemetryd reads the signing keys from
`https://id.example.com/.well-known/jwks.json`, caches them, and validates every
presented token itself.

## Getting a token

Tokens come from Cbox ID, not from telemetryd. Ask for the scope that matches what the
holder should be able to do:

| Scope | Opens |
|---|---|
| `telemetry:write` | `/v1/logs`, `/v1/traces`, `/v1/metrics`, `/api/v1/write` |
| `telemetry:read` | the Loki, Tempo and Prometheus read APIs |
| `telemetry:admin` | `/status` and `/metrics` |

A token may carry several. **They are not a hierarchy**: an admin scope does not grant
reads, because running a dashboard is not a reason to read anyone's log lines.

```bash
export TELEMETRYD_AUTH_QUERY_TOKEN="$(cbox-id token --scope telemetry:read)"
telemetryd query '{app="checkout"}'
```

Rename them if they collide with something else on a shared issuer:

```toml
[auth.oidc]
issuer = "https://id.example.com"
scope_read = "acme:telemetry:read"
```

## Turning it on closes open surfaces

An empty `query_token` means "unguarded" only while nothing else guards that surface.
The moment an issuer is set, the read API demands a valid token — and a
`telemetry:admin` token does not satisfy it.

If you were deliberately leaving a surface open, that stops being true here. It is the
safe direction to fail in, but check it before rolling out, not after.

## Static tokens keep working

Enabling this does not turn them off, and it should not: an app server pushing OTLP
wants a token in an environment variable, not an OAuth flow. Static tokens are checked
first, so a machine-to-machine deployment pays nothing for this being switched on.

## When several services share an issuer

Set the audience, and Cbox ID will mint tokens bound to it:

```toml
[auth.oidc]
issuer = "https://id.example.com"
audience = "https://telemetry.example.com"
```

Without it, telemetryd accepts the issuer's own value — which is what Cbox ID puts in
`aud` when no resource was requested. With it, a token minted for another service on
the same issuer cannot be replayed here.

## What happens when Cbox ID is down

**Nothing, for tokens already issued.** telemetryd validates signatures against cached
keys and never asks the provider about a token. That is deliberate: this is the thing
you open when something is broken, and an identity provider that must be reachable to
read your logs is a dependency pointing the wrong way — if Cbox ID is down, you cannot
read the logs that would tell you why.

What stops is Cbox ID issuing *new* tokens, which is its job to worry about.

telemetryd also starts without it. If the key set cannot be fetched at boot it warns and
serves anyway; static tokens are unaffected, and the refresh loop picks the keys up when
the provider returns.

## Key rotation

No restart. Keys refresh on a timer, and a token carrying a key id telemetryd has not
seen triggers an immediate refetch — rate-limited to once a minute, so a stream of
forged key ids cannot become a stream of outbound requests.

## Checking it is working

```bash
curl -sS -H "Authorization: Bearer $ADMIN_TOKEN" localhost:4319/status | jq .auth.oidc
```

```json
{ "issuer": "https://id.example.com", "keys": 2, "keys_stale": false }
```

**`keys: 0` is the thing to alert on.** It means every Cbox ID token is being refused
because the key set never loaded, and no other field would tell you.
`telemetryd_oidc_keys` carries the same number for a dashboard.

## The issuer must be https

telemetryd refuses to start otherwise, loopback aside for testing. It fetches signing
keys from that URL, so anyone able to answer the request over plaintext can mint tokens
this instance will accept.
