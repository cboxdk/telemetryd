---
title: "Single sign-on with Cbox ID"
weight: 80
description: "Accept Cbox ID access tokens — or any other OIDC provider's — and why the identity provider going down does not take telemetryd with it."
---

# Single sign-on with Cbox ID

```toml
[auth.oidc]
issuer = "https://acme.cboxid.com"
```

That is the whole configuration. telemetryd reads the signing keys from
`https://acme.cboxid.com/.well-known/jwks.json`, caches them, and validates every
presented token itself. Hosted Cbox ID environments are one subdomain of `cboxid.com`
per tenant; if you run Cbox ID yourself, use your own host.

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
issuer = "https://acme.cboxid.com"
scope_read = "acme:telemetry:read"
```

## Turning it on closes open surfaces

An empty `query_token` means "unguarded" only while nothing else guards that surface.
The moment an issuer is set, the read API demands a valid token — and a
`telemetry:admin` token does not satisfy it.

If you were deliberately leaving a surface open, that stops being true here. It is the
safe direction to fail in, but check it before rolling out, not after.

## Bearer tokens, not DPoP

Cbox ID can bind an access token to a key the client holds (DPoP, RFC 9449), putting the
thumbprint in a `cnf` claim so a stolen token alone is useless.

**telemetryd refuses those tokens.** It does not validate DPoP proofs, and accepting a
bound token as an ordinary bearer would hand back exactly the property the binding was
bought for — so it fails closed rather than quietly downgrading.

If DPoP is on for the client you point at telemetryd, every request gets a `401` with an
empty body, because a 401 that explains itself is a hint to whoever is guessing. The
reason goes to the log instead, once, at `warn`:

```
refusing a sender-constrained (DPoP) access token: telemetryd cannot validate
the proof, and accepting it as a plain bearer would discard the binding.
```

Issue plain bearer tokens for telemetryd. Validating the proofs is future work.

## It must be an access token

RFC 9068 gives access tokens the media type `at+jwt`, and Cbox ID sets it on every one
it mints. An **id token** says `JWT`, is signed by the same key, and authorises nothing —
so it is refused, with the same empty 401 and a `warn` line naming what arrived.

A token with no `typ` at all is accepted; not every issuer sets one, and `aud` and
`scope` still have to hold.

## Static tokens keep working

Enabling this does not turn them off, and it should not: an app server pushing OTLP
wants a token in an environment variable, not an OAuth flow. Static tokens are checked
first, so a machine-to-machine deployment pays nothing for this being switched on.

## When several services share an issuer

Set the audience, and Cbox ID will mint tokens bound to it:

```toml
[auth.oidc]
issuer = "https://acme.cboxid.com"
audience = "https://telemetry.example.com"
```

Without it, telemetryd accepts the issuer's own value — which is what Cbox ID puts in
`aud` when no resource was requested. With it, a token minted for another service on
the same issuer cannot be replayed here.

## Another provider than Cbox ID

Nothing here is specific to Cbox ID — telemetryd validates a standards-shaped access
token, so any OIDC provider that mints one will work. Two things are *not* universal,
so they are settings rather than assumptions.

**Where the keys live.** The default derives `{issuer}/.well-known/jwks.json`, which is
a convention, not a rule. A provider's discovery document names the real location in its
`jwks_uri`, and if it differs, say so:

```bash
curl -s https://<issuer>/.well-known/openid-configuration | jq -r .jwks_uri
```

```toml
[auth.oidc]
issuer   = "https://accounts.google.com"
jwks_url = "https://www.googleapis.com/oauth2/v3/certs"   # nowhere near the issuer
```

**Which claim carries the scopes.** OAuth specifies `scope`, a space-separated string.
Some providers use `scp` instead, and some send an array rather than a string —
telemetryd accepts either shape, but it has to be told the name:

```toml
scope_claim = "scp"
```

Both are `https`-only for the same reason the issuer is: whoever answers that request
decides which keys mint valid admin tokens. Loopback is exempt, for testing.

### What "works" does and does not mean

A provider working means telemetryd can verify its signatures and trust its issuer. It
does **not** mean the provider can express *these* scopes. Google is the useful example:
the key set above fetches fine, but a Google ID token carries no scope claim at all, so
there is nothing to map `telemetry:read` onto. You would be authenticating a user and
then granting them nothing.

So the question to ask of a provider is not "can telemetryd read its keys" but "can I
mint a token that carries a claim naming the access I want". A provider with
configurable scopes or custom claims — Cbox ID, Entra, Auth0, Keycloak — can. A
consumer sign-in provider generally cannot, and for those, static tokens remain the
straightforward answer.

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
{ "issuer": "https://acme.cboxid.com", "keys": 2, "keys_stale": false }
```

**`keys: 0` is the thing to alert on.** It means every Cbox ID token is being refused
because the key set never loaded, and no other field would tell you.
`telemetryd_oidc_keys` carries the same number for a dashboard.

## If you also run relay mode

Relay mode stamps each record's `app` label from the credential rather than the payload,
and for a Cbox ID token that comes from the **`client_id`** claim — the registered OAuth
client, which the issuer reserves against being overwritten by enrichment hooks. `sub` is
a *user*, which is not what an application name means.

So the client each mobile app authenticates as is what its telemetry is labelled with.
See the [relay guide](relay-mode.md).

## The issuer must be https

telemetryd refuses to start otherwise, loopback aside for testing. It fetches signing
keys from that URL, so anyone able to answer the request over plaintext can mint tokens
this instance will accept.
