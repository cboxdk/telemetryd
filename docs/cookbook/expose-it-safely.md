---
title: "Expose it beyond localhost"
weight: 32
description: "telemetryd refuses to start exposed without a token. Here is how to do it properly."
---

# Expose it beyond localhost

telemetryd **refuses to start** on a non-loopback address with no token configured —
including `0.0.0.0` and `[::]`, which are the binds that actually expose people.

That is not an inconvenience to work around. Telemetry routinely contains email
addresses, session identifiers, API tokens in stack traces, and request bodies. An
exposed instance without authentication publishes all of it.

## The recommended shape: reverse proxy

Keep telemetryd on loopback and let something that already does TLS terminate it.

```toml
[server]
listen = "127.0.0.1:4319"
```

```nginx
location / {
    proxy_pass http://127.0.0.1:4319;
    proxy_http_version 1.1;
    # Required for live tail — without these the WebSocket upgrade fails.
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_read_timeout 3600s;
}
```

This is still the best shape at a public edge, where an ingress or load balancer
already holds certificates for several services — not because telemetryd cannot do it,
but because doing it twice means two places for cipher and protocol policy to be right.

## Or: terminate TLS in telemetryd

Where there is no proxy and none is coming — an internal network, one container talking
to another — telemetryd can hold the certificate itself:

```toml
[server.tls]
cert_file = "/etc/telemetryd/tls/server.pem"   # chain, leaf first
key_file  = "/etc/telemetryd/tls/server.key"   # unencrypted
```

Use a certificate from an authority your clients already trust — your internal CA, if
you have one — because that is what makes the connection authenticated as well as
encrypted. telemetryd's outbound side trusts a private authority through
[`tls.ca_file`](../configuration/reference.md#trusting-a-private-ca), so a relay and its
upstream can share one.

With nothing to hand, it will generate its own:

```bash
TELEMETRYD_SERVER_TLS_SELF_SIGNED=telemetry.internal telemetryd serve
```

That encrypts, which closes passive capture — a tap, a mirrored port, another host on
the same network. It does not authenticate: clients cannot tell the certificate is
yours, so they must be told to skip verification, and **that instruction outlives the
certificate**. It tends to stay in client configuration after a real certificate is
installed, leaving the deployment interceptable while looking encrypted. Better than
plain HTTP, and not the destination.

telemetryd does not obtain or renew certificates. Replace the files and restart. A
generated one lasts ten years and is reused across restarts — delete `<data_dir>/tls/`
to get a new one.

## Or: bind wide, with tokens

```toml
[server]
listen = "0.0.0.0:4319"

[auth]
ingest_token = "file:/run/secrets/ingest"
query_token  = ["current-token", "previous-token"]
```

Two independent tokens because the trust boundaries differ: app servers push, humans and
the UI read. Both accept a list, so a token can be rotated without a window where either
the old or the new one is rejected.

Use `file:` or `env:` indirection rather than putting a secret in a world-readable TOML.
telemetryd warns if a token file is readable beyond its owner.

## What `--insecure` actually means

```bash
telemetryd serve --listen 0.0.0.0:4319 --insecure
```

Anyone who can reach that address can read and write your telemetry. It logs a `WARN` on
every start and shows as `insecure: true` in `/status`, so it cannot be forgotten
silently. Reasonable on a trusted private network; never on anything reachable from the
internet.

## What stays open no matter what you configure

Three answers need no credential, and it is worth knowing exactly what they are before
you put this on an address the internet can reach:

- `GET /healthz` — `ok`, and never anything else.
- `GET /` — the identity document and the route table; a page in a browser.
- `GET /status` — the identity document, when the caller sends no admin token. With one,
  the deployment picture, unchanged.

Identity is four constants of the build: `product`, `version`, `storage_format_version`,
`signals`. Nothing that varies with the deployment is in any of them. That includes the
version being published on purpose — the reasoning, and why it is not a setting, is under
[what an unauthenticated caller can see](../configuration/reference.md#what-an-unauthenticated-caller-can-see).

If your edge policy says a version must not leave the building, the proxy already
terminating TLS is the place to enforce it — with `sub_filter` on the response body, or
by returning a fixed document for `GET /` and for an unauthenticated `GET /status`. Know
what it costs before you do: a client that cannot read a version goes back to probing a
list of candidate URLs, which is exactly the behaviour these endpoints exist to remove.

## Verifying

```bash
telemetryd validate
```

Prints the security posture in plain terms — whether the address is reachable from the
network, whether each surface requires a token, and what an unauthenticated caller is
told about the build.
