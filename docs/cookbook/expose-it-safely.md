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

telemetryd does not terminate TLS, on purpose: shipping TLS means shipping certificate
lifecycle management, and it would put a TLS stack into a binary whose static-linking
story is a product constraint.

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

## Verifying

```bash
telemetryd validate
```

Prints the security posture in plain terms — whether the address is reachable from the
network, and whether each surface requires a token.
