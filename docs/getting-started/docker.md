---
title: "Docker"
weight: 14
description: "Run telemetryd as a container with no configuration file — what the defaults are, how the generated tokens work, and how to set every option from the environment."
---

# Docker

```bash
docker run -d -p 4319:4319 -v telemetryd-data:/var/lib/telemetryd \
  ghcr.io/cboxdk/telemetryd:latest
```

That is a working instance. No configuration file, no flags.

## The first thing it prints is a set of tokens

telemetryd refuses to listen on an address reachable from outside the machine with no
authentication configured ([ADR-004](../adr/0004-auth-and-network-binding.md)), and a
container binds `0.0.0.0` by definition. The two obvious ways to make a zero-config
image are both bad: fail to start, or default to `insecure` and serve everyone's
telemetry to anyone who can reach the port.

So on its **first** start the image generates three tokens, stores them next to the data
and prints them once:

```
  ────────────────────────────────────────────────────────────────────────
  No authentication was configured, so telemetryd generated its own.
  ...
    ingest (write telemetry)  8Kq2m-vX...
    query  (read telemetry)   pR4nT9wz...
    admin  (/status, /metrics) Lf7cH2ae...
  ────────────────────────────────────────────────────────────────────────
```

```bash
docker logs <container> 2>&1 | head -20     # they are printed once, at first start
```

They live in `/var/lib/telemetryd/generated-tokens.env` and are reused on restart.
Delete that file to get new ones.

**Set your own for anything that is not a laptop.** Supplying any
`TELEMETRYD_AUTH_*_TOKEN`, mounting a config file, or setting
`TELEMETRYD_SERVER_INSECURE` skips the generation entirely.

## The port is plaintext

telemetryd terminates no TLS of its own ([ADR-004](../adr/0004-auth-and-network-binding.md)),
and this image changes nothing about that — it binds `0.0.0.0:4319` speaking plain HTTP.

That is fine in plenty of places and it is worth knowing which. On a laptop, on a
private network, or between containers on the same Docker network, plain HTTP costs you
nothing. What crosses the wire is different once the port is published to a public
address: the generated token above is a bearer credential, and on an unencrypted
connection it is readable by anything on the path — along with every log line and trace
you send. Someone who reads it can write telemetry as you, and read yours.

So it is a matter of practice rather than a hard stop: **terminate TLS in front when the
port is reachable from the internet.** The `docker run` at the top of this page is the
convenient thing, not the hardened thing, and it does not stop you doing either.

Let a proxy hold the certificate:

```yaml
services:
  telemetryd:
    image: ghcr.io/cboxdk/telemetryd:latest
    expose: ["4319"]          # reachable from the proxy, not from the host
  caddy:
    image: caddy:2
    ports: ["443:443"]
    # Caddyfile: telemetry.example.com { reverse_proxy telemetryd:4319 }
```

The relay cookbook puts it more strongly, and should: a relay taking mobile traffic is
internet-facing by definition, so there it is not a matter of taste — see
[relay mode](../cookbook/relay-mode.md).

## Configuring it

Every configuration key is available as an environment variable, named
`TELEMETRYD_<SECTION>_<KEY>`. Precedence is **defaults → `telemetryd.toml` → environment
→ CLI flags**, so a mounted file can carry the bulk of the configuration while compose
overrides a few values, and neither fights the other.

```yaml
services:
  telemetryd:
    image: ghcr.io/cboxdk/telemetryd:latest
    ports: ["4319:4319"]
    volumes: ["telemetryd-data:/var/lib/telemetryd"]
    environment:
      TELEMETRYD_RETENTION_LOGS: "24h"
      TELEMETRYD_STORAGE_DISK_BUDGET: "2GiB"
      TELEMETRYD_STORAGE_SEGMENT_DURATION: "1m"
      TELEMETRYD_AUTH_INGEST_TOKEN: "..."
```

The full list is in the [configuration reference](../configuration/reference.md). Single
sign-on and relay mode are configurable this way too —
`TELEMETRYD_AUTH_OIDC_ISSUER`, `TELEMETRYD_RELAY_UPSTREAM` and the rest.

The one thing the environment cannot express is `[[relay.client]]`, which is a list of
tables. Mount a config file for that; its values are credentials, so a mounted secret is
where they belong anyway.

### Defaults the image sets

| | |
|---|---|
| `TELEMETRYD_SERVER_LISTEN` | `0.0.0.0:4319` — a container port is useless bound to loopback |
| `TELEMETRYD_STORAGE_DATA_DIR` | `/var/lib/telemetryd`, a named volume |
| `TELEMETRYD_LOG_FORMAT` | `json`, because something is collecting these |

Everything else is telemetryd's own default, unchanged. Retention is still seven days
and the disk budget still 10 GiB — deliberately, since a default that means something
different in a container is how "why is my data gone" happens.

## What runs inside

`cbox-init` is PID 1, as in the Cbox base images. Here it supervises a single process
rather than several, so what it buys is narrower: SIGTERM reaches telemetryd instead of
Docker eventually killing the container, anything orphaned is reaped, and the container
has the same shutdown and inspection surface as the rest of the fleet.

That shutdown path matters more than it sounds. telemetryd flushes its WAL and seals the
open segment on SIGTERM; cutting that short turns a clean stop into a crash recovery on
the next boot. `cbox-init`'s shutdown timeout is set to 45s for that reason — above
`server.shutdown_grace`, which governs draining in-flight requests.

The binary is statically linked and verifies outbound TLS against roots compiled into
it, so the image installs no `ca-certificates` and cannot behave differently depending
on whether a system trust store happens to be present.

## What the image contains

The binary is **not** rebuilt for the image. It is the same musl artifact the release
workflow builds, soak-tests and signs, copied in after its checksum is verified against
the release's published `SHA256SUMS`. Building it separately here would ship an image
whose binary nothing had verified.

Images are published to `ghcr.io/cboxdk/telemetryd` for `linux/amd64` and `linux/arm64`,
tagged with the version and `latest`, with build provenance and an SBOM attached, and
signed with cosign:

```bash
cosign verify ghcr.io/cboxdk/telemetryd:latest \
  --certificate-identity-regexp 'https://github.com/cboxdk/telemetryd/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Building it yourself

```bash
python3 scripts/docker-context.py local --arch arm64
docker build -t telemetryd:dev ./docker
```

The binary that produces is unsigned and has not been through the release soak — fine
for trying a change, not what you should deploy.
