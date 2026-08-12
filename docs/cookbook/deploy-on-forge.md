---
title: "Deploy on a Forge server"
weight: 35
description: "Install telemetryd beside an application on a Laravel Forge box, run it as a service, and put nginx and Let's Encrypt in front of it."
---

# Deploy on a Forge server

Forge gives you an Ubuntu box with nginx, a `forge` user and Let's Encrypt already
wired up. telemetryd needs none of that for itself — it is one binary on one port — so
this is short: install it, run it as a service on loopback, and let the nginx that is
already there terminate TLS.

Ten minutes, and nothing here is Forge-specific except which buttons you press.

## 1. Install the binary

SSH in as `forge` and run:

```bash
curl -fsSL https://raw.githubusercontent.com/cboxdk/telemetryd/main/install.sh | sh
```

It verifies the release checksum, and the signature too if `cosign` is on the box.

## 2. Write a configuration

```bash
sudo telemetryd init
```

That writes `/etc/telemetryd/telemetryd.toml` with three generated tokens — one per
surface, so a leaked write token does not open reads — and prints them once. The values
live in `0600` files beside the config and are referenced by path, which keeps them out
of the file people paste into issues and out of `ps`.

Re-running is refused rather than silently rotating every token and locking out
everything that holds one. `--force` if that is genuinely what you want.

Edit the retention windows and the disk budget to suit the box — Forge's default disk is
40–80 GB and the application needs most of it — then check what it resolved to:

```bash
telemetryd validate
```

## 3. Run it as a service

telemetryd writes the unit for the machine it is on rather than asking you to copy one
from a wiki:

```bash
telemetryd service print          # read it first
sudo telemetryd service install
```

By default it runs as a dedicated `telemetryd` system user, created for you if it does
not exist — least privilege, and it cannot read the application's files. On a Forge box
where `forge` already owns everything and you would rather not add an account:

```bash
sudo telemetryd service install --user forge
```

A user you name yourself is never created. An unknown one is a typo, and inventing an
account from a misspelling is worse than refusing.

The generated unit runs as that user with `StateDirectory`,
`ProtectSystem=strict`, `NoNewPrivileges` and `PrivateTmp`, and gives SIGTERM 30 seconds
— telemetryd flushes its write-ahead log and seals the open segment on the way out, and
cutting that short turns a clean stop into a crash recovery on the next boot.

```bash
sudo systemctl status telemetryd
curl -fsS http://127.0.0.1:4319/healthz
```

`install` also hands the token files `init` wrote to that account — they are `0600` and
owned by whoever ran `init`, and the service is somebody else. That was a real failure
before it was a step: the unit started, could not read its own tokens, and exited.

## 4. Put nginx in front of it

In Forge, add a site for the hostname you want — `telemetry.example.com` — and issue a
Let's Encrypt certificate for it from **SSL → LetsEncrypt**. Then replace the site's
nginx configuration (**Site → Edit Nginx Configuration**) so the server block proxies to
telemetryd:

```nginx
location / {
    proxy_pass http://127.0.0.1:4319;
    proxy_http_version 1.1;

    # Live tail is a WebSocket. Without these two headers it fails with a 400 that
    # looks like a query problem.
    proxy_set_header Upgrade    $http_upgrade;
    proxy_set_header Connection $connection_upgrade;

    proxy_set_header Host              $host;
    proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;

    # A tail connection is meant to stay open.
    proxy_read_timeout 3600s;

    # Ingest batches are larger than nginx's 1 MB default, and a rejection here is a
    # 413 from nginx that never reaches telemetryd's own limit.
    client_max_body_size 32m;
}
```

`$connection_upgrade` needs a map, which Forge's default site does not include. Add it
once at the top of the file, outside the `server` block:

```nginx
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}
```

Reload nginx from the Forge UI, or `sudo nginx -t && sudo systemctl reload nginx`.

## 5. Point the application at it

In the Laravel app on the same box, or on any other:

```env
TELEMETRY_OTLP_ENDPOINT=https://telemetry.example.com
TELEMETRY_OTLP_TOKEN=<the ingest token>
```

Confirm it arrives rather than assuming — a `200` from an exporter is not proof, because
telemetryd rejects per record and reports what it dropped in the response body:

```bash
curl -G https://telemetry.example.com/loki/api/v1/query_range \
  -H "Authorization: Bearer $(sudo cat /etc/telemetryd/query.token)" \
  --data-urlencode 'query={app="your-app"}'
```

## What to watch afterwards

```bash
telemetryd status --token "$(sudo cat /etc/telemetryd/admin.token)"
```

Two numbers matter more than the rest. `deleted_by_budget` above zero means the disk
budget and the retention window are fighting and data you asked to keep is being
deleted — [size the disk budget](size-the-disk-budget.md) has the arithmetic.
`segments_unreadable` above zero means damage, which costs that segment and nothing
else, but is worth knowing about.

## If it does not work

- **502 from nginx** — telemetryd is not listening. `sudo systemctl status telemetryd`,
  then `journalctl -u telemetryd -n 50`. A configuration error stops it at startup and
  says which setting on the way out.
- **401 on every request** — the token file is unreadable by the `telemetryd` user, or
  the header is missing. `telemetryd validate` shows which files it resolved.
- **400 on live tail** — the `Upgrade` and `Connection` headers, or the missing
  `map` block above.
- **413 on ingest** — `client_max_body_size`, before telemetryd ever sees the request.
- **Empty query results** — work through [diagnose a query that returns
  nothing](diagnose-empty-results.md), which starts from the most common cause rather
  than the most interesting one.
