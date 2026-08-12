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

telemetryd runs with none, but a server deployment wants tokens — the service will be
reachable through nginx, and the bind check that protects you on a laptop does not know
that.

```bash
sudo mkdir -p /etc/telemetryd
sudo tee /etc/telemetryd/telemetryd.toml >/dev/null <<'TOML'
[server]
listen = "127.0.0.1:4319"

[auth]
# One per writer, so a leak is revoked on its own. `file:` keeps the value out of
# this file and out of `ps`.
ingest_token = ["file:/etc/telemetryd/ingest.token"]
query_token  = ["file:/etc/telemetryd/query.token"]
admin_token  = ["file:/etc/telemetryd/admin.token"]

[retention]
logs    = "14d"
traces  = "14d"
metrics = "90d"

[storage]
# Forge's default disk is 40–80 GB and the application needs most of it.
disk_budget = "8GiB"
TOML

for name in ingest query admin; do
  openssl rand -base64 24 | tr -d '\n' | sudo tee /etc/telemetryd/$name.token >/dev/null
  sudo chmod 600 /etc/telemetryd/$name.token
done
```

Check it before starting anything. `validate` prints every resolved value and where it
came from, which is the fastest way to find a typo in a path:

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

The generated unit runs as a dedicated `telemetryd` user with `StateDirectory`,
`ProtectSystem=strict`, `NoNewPrivileges` and `PrivateTmp`, and gives SIGTERM 30 seconds
— telemetryd flushes its write-ahead log and seals the open segment on the way out, and
cutting that short turns a clean stop into a crash recovery on the next boot.

```bash
sudo systemctl status telemetryd
curl -fsS http://127.0.0.1:4319/healthz
```

Then make the token available to the tokens' owner:

```bash
sudo chown telemetryd:telemetryd /etc/telemetryd/*.token
```

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
