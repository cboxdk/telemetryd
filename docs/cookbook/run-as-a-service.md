---
title: "Run it as a service"
weight: 31
description: "systemd and launchd, with a hardened unit telemetryd generates for you."
---

# Run it as a service

## Let telemetryd do it

```bash
telemetryd service install
```

Writes the unit for your platform, reloads the service manager, and starts it. Remove it
again with `telemetryd service uninstall` — which **leaves the data directory alone**,
because removing a service should not delete the telemetry it collected.

telemetryd never invokes `sudo` on your behalf. If the unit path needs root it prints
the exact command to run instead.

## Do it by hand

```bash
telemetryd service print | sudo tee /etc/systemd/system/telemetryd.service
sudo useradd --system --no-create-home telemetryd
sudo systemctl daemon-reload
sudo systemctl enable --now telemetryd
```

On macOS:

```bash
telemetryd service print > ~/Library/LaunchAgents/dk.cbox.telemetryd.plist
launchctl load -w ~/Library/LaunchAgents/dk.cbox.telemetryd.plist
```

## What the generated unit does

The systemd unit is locked down: `NoNewPrivileges`, `ProtectSystem=strict`,
`PrivateTmp`, `MemoryDenyWriteExecute`, a `SystemCallFilter`, and address families
restricted to IPv4 and IPv6. telemetryd needs one directory and one socket; everything
else is denied. A telemetry backend is an attractive target precisely because everything
sends it data.

Two settings matter for correctness rather than security:

```ini
KillSignal=SIGTERM
TimeoutStopSec=30s
```

`SIGTERM` triggers a graceful drain and a final write-ahead log flush. Killing telemetryd
before that finishes costs the unsynced window — up to `wal_sync_interval` of telemetry.

## Where data goes

The unit sets `TELEMETRYD_STORAGE_DATA_DIR=/var/lib/telemetryd` and uses systemd's
`StateDirectory`. Verify the resolved path with:

```bash
telemetryd validate
```
\n
## Hand this to an agent

A self-contained brief. It names only commands and endpoints that exist, so an agent can
execute it without reading the rest of this page — and without inventing the parts of the
Loki and Prometheus APIs telemetryd deliberately does not implement.

````markdown
# Task: run telemetryd as a managed service on this machine

telemetryd generates its own service unit for the platform it is running on. Do not
hand-write a systemd unit or a launchd plist — the generated one is hardened and
matched to the binary's own paths.

```bash
telemetryd service print       # inspect the unit first
sudo telemetryd service install
```

`install` writes and enables the unit. `uninstall` stops and removes it.

## Before installing

Decide the data directory and the tokens, because changing them later means editing the
unit rather than a flag:

```bash
telemetryd validate            # shows every resolved value and its origin
```

If the service will listen on anything other than loopback, it needs
`TELEMETRYD_AUTH_INGEST_TOKEN` and `TELEMETRYD_AUTH_QUERY_TOKEN` set, or it will refuse
to start. That refusal is deliberate.

## Confirm it is running

```bash
telemetryd status              # version, uptime, listen address, per-app usage
curl -fsS http://127.0.0.1:4319/healthz
```

`/healthz` is unauthenticated by design and is the right target for a supervisor probe.
`/status` and `/metrics` require the admin token when one is configured.

## Changing configuration afterwards

`SIGHUP` reloads retention, the disk budget and the log level in place. Anything else
that changed in the file is **refused by name** rather than silently ignored, so a
reload that prints a refusal means the process is still running the old value and needs
a restart.

````
