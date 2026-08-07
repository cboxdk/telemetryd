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
