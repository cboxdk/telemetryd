---
title: "Run it as a service"
weight: 31
description: "systemd and launchd, with a hardened unit telemetryd generates for you."
---

# Run it as a service

## Let telemetryd do it

```bash
sudo telemetryd init
sudo telemetryd service install
```

`init` writes `/etc/telemetryd/telemetryd.toml` with three generated tokens — one per
surface — and prints them once. `install` writes the unit for your platform, reloads the
service manager, starts it, and then **checks that it actually started** rather than
reporting success and leaving you to find out later.

By default the service runs as a dedicated `telemetryd` system user, created for you if
it does not exist. To run as an account that already exists — `forge`, `deploy`, your own
login:

```bash
sudo telemetryd service install --user forge
```

A user you name yourself is never created. An unknown name is a typo, and inventing an
account from a misspelling is worse than refusing.

`install` also hands the token files `init` wrote to that account. They are `0600` and
owned by whoever ran `init`, and the service is somebody else — that mismatch used to
produce a unit that started, could not read its own tokens, and exited.

Remove it again with `telemetryd service uninstall`, which stops the service, removes
the unit, and **leaves the data directory alone** — removing a service should not delete
the telemetry it collected. It prints the path and the size of what it left behind, so
"where is my data" is answered at the moment you are most likely to ask.

telemetryd never invokes `sudo` on your behalf. If the unit path needs root it prints
the exact command to run instead.

## Do it by hand

```bash
telemetryd service print | sudo tee /etc/systemd/system/telemetryd.service
sudo useradd --system --no-create-home telemetryd
sudo systemctl daemon-reload
sudo systemctl enable --now telemetryd
```

The `useradd` is what `service install` does for you. Skipping it is the most common way
to get a unit that fails with `status=217/USER`, which names the exit code and not the
cause.

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
sudo telemetryd init           # config + three tokens, printed once
telemetryd service print       # inspect the unit first
sudo telemetryd service install
```

`init` generates the configuration and the tokens. Do not hand-write either; the tokens
it writes are `0600` files referenced by path, which keeps them out of the file people
paste into issues. Re-running `init` is refused rather than silently rotating every
token — pass `--force` only if locking out every existing writer is the intent.

`install` writes and enables the unit, then verifies it started. `uninstall` stops and
removes it, and leaves the data directory in place.

To run as an account that already exists rather than a dedicated `telemetryd` user:

```bash
sudo telemetryd service install --user forge
```

## Before installing

Decide the data directory, because changing it later means editing the unit rather than
a flag:

```bash
telemetryd validate            # shows every resolved value and its origin
```

If the service will listen on anything other than loopback, it needs tokens configured
or it will refuse to start. `init` satisfies that. That refusal is deliberate.

## Confirm it is running

```bash
telemetryd status              # version, uptime, listen address, per-app usage
curl -fsS http://127.0.0.1:4319/healthz
curl -fsS http://127.0.0.1:4319/           # what this server is, and its routes
```

`telemetryd status` needs no token against a loopback URL: it reads the admin token from
this machine's own configuration. Against a remote URL it will ask, because sending a
local credential to another host is not something a convenience should do quietly.

`/healthz` is unauthenticated by design and is the right target for a supervisor probe.
`/` is open too and names the product, its version and its routes — no token, and
nothing about what this instance holds. `/status` and `/metrics` require the admin
token, falling back to the query token when no admin token is set.

## Changing configuration afterwards

`SIGHUP` reloads retention, the disk budget and the log level in place. Anything else
that changed in the file is **refused by name** rather than silently ignored, so a
reload that prints a refusal means the process is still running the old value and needs
a restart.

````
