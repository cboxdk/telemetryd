---
title: "ADR-003: Configuration model"
weight: 3
description: "Defaults, then file, then environment, then flags — and why the empty configuration is valid."
---

# ADR-003: Configuration model

- **Status:** Accepted
- **Date:** 2026-08-07
- **Milestone:** M0

## Context

Two constraints pull against each other: *zero config to start* (`telemetryd serve`
with no flags must work) and *every option settable via env var* (for containers and
systemd drop-ins). A single TOML file is the third channel.

## Decision

**Precedence, lowest to highest:** built-in defaults → `telemetryd.toml` → environment
→ CLI flags.

Every layer is partial. Defaults are complete, so any subset of the others suffices —
this is what makes zero-config work: the empty configuration is a valid configuration.

**Layering is implemented with `figment`.** Serde-native, supports partial merge across
providers, and reports the *provenance* of a bad value ("in telemetryd.toml, key
`storage.disk_budget`") which matters for `telemetryd validate`.

**Config file discovery** when `--config` is not passed, first hit wins:

1. `./telemetryd.toml`
2. `$XDG_CONFIG_HOME/telemetryd/telemetryd.toml` (`~/Library/Application Support/telemetryd/` on macOS)
3. `/etc/telemetryd/telemetryd.toml`

Absence of a config file is not an error. An unreadable or malformed file *is* — we do
not silently fall back to defaults when the operator clearly intended a file.

**Env var naming:** `TELEMETRYD_<SECTION>_<KEY>`, uppercased, `.` → `_`. So
`storage.data_dir` is `TELEMETRYD_STORAGE_DATA_DIR`. Env is for scalars, which is
where the container use case actually is; if a nested array is ever needed it will be
file-only.

**Unknown keys are rejected** (`#[serde(deny_unknown_fields)]`). A typo'd
`retetnion` that silently does nothing is a worse experience than a startup error
naming the key.

**Human units everywhere.** Durations are `humantime` strings (`7d`, `100ms`, `1h`),
sizes are `bytesize` strings (`10GiB`, `256MiB`). Bare integers in a duration field are
a validation error rather than an ambiguous unit guess.

## Data directory default

1. `--data-dir` / `TELEMETRYD_STORAGE_DATA_DIR` if set
2. `./telemetryd-data` **if it already exists** — so a developer who ran once in a
   project directory keeps hitting the same data
3. otherwise the XDG data dir: `$XDG_DATA_HOME/telemetryd` /
   `~/Library/Application Support/telemetryd`

This is a deliberate deviation from a flat "always `./telemetryd-data`". Always using
CWD means running the binary from a different directory silently gives you an empty
store, and a service unit's working directory is rarely what the operator expects.
Preferring an existing `./telemetryd-data` preserves the good dev-laptop behaviour
without that trap. `telemetryd status` and startup logs always print the resolved path.

## Validation

`telemetryd validate` type-checks, applies the same cross-field rules as `serve`, and
prints the fully resolved configuration with each value's origin. Cross-field rules
enforced at load time (not at first use):

- non-loopback `server.listen` requires a token or `--insecure` (see ADR-004)
- `retention.*` must be ≥ `storage.segment_duration`
- `storage.disk_budget` must exceed a floor derived from segment sizing, so the reaper
  cannot be configured into deleting data as fast as it arrives

## Reloading

**Superseded — `SIGHUP` reloads a deliberately narrow set.** This section originally
declined reload, arguing that a fast-starting process with a durable log makes
restarting cheap. That holds for most settings and fails for the ones people need to
change in a hurry: a disk filling at three in the morning is fixed by shortening
retention, and restarting the observability backend during an incident throws away the
live tail of the incident you were watching.

Reloadable:

- `retention.*` — the window per signal
- `storage.disk_budget` — the ceiling the reaper enforces
- `log.level` — turning up logging should not require restarting the process whose
  behaviour you are trying to observe

**Everything else is refused by name, in a log line, naming both values.** That is the
part that makes this safe. The original objection — that every field would have to be
classified as reload-safe or not — was correct, and it is answered by classifying them
explicitly rather than by declining the feature: an unlisted field is refused, so
adding one forces a decision instead of defaulting to silence.

A reload that quietly ignored half the file would be worse than no reload, because the
operator would believe a setting had taken effect and go looking for the problem
somewhere else.

A malformed or invalid file leaves the running configuration untouched and logs the
error. The worst outcome of a `SIGHUP` is a log line saying nothing changed.

`SIGHUP` is Unix only. Windows has no equivalent and inventing one — a control socket,
a file watcher — is a larger surface than the feature is worth for a Linux service.
