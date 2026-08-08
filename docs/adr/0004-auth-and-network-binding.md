---
title: "ADR-004: Auth and network binding"
weight: 4
description: "Failing closed on an exposed bind, two independent tokens, and what is deliberately absent."
---

# ADR-004: Auth and network binding

- **Status:** Accepted
- **Date:** 2026-08-07
- **Milestone:** M0

## Context

The brief: optional static bearer tokens, one for ingest and one for query; off by
default on localhost; refuse to bind non-localhost without a token unless `--insecure`.

The failure mode we are designing against is specific and common: someone runs
telemetryd on a VPS with `--listen 0.0.0.0:4319` to "just try it", and their
application logs — which contain user emails, tokens and stack traces — become a
public HTTP endpoint. The product promise is "your telemetry never leaves your infra",
so this must fail closed.

## Decision

**Bind classification.** At startup the listen address is classified:

- *loopback* — `127.0.0.0/8`, `::1`
- *non-loopback* — everything else, **including `0.0.0.0` and `[::]`**

Wildcard binds count as non-loopback. They are the exact case that gets someone
exposed, and treating "unspecified" as "probably fine" is how that happens.

**The rule.** Binding non-loopback with no `auth.ingest_token` and no
`auth.query_token` is a startup **error**, not a warning. It exits non-zero with a
message that states the problem, the three ways to fix it (set a token, bind loopback
and use a reverse proxy, or pass `--insecure`), and generates a ready-to-paste random
token in the error text. `--insecure` / `TELEMETRYD_SERVER_INSECURE=true` overrides,
logs a `WARN` on every startup, and surfaces as `insecure: true` in `/status`.

**Three independent roles.** `ingest_token` (write) guards `/v1/*` and
`/api/v1/write`; `query_token` (read) guards the Prometheus, Loki and Tempo read APIs;
`admin_token` guards `/status` and `/metrics`. Any may be set without the others; an
unset token means that surface is unauthenticated. The split exists because the trust
boundaries genuinely differ — app servers push, humans and the UI read, and those are
different credentials with different rotation cadences.

**Why admin is separate from read.** The two answer different questions. Reading
telemetry tells you about your applications. `/status` and `/metrics` tell you about
the *deployment*: every app name, its series count, its share of the disk, whether the
instance is running unauthenticated, how close the cardinality caps are. Handing that
to everyone who may read logs is more than is usually meant, and it is the surface an
attacker would enumerate first.

The roles are not a hierarchy. Admin does not imply read, because the reason to hold an
admin token — running a dashboard, wiring an alert — is not a reason to read anyone's
log lines.

**`admin_token` unset means `query_token` guards those routes**, which is exactly what
guarded them before the role existed. A deployment that never hears about it keeps
working; setting it is opting into the tighter split.

**Multiple tokens per role.** Both fields accept a list, so a token can be rotated
without a window where either the old or the new one is rejected. A single string is
accepted and treated as a one-element list.

**Comparison is constant-time** (`subtle::ConstantTimeEq`) over the SHA-256 of the
presented and configured tokens. Hashing first makes the comparison length-independent,
so token length does not leak.

**Tokens are never logged, never in `/status`, never in `telemetryd validate` output.**
`Token` is a newtype whose `Debug`/`Display` render `Token(<redacted>)`, so it cannot
leak through a stray `{:?}` in an error path. Config output shows `set`/`unset`.

**Indirection for secrets.** `ingest_token = "file:/run/secrets/ingest"` and
`"env:MY_VAR"` are supported so the token need not sit in a world-readable TOML. If a
`file:` target is group- or world-readable, we log a `WARN` naming the mode.

**Always unauthenticated:** `/healthz`. It carries no telemetry and load balancers
need it. `/status` and `/metrics` are guarded by `query_token` when set — both disclose
app names, series cardinality and volumes, which is more than we should hand out.

## Explicitly out of scope for v1

- **TLS termination.** Use a reverse proxy. Shipping TLS means shipping certificate
  lifecycle management, and the deployment story ("one VPS, probably already behind
  Caddy/nginx/Traefik") does not need it. This is documented, not silently absent.
- **Per-app tokens / multi-tenancy.** The `app` label is a namespace, not a security
  boundary — a holder of the ingest token can write any `app` value. Anything stronger
  is a different product.
- **mTLS, OIDC, user accounts.** Out of frame for a single-team tool.

These limits are stated in the README's security section, because an unstated limit is
indistinguishable from a bug.
