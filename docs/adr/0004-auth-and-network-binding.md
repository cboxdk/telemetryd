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

## Amendment: "no TLS" was about serving, and got read as about dialling

**Date:** 2026-08-10. Amends the first bullet above.

The rule stands for inbound traffic: telemetryd still does not terminate TLS, and a
reverse proxy is still the answer. What was wrong was the reach of the phrase. The HTTP
client was declared with no TLS backend at all, commented "plain HTTP only, no TLS
stack (see ADR-004)" — accurate when its only job was reaching localhost from the CLI.

Three later features gave that same client work on the public internet: the key fetch
in [ADR-011](0011-cbox-id-integration.md), relay shipping in
[ADR-013](0013-relay-mode.md), and transfer's remote read and write in
[ADR-012](0012-import-and-export.md). None of them could work. Every https request
failed with "TLS required, but transport is unsecured", while `Config::validate`
*demanded* https for `auth.oidc.issuer` and `relay.upstream` — so the configuration
required precisely what the client could not do, and both features were unreachable in
any valid production setup.

Nothing caught it because every test and soak run points at loopback, where plain HTTP
is deliberately allowed. The suite was green against something the release never was.

**So: outbound TLS, in one place.** `telemetryd_core::http::tls` is the only
constructor, because trust configured per call site is one forgotten line away from two
requests trusting different roots. It verifies against `webpki-roots` compiled into the
binary, which behaves identically on all four targets — including static musl, where
there is no system trust store for a platform verifier to read. The
`platform-verifier` cargo feature switches to the host's store for deployments behind
an internal CA; it is not the default because that store is empty in most containers.

`tls.ca_file` came out of trying to test this. A soak case cannot use a publicly
trusted certificate, and there was no way to point telemetryd at a private authority
short of rebuilding with `platform-verifier` and installing a root into the host's
store — which is also true of the deployment [ADR-013](0013-relay-mode.md) describes,
where the upstream is internal infrastructure behind an internal CA. So the thing that
made the feature testable was the thing that made it deployable. It replaces the
built-in roots rather than adding to them, because an instance pointed at a private
authority is usually talking only to internal hosts, and trusting that authority *and*
every public CA is the surprising option.

`telemetryd status` also stopped refusing https URLs. That refusal cited this ADR, and
misread it the same way: the server does not terminate TLS, but this ADR *recommends* a
proxy that does, and declining to talk to one made the recommended deployment
unqueryable from our own CLI.

The lesson is the one the section above already states, turned on itself: an unstated
limit is indistinguishable from a bug, and this limit was stated in a place — a
dependency comment — where no operator would ever read it.

The second lesson is about the tests. Every OIDC and relay case in the soak pointed at
plain-HTTP loopback, which is allowed deliberately and is why none of them noticed. The
suite now runs both across a genuine handshake against a private CA, and the OIDC case
asserts that removing the CA bundle *breaks* it — without that, a passing test would be
equally consistent with trust never being applied.
