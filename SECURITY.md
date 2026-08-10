# Security

## Reporting a vulnerability

Report privately through GitHub's **[Private Vulnerability Reporting](https://github.com/cboxdk/telemetryd/security/advisories/new)**
on this repository. Please do not open a public issue for a security problem.

We do not publish a security mailbox, a PGP key, or a response-time commitment,
because we would rather state what is true than advertise a process we do not
operate. Reports are read and acted on by the maintainers on a best-effort basis.

## What telemetryd protects

telemetryd stores telemetry, and telemetry routinely contains personal data — email
addresses, IP addresses, session identifiers, API tokens in stack traces, request
bodies. The threat model treats the stored data as sensitive by default.

**Fails closed on exposure.** telemetryd refuses to start on a non-loopback address
with no authentication token configured. This includes `0.0.0.0` and `[::]`, which are
the binds that actually expose an instance; treating "unspecified" as "probably fine"
is how data ends up public. The refusal can be overridden with `--insecure`, which
logs a warning on every start and is reported as `insecure: true` in `/status`.

**Three independent bearer tokens.** `ingest_token` guards the write endpoints,
`query_token` the read APIs, and `admin_token` `/status` and `/metrics`. Each accepts a
list so a token can be rotated without a window where either the old or the new one is
rejected.

They are not a hierarchy. An admin token does not grant reads — operating an instance
and reading the telemetry inside it are different privileges, and a dashboard scraping
`/metrics` is not a reason to hand out everyone's log lines.

**Single sign-on, without a runtime dependency on the provider.** Cbox ID access tokens
are accepted alongside the static ones. They are validated locally against the issuer's
published key set; the provider is never asked about a token, so an identity provider
that is down stops new tokens being issued without stopping you reading the telemetry
that would explain why. The signing algorithm is taken from the key the token's `kid`
selects, never from the token itself, which is what closes `alg: none` and
RS256-verified-as-HS256 confusion. `iss`, `aud` and `exp` are all required. See


Setting an issuer guards **every** surface, including any whose static token is
deliberately empty — an empty token means "unguarded" only while nothing else guards
that surface.

**Constant-time comparison.** Presented and configured tokens are SHA-256'd and
compared with `subtle::ConstantTimeEq`. Hashing first makes the comparison
length-independent, so token length does not leak through timing.

**Secrets cannot leak through logging.** `Secret` has no `Display`, its `Debug`
renders `Secret(<redacted>)`, and its `Serialize` emits `"set"`/`"unset"` rather than
a value. A stray `{:?}` or a `serde_json::to_string(&config)` therefore cannot expose
a token. This is enforced by the type, and there are tests asserting it.

**Bounded by design.** Request body size, ingest queue depth, metric cardinality, log
line length and attribute counts are all capped and configurable. Exceeding a cap is
rejected with a structured error and a labelled counter — never a silent drop, and
never unbounded memory growth driven by a caller.

**Self-metrics use matched routes, never raw paths.** A caller cannot mint unbounded
label cardinality inside telemetryd's own metrics by varying the URL.

**Single writer.** A data directory is held under an advisory lock. Two processes
against one directory is a refused startup, not a race.

## Deliberately out of scope

These are absent by decision, not by oversight. An unstated limit is
indistinguishable from a bug.

- **TLS termination.** telemetryd speaks plain HTTP. Terminate TLS at a reverse
  proxy. Shipping TLS would mean shipping certificate lifecycle management, and it
  would put a TLS stack into a binary whose static-linking story is a product
  constraint.
- **Per-app authorization.** The `app` label is a query namespace, not a security
  boundary. A holder of the ingest token can write any `app` value, and a holder of
  the query token can read every app.
- **mTLS, user accounts, audit logging of queries.** Out of frame for a single-team
  tool. OIDC was on this list until Cbox ID support was built; it is now described
  above.
- **Encryption at rest.** Use full-disk or filesystem-level encryption. telemetryd
  stores plain Parquet and its own log format, on purpose — the data directory is
  meant to be inspectable with ordinary tools.
- **Multi-tenant isolation.** telemetryd is single-tenant. Running telemetry for
  parties who should not see each other's data requires separate instances.

## Cryptography

telemetryd does not implement cryptographic primitives. Token hashing uses
[`sha2`](https://crates.io/crates/sha2) and constant-time comparison uses
[`subtle`](https://crates.io/crates/subtle), both from the RustCrypto project.

**JWT signature verification is delegated, not written here.**
[`jsonwebtoken`](https://crates.io/crates/jsonwebtoken) parses and verifies tokens, and
`ring` performs the signature checks. This is exactly the code not to hand-roll, so the
choice was a vetted dependency or no single sign-on at all. What this repository does
own is the policy around it: which key is selected, which algorithm is permitted, and
which claims are required — decisions a library cannot make correctly on your behalf.

## Supply chain

- `cargo deny check` runs in CI and gates on advisories (RustSec), licenses
  (permissive only), banned crates, and source registries.
- `cargo audit` runs in CI against the RustSec advisory database.
- A deterministic CycloneDX SBOM is committed as `sbom.json`; CI regenerates it and
  fails on drift, so the manifest cannot silently fall out of date.
- The dependency budget is a deliberate constraint. telemetryd has no C-toolchain
  dependencies at all, which is what makes the static musl builds a straightforward
  link rather than a source of surprises.

## Supported versions

telemetryd is pre-1.0. Security fixes land on `main` and in the next release; there
are no maintained release branches yet.
