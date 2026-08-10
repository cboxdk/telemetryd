# Build status

The living record of what is built, what is not, and what is deliberately absent. Kept
current so scope and gaps are always auditable without reading the source.

## What works

**Ingest.** OTLP/HTTP JSON for logs, traces and metrics; Prometheus `remote_write`
(snappy + protobuf, decoded by hand — no `protoc` at build time). Per-record rejection
with OTLP `partialSuccess`. Timestamps in the wrong unit are detected and corrected,
and counted so the producer bug stays visible.

**Compressed bodies.** `Content-Encoding: gzip`, `deflate` and `zstd` are undone before
decoding, on every ingest route; an unknown coding is a `400` naming it. gzip is part of
the OTLP/HTTP spec and every SDK enables it above some batch size, so this was not an
optimisation we were missing — it was a server that worked for empty batches and failed
for real ones. Reported from the field exactly that way: a health check sending an empty
batch got `200` and said "all checks passed" while the 8.6 KB batch behind it got `400`
and vanished.

Every decoder is bounded by `server.max_body_bytes` through a `Take`, so a compression
bomb is refused with the same `413` an oversized raw body gets rather than inflated into
memory and measured afterwards, and zstd's declared window is capped at the same number
so the frame header cannot buy an allocation either. Fuzzed as
`fuzz/fuzz_targets/body_decompression.rs`, which asserts the cap holds rather than only
that nothing panicked.

**Cardinality is capped.** `limits.max_series` and `limits.max_series_per_app` refuse
records that would create a *new* series past the limit, while series already being
stored keep ingesting — a labelling mistake should not take out the telemetry that
still works. Refusals reach the producer in `partialSuccess`, a log line naming the
limit, and `telemetryd_series_rejected_total`. The count is rebuilt after each
retention pass, so expired data gives its budget back.

**Storage.** Write-ahead log with crash recovery, immutable Parquet segments, per-segment
stream dictionary, Bloom filters for exact-key lookup, trigram indexes for substring
search, retention by age and by a global
disk budget. One engine serves all three signals.

**Damage is survivable.** A truncated, zero-length, garbled or deleted segment file
costs that segment and nothing else: the query answers from what is left, the segment
is marked so later queries skip it cheaply, and the loss is reported in `/status` and
as `telemetryd_segments_unreadable_total` rather than being inferred. Verified by
damaging a real data directory five different ways.

**Query.** LogQL, TraceQL and PromQL subsets, each parsed in full and lowered so an
unsupported construct reports itself by name. Live tail over WebSocket. Label and series
queries answered from metadata with no file I/O.

**Authentication.** Three surfaces — ingest, query, admin — each guarded by its own
static bearer token, with rotation via a list and indirection through a file or
environment variable. OIDC access tokens are accepted alongside them, validated
locally against the issuer's published key set so that an identity provider being down
never stops you reading your telemetry. The algorithm comes from the key the
`kid` selects, never from the token. Scopes are not a hierarchy: admin does not imply
read.

Cbox ID is what this was built for, but nothing in it is specific to Cbox ID. Two
things are not universal across providers, so they are settings: `jwks_url`, because
`{issuer}/.well-known/jwks.json` is a convention rather than a rule, and `scope_claim`,
because some providers use `scp` and some send an array where OAuth specifies a
space-separated string. Verified against a real third-party key set, not only against
the test issuer. What that does *not* buy is a provider with no scope claim at all —
telemetryd can then verify who the caller is and still have nothing to authorise them
with; the cookbook says which providers that rules out.

The roles are also not a hierarchy of *convenience*: setting an OIDC issuer guards
every surface, including any whose static token is deliberately empty. That is the safe
direction, and it is documented in the configuration reference because it is a
behaviour change, not a default.

**Relay mode.** telemetryd can stand in front of a central instance and decide who each
client is from its credential rather than from its payload — the point of it for mobile
fleets, where the credential lives in a binary anyone can open. Sealed
segments are forwarded in arrival order behind a durable cursor that advances only after
upstream confirms; retention will not delete what has not been forwarded, and
`relay.when_full` decides between losing the oldest unsent data and refusing new writes.

**Export and import.** `telemetryd export` writes a time range as OTLP NDJSON;
`telemetryd import` reads one back, from a file or straight from another instance's
Loki API. Neither reads the other side's storage, so the same pair covers migrating in,
migrating out, and pulling an incident window onto a laptop. All three signals round trip, asserted against a
running pair rather than in prose. Against telemetryd, export reads records directly
through `/api/v1/export`, so nothing passes through a query language; against a foreign backend it
uses the read APIs, which cover logs and traces — metrics cannot be pulled that way,
because a range query returns resampled points rather than stored samples.

**Operations.** `telemetryd query` runs a LogQL query from the shell — for debugging
over SSH when the UI is the thing you cannot reach, and for getting data out as JSON
lines. `telemetryd bench` measures what the machine can ingest and query, on a
throwaway directory, so sizing is a measurement rather than a guess.
`/healthz`, `/status`, `/metrics`. `telemetryd validate` prints every
resolved value with its origin. `SIGHUP` reloads retention, the disk budget and the log
level; anything else that changed in the file is refused by name rather than ignored. `telemetryd service install` writes a hardened unit.

**Packaging.** Four cross-compiled targets, install script with checksum verification
(and signature verification, once a signed release exists), Homebrew formula, `.deb`, deterministic CycloneDX SBOM with a
CI drift check.

**A container that starts with nothing set.** `ghcr.io/cboxdk/telemetryd`, multi-arch,
with `cbox-init` as PID 1 so SIGTERM reaches telemetryd and the WAL is flushed rather
than the container being killed mid-write. The image copies the *same* musl artifact the
release workflow soak-tests and signs, after verifying it against the published
`SHA256SUMS` — building a second binary for the image would ship one nothing had checked.

The interesting part is authentication. A container binds `0.0.0.0`, which telemetryd
refuses to do unauthenticated, so a zero-config image has to either fail to start or
default to `insecure` — both wrong. Instead it generates tokens on first start, persists
them beside the data, and prints them once; supplying your own skips it entirely.
Verified by running the built image: healthy in 2s, the generated token accepted, an
unauthenticated write refused with `401`, a clean exit 0 through `cbox-init`, and data
plus tokens intact across a restart.

**Release signing — exercised.** `SHA256SUMS` is signed by the release workflow's own
OIDC identity, keyless, with no private key to store or rotate. The release job
verifies its own signature before publishing, and `install.sh` verifies it when
`cosign` is present, pinning `--certificate-identity` — without which any valid
Sigstore signature by anyone would pass.

This entry used to say the path was written but unproven. It is proven now: v0.13.0's
bundle was downloaded from the published release and verified against
`refs/tags/v0.13.0` with the same command the documentation gives users, on a machine
that had not built it.

## Gates

**The toolchain is pinned** in `rust-toolchain.toml` (currently 1.97.1), so the gate
gives the same answer on a laptop as in CI. This was a real defect, not a precaution:
development ran 1.95 while CI resolved `stable` to 1.97.1, and a lint added in between
failed CI on code the local gate had called clean. Bumping the pin is a deliberate
commit that also fixes whatever new lints surface.

Every one of these is green, and CI enforces all of them:

```
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps
cargo deny check                      # advisories, licenses, bans, sources
python3 scripts/generate-sbom.py --check
python3 scripts/check-docs.py
python3 scripts/soak.py target/debug/telemetryd   # the binary, over HTTP
```

The soak is the one that runs the product rather than its parts: it starts the real
binary, pushes all three signals at it, queries them back through the Loki, Tempo and
Prometheus APIs, restarts the process and checks the data survived. It exists because
two defects reached a tagged release with the whole unit suite green — a `.deb` whose
systemd unit pointed at the build runner's filesystem, and a TraceQL endpoint that
rejected its own documented `.attribute` syntax. It also runs in the release workflow,
against the exact artifact being published, before anything is uploaded.

Plus: all four release targets cross-compile, and the musl builds are asserted to be
statically linked.

**The corpus survives the night.** Each run used to begin from a handful of committed
seeds and re-explore from zero, which throws away the one thing fuzzing accumulates. It
is cached per target now, and minimised before caching so it does not fill with inputs
that reach no new code. Five minutes on top of ten thousand known-interesting inputs is
not the same search as five minutes from nothing.

**Fuzzing runs nightly, not on the merge gate.** It found a defect the first time it
ever ran: eight bytes of snappy declaring 2.8 GB of output, which `/api/v1/write`
allocated before decompressing anything. Three separate faults in the workflow had kept
it from executing at all — a stable toolchain where it needed nightly, a corpus
directory that only exists after a run, and a target triple ASAN cannot use — so
"fuzzing runs nightly" was true of the schedule and false of the outcome. Worth stating
plainly: the value arrived the moment it actually ran, not when it was written.

Nine targets cover every parser that reads bytes
off a wire: the three query languages, **all three** OTLP JSON decoders, both hand-rolled
protobuf paths — `remote_write` inbound and remote read outbound — and body
decompression.

The count used to say six and the sentence said "OTLP JSON" as though it were one
decoder. There are three, and only logs was covered — so the claim was nearly true,
which in a document like this is the same as false. The remote-read decoder was added
later still: it was written into the CLI, where no fuzzer can reach it, and moving it
into a library crate was most of the reason for moving it at all. A clean sixty seconds proves very little, so making a merge
wait on one would trade real signal for the appearance of it.

## Load, measured on the target platform

All numbers are the musl release build — the one that ships — driven over HTTP.

| | |
|---|---|
| Ingest, no queries running | 389,000 records/sec |
| Ingest, one concurrent reader | 357,000 records/sec (8% lower) |
| Query p50 / p95 under that load | 8 ms / 31 ms |
| `kill -9` durability | 5,000 of 5,000 acknowledged records recovered |
| Resident memory | ~`80 MB + 1.3 × storage.max_segment_bytes`, stable across seals |

On a four-million-record store across 500 segments:

| | |
|---|---|
| Cold start, empty | 59 ms |
| **Restart, full store** | **55 ms** |
| Newest 100 for one app | 8 ms |
| Newest 100 with a line filter | 33 ms |
| Label names, series list | under 1 ms (metadata only, no file I/O) |

Restart time is an availability number — it is how long you are blind after a deploy —
and it does not grow with the store, because sealed segments are read from their
manifests rather than replayed.

**Relay mode's per-request work is free, and that was checked rather than assumed.**
Identity stamping and the per-client queue share both add work to the ingest path, and
the share takes a mutex on every request — which is exactly the shape of the defect that
once cost 45% of throughput when a query held the writer lock. Measured on an
aarch64-apple-darwin laptop rather than the shipping musl build, so the absolute figure
does not belong in the table above: 663–679k records/sec with relay on against 621–672k
with it off, in both orders. The difference is noise, in the wrong direction as often as
the right one. The mutex is held for a `HashMap` increment against the cost of parsing a
500-record batch.

**Sealing costs the request that triggers it, and only that one.** A seal writes a
Parquet file, which adds roughly 120 ms to one ingest request; with six concurrent
writers no request exceeded three times the median, because the Parquet write happens
outside the buffer lock. Smaller `max_segment_bytes` trades throughput for smoother
latency: at 8 MiB every request seals.

Ingest gave up 13% when the trigram index landed: it is paid on every write
whether or not anyone searches, and it buys a substring query going from 98 seconds to
0.2 over a full store.

Three things had to be true for the throughput numbers, and none of them were a month
ago: the allocator, the buffer no longer being scanned under the append lock,
and `max_segment_bytes` meaning what it says. Each is guarded by a test that was checked
against the broken version rather than assumed to work.

## Known gaps

Named rather than left to be discovered.

**Outbound TLS trusts one set of roots at a time.** `tls.ca_file` replaces the
built-in roots rather than adding to them, so an instance that must reach both a public
issuer and an internal upstream behind a private CA needs the public roots it wants
concatenated into the same bundle. Deliberate — see the configuration reference — but
it is a sharp edge worth knowing before you meet it.

**`[[relay.client]]` cannot be set from the environment.** Every other configuration
key can, which is what lets a container run with no file at all. That one is a list of
tables with no flat spelling that is not worse than a mounted file — and its values are
credentials, which is where they belong anyway. Named because "every key except one" is
the kind of thing that should not be discovered.

**No hosted apt repository.** Deliberate — running one means running signing
infrastructure and keeping it available. `dpkg -i` from a release asset is documented
instead.

**Attribute maps are still per-row JSON.** They are genuinely high cardinality, so
interning does not obviously apply. Only paid for rows that survive filtering, so it no
longer dominates a query.

**Segment count does not need compacting.** Listed as a gap from reasoning — segments
accumulate and are never merged — and then measured, which said otherwise. Over a
sixteenfold increase in segment count:

| segments | newest 100 | label names | line filter, common trigrams |
|---|---|---|---|
| 252 | 2.5 ms | 0.4 ms | 31 ms |
| 4,002 | **2.9 ms** | **0.5 ms** | 474 ms |

Limited queries and metadata queries are flat: per-stream bounds let the cutoff skip
segments on their manifests, so the count barely matters. The line filter grows
linearly — but the *data* grew sixteenfold too, so that is cost per byte scanned, not
per segment, and merging segments would not reduce a byte of it. Compaction would be
work that buys nothing measurable.

**The operational surface is pinned.** `/status`'s key set and every exported metric
name are asserted as sets, not field by field. The query APIs were always frozen in
COMPATIBILITY.md and contract-tested; the surface dashboards and alerts read was not, so
removing a field no test happened to mention passed silently — which is exactly how
`milestone` left in 0.25.0 without a single failure. Both assertions are
mutation-verified: removing a field or a metric fails them, restoring it passes.

**The text filter is sized from the trigrams a segment actually contains.** It used to
be sized from row count times four, which the comment above it argued against in the
same breath. Measured on real text, that was thirty times too large on repetitive logs
and ten times too *small* on high-entropy ones — base64 payloads, hex-laden stack
traces — where the filter saturated, answered "maybe" to everything, and left every
query reading every segment while the sidecar still cost disk. Proven against a running
instance: a term guaranteed absent pruned 44 of 44 segments on repetitive text and 0 of
2 on random text. Sizing from the exact count took one corpus from 30,008 bytes of
filter per segment to 1,156, and the whole store from 6.7 MB to 5.5 MB, with pruning
unchanged. A segment too varied to index now gets no filter at all, because a filter
that cannot prune is worse than none: the caller scans either way and now pays nothing
to be told so.

**Substring search prunes, but not equally well for every pattern.** A per-segment
trigram index skips segments that cannot contain a `|=` filter's text. A term
present nowhere went from 3.9 s to 6 ms over 405 MB — 98 s to 0.2 s extrapolated to a
full 10 GiB store. A pattern built entirely from *common* trigrams still prunes poorly:
searching one specific order number costs about 3 s over the same store, because its
digit trigrams appear in most segments. Bounding the time range remains the answer
there.

**Queries are single-threaded by default**. Parallel scanning is
opt-in via `storage.query_parallelism` and worth about 7% on an unbounded scan; it is
not worth three cores by default on a box that is also ingesting. Limited queries never
use it at all — their speed comes from a cutoff that tightens as it goes, which is a
sequential dependency, and splitting it measured 60% slower.

## Deliberately absent

Not gaps. These are decisions:

- **Inbound TLS** — terminate at a reverse proxy. *Outbound* TLS is built: the key fetch, relay shipping and transfer all verify against roots compiled into the binary, or the host's store with the `platform-verifier` feature. That distinction was a defect before it was a decision.
- **Multi-tenancy** — `app` is a query namespace, not a security boundary
- **Clustering, replication, object-store tiering** — single node is the design
- **Plugins, relabelling rules, write-path transformations** — shape data in the
  instrumentation
- **OTLP/gRPC** — JSON is first-class because that is what the client emits
- **A separate "events" signal** — OpenTelemetry has no such signal. An event is a log
  record carrying `event.name`, so events already arrive on `/v1/logs` and are queryable
  through the Loki API. telemetryd used to carry a fourth `Signal::Events` that nothing
  could write to, along with a `retention.events` setting that governed nothing
- **Scraping Prometheus targets** — telemetryd receives (`remote_write`, OTLP), it
  does not go and fetch. A `[[scrape]]` config section existed, complete with
  validation and documentation, and was read by nothing; it has been removed rather
  than left to look like a feature
- **A bespoke metric chunk store** — superseded by reusing the record store, with the cost stated

## Where the reasoning lives

`docs/adr/` holds eight decision records. Two are worth reading before changing
anything:

- **The compatibility subset** is derived from the client's source, not the
  upstream API references. Four requirements the published specs do not imply; two were
  already costing correctness.
- **Query performance**, with before-and-after measurements and an honest
  account of where a general analytical engine stays ahead.
