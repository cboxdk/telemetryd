---
title: "Installation"
weight: 11
description: "Install script, Homebrew, .deb, or from source — and how to verify the result."
---

# Installation

## Install script

The primary channel. Works on macOS and Linux, including servers where Homebrew does
not belong.

```bash
curl -fsSL https://raw.githubusercontent.com/cboxdk/telemetryd/main/install.sh | sh
```

It detects your platform, downloads the matching release, **verifies the SHA-256
checksum and refuses to install on a mismatch**, and places the binary in the first
writable directory on your `PATH`.

It never invokes `sudo`. If the target directory is not writable it prints the exact
command to run instead — a tool that silently escalates is one you cannot reason about.

Overrides:

```bash
TELEMETRYD_VERSION=0.20.9 sh install.sh
TELEMETRYD_INSTALL_DIR="$HOME/.local/bin" sh install.sh
```

## Homebrew

```bash
brew install cboxdk/tap/telemetryd
```

The formula is generated from a published release by
`scripts/publish-formula.py`, so it can only ever describe a build that exists. It
carries the release's own checksums and Homebrew verifies them on install.

## Debian and Ubuntu

Download the `.deb` for your architecture from the
[releases page](https://github.com/cboxdk/telemetryd/releases) and install it:

```bash
sudo dpkg -i telemetryd_0.20.9_amd64.deb
sudo systemctl enable --now telemetryd
```

The package creates an unprivileged `telemetryd` user, a state directory at
`/var/lib/telemetryd`, and a hardened systemd unit.

**There is no hosted apt repository**, deliberately. Running one means running signing
infrastructure and keeping it available; for a project at this scale a signed release
asset carries the same guarantee with far less that can quietly break. Stated here
rather than left as a gap you discover.

## Verifying a release

Every release publishes `SHA256SUMS` and `SHA256SUMS.cosign.bundle`, a keyless
[Sigstore](https://www.sigstore.dev/) signature over it, in the standardised bundle format
that cosign 2.4+, `sigstore-python` and `sigstore-go` all read. There is no public key to
fetch or trust on first use: the signing identity is the release workflow itself,
attested by GitHub's OIDC provider and recorded in the public transparency log.

### Getting a verifier

`install.sh` verifies automatically when `cosign` is on your `PATH`, and says so when it
is not — which on a server is most of the time. **Ubuntu and Debian do not package
cosign**; `apt-get install cosign` fails, which is where most people stop.

Two ways to get one. Sigstore's own `.deb`, checked before it is installed:

```bash
cd /tmp
curl -fsSLO https://github.com/sigstore/cosign/releases/download/v3.1.3/cosign_3.1.3_arm64.deb
curl -fsSL https://github.com/sigstore/cosign/releases/download/v3.1.3/cosign_checksums.txt \
  | grep 'cosign_3.1.3_arm64.deb$' | sha256sum -c -
sudo apt-get install -y ./cosign_3.1.3_arm64.deb
```

`sha256sum -c` fails and stops the chain if the download does not match, so `apt-get`
only ever sees a file that checked out. Swap `arm64` for `amd64` on an Intel host.

Or, without leaving Python — this verifies the same bundle, and needs no binary from
another project's releases:

```bash
pipx install sigstore     # or: pip install sigstore
python -m sigstore verify identity \
  --bundle SHA256SUMS.cosign.bundle \
  --cert-identity "https://github.com/cboxdk/telemetryd/.github/workflows/release.yml@refs/tags/v0.41.1" \
  --cert-oidc-issuer "https://token.actions.githubusercontent.com" \
  SHA256SUMS
```

**This works from v0.41.1 onward.** Earlier releases were signed into cosign's own older
envelope, which only cosign reads — `sigstore-python` rejects it as malformed, and that
is a statement about the format rather than about the file. cosign verifies every release,
old and new.

**Whichever you install, you cannot verify the verifier.** The checksum above protects
against a corrupted or swapped download, not against the upstream release itself being
bad. That is the bootstrap problem and it has no clean answer; it is worth knowing rather
than assuming the chain closes all the way down.

### By hand

To check with cosign:

```bash
cosign verify-blob \
  --bundle SHA256SUMS.cosign.bundle \
  --certificate-identity "https://github.com/cboxdk/telemetryd/.github/workflows/release.yml@refs/tags/v0.20.9" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  SHA256SUMS
```

Then check the archive against the verified checksums:

```bash
sha256sum --check --ignore-missing SHA256SUMS
```

**Pin `--certificate-identity`.** Without it, cosign accepts any valid Sigstore
signature — including one made by someone else entirely. The identity is what ties
the signature to this repository's release workflow at that tag.

The checksum file alone proves your download was not corrupted in transit. It comes
from the same server as the archive, so on its own it says nothing about who produced
it; the signature is what does that.

## From source

```bash
git clone https://github.com/cboxdk/telemetryd
cd telemetryd
cargo build --release
```

Needs Rust 1.89+ and nothing else — there are no C dependencies in the tree.

## Verifying what you got

```bash
telemetryd version
```

Prints the version, the storage format version, and **the build target**
— which is the first question in any "it works on my machine" report about a static
binary.

Every release also publishes `SHA256SUMS` and `sbom.json`, a CycloneDX 1.5 manifest of
everything linked into the binary.

## Signing

Not yet in place. Releases carry checksums, which protect against a corrupted download
but not against a compromised release. Named here as a known gap rather than implied to
be covered by an unsigned checksum file.
\n
## Is there a newer one?

```bash
telemetryd version --check
```

Prints what you are running, then asks GitHub for the newest release and says which of
the two is ahead. It is the only code path in telemetryd that contacts anything outside
your infrastructure, and it runs **only** when you ask — no background timer, no line in
the server log, nothing on start-up. A binary whose pitch is that your telemetry stays
put should not be phoning anywhere on its own.

Being offline is not an error: the check says it could not reach the feed and the
command still exits `0`, so putting it in a script does not make the script fail on a
box without egress.

## Hand this to an agent

A self-contained brief. It names only commands and endpoints that exist, so an agent can
execute it without reading the rest of this page — and without inventing the parts of the
Loki and Prometheus APIs telemetryd deliberately does not implement.

````markdown
# Task: install telemetryd on this machine and prove it works

telemetryd is a single statically linked binary. No runtime, no libc to match, no
sidecar, no collector. Do not install a package manager, a container runtime, or an
OpenTelemetry Collector — none is needed.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/cboxdk/telemetryd/main/install.sh | sh
```

The installer verifies the release checksum and refuses to install on a mismatch. On
macOS, `brew install cboxdk/tap/telemetryd` is equivalent.

## Run

```bash
telemetryd serve
```

That is the whole setup: no configuration file and no flags. It listens on
`127.0.0.1:4319`, stores data in `./telemetryd-data` or the platform data directory,
keeps 7 days of logs and traces and 30 days of metrics, and stays under a 10 GiB budget.

**It will refuse to start if you bind a non-loopback address with no token configured.**
That is deliberate, not a bug. Either keep it on loopback, or set
`TELEMETRYD_AUTH_INGEST_TOKEN` and `TELEMETRYD_AUTH_QUERY_TOKEN`.

## Prove it works — do all three

1. `curl -fsS http://127.0.0.1:4319/healthz` returns `ok`.
2. Send one record and read it back:

```bash
curl -X POST http://127.0.0.1:4319/v1/logs -H 'Content-Type: application/json' -d '{
  "resourceLogs": [{"resource": {"attributes": [
    {"key": "service.name", "value": {"stringValue": "smoke"}}]},
    "scopeLogs": [{"logRecords": [{
      "timeUnixNano": "'"$(date +%s)000000000"'",
      "severityNumber": 17, "severityText": "ERROR",
      "body": {"stringValue": "hello from the install check"}}]}]}]}'

curl -G http://127.0.0.1:4319/loki/api/v1/query_range \
  --data-urlencode 'query={app="smoke"} |= "hello"'
```

The query must return the line the send step created. If it returns an empty result,
the write failed — check the response body of the POST rather than retrying.

3. `telemetryd validate` prints every resolved setting and where it came from.

## Do not

- Do not add a reverse proxy or TLS unless the port must be reachable from another
  machine. On loopback it buys nothing.
- Do not write a configuration file to change defaults you have not measured a need
  for. Every setting is also an environment variable, `TELEMETRYD_<SECTION>_<KEY>`.

````
