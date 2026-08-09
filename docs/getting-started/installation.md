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
TELEMETRYD_VERSION=0.16.1 sh install.sh
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
sudo dpkg -i telemetryd_0.16.1_amd64.deb
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
[Sigstore](https://www.sigstore.dev/) signature over it. There is no public key to
fetch or trust on first use: the signing identity is the release workflow itself,
attested by GitHub's OIDC provider and recorded in the public transparency log.

`install.sh` does this automatically when `cosign` is on your `PATH`, and tells you
when it is not. To check by hand:

```bash
cosign verify-blob \
  --bundle SHA256SUMS.cosign.bundle \
  --certificate-identity "https://github.com/cboxdk/telemetryd/.github/workflows/release.yml@refs/tags/v0.16.1" \
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

Prints the version, the storage format version, the milestone, and **the build target**
— which is the first question in any "it works on my machine" report about a static
binary.

Every release also publishes `SHA256SUMS` and `sbom.json`, a CycloneDX 1.5 manifest of
everything linked into the binary.

## Signing

Not yet in place. Releases carry checksums, which protect against a corrupted download
but not against a compromised release. Named here as a known gap rather than implied to
be covered by an unsigned checksum file.
