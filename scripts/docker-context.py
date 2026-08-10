#!/usr/bin/env python3
"""Assemble the Docker build context.

The image copies a prebuilt binary rather than compiling one, so that what ships in the
container is byte-identical to the artifact the release workflow soak-tests and signs.
This script is what puts that artifact where the Dockerfile expects it.

Two sources, because the two callers want different things:

  release <tag>   download the published linux-musl assets and verify them against the
                  release's own SHA256SUMS. What CI uses.
  local           cross-compile with `cross` in a container. What a developer uses to
                  try a change before tagging it — the binary is then unsigned and
                  unverified, and the script says so rather than pretending otherwise.

Usage:
  scripts/docker-context.py release v0.21.0 [--arch amd64,arm64]
  scripts/docker-context.py local [--arch arm64]
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import urllib.request

CONTEXT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docker")
REPO = "cboxdk/telemetryd"

# Docker's TARGETARCH values, and the Rust target each maps to.
TARGETS = {
    "amd64": "x86_64-unknown-linux-musl",
    "arm64": "aarch64-unknown-linux-musl",
}


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=300) as response:  # noqa: S310
        return response.read()


def from_release(tag: str, arches: list[str]) -> None:
    base = f"https://github.com/{REPO}/releases/download/{tag}"
    sums = fetch(f"{base}/SHA256SUMS").decode()
    expected = {}
    for line in sums.splitlines():
        parts = line.split()
        if len(parts) == 2:
            expected[parts[1].lstrip("*")] = parts[0]

    for arch in arches:
        target = TARGETS[arch]
        # Found rather than constructed. The first version of this built the filename
        # from the tag — `telemetryd-v0.21.0-<target>.tar.gz` — and the assets are named
        # after the *version*, so the job failed on its first real release. SHA256SUMS
        # already lists every published asset, so ask it instead of guessing, and a
        # future rename cannot break this again.
        matches = [name for name in expected
                   if target in name and name.endswith(".tar.gz")]
        if len(matches) != 1:
            sys.exit(f"expected exactly one {target} archive in SHA256SUMS, found "
                     f"{len(matches)}: {', '.join(sorted(matches)) or 'none'}")
        name = matches[0]

        blob = fetch(f"{base}/{name}")
        actual = hashlib.sha256(blob).hexdigest()
        if actual != expected[name]:
            sys.exit(f"{name}: checksum mismatch (expected {expected[name]}, got {actual})")

        archive = os.path.join(CONTEXT, name)
        with open(archive, "wb") as handle:
            handle.write(blob)
        # Extract into a scratch directory and find the binary wherever it sits, for the
        # same reason: the archive's top-level directory name is not ours to predict.
        staging = os.path.join(CONTEXT, f".staging-{arch}")
        shutil.rmtree(staging, ignore_errors=True)
        os.makedirs(staging)
        subprocess.run(["tar", "xzf", archive, "-C", staging], check=True)
        os.remove(archive)

        found = [os.path.join(root, "telemetryd")
                 for root, _, files in os.walk(staging) if "telemetryd" in files]
        if len(found) != 1:
            sys.exit(f"{name}: expected one telemetryd binary, found {len(found)}")
        shutil.move(found[0], os.path.join(CONTEXT, f"telemetryd-linux-{arch}"))
        shutil.rmtree(staging, ignore_errors=True)
        print(f"  {arch}: {name} verified against the release SHA256SUMS")


def from_local(arches: list[str]) -> None:
    print("  building locally — this binary is not the signed release artifact")
    for arch in arches:
        target = TARGETS[arch]
        subprocess.run(["cross", "build", "--profile", "dist", "--target", target,
                        "--bin", "telemetryd"], check=True)
        built = os.path.join("target", target, "dist", "telemetryd")
        shutil.copy2(built, os.path.join(CONTEXT, f"telemetryd-linux-{arch}"))
        print(f"  {arch}: {built}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=["release", "local"])
    parser.add_argument("tag", nargs="?")
    parser.add_argument("--arch", default="amd64,arm64")
    args = parser.parse_args()

    arches = [a.strip() for a in args.arch.split(",") if a.strip()]
    for arch in arches:
        if arch not in TARGETS:
            sys.exit(f"unknown architecture {arch!r}; expected one of {', '.join(TARGETS)}")

    if args.source == "release":
        if not args.tag:
            sys.exit("release needs a tag, e.g. scripts/docker-context.py release v0.21.0")
        from_release(args.tag, arches)
    else:
        from_local(arches)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
