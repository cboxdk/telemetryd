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
        name = f"telemetryd-{tag}-{target}.tar.gz"
        if name not in expected:
            sys.exit(f"{name} is not in the release's SHA256SUMS — nothing to verify against")

        blob = fetch(f"{base}/{name}")
        actual = hashlib.sha256(blob).hexdigest()
        if actual != expected[name]:
            sys.exit(f"{name}: checksum mismatch (expected {expected[name]}, got {actual})")

        archive = os.path.join(CONTEXT, name)
        with open(archive, "wb") as handle:
            handle.write(blob)
        subprocess.run(["tar", "xzf", archive, "-C", CONTEXT], check=True)
        os.remove(archive)

        extracted = os.path.join(CONTEXT, f"telemetryd-{tag}-{target}", "telemetryd")
        shutil.move(extracted, os.path.join(CONTEXT, f"telemetryd-linux-{arch}"))
        shutil.rmtree(os.path.join(CONTEXT, f"telemetryd-{tag}-{target}"), ignore_errors=True)
        print(f"  {arch}: verified against the release SHA256SUMS")


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
