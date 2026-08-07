#!/usr/bin/env python3
"""Generate a deterministic CycloneDX 1.5 SBOM from Cargo.lock.

Deterministic on purpose. An SBOM that changes on every run cannot be committed, and
one that is not committed cannot be diffed — so CI has no way to notice that a
dependency appeared. Three things are pinned to make byte-for-byte reproduction
possible:

  * components are sorted by (name, version)
  * the serial number is derived from the component list, not randomly generated
  * no build timestamp is emitted

That means `git diff --exit-code sbom.json` after regenerating is a real signal: it is
non-empty exactly when the dependency graph changed.

Reads `cargo metadata`, so it sees the resolved graph rather than the manifests.
Self-contained: standard library only, no cargo plugin, no network.

Usage:
    scripts/generate-sbom.py [--output sbom.json] [--check]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import uuid
from pathlib import Path

# Packages in this workspace are the subject of the SBOM, not entries in it.
WORKSPACE_PREFIX = "telemetryd"


def cargo_metadata(root: Path) -> dict:
    """The resolved dependency graph."""
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        sys.exit(f"cargo metadata failed:\n{result.stderr}")
    return json.loads(result.stdout)


def shipped_package_ids(metadata: dict) -> set[str]:
    """Package ids reachable from the binary through non-dev dependencies.

    An SBOM describes what a user receives. `cargo metadata` lists the whole graph,
    which includes proptest, criterion and a WebSocket client that exist only to test
    this code and are never linked into the binary. Listing them would overstate the
    attack surface a consumer inherits — and an SBOM that overstates is one people stop
    reading.
    """
    nodes = {node["id"]: node for node in metadata.get("resolve", {}).get("nodes", [])}
    roots = [
        package["id"]
        for package in metadata.get("packages", [])
        if package["name"].startswith(WORKSPACE_PREFIX)
    ]

    seen: set[str] = set()
    queue = list(roots)
    while queue:
        current = queue.pop()
        if current in seen:
            continue
        seen.add(current)

        for dep in nodes.get(current, {}).get("deps", []):
            kinds = {kind.get("kind") for kind in dep.get("dep_kinds", [])}
            # `None` is a normal dependency; "build" is linked through a build script.
            # "dev" is not shipped.
            if kinds and not (kinds & {None, "build"}):
                continue
            queue.append(dep["pkg"])

    return seen


def purl(name: str, version: str) -> str:
    """Package URL, as CycloneDX expects for a crates.io package."""
    return f"pkg:cargo/{name}@{version}"


def normalise_licenses(raw: str | None) -> list[dict]:
    """Split an SPDX expression into CycloneDX license entries.

    Dual licensing is preserved rather than flattened: `MIT OR Apache-2.0` means the
    consumer may pick either, and collapsing it to one would misstate their options.
    """
    if not raw:
        return []
    parts = [
        part.strip()
        for chunk in raw.replace(" OR ", "/").replace(" AND ", "/").split("/")
        for part in [chunk]
        if part.strip()
    ]
    return [{"license": {"id": part}} for part in sorted(set(parts))]


def build_components(metadata: dict) -> list[dict]:
    shipped = shipped_package_ids(metadata)

    components = []
    for package in metadata.get("packages", []):
        name = package["name"]
        if name.startswith(WORKSPACE_PREFIX) or package["id"] not in shipped:
            continue

        component = {
            "type": "library",
            "bom-ref": purl(name, package["version"]),
            "name": name,
            "version": package["version"],
            "purl": purl(name, package["version"]),
        }
        if description := package.get("description"):
            component["description"] = " ".join(description.split())
        if licenses := normalise_licenses(package.get("license")):
            component["licenses"] = licenses
        if repository := package.get("repository"):
            component["externalReferences"] = [
                {"type": "vcs", "url": repository},
            ]
        components.append(component)

    components.sort(key=lambda c: (c["name"], c["version"]))
    return components


def serial_number(components: list[dict]) -> str:
    """A UUID derived from the components, so identical input gives identical output."""
    digest = hashlib.sha256(
        json.dumps(components, sort_keys=True, separators=(",", ":")).encode()
    ).digest()
    return f"urn:uuid:{uuid.UUID(bytes=digest[:16], version=5)}"


def workspace_version(metadata: dict) -> str:
    for package in metadata.get("packages", []):
        if package["name"] == "telemetryd":
            return package["version"]
    return "0.0.0"


def build_sbom(metadata: dict) -> dict:
    components = build_components(metadata)
    version = workspace_version(metadata)

    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": serial_number(components),
        "version": 1,
        "metadata": {
            # No timestamp: it would change every run and make the file undiffable.
            "component": {
                "type": "application",
                "bom-ref": purl("telemetryd", version),
                "name": "telemetryd",
                "version": version,
                "purl": purl("telemetryd", version),
                "description": "Single-binary, zero-config observability backend",
                "licenses": [{"license": {"id": "Apache-2.0"}}],
            },
        },
        "components": components,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="sbom.json")
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the file on disk is stale, without rewriting it",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent
    target = root / args.output

    rendered = json.dumps(build_sbom(cargo_metadata(root)), indent=2) + "\n"

    if args.check:
        if not target.exists():
            print(f"{args.output} does not exist; run scripts/generate-sbom.py")
            return 1
        if target.read_text() != rendered:
            print(
                f"{args.output} is stale — the dependency graph changed.\n"
                f"Run scripts/generate-sbom.py and commit the result."
            )
            return 1
        print(f"{args.output} is up to date")
        return 0

    target.write_text(rendered)
    count = rendered.count('"type": "library"')
    print(f"wrote {args.output} with {count} components")
    return 0


if __name__ == "__main__":
    sys.exit(main())
