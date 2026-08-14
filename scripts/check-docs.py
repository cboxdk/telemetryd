#!/usr/bin/env python3
"""Check the docs tree against the cboxdk documentation standard, and check links.

Two rules from the standard, both of which downgrade a package from `complete` to
`partial` on the docs site:

  * every subfolder has an `_index.md` (that exact filename; `index.md` does not count)
  * every file carries `title`, `weight` and `description` frontmatter

Plus one rule that is ours: no relative link may dangle. Moving a doc and leaving a
dead link behind is the most common way documentation rots, and it is trivially
checkable.

Usage:
    scripts/check-docs.py
"""

from __future__ import annotations

import pathlib
import re
import sys

DOCS = pathlib.Path(__file__).resolve().parent.parent / "docs"
REPO = DOCS.parent

# The standard exempts the three root files from the `_index.md` rule.
ROOT_FILES = {"index.md", "quickstart.md", "requirements.md"}
REQUIRED_FRONTMATTER = ("title:", "weight:", "description:")


def check_structure() -> list[str]:
    problems = []

    for name in ROOT_FILES:
        if not (DOCS / name).exists():
            problems.append(f"docs/{name} is missing (required at the docs root)")

    for folder in sorted(p for p in DOCS.rglob("*") if p.is_dir()):
        if not (folder / "_index.md").exists():
            rel = folder.relative_to(REPO)
            problems.append(f"{rel}/ has no _index.md (section landing)")

    # A flat docs root is the failure the standard is written against.
    stray = [p.name for p in DOCS.glob("*.md") if p.name not in ROOT_FILES]
    for name in stray:
        problems.append(f"docs/{name} is at the root; only {sorted(ROOT_FILES)} belong there")

    return problems


def check_frontmatter() -> list[str]:
    problems = []
    for md in sorted(DOCS.rglob("*.md")):
        rel = md.relative_to(REPO)
        text = md.read_text()
        if not text.startswith("---\n"):
            problems.append(f"{rel} has no frontmatter")
            continue
        block = text.split("---\n", 2)[1]
        for field in REQUIRED_FRONTMATTER:
            if field not in block:
                problems.append(f"{rel} is missing {field.rstrip(':')}")
    return problems


def check_links() -> list[str]:
    problems = []
    files = list(REPO.glob("*.md")) + list(DOCS.rglob("*.md"))
    for md in sorted(files):
        for match in re.finditer(r"\[([^\]]*)\]\(([^)]+)\)", md.read_text()):
            target = match.group(2).split("#")[0].strip()
            if not target or target.startswith(("http://", "https://", "mailto:")):
                continue
            if not (md.parent / target).resolve().exists():
                rel = md.relative_to(REPO)
                problems.append(f"{rel}: dangling link [{match.group(1)}]({target})")
    return problems


def check_versions() -> list[str]:
    """Catch a version string in the docs that the release left behind.

    A worked example is worth more than a placeholder, so the docs quote real version
    numbers — and a quoted number is a fact that goes stale on its own, silently, every
    time the crate is bumped. This is the cheapest place to notice.
    """
    version = re.search(
        r'^version = "([^"]+)"', (REPO / "Cargo.toml").read_text(), re.M
    )
    if not version:
        return ["Cargo.toml has no workspace version to check the docs against"]
    current = version.group(1)

    problems = []
    pattern = re.compile(r"\b\d+\.\d+\.\d+\b")
    for md in sorted(list(DOCS.rglob("*.md")) + [REPO / "README.md", REPO / "llms.txt"]):
        for number, line in (
            (match.group(0), line)
            for line in md.read_text().splitlines()
            # Only lines presenting telemetryd's own version. A dependency's version, a
            # Rust release or a port number is none of our business.
            if '"version"' in line or "telemetryd 0." in line
            for match in [pattern.search(line)]
            if match
        ):
            if number != current:
                rel = md.relative_to(REPO)
                problems.append(f"{rel}: says version {number}, crate is {current}")

    # The Homebrew formula is not a doc, but it is the same failure and it is worse: a
    # stale one fails the release *after* the tag is published, when the fix costs
    # another version rather than another line. CI catches it and the publish script
    # refuses to run — both of them too late to be useful. This is the check running at
    # the moment it can still be acted on.
    formula = REPO / "packaging" / "telemetryd.rb"
    declared = re.search(r'^\s*version "([^"]+)"', formula.read_text(), re.M)
    if not declared:
        problems.append("packaging/telemetryd.rb declares no version")
    elif declared.group(1) != current:
        problems.append(
            f"packaging/telemetryd.rb: says version {declared.group(1)}, crate is "
            f"{current} — bump it in the same commit or the release fails after tagging"
        )
    return problems


def main() -> int:
    problems = (
        check_structure() + check_frontmatter() + check_links() + check_versions()
    )

    files = list(DOCS.rglob("*.md"))
    folders = [p for p in DOCS.rglob("*") if p.is_dir()]
    print(f"checked {len(files)} files in {len(folders)} folders")

    if problems:
        print("\nproblems:")
        for problem in problems:
            print(f"  - {problem}")
        return 1

    print("metadata_quality: complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
