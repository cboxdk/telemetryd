#!/usr/bin/env python3
"""PromQL answers checked against Prometheus itself.

`crates/query/tests/conformance/promql.json` is a promtool unit-test file — JSON, which
promtool's YAML reader takes as it is — holding input series, expressions and the
samples each should evaluate to. The same file is read twice:

- by `crates/query/tests/promql_conformance.rs`, which evaluates every expression with
  telemetryd and requires the same samples;
- by real Prometheus, through `promtool test rules`, here.

So the expectations are not ours. `--update` asks Prometheus for them and writes them
in; `--check` asks again and fails if Prometheus now disagrees with the file — which is
what keeps a hand edit, or a Prometheus release that changed an answer, from passing
silently. Both need Docker and the `prom/prometheus` image.

    python3 scripts/promql-conformance.py --check
    python3 scripts/promql-conformance.py --update
"""

from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
CASES = ROOT / "crates/query/tests/conformance/promql.json"
# Pinned by digest: an answer that changes should change because we chose a newer
# Prometheus, not because a tag moved. This is v3.15.0.
IMAGE = "prom/prometheus@sha256:efd719c99d83b060d9daefdcf00360461adf279f45ef5391f8d111892118753e"

EXPR = re.compile(r'^\s*expr: "(?P<expr>.*)", time: (?P<time>[^,]+),\s*$')
GOT = re.compile(r"^\s*got: (?P<samples>.*)$")
SAMPLE = re.compile(r'(?P<labels>[a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?|\{[^}]*\}) (?P<value>\S+?)(?:, |$)')


def promtool(path: pathlib.Path) -> tuple[int, str]:
    result = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{path.parent}:/w:ro", "-w", "/w",
         "--entrypoint", "promtool", IMAGE, "test", "rules", path.name],
        capture_output=True, text=True, check=False,
    )
    return result.returncode, result.stdout + result.stderr


def answers(output: str) -> dict[tuple[str, str], list[dict]]:
    """What Prometheus said each failing expression evaluates to."""
    found: dict[tuple[str, str], list[dict]] = {}
    current = None
    for line in output.splitlines():
        if match := EXPR.match(line):
            expr = match["expr"].encode().decode("unicode_escape")
            current = (expr, match["time"].strip())
        elif (match := GOT.match(line)) and current:
            samples = []
            if match["samples"].strip() != "nil":
                for sample in SAMPLE.finditer(match["samples"]):
                    value = float(sample["value"])
                    if value != value or value in (float("inf"), float("-inf")):
                        sys.exit(f"{current}: {sample['value']} cannot be written in JSON; "
                                 "choose inputs whose answer is finite")
                    samples.append({"labels": sample["labels"], "value": value})
            found[current] = samples
            current = None
    return found


def update() -> int:
    doc = json.loads(CASES.read_text())
    for case in doc["tests"][0]["promql_expr_test"]:
        case["exp_samples"] = []
    CASES.write_text(json.dumps(doc, indent=1) + "\n")
    _, output = promtool(CASES)
    found = answers(output)
    for case in doc["tests"][0]["promql_expr_test"]:
        case["exp_samples"] = found.get((case["expr"], case["eval_time"]), [])
    CASES.write_text(json.dumps(doc, indent=1) + "\n")
    return check()


def check() -> int:
    code, output = promtool(CASES)
    if code != 0:
        print(output)
        print(f"Prometheus ({IMAGE}) disagrees with {CASES.relative_to(ROOT)}")
        return 1
    cases = len(json.loads(CASES.read_text())["tests"][0]["promql_expr_test"])
    print(f"Prometheus ({IMAGE}) agrees with all {cases} expectations")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--update"]:
        sys.exit(update())
    if sys.argv[1:] == ["--check"]:
        sys.exit(check())
    sys.exit(__doc__)
