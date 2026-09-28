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

EXP = re.compile(r"^\s*exp: (?P<samples>.*)$")
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


def parse_samples(text: str) -> list[tuple[str, float]]:
    if text.strip() == "nil":
        return []
    return [(m["labels"], float(m["value"])) for m in SAMPLE.finditer(text)]


def only_rounding(output: str) -> bool:
    """Whether every disagreement promtool reports is in the last bits of a float.

    promtool compares exactly, and Go fuses multiply-add on arm64 but not on amd64: the
    same Prometheus answered 26.3 on an ARM laptop and 26.299999999999997 on the x86 CI
    runner. Labels and sample counts still have to match exactly.
    """
    expected = None
    compared = 0
    for line in output.splitlines():
        if match := EXP.match(line):
            expected = parse_samples(match["samples"])
        elif (match := GOT.match(line)) and expected is not None:
            got = parse_samples(match["samples"])
            if [l for l, _ in expected] != [l for l, _ in got]:
                return False
            for (_, a), (_, b) in zip(expected, got):
                if abs(a - b) > 1e-12 * max(abs(a), abs(b), 1.0):
                    return False
            compared += 1
            expected = None
    return compared > 0


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
    if code != 0 and only_rounding(output):
        print("Prometheus differs from the file only in the last bits of some floats "
              "(this machine's floating point); accepted")
        code = 0
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
