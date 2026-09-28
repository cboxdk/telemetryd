#!/usr/bin/env python3
"""LogQL answers checked against Loki itself.

`crates/server/tests/conformance/logql.json` holds log lines, queries, and what Loki
answered to each. The same file is read twice:

- by `crates/server/tests/logql_conformance.rs`, which pushes the lines into telemetryd
  through `/loki/api/v1/push`, asks every query, and requires Loki's answers;
- by real Loki, here.

So the expectations are not ours. `--update` writes the lines and queries defined below
into the file and asks Loki for the answers; `--check` asks again and fails if Loki now
disagrees with the file — which keeps a hand edit, or a Loki release that changed an
answer, from passing silently. Both need Docker and the `grafana/loki` image.

    python3 scripts/loki-conformance.py --check
    python3 scripts/loki-conformance.py --update
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import time
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
CASES = ROOT / "crates/server/tests/conformance/logql.json"
CONFIG = ROOT / "scripts/conformance/loki.yaml"
# Pinned by digest: an answer that changes should change because we chose a newer Loki,
# not because a tag moved. This is 3.7.8.
IMAGE = "grafana/loki@sha256:1107dd5274e0ada47e42472b7a7e71f3b2a2fe878878108f3e2f9e51528f0193"

# The file's lines start here, in whole seconds. Loki answers only recent data from a
# fresh ingester, so a run moves every line and query forward to just before now by a
# whole number of minutes and moves the answers back; the file never changes with the
# date it was checked on.
T0 = 1_790_000_000
NS = 1_000_000_000


def lines() -> list[dict]:
    """Four streams over five minutes: logfmt, JSON, plain text, and metadata."""
    shop_info, shop_error, api_info, api_warn = [], [], [], []
    for i in range(30):
        at = str((T0 + i * 10) * NS)
        path = ["/a", "/b", "/checkout"][i % 3]
        took = ["12ms", "250ms", "1.5s"][i % 3]
        size = ["1KB", "20KB", "3MB"][i % 3]
        shop_info.append([at, f"method=GET path={path} status=200 took={took} size={size}"])
        if i % 3 == 0:
            shop_error.append([
                str((T0 + i * 10 + 1) * NS),
                f'method=POST path=/pay status={500 + i % 7} took=2s msg="upstream timed out"',
                {"trace_id": f"{i:032x}"},
            ])
        api_info.append([
            str((T0 + i * 10 + 2) * NS),
            json.dumps({"route": path, "status": 200 + (i % 4) * 100, "duration": round(0.05 * (i + 1), 3),
                        "user": {"id": i % 5}}),
        ])
        if i % 5 == 0:
            api_warn.append([str((T0 + i * 10 + 3) * NS), f"disk {80 + i // 5}% full on /var"])
    return [
        {"stream": {"app": "shop", "level": "info", "env": "prod"}, "values": shop_info},
        {"stream": {"app": "shop", "level": "error", "env": "prod"}, "values": shop_error},
        {"stream": {"app": "api", "level": "info"}, "values": api_info},
        {"stream": {"app": "api", "level": "warn"}, "values": api_warn},
    ]


START, END = str(T0 * NS), str((T0 + 300) * NS)


def logs(query: str, **extra) -> dict:
    return {"kind": "range", "query": query, "start": START, "end": END, "limit": 1000, **extra}


def metric(query: str, step: str = "60s", **extra) -> dict:
    return {"kind": "range", "query": query, "start": START, "end": END, "step": step, **extra}


def instant(query: str, at: int = T0 + 300) -> dict:
    return {"kind": "instant", "query": query, "time": str(at * NS)}


QUERIES = [
    # selectors and line filters
    logs('{app="shop"}'),
    logs('{app="shop"}', direction="forward", limit=5),
    logs('{app=~"sh.p|api", level!="info"}'),
    logs('{app="shop"} |= "checkout"'),
    logs('{app="shop"} != "GET"'),
    logs('{app="shop"} |~ "status=5[0-9]{2}"'),
    logs('{app="shop"} !~ "took=[0-9]+ms"'),
    # parsers and label filters
    logs('{app="shop"} | logfmt | status >= 503'),
    logs('{app="shop"} | logfmt | took > 200ms'),
    logs('{app="shop"} | logfmt | size >= 20KB'),
    logs('{app="shop"} | logfmt | path="/checkout" or path="/pay"'),
    logs('{app="api"} | json | status = 300'),
    logs('{app="api"} | json | user_id >= 3 and route =~ "/[ab]"'),
    logs('{app="api", level="info"} | json | __error__=""'),
    logs('{app="api"} | json | __error__!=""'),
    logs('{app="shop", level="info"} | pattern "method=<method> path=<path> <_>"'),
    logs('{app="shop", level="info"} | regexp "path=(?P<route>\\\\S+)"'),
    # formatting
    logs('{app="shop", level="info"} | logfmt | line_format "{{.method}} {{.path}} {{.status}}"'),
    logs('{app="shop", level="info"} | logfmt | label_format route=path'),
    logs('{app="shop", level="info"} | logfmt | drop took, size'),
    logs('{app="shop", level="info"} | logfmt | keep path'),
    # metric queries
    metric('count_over_time({app="shop"}[1m])'),
    metric('sum by (level) (count_over_time({app=~".+"}[1m]))'),
    metric('sum(rate({app="shop"} |= "POST" [2m]))'),
    metric('sum by (app) (bytes_over_time({app=~".+"}[1m]))'),
    metric('sum(bytes_rate({app="api"}[1m]))'),
    metric('sum by (path) (sum_over_time({app="shop", level="info"} | logfmt | unwrap duration(took) [2m]))'),
    metric('max_over_time({app="shop", level="info"} | logfmt | unwrap bytes(size) [5m]) by (path)'),
    metric('avg_over_time({app="api"} | json | unwrap duration [2m]) by (route)'),
    metric('quantile_over_time(0.9, {app="api"} | json | unwrap duration [5m]) by (app)'),
    # Ties break however each implementation iterates; PromQL leaves them unspecified.
    metric('topk(1, sum by (level) (count_over_time({app=~".+"}[1m])))'),
    metric('sum(count_over_time({app="shop"} |= "POST" [1m])) / sum(count_over_time({app="shop"}[1m]))'),
    metric('sum by (level) (count_over_time({app=~".+"}[1m])) > 5'),
    metric('absent_over_time({app="nope"}[1m])'),
    metric('sum(count_over_time({app="shop"}[1m] offset 2m))'),
    instant('sum by (app, level) (count_over_time({app=~".+"}[5m]))'),
    instant('rate({app="api", level="warn"}[5m])'),
    instant('sum(count_over_time({app="api"} | json | __error__="" [5m]))'),
    instant('vector(1)+vector(1)'),
    # the edges Loki's own answers pinned down
    logs('{app="shop", level="info"} | logfmt | took > 1'),
    logs('{app="shop", level="info"} | logfmt | json | status < 1'),
    logs('{app="api", level="warn"} | json | status != "300"'),
    logs('{app="shop"} | detected_level="error"'),
    logs('{app="shop"} | trace_id=~"0+1b"'),
    logs('{app="shop", level="info"} | logfmt | drop path="/a"'),
    logs('{app="shop", level="info"} | logfmt | keep method, status="200"'),
    logs('{app="shop", level="info"} | logfmt | label_format summary="{{.method}} {{ToUpper .path}}"'),
    logs('{app="shop", level="info"} | logfmt | line_format "{{.path | trunc 3}}|{{default \\"none\\" .missing}}"'),
    instant('sum(count_over_time({app="api"} | json [5m]))'),
    instant('sum by (app) (count_over_time({app="api"} | json [5m]))'),
    instant('max_over_time({app="api", level="warn"} | json | unwrap duration [5m])'),
    instant('max_over_time({app="shop", level="info"} | logfmt | unwrap size [5m])'),
    instant('avg_over_time({app="shop"} | regexp "status=(?P<s>200)" | unwrap s [5m]) by (app)'),
    instant('avg_over_time({app="shop"} | regexp "status=(?P<s>200)" | unwrap s [5m])'),
    instant('stddev_over_time({app="api"} | json | __error__="" | unwrap duration [5m]) by (app)'),
    instant('first_over_time({app="api"} | json | __error__="" | unwrap duration [5m]) by (app)'),
    instant('last_over_time({app="api"} | json | __error__="" | unwrap duration [5m]) by (app)'),
    instant('sum_over_time({app="api"} | json | unwrap duration [5m]) by (app)'),
    instant('rate({app="api"}[5m]) by (app)'),
    instant('sort_desc(sum by (level) (count_over_time({app=~".+"}[5m])))'),
    instant('label_replace(sum by (app) (count_over_time({app=~".+"}[5m])), "svc", "$1", "app", "(.*)")'),
    instant('sum by (level) (count_over_time({app=~".+"}[5m])) > bool 10'),
]


def canonical(body: dict) -> dict:
    """The answer, reduced to what both sides must agree on and ordered one way."""
    data = body["data"]
    kind = data["resultType"]
    result = data["result"]
    if kind == "streams":
        streams = []
        for s in result:
            entries = []
            for v in s["values"]:
                extra = v[2] if len(v) > 2 else {}
                entries.append([v[0], v[1], dict(sorted(extra.get("structuredMetadata", {}).items())),
                                dict(sorted(extra.get("parsed", {}).items()))])
            streams.append({"stream": dict(sorted(s["stream"].items())), "entries": entries})
        streams.sort(key=lambda s: json.dumps(s["stream"]))
        return {"streams": streams}
    if kind == "matrix":
        series = [{"metric": dict(sorted(s["metric"].items())),
                   "values": [[float(t), float(v)] for t, v in s["values"]]} for s in result]
        series.sort(key=lambda s: json.dumps(s["metric"]))
        return {"matrix": series}
    if kind == "vector":
        series = [{"metric": dict(sorted(s["metric"].items())),
                   "value": [float(s["value"][0]), float(s["value"][1])]} for s in result]
        series.sort(key=lambda s: json.dumps(s["metric"]))
        return {"vector": series}
    return {"scalar": [float(result[0]), float(result[1])]}


def shifted(streams: list[dict], shift: int) -> list[dict]:
    return [{"stream": s["stream"], "values": [[str(int(v[0]) + shift * NS), *v[1:]] for v in s["values"]]}
            for s in streams]


def unshift(answer: dict, shift: int) -> dict:
    """Loki's answer, moved back to the file's times."""
    if "streams" in answer:
        for s in answer["streams"]:
            for entry in s["entries"]:
                entry[0] = str(int(entry[0]) - shift * NS)
    for key in ("matrix",):
        for s in answer.get(key, []):
            for point in s["values"]:
                point[0] -= shift
    for s in answer.get("vector", []):
        s["value"][0] -= shift
    if "scalar" in answer:
        answer["scalar"][0] -= shift
    return answer


def ask(base: str, case: dict, shift: int) -> dict:
    params = {k: v for k, v in case.items() if k not in ("kind", "expect")}
    for key in ("start", "end", "time"):
        if key in params:
            params[key] = str(int(params[key]) + shift * NS)
    path = "query_range" if case["kind"] == "range" else "query"
    url = f"{base}/loki/api/v1/{path}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"X-Loki-Response-Encoding-Flags": "categorize-labels"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return unshift(canonical(json.load(response)), shift)
    except urllib.error.HTTPError as error:
        return {"error": error.code, "message": error.read().decode()[:300]}


def start_loki() -> tuple[str, str]:
    container = subprocess.run(
        ["docker", "run", "-d", "--rm", "-p", "127.0.0.1::3100",
         "-v", f"{CONFIG}:/etc/loki/conformance.yaml:ro", IMAGE,
         "-config.file=/etc/loki/conformance.yaml"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    port = subprocess.run(["docker", "port", container, "3100"], capture_output=True, text=True,
                          check=True).stdout.strip().rsplit(":", 1)[1]
    base = f"http://127.0.0.1:{port}"
    for _ in range(120):
        try:
            with urllib.request.urlopen(f"{base}/ready", timeout=2) as response:
                if response.status == 200:
                    return container, base
        except OSError:
            pass
        time.sleep(1)
    subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)
    sys.exit("Loki did not become ready")


def push(base: str, streams: list[dict]) -> None:
    body = json.dumps({"streams": streams}).encode()
    request = urllib.request.Request(f"{base}/loki/api/v1/push", data=body,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status == 204, response.status


def wait_for_ingester(base: str) -> None:
    """`/ready` answers before the ingester has joined its ring, and a push then is
    accepted and dropped. Push a probe until a query finds it."""
    for _ in range(90):
        now = time.time_ns()
        push(base, [{"stream": {"probe": "ingester"}, "values": [[str(now), "probe"]]}])
        query = urllib.parse.urlencode({"query": '{probe="ingester"}', "start": now - NS, "end": now + NS})
        with urllib.request.urlopen(f"{base}/loki/api/v1/query_range?{query}", timeout=10) as response:
            if json.load(response)["data"]["result"]:
                return
        time.sleep(1)
    sys.exit("Loki's ingester never started taking lines")


def close(a, b) -> bool:
    # An error matches on its status: the message names whichever series failed first,
    # which is not an order either side promises.
    if isinstance(a, dict) and isinstance(b, dict) and "error" in b:
        return a.get("error") == b["error"]
    if isinstance(a, float) and isinstance(b, float):
        return a == b or abs(a - b) <= 1e-9 * max(abs(a), abs(b), 1.0)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(close(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(close(a[k], b[k]) for k in a)
    return a == b


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else "--check"
    if mode not in ("--check", "--update"):
        sys.exit(__doc__)
    if mode == "--update":
        doc = {"lines": lines(), "cases": QUERIES}
    else:
        doc = json.loads(CASES.read_text())
    container, base = start_loki()
    try:
        wait_for_ingester(base)
        shift = (time.time_ns() // NS - 1800 - T0) // 60 * 60
        push(base, shifted(doc["lines"], shift))
        time.sleep(2)
        failures = 0
        for case in doc["cases"]:
            got = ask(base, case, shift)
            if mode == "--update":
                case["expect"] = got
            elif not close(got, case["expect"]):
                failures += 1
                print(f"Loki now disagrees on {case['query']!r}:\n  file: {json.dumps(case['expect'])[:400]}\n"
                      f"  loki: {json.dumps(got)[:400]}")
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)
    if mode == "--update":
        CASES.write_text(json.dumps(doc, indent=1) + "\n")
        print(f"wrote {len(doc['cases'])} expectations from Loki ({IMAGE})")
        return 0
    if failures:
        return 1
    print(f"Loki ({IMAGE}) agrees with all {len(doc['cases'])} expectations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
