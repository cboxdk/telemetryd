#!/usr/bin/env python3
"""Tempo answers checked against Tempo itself.

`crates/server/tests/conformance/tempo.json` holds OTLP traces, queries — trace by id,
TraceQL search, tag listings, TraceQL metrics — and what Tempo answered to each. The
same file is read twice:

- by `crates/server/tests/tempo_conformance.rs`, which sends the traces into telemetryd
  through `/v1/traces`, asks every query, and requires Tempo's answers;
- by real Tempo, here.

`--update` writes the traces and queries defined below into the file and asks Tempo for
the answers; `--check` asks again and fails if Tempo now disagrees with the file. Both
need Docker and the `grafana/tempo` image.

    python3 scripts/tempo-conformance.py --check
    python3 scripts/tempo-conformance.py --update
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
CASES = ROOT / "crates/server/tests/conformance/tempo.json"
CONFIG = ROOT / "scripts/conformance/tempo.yaml"
# Pinned by digest; this is 2.10.1, the newest with a single-binary ingester that answers
# ranged search and TraceQL metrics over what it holds. The API is the same in 3.0.
IMAGE = "grafana/tempo@sha256:9371af1b75b4e057eb77f22dc4dd4d9176cd6985e29f181527be6723b7f29c41"

# As in the Loki suite: the file's times start here, and a run moves them to just before
# now by whole minutes and the answers back.
T0 = 1_790_000_000
NS = 1_000_000_000
MS = 1_000_000


def attr(key: str, value) -> dict:
    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    return {"key": key, "value": {"stringValue": value}}


def span(trace: str, span_id: str, parent: str | None, name: str, kind: int, start_ms: int,
         duration_ms: int, attributes: list, status: int = 0, message: str = "", events=()) -> dict:
    out = {
        "traceId": trace, "spanId": span_id, "name": name, "kind": kind,
        "startTimeUnixNano": str(T0 * NS + start_ms * MS),
        "endTimeUnixNano": str(T0 * NS + (start_ms + duration_ms) * MS),
        "attributes": attributes,
    }
    if parent:
        out["parentSpanId"] = parent
    if status:
        out["status"] = {"code": status, "message": message}
    if events:
        out["events"] = [{"timeUnixNano": str(T0 * NS + at * MS), "name": n, "attributes": a}
                         for at, n, a in events]
    return out


def traces() -> dict:
    """Two services, four traces: a slow failing checkout, fast ones, a lone job."""
    shop = {"attributes": [attr("service.name", "shop"), attr("deployment.environment", "prod"),
                           attr("k8s.pod.name", "shop-7f9")]}
    db = {"attributes": [attr("service.name", "db")]}
    t1, t2, t3, t4 = (f"{i:032x}" for i in (0xA1, 0xA2, 0xA3, 0xA4))
    shop_spans = [
        span(t1, "00000000000000b1", None, "POST /checkout", 2, 1_000, 1_200,
             [attr("http.method", "POST"), attr("http.status_code", 500), attr("http.route", "/checkout")],
             status=2, message="payment declined",
             events=[(1_900, "exception", [attr("exception.type", "PaymentError")])]),
        span(t1, "00000000000000b2", "00000000000000b1", "charge", 3, 1_100, 900,
             [attr("peer.service", "payments")]),
        span(t2, "00000000000000c1", None, "GET /products", 2, 20_000, 40,
             [attr("http.method", "GET"), attr("http.status_code", 200), attr("http.route", "/products")]),
        span(t3, "00000000000000d1", None, "GET /products", 2, 80_000, 60,
             [attr("http.method", "GET"), attr("http.status_code", 200), attr("http.route", "/products")]),
    ]
    db_spans = [
        span(t1, "00000000000000b3", "00000000000000b2", "SELECT orders", 3, 1_150, 300,
             [attr("db.system", "mysql"), attr("db.rows", 12)]),
        span(t2, "00000000000000c2", "00000000000000c1", "SELECT products", 3, 20_005, 20,
             [attr("db.system", "mysql"), attr("db.rows", 40)]),
        span(t4, "00000000000000e1", None, "vacuum", 1, 150_000, 5_000,
             [attr("db.system", "postgres")]),
    ]
    return {"resourceSpans": [
        {"resource": shop, "scopeSpans": [{"scope": {"name": "shop-tracer"}, "spans": shop_spans}]},
        {"resource": db, "scopeSpans": [{"scope": {"name": "db-tracer"}, "spans": db_spans}]},
    ]}


START, END = str(T0), str(T0 + 300)
# Wide around the traces: a ranged search over the ingester's newest minutes misses
# traces in Tempo, and what is compared is which traces match, not where a window cuts.
WIDE_START, WIDE_END = str(T0 - 600), str(T0 + 900)


def search(q: str, **extra) -> dict:
    return {"kind": "search", "q": q, "start": WIDE_START, "end": WIDE_END, "limit": 20, **extra}


def metrics(q: str, step: str = "60s") -> dict:
    return {"kind": "metrics", "q": q, "start": START, "end": END, "step": step}


QUERIES = [
    {"kind": "trace", "id": f"{0xA1:032x}"},
    {"kind": "trace", "id": f"{0xA4:032x}"},
    search("{}"),
    search('{ resource.service.name = "shop" }'),
    search("{ status = error }"),
    search('{ span.http.status_code >= 500 }'),
    search('{ .http.method = "GET" && duration > 50ms }'),
    search('{ name =~ "SELECT.*" }'),
    search('{ resource.k8s.pod.name = "shop-7f9" && kind = client }'),
    search('{ span.db.rows > 20 }'),
    search('{ .db.system != "mysql" }'),
    # Not `= nil`: Tempo 2.10 answers it for some of the spans that lack the attribute
    # and not others, by how its storage lays the attribute out.
    search("{}", minDuration="1s"),
    {"kind": "tags", "scope": "resource", "start": WIDE_START, "end": WIDE_END},
    {"kind": "tags", "scope": "span", "start": WIDE_START, "end": WIDE_END},
    {"kind": "values", "tag": "resource.service.name", "start": WIDE_START, "end": WIDE_END},
    {"kind": "values", "tag": "span.http.route", "start": WIDE_START, "end": WIDE_END},
    metrics("{} | count_over_time() by (resource.service.name)"),
    metrics("{ status = error } | rate()"),
    metrics("{} | max_over_time(duration) by (name)"),
]


def attribute_map(attributes: list) -> dict:
    out = {}
    for kv in attributes or []:
        value = kv.get("value", {})
        out[kv["key"]] = str(next(iter(value.values()), "")) if value else ""
    return dict(sorted(out.items()))


def canonical(kind: str, body: dict, shift: int) -> dict:
    """The answer, reduced to what both sides must agree on and moved back to the file's
    times."""
    if kind == "trace":
        spans = []
        for batch in body.get("batches") or body.get("resourceSpans") or []:
            resource = attribute_map(batch.get("resource", {}).get("attributes"))
            for scope in batch.get("scopeSpans") or batch.get("instrumentationLibrarySpans") or []:
                for s in scope.get("spans", []):
                    status = s.get("status") or {}
                    spans.append({
                        "resource": resource,
                        "spanId": s["spanId"],
                        "parentSpanId": s.get("parentSpanId", ""),
                        "name": s["name"],
                        "kind": s.get("kind"),
                        "start": int(s["startTimeUnixNano"]) - shift * NS,
                        "end": int(s["endTimeUnixNano"]) - shift * NS,
                        "status": [status.get("code", 0), status.get("message", "")],
                        "attributes": attribute_map(s.get("attributes")),
                        "events": [[int(e["timeUnixNano"]) - shift * NS, e["name"],
                                    attribute_map(e.get("attributes"))] for e in s.get("events", [])],
                    })
        spans.sort(key=lambda s: s["spanId"])
        return {"spans": spans}
    if kind == "search":
        rows = []
        for t in body.get("traces", []):
            matched = sorted(s["spanID"] for ss in t.get("spanSets") or [t.get("spanSet") or {}]
                             for s in ss.get("spans", []))
            rows.append({"traceID": t["traceID"], "rootServiceName": t.get("rootServiceName"),
                         "rootTraceName": t.get("rootTraceName"),
                         "start": int(t["startTimeUnixNano"]) - shift * NS,
                         "durationMs": t.get("durationMs", 0), "matched": matched})
        rows.sort(key=lambda r: r["traceID"])
        return {"traces": rows}
    if kind == "tags":
        return {"tags": sorted(tag for scope in body.get("scopes", []) for tag in scope.get("tags", []))}
    if kind == "values":
        return {"values": sorted(v["value"] for v in body.get("tagValues", []))}
    series = []
    for s in body.get("series", []):
        # Protobuf's JSON leaves zero out: a sample without `value` is a zero.
        labels = {kv["key"]: str(next(iter(kv.get("value", {}).values()), "")) for kv in s.get("labels", [])}
        points = [[int(p.get("timestampMs", 0)) // 1000 - shift, float(p.get("value", 0))]
                  for p in s.get("samples", [])]
        series.append({"labels": dict(sorted(labels.items())), "points": sorted(points)})
    series.sort(key=lambda s: json.dumps(s["labels"]))
    return {"series": series}


def ask(base: str, case: dict, shift: int) -> dict:
    kind = case["kind"]
    moved = {k: str(int(v) + shift) for k, v in case.items() if k in ("start", "end")}
    if kind == "trace":
        path = f"/api/traces/{case['id']}"
    elif kind == "search":
        params = {"q": case["q"], "limit": case["limit"], **moved}
        if "minDuration" in case:
            params["minDuration"] = case["minDuration"]
        path = f"/api/search?{urllib.parse.urlencode(params)}"
    elif kind == "tags":
        path = f"/api/v2/search/tags?{urllib.parse.urlencode({'scope': case['scope'], **moved})}"
    elif kind == "values":
        path = f"/api/v2/search/tag/{case['tag']}/values?{urllib.parse.urlencode(moved)}"
    else:
        params = {"q": case["q"], "step": case["step"], **moved}
        path = f"/api/metrics/query_range?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(base + path, timeout=30) as response:
            return canonical(kind, json.load(response), shift)
    except urllib.error.HTTPError as error:
        return {"error": error.code, "message": error.read().decode()[:300]}


def start_tempo() -> tuple[str, str, str]:
    container = subprocess.run(
        ["docker", "run", "-d", "--rm", "-p", "127.0.0.1::3200", "-p", "127.0.0.1::4318",
         "-v", f"{CONFIG}:/etc/tempo.yaml:ro", IMAGE, "-config.file=/etc/tempo.yaml"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    def port(inner: str) -> str:
        return subprocess.run(["docker", "port", container, inner], capture_output=True, text=True,
                              check=True).stdout.strip().rsplit(":", 1)[1]

    query, ingest = f"http://127.0.0.1:{port('3200')}", f"http://127.0.0.1:{port('4318')}"
    for _ in range(120):
        try:
            with urllib.request.urlopen(f"{query}/ready", timeout=2) as response:
                if response.status == 200:
                    return container, query, ingest
        except OSError:
            pass
        time.sleep(1)
    subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)
    sys.exit("Tempo did not become ready")


def shifted(payload: dict, shift: int) -> dict:
    text = json.dumps(payload)
    doc = json.loads(text)
    for rs in doc["resourceSpans"]:
        for ss in rs["scopeSpans"]:
            for s in ss["spans"]:
                s["startTimeUnixNano"] = str(int(s["startTimeUnixNano"]) + shift * NS)
                s["endTimeUnixNano"] = str(int(s["endTimeUnixNano"]) + shift * NS)
                for e in s.get("events", []):
                    e["timeUnixNano"] = str(int(e["timeUnixNano"]) + shift * NS)
    return doc


def send(ingest: str, payload: dict) -> None:
    request = urllib.request.Request(f"{ingest}/v1/traces", data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status == 200, response.status


def close(a, b) -> bool:
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
    doc = {"traces": traces(), "cases": QUERIES} if mode == "--update" else json.loads(CASES.read_text())
    container, query, ingest = start_tempo()
    try:
        # Tempo's ingester answers a range by when it received a trace, so the queries'
        # window, which ends 300 s after the first span, has to reach past now.
        shift = (time.time_ns() // NS - 200 - T0) // 60 * 60
        send(ingest, shifted(doc["traces"], shift))
        # Search reads the ingester's blocks, metrics the generator's.
        time.sleep(25)
        failures = 0
        for case in doc["cases"]:
            got = ask(query, case, shift)
            if mode == "--update":
                case["expect"] = got
            elif not close(got, case["expect"]):
                failures += 1
                print(f"Tempo now disagrees on {json.dumps({k: v for k, v in case.items() if k != 'expect'})}:\n"
                      f"  file:  {json.dumps(case['expect'])[:400]}\n  tempo: {json.dumps(got)[:400]}")
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)
    if mode == "--update":
        CASES.write_text(json.dumps(doc, indent=1) + "\n")
        print(f"wrote {len(doc['cases'])} expectations from Tempo ({IMAGE})")
        return 0
    if failures:
        return 1
    print(f"Tempo ({IMAGE}) agrees with all {len(doc['cases'])} expectations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
