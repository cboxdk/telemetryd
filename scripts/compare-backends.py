#!/usr/bin/env python3
"""telemetryd's Prometheus API against Prometheus 3.15 and Mimir 3.2.1, on the same data.

Starts Prometheus and Mimir in Docker and one or two telemetryd binaries on fresh data
directories, pushes each the same day (or `--days`) of one app's OTLP request histogram
— 25 routes by 2 methods, 15 buckets, a count and a sum, 850 series at 30 s — and times
the queries a Laravel telemetry dashboard sends: the p95 over the window as an instant
query and in hourly slices, per-route counts and sums, and four charts. Every answer is
compared with Prometheus's, within 1e-6 relative; any difference fails the run.

    python3 scripts/compare-backends.py target/release/telemetryd
    python3 scripts/compare-backends.py --baseline /path/to/telemetryd-0.69.3 target/release/telemetryd
    python3 scripts/compare-backends.py --days 7 target/release/telemetryd
    python3 scripts/compare-backends.py --sealed target/release/telemetryd

Two things make the comparison fair, and both were wrong in the first version of it: every
backend receives the same timestamps (one `now` for all pushes, so the samples sit on one
30 s grid), and every instant query names its `time` (so the backends evaluate the same
window rather than each its own `now`, seconds apart). Without them the backends disagree
by up to 1e-2 on data they hold identically.

`--sealed` pushes telemetryd an hour at a time with a pause for a seal in between, so it
holds hourly segments as a live server would rather than one buffer of everything.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request

PROMETHEUS = "prom/prometheus:v3.15.0"
MIMIR = "grafana/mimir:3.2.1"
LES = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]
B = "http_server_request_duration_seconds_bucket"
C = "http_server_request_duration_seconds_count"
S = "http_server_request_duration_seconds_sum"
BY = "http_route, http_request_method"
TOL = 1e-6


def batches(days: float, now: int):
    """The benchmark's data, seeded: lists of (timestamp, per-series snapshot)."""
    rng = random.Random(7)
    start = now - int((days * 24 + 1) * 3600)
    names = ("geocode", "reverse", "batch", "lists", "places", "census", "timezone",
             "districts", "acs", "zip4", "school", "stats", "keys")
    routes = [f"/api/v{v}/{n}" for v in (1, 2) for n in names][:25]
    series = []
    for i, route in enumerate(routes):
        for method in ("GET", "POST"):
            series.append(dict(
                r=route, m=method, code="500" if i % 11 == 0 and method == "POST" else "200",
                scale=0.02 + (i % 7) * 0.03 + (0.05 if method == "POST" else 0),
                rate=5 + (i * 3) % 40, counts=[0] * (len(LES) + 1), count=0, total=0.0))
    batch = []
    for ts in range(start, now + 1, 30):
        snap = []
        for s in series:
            n = max(0, int(rng.gauss(s["rate"] * 30, math.sqrt(s["rate"] * 30))))
            k = n / max(1, min(n, 40))
            for _ in range(min(n, 40)):
                v = rng.lognormvariate(math.log(s["scale"]), 0.8)
                s["total"] += v * k
                for j, le in enumerate(LES):
                    if v <= le:
                        s["counts"][j] += k
                        break
                else:
                    s["counts"][-1] += k
            s["count"] += n
            ints = [int(c) for c in s["counts"]]
            ints[-1] += s["count"] - sum(ints)
            snap.append((ints, s["count"], round(s["total"], 3)))
        batch.append((ts, snap))
        if len(batch) == 60:
            yield start, series, batch
            batch = []
    if batch:
        yield start, series, batch


def body(start: int, series: list, batch: list) -> bytes:
    attr = lambda k, v: {"key": k, "value": {"stringValue": v}}  # noqa: E731
    points = []
    for ts, snap in batch:
        for s, (counts, count, total) in zip(series, snap):
            points.append({
                "attributes": [attr("http.route", s["r"]), attr("http.request.method", s["m"]),
                               attr("http.response.status_code", s["code"])],
                "startTimeUnixNano": str(start * 10**9), "timeUnixNano": str(ts * 10**9),
                "count": str(count), "sum": total, "bucketCounts": [str(c) for c in counts],
                "explicitBounds": LES})
    return json.dumps({"resourceMetrics": [{
        "resource": {"attributes": [attr("service.name", "geocodio-api"),
                                    attr("deployment.environment.name", "production")]},
        "scopeMetrics": [{"scope": {"name": "cbox/laravel-telemetry"}, "metrics": [{
            "name": "http.server.request.duration", "unit": "s",
            "histogram": {"aggregationTemporality": 2, "dataPoints": points}}]}]}]}).encode()


def push(url: str, bodies: list[bytes], pause_every: int = 0) -> float:
    started = time.perf_counter()
    for i, payload in enumerate(bodies, 1):
        request = urllib.request.Request(url, data=payload, method="POST",
                                         headers={"Content-Type": "application/json"})
        urllib.request.urlopen(request, timeout=300).read()
        if pause_every and i % pause_every == 0:
            time.sleep(6)
    return time.perf_counter() - started


def queries(days: float, now: int) -> list:
    w = int(days * 86400)
    step = max(60, w // 250)
    chart = {"start": now - w, "end": now, "step": step}
    sliced = {"start": now - w + 3600, "end": now, "step": 3600}
    return [
        ("p95 window, instant", "query", {"query": f"histogram_quantile(0.95, sum by (le) (rate({B}[{w}s])))", "time": now}),
        ("p95 window, sliced", "query_range", {"query": f"sum by (le) (increase({B}[3600s]))", **sliced}),
        ("p95/route window, instant", "query", {"query": f"histogram_quantile(0.95, sum by ({BY}, le) (rate({B}[{w}s])))", "time": now}),
        ("p95/route window, sliced", "query_range", {"query": f"sum by ({BY}, le) (increase({B}[3600s]))", **sliced}),
        ("count/route/status, instant", "query", {"query": f"sum by ({BY}, http_response_status_code) (increase({C}[{w}s]))", "time": now}),
        ("sum/route, instant", "query", {"query": f"sum by ({BY}) (increase({S}[{w}s]))", "time": now}),
        ("requests total, instant", "query", {"query": f"sum(increase({C}[{w}s]))", "time": now}),
        ("p95 chart, range", "query_range", {"query": f"histogram_quantile(0.95, sum by (le) (rate({B}[15m])))", **chart}),
        ("avg chart sum, range", "query_range", {"query": f"sum(rate({S}[15m]))", **chart}),
        ("requests chart by status, range", "query_range", {"query": f"sum by (http_response_status_code) (rate({C}[15m])) * 60", **chart}),
        ("route sparklines, range", "query_range", {"query": f"sum by ({BY}) (rate({C}[15m])) * 60", **chart}),
    ]


def ask(url: str) -> tuple[float, dict]:
    started = time.perf_counter()
    answer = json.loads(urllib.request.urlopen(url, timeout=300).read())
    if answer.get("status") != "success":
        raise RuntimeError(answer.get("error", "error"))
    return time.perf_counter() - started, answer["data"]


def canon(data: dict) -> dict:
    out = {}
    for r in data["result"]:
        key = json.dumps({k: v for k, v in sorted(r["metric"].items()) if k != "__name__"})
        out[key] = [float(v) for _, v in r["values"]] if "values" in r else [float(r["value"][1])]
    return out


def differs(a: dict, b: dict) -> float:
    if a.keys() != b.keys():
        return math.inf
    worst = 0.0
    for key in a:
        if len(a[key]) != len(b[key]):
            return math.inf
        for x, y in zip(a[key], b[key]):
            if not (math.isnan(x) and math.isnan(y)):
                worst = max(worst, abs(x - y) / max(1e-12, abs(x), abs(y)))
    return worst


def wait_for(url: str, seconds: int = 180) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2).read()
            return
        except Exception:  # noqa: BLE001 — not up yet
            time.sleep(0.5)
    raise SystemExit(f"{url} did not come up")


class Server:
    """One telemetryd on a fresh data directory, its peak resident memory sampled."""

    def __init__(self, binary: str, port: int, sealed: bool):
        self.dir = tempfile.mkdtemp(prefix="telemetryd-bench-")
        env = dict(os.environ)
        if sealed:
            env["TELEMETRYD_STORAGE_SEGMENT_DURATION"] = "5s"
        self.process = subprocess.Popen(
            [binary, "serve", "--data-dir", self.dir, "--listen", f"127.0.0.1:{port}",
             "--log-level", "warn"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.base = f"http://127.0.0.1:{port}"
        self.peak_kib = 0
        threading.Thread(target=self.sample, daemon=True).start()

    def sample(self) -> None:
        while self.process.poll() is None:
            rss = subprocess.run(["ps", "-o", "rss=", "-p", str(self.process.pid)],
                                 capture_output=True, text=True).stdout.strip()
            if rss:
                self.peak_kib = max(self.peak_kib, int(rss))
            time.sleep(0.2)

    def stop(self) -> None:
        self.process.terminate()
        self.process.wait(timeout=60)
        shutil.rmtree(self.dir, ignore_errors=True)


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, stdout=subprocess.DEVNULL)


def start_upstream(workdir: str, days: float) -> None:
    window = f"{int(days * 24 + 2)}h"
    with open(os.path.join(workdir, "prom.yml"), "w") as f:
        f.write("global: {scrape_interval: 1h}\n"
                f"storage: {{tsdb: {{out_of_order_time_window: {window}}}}}\n"
                "otlp: {promote_resource_attributes: [service.name, deployment.environment.name]}\n")
    with open(os.path.join(workdir, "mimir.yaml"), "w") as f:
        f.write("""multitenancy_enabled: false
server: { http_listen_port: 9009, log_level: warn }
ruler_storage: { backend: filesystem, filesystem: { dir: /data/rules } }
alertmanager_storage: { backend: filesystem, filesystem: { dir: /data/alertmanager } }
blocks_storage:
  backend: filesystem
  filesystem: { dir: /data/blocks }
  tsdb: { dir: /data/tsdb }
  bucket_store: { sync_dir: /data/tsdb-sync }
ingester: { ring: { replication_factor: 1, kvstore: { store: inmemory } } }
distributor: { ring: { kvstore: { store: inmemory } } }
compactor: { data_dir: /data/compactor }
store_gateway: { sharding_ring: { replication_factor: 1 } }
limits:
  out_of_order_time_window: %s
  max_global_series_per_user: 0
  ingestion_rate: 10000000
  ingestion_burst_size: 20000000
  query_ingesters_within: 0
  otel_metric_suffixes_enabled: true
  promote_otel_resource_attributes: service.name,deployment.environment.name
""" % window)
    data = os.path.join(workdir, "mimir-data")
    os.makedirs(data)
    os.chmod(data, 0o777)
    docker("run", "-d", "--name", "compare-prometheus", "-p", "29090:9090", "-v",
           f"{workdir}/prom.yml:/etc/prometheus/prometheus.yml", PROMETHEUS,
           "--config.file=/etc/prometheus/prometheus.yml", "--web.enable-otlp-receiver")
    docker("run", "-d", "--name", "compare-mimir", "-p", "29009:9009", "-v",
           f"{workdir}/mimir.yaml:/etc/mimir.yaml", "-v", f"{data}:/data", MIMIR,
           "-config.file=/etc/mimir.yaml", "-target=all")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("binary")
    parser.add_argument("--baseline", help="another telemetryd binary, benchmarked beside it")
    parser.add_argument("--days", type=float, default=1)
    parser.add_argument("--sealed", action="store_true")
    parser.add_argument("--runs", type=int, default=7)
    args = parser.parse_args()

    workdir = tempfile.mkdtemp(prefix="compare-backends-")
    servers = {}
    try:
        start_upstream(workdir, args.days)
        if args.baseline:
            servers["telemetryd (baseline)"] = Server(args.baseline, 24318, args.sealed)
        servers["telemetryd"] = Server(args.binary, 24319, args.sealed)
        for server in servers.values():
            wait_for(f"{server.base}/api/v1/status/buildinfo")
        wait_for("http://localhost:29090/-/ready")
        wait_for("http://localhost:29009/ready")

        now = int(time.time()) - 120
        bodies = [body(*b) for b in batches(args.days, now)]
        print(f"{args.days:g} day(s), {len(bodies)} requests; ingest time:")
        for name, url in [("Prometheus", "http://localhost:29090/api/v1/otlp/v1/metrics"),
                          ("Mimir", "http://localhost:29009/otlp/v1/metrics")]:
            print(f"  {name}: {push(url, bodies):.2f} s")
        for name, server in servers.items():
            took = push(f"{server.base}/v1/metrics", bodies, 2 if args.sealed else 0)
            print(f"  {name}: {took:.2f} s" + (" (with seal pauses)" if args.sealed else ""))
        time.sleep(8 if args.sealed else 3)

        backends = {"Prometheus": "http://localhost:29090/api/v1",
                    "Mimir": "http://localhost:29009/prometheus/api/v1"}
        backends.update({name: f"{server.base}/api/v1" for name, server in servers.items()})
        at = int(time.time()) - 60
        failed = False
        print(f"\nmedian of {args.runs}, answers compared with Prometheus within {TOL:g}\n")
        print("| Query | " + " | ".join(backends) + " | vs Prometheus |")
        print("|---|" + "---|" * (len(backends) + 1))
        for label, endpoint, params in queries(args.days, at):
            cells, answers = [], {}
            for name, base in backends.items():
                url = f"{base}/{endpoint}?" + urllib.parse.urlencode(params)
                try:
                    ask(url)
                    times = []
                    for _ in range(args.runs):
                        took, data = ask(url)
                        times.append(took)
                    answers[name] = canon(data)
                    cells.append(f"{statistics.median(times) * 1000:.1f} ms")
                except Exception as error:  # noqa: BLE001 — reported in the table
                    cells.append(f"error: {str(error)[:40]}")
            verdicts = []
            for name in servers:
                worst = differs(answers.get(name, {}), answers.get("Prometheus", {}))
                failed |= worst > TOL
                verdicts.append("same" if worst <= TOL else f"**{worst:.2e} off**")
            print(f"| {label} | " + " | ".join(cells) + f" | {' / '.join(verdicts)} |")
        print()
        for name, server in servers.items():
            print(f"peak resident memory, {name}: {server.peak_kib // 1024} MiB")
        return 1 if failed else 0
    finally:
        for server in servers.values():
            server.stop()
        for name in ("compare-prometheus", "compare-mimir"):
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
