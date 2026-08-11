#!/usr/bin/env python3
"""End-to-end soak against a built telemetryd binary.

Everything else in the suite tests telemetryd from the inside. This starts the real
binary, pushes all three signals at it over HTTP, queries them back through the
Loki/Tempo/Prometheus APIs, restarts the process, and checks the data is still there.

It exists because two defects reached a tagged release that every unit test passed:
a `.deb` whose systemd unit pointed at the build runner's own filesystem, and a
TraceQL endpoint that rejected the standard `.attribute` syntax. Neither is visible
from inside the crate — you have to run the thing.

    python3 scripts/soak.py target/debug/telemetryd

Exits non-zero on the first failed expectation, and always stops the server it began.
"""

from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import pathlib
import signal
import socket
import threading
import shutil
import ssl
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

PORT = int(os.environ.get("SOAK_PORT", "14319"))
BASE = f"http://127.0.0.1:{PORT}"
NOW = int(time.time() * 1_000_000_000)
SECOND = 1_000_000_000
TRACE_ID = "5b8efff798038103d269b633813fc60c"
LOG_LINES = 500
METRIC_POINTS = 60
METRIC_STEP_PER_SECOND = 10.0

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  — {detail}" if detail else ""))
    if not ok:
        failures.append(name)


def request(path: str, payload: object | None = None) -> tuple[int, object]:
    body = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(BASE + path, data=body)
    if body:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            raw = response.read().decode()
            try:
                return response.status, json.loads(raw)
            except json.JSONDecodeError:
                return response.status, raw
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode()


def query(path: str, **params: object) -> tuple[int, object]:
    return request(f"{path}?{urllib.parse.urlencode(params)}")


def log_lines(body: object) -> int:
    if not isinstance(body, dict):
        return 0
    return sum(len(s.get("values", [])) for s in body.get("data", {}).get("result", []))


def span_count(body: object) -> int:
    if not isinstance(body, dict):
        return 0
    return sum(
        len(scope.get("spans", []))
        for batch in body.get("batches", [])
        for scope in batch.get("scopeSpans", [])
    )


def series(body: object) -> list:
    return body.get("data", {}).get("result", []) if isinstance(body, dict) else []


def start(binary: str, data_dir: str, env: dict | None = None) -> subprocess.Popen:
    proc = subprocess.Popen(
        [binary, "serve", "--listen", f"127.0.0.1:{PORT}", "--data-dir", data_dir],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={**os.environ, **(env or {})},
    )
    for _ in range(240):
        if proc.poll() is not None:
            sys.exit(f"server exited during startup with code {proc.returncode}")
        try:
            if request("/healthz")[0] == 200:
                return proc
        except OSError:
            pass
        time.sleep(0.25)
    proc.kill()
    sys.exit("server never became healthy")


def stop(proc: subprocess.Popen) -> int:
    proc.send_signal(signal.SIGTERM)
    try:
        return proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        proc.kill()
        return -1


# --------------------------------------------------------------------- payloads

RESOURCE = {"attributes": [{"key": "service.name", "value": {"stringValue": "checkout"}}]}

LOGS = {
    "resourceLogs": [
        {
            "resource": RESOURCE,
            "scopeLogs": [
                {
                    "logRecords": [
                        {
                            "timeUnixNano": str(NOW + i * 1_000_000),
                            "severityText": "ERROR" if i % 5 == 0 else "INFO",
                            "body": {"stringValue": f"payment attempt {i} for order {1000 + i}"},
                            # A dotted key: ingest must keep the producer's spelling.
                            "attributes": [
                                {"key": "exception.type", "value": {"stringValue": "TimeoutError"}}
                            ],
                        }
                        for i in range(LOG_LINES)
                    ]
                }
            ],
        }
    ]
}

TRACES = {
    "resourceSpans": [
        {
            "resource": RESOURCE,
            "scopeSpans": [
                {
                    "spans": [
                        {
                            "traceId": TRACE_ID,
                            "spanId": "eee19b7ec3c1b174",
                            "name": "POST /checkout",
                            "kind": 2,
                            "startTimeUnixNano": str(NOW),
                            "endTimeUnixNano": str(NOW + 45_000_000),
                            "attributes": [
                                {"key": "http.method", "value": {"stringValue": "POST"}},
                                {"key": "http.status_code", "value": {"intValue": "500"}},
                            ],
                        },
                        {
                            "traceId": TRACE_ID,
                            "spanId": "aaa19b7ec3c1b175",
                            "parentSpanId": "eee19b7ec3c1b174",
                            "name": "charge card",
                            "kind": 3,
                            "startTimeUnixNano": str(NOW + 5_000_000),
                            "endTimeUnixNano": str(NOW + 40_000_000),
                            "attributes": [],
                        },
                    ]
                }
            ],
        }
    ]
}

METRICS = {
    "resourceMetrics": [
        {
            "resource": RESOURCE,
            "scopeMetrics": [
                {
                    "metrics": [
                        {
                            "name": "http_requests_total",
                            "sum": {
                                "aggregationTemporality": 2,
                                "isMonotonic": True,
                                "dataPoints": [
                                    {
                                        "timeUnixNano": str(NOW + i * SECOND),
                                        "asDouble": float(i * METRIC_STEP_PER_SECOND),
                                        "attributes": [
                                            {"key": "route", "value": {"stringValue": "/checkout"}}
                                        ],
                                    }
                                    for i in range(METRIC_POINTS)
                                ],
                            },
                        }
                    ]
                }
            ],
        }
    ]
}

# -------------------------------------------------------------------------- run


def main() -> int:
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <path-to-telemetryd>")
    binary = os.path.abspath(sys.argv[1])
    if not os.access(binary, os.X_OK):
        sys.exit(f"{binary} is not executable")

    version = subprocess.run([binary, "version"], capture_output=True, text=True)
    print(f"=== {version.stdout.splitlines()[0] if version.stdout else binary} ===")

    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-")
    proc = start(binary, data_dir)
    try:
        print("\n=== ingest ===")
        for name, path, payload in (
            ("logs", "/v1/logs", LOGS),
            ("traces", "/v1/traces", TRACES),
            ("metrics", "/v1/metrics", METRICS),
        ):
            status, body = request(path, payload)
            rejected = (
                body.get("partialSuccess", {}).get("rejectedDataPoints", 0)
                if isinstance(body, dict)
                else "?"
            )
            check(f"OTLP {name} accepted", status == 200 and not rejected, f"HTTP {status}")

        # Records are queryable from the in-memory buffer; no seal needed.
        window = {"start": NOW - SECOND, "end": NOW + 3600 * SECOND}

        print("\n=== logs ===")
        status, body = query(
            "/loki/api/v1/query_range", query='{service_name="checkout"}', limit=10_000, **window
        )
        check("LogQL selector returns every line", status == 200 and log_lines(body) == LOG_LINES,
              f"HTTP {status}, {log_lines(body)}/{LOG_LINES}")

        status, body = query(
            "/loki/api/v1/query_range",
            query='{service_name="checkout"} |= "order 1042"',
            limit=100,
            **window,
        )
        check("LogQL line filter narrows to one", status == 200 and log_lines(body) == 1,
              f"HTTP {status}, {log_lines(body)} lines")

        # Severity is a stream label, lower-cased on the way in as Loki spells it, so
        # this is `level="error"` and not the OTLP `severityText` of "ERROR".
        for expression in (
            '{service_name="checkout"} | level="error"',
            '{service_name="checkout", level="error"}',
        ):
            status, body = query(
                "/loki/api/v1/query_range", query=expression, limit=10_000, **window
            )
            check(f"LogQL {expression}",
                  status == 200 and log_lines(body) == LOG_LINES // 5,
                  f"HTTP {status}, {log_lines(body)}/{LOG_LINES // 5}")

        status, body = query(
            "/loki/api/v1/query_range", query='{service_name="checkout"}', limit=1, **window
        )
        raw = json.dumps(body)
        check("attribute keys keep the producer's spelling",
              "exception.type" in raw and "exception_type" not in raw)

        print("\n=== traces ===")
        status, body = request(f"/api/traces/{TRACE_ID}")
        check("Tempo returns the whole trace", status == 200 and span_count(body) == 2,
              f"HTTP {status}, {span_count(body)} spans")

        # The unscoped `.attribute` form is the one TraceQL specifies, and the one a
        # released build once rejected outright.
        for expression, expected in (
            ('{ .service.name = "checkout" }', 1),
            ('{ .http.method = "POST" }', 1),
            ('{ resource.service.name = "checkout" }', 1),
            ('{ span.http.status_code = 500 }', 1),
            ('{ name = "POST /checkout" }', 1),
            ('{ .name = "POST /checkout" }', 0),
            ('{ duration > 40ms }', 1),
            ('{ status = error }', 0),
        ):
            status, body = query("/api/search", q=expression)
            found = len(body.get("traces", [])) if isinstance(body, dict) else -1
            check(f"TraceQL {expression}", status == 200 and found == expected,
                  f"HTTP {status}, {found} traces (expected {expected})")

        print("\n=== metrics ===")
        at = (NOW + (METRIC_POINTS - 1) * SECOND) // SECOND
        status, body = query("/api/v1/query", query="http_requests_total", time=at)
        check("PromQL instant query", status == 200 and len(series(body)) == 1,
              f"HTTP {status}, {len(series(body))} series")

        status, body = query("/api/v1/query", query="rate(http_requests_total[5m])", time=at)
        rate = float(series(body)[0]["value"][1]) if series(body) else -1.0
        check("PromQL rate() is the true rate",
              status == 200 and abs(rate - METRIC_STEP_PER_SECOND) < 0.5,
              f"{rate}/s (expected ~{METRIC_STEP_PER_SECOND}/s)")

        status, body = request("/api/v1/labels")
        names = body.get("data", []) if isinstance(body, dict) else []
        check("Prometheus label names", status == 200 and "__name__" in names, f"HTTP {status}")

        print("\n=== restart durability ===")
        code = stop(proc)
        check("graceful shutdown", code in (0, -signal.SIGTERM), f"exit {code}")
        proc = start(binary, data_dir)

        status, body = query(
            "/loki/api/v1/query_range", query='{service_name="checkout"}', limit=10_000, **window
        )
        check("every log line survived the restart", log_lines(body) == LOG_LINES,
              f"{log_lines(body)}/{LOG_LINES}")

        status, body = request(f"/api/traces/{TRACE_ID}")
        check("the trace survived the restart", span_count(body) == 2, f"{span_count(body)} spans")

        status, body = query("/api/v1/query", query="http_requests_total", time=at)
        check("metrics survived the restart", len(series(body)) == 1, f"{len(series(body))} series")

        status, body = request("/status")
        check("/status answers after restart", status == 200 and isinstance(body, dict))
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)

    check_disk_budget(binary)
    check_reload(binary)
    check_damaged_segment(binary)
    check_oidc(binary)
    check_oidc_survives_a_missing_provider(binary)
    check_outbound_tls(binary)
    check_oidc_over_tls(binary)
    check_relay_over_tls(binary)
    check_serving_tls(binary)
    check_self_signed(binary)
    check_memory_ceiling(binary)
    check_pipes(binary)
    check_relay(binary)
    check_relay_fair_share(binary)
    check_relay_oversized_segments(binary)
    check_export_import(binary)

    print("\n" + "=" * 52)
    if failures:
        print(f"SOAK FAILED: {', '.join(failures)}")
        return 1
    print("SOAK PASSED")
    return 0


def check_disk_budget(binary: str) -> None:
    """The disk budget has to hold while writes continue, not once they stop.

    It used to be enforced on a fixed 60-second tick, so a fast writer passed the
    ceiling by 65% and the peak grew run over run. Retention now runs when a segment
    seals — the only thing that grows segment bytes — so the overshoot is bounded by
    one segment rather than by a minute of traffic.
    """
    print("\n=== disk budget under sustained writes ===")
    budget_mib = 4
    data_dir = tempfile.mkdtemp(prefix="telemetryd-budget-")
    proc = start(binary, data_dir, {
        "TELEMETRYD_STORAGE_DISK_BUDGET": f"{budget_mib}MiB",
        "TELEMETRYD_STORAGE_MAX_SEGMENT_BYTES": "1MiB",
        # Long enough that only the budget can bind. With a short window the records
        # expire by age instead and the budget path is never exercised at all.
        "TELEMETRYD_RETENTION_LOGS": "30d",
    })
    try:
        now = int(time.time() * 1_000_000_000)
        peak = 0.0
        errors = 0
        deleted = 0.0
        # Write until the reaper has actually deleted for the budget, rather than for a
        # fixed number of rounds. How much a batch costs on disk depends on how well it
        # compresses, and a fixed count that crossed the ceiling here sat under it on a
        # CI runner — so the loop ends on the condition being tested, not on a guess.
        for round_index in range(120):
            for batch in range(5):
                status, _ = request(
                    "/v1/logs",
                    {"resourceLogs": [{
                        "resource": RESOURCE,
                        "scopeLogs": [{"logRecords": [
                            {"timeUnixNano": str(now + (round_index * 5 + batch) * 5000 * 1000 + i * 1000),
                             "severityText": "INFO",
                             # High-entropy tail, or zstd collapses a repetitive body to
                             # almost nothing and the budget is never reached.
                             "body": {"stringValue":
                                      f"payment {i} order {1000 + i} "
                                      f"{(i * 2654435761 + round_index * 40503) % 10**12:012x}"},
                             "attributes": []}
                            for i in range(5000)]}]}]},
                )
                if status != 200:
                    errors += 1
            peak = max(peak, directory_bytes(data_dir) / 1024 / 1024)
            deleted = budget_deletions()
            if deleted > 0:
                break

        check("no ingest errors while enforcing the budget", errors == 0,
              f"{errors} failed posts")

        # That the budget path *ran* is checked by its counter, not by catching usage
        # above the ceiling. Whether the peak is ever seen above the budget depends on
        # whether the writer outruns the reaper, which varies with the machine.
        # Retention is 30 days against timestamps from now, so nothing can expire by
        # age and every deletion here is the budget's doing.
        check("the reaper deleted segments to hold the budget", deleted > 0,
              f"deleted_by_budget={deleted:.0f}, peak {peak:.1f} MB of a {budget_mib} MB budget")
        # One segment of slack over the ceiling, not one reaper interval.
        check("disk stays near the budget", peak < budget_mib * 1.5,
              f"peak {peak:.1f} MB against a {budget_mib} MB budget ({peak / budget_mib:.2f}x)")

        status, body = query(
            "/loki/api/v1/query_range", query='{service_name="checkout"}', limit=10,
            start=0, end=9_223_372_036_854_775_807,
        )
        check("queries still answer after reaping", status == 200 and log_lines(body) > 0)
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def budget_deletions() -> float:
    """Segments the reaper has deleted to stay inside the disk budget."""
    _, metrics = request("/metrics")
    for line in str(metrics).splitlines():
        if line.startswith('telemetryd_retention_deleted_total{reason="disk_budget"}'):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


def check_reload(binary: str) -> None:
    """SIGHUP must change the running configuration, and refuse what it cannot."""
    if os.name != "posix":
        return
    print("\n=== configuration reload ===")
    data_dir = tempfile.mkdtemp(prefix="telemetryd-reload-")
    config_path = os.path.join(data_dir, "telemetryd.toml")

    def write_config(budget: str, extra: str = "") -> None:
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write(
                f'[server]\nlisten = "127.0.0.1:{PORT}"\n'
                f'[storage]\ndata_dir = "{data_dir}/data"\ndisk_budget = "{budget}"\n{extra}'
            )

    write_config("10GiB")
    proc = subprocess.Popen(
        [binary, "serve", "--config", config_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)

        _, before = request("/status")
        write_config("2GiB", 'max_segment_bytes = "64MiB"\n')
        proc.send_signal(signal.SIGHUP)

        # The reload is asynchronous; poll rather than sleeping a guessed interval.
        applied = False
        for _ in range(80):
            time.sleep(0.25)
            _, after = request("/status")
            if isinstance(after, dict) and after["storage"]["disk_budget_bytes"] == 2 * 1024**3:
                applied = True
                break
        check("SIGHUP applies the new disk budget", applied,
              f"{before['storage']['disk_budget_bytes']} -> {after['storage']['disk_budget_bytes']}"
              if isinstance(after, dict) else "no status")

        # max_segment_bytes cannot change under a running store; the server must keep
        # serving rather than exiting or reopening anything.
        check("the server survives an unreloadable change", request("/healthz")[0] == 200)

        # The reaper read the new window from the store while `/status` kept reporting
        # the configuration captured at startup, so a reload left the two disagreeing on
        # the single field an operator checks when asking where their data went. Asserted
        # against the *reported* value rather than the log line, because the log said the
        # change had applied — which was true, and was not the bug.
        write_config("2GiB", 'max_segment_bytes = "64MiB"\n[retention]\nlogs = "3d"\n')
        proc.send_signal(signal.SIGHUP)
        reported = None
        deadline = time.time() + 15
        while time.time() < deadline:
            _, body = request("/status")
            if isinstance(body, dict):
                reported = body.get("retention", {}).get("logs")
                if reported == "3days":
                    break
            time.sleep(0.5)
        check("/status reports the retention actually in force after a reload",
              reported == "3days", f"reported {reported!r}, expected '3days'")

        # A broken file must leave the running configuration alone.
        with open(config_path, "w", encoding="utf-8") as handle:
            handle.write("this is not valid toml {{{")
        proc.send_signal(signal.SIGHUP)
        time.sleep(1)
        status, after = request("/status")
        check("a malformed file does not disturb the running server",
              status == 200 and after["storage"]["disk_budget_bytes"] == 2 * 1024**3)
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def check_damaged_segment(binary: str) -> None:
    """One bad file must cost that file, not the store.

    A truncated Parquet segment used to fail every query over its time range, so a
    single bad sector denied access to every healthy segment beside it. The store-level
    tests cover the logic; this covers the server, because "the process still starts and
    still answers" is the property an operator actually cares about.
    """
    print("\n=== a damaged segment ===")
    data_dir = tempfile.mkdtemp(prefix="telemetryd-damage-")
    proc = start(binary, data_dir, {"TELEMETRYD_STORAGE_MAX_SEGMENT_BYTES": "1MiB"})
    now = int(time.time() * 1_000_000_000)
    try:
        for batch in range(12):
            request("/v1/logs", {"resourceLogs": [{
                "resource": RESOURCE,
                "scopeLogs": [{"logRecords": [
                    {"timeUnixNano": str(now + (batch * 4000 + i) * 1000),
                     "severityText": "INFO",
                     "body": {"stringValue": f"payment attempt {i} for order {1000 + i}"},
                     "attributes": []}
                    for i in range(4000)]}]}]})
        window = {"start": 0, "end": 9_223_372_036_854_775_807}
        status, body = query("/loki/api/v1/query_range", query='{service_name="checkout"}',
                             limit=5000, **window)
        before = log_lines(body)
    finally:
        stop(proc)

    segments = sorted(
        root
        for root, _dirs, files in os.walk(os.path.join(data_dir, "segments"))
        if "data.parquet" in files and "manifest.json" in files
    )
    if len(segments) < 3:
        check("the store produced enough segments to damage one", False,
              f"only {len(segments)} segments")
        shutil.rmtree(data_dir, ignore_errors=True)
        return

    victim = segments[len(segments) // 2]
    with open(os.path.join(victim, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    with open(os.path.join(victim, "data.parquet"), "r+b") as handle:
        handle.truncate(64)

    # The damaged segment's own window. Querying the whole store just fills the limit
    # from the newest segments and never opens the damaged one at all — which is how an
    # earlier version of this check passed while testing nothing.
    damaged_window = {
        "start": manifest["min_time_nanos"],
        "end": manifest["max_time_nanos"],
    }

    proc = start(binary, data_dir)
    try:
        check("the server starts with a damaged segment", request("/healthz")[0] == 200)

        # Over the damaged segment itself: this is the query that has to open it.
        status, body = query("/loki/api/v1/query_range", query='{service_name="checkout"}',
                             limit=5000, **damaged_window)
        check("a query over the damaged window still answers", status == 200,
              f"HTTP {status}, {log_lines(body)} lines")

        # And the healthy segments are untouched.
        status, body = query("/loki/api/v1/query_range", query='{service_name="checkout"}',
                             limit=5000, **window)
        check("the rest of the store is unaffected", status == 200 and log_lines(body) == before,
              f"{log_lines(body)} of {before} lines")

        status, health = request("/status")
        unreadable = health["storage"]["logs"]["segments_unreadable"] if isinstance(health, dict) else 0
        check("the loss is reported rather than silent", unreadable > 0,
              f"segments_unreadable={unreadable}")
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def directory_bytes(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


# --- Cbox ID, stood in for ------------------------------------------------------
#
# The unit tests sign real tokens and verify them through the real path, but they do
# it inside the crate. What they cannot see is the part that only exists once the
# binary is running: fetching a key set over the network at startup, surviving a
# provider that is not there yet, and mapping a scope onto a route. That is exactly
# the seam the `.deb` unit-file defect lived in, so it gets covered here too.
#
# The signing below is written out rather than imported. soak.py runs anywhere a
# stock python3 does — the same promise the binary makes — and one phase is not worth
# giving that up for.

RSA_KEY_DER = "crates/server/tests/data/oidc-test-key.der"
# EMSA-PKCS1-v1_5 DigestInfo prefix for SHA-256 (RFC 8017 §9.2, notes on DigestInfo).
SHA256_DIGEST_INFO = bytes.fromhex("3031300d060960864801650304020105000420")


def der_integers(blob: bytes) -> list[int]:
    """Every INTEGER in a PKCS#1 RSAPrivateKey, in order: version, n, e, d, p, q, ..."""
    if blob[0] != 0x30:
        raise ValueError("not a DER SEQUENCE")
    at = 1
    length = blob[at]
    at += 1
    if length & 0x80:
        at += length & 0x7F
    out = []
    while at < len(blob):
        tag, at = blob[at], at + 1
        length, at = blob[at], at + 1
        if length & 0x80:
            count = length & 0x7F
            length = int.from_bytes(blob[at:at + count], "big")
            at += count
        if tag == 0x02:
            out.append(int.from_bytes(blob[at:at + length], "big"))
        at += length
    return out


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


# ---------------------------------------------------------------------------
# Real TLS, because loopback HTTP is what hid the last one
# ---------------------------------------------------------------------------

_TLS_MATERIAL: dict | None = None


def tls_material() -> dict:
    """A private CA and a `localhost` server certificate signed by it.

    Generated once per run and reused. This exists because every OIDC and relay test
    in this file used to speak plain HTTP to loopback — which is exactly why nobody
    noticed that the binary had no TLS stack at all, while the configuration
    *required* https for both `auth.oidc.issuer` and `relay.upstream`. A green suite
    against a shape no production deployment has is not evidence.
    """
    global _TLS_MATERIAL  # noqa: PLW0603
    if _TLS_MATERIAL is not None:
        return _TLS_MATERIAL

    directory = tempfile.mkdtemp(prefix="telemetryd-soak-tls-")
    ca_key = os.path.join(directory, "ca.key")
    ca_pem = os.path.join(directory, "ca.pem")
    key = os.path.join(directory, "server.key")
    csr = os.path.join(directory, "server.csr")
    cert = os.path.join(directory, "server.pem")
    ext = os.path.join(directory, "server.ext")

    def run(*args: str) -> None:
        subprocess.run(args, check=True, capture_output=True)

    # The CA extensions are not decoration. Without basicConstraints and keyUsage a
    # modern verifier refuses the chain — rustls happened to accept it, Python did not,
    # and the difference is exactly the sort of thing that makes a test pass against one
    # client and mislead about every other.
    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-keyout", ca_key, "-out", ca_pem, "-subj", "/CN=telemetryd soak CA",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    run("openssl", "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", key, "-out", csr, "-subj", "/CN=localhost")
    # A SAN is not optional: rustls rejects a certificate matched only on common name.
    with open(ext, "w", encoding="utf-8") as handle:
        handle.write("subjectAltName=DNS:localhost,IP:127.0.0.1\n")
        handle.write("basicConstraints=critical,CA:FALSE\n")
        handle.write("keyUsage=critical,digitalSignature,keyEncipherment\n")
        handle.write("extendedKeyUsage=serverAuth\n")
    run("openssl", "x509", "-req", "-in", csr, "-CA", ca_pem, "-CAkey", ca_key,
        "-CAcreateserial", "-out", cert, "-days", "1", "-extfile", ext)

    _TLS_MATERIAL = {"ca": ca_pem, "cert": cert, "key": key}
    return _TLS_MATERIAL


def wrap_tls(server: http.server.HTTPServer) -> None:
    """Put a real TLS handshake in front of a stand-in server."""
    material = tls_material()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(material["cert"], material["key"])
    server.socket = context.wrap_socket(server.socket, server_side=True)


class Issuer:
    """A stand-in Cbox ID: a key pair, a JWKS endpoint, and tokens signed with it."""

    def __init__(self, tls: bool = False) -> None:
        _, self.n, self.e, self.d = der_integers(pathlib.Path(RSA_KEY_DER).read_bytes())[:4]
        self.size = (self.n.bit_length() + 7) // 8
        self.kid = "test-key-1"
        self.jwks = json.dumps({"keys": [{
            "kty": "RSA", "kid": self.kid, "use": "sig", "alg": "RS256",
            "n": b64url(self.n.to_bytes(self.size, "big")),
            "e": b64url(self.e.to_bytes((self.e.bit_length() + 7) // 8, "big")),
        }]}).encode()
        self.server = http.server.HTTPServer(("127.0.0.1", 0), self._handler())
        port = self.server.server_address[1]
        if tls:
            wrap_tls(self.server)
            # `localhost` rather than the address: the certificate carries it as a SAN,
            # and a hostname is what a real issuer or upstream URL has.
            self.url = f"https://localhost:{port}"
        else:
            self.url = f"http://127.0.0.1:{port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def _handler(self):
        jwks = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path != "/.well-known/jwks.json":
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(jwks.jwks)))
                self.end_headers()
                self.wfile.write(jwks.jwks)

            def log_message(self, *_args) -> None:
                pass
        return Handler

    def sign(self, message: bytes) -> bytes:
        digest = SHA256_DIGEST_INFO + hashlib.sha256(message).digest()
        # PKCS#1 v1.5: 0x00 0x01 <0xFF padding> 0x00 <DigestInfo>
        padded = b"\x00\x01" + b"\xff" * (self.size - len(digest) - 3) + b"\x00" + digest
        signed = pow(int.from_bytes(padded, "big"), self.d, self.n)
        return signed.to_bytes(self.size, "big")

    def token(self, scope: str, lifetime: int = 300, audience: str | None = None,
              typ: str = "at+jwt", cnf: bool = False) -> str:
        # `at+jwt`, per RFC 9068 and what Cbox ID's JwtTokenIssuer actually signs. The
        # obvious `JWT` is an *id* token's media type, and a stand-in issuer that mints
        # it is not standing in for the real one.
        header = {"alg": "RS256", "typ": typ, "kid": self.kid}
        now = int(time.time())
        claims = {
            "iss": self.url, "aud": audience or self.url, "sub": "soak-user",
            "iat": now, "exp": now + lifetime, "scope": scope,
        }
        if cnf:
            claims["cnf"] = {"jkt": "0ZcOCORZNYy-DWpqq30jZyJGHTN0d2HglBV3uiguA4I"}
        body = b64url(json.dumps(header).encode()) + "." + b64url(json.dumps(claims).encode())
        return body + "." + b64url(self.sign(body.encode()))


def oidc_status(token: str) -> dict:
    """`/status`'s OIDC block. Needs the admin token — `/status` is an admin surface."""
    req = urllib.request.Request(BASE + "/status")
    req.add_header("authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            body = json.loads(response.read())
    except (urllib.error.HTTPError, ValueError):
        return {}
    block = body.get("auth", {}).get("oidc")
    return block if isinstance(block, dict) else {}


def with_token(path: str, token: str | None, payload: object | None = None) -> int:
    """Status code only — this phase is about who gets in, not what comes back."""
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(BASE + path, data=data, method="POST" if data else "GET")
    if data:
        req.add_header("content-type", "application/json")
    if token:
        req.add_header("authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def check_oidc(binary: str) -> None:
    """A scope has to open exactly the surface it names, from outside the process."""
    print("\n=== Cbox ID tokens ===")
    issuer = Issuer()
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-oidc-")
    config = os.path.join(data_dir, "telemetryd.toml")
    with open(config, "w", encoding="utf-8") as handle:
        handle.write(
            "[auth]\nadmin_token = [\"static-admin\"]\n"
            f"[auth.oidc]\nissuer = \"{issuer.url}\"\n"
        )

    proc = subprocess.Popen(
        [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
         "--data-dir", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env={**os.environ},
    )
    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)

        # Zero keys is the failure that looks like working software: the server is up,
        # every token is refused, and nothing else in the response says why.
        oidc = oidc_status("static-admin")
        # The export endpoint returns telemetry, so it sits behind the query token.
        # A new read surface that forgot its guard is the expensive kind of mistake.
        check("the export endpoint refuses an unauthenticated read",
              with_token("/api/v1/export?signal=logs&start=0&end=1", None) == 401)

        check("the key set was fetched at startup", oidc.get("keys") == 1,
              f"keys={oidc.get('keys', 'absent')}")

        entry = {"resourceLogs": [{"resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "soak"}}]},
            "scopeLogs": [{"logRecords": [{
                "timeUnixNano": str(NOW), "body": {"stringValue": "oidc"}}]}]}]}

        surfaces = {
            "query": lambda t: with_token("/loki/api/v1/labels", t),
            "status": lambda t: with_token("/status", t),
            "ingest": lambda t: with_token("/v1/logs", t, entry),
        }
        # Not a hierarchy: admin does not imply read. Running a dashboard is not a
        # reason to be able to read anyone's log lines.
        expected = {
            "telemetry:read":   {"query": 200, "status": 401, "ingest": 401},
            "telemetry:admin":  {"query": 401, "status": 200, "ingest": 401},
            "telemetry:write":  {"query": 401, "status": 401, "ingest": 200},
        }
        for scope, wanted in expected.items():
            token = issuer.token(scope)
            got = {name: call(token) for name, call in surfaces.items()}
            check(f"{scope} opens exactly its own surface", got == wanted,
                  " ".join(f"{k}={v}" for k, v in got.items()))

        refused = {
            "an expired token": issuer.token("telemetry:read", lifetime=-3600),
            "a token for another audience": issuer.token("telemetry:read", audience="https://elsewhere"),
            "a near-miss scope": issuer.token("telemetry:readonly"),
            "a token with no signature": issuer.token("telemetry:read").rsplit(".", 1)[0] + ".",
            "not a token at all": "hunter2",
            # An id token is signed by the same key and authorises nothing.
            "an id token in an access token's place": issuer.token("telemetry:read", typ="JWT"),
            # RFC 9449: the issuer bound this to a key the client holds. Accepting it as
            # a plain bearer would hand back the property the binding was bought for.
            "a sender-constrained token": issuer.token("telemetry:read", cnf=True),
        }
        for name, token in refused.items():
            check(f"{name} is refused", with_token("/loki/api/v1/labels", token) == 401)

        check("no token is refused", with_token("/loki/api/v1/labels", None) == 401)
        # Turning this on must not cost an existing static deployment anything.
        check("a static token still works alongside it",
              with_token("/status", "static-admin") == 200)
    finally:
        stop(proc)
        issuer.server.shutdown()
        shutil.rmtree(data_dir, ignore_errors=True)


def check_oidc_survives_a_missing_provider(binary: str) -> None:
    """telemetryd must start when the identity provider does not answer.

    Refusing to start would be the coupling ADR-011 exists to avoid, in its worst
    form: the tool you open when something is broken, refusing to open because
    something is broken.
    """
    print("\n=== Cbox ID unreachable ===")
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-nooidc-")
    config = os.path.join(data_dir, "telemetryd.toml")
    # A port with nothing behind it, chosen by binding and immediately releasing.
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        dead = probe.getsockname()[1]
    with open(config, "w", encoding="utf-8") as handle:
        handle.write(
            "[auth]\nadmin_token = [\"static-admin\"]\n"
            f"[auth.oidc]\nissuer = \"http://127.0.0.1:{dead}\"\n"
        )

    proc = subprocess.Popen(
        [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
         "--data-dir", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ},
    )
    try:
        healthy = False
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    healthy = True
                    break
            except OSError:
                pass
            time.sleep(0.25)
        check("the server starts anyway", healthy)
        check("static tokens are unaffected", with_token("/status", "static-admin") == 200)
        oidc = oidc_status("static-admin")
        check("the empty key set is visible rather than silent", oidc.get("keys") == 0,
              f"keys={oidc.get('keys', 'absent')}")
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


# --- relay mode --------------------------------------------------------------------
#
# The delivery guarantee is the part worth testing, and the interesting cases are the
# unhappy ones: an upstream that is down when the data arrives, and a client that lies
# about who it is. Both are silent failures — the server answers 200 either way — so
# neither shows up without asserting on it.


class Upstream:
    """A stand-in central instance. Can be told to fail, and remembers what arrived."""

    def __init__(self, limit: int | None = None, tls: bool = False) -> None:
        self.received: list[dict] = []
        self.failing = False
        # A body ceiling, as any real receiver has. `server.max_body_bytes` defaults to
        # 16 MiB while a segment may be 256 MiB, so this is the normal case and not an
        # exotic one.
        self.limit = limit
        self.refused_too_large = 0
        self.server = http.server.HTTPServer(("127.0.0.1", 0), self._handler())
        port = self.server.server_address[1]
        if tls:
            wrap_tls(self.server)
            # `localhost` rather than the address: the certificate carries it as a SAN,
            # and a hostname is what a real issuer or upstream URL has.
            self.url = f"https://localhost:{port}"
        else:
            self.url = f"http://127.0.0.1:{port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def _handler(self):
        upstream = self
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("content-length", "0"))
                raw = self.rfile.read(length)
                if upstream.limit is not None and length > upstream.limit:
                    upstream.refused_too_large += 1
                    self.send_response(413)
                    self.send_header("content-length", "0")
                    self.end_headers()
                    return
                if upstream.failing:
                    # 503, not a dropped connection: the cursor must not advance on a
                    # refusal the shipper *did* get an answer to.
                    self.send_response(503)
                    self.send_header("content-length", "0")
                    self.end_headers()
                    return
                try:
                    upstream.received.append(json.loads(raw))
                except ValueError:
                    upstream.received.append({})
                self.send_response(200)
                self.send_header("content-length", "0")
                self.end_headers()

            def log_message(self, *_args) -> None:
                pass
        return Handler

    def lines(self) -> list[str]:
        out = []
        for payload in self.received:
            for resource in payload.get("resourceLogs", []):
                for scope in resource.get("scopeLogs", []):
                    for record in scope.get("logRecords", []):
                        out.append(record.get("body", {}).get("stringValue", ""))
        return out

    def spans(self) -> int:
        return sum(
            len(scope.get("spans", []))
            for payload in self.received
            for resource in payload.get("resourceSpans", [])
            for scope in resource.get("scopeSpans", [])
        )

    def span_statuses(self) -> set[int]:
        return {
            span.get("status", {}).get("code", 0)
            for payload in self.received
            for resource in payload.get("resourceSpans", [])
            for scope in resource.get("scopeSpans", [])
            for span in scope.get("spans", [])
        }

    def sum_temporalities(self) -> set[int]:
        """0 is AGGREGATION_TEMPORALITY_UNSPECIFIED, which the proto forbids."""
        return {
            metric["sum"].get("aggregationTemporality", 0)
            for payload in self.received
            for resource in payload.get("resourceMetrics", [])
            for scope in resource.get("scopeMetrics", [])
            for metric in scope.get("metrics", [])
            if "sum" in metric
        }

    def metric_names(self) -> set[str]:
        return {
            metric.get("name", "")
            for payload in self.received
            for resource in payload.get("resourceMetrics", [])
            for scope in resource.get("scopeMetrics", [])
            for metric in scope.get("metrics", [])
        }

    def apps(self) -> set[str]:
        out = set()
        for payload in self.received:
            for resource in payload.get("resourceLogs", []):
                for attribute in resource.get("resource", {}).get("attributes", []):
                    if attribute.get("key") == "app":
                        out.add(attribute.get("value", {}).get("stringValue", ""))
        return out


def relay_logs(count: int, claimed_app: str, token: str, offset_nanos: int = 0) -> int:
    entry = {"resourceLogs": [{
        "resource": {"attributes": [
            # The client says it is `claimed_app`. It does not get a vote.
            {"key": "app", "value": {"stringValue": claimed_app}},
            {"key": "service.name", "value": {"stringValue": claimed_app}},
        ]},
        "scopeLogs": [{"logRecords": [
            {"timeUnixNano": str(NOW + offset_nanos + i), "body": {"stringValue": f"relay-{i}"}}
            for i in range(count)
        ]}],
    }]}
    return with_token("/v1/logs", token, entry)


def admin_json(path: str) -> dict:
    req = urllib.request.Request(BASE + path)
    req.add_header("authorization", "Bearer static-admin")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read())
    except (urllib.error.HTTPError, ValueError):
        return {}


def admin_metrics() -> str:
    req = urllib.request.Request(BASE + "/metrics")
    req.add_header("authorization", "Bearer static-admin")
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.read().decode()
    except urllib.error.HTTPError:
        return ""


def relay_traces(claimed_app: str, token: str) -> int:
    payload = {"resourceSpans": [{
        "resource": {"attributes": [
            {"key": "app", "value": {"stringValue": claimed_app}},
            {"key": "service.name", "value": {"stringValue": claimed_app}},
        ]},
        "scopeSpans": [{"spans": [{
            "traceId": TRACE_ID,
            "spanId": "eee19b7ec3c1b174",
            "name": "POST /charge",
            "kind": 2,
            "startTimeUnixNano": str(NOW),
            "endTimeUnixNano": str(NOW + 1_000_000),
            "status": {"code": 2, "message": "card declined"},
            "attributes": [{"key": "http.method", "value": {"stringValue": "POST"}}],
        }]}],
    }]}
    return with_token("/v1/traces", token, payload)


def relay_metrics(claimed_app: str, token: str) -> int:
    payload = {"resourceMetrics": [{
        "resource": {"attributes": [
            {"key": "app", "value": {"stringValue": claimed_app}},
        ]},
        "scopeMetrics": [{"metrics": [{
            "name": "charges_total",
            "sum": {
                "isMonotonic": True,
                "aggregationTemporality": 2,
                "dataPoints": [{"timeUnixNano": str(NOW), "asDouble": 7.0}],
            },
        }]}],
    }]}
    return with_token("/v1/metrics", token, payload)


def check_outbound_tls(binary: str) -> None:
    """The binary must actually be able to speak TLS.

    This is the check that was missing. `ureq` was declared with no TLS backend, so
    every https request failed with "TLS required, but transport is unsecured" — while
    the configuration *demanded* https for `auth.oidc.issuer` and `relay.upstream`.
    Cbox ID login and relay mode were both unreachable in any valid production setup,
    and the whole suite stayed green because every test here points at loopback, where
    plain HTTP is deliberately allowed.

    No network needed to catch it: a closed local port distinguishes the two failures
    on its own. With a TLS stack the connection is attempted and refused; without one
    ureq gives up before opening a socket.
    """
    print("\n=== outbound TLS ===")
    result = subprocess.run(
        [binary, "import", "--from", "https://127.0.0.1:1", "--signal", "logs",
         "--since", "5m"],
        capture_output=True, text=True, timeout=120, check=False,
    )
    stderr = result.stderr
    check("an https URL reaches the network layer",
          "TLS required" not in stderr and "transport is unsecured" not in stderr,
          stderr.strip().splitlines()[-1] if stderr.strip() else "no output")
    check("a closed https port is refused, not rejected before dialling",
          "Connection refused" in stderr or "connect" in stderr.lower(),
          stderr.strip().splitlines()[-1] if stderr.strip() else "no output")


def check_oidc_over_tls(binary: str) -> None:
    """The whole path a real deployment uses: https, a real handshake, a real token.

    Every other OIDC test here talks plain HTTP to loopback, which is allowed on
    purpose and is precisely why nobody noticed the binary had no TLS stack while
    `Config::validate` *demanded* https for the issuer. This runs the issuer behind a
    genuine TLS handshake against a private CA, so the fetch, the verification and the
    authorisation all happen the way they do in production.

    The last check is the important one: the same configuration *without* the CA
    bundle must fail. Without it this test would pass just as happily if trust were
    not being applied at all.
    """
    print("\n=== OIDC over real TLS ===")
    issuer = Issuer(tls=True)
    material = tls_material()
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-oidc-tls-")
    config = os.path.join(data_dir, "telemetryd.toml")

    def write_config(ca_file: str | None) -> None:
        with open(config, "w", encoding="utf-8") as handle:
            handle.write("[auth]\nadmin_token = [\"static-admin\"]\n"
                         f"[auth.oidc]\nissuer = \"{issuer.url}\"\n")
            if ca_file:
                handle.write(f"[tls]\nca_file = \"{ca_file}\"\n")

    def serve() -> subprocess.Popen:
        return subprocess.Popen(
            [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
             "--data-dir", data_dir],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ})

    write_config(material["ca"])
    proc = serve()
    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)

        oidc = oidc_status("static-admin")
        check("the key set is fetched over https", oidc.get("keys") == 1,
              f"keys={oidc.get('keys', 'absent')}")

        # End to end, not just the fetch: sign a token with the key that key set
        # publishes and use it on the surface its scope names.
        entry = {"resourceLogs": [{"resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "soak"}}]},
            "scopeLogs": [{"logRecords": [{
                "timeUnixNano": str(NOW), "body": {"stringValue": "tls"}}]}]}]}
        check("a token from the https issuer is accepted",
              with_token("/v1/logs", issuer.token("telemetry:write"), entry) == 200)
        check("a token minted for another audience is still refused",
              with_token("/loki/api/v1/labels",
                         issuer.token("telemetry:read", audience="https://elsewhere")) == 401)
    finally:
        stop(proc)

    # Now the part that makes the checks above mean something. Same issuer, same
    # token, no CA bundle — the handshake must fail, so no keys load.
    write_config(None)
    proc = serve()
    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)
        oidc = oidc_status("static-admin")
        check("without the CA bundle the handshake fails, so nothing is trusted",
              oidc.get("keys") == 0,
              f"keys={oidc.get('keys', 'absent')} — if this is 1, trust is not being applied")
        check("and the instance still serves, rather than refusing to start",
              request("/healthz")[0] == 200)
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def check_relay_over_tls(binary: str) -> None:
    """Relay shipping across a real TLS handshake.

    `relay.upstream` must be https, and until today the shipper could not open an https
    connection at all — so this configuration, the only one a real relay can have, had
    never once run. Everything else in this file points the shipper at plain-HTTP
    loopback, which is allowed for testing and proves nothing about the deployment
    ADR-013 describes.
    """
    print("\n=== relay over real TLS ===")
    upstream = Upstream(tls=True)
    material = tls_material()
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-relay-tls-")
    config = os.path.join(data_dir, "telemetryd.toml")
    with open(config, "w", encoding="utf-8") as handle:
        handle.write(
            "[auth]\nadmin_token = [\"static-admin\"]\n"
            "[storage]\nmax_segment_bytes = \"16KiB\"\nsegment_duration = \"2s\"\n"
            f"[relay]\nupstream = \"{upstream.url}\"\ninterval = \"1s\"\n"
            "[[relay.client]]\napp = \"mobile\"\ntoken = \"mobile-secret\"\n"
            f"[tls]\nca_file = \"{material['ca']}\"\n"
        )

    proc = subprocess.Popen(
        [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
         "--data-dir", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ},
    )
    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)

        check("the relay accepts writes", relay_logs(400, "payments", "mobile-secret") == 200)

        deadline = time.time() + 60
        delivered = 0
        while time.time() < deadline:
            delivered = sum(len(batch.get("resourceLogs", [])) for batch in upstream.received)
            if delivered:
                break
            time.sleep(1)
        check("segments are delivered over https", delivered > 0,
              f"{delivered} batches arrived at the TLS upstream")

        # The identity stamp is the point of relay mode, and it has to survive the
        # transport rather than be a property of the plaintext path.
        stamped = any(
            attribute.get("value", {}).get("stringValue") == "mobile"
            for batch in upstream.received
            for resource in batch.get("resourceLogs", [])
            for attribute in resource.get("resource", {}).get("attributes", [])
        )
        check("what arrives upstream carries the credential's identity", stamped)
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def check_serving_tls(binary: str) -> None:
    """telemetryd terminating TLS itself, verified by a client that checks the chain.

    The point is not that bytes are encrypted — a self-signed certificate nobody
    verifies would achieve that and be worth much less. The point is that a client
    which validates against the issuing CA connects, and one without it does not. That
    second half is what makes this authentication rather than obfuscation, so it is
    asserted rather than assumed.
    """
    print("\n=== serving TLS ===")
    material = tls_material()
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-servetls-")
    config = os.path.join(data_dir, "telemetryd.toml")
    with open(config, "w", encoding="utf-8") as handle:
        handle.write(
            "[auth]\nadmin_token = [\"static-admin\"]\ningest_token = [\"ingest\"]\n"
            f"[server.tls]\ncert_file = \"{material['cert']}\"\n"
            f"key_file = \"{material['key']}\"\n"
        )

    proc = subprocess.Popen(
        [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
         "--data-dir", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ},
    )
    base = f"https://localhost:{PORT}"
    try:
        trusting = ssl.create_default_context(cafile=material["ca"])

        def get(path: str, context: ssl.SSLContext) -> int:
            request = urllib.request.Request(base + path)
            with urllib.request.urlopen(request, timeout=10, context=context) as response:
                return response.status

        ready = False
        for _ in range(240):
            try:
                if get("/healthz", trusting) == 200:
                    ready = True
                    break
            except Exception:  # noqa: BLE001 — any failure here means "not up yet"
                pass
            time.sleep(0.25)
        check("a client that trusts the CA gets a TLS connection", ready)

        payload = json.dumps({"resourceLogs": [{"resource": {"attributes": [
            {"key": "service.name", "value": {"stringValue": "soak"}}]},
            "scopeLogs": [{"logRecords": [{
                "timeUnixNano": str(NOW), "body": {"stringValue": "https"}}]}]}]}).encode()
        request = urllib.request.Request(
            base + "/v1/logs", data=payload,
            headers={"content-type": "application/json", "authorization": "Bearer ingest"})
        with urllib.request.urlopen(request, timeout=10, context=trusting) as response:
            check("telemetry is accepted over https", response.status == 200)

        # The half that makes it authentication: a client with the system trust store
        # has never heard of this CA and must refuse.
        refused = False
        try:
            get("/healthz", ssl.create_default_context())
        except Exception:  # noqa: BLE001 — a verification failure is the pass condition
            refused = True
        check("a client that does not trust the CA is refused", refused)

        # And the port no longer speaks plain HTTP, which is the mistake someone makes
        # once after turning this on.
        plaintext_failed = False
        try:
            urllib.request.urlopen(f"http://localhost:{PORT}/healthz", timeout=5)
        except Exception:  # noqa: BLE001
            plaintext_failed = True
        check("plain HTTP to the TLS port does not work", plaintext_failed)
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def check_self_signed(binary: str) -> None:
    """One environment variable, and the certificate must survive a restart.

    Stability is the property worth testing. A server that mints a fresh certificate
    on every start looks, to anything that pins or caches, exactly like an attacker —
    and it would be the easy implementation, since generating is cheaper than checking
    whether one already exists.
    """
    print("\n=== self-signed certificate ===")
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-selfsigned-")
    env = {**os.environ,
           "TELEMETRYD_AUTH_ADMIN_TOKEN": "static-admin",
           "TELEMETRYD_AUTH_INGEST_TOKEN": "ingest",
           "TELEMETRYD_SERVER_TLS_SELF_SIGNED": "telemetry.internal"}
    base = f"https://localhost:{PORT}"
    unverified = ssl.create_default_context()
    unverified.check_hostname = False
    unverified.verify_mode = ssl.CERT_NONE

    def start() -> subprocess.Popen:
        proc = subprocess.Popen(
            [binary, "serve", "--listen", f"127.0.0.1:{PORT}", "--data-dir", data_dir],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
        for _ in range(240):
            try:
                with urllib.request.urlopen(base + "/healthz", timeout=5,
                                            context=unverified) as response:
                    if response.status == 200:
                        return proc
            except Exception:  # noqa: BLE001 — not up yet
                pass
            time.sleep(0.25)
        return proc

    cert = os.path.join(data_dir, "tls", "self-signed.pem")
    key = os.path.join(data_dir, "tls", "self-signed.key")
    proc = start()
    try:
        check("one environment variable produces a working TLS server",
              os.path.isfile(cert) and os.path.isfile(key))
        # A private key readable by anyone on the box is the sort of thing nobody
        # notices until it matters.
        mode = stat.S_IMODE(os.stat(key).st_mode)
        check("the generated key is readable only by its owner", mode == 0o600,
              f"mode {mode:o}")

        first = hashlib.sha256(pathlib.Path(cert).read_bytes()).hexdigest()
    finally:
        stop(proc)

    proc = start()
    try:
        second = hashlib.sha256(pathlib.Path(cert).read_bytes()).hexdigest()
        check("a restart reuses the certificate rather than minting a new one",
              first == second)

        # rcgen's default window is 1975–4096, which is the absence of a validity
        # period rather than a long one: it makes a leaked key valid forever and trips
        # clients that sanity-check the range.
        dates = subprocess.run(
            ["openssl", "x509", "-in", cert, "-noout", "-dates"],
            capture_output=True, text=True, check=True).stdout
        sane = "1975" not in dates and "4096" not in dates
        check("the certificate has a real validity window", sane,
              dates.replace("\n", " ").strip())

        # It encrypts. It does not authenticate, and claiming otherwise in the docs
        # would be the dangerous part — so assert the honest half too.
        refused = False
        try:
            urllib.request.urlopen(base + "/healthz", timeout=5,
                                   context=ssl.create_default_context())
        except Exception:  # noqa: BLE001 — verification failure is the pass condition
            refused = True
        check("a verifying client still refuses it, as a self-signed certificate should",
              refused)
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def resident_bytes(pid: int) -> int | None:
    """Resident set size, or `None` where it cannot be read.

    `/proc` on Linux, `ps` elsewhere. Returning `None` rather than guessing keeps the
    caller honest: the number in `requirements.md` is scoped to musl, and a figure
    measured with a different allocator is not evidence about it either way.
    """
    status = pathlib.Path(f"/proc/{pid}/status")
    if status.is_file():
        for line in status.read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
        return None
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                             capture_output=True, text=True, check=True).stdout.strip()
        return int(out) * 1024 if out else None
    except Exception:  # noqa: BLE001 — no RSS is a skip, not a failure
        return None


def check_memory_ceiling(binary: str) -> None:
    """The sizing number people provision machines from.

    `requirements.md` promises roughly 80 MB idle plus 1.3x `storage.max_segment_bytes`
    under load — about 400 MB at the 256 MiB default. That number was measured once, by
    hand, on musl, and then never again. It is also the claim that goes silently wrong:
    a buffer that stops respecting its cap does not fail a test, it just makes the box
    the operator sized from this page run out of memory.

    Asserted only on Linux, which is where the claim is scoped and where CI builds and
    soaks the musl binary. Elsewhere it measures and reports, because a figure from a
    different allocator is not evidence about musl.
    """
    print("\n=== memory against the documented ceiling ===")
    cap_mib = 64
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-mem-")
    config = os.path.join(data_dir, "telemetryd.toml")
    with open(config, "w", encoding="utf-8") as handle:
        # No admin token: this binds loopback, and guarding `/status` here would only
        # mean reading the buffer through an auth header the measurement does not need.
        handle.write(
            # A long window so nothing seals: the ceiling is about what the open buffer
            # is allowed to hold, and sealing mid-measurement would hide exactly that.
            f"[storage]\nmax_segment_bytes = \"{cap_mib}MiB\"\nsegment_duration = \"24h\"\n"
        )

    proc = subprocess.Popen(
        [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
         "--data-dir", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ},
    )
    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)

        idle = resident_bytes(proc.pid)

        # Fill the buffer towards the cap without sealing it.
        line = "m" * 300
        buffered = 0
        for batch in range(240):
            records = [{"timeUnixNano": str(NOW + (batch * 1000 + i) * 1_000_000),
                        "body": {"stringValue": line}} for i in range(1000)]
            request("/v1/logs", {"resourceLogs": [{"resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "mem"}}]},
                "scopeLogs": [{"logRecords": records}]}]})
            _, status = request("/status")
            if isinstance(status, dict):
                buffered = status["storage"]["logs"]["buffered_bytes"]
                if buffered > cap_mib * 1024 * 1024 * 0.75:
                    break

        loaded = resident_bytes(proc.pid)
        check("the buffer actually filled, so this measured something",
              buffered > cap_mib * 1024 * 1024 * 0.5,
              f"{buffered / 1048576:.0f} MiB buffered of a {cap_mib} MiB cap")

        if loaded is None:
            check("resident memory could be read", False, "no RSS available")
            return

        # The documented rule, with a quarter of headroom. Tight enough that a buffer
        # which stops respecting its cap fails here — the defect this file already
        # caught once — and loose enough not to flake on allocator noise.
        predicted = 80 * 1024 * 1024 + int(1.3 * cap_mib * 1024 * 1024)
        ceiling = 40 * 1024 * 1024
        detail = (f"idle {(idle or 0) / 1048576:.0f} MB, loaded {loaded / 1048576:.0f} MB, "
                  f"documented {predicted / 1048576:.0f} MB, ceiling {ceiling / 1048576:.0f} MB")

        if sys.platform.startswith("linux"):
            check("resident memory stays under the documented ceiling",
                  loaded <= ceiling, detail)
        else:
            # Reported, not asserted: requirements.md scopes the figure to musl.
            print(f"    note  not asserted on {sys.platform} — {detail}")
    finally:
        stop(proc)
        shutil.rmtree(data_dir, ignore_errors=True)


def check_pipes(binary: str) -> None:
    """Every command has to survive its reader leaving.

    `println!` panics on a closed pipe, and piping into `head` is an ordinary thing to
    do — `telemetryd query … | head -20` used to answer with a panic and a backtrace.
    The usual fix resets SIGPIPE, which needs `unsafe`, which this workspace forbids and
    `forbid` cannot be locally overridden; so it is handled at every write instead, and
    that is worth checking rather than trusting.
    """
    print("\n=== a closed pipe ===")
    for name, argv in [
        ("version", ["version"]),
        ("validate", ["validate"]),
    ]:
        # Enough output to fill the pipe buffer after `head` has gone.
        script = f"for i in $(seq 1 80); do {binary} {' '.join(argv)}; done | head -1"
        result = subprocess.run(["sh", "-c", script], capture_output=True, text=True,
                                timeout=180, check=False)
        check(f"{name} stops rather than panicking",
              "panicked" not in result.stderr and "Broken pipe" not in result.stderr,
              result.stderr.strip().splitlines()[0] if result.stderr.strip() else "clean")


def check_relay(binary: str) -> None:
    """Forwarding, and the identity stamp that is the point of it."""
    print("\n=== relay mode ===")
    upstream = Upstream()
    upstream.failing = True  # down before the first record even arrives
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-relay-")
    config = os.path.join(data_dir, "telemetryd.toml")
    with open(config, "w", encoding="utf-8") as handle:
        handle.write(
            "[auth]\nadmin_token = [\"static-admin\"]\n"
            # Small segments so a seal happens within the test rather than in an hour.
            "[storage]\nmax_segment_bytes = \"16KiB\"\nsegment_duration = \"2s\"\n"
            f"[relay]\nupstream = \"{upstream.url}\"\ninterval = \"1s\"\n"
            "[[relay.client]]\napp = \"mobile\"\ntoken = \"mobile-secret\"\n"
        )

    proc = subprocess.Popen(
        [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
         "--data-dir", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ},
    )
    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)

        check("an unregistered credential cannot write",
              relay_logs(1, "payments", "not-a-client") == 401)

        status = relay_logs(400, "payments", "mobile-secret")
        check("a registered client can write", status == 200, f"HTTP {status}")

        # Relay mode claims all three signals, and `deliver` has a branch for each.
        # Only the logs branch had ever run against a real upstream.
        traces = relay_traces("payments", "mobile-secret")
        metrics = relay_metrics("payments", "mobile-secret")
        check("traces and metrics are accepted too",
              traces == 200 and metrics == 200,
              f"traces={traces} metrics={metrics}")

        # Stored under the credential's identity, not the payload's claim. This is the
        # whole security argument: everything downstream is keyed on this label.
        deadline = time.time() + 30
        stamped = claimed = -1
        while time.time() < deadline:
            _, body = query("/loki/api/v1/query_range", query='{app="mobile"}',
                            start=NOW - SECOND, end=NOW + 10 * SECOND, limit=1000)
            stamped = log_lines(body)
            _, body = query("/loki/api/v1/query_range", query='{app="payments"}',
                            start=NOW - SECOND, end=NOW + 10 * SECOND, limit=1000)
            claimed = log_lines(body)
            if stamped > 0:
                break
            time.sleep(0.5)
        check("the credential decides the app, not the payload",
              stamped == 400 and claimed == 0,
              f"app=mobile:{stamped} app=payments:{claimed}")

        # Upstream has been refusing the whole time. Nothing may be lost, and the
        # cursor must not have moved past anything it did not deliver.
        time.sleep(3)
        check("nothing is delivered while upstream refuses", not upstream.lines(),
              f"{len(upstream.lines())} lines")

        upstream.failing = False
        delivered = []
        deadline = time.time() + 60
        while time.time() < deadline:
            delivered = upstream.lines()
            if len(delivered) >= 400:
                break
            time.sleep(1)

        check("everything accepted is delivered once upstream recovers",
              len(delivered) >= 400, f"{len(delivered)} of 400 lines")
        check("upstream sees the stamped identity, never the claimed one",
              upstream.apps() == {"mobile"}, f"{upstream.apps() or 'nothing'}")

        # The other two branches of `deliver`, end to end: encoded from a sealed
        # segment, posted, and accepted by something that decodes OTLP.
        deadline = time.time() + 45
        while time.time() < deadline and not (upstream.spans() and upstream.metric_names()):
            time.sleep(1)
        check("spans reach upstream", upstream.spans() >= 1, f"{upstream.spans()} spans")
        check("metrics reach upstream", "charges_total" in upstream.metric_names(),
              f"{sorted(upstream.metric_names()) or 'nothing'}")
        # The proto: "UNSPECIFIED is the default AggregationTemporality, it MUST not
        # be used." A strict receiver may reject the whole payload over it.
        check("a forwarded counter names its temporality",
              upstream.sum_temporalities() == {2},
              f"{upstream.sum_temporalities() or 'no sums'}")
        check("a span keeps its status through the round trip",
              upstream.span_statuses() == {2}, f"{upstream.span_statuses() or 'none'}")

        # Visible, or an operator is reading log files to find out how far behind the
        # shipper is. Checked before the restart: these are "since start" counters, and
        # resetting is what a Prometheus counter is supposed to do.
        relay = admin_json("/status").get("relay", {})
        check("/status reports the relay",
              relay.get("upstream", "").startswith("http://127.0.0.1")
              and relay.get("segments_delivered", 0) > 0,
              f"delivered={relay.get('segments_delivered', 'absent')}")
        check("the backlog is reported per signal",
              isinstance(relay.get("pending"), dict)
              and relay["pending"].get("logs") == 0,
              f"pending={relay.get('pending', 'absent')}")
        check("the stamp is reported as on", relay.get("trust_client_identity") is False)
        delivered_through = relay.get("delivered_through", {}).get("logs")
        check("the cursor position is reported", bool(delivered_through),
              delivered_through or "absent")

        metrics = admin_metrics()
        for name in [
            "telemetryd_relay_pending_segments",
            "telemetryd_relay_segments_delivered_total",
            "telemetryd_relay_records_delivered_total",
            "telemetryd_relay_identity_overridden_total",
        ]:
            check(f"{name} is exported", f"{name}{{" in metrics or f"\n{name} " in metrics)

        # The cursor is durable: a restart must not re-send what already arrived.
        before = len(upstream.lines())
        stop(proc)
        proc = subprocess.Popen(
            [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
             "--data-dir", data_dir],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ},
        )
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)
        time.sleep(4)
        check("a restart does not re-send what was already delivered",
              len(upstream.lines()) == before,
              f"{len(upstream.lines())} lines, was {before}")

        # The counters reset, and should: they are "since start". What must survive is
        # the cursor, and that is the field an operator should read to answer "did this
        # actually get there" after a restart.
        after = admin_json("/status").get("relay", {})
        check("the cursor survives the restart, even though the counters do not",
              after.get("delivered_through", {}).get("logs") == delivered_through,
              f"{after.get('delivered_through', {}).get('logs') or 'absent'} "
              f"(was {delivered_through})")
    finally:
        stop(proc)
        upstream.server.shutdown()
        shutil.rmtree(data_dir, ignore_errors=True)


def check_relay_fair_share(binary: str) -> None:
    """One noisy client must not be able to lock the others out.

    `limits.ingest_queue_depth` is global, so a retry loop shipped to a fleet fills it
    and every other client gets 429 through a mechanism working exactly as designed.
    The queue is deliberately tiny here so the contention is real rather than simulated.
    """
    print("\n=== relay: one client cannot starve another ===")
    upstream = Upstream()
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-share-")
    config = os.path.join(data_dir, "telemetryd.toml")
    with open(config, "w", encoding="utf-8") as handle:
        handle.write(
            "[limits]\ningest_queue_depth = 4\n"
            f"[relay]\nupstream = \"{upstream.url}\"\ninterval = \"60s\"\n"
            "max_queue_share = 0.5\n"
            "[[relay.client]]\napp = \"noisy\"\ntoken = \"noisy-secret\"\n"
            "[[relay.client]]\napp = \"quiet\"\ntoken = \"quiet-secret\"\n"
        )

    proc = subprocess.Popen(
        [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
         "--data-dir", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ},
    )
    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)

        noisy_codes: list[int] = []
        quiet_codes: list[int] = []
        stop_flag = threading.Event()

        def flood() -> None:
            while not stop_flag.is_set():
                noisy_codes.append(relay_logs(600, "noisy", "noisy-secret"))

        floods = [threading.Thread(target=flood, daemon=True) for _ in range(12)]
        for thread in floods:
            thread.start()

        time.sleep(1.0)
        for _ in range(12):
            quiet_codes.append(relay_logs(1, "quiet", "quiet-secret"))
            time.sleep(0.1)

        stop_flag.set()
        for thread in floods:
            thread.join(timeout=20)

        refused = noisy_codes.count(429)
        check("the noisy client is capped at its share", refused > 0,
              f"{refused} of {len(noisy_codes)} refused")
        # The actual promise. Two of four slots are reserved from `noisy` by the
        # share, so `quiet` always has somewhere to go.
        check("the quiet client is never refused",
              quiet_codes and all(code == 200 for code in quiet_codes),
              f"{sorted(set(quiet_codes))}")
    finally:
        stop(proc)
        upstream.server.shutdown()
        shutil.rmtree(data_dir, ignore_errors=True)


def check_relay_oversized_segments(binary: str) -> None:
    """A segment is larger than any receiver will take in one request.

    The defaults guarantee it — a 256 MiB segment against a 16 MiB body limit, and the
    OTLP encoding is larger than the Parquet it came from. One request per segment meant
    the first sealed segment was refused forever while the backlog grew until the disk
    budget started discarding telemetry. This is that case, in miniature.
    """
    print("\n=== relay: a segment bigger than upstream accepts ===")
    upstream = Upstream(limit=50 * 1024)
    data_dir = tempfile.mkdtemp(prefix="telemetryd-soak-big-")
    config = os.path.join(data_dir, "telemetryd.toml")
    with open(config, "w", encoding="utf-8") as handle:
        handle.write(
            "[auth]\nadmin_token = [\"static-admin\"]\n"
            "[storage]\nmax_segment_bytes = \"1MiB\"\nsegment_duration = \"2s\"\n"
            f"[relay]\nupstream = \"{upstream.url}\"\ninterval = \"1s\"\n"
            "[[relay.client]]\napp = \"mobile\"\ntoken = \"mobile-secret\"\n"
        )

    proc = subprocess.Popen(
        [binary, "serve", "--config", config, "--listen", f"127.0.0.1:{PORT}",
         "--data-dir", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ},
    )
    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)

        wanted = 0
        for batch in range(5):
            payload = {"resourceLogs": [{
                "resource": {"attributes": [{"key": "app", "value": {"stringValue": "mobile"}}]},
                "scopeLogs": [{"logRecords": [
                    {"timeUnixNano": str(NOW + batch * 1000 + i),
                     "body": {"stringValue": "x" * 300}}
                    for i in range(800)
                ]}],
            }]}
            with_token("/v1/logs", "mobile-secret", payload)
            wanted += 800

        delivered = 0
        deadline = time.time() + 90
        while time.time() < deadline:
            delivered = len(upstream.lines())
            if delivered >= wanted:
                break
            time.sleep(1)

        check("every record is delivered despite the size limit",
              delivered >= wanted, f"{delivered} of {wanted}")
        check("upstream did refuse oversized requests, so this was a real test",
              upstream.refused_too_large > 0, f"{upstream.refused_too_large} refusals")

        relay = admin_json("/status").get("relay", {})
        check("the backlog drains rather than wedging",
              relay.get("pending", {}).get("logs") == 0,
              f"pending={relay.get('pending', 'absent')}")
        check("nothing was dropped for being individually too large",
              relay.get("records_dropped") == 0, f"{relay.get('records_dropped')}")
        # Learned from upstream's refusals, so the descent is paid once rather than
        # per batch forever.
        check("the receiver's limit is learned",
              (relay.get("learned_request_ceiling") or 0) > 0,
              f"{relay.get('learned_request_ceiling')}")
    finally:
        stop(proc)
        upstream.server.shutdown()
        shutil.rmtree(data_dir, ignore_errors=True)


def check_export_import(binary: str) -> None:
    """The round trip is the test ADR-012 named, and it earned its place immediately.

    The first version of the exporter produced matching record *counts* and different
    content: `level` came back as `unknown`, because ingest derives it from
    `severityNumber` and the exporter carried no severity at all. Counting is not
    comparing.
    """
    print("\n=== export and import ===")
    source_dir = tempfile.mkdtemp(prefix="telemetryd-soak-export-src-")
    dest_dir = tempfile.mkdtemp(prefix="telemetryd-soak-export-dst-")
    dump = os.path.join(source_dir, "dump.ndjson")
    dest_port = PORT + 1

    source = subprocess.Popen(
        [binary, "serve", "--listen", f"127.0.0.1:{PORT}", "--data-dir", source_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ})
    dest = subprocess.Popen(
        [binary, "serve", "--listen", f"127.0.0.1:{dest_port}", "--data-dir", dest_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ})

    def lines_at(port: str | int) -> list:
        url = (f"http://127.0.0.1:{port}/loki/api/v1/query_range"
               f"?query={urllib.parse.quote('{app=\"checkout\"}')}"
               f"&start={NOW - 3600 * SECOND}&end={NOW + 60 * SECOND}&limit=5000")
        with urllib.request.urlopen(url, timeout=60) as response:
            body = json.loads(response.read())
        return sorted(
            (stream["stream"].get("level"), stream["stream"].get("service_name"),
             value[0], value[1], json.dumps(value[2] if len(value) > 2 else {}, sort_keys=True))
            for stream in body["data"]["result"] for value in stream["values"]
        )

    try:
        for _ in range(240):
            try:
                if request("/healthz")[0] == 200:
                    break
            except OSError:
                pass
            time.sleep(0.25)

        # Big enough that one window's response passes 10 MB, which is where ureq's
        # default `read_to_string` ceiling sits. The first version of this phase used
        # 400 short records — well under it — so export looked fine and failed on the
        # first window anyone would actually export.
        records = [
            {"timeUnixNano": str(NOW - i * 1_000_000),
             "severityNumber": [9, 13, 17, 5][i % 4],
             "body": {"stringValue": f"payment declined {i} " + "detail " * 250},
             "attributes": [{"key": "order.id", "value": {"stringValue": str(1000 + i)}}]}
            for i in range(7_000)
        ]
        entry = {"resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "app", "value": {"stringValue": "checkout"}}]},
            "scopeLogs": [{"logRecords": records}]}]}
        check("the source accepted the corpus", with_token("/v1/logs", None, entry) == 200)
        time.sleep(1)

        # stdout is data, stderr is progress. That separation is what lets an export be
        # piped into gzip while a meter runs.
        exported = subprocess.run(
            [binary, "export", "--url", BASE, "--since", "1h",
             "--progress", "json"],
            capture_output=True, text=True, timeout=180, check=False)
        check("export writes NDJSON to stdout", exported.returncode == 0
              and exported.stdout.strip().startswith("{"), f"exit {exported.returncode}")
        events = [json.loads(l) for l in exported.stderr.splitlines() if l.strip()]
        check("progress goes to stderr as parseable NDJSON",
              bool(events) and events[-1]["event"] == "done",
              f"{len(events)} events")
        check("the final event carries the high-water mark",
              events and events[-1].get("high_water_nanos") is not None)

        with open(dump, "w", encoding="utf-8") as handle:
            handle.write(exported.stdout)

        imported = subprocess.run(
            [binary, "import", "--file", dump, "--url", f"http://127.0.0.1:{dest_port}",
             "--progress", "none"],
            capture_output=True, text=True, timeout=300, check=False)
        check("import replays the file", imported.returncode == 0,
              imported.stderr.strip()[:200] or f"exit {imported.returncode}")
        time.sleep(1)

        before, after = lines_at(PORT), lines_at(dest_port)
        check("the destination holds the same records", len(after) == len(before),
              f"{len(after)} of {len(before)}")
        check("the export was large enough to have caught the response ceiling",
              len(exported.stdout) > 10 * 1024 * 1024,
              f"{len(exported.stdout) // (1024 * 1024)} MB")
        # The assertion that matters: same *content*, not the same number of rows.
        check("every field survived the round trip", after == before,
              "levels, timestamps, bodies and metadata all match" if after == before
              else "content differs")

        # The direct path between two instances: no file, all three signals, records
        # read rather than re-derived. ADR-012 refused to offer this on the grounds
        # that telemetryd never writes to a foreign store — while relay mode had been
        # posting OTLP upstream since it shipped.
        direct = subprocess.run(
            [binary, "export", "--url", BASE, "--signal", "metrics", "--since", "1h",
             "--to", f"http://127.0.0.1:{dest_port}", "--progress", "none"],
            capture_output=True, text=True, timeout=180, check=False)
        check("export --to posts straight to another instance",
              direct.returncode == 0,
              direct.stderr.strip()[:160] or f"exit {direct.returncode}")

        # Traces from a "foreign" backend — the source is a telemetryd here, which is
        # exactly what a Tempo-compatible one looks like from the client's side.
        #
        # The assertion that matters is that it *terminates*. The first version walked
        # by time, and because search takes seconds a window ending at the oldest
        # trace's second still contained that trace: 44 traces became 8,008 records in
        # 182 requests before it ran the machine out of sockets.
        traces = {"resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}]},
            "scopeSpans": [{"spans": [
                {"traceId": f"{i:032x}", "spanId": f"{i:016x}", "name": "POST /charge",
                 "kind": 2, "startTimeUnixNano": str(NOW - i * 2 * SECOND),
                 "endTimeUnixNano": str(NOW - i * 2 * SECOND + 5000),
                 "status": {"code": 2, "message": "declined"}}
                for i in range(1, 30)]}]}]}
        check("the source accepted traces", with_token("/v1/traces", None, traces) == 200)
        time.sleep(1)

        pulled = subprocess.run(
            [binary, "import", "--from", BASE, "--signal", "traces", "--since", "2h",
             "--url", f"http://127.0.0.1:{dest_port}", "--progress", "none"],
            capture_output=True, text=True, timeout=120, check=False)
        check("traces pull from a read API and the walk terminates",
              pulled.returncode == 0, pulled.stderr.strip()[:160] or f"exit {pulled.returncode}")
        time.sleep(1)

        def trace_count(port: str | int) -> int:
            url = (f"http://127.0.0.1:{port}/api/search"
                   f"?start={(NOW - 7200 * SECOND) // SECOND}"
                   f"&end={(NOW + 60 * SECOND) // SECOND}&limit=1000")
            with urllib.request.urlopen(url, timeout=60) as response:
                return len(json.loads(response.read()).get("traces", []))

        check("every trace arrived", trace_count(dest_port) == trace_count(PORT),
              f"{trace_count(dest_port)} of {trace_count(PORT)}")

        # Refusing beats an import that appears to work and silently produces nothing.
        refused = subprocess.run(
            [binary, "import", "--file", dump, "--url", f"http://127.0.0.1:{dest_port}",
             "--since", "30d", "--progress", "none"],
            capture_output=True, text=True, timeout=120, check=False)
        check("a range past the destination's retention is refused",
              refused.returncode != 0 and "retention" in refused.stderr.lower(),
              refused.stderr.strip()[:120] or "no error")
    finally:
        stop(source)
        dest.terminate()
        try:
            dest.wait(timeout=30)
        except subprocess.TimeoutExpired:
            dest.kill()
        shutil.rmtree(source_dir, ignore_errors=True)
        shutil.rmtree(dest_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
