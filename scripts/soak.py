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


class Issuer:
    """A stand-in Cbox ID: a key pair, a JWKS endpoint, and tokens signed with it."""

    def __init__(self) -> None:
        _, self.n, self.e, self.d = der_integers(pathlib.Path(RSA_KEY_DER).read_bytes())[:4]
        self.size = (self.n.bit_length() + 7) // 8
        self.kid = "test-key-1"
        self.jwks = json.dumps({"keys": [{
            "kty": "RSA", "kid": self.kid, "use": "sig", "alg": "RS256",
            "n": b64url(self.n.to_bytes(self.size, "big")),
            "e": b64url(self.e.to_bytes((self.e.bit_length() + 7) // 8, "big")),
        }]}).encode()
        self.server = http.server.HTTPServer(("127.0.0.1", 0), self._handler())
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
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


if __name__ == "__main__":
    sys.exit(main())
