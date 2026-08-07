---
title: "Requirements"
weight: 3
description: "What telemetryd needs to run, and what it deliberately does not."
---

# Requirements

## To run

**Nothing.** telemetryd is a single statically linked binary with no runtime
dependencies — no libc to match, no JVM, no Python, no shared libraries.

| | |
|---|---|
| Operating system | Linux (any distribution, musl-static) or macOS 11+ |
| Architecture | `x86_64` or `aarch64` |
| Disk | The configured budget, default 10 GiB, plus headroom |
| Memory | ~80 MB idle, plus roughly 1.3x `storage.max_segment_bytes` under load — about 400 MB at the 256 MiB default. Measured on musl; see below |
| Network | One TCP port, default 4319 |

**On memory.** The figure above is measured rather than estimated, driving the musl
release build at full ingest rate: resident size tracks `storage.max_segment_bytes`
and is stable across many seal cycles, not a function of how much traffic arrives.
Lower that setting on a small machine — it is the one knob that moves memory — at the
cost of more, smaller segments.

Prebuilt binaries are published for all four combinations:
`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, `x86_64-apple-darwin`,
`aarch64-apple-darwin`.

## Not required

Stated because these are the dependencies comparable systems do need:

- **No database.** Storage is files in one directory.
- **No object storage.**
- **No collector or agent.** Applications send OTLP straight to telemetryd.
- **No protobuf or C extension on the client.** OTLP/HTTP JSON is first-class, which is
  what makes this work under PHP-FPM.
- **No TLS certificate.** telemetryd speaks plain HTTP; terminate TLS at a reverse
  proxy. See [Security](security/_index.md).

## To build from source

| | |
|---|---|
| Rust | 1.89 or newer, stable |
| C toolchain | None. There are no C dependencies in the tree at all, which is what keeps the static musl builds straightforward |

```bash
cargo build --release
```

## Client compatibility

telemetryd is built for `cboxdk/laravel-telemetry` (ingest) and
`cboxdk/laravel-telemetry-ui` (query). The exact API subset, and how it was derived, is
in [COMPATIBILITY.md](https://github.com/cboxdk/telemetryd/blob/main/COMPATIBILITY.md).

Anything else speaking OTLP/HTTP JSON, Prometheus `remote_write`, or the Loki, Tempo and
Prometheus query APIs will work to the extent it stays inside that subset.
