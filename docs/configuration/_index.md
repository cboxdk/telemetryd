---
title: "Configuration"
weight: 40
description: "Every option, its default, and the layering rules."
---

# Configuration

telemetryd starts with no configuration at all. `telemetryd serve` with no file, no
environment and no flags is a complete, supported setup — the empty configuration is a
valid configuration, and everything else is an override on top of it.

- [Reference](reference.md) — every option, with defaults

**Precedence**, lowest to highest: built-in defaults → `telemetryd.toml` → environment →
CLI flags. Each layer is partial, so any subset of the upper ones suffices.

```bash
telemetryd validate
```

Type-checks, runs the cross-field rules, and prints every resolved value **with the
layer it came from**. "Is this setting actually taking effect?" is the question a config
check exists to answer, and a plain syntax check does not answer it.
