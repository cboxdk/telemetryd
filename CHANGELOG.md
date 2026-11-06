# Changelog

## 0.73.1 — 2026-11-06

- Honor the OTLP no-recorded-value flag in JSON and protobuf gauges, sums, histograms, exponential histograms and summaries. Unavailable observations do not become measured zeros.
- Update compression and futures dependencies for the verified security fixes.
