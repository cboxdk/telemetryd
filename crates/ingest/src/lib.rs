//! Ingest decoders.
//!
//! Turns wire formats into the internal signal types, applies the limits from
//! `[limits]`, and hands records to `telemetryd-store`. Rejections are never silent:
//! every one increments `telemetryd_ingest_rejected_total{signal,reason}` and is
//! visible in `/status`.
//!
//! # Planned surface
//!
//! | Format                              | Endpoint          | Milestone |
//! |-------------------------------------|-------------------|-----------|
//! | OTLP/HTTP **JSON** logs             | `/v1/logs`        | M1        |
//! | OTLP/HTTP **JSON** traces           | `/v1/traces`      | M2        |
//! | OTLP/HTTP **JSON** metrics          | `/v1/metrics`     | M3        |
//! | Prometheus `remote_write`           | `/api/v1/write`   | M3        |
//! | Prometheus scrape (client)          | `[[scrape]]`      | M3        |
//!
//! JSON is the first-class OTLP encoding because that is what `cboxdk/laravel-telemetry`
//! emits — no protobuf, no C extension on the client. Protobuf on the *server* side is
//! fine, which is why `remote_write` (snappy + protobuf) is in scope; OTLP/gRPC is not,
//! in v1.

#![doc(html_root_url = "https://docs.rs/telemetryd-ingest")]
