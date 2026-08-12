#![no_main]
//! The protobuf ingest path, parsed from an untrusted body.
//!
//! Higher risk than the JSON one, and worth its own target for three reasons. It reads
//! raw bytes rather than a validated string, so length prefixes and wire types come
//! straight from the sender. It recurses through `AnyValue`, which a few crafted bytes
//! can nest arbitrarily deep. And it is the encoding every stock OpenTelemetry SDK
//! sends, so it is the path most likely to meet a producer nobody here has tested.
//!
//! All three signals are driven from the same input: the same bytes are a valid message
//! in more than one schema, and a length prefix that is safe in one field number can
//! run off the end in another.

use libfuzzer_sys::fuzz_target;
use telemetryd_core::config::{IngestConfig, LimitsConfig};
use telemetryd_ingest::logs::DecodeContext;
use telemetryd_ingest::otlp_metrics::MetricContext;

fuzz_target!(|data: &[u8]| {
    let limits = LimitsConfig::default();
    let ingest = IngestConfig::default();
    let now = 1_760_000_000_000_000_000;

    // Decode, and then convert: a decoder that produces a structurally valid but absurd
    // payload — a million empty scopes, a bucket count vector longer than the body —
    // must not panic downstream either, and conversion is where the limits live.
    if let Ok(payload) = telemetryd_ingest::otlp_protobuf::logs(data) {
        let _ = telemetryd_ingest::logs::convert_data(
            &payload,
            DecodeContext {
                limits: &limits,
                ingest: &ingest,
                now_nanos: now,
            },
        );
    }

    if let Ok(payload) = telemetryd_ingest::otlp_protobuf::traces(data) {
        let _ = telemetryd_ingest::traces::convert_data(
            &payload,
            DecodeContext {
                limits: &limits,
                ingest: &ingest,
                now_nanos: now,
            },
        );
    }

    if let Ok(payload) = telemetryd_ingest::otlp_protobuf::metrics(data) {
        let _ = telemetryd_ingest::otlp_metrics::convert_data(
            &payload,
            MetricContext {
                limits: &limits,
                ingest: &ingest,
                now_nanos: now,
            },
        );
    }
});
