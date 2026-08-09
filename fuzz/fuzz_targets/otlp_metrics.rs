#![no_main]
//! OTLP/HTTP JSON metrics, from an untrusted body.
//!
//! The second of the two OTLP decoders that were never fuzzed. It has the widest
//! surface of the three: gauge, sum, histogram and summary bodies, each with their own
//! data-point shape, plus bucket bounds and counts that have to line up with each other
//! or the record is nonsense.

use libfuzzer_sys::fuzz_target;
use telemetryd_core::config::{IngestConfig, LimitsConfig};

fuzz_target!(|data: &[u8]| {
    let limits = LimitsConfig::default();
    let ingest = IngestConfig::default();
    let _ = telemetryd_ingest::otlp_metrics::decode(
        data,
        telemetryd_ingest::otlp_metrics::MetricContext {
            limits: &limits,
            ingest: &ingest,
            now_nanos: 1_760_000_000_000_000_000,
        },
    );
});
