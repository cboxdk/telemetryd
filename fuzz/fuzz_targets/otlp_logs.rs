#![no_main]
//! OTLP/HTTP JSON is the primary ingest path, and it is parsed from an untrusted body.
//!
//! The decoder is deliberately permissive — int64 as string, camelCase and snake_case
//! spellings, enums by name or number — and permissive parsers are where panics live.

use libfuzzer_sys::fuzz_target;
use telemetryd_core::config::{IngestConfig, LimitsConfig};
use telemetryd_ingest::logs::DecodeContext;

fuzz_target!(|data: &[u8]| {
    let limits = LimitsConfig::default();
    let ingest = IngestConfig::default();
    let _ = telemetryd_ingest::logs::decode(
        data,
        DecodeContext {
            limits: &limits,
            ingest: &ingest,
            now_nanos: 1_760_000_000_000_000_000,
        },
    );
});
