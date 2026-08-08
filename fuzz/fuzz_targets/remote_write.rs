#![no_main]
//! Prometheus `remote_write`: snappy over a hand-rolled protobuf reader.
//!
//! Hand-rolled wire-format decoding of untrusted bytes is the highest-risk parser in
//! the project. It already refuses overlong varints, group encoding and unterminated
//! fields; this is how those refusals stay true for inputs nobody thought of.

use libfuzzer_sys::fuzz_target;
use telemetryd_core::config::LimitsConfig;
use telemetryd_ingest::remote_write::WriteContext;

fuzz_target!(|data: &[u8]| {
    let limits = LimitsConfig::default();
    let _ = telemetryd_ingest::remote_write::decode(
        data,
        WriteContext {
            limits: &limits,
            default_app: "fuzz",
        },
    );
});
