#![no_main]
//! OTLP/HTTP JSON traces, from an untrusted body.
//!
//! `BUILD-STATUS.md` claimed six targets covered "every parser that reads untrusted
//! bytes — the three query languages, OTLP JSON, and the hand-rolled `remote_write`
//! protobuf". There are *three* OTLP JSON decoders and only logs was fuzzed. This is
//! one of the two that were not, and the claim is now true rather than nearly true.
//!
//! Spans carry more structure than log records — nested events, a status object, hex
//! ids of a fixed width, a duration made of two timestamps — so there is more here for
//! a permissive parser to get wrong.

use libfuzzer_sys::fuzz_target;
use telemetryd_core::config::{IngestConfig, LimitsConfig};
use telemetryd_ingest::logs::DecodeContext;

fuzz_target!(|data: &[u8]| {
    let limits = LimitsConfig::default();
    let ingest = IngestConfig::default();
    let _ = telemetryd_ingest::traces::decode(
        data,
        DecodeContext {
            limits: &limits,
            ingest: &ingest,
            now_nanos: 1_760_000_000_000_000_000,
        },
    );
});
