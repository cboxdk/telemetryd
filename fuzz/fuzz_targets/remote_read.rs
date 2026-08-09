#![no_main]
//! Prometheus remote-read responses: hand-written protobuf, straight off a network.
//!
//! The same class of code as the `remote_write` decoder next door, which shipped a
//! defect where eight bytes could declare 2.8 GB of output. This one was written in an
//! afternoon and lived in a binary crate where nothing could fuzz it; moving it here was
//! most of the point.
//!
//! The source is a configured backend rather than an anonymous client, so this is a
//! narrower threat than open ingest — but "we only parse bytes from someone we
//! configured" is exactly what was true of the JWKS reader too, and a hand-rolled wire
//! decoder is where panics live regardless of who is on the other end.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(series) = telemetryd_ingest::remote_read::parse_response(data) {
        // Encoding is part of the path a real response takes, so it is part of what
        // has to survive a hostile one.
        let _ = telemetryd_ingest::remote_read::to_otlp(&series);
    }
});
