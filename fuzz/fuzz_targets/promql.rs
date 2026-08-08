#![no_main]
//! Every input is either parsed or rejected. Neither may panic.
//!
//! A query language reaches this parser straight from an HTTP query string, so a panic
//! here is a request that takes down a worker. A proptest over the shared lexer already
//! found one — a backslash before a multi-byte character left the cursor mid-character
//! and the next slice panicked — which is the kind of thing coverage-guided fuzzing
//! finds far faster than generated examples do.

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(input) = std::str::from_utf8(data) else {
        return;
    };
    // The result is deliberately ignored: a rejection is a correct outcome, and the
    // only thing being asserted is that neither outcome panics.
    let _ = telemetryd_query::promql::parse(input);
});
