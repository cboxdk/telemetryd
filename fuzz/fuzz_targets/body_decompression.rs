#![no_main]
//! Ingest request bodies arrive compressed, and the decompressor sees the bytes before
//! any parser does.
//!
//! That makes it the outermost thing reading untrusted input on the write path — and
//! unlike the JSON decoders, it hands work to third-party inflate code with an
//! attacker-chosen stream. Two properties matter here and neither is provable by
//! example: it must not panic, and whatever it returns must fit under the cap. A cap
//! that holds for every input a fuzzer can reach is the difference between
//! `server.max_body_bytes` being a bound and being a suggestion.

use libfuzzer_sys::fuzz_target;
use telemetryd_ingest::compression::{self, Encoding};

/// Deliberately small. The interesting inputs are the ones that try to cross it, and a
/// low cap means the fuzzer reaches them with tiny bodies.
const MAX_BYTES: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    for encoding in [Encoding::Gzip, Encoding::Deflate, Encoding::Zstd] {
        if let Ok(decoded) = compression::decode(encoding, data, MAX_BYTES) {
            assert!(
                decoded.len() <= MAX_BYTES,
                "{} decoded {} bytes past a {MAX_BYTES} byte cap",
                encoding.as_str(),
                decoded.len()
            );
        }
    }

    // Identity must stay a borrow of exactly what came in, whatever came in.
    if let Ok(decoded) = compression::decode(Encoding::Identity, data, MAX_BYTES) {
        assert_eq!(&*decoded, data);
    }

    // The header is untrusted too, and it is parsed by hand.
    if let Ok(header) = std::str::from_utf8(data) {
        let _ = Encoding::parse(header, &[]);
        let _ = Encoding::parse(header, compression::REMOTE_WRITE_PASSTHROUGH);
    }
});
