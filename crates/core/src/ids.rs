//! Trace and span ids between the spellings clients use: hex, as OTLP/JSON and the
//! Loki and Tempo search APIs write them, and base64, as protobuf's JSON mapping — and
//! so Tempo's trace JSON — writes bytes.

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// A hex id as standard, padded base64.
#[must_use]
pub fn hex_to_base64(hex: &str) -> String {
    let data: Vec<u8> = (0..hex.len() / 2)
        .filter_map(|i| u8::from_str_radix(hex.get(2 * i..2 * i + 2)?, 16).ok())
        .collect();
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | u32::from(*b) << (16 - 8 * i));
        for i in 0..4 {
            out.push(if i <= chunk.len() {
                char::from(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize])
            } else {
                '='
            });
        }
    }
    out
}

/// Base64 as lowercase hex; `None` if it is not base64.
#[must_use]
pub fn base64_to_hex(text: &str) -> Option<String> {
    use std::fmt::Write as _;
    let mut bits = 0u32;
    let mut held = 0;
    let mut out = String::new();
    for c in text.trim_end_matches('=').bytes() {
        let value = ALPHABET.iter().position(|a| *a == c)?;
        bits = (bits << 6 | u32::try_from(value).ok()?) & 0xffff;
        held += 6;
        if held >= 8 {
            held -= 8;
            let _ = write!(out, "{:02x}", (bits >> held) & 0xff);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_survive_both_ways() {
        for hex in ["4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", "0a"] {
            assert_eq!(base64_to_hex(&hex_to_base64(hex)).as_deref(), Some(hex));
        }
        assert_eq!(
            hex_to_base64("4bf92f3577b34da6a3ce929d0e0e4736"),
            "S/kvNXezTaajzpKdDg5HNg=="
        );
        assert!(base64_to_hex("not base64!").is_none());
    }
}
