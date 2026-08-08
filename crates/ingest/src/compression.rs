//! `Content-Encoding` on an ingest request body.
//!
//! gzip is part of the OTLP/HTTP specification rather than an optional extra: every
//! OpenTelemetry SDK compresses batches above some size threshold. Ignoring the header
//! therefore does not degrade gracefully — it works for the empty batch a health check
//! sends and fails for every batch that carries data. That is exactly how it reached
//! us: a diagnostic reported "all checks passed" while the 8.6 KB metric batch behind
//! it got a 400 and vanished.
//!
//! # Decompression is bounded, always
//!
//! An ingest endpoint that decompresses is a bomb surface: a few KB of gzip expands to
//! gigabytes, and nothing about the compressed body says so in advance. Every decoder
//! here writes through a [`std::io::Take`] sized by `server.max_body_bytes` — the same
//! limit `RequestBodyLimitLayer` holds an uncompressed body to — and stops the moment
//! the output would exceed it. Nothing is inflated into an unbounded buffer and
//! measured afterwards, and no buffer is ever sized from a length the sender chose
//! (gzip's trailing `ISIZE` is four attacker-controlled bytes).
//!
//! The codings supported are the ones already paid for. gzip and deflate come from
//! `flate2`; zstd was in the tree for Parquet before this module existed. Anything
//! else is refused by name — a client sending brotli should be told so, not handed a
//! JSON parse error pointing at byte one.

use std::borrow::Cow;
use std::io::Read;

use telemetryd_core::{Error, Result};

/// A content coding telemetryd can undo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// No coding applied — an absent header, or an explicit `identity`.
    Identity,
    Gzip,
    Deflate,
    Zstd,
}

impl Encoding {
    /// Parse a `Content-Encoding` header value.
    ///
    /// `already_handled` names codings the *caller* undoes itself, which are then
    /// treated as `identity` here. Prometheus `remote_write` needs it: the snappy
    /// framing there is part of the protocol and belongs to
    /// [`crate::remote_write::decode`], so a `Content-Encoding: snappy` on that route
    /// must not be unwrapped twice — nor refused, since Prometheus sends it.
    pub fn parse(value: &str, already_handled: &[&str]) -> Result<Self> {
        let mut coding = Self::Identity;

        // The header is a list. In practice it holds one token, but a proxy that adds
        // `identity` is legal and must not turn into a 400.
        for token in value.split(',') {
            let token = token.trim();
            if token.is_empty()
                || token.eq_ignore_ascii_case("identity")
                || already_handled
                    .iter()
                    .any(|handled| token.eq_ignore_ascii_case(handled))
            {
                continue;
            }

            let parsed = if token.eq_ignore_ascii_case("gzip") || token.eq_ignore_ascii_case("x-gzip")
            {
                Self::Gzip
            } else if token.eq_ignore_ascii_case("deflate") {
                Self::Deflate
            } else if token.eq_ignore_ascii_case("zstd") {
                Self::Zstd
            } else {
                return Err(unsupported(token));
            };

            if coding != Self::Identity {
                // Two real codings stacked. Decodable in principle, and a shape no
                // OTLP client emits — refusing it by name beats carrying the recursion
                // and the extra bomb surface that comes with it.
                return Err(Error::unsupported_with_hint(
                    format!("layered Content-Encoding: {}", value.trim()),
                    "apply a single content coding to the request body",
                ));
            }
            coding = parsed;
        }

        Ok(coding)
    }

    /// The header token, for messages and metric labels.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
            Self::Zstd => "zstd",
        }
    }
}

/// Codings the caller of [`Encoding::parse`] handles itself on the `remote_write`
/// route. See [`Encoding::parse`].
pub const REMOTE_WRITE_PASSTHROUGH: &[&str] = &["snappy", "x-snappy-framed"];

/// Undo `encoding`, refusing anything that expands past `max_bytes`.
///
/// `Identity` borrows the body, so the uncompressed path costs exactly what it did
/// before this existed: no copy, no allocation.
pub fn decode<'a>(encoding: Encoding, body: &'a [u8], max_bytes: usize) -> Result<Cow<'a, [u8]>> {
    match encoding {
        Encoding::Identity => Ok(Cow::Borrowed(body)),
        Encoding::Gzip => {
            // Multi-member, because a producer that flushes per chunk emits one gzip
            // member per flush and `gzip -d` reads that happily.
            bounded(
                flate2::read::MultiGzDecoder::new(body),
                body.len(),
                max_bytes,
                "gzip",
            )
            .map(Cow::Owned)
        }
        Encoding::Deflate => deflate(body, max_bytes).map(Cow::Owned),
        Encoding::Zstd => zstd_decode(body, max_bytes).map(Cow::Owned),
    }
}

/// `Content-Encoding: deflate` is zlib-wrapped (RFC 9110 §8.4.1.2). Enough clients
/// send a bare deflate stream instead that refusing outright would be pedantry, so a
/// malformed zlib header is retried as raw deflate — the same forgiveness
/// [`crate::remote_write::decode`] already extends to uncompressed protobuf.
///
/// A body that blew the cap is over the cap either way, so only a stream error falls
/// back, and the zlib error is the one reported if the retry also fails.
fn deflate(body: &[u8], max_bytes: usize) -> Result<Vec<u8>> {
    match bounded(
        flate2::read::ZlibDecoder::new(body),
        body.len(),
        max_bytes,
        "deflate",
    ) {
        Ok(bytes) => Ok(bytes),
        Err(zlib_error) => {
            if matches!(zlib_error, Error::LimitExceeded { .. }) {
                return Err(zlib_error);
            }
            bounded(
                flate2::read::DeflateDecoder::new(body),
                body.len(),
                max_bytes,
                "deflate",
            )
            .map_err(|raw_error| {
                if matches!(raw_error, Error::LimitExceeded { .. }) {
                    raw_error
                } else {
                    zlib_error
                }
            })
        }
    }
}

fn zstd_decode(body: &[u8], max_bytes: usize) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::read::Decoder::new(body)
        .map_err(|e| malformed("zstd", &e.to_string()))?;

    // A zstd frame declares its own window size and the decoder allocates it up front,
    // before a single byte of output exists — attacker-controlled memory that the
    // output cap alone does not bound. No frame whose output fits in `max_bytes` needs
    // a window bigger than that, so cap the window there. The floor is zstd's smallest
    // legal window and the ceiling is its default limit, so this only ever tightens.
    let window_log = max_bytes
        .max(1)
        .ilog2()
        .saturating_add(1)
        .clamp(10, 27);
    decoder
        .window_log_max(window_log)
        .map_err(|e| malformed("zstd", &e.to_string()))?;

    bounded(decoder, body.len(), max_bytes, "zstd")
}

/// Read a decoder to exhaustion, or to one byte past the cap — whichever comes first.
fn bounded<R: Read>(
    reader: R,
    compressed_len: usize,
    max_bytes: usize,
    coding: &'static str,
) -> Result<Vec<u8>> {
    // Never size the buffer from anything the sender declares. A small multiple of the
    // bytes actually received is a safe starting guess; the Vec grows from there, and
    // the `take` below is what makes that growth terminate.
    let initial = compressed_len
        .saturating_mul(4)
        .min(max_bytes)
        .min(1024 * 1024);
    let mut out = Vec::with_capacity(initial);

    let ceiling = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    reader
        .take(ceiling)
        .read_to_end(&mut out)
        .map_err(|e| malformed(coding, &e.to_string()))?;

    if out.len() > max_bytes {
        return Err(Error::LimitExceeded {
            limit: "server.max_body_bytes",
            detail: format!(
                "the {coding} request body expands past the {max_bytes} byte limit \
                 ({} compressed bytes received)",
                compressed_len
            ),
        });
    }

    Ok(out)
}

fn malformed(coding: &'static str, detail: &str) -> Error {
    Error::BadRequest(format!(
        "the request body is not valid {coding} (Content-Encoding says it is): {detail}"
    ))
}

fn unsupported(coding: &str) -> Error {
    Error::unsupported_with_hint(
        format!("Content-Encoding: {coding}"),
        "send the body uncompressed, or compress it with gzip, deflate or zstd",
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Write;

    use super::*;

    const MAX: usize = 1024 * 1024;

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn zlib(bytes: &[u8]) -> Vec<u8> {
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn raw_deflate(bytes: &[u8]) -> Vec<u8> {
        let mut encoder =
            flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    // ---- header parsing ----

    #[test]
    fn the_codings_we_speak_are_recognised_however_they_are_spelled() {
        for (value, expected) in [
            ("gzip", Encoding::Gzip),
            ("GZIP", Encoding::Gzip),
            ("  gzip  ", Encoding::Gzip),
            ("x-gzip", Encoding::Gzip),
            ("deflate", Encoding::Deflate),
            ("zstd", Encoding::Zstd),
            ("identity", Encoding::Identity),
            ("", Encoding::Identity),
            // A proxy that adds `identity` alongside the real coding is legal.
            ("identity, gzip", Encoding::Gzip),
        ] {
            assert_eq!(
                Encoding::parse(value, &[]).unwrap(),
                expected,
                "Content-Encoding: {value:?}"
            );
        }
    }

    #[test]
    fn an_unsupported_coding_is_named_rather_than_guessed_at() {
        let err = Encoding::parse("br", &[]).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }));
        assert!(
            err.to_string().contains("br"),
            "the message has to name the coding, got {err}"
        );
        assert_eq!(err.code(), "unsupported_feature");
    }

    #[test]
    fn stacked_codings_are_refused_by_name() {
        let err = Encoding::parse("gzip, zstd", &[]).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }));
        assert!(err.to_string().contains("gzip, zstd"));
    }

    #[test]
    fn a_coding_the_caller_owns_reads_as_identity() {
        // remote_write's snappy belongs to the decoder, not to this layer.
        assert_eq!(
            Encoding::parse("snappy", REMOTE_WRITE_PASSTHROUGH).unwrap(),
            Encoding::Identity
        );
        // ...and only for the caller that claims it.
        assert!(Encoding::parse("snappy", &[]).is_err());
    }

    // ---- decoding ----

    #[test]
    fn identity_borrows_the_body_untouched() {
        let body = b"{\"resourceLogs\":[]}";
        let decoded = decode(Encoding::Identity, body, MAX).unwrap();
        assert!(matches!(decoded, Cow::Borrowed(_)), "identity must not copy");
        assert_eq!(&*decoded, body);
    }

    #[test]
    fn every_coding_round_trips() {
        let payload = br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[]}]}]}"#;
        for (encoding, bytes) in [
            (Encoding::Gzip, gzip(payload)),
            (Encoding::Deflate, zlib(payload)),
            (Encoding::Zstd, zstd::encode_all(&payload[..], 3).unwrap()),
        ] {
            let decoded = decode(encoding, &bytes, MAX).unwrap();
            assert_eq!(&*decoded, payload, "coding {}", encoding.as_str());
        }
    }

    #[test]
    fn a_bare_deflate_stream_is_accepted_as_well_as_a_zlib_wrapped_one() {
        let payload = b"the spec says zlib; some clients disagree";
        assert_eq!(
            &*decode(Encoding::Deflate, &raw_deflate(payload), MAX).unwrap(),
            payload
        );
    }

    #[test]
    fn concatenated_gzip_members_decode_as_one_body() {
        let mut bytes = gzip(b"first ");
        bytes.extend_from_slice(&gzip(b"second"));
        assert_eq!(
            &*decode(Encoding::Gzip, &bytes, MAX).unwrap(),
            b"first second"
        );
    }

    // ---- the bomb ----

    #[test]
    fn a_body_that_expands_past_the_cap_is_refused_not_buffered() {
        // 64 MiB of zeroes compresses to a handful of KB. With the cap at 1 MiB this
        // must fail, and it must fail without ever holding 64 MiB.
        let bomb = gzip(&vec![0u8; 64 * 1024 * 1024]);
        assert!(
            bomb.len() < 128 * 1024,
            "the point of the test is that the compressed form is tiny, got {}",
            bomb.len()
        );

        let err = decode(Encoding::Gzip, &bomb, MAX).unwrap_err();
        assert!(
            matches!(err, Error::LimitExceeded { limit, .. } if limit == "server.max_body_bytes"),
            "expected a limit error naming the setting, got {err}"
        );
        assert_eq!(err.code(), "limit_exceeded");
    }

    #[test]
    fn the_cap_is_enforced_for_every_coding() {
        let big = vec![b'x'; 4 * 1024 * 1024];
        for (encoding, bytes) in [
            (Encoding::Gzip, gzip(&big)),
            (Encoding::Deflate, zlib(&big)),
            (Encoding::Zstd, zstd::encode_all(&big[..], 3).unwrap()),
        ] {
            let err = decode(encoding, &bytes, MAX).unwrap_err();
            assert!(
                matches!(err, Error::LimitExceeded { .. }),
                "coding {} was not capped: {err}",
                encoding.as_str()
            );
        }
    }

    #[test]
    fn a_body_exactly_at_the_cap_is_still_accepted() {
        // Off-by-one on a limit is the difference between "16 MiB" and "16 MiB minus
        // one byte", and a client batching to the documented number would hit it.
        let payload = vec![b'y'; 4096];
        let decoded = decode(Encoding::Gzip, &gzip(&payload), payload.len()).unwrap();
        assert_eq!(decoded.len(), payload.len());

        let err = decode(Encoding::Gzip, &gzip(&payload), payload.len() - 1).unwrap_err();
        assert!(matches!(err, Error::LimitExceeded { .. }));
    }

    #[test]
    fn a_zero_cap_admits_nothing_rather_than_panicking() {
        assert!(decode(Encoding::Gzip, &gzip(b"x"), 0).is_err());
        assert!(decode(Encoding::Zstd, &zstd::encode_all(&b"x"[..], 3).unwrap(), 0).is_err());
    }

    // ---- malformed input ----

    #[test]
    fn a_body_that_is_not_what_the_header_claims_says_so() {
        let err = decode(Encoding::Gzip, b"{\"resourceLogs\":[]}", MAX).unwrap_err();
        assert!(matches!(err, Error::BadRequest(_)));
        assert!(
            err.to_string().contains("gzip"),
            "the message has to point at the coding, got {err}"
        );
    }

    #[test]
    fn a_truncated_stream_is_a_bad_request_not_a_panic() {
        let bytes = gzip(b"a payload long enough to survive being cut in half");
        for cut in [1, bytes.len() / 2, bytes.len() - 1] {
            let err = decode(Encoding::Gzip, &bytes[..cut], MAX).unwrap_err();
            assert!(matches!(err, Error::BadRequest(_)), "cut at {cut}: {err}");
        }
    }

    #[test]
    fn an_empty_body_under_a_coding_is_a_bad_request() {
        assert!(matches!(
            decode(Encoding::Gzip, b"", MAX).unwrap_err(),
            Error::BadRequest(_)
        ));
    }
}
