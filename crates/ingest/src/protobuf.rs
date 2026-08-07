//! A minimal protobuf wire-format reader.
//!
//! Prometheus `remote_write` is the one protobuf on telemetryd's ingest path, and its
//! schema is four messages and eight fields. Decoding it needs varints, length-delimited
//! bytes, and a `double` — about a hundred lines.
//!
//! The alternative was `prost`, which means a `protoc` binary at build time or a vendored
//! one, plus a code-generation step, for this. The dependency budget is a product
//! constraint here (one static binary, four cross-compiled targets), and "generate code
//! from a `.proto` at build time" is exactly the kind of thing that breaks a musl
//! cross-build on someone else's machine.
//!
//! Scope is deliberately narrow: this reads the wire format, it is not a protobuf
//! implementation. Unknown fields are skipped, as the format requires, so a producer
//! sending a newer `remote_write` is handled rather than rejected.

use telemetryd_core::{Error, Result};

/// Protobuf wire types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireType {
    Varint,
    Fixed64,
    LengthDelimited,
    Fixed32,
    /// Deprecated group encoding. Recognised only so it can be refused clearly.
    StartGroup,
    EndGroup,
}

impl WireType {
    fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            0 => Self::Varint,
            1 => Self::Fixed64,
            2 => Self::LengthDelimited,
            3 => Self::StartGroup,
            4 => Self::EndGroup,
            5 => Self::Fixed32,
            _ => return None,
        })
    }
}

/// A cursor over protobuf-encoded bytes.
#[derive(Debug)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    /// Read the next field's number and wire type.
    pub fn next_field(&mut self) -> Result<Option<(u32, WireType)>> {
        if self.is_empty() {
            return Ok(None);
        }
        let key = self.varint()?;
        let wire = WireType::from_tag((key & 0x07) as u8)
            .ok_or_else(|| self.error("unknown protobuf wire type"))?;
        let number =
            u32::try_from(key >> 3).map_err(|_| self.error("field number out of range"))?;

        if matches!(wire, WireType::StartGroup | WireType::EndGroup) {
            return Err(self.error("protobuf group encoding is not supported"));
        }
        Ok(Some((number, wire)))
    }

    pub fn varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let byte = *self
                .bytes
                .get(self.pos)
                .ok_or_else(|| self.error("truncated varint"))?;
            self.pos += 1;

            // Ten groups of 7 bits is the most a u64 can hold; more means corruption,
            // and shifting past 63 would silently wrap.
            if shift >= 64 {
                return Err(self.error("varint overflows 64 bits"));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
        }
    }

    pub fn fixed64(&mut self) -> Result<u64> {
        let end = self
            .pos
            .checked_add(8)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| self.error("truncated fixed64"))?;
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.bytes[self.pos..end]);
        self.pos = end;
        Ok(u64::from_le_bytes(buf))
    }

    pub fn double(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.fixed64()?))
    }

    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = usize::try_from(self.varint()?).map_err(|_| {
            self.error("length-delimited field is longer than this platform allows")
        })?;
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| self.error("truncated length-delimited field"))?;
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    pub fn string(&mut self) -> Result<&'a str> {
        let bytes = self.bytes()?;
        std::str::from_utf8(bytes).map_err(|_| self.error("string field is not valid UTF-8"))
    }

    /// A nested message, as its own reader.
    pub fn message(&mut self) -> Result<Reader<'a>> {
        Ok(Reader::new(self.bytes()?))
    }

    /// Skip a field this decoder does not know about.
    ///
    /// Required by the format, not optional politeness: a producer on a newer
    /// `remote_write` sends fields we have never seen, and refusing them would break
    /// ingest for no reason.
    pub fn skip(&mut self, wire: WireType) -> Result<()> {
        match wire {
            WireType::Varint => {
                self.varint()?;
            }
            WireType::Fixed64 => {
                self.fixed64()?;
            }
            WireType::Fixed32 => {
                let end = self
                    .pos
                    .checked_add(4)
                    .filter(|end| *end <= self.bytes.len())
                    .ok_or_else(|| self.error("truncated fixed32"))?;
                self.pos = end;
            }
            WireType::LengthDelimited => {
                self.bytes()?;
            }
            WireType::StartGroup | WireType::EndGroup => {
                return Err(self.error("protobuf group encoding is not supported"));
            }
        }
        Ok(())
    }

    fn error(&self, message: &str) -> Error {
        Error::BadRequest(format!(
            "{message} at byte {} of the protobuf payload",
            self.pos
        ))
    }
}

#[cfg(test)]
// Exact float comparison is the assertion: a double must survive the wire format
// bit for bit.
#[allow(clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::*;

    /// Encode a field key, for building test payloads.
    fn key(number: u32, wire: u8) -> Vec<u8> {
        encode_varint(u64::from(number) << 3 | u64::from(wire))
    }

    fn encode_varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    #[test]
    fn varints_round_trip_including_the_boundaries() {
        for value in [0u64, 1, 127, 128, 300, u64::from(u32::MAX), u64::MAX] {
            let encoded = encode_varint(value);
            assert_eq!(Reader::new(&encoded).varint().unwrap(), value, "{value}");
        }
    }

    #[test]
    fn a_truncated_varint_is_an_error_not_a_hang() {
        // 0x80 sets the continuation bit with nothing after it.
        assert!(Reader::new(&[0x80]).varint().is_err());
        assert!(Reader::new(&[]).varint().is_err());
    }

    #[test]
    fn an_overlong_varint_is_refused_rather_than_wrapping() {
        // Eleven continuation bytes would shift past 64 and silently wrap.
        let bogus = vec![0xff; 11];
        let err = Reader::new(&bogus).varint().unwrap_err();
        assert!(err.to_string().contains("overflows"), "{err}");
    }

    #[test]
    fn doubles_round_trip_bit_for_bit() {
        for value in [0.0, -1.5, f64::MAX, f64::MIN, 1e-300] {
            let mut payload = Vec::new();
            payload.extend_from_slice(&value.to_bits().to_le_bytes());
            assert_eq!(Reader::new(&payload).double().unwrap(), value);
        }

        // NaN survives as NaN rather than becoming zero.
        let mut payload = Vec::new();
        payload.extend_from_slice(&f64::NAN.to_bits().to_le_bytes());
        assert!(Reader::new(&payload).double().unwrap().is_nan());
    }

    #[test]
    fn length_delimited_fields_are_bounds_checked() {
        // Claims 100 bytes, provides 3.
        let mut payload = encode_varint(100);
        payload.extend_from_slice(b"abc");
        assert!(Reader::new(&payload).bytes().is_err());
    }

    #[test]
    fn strings_must_be_valid_utf8() {
        let mut payload = encode_varint(2);
        payload.extend_from_slice(&[0xff, 0xfe]);
        assert!(Reader::new(&payload).string().is_err());
    }

    #[test]
    fn fields_are_read_with_their_number_and_wire_type() {
        let mut payload = key(1, 2);
        payload.extend(encode_varint(5));
        payload.extend_from_slice(b"hello");
        payload.extend(key(2, 0));
        payload.extend(encode_varint(42));

        let mut reader = Reader::new(&payload);
        let (number, wire) = reader.next_field().unwrap().unwrap();
        assert_eq!((number, wire), (1, WireType::LengthDelimited));
        assert_eq!(reader.string().unwrap(), "hello");

        let (number, wire) = reader.next_field().unwrap().unwrap();
        assert_eq!((number, wire), (2, WireType::Varint));
        assert_eq!(reader.varint().unwrap(), 42);

        assert!(reader.next_field().unwrap().is_none());
    }

    #[test]
    fn unknown_fields_are_skipped_not_refused() {
        // A producer on a newer remote_write sends fields we have never seen; the
        // format requires us to skip them, and refusing would break ingest for
        // nothing.
        let mut payload = key(99, 0);
        payload.extend(encode_varint(1234));
        payload.extend(key(98, 2));
        payload.extend(encode_varint(3));
        payload.extend_from_slice(b"xyz");
        payload.extend(key(97, 1));
        payload.extend_from_slice(&[0u8; 8]);
        payload.extend(key(96, 5));
        payload.extend_from_slice(&[0u8; 4]);
        // …followed by a field we do care about.
        payload.extend(key(1, 0));
        payload.extend(encode_varint(7));

        let mut reader = Reader::new(&payload);
        loop {
            let Some((number, wire)) = reader.next_field().unwrap() else {
                panic!("ran out before reaching field 1");
            };
            if number == 1 {
                assert_eq!(reader.varint().unwrap(), 7);
                break;
            }
            reader.skip(wire).unwrap();
        }
    }

    #[test]
    fn group_encoding_is_refused_by_name() {
        let payload = key(1, 3);
        let err = Reader::new(&payload).next_field().unwrap_err();
        assert!(err.to_string().contains("group"), "{err}");
    }

    #[test]
    fn nested_messages_read_as_their_own_reader() {
        let mut inner = key(1, 0);
        inner.extend(encode_varint(9));

        let mut payload = key(1, 2);
        payload.extend(encode_varint(inner.len() as u64));
        payload.extend_from_slice(&inner);

        let mut reader = Reader::new(&payload);
        reader.next_field().unwrap().unwrap();
        let mut nested = reader.message().unwrap();
        assert_eq!(nested.next_field().unwrap().unwrap(), (1, WireType::Varint));
        assert_eq!(nested.varint().unwrap(), 9);
    }

    #[test]
    fn garbage_input_errors_rather_than_panicking() {
        for payload in [
            vec![0xff],
            vec![0x08],
            vec![0x0a, 0xff, 0xff],
            vec![0xff; 64],
        ] {
            let mut reader = Reader::new(&payload);
            // Whatever happens, it must be a Result, not a panic.
            while let Ok(Some((_, wire))) = reader.next_field() {
                if reader.skip(wire).is_err() {
                    break;
                }
            }
        }
    }
}
