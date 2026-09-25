//! A segment's stream dictionary, in a file of its own.
//!
//! # Why it left the manifest
//!
//! The dictionary used to be part of `manifest.json`: every stream's labels spelled out
//! in full, pretty-printed, in every segment. On the deployment this was measured on, a
//! metric segment held 12,567 streams and its manifest was five megabytes — eight times
//! the Parquet file holding the samples. A week of segments was 2.55 GB of JSON for
//! 303 MB of data: most of what `/status` reported as used, and parsing it was most of
//! a restart.
//!
//! Here each distinct name and value is written once, a stream is a list of indices into
//! that table, and the whole is zstd-compressed behind the checksum trailer the other
//! sidecars carry. The manifest keeps what is small and what a person reads: the id, the
//! time range, the label index.
//!
//! # Format
//!
//! `TSD1`, then zstd over postcard: the string table, each stream's `(name, value)`
//! indices in name order, each stream's time bounds relative to the segment's first
//! sample, and each stream's row count.

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use telemetryd_core::{Error, Labels, Result};

pub(crate) const FILE: &str = "streams.bin";

const MAGIC: &[u8; 4] = b"TSD1";

/// Far above any real dictionary. What a damaged file could make a load allocate.
const MAX_DECODED_BYTES: u64 = 1 << 30;

#[derive(Serialize, Deserialize)]
struct Encoded {
    strings: Vec<String>,
    /// Per stream, `(name, value)` indices into `strings`, in name order.
    streams: Vec<Vec<(u32, u32)>>,
    /// Per stream, `(first - base, last - first)`. Wrapping arithmetic both ways, so the
    /// bounds come back exactly whatever they were.
    bounds: Vec<(u64, u64)>,
    rows: Vec<u32>,
}

/// What a segment's dictionary holds, indexed by the `stream_id` column.
pub(crate) struct Dictionary {
    pub(crate) streams: Vec<Labels>,
    pub(crate) bounds: Vec<(u64, u64)>,
    pub(crate) rows: Vec<u32>,
}

/// Each distinct string once, numbered in the order first seen.
#[derive(Default)]
struct StringTable<'a> {
    numbered: HashMap<&'a str, u32>,
    strings: Vec<String>,
}

impl<'a> StringTable<'a> {
    fn number(&mut self, text: &'a str) -> u32 {
        if let Some(&number) = self.numbered.get(text) {
            return number;
        }
        let number = u32::try_from(self.strings.len()).unwrap_or(u32::MAX);
        self.strings.push(text.to_owned());
        self.numbered.insert(text, number);
        number
    }
}

/// Write the dictionary into `dir`. `base_nanos` is the segment's first sample.
pub(crate) fn write(
    dir: &Path,
    base_nanos: u64,
    streams: &[Labels],
    bounds: &[(u64, u64)],
    rows: &[u32],
) -> Result<()> {
    let mut table = StringTable::default();
    let encoded_streams: Vec<Vec<(u32, u32)>> = streams
        .iter()
        .map(|labels| {
            labels
                .iter()
                .map(|(name, value)| (table.number(name), table.number(value)))
                .collect()
        })
        .collect();
    let encoded = Encoded {
        strings: table.strings,
        streams: encoded_streams,
        bounds: bounds
            .iter()
            .map(|(first, last)| (first.wrapping_sub(base_nanos), last.wrapping_sub(*first)))
            .collect(),
        rows: rows.to_vec(),
    };

    let path = dir.join(FILE);
    let failed = |what: &str, detail: String| {
        Error::Config(format!(
            "{what} the stream dictionary {}: {detail}",
            path.display()
        ))
    };
    let raw = postcard::to_stdvec(&encoded).map_err(|e| failed("encoding", e.to_string()))?;
    let compressed =
        zstd::encode_all(&raw[..], 3).map_err(|e| failed("compressing", e.to_string()))?;
    let mut contents = Vec::with_capacity(MAGIC.len() + compressed.len());
    contents.extend_from_slice(MAGIC);
    contents.extend_from_slice(&compressed);
    crate::sidecar::write(&path, &contents)
}

/// Read the dictionary in `dir`, or say what is wrong with it.
pub(crate) fn read(dir: &Path, base_nanos: u64) -> std::result::Result<Dictionary, String> {
    let path = dir.join(FILE);
    let contents = crate::sidecar::read(&path)
        .ok_or_else(|| format!("{} is missing or failed its checksum", path.display()))?;
    let compressed = contents
        .strip_prefix(MAGIC.as_slice())
        .ok_or_else(|| format!("{} is not a stream dictionary", path.display()))?;
    let mut raw = Vec::new();
    zstd::stream::read::Decoder::new(compressed)
        .map_err(|e| e.to_string())?
        .take(MAX_DECODED_BYTES)
        .read_to_end(&mut raw)
        .map_err(|e| format!("decompressing {}: {e}", path.display()))?;
    let encoded: Encoded =
        postcard::from_bytes(&raw).map_err(|e| format!("decoding {}: {e}", path.display()))?;
    if encoded.bounds.len() != encoded.streams.len() || encoded.rows.len() != encoded.streams.len()
    {
        return Err(format!("{} lists its streams unevenly", path.display()));
    }

    let mut streams = Vec::with_capacity(encoded.streams.len());
    let mut pairs: Vec<(&str, &str)> = Vec::new();
    for stream in &encoded.streams {
        pairs.clear();
        for &(name, value) in stream {
            let text = |index: u32| encoded.strings.get(index as usize).map(String::as_str);
            let (Some(name), Some(value)) = (text(name), text(value)) else {
                return Err(format!(
                    "{} names a string it does not hold",
                    path.display()
                ));
            };
            pairs.push((name, value));
        }
        streams.push(crate::intern::shared_pairs(&pairs));
    }
    Ok(Dictionary {
        streams,
        bounds: encoded
            .bounds
            .iter()
            .map(|(offset, span)| {
                let first = base_nanos.wrapping_add(*offset);
                (first, first.wrapping_add(*span))
            })
            .collect(),
        rows: encoded.rows,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn a_dictionary_comes_back_as_it_went_in() {
        let dir = tempfile::tempdir().unwrap();
        let streams = vec![
            labels(&[("__name__", "http_requests_total"), ("route", "/a")]),
            labels(&[("__name__", "http_requests_total"), ("route", "/b")]),
            labels(&[]),
        ];
        // The last stream starts before the base: bounds must survive it exactly.
        let bounds = vec![(1_000, 5_000), (1_200, 1_200), (900, u64::MAX)];
        let rows = vec![10, 1, u32::MAX];
        write(dir.path(), 1_000, &streams, &bounds, &rows).unwrap();

        let read = read(dir.path(), 1_000).unwrap();
        assert_eq!(read.streams, streams);
        assert_eq!(read.bounds, bounds);
        assert_eq!(read.rows, rows);
    }

    #[test]
    fn a_damaged_dictionary_is_refused_not_misread() {
        let dir = tempfile::tempdir().unwrap();
        let streams = vec![labels(&[("app", "checkout")])];
        write(dir.path(), 0, &streams, &[(0, 1)], &[1]).unwrap();

        let path = dir.path().join(FILE);
        let mut raw = std::fs::read(&path).unwrap();
        raw[6] ^= 0x40;
        std::fs::write(&path, &raw).unwrap();
        assert!(read(dir.path(), 0).is_err());

        std::fs::remove_file(&path).unwrap();
        assert!(read(dir.path(), 0).is_err());
    }
}
