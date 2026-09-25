//! Writing and reading a segment's sidecar files — the Bloom filter, the trigram index
//! and the counter summaries.
//!
//! A sidecar is trusted to *skip* data: a filter that says "not here" means the segment
//! is never opened. So a damaged one is worse than a missing one — missing means "read
//! the segment", damaged can mean "the answer quietly lacks rows". They were written
//! with a plain `fs::write`, never fsynced, and read back on no more than a magic
//! number. Now each is written to a temporary name, fsynced and renamed into place, and
//! carries a CRC-32 that is checked on read.
//!
//! The trailer is eight bytes after the contents: `TCRC`, then the CRC of the contents,
//! little-endian. A file from before this has no trailer and is read as it always was;
//! one whose trailer does not match is refused, and the caller reads the segment instead.

use std::fs;
use std::io::Write;
use std::path::Path;

use telemetryd_core::{Error, Result};

const TRAILER: &[u8; 4] = b"TCRC";

/// Write `contents` at `path` with its checksum, durably and atomically: a reader sees
/// the old file, the new one, or none — never half of one.
pub(crate) fn write(path: &Path, contents: &[u8]) -> Result<()> {
    let staged = path.with_extension("partial");
    let io = |what: &str, e| Error::io(format!("{what} {}", path.display()), e);
    let mut file = fs::File::create(&staged).map_err(|e| io("writing", e))?;
    file.write_all(contents).map_err(|e| io("writing", e))?;
    file.write_all(TRAILER).map_err(|e| io("writing", e))?;
    file.write_all(&crc32fast::hash(contents).to_le_bytes())
        .map_err(|e| io("writing", e))?;
    file.sync_all().map_err(|e| io("syncing", e))?;
    drop(file);
    fs::rename(&staged, path).map_err(|e| io("publishing", e))
}

/// The contents of the sidecar at `path`, or `None` when it is missing or damaged.
pub(crate) fn read(path: &Path) -> Option<Vec<u8>> {
    let mut raw = fs::read(path).ok()?;
    let split = raw.len().checked_sub(8)?;
    if &raw[split..split + 4] != TRAILER {
        // Written before sidecars carried a checksum.
        return Some(raw);
    }
    let stored = u32::from_le_bytes(raw[split + 4..].try_into().ok()?);
    raw.truncate(split);
    if crc32fast::hash(&raw) != stored {
        tracing::warn!(
            path = %path.display(),
            "a segment's index file failed its checksum; reading the segment instead"
        );
        return None;
    }
    Some(raw)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn contents_survive_and_damage_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloom.bin");
        write(&path, b"TDBF0123456789").unwrap();
        assert_eq!(read(&path).unwrap(), b"TDBF0123456789");
        assert!(!dir.path().join("bloom.partial").exists());

        // One flipped bit in the contents and the file is not trusted.
        let mut raw = fs::read(&path).unwrap();
        raw[6] ^= 0x01;
        fs::write(&path, &raw).unwrap();
        assert!(read(&path).is_none());

        // A file from before the trailer existed reads as it always did.
        fs::write(&path, b"TDBF-legacy").unwrap();
        assert_eq!(read(&path).unwrap(), b"TDBF-legacy");
    }
}
