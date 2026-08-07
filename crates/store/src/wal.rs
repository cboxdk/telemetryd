//! Segmented write-ahead log.
//!
//! The WAL is the durability boundary for ingest and nothing more — it is not a
//! replication log and not a query source beyond crash replay (ADR-001).
//!
//! # Frame format
//!
//! Each segment file begins with an 8-byte header (`TDWL` + `u32` format version),
//! followed by frames:
//!
//! ```text
//! ┌────────────┬────────────┬───────────────┐
//! │ len: u32le │ crc: u32le │ payload: len  │
//! └────────────┴────────────┴───────────────┘
//! ```
//!
//! # Crash semantics
//!
//! A process killed mid-write leaves a partial frame at the end of the newest segment.
//! Replay detects this — short read, or a CRC that does not match — and treats it as
//! the end of the valid log: the file is truncated to the last good offset and the
//! event is reported, never hidden. The same damage found in *any earlier* segment is
//! real corruption rather than a torn tail, and is an error, because nothing should
//! ever have been appended after it.

use std::fs::{self, File};
use std::io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use telemetryd_core::config::WalSync;
use telemetryd_core::{Error, Result};

const MAGIC: &[u8; 4] = b"TDWL";
const FORMAT: u32 = 1;
const HEADER_LEN: usize = 8;
/// The same value for byte-offset arithmetic, which is `u64` throughout.
const HEADER_LEN_U64: u64 = HEADER_LEN as u64;
const FRAME_HEADER_LEN: usize = 8;

/// Refuse to allocate on a length field from a corrupt frame. No legitimate WAL
/// payload approaches this — the ingest body limit is enforced far below it.
const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// An open write-ahead log for one signal.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    writer: BufWriter<File>,
    seq: u64,
    segment_bytes: u64,
    max_segment_bytes: u64,
    sync: WalSync,
    sync_interval: Duration,
    last_sync: Instant,
    unsynced_records: u64,
    appended_records: u64,
    appended_bytes: u64,
}

impl Wal {
    /// Open the log in `dir`, appending to the newest segment or starting one.
    ///
    /// The caller is expected to have replayed first — [`Self::open`] does not
    /// validate existing content, so a torn tail left by a crash must be repaired by
    /// [`replay`] before appending, or the new records land after the damage.
    pub fn open(
        dir: impl AsRef<Path>,
        sync: WalSync,
        sync_interval: Duration,
        max_segment_bytes: u64,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .map_err(|e| Error::io(format!("creating WAL directory {}", dir.display()), e))?;

        let seq = segment_sequences(&dir)?.last().copied().unwrap_or(1);
        let (file, segment_bytes) = open_segment(&dir, seq)?;

        Ok(Self {
            dir,
            writer: BufWriter::new(file),
            seq,
            segment_bytes,
            max_segment_bytes: max_segment_bytes.max(HEADER_LEN_U64 + 1),
            sync,
            sync_interval,
            last_sync: Instant::now(),
            unsynced_records: 0,
            appended_records: 0,
            appended_bytes: 0,
        })
    }

    /// Append one record, rotating to a new segment first if this one is full.
    pub fn append(&mut self, payload: &[u8]) -> Result<()> {
        let len = u32::try_from(payload.len()).map_err(|_| Error::LimitExceeded {
            limit: "wal_frame_len",
            detail: format!("record of {} bytes exceeds the frame limit", payload.len()),
        })?;
        if len > MAX_FRAME_LEN {
            return Err(Error::LimitExceeded {
                limit: "wal_frame_len",
                detail: format!(
                    "record of {len} bytes exceeds the {MAX_FRAME_LEN} byte frame limit"
                ),
            });
        }

        let frame_len = FRAME_HEADER_LEN as u64 + u64::from(len);
        // Rotate before writing, but never produce an empty segment: an oversized
        // record still has to go somewhere.
        if self.segment_bytes > HEADER_LEN_U64
            && self.segment_bytes + frame_len > self.max_segment_bytes
        {
            self.rotate()?;
        }

        let crc = crc32fast::hash(payload);
        let mut header = [0u8; FRAME_HEADER_LEN];
        header[..4].copy_from_slice(&len.to_le_bytes());
        header[4..].copy_from_slice(&crc.to_le_bytes());

        self.write_all(&header)?;
        self.write_all(payload)?;

        self.segment_bytes += frame_len;
        self.appended_records += 1;
        self.appended_bytes += frame_len;
        self.unsynced_records += 1;

        self.maybe_sync()
    }

    /// Apply the configured sync policy. Called after every append; also worth calling
    /// from a timer so an idle-then-crash sequence does not hold unsynced records
    /// longer than the configured interval.
    pub fn maybe_sync(&mut self) -> Result<()> {
        let due = match self.sync {
            WalSync::Always => true,
            WalSync::Interval => self.last_sync.elapsed() >= self.sync_interval,
            WalSync::Never => false,
        };
        if due && self.unsynced_records > 0 {
            self.sync()?;
        }
        Ok(())
    }

    /// Flush userspace buffers and fsync, regardless of policy. Called on shutdown and
    /// before sealing a segment.
    pub fn sync(&mut self) -> Result<()> {
        self.writer
            .flush()
            .map_err(|e| Error::io(format!("flushing WAL segment {}", self.path().display()), e))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|e| Error::io(format!("syncing WAL segment {}", self.path().display()), e))?;
        self.unsynced_records = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    /// Close the current segment and start the next one.
    pub fn rotate(&mut self) -> Result<()> {
        self.sync()?;
        self.seq += 1;
        let (file, bytes) = open_segment(&self.dir, self.seq)?;
        self.writer = BufWriter::new(file);
        self.segment_bytes = bytes;
        tracing::debug!(dir = %self.dir.display(), seq = self.seq, "rotated WAL segment");
        Ok(())
    }

    /// Delete every segment strictly older than the current one.
    ///
    /// Called once the records in those segments are durable in a sealed on-disk
    /// segment — until then the WAL is the only copy.
    pub fn truncate_to_current(&mut self) -> Result<u64> {
        let mut removed = 0;
        for seq in segment_sequences(&self.dir)? {
            if seq < self.seq {
                let path = segment_path(&self.dir, seq);
                match fs::remove_file(&path) {
                    Ok(()) => removed += 1,
                    Err(e) if e.kind() == ErrorKind::NotFound => {}
                    Err(e) => return Err(Error::io(format!("removing {}", path.display()), e)),
                }
            }
        }
        Ok(removed)
    }

    pub fn path(&self) -> PathBuf {
        segment_path(&self.dir, self.seq)
    }

    pub fn stats(&self) -> WalStats {
        WalStats {
            segments: segment_sequences(&self.dir).map_or(1, |s| s.len() as u64),
            current_seq: self.seq,
            current_segment_bytes: self.segment_bytes,
            appended_records: self.appended_records,
            appended_bytes: self.appended_bytes,
            unsynced_records: self.unsynced_records,
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer
            .write_all(bytes)
            .map_err(|e| Error::io(format!("writing WAL segment {}", self.path().display()), e))
    }
}

impl Drop for Wal {
    /// Best-effort durability if the process exits without an explicit `sync`.
    fn drop(&mut self) {
        if self.unsynced_records > 0 && !matches!(self.sync, WalSync::Never) {
            let _ = self.writer.flush();
            let _ = self.writer.get_ref().sync_data();
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct WalStats {
    pub segments: u64,
    pub current_seq: u64,
    pub current_segment_bytes: u64,
    pub appended_records: u64,
    pub appended_bytes: u64,
    pub unsynced_records: u64,
}

/// What replay found at the end of the log.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Truncation {
    pub path: PathBuf,
    pub valid_bytes: u64,
    pub discarded_bytes: u64,
    pub reason: TruncationReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncationReason {
    /// The process died between starting and finishing a frame.
    PartialFrame,
    /// The frame was fully sized but its contents did not survive — a partially
    /// persisted write.
    ChecksumMismatch,
}

#[derive(Debug, Default)]
pub struct Replay {
    pub records: u64,
    pub bytes: u64,
    /// `Some` when a torn tail was found and repaired. Surfaced in logs and `/status`;
    /// data loss is never silent.
    pub truncated: Option<Truncation>,
}

/// Replay every segment in `dir`, repairing a torn tail if present.
///
/// `visit` is called for each intact record in append order. Records are streamed
/// rather than collected so replaying a large log does not need it all in memory.
pub fn replay<F>(dir: impl AsRef<Path>, mut visit: F) -> Result<Replay>
where
    F: FnMut(&[u8]) -> Result<()>,
{
    let dir = dir.as_ref();
    let sequences = segment_sequences(dir)?;
    let mut outcome = Replay::default();

    for (index, seq) in sequences.iter().copied().enumerate() {
        let is_last = index + 1 == sequences.len();
        let path = segment_path(dir, seq);
        let mut file = File::open(&path)
            .map_err(|e| Error::io(format!("opening WAL segment {}", path.display()), e))?;
        let total = file
            .metadata()
            .map_err(|e| Error::io(format!("stat {}", path.display()), e))?
            .len();

        verify_header(&mut file, &path)?;
        let mut offset = HEADER_LEN_U64;

        loop {
            match read_frame(&mut file, &path)? {
                Frame::Record { payload, size } => {
                    visit(&payload)?;
                    outcome.records += 1;
                    outcome.bytes += size;
                    offset += size;
                }
                Frame::End => break,
                Frame::Torn(reason) => {
                    // Damage anywhere but the newest segment means something was
                    // appended after a bad record, which the writer cannot do.
                    if !is_last {
                        return Err(Error::WalCorrupt {
                            path,
                            detail: format!(
                                "{reason:?} at offset {offset}, but this is not the newest \
                                 segment — the log is damaged beyond a torn tail"
                            ),
                        });
                    }
                    drop(file);
                    repair_tail(&path, offset)?;
                    outcome.truncated = Some(Truncation {
                        path: path.clone(),
                        valid_bytes: offset,
                        discarded_bytes: total.saturating_sub(offset),
                        reason,
                    });
                    tracing::warn!(
                        path = %path.display(),
                        valid_bytes = offset,
                        discarded_bytes = total.saturating_sub(offset),
                        ?reason,
                        "repaired a torn write-ahead log tail; records after this point \
                         were not durable and have been discarded"
                    );
                    break;
                }
            }
        }
    }

    Ok(outcome)
}

enum Frame {
    Record { payload: Vec<u8>, size: u64 },
    End,
    Torn(TruncationReason),
}

fn read_frame(file: &mut File, path: &Path) -> Result<Frame> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    match read_exact_or_eof(file, &mut header, path)? {
        ReadOutcome::Eof => return Ok(Frame::End),
        ReadOutcome::Partial => return Ok(Frame::Torn(TruncationReason::PartialFrame)),
        ReadOutcome::Full => {}
    }

    let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

    // A wild length is a corrupt header, not a request to allocate 3 GiB.
    if len > MAX_FRAME_LEN {
        return Ok(Frame::Torn(TruncationReason::ChecksumMismatch));
    }

    let mut payload = vec![0u8; len as usize];
    match read_exact_or_eof(file, &mut payload, path)? {
        ReadOutcome::Full => {}
        _ => return Ok(Frame::Torn(TruncationReason::PartialFrame)),
    }

    if crc32fast::hash(&payload) != crc {
        return Ok(Frame::Torn(TruncationReason::ChecksumMismatch));
    }

    Ok(Frame::Record {
        payload,
        size: FRAME_HEADER_LEN as u64 + u64::from(len),
    })
}

enum ReadOutcome {
    Full,
    Partial,
    Eof,
}

fn read_exact_or_eof(file: &mut File, buf: &mut [u8], path: &Path) -> Result<ReadOutcome> {
    if buf.is_empty() {
        return Ok(ReadOutcome::Full);
    }
    let mut filled = 0;
    while filled < buf.len() {
        match file.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(Error::io(format!("reading {}", path.display()), e)),
        }
    }
    Ok(if filled == buf.len() {
        ReadOutcome::Full
    } else if filled == 0 {
        ReadOutcome::Eof
    } else {
        ReadOutcome::Partial
    })
}

fn repair_tail(path: &Path, valid_bytes: u64) -> Result<()> {
    let file = File::options()
        .write(true)
        .open(path)
        .map_err(|e| Error::io(format!("opening {} for repair", path.display()), e))?;
    file.set_len(valid_bytes)
        .map_err(|e| Error::io(format!("truncating {}", path.display()), e))?;
    file.sync_all()
        .map_err(|e| Error::io(format!("syncing {}", path.display()), e))?;
    Ok(())
}

fn verify_header(file: &mut File, path: &Path) -> Result<()> {
    let mut header = [0u8; HEADER_LEN];
    match read_exact_or_eof(file, &mut header, path)? {
        // A zero-length segment is what `open_segment` leaves after creating a file it
        // has not written to yet; nothing to replay.
        ReadOutcome::Eof => {
            file.seek(SeekFrom::End(0))
                .map_err(|e| Error::io(format!("seeking {}", path.display()), e))?;
            return Ok(());
        }
        ReadOutcome::Partial => {
            return Err(Error::WalCorrupt {
                path: path.to_path_buf(),
                detail: "segment header is incomplete".to_owned(),
            });
        }
        ReadOutcome::Full => {}
    }

    if &header[..4] != MAGIC {
        return Err(Error::WalCorrupt {
            path: path.to_path_buf(),
            detail: "missing TDWL magic; this file was not written by telemetryd".to_owned(),
        });
    }
    let format = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if format != FORMAT {
        return Err(Error::WalCorrupt {
            path: path.to_path_buf(),
            detail: format!("segment format v{format}, this build speaks v{FORMAT}"),
        });
    }
    Ok(())
}

fn open_segment(dir: &Path, seq: u64) -> Result<(File, u64)> {
    let path = segment_path(dir, seq);
    let mut file = File::options()
        .create(true)
        .read(true)
        .append(true)
        .open(&path)
        .map_err(|e| Error::io(format!("opening WAL segment {}", path.display()), e))?;

    let len = file
        .metadata()
        .map_err(|e| Error::io(format!("stat {}", path.display()), e))?
        .len();

    if len == 0 {
        let mut header = [0u8; HEADER_LEN];
        header[..4].copy_from_slice(MAGIC);
        header[4..].copy_from_slice(&FORMAT.to_le_bytes());
        file.write_all(&header)
            .map_err(|e| Error::io(format!("writing WAL header {}", path.display()), e))?;
        file.sync_data()
            .map_err(|e| Error::io(format!("syncing {}", path.display()), e))?;
        return Ok((file, HEADER_LEN_U64));
    }

    Ok((file, len))
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:08}.wal"))
}

/// Every segment sequence number in `dir`, ascending.
fn segment_sequences(dir: &Path) -> Result<Vec<u64>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(Error::io(format!("reading {}", dir.display()), e)),
    };

    let mut sequences = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(format!("reading {}", dir.display()), e))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(stem) = name.strip_suffix(".wal")
            && let Ok(seq) = stem.parse::<u64>()
        {
            sequences.push(seq);
        }
    }
    sequences.sort_unstable();
    Ok(sequences)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const BIG: u64 = 1 << 30;

    fn collect(dir: &Path) -> (Vec<Vec<u8>>, Replay) {
        let mut out = Vec::new();
        let replayed = replay(dir, |payload| {
            out.push(payload.to_vec());
            Ok(())
        })
        .unwrap();
        (out, replayed)
    }

    #[test]
    fn records_round_trip_through_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        let mut wal = Wal::open(dir, WalSync::Always, Duration::ZERO, BIG).unwrap();
        for i in 0..100u32 {
            wal.append(format!("record-{i}").as_bytes()).unwrap();
        }
        wal.sync().unwrap();
        drop(wal);

        let (records, replayed) = collect(dir);
        assert_eq!(replayed.records, 100);
        assert!(replayed.truncated.is_none());
        assert_eq!(records[0], b"record-0");
        assert_eq!(records[99], b"record-99");
    }

    #[test]
    fn reopening_appends_rather_than_overwrites() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        let mut wal = Wal::open(dir, WalSync::Always, Duration::ZERO, BIG).unwrap();
        wal.append(b"first").unwrap();
        drop(wal);

        let mut wal = Wal::open(dir, WalSync::Always, Duration::ZERO, BIG).unwrap();
        wal.append(b"second").unwrap();
        drop(wal);

        let (records, _) = collect(dir);
        assert_eq!(records, vec![b"first".to_vec(), b"second".to_vec()]);
    }

    #[test]
    fn empty_log_replays_cleanly() {
        let tmp = tempfile::tempdir().unwrap();
        let (records, replayed) = collect(tmp.path());
        assert!(records.is_empty());
        assert_eq!(replayed.records, 0);

        // A freshly created, never-appended segment is also fine.
        drop(Wal::open(tmp.path(), WalSync::Always, Duration::ZERO, BIG).unwrap());
        let (records, _) = collect(tmp.path());
        assert!(records.is_empty());
    }

    #[test]
    fn rotates_on_size_and_replays_across_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        // Each record is 8 + 16 = 24 bytes; a 64-byte cap holds two per segment.
        let mut wal = Wal::open(dir, WalSync::Always, Duration::ZERO, 64).unwrap();
        for i in 0..10u32 {
            wal.append(format!("{i:016}").as_bytes()).unwrap();
        }
        wal.sync().unwrap();
        assert!(wal.stats().segments > 1, "expected rotation");
        drop(wal);

        let (records, replayed) = collect(dir);
        assert_eq!(replayed.records, 10);
        assert_eq!(records[9], b"0000000000000009");
    }

    #[test]
    fn an_oversized_record_still_gets_written() {
        let tmp = tempfile::tempdir().unwrap();
        // Cap smaller than a single record: it must land, not loop or fail.
        let mut wal = Wal::open(tmp.path(), WalSync::Always, Duration::ZERO, 16).unwrap();
        let payload = vec![7u8; 1024];
        wal.append(&payload).unwrap();
        drop(wal);

        let (records, _) = collect(tmp.path());
        assert_eq!(records, vec![payload]);
    }

    #[test]
    fn a_partial_frame_at_the_tail_is_repaired_and_reported() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        let mut wal = Wal::open(dir, WalSync::Always, Duration::ZERO, BIG).unwrap();
        wal.append(b"durable-one").unwrap();
        wal.append(b"durable-two").unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Simulate a kill mid-frame: a length header with no payload behind it.
        let path = segment_path(dir, 1);
        let good_len = fs::metadata(&path).unwrap().len();
        let mut file = File::options().append(true).open(&path).unwrap();
        file.write_all(&[64, 0, 0, 0, 1, 2, 3, 4]).unwrap();
        file.write_all(b"only some of it").unwrap();
        drop(file);

        let (records, replayed) = collect(dir);
        assert_eq!(
            records,
            vec![b"durable-one".to_vec(), b"durable-two".to_vec()]
        );

        let truncation = replayed.truncated.expect("torn tail should be reported");
        assert_eq!(truncation.reason, TruncationReason::PartialFrame);
        assert_eq!(truncation.valid_bytes, good_len);
        // The repair is real, so a second replay is clean and appends land correctly.
        assert_eq!(fs::metadata(&path).unwrap().len(), good_len);

        let (_, second) = collect(dir);
        assert!(second.truncated.is_none());
    }

    #[test]
    fn a_corrupt_payload_at_the_tail_is_treated_as_a_torn_write() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        let mut wal = Wal::open(dir, WalSync::Always, Duration::ZERO, BIG).unwrap();
        wal.append(b"keep-me").unwrap();
        let good_len = fs::metadata(segment_path(dir, 1)).unwrap().len();
        wal.append(b"tail-record").unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Flip a byte inside the final record's payload.
        let path = segment_path(dir, 1);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let (records, replayed) = collect(dir);
        assert_eq!(records, vec![b"keep-me".to_vec()]);
        assert_eq!(
            replayed.truncated.map(|t| t.reason),
            Some(TruncationReason::ChecksumMismatch)
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), good_len);
    }

    #[test]
    fn damage_in_an_older_segment_is_corruption_not_a_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        let mut wal = Wal::open(dir, WalSync::Always, Duration::ZERO, 40).unwrap();
        for i in 0..6u32 {
            wal.append(format!("{i:016}").as_bytes()).unwrap();
        }
        wal.sync().unwrap();
        assert!(segment_sequences(dir).unwrap().len() > 1);
        drop(wal);

        // Corrupt the *first* segment — nothing could legitimately have been appended
        // after a bad record, so this is not recoverable damage.
        let path = segment_path(dir, 1);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let err = replay(dir, |_| Ok(())).unwrap_err();
        assert!(matches!(err, Error::WalCorrupt { .. }), "got {err:?}");
    }

    #[test]
    fn a_foreign_file_is_rejected_rather_than_misread() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join("00000001.wal"),
            b"this is not a telemetryd WAL",
        )
        .unwrap();

        let err = replay(tmp.path(), |_| Ok(())).unwrap_err();
        match err {
            Error::WalCorrupt { detail, .. } => assert!(detail.contains("magic"), "{detail}"),
            other => panic!("expected corruption, got {other:?}"),
        }
    }

    #[test]
    fn truncate_to_current_drops_only_older_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        let mut wal = Wal::open(dir, WalSync::Always, Duration::ZERO, 40).unwrap();
        for i in 0..8u32 {
            wal.append(format!("{i:016}").as_bytes()).unwrap();
        }
        wal.sync().unwrap();
        let current = wal.seq;
        assert!(current > 1);

        let removed = wal.truncate_to_current().unwrap();
        assert_eq!(removed, current - 1);
        assert_eq!(segment_sequences(dir).unwrap(), vec![current]);

        // The surviving segment is still intact and appendable.
        wal.append(b"after-truncate").unwrap();
        wal.sync().unwrap();
        drop(wal);
        let (records, replayed) = collect(dir);
        assert!(replayed.truncated.is_none());
        assert_eq!(records.last().unwrap(), b"after-truncate");
    }

    #[test]
    fn never_sync_leaves_records_unsynced_but_readable() {
        let tmp = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(tmp.path(), WalSync::Never, Duration::ZERO, BIG).unwrap();
        wal.append(b"x").unwrap();
        assert_eq!(wal.stats().unsynced_records, 1);
        wal.sync().unwrap();
        assert_eq!(wal.stats().unsynced_records, 0);
    }

    #[test]
    fn interval_sync_defers_until_the_interval_elapses() {
        let tmp = tempfile::tempdir().unwrap();
        let mut wal = Wal::open(
            tmp.path(),
            WalSync::Interval,
            Duration::from_secs(3600),
            BIG,
        )
        .unwrap();
        for _ in 0..5 {
            wal.append(b"x").unwrap();
        }
        // Nothing is due yet, so the records are still only in the page cache.
        assert_eq!(wal.stats().unsynced_records, 5);
    }
}
