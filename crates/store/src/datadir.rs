//! Data directory layout, format versioning, and the single-writer lock.
//!
//! The layout is deliberately inspectable — a user should be able to `ls` their way
//! around it and `rm -rf` a segment without a recovery tool. See ADR-001.
//!
//! ```text
//! telemetryd-data/
//! ├── VERSION
//! ├── LOCK
//! ├── wal/{logs,traces,events,metrics}/NNNNNNNN.wal
//! ├── segments/{logs,traces,events}/<start>-<end>-<id>/
//! ├── metrics/{chunks,labels}/
//! └── tmp/
//! ```

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use telemetryd_core::{Error, Result, STORAGE_FORMAT_VERSION, Signal};

/// An opened data directory. Holds the advisory lock for as long as it lives, so
/// dropping it releases the directory for another process.
#[derive(Debug)]
pub struct DataDir {
    root: PathBuf,
    /// Held for the lifetime of the process. Never read — the lock is released when
    /// the descriptor closes, which is exactly the behaviour we want on a crash.
    _lock: File,
}

impl DataDir {
    /// Create the layout if absent, verify the format version, and take the writer
    /// lock.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)
            .map_err(|e| Error::io(format!("creating data directory {}", root.display()), e))?;

        // Lock before touching anything else, so two concurrent starts cannot both
        // decide the directory is empty and race on VERSION.
        let lock = acquire_lock(&root)?;
        check_or_write_version(&root)?;
        create_layout(&root)?;

        Ok(Self { root, _lock: lock })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn wal_dir(&self, signal: Signal) -> PathBuf {
        self.root.join("wal").join(signal.as_str())
    }

    pub fn segments_dir(&self, signal: Signal) -> PathBuf {
        self.root.join("segments").join(signal.as_str())
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }

    /// Total bytes on disk, and a per-subtree breakdown for `/status`.
    ///
    /// Walks the tree rather than tracking a counter: the reaper, a crash, and an
    /// operator with `rm` can all change usage behind our back, and the budget is only
    /// meaningful if it reflects what is actually there.
    pub fn usage(&self) -> Result<DiskUsage> {
        let mut usage = DiskUsage::default();
        for signal in Signal::ALL {
            usage.wal_bytes += dir_size(&self.wal_dir(signal))?;
            if signal.uses_record_store() {
                usage.segment_bytes += dir_size(&self.segments_dir(signal))?;
            }
        }
        usage.metric_bytes = dir_size(&self.root.join("metrics"))?;
        usage.tmp_bytes = dir_size(&self.tmp_dir())?;
        Ok(usage)
    }

    /// Remove anything left in `tmp/` by a crash mid-seal. Safe to call at any time:
    /// segments become visible via `rename(2)`, so nothing in `tmp/` is ever
    /// referenced by the catalogue.
    pub fn clean_tmp(&self) -> Result<u64> {
        let tmp = self.tmp_dir();
        let mut removed = 0;
        let entries = match fs::read_dir(&tmp) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(Error::io(format!("reading {}", tmp.display()), e)),
        };
        for entry in entries {
            let entry = entry.map_err(|e| Error::io(format!("reading {}", tmp.display()), e))?;
            let path = entry.path();
            let result = if path.is_dir() {
                fs::remove_dir_all(&path)
            } else {
                fs::remove_file(&path)
            };
            if let Err(e) = result {
                tracing::warn!(path = %path.display(), error = %e, "could not clean tmp entry");
            } else {
                removed += 1;
            }
        }
        if removed > 0 {
            tracing::info!(
                removed,
                "cleaned incomplete segments left by a previous run"
            );
        }
        Ok(removed)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct DiskUsage {
    pub wal_bytes: u64,
    pub segment_bytes: u64,
    pub metric_bytes: u64,
    pub tmp_bytes: u64,
}

impl DiskUsage {
    pub fn total(self) -> u64 {
        self.wal_bytes + self.segment_bytes + self.metric_bytes + self.tmp_bytes
    }
}

/// Take the exclusive advisory lock that makes "one writer per data directory" a
/// checked invariant rather than a convention.
///
/// Uses `std::fs::File::try_lock` (stable since 1.89) rather than a crate — the OS
/// primitive is the whole feature, and this project's dependency budget is a product
/// constraint, not an aesthetic one.
fn acquire_lock(root: &Path) -> Result<File> {
    use std::fs::TryLockError;

    let path = root.join("LOCK");
    let file = File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| Error::io(format!("opening {}", path.display()), e))?;

    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(Error::DataDirLocked {
            path: root.to_path_buf(),
        }),
        Err(TryLockError::Error(e)) => Err(Error::io(format!("locking {}", path.display()), e)),
    }
}

fn check_or_write_version(root: &Path) -> Result<()> {
    let path = root.join("VERSION");
    match fs::read_to_string(&path) {
        Ok(contents) => {
            let found: u32 = contents.trim().parse().map_err(|_| Error::WalCorrupt {
                path: path.clone(),
                detail: format!(
                    "VERSION file does not contain a number: {:?}",
                    contents.trim()
                ),
            })?;
            if found == STORAGE_FORMAT_VERSION {
                Ok(())
            } else {
                // Refuse rather than guess. Silently reading a directory written by a
                // different format is how you get corrupt results that look fine.
                Err(Error::StorageVersionMismatch {
                    path: root.to_path_buf(),
                    found,
                    expected: STORAGE_FORMAT_VERSION,
                })
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut file = File::create(&path)
                .map_err(|e| Error::io(format!("creating {}", path.display()), e))?;
            writeln!(file, "{STORAGE_FORMAT_VERSION}")
                .map_err(|e| Error::io(format!("writing {}", path.display()), e))?;
            file.sync_all()
                .map_err(|e| Error::io(format!("syncing {}", path.display()), e))?;
            Ok(())
        }
        Err(e) => Err(Error::io(format!("reading {}", path.display()), e)),
    }
}

fn create_layout(root: &Path) -> Result<()> {
    let mut dirs = vec![
        root.join("tmp"),
        root.join("metrics").join("chunks"),
        root.join("metrics").join("labels"),
    ];
    for signal in Signal::ALL {
        dirs.push(root.join("wal").join(signal.as_str()));
        if signal.uses_record_store() {
            dirs.push(root.join("segments").join(signal.as_str()));
        }
    }
    for dir in dirs {
        fs::create_dir_all(&dir)
            .map_err(|e| Error::io(format!("creating {}", dir.display()), e))?;
    }
    Ok(())
}

fn dir_size(path: &Path) -> Result<u64> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(Error::io(format!("reading {}", path.display()), e)),
    };
    let mut total = 0;
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(format!("reading {}", path.display()), e))?;
        let meta = match entry.metadata() {
            Ok(meta) => meta,
            // A file the reaper deleted mid-walk is not an error.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(Error::io(format!("stat {}", entry.path().display()), e)),
        };
        total += if meta.is_dir() {
            dir_size(&entry.path())?
        } else {
            meta.len()
        };
    }
    Ok(total)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn creates_the_full_layout_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("data");

        let dir = DataDir::open(&root).unwrap();
        assert!(root.join("VERSION").is_file());
        assert!(root.join("tmp").is_dir());
        assert!(root.join("wal/logs").is_dir());
        assert!(root.join("wal/metrics").is_dir());
        assert!(root.join("segments/traces").is_dir());
        // Metrics do not use the record store, so they get no segments directory.
        assert!(!root.join("segments/metrics").exists());
        drop(dir);

        // Reopening an existing directory must not fail or reset anything.
        let _dir = DataDir::open(&root).unwrap();
    }

    #[test]
    fn refuses_a_directory_written_by_another_format_version() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("data");
        drop(DataDir::open(&root).unwrap());

        fs::write(root.join("VERSION"), "999\n").unwrap();
        let err = DataDir::open(&root).unwrap_err();
        match err {
            Error::StorageVersionMismatch {
                found, expected, ..
            } => {
                assert_eq!(found, 999);
                assert_eq!(expected, STORAGE_FORMAT_VERSION);
            }
            other => panic!("expected a version mismatch, got {other:?}"),
        }
    }

    #[test]
    fn second_process_cannot_open_the_same_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("data");

        let first = DataDir::open(&root).unwrap();
        let err = DataDir::open(&root).unwrap_err();
        assert!(matches!(err, Error::DataDirLocked { .. }), "got {err:?}");

        // Releasing the lock makes the directory available again.
        drop(first);
        let _second = DataDir::open(&root).unwrap();
    }

    #[test]
    fn usage_accounts_for_every_subtree() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(tmp.path().join("data")).unwrap();
        assert_eq!(dir.usage().unwrap().total(), 0);

        fs::write(
            dir.wal_dir(Signal::Logs).join("00000001.wal"),
            vec![0u8; 128],
        )
        .unwrap();
        let nested = dir.segments_dir(Signal::Logs).join("seg-1");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("data.parquet"), vec![0u8; 256]).unwrap();

        let usage = dir.usage().unwrap();
        assert_eq!(usage.wal_bytes, 128);
        assert_eq!(usage.segment_bytes, 256);
        assert_eq!(usage.total(), 384);
    }

    #[test]
    fn clean_tmp_removes_incomplete_segments_from_a_crash() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(tmp.path().join("data")).unwrap();

        let partial = dir.tmp_dir().join("seg-in-progress");
        fs::create_dir_all(&partial).unwrap();
        fs::write(partial.join("data.parquet"), b"half written").unwrap();
        fs::write(dir.tmp_dir().join("stray"), b"x").unwrap();

        assert_eq!(dir.clean_tmp().unwrap(), 2);
        assert_eq!(dir.usage().unwrap().tmp_bytes, 0);
        assert_eq!(dir.clean_tmp().unwrap(), 0);
    }
}
