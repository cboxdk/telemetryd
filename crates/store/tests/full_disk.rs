//! The write-ahead log meeting a genuinely full disk.
//!
//! A full disk cannot be faked convincingly — the interesting failures are a frame cut
//! short part-way and a buffer that flushes only some of what it holds — so this runs
//! against a real, small filesystem named by `TELEMETRYD_FULL_DISK_DIR`, and is skipped
//! without one. CI mounts a 4 MiB tmpfs for it; on macOS:
//!
//! ```text
//! hdiutil create -size 4m -fs HFS+ -volname full full.dmg
//! hdiutil attach full.dmg -mountpoint /tmp/full -nobrowse
//! TELEMETRYD_FULL_DISK_DIR=/tmp/full cargo test -p telemetryd-store --test full_disk
//! ```
//!
//! The promise under test: every batch the store acknowledged is there after a
//! restart, exactly once, and no batch it refused is.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::io::Write;
use std::time::Duration;

use telemetryd_core::config::{Compression, WalSync};
use telemetryd_core::{Labels, LogRecord, Severity};
use telemetryd_store::logs::LogSchema;
use telemetryd_store::records::{RecordStore, StoreSettings};

const BASE: u64 = 1_750_000_000_000_000_000;

fn record(batch: usize, i: usize) -> LogRecord {
    let mut stream = Labels::new();
    stream.insert("app", "full-disk");
    LogRecord {
        timestamp_nanos: BASE + (batch * 100 + i) as u64,
        stream,
        severity: Severity::Info,
        severity_text: "INFO".to_owned(),
        // Incompressible enough that the log grows as fast as the batches do.
        body: format!(
            "batch {batch} line {i} {}",
            "x".repeat(900 + (batch * 7 + i) % 100)
        ),
        attributes: Labels::new(),
        trace_id: None,
        span_id: None,
    }
}

fn open(dir: &std::path::Path, wal_sync: WalSync) -> RecordStore<LogSchema> {
    for sub in ["wal", "segments", "tmp"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    RecordStore::<LogSchema>::open(
        &dir.join("wal"),
        dir.join("segments"),
        dir.join("tmp"),
        StoreSettings {
            segment_duration: Duration::from_secs(3600),
            max_segment_bytes: 1 << 30,
            wal_sync,
            wal_sync_interval: Duration::from_millis(100),
            compression: Compression::Zstd,
            query_parallelism: 1,
        },
    )
    .unwrap()
}

/// Fill the filesystem to within `room` bytes of full, and return the file doing it.
fn leave_room(dir: &std::path::Path, room: u64) -> std::path::PathBuf {
    let filler = dir.join("filler");
    let mut file = std::fs::File::create(&filler).unwrap();
    let chunk = vec![7u8; 64 * 1024];
    let mut written = 0u64;
    while file
        .write_all(&chunk)
        .and_then(|()| file.sync_all())
        .is_ok()
    {
        written += chunk.len() as u64;
    }
    drop(file);
    std::fs::File::options()
        .write(true)
        .open(&filler)
        .unwrap()
        .set_len(written.saturating_sub(room))
        .unwrap();
    filler
}

fn bodies(records: &[LogRecord]) -> Vec<String> {
    records.iter().map(|r| r.body.clone()).collect()
}

fn fill_free_restart(root: &std::path::Path, wal_sync: WalSync) {
    let dir = root.join(format!("{wal_sync:?}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let filler = leave_room(root, 256 * 1024);

    let store = open(&dir, wal_sync);
    let mut acknowledged: BTreeSet<String> = BTreeSet::new();
    let mut refused: BTreeSet<String> = BTreeSet::new();
    let mut batch = 0;
    // Until the disk says no, and a few times after, so a refusal is not a one-off.
    let mut refusals = 0;
    while refusals < 5 {
        let records: Vec<LogRecord> = (0..7).map(|i| record(batch, i)).collect();
        if store.append(&records).is_ok() {
            acknowledged.extend(bodies(&records));
        } else {
            refusals += 1;
            refused.extend(bodies(&records));
        }
        batch += 1;
        assert!(batch < 10_000, "the disk never filled");
    }
    assert!(
        !acknowledged.is_empty(),
        "nothing fitted before the disk filled"
    );

    // Space comes back, and writing carries on where it stopped.
    std::fs::remove_file(&filler).unwrap();
    for _ in 0..20 {
        let records: Vec<LogRecord> = (0..7).map(|i| record(batch, i)).collect();
        store.append(&records).unwrap();
        acknowledged.extend(bodies(&records));
        batch += 1;
    }
    store.sync().unwrap();
    drop(store);

    // What a restart finds.
    let store = open(&dir, wal_sync);
    let found = store.query(0, u64::MAX, &[], &|_| true).unwrap();
    let found_bodies = bodies(&found);
    let distinct: BTreeSet<String> = found_bodies.iter().cloned().collect();
    assert_eq!(
        found_bodies.len(),
        distinct.len(),
        "{wal_sync:?}: a record came back twice"
    );
    let lost: Vec<_> = acknowledged.difference(&distinct).take(3).collect();
    assert!(
        lost.is_empty(),
        "{wal_sync:?}: acknowledged and lost: {lost:?}"
    );
    let resurrected: Vec<_> = refused.intersection(&distinct).take(3).collect();
    assert!(
        resurrected.is_empty(),
        "{wal_sync:?}: refused, yet stored: {resurrected:?}"
    );
    drop(store);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_full_disk_loses_nothing_acknowledged_and_keeps_nothing_refused() {
    let Some(root) = std::env::var_os("TELEMETRYD_FULL_DISK_DIR") else {
        eprintln!("skipped: set TELEMETRYD_FULL_DISK_DIR to a small filesystem to run this");
        return;
    };
    let root = std::path::PathBuf::from(root);
    for wal_sync in [WalSync::Always, WalSync::Interval] {
        fill_free_restart(&root, wal_sync);
    }
}
