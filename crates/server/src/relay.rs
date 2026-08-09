//! Forwarding sealed segments to a central instance (ADR-013).
//!
//! # The cursor is over segments, not time
//!
//! The obvious design is a high-water mark on event time, and it is wrong for the
//! clients this exists to serve. A phone buffers while it has no signal and sends when
//! it reconnects, so its records arrive with event times well behind everything already
//! shipped — a time cursor would skip exactly the data offline devices produce.
//!
//! Segments seal in **arrival** order, so a late record lands in whichever segment is
//! open when it turns up. A cursor over segments therefore delivers it. The property
//! was already there; this only uses it.
//!
//! # The cursor advances after delivery, never before
//!
//! Written atomically, and only once upstream has answered 2xx. A cursor that moved
//! first would turn any crash — or any timeout misread as success — into silent data
//! loss, which is the failure mode nobody notices until they go looking for records
//! that were never sent.
//!
//! Delivery is therefore **at-least-once**: a response lost after upstream committed
//! means the segment is sent again. telemetryd has no dedup anywhere, so upstream sees
//! duplicates. That is the honest trade against losing data, and it is stated in
//! ADR-013 rather than discovered in a dashboard showing twice the traffic.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use telemetryd_core::config::RelayConfig;
use telemetryd_core::{Result, Signal};
use telemetryd_ingest::otlp_encode;
use telemetryd_store::Store;

const CURSOR_FILE: &str = "relay-cursor.json";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(120);

/// How far delivery has got, per signal.
///
/// `(created_at_nanos, id)` rather than the id alone: ids are not ordered, and the
/// pair is exactly the key segments are listed by, so "everything up to here" is a
/// comparison rather than a set membership test that would grow without bound.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Position {
    pub created_at_nanos: u64,
    pub id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Cursor {
    #[serde(default)]
    delivered: BTreeMap<String, Position>,
}

/// What a relay looks like from outside, for `/status` and `/metrics`.
///
/// `pending` is the number to watch. Delivered totals only ever go up, so they cannot
/// tell you the shipper has stopped; a backlog that keeps growing is the one signal
/// that says so, and it is what an alert should be written against.
#[derive(Debug, Serialize)]
pub struct RelayStatus {
    pub upstream: String,
    pub trust_client_identity: bool,
    pub when_full: &'static str,
    /// The most of the ingest queue one client may hold. `1.0` means the cap is off.
    pub max_queue_share: f64,
    /// Sealed segments not yet accepted upstream, by signal.
    pub pending: BTreeMap<String, usize>,
    pub segments_delivered: u64,
    pub records_delivered: u64,
    /// Failed delivery attempts since start. Retried, not lost.
    pub failures: u64,
    /// How far each signal has been delivered. Absent means nothing has yet.
    pub delivered_through: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
pub struct RelayStats {
    pub segments_delivered: AtomicU64,
    pub records_delivered: AtomicU64,
    pub failures: AtomicU64,
}

pub struct Relay {
    config: RelayConfig,
    cursor_path: PathBuf,
    cursor: RwLock<Cursor>,
    pub stats: RelayStats,
}

impl std::fmt::Debug for Relay {
    /// Upstream and progress. The token is a `Secret`, and this never touches it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Relay")
            .field("upstream", &self.config.upstream)
            .field("delivered", &self.stats.segments_delivered)
            .finish_non_exhaustive()
    }
}

impl Relay {
    /// Load the cursor, or start from nothing.
    ///
    /// A cursor file that cannot be parsed is treated as absent, which re-sends
    /// everything still on disk. That is the safe direction: duplicates upstream are
    /// recoverable, and a corrupt file read as "already delivered" would silently drop
    /// whatever it claimed.
    pub fn new(config: RelayConfig, data_dir: &Path) -> Self {
        let cursor_path = data_dir.join(CURSOR_FILE);
        let cursor = std::fs::read(&cursor_path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<Cursor>(&raw).ok())
            .unwrap_or_default();
        Self {
            config,
            cursor_path,
            cursor: RwLock::new(cursor),
            stats: RelayStats::default(),
        }
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }

    /// Whether the reaper may delete undelivered data to hold the disk budget.
    #[must_use]
    pub fn drops_when_full(&self) -> bool {
        matches!(
            self.config.when_full,
            telemetryd_core::config::WhenFull::DropOldest
        )
    }

    fn position(&self, signal: Signal) -> Option<Position> {
        self.cursor
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .delivered
            .get(signal.as_str())
            .cloned()
    }

    /// Record a segment as delivered, durably, before returning.
    ///
    /// Temp file plus rename: a crash mid-write leaves either the old cursor or the
    /// new one, never half of either. The old one costs a duplicate; a truncated one
    /// would cost the whole history.
    fn advance(&self, signal: Signal, position: Position) -> Result<()> {
        let snapshot = {
            let mut cursor = self
                .cursor
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cursor
                .delivered
                .insert(signal.as_str().to_owned(), position);
            cursor.clone()
        };

        let encoded = serde_json::to_vec_pretty(&snapshot).map_err(|e| {
            telemetryd_core::Error::RelayDelivery(format!("serialising the relay cursor: {e}"))
        })?;
        let temporary = self.cursor_path.with_extension("json.tmp");
        std::fs::write(&temporary, &encoded).map_err(|e| {
            telemetryd_core::Error::io(format!("writing {}", temporary.display()), e)
        })?;
        std::fs::rename(&temporary, &self.cursor_path)
            .map_err(|e| telemetryd_core::Error::io(format!("renaming {}", temporary.display()), e))
    }

    #[must_use]
    pub fn status(&self, store: &Store) -> RelayStatus {
        let mut pending = BTreeMap::new();
        let mut delivered_through = BTreeMap::new();
        for signal in [Signal::Logs, Signal::Traces, Signal::Metrics] {
            pending.insert(
                signal.as_str().to_owned(),
                self.pending(store, signal).len(),
            );
            if let Some(position) = self.position(signal) {
                delivered_through.insert(signal.as_str().to_owned(), position.id);
            }
        }
        RelayStatus {
            upstream: self.config.upstream.clone(),
            trust_client_identity: self.config.trust_client_identity,
            max_queue_share: self.config.max_queue_share,
            when_full: if self.drops_when_full() {
                "drop_oldest"
            } else {
                "reject"
            },
            pending,
            segments_delivered: self.stats.segments_delivered.load(Ordering::Relaxed),
            records_delivered: self.stats.records_delivered.load(Ordering::Relaxed),
            failures: self.stats.failures.load(Ordering::Relaxed),
            delivered_through,
        }
    }

    /// Sealed segments not yet accepted upstream, by signal.
    #[must_use]
    pub fn backlog(&self, store: &Store) -> Vec<(Signal, usize)> {
        [Signal::Logs, Signal::Traces, Signal::Metrics]
            .into_iter()
            .map(|signal| (signal, self.pending(store, signal).len()))
            .collect()
    }

    /// Segment ids for `signal` that have not been delivered.
    ///
    /// What retention consults before deleting anything: a segment the reaper removes
    /// before it ships is data lost without a trace.
    #[must_use]
    pub fn undelivered(&self, store: &Store, signal: Signal) -> Vec<String> {
        self.pending(store, signal)
            .into_iter()
            .map(|(_, id, _)| id)
            .collect()
    }

    /// `(created_at, id, rows)` for everything after the cursor, oldest first.
    fn pending(&self, store: &Store, signal: Signal) -> Vec<(u64, String, u64)> {
        let after = self.position(signal);
        let mut segments: Vec<(u64, String, u64)> = match signal {
            Signal::Logs => store.logs().segments(),
            Signal::Traces => store.traces().segments(),
            Signal::Metrics => store.metrics().segments(),
        }
        .iter()
        .map(|segment| {
            (
                segment.manifest.created_at_nanos,
                segment.manifest.id.clone(),
                segment.manifest.rows,
            )
        })
        .filter(|(created, id, _)| {
            after.as_ref().is_none_or(|position| {
                (*created, id.as_str()) > (position.created_at_nanos, position.id.as_str())
            })
        })
        .collect();
        segments.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        segments
    }

    /// Ship at most `limit` segments per signal, oldest first.
    ///
    /// Stops at the first failure for that signal rather than skipping ahead: order is
    /// the one property a cursor can express, and a gap in it cannot be represented,
    /// so it must not be created.
    pub fn ship(&self, store: &Store, limit: usize) -> Result<u64> {
        let mut shipped = 0;
        for signal in [Signal::Logs, Signal::Traces, Signal::Metrics] {
            for (created_at_nanos, id, _) in self.pending(store, signal).into_iter().take(limit) {
                match self.deliver(store, signal, &id) {
                    Ok(records) => {
                        self.advance(
                            signal,
                            Position {
                                created_at_nanos,
                                id: id.clone(),
                            },
                        )?;
                        self.stats
                            .segments_delivered
                            .fetch_add(1, Ordering::Relaxed);
                        self.stats
                            .records_delivered
                            .fetch_add(records, Ordering::Relaxed);
                        shipped += 1;
                    }
                    Err(error) => {
                        self.stats.failures.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            signal = signal.as_str(),
                            segment = %id,
                            %error,
                            "could not deliver a segment upstream; will retry"
                        );
                        break;
                    }
                }
            }
        }
        Ok(shipped)
    }

    /// Read one segment, encode it, and post it. Returns the record count.
    fn deliver(&self, store: &Store, signal: Signal, id: &str) -> Result<u64> {
        let (path, body, count) = match signal {
            Signal::Logs => {
                let records = read_segment(store.logs().segments(), id, |segment| {
                    segment.read::<telemetryd_store::LogSchema>()
                })?;
                let count = records.len() as u64;
                ("/v1/logs", otlp_encode::encode_logs(&records), count)
            }
            Signal::Traces => {
                let records = read_segment(store.traces().segments(), id, |segment| {
                    segment.read::<telemetryd_store::SpanSchema>()
                })?;
                let count = records.len() as u64;
                ("/v1/traces", otlp_encode::encode_spans(&records), count)
            }
            Signal::Metrics => {
                let records = read_segment(store.metrics().segments(), id, |segment| {
                    segment.read::<telemetryd_store::MetricSchema>()
                })?;
                let count = records.len() as u64;
                ("/v1/metrics", otlp_encode::encode_metrics(&records), count)
            }
        };

        if count == 0 {
            // Nothing to send, but the cursor must still move or an empty segment
            // blocks every one behind it forever.
            return Ok(0);
        }

        self.post(path, &body)?;
        Ok(count)
    }

    fn post(&self, path: &str, body: &serde_json::Value) -> Result<()> {
        let url = format!("{}{path}", self.config.upstream.trim_end_matches('/'));
        let token = self.config.token.resolve()?;

        let mut request = ureq::post(&url)
            .config()
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_global(Some(TOTAL_TIMEOUT))
            .http_status_as_error(false)
            .build()
            .header("content-type", "application/json");
        if !token.is_empty() {
            request = request.header("authorization", &format!("Bearer {token}"));
        }

        let encoded = serde_json::to_string(body).map_err(|e| {
            telemetryd_core::Error::RelayDelivery(format!("serialising the payload: {e}"))
        })?;
        let mut response = request
            .send(&encoded)
            .map_err(|e| telemetryd_core::Error::RelayDelivery(format!("posting to {url}: {e}")))?;

        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }

        // The body is where an upstream says *why* — an unsupported field, a limit,
        // a rejected token. Dropping it would leave an operator with a bare status
        // code and a segment that never moves.
        let detail = response
            .body_mut()
            .read_to_string()
            .unwrap_or_default()
            .chars()
            .take(400)
            .collect::<String>();
        Err(telemetryd_core::Error::RelayDelivery(format!(
            "{url} answered {status}: {detail}"
        )))
    }
}

/// Find a segment by id and read it, or say which one went missing.
fn read_segment<T, F>(
    segments: Vec<std::sync::Arc<telemetryd_store::Segment>>,
    id: &str,
    read: F,
) -> Result<Vec<T>>
where
    F: Fn(&telemetryd_store::Segment) -> Result<Vec<T>>,
{
    let segment = segments
        .into_iter()
        .find(|segment| segment.manifest.id == id)
        .ok_or_else(|| {
            telemetryd_core::Error::RelayDelivery(format!(
                "segment {id} vanished before it was shipped"
            ))
        })?;
    read(&segment)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn open(dir: &Path) -> Relay {
        Relay::new(
            RelayConfig {
                upstream: "https://central.example.com".to_owned(),
                ..RelayConfig::default()
            },
            dir,
        )
    }

    #[test]
    fn a_cursor_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let relay = open(dir.path());
        relay
            .advance(
                Signal::Logs,
                Position {
                    created_at_nanos: 42,
                    id: "seg-a".into(),
                },
            )
            .unwrap();

        let reopened = open(dir.path());
        assert_eq!(
            reopened.position(Signal::Logs),
            Some(Position {
                created_at_nanos: 42,
                id: "seg-a".into()
            })
        );
        // Signals advance independently: one upstream refusing traces must not stall
        // logs behind it.
        assert_eq!(reopened.position(Signal::Traces), None);
    }

    #[test]
    fn a_damaged_cursor_resends_rather_than_skips() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(CURSOR_FILE), b"{ not json").unwrap();

        // Read as "nothing delivered". Duplicates upstream are recoverable; treating
        // an unreadable file as "all delivered" would drop whatever it covered.
        assert_eq!(open(dir.path()).position(Signal::Logs), None);
    }

    #[test]
    fn advancing_leaves_no_temporary_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        let relay = open(dir.path());
        relay
            .advance(
                Signal::Metrics,
                Position {
                    created_at_nanos: 1,
                    id: "seg-1".into(),
                },
            )
            .unwrap();

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| {
                std::path::Path::new(name)
                    .extension()
                    .is_some_and(|e| e == "tmp")
            })
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }
}
