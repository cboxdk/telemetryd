//! Shared request state.

use std::sync::Arc;
use std::time::Instant;

use telemetryd_core::{Config, LogRecord, Result, TokenSet};
use telemetryd_store::Store;
use time::OffsetDateTime;
use tokio::sync::{Semaphore, broadcast};

use crate::metrics::Metrics;

/// How many records a live-tail subscriber may fall behind before it starts missing
/// them.
///
/// Bounded on purpose. An unbounded fan-out lets one slow WebSocket client pin every
/// record in memory until the process dies — the storage layer has a disk budget, and
/// this is the same idea applied to the live path. A subscriber that overruns is told
/// it dropped entries rather than being shown an incomplete tail that looks complete.
const TAIL_BUFFER: usize = 1024;

#[derive(Clone, Debug)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Arc<Store>,
    pub metrics: Arc<Metrics>,
    /// Resolved at startup, not per request: a `file:` token indirection should be read
    /// once, and a token that cannot be resolved must stop the process rather than
    /// silently failing every request.
    pub ingest_tokens: Arc<TokenSet>,
    pub query_tokens: Arc<TokenSet>,
    /// Guards `/status` and `/metrics`.
    ///
    /// Falls back to the query tokens when unset, which is what guarded them before
    /// the role existed — so a deployment that never heard of it keeps working.
    pub admin_tokens: Arc<TokenSet>,
    pub started: Instant,
    pub started_at: OffsetDateTime,
    /// Bounds how many ingest requests are in flight at once.
    ///
    /// `limits.ingest_queue_depth` documented itself as "a full queue returns 429 with
    /// Retry-After rather than buffering without bound", and enforced nothing: it was
    /// reported in /status and acted on nowhere. Rejecting is the point — queueing the
    /// excess would be the unbounded buffering the setting exists to prevent, just
    /// moved somewhere less visible.
    ingest_permits: Arc<Semaphore>,
    tail: broadcast::Sender<Arc<LogRecord>>,
}

impl AppState {
    /// Claim a slot for one ingest request, or `None` when the queue is full.
    ///
    /// The permit is held for the whole handler, blocking work included, so this
    /// bounds concurrent *work* rather than concurrent parsing.
    pub fn ingest_slot(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.ingest_permits).try_acquire_owned().ok()
    }

    pub fn new(config: Arc<Config>, store: Arc<Store>) -> Result<Self> {
        let (tail, _) = broadcast::channel(TAIL_BUFFER);
        let queue_depth = usize::try_from(config.limits.ingest_queue_depth).unwrap_or(usize::MAX);
        Ok(Self {
            ingest_tokens: Arc::new(config.auth.ingest_token.resolve()?),
            query_tokens: Arc::new(config.auth.query_token.resolve()?),
            admin_tokens: Arc::new(if config.auth.admin_token.is_empty() {
                config.auth.query_token.resolve()?
            } else {
                config.auth.admin_token.resolve()?
            }),
            config,
            store,
            metrics: Arc::new(Metrics::new()),
            started: Instant::now(),
            started_at: OffsetDateTime::now_utc(),
            ingest_permits: Arc::new(Semaphore::new(queue_depth)),
            tail,
        })
    }

    pub fn uptime_seconds(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }

    /// Fan out newly accepted records to live-tail subscribers.
    ///
    /// Never fails the ingest path: with no subscribers `send` returns an error, and
    /// that is the normal case, not a problem.
    pub fn publish_tail(&self, records: &[LogRecord]) {
        if self.tail.receiver_count() == 0 {
            return;
        }
        for record in records {
            let _ = self.tail.send(Arc::new(record.clone()));
        }
    }

    pub fn subscribe_tail(&self) -> broadcast::Receiver<Arc<LogRecord>> {
        self.tail.subscribe()
    }

    pub fn tail_subscribers(&self) -> usize {
        self.tail.receiver_count()
    }
}
