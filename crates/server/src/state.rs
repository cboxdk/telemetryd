//! Shared request state.

use std::sync::Arc;
use std::time::Instant;

use telemetryd_core::{Config, LogRecord, Result, TokenSet};
use telemetryd_store::Store;
use time::OffsetDateTime;
use tokio::sync::broadcast;

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
    pub started: Instant,
    pub started_at: OffsetDateTime,
    tail: broadcast::Sender<Arc<LogRecord>>,
}

impl AppState {
    pub fn new(config: Arc<Config>, store: Arc<Store>) -> Result<Self> {
        let (tail, _) = broadcast::channel(TAIL_BUFFER);
        Ok(Self {
            ingest_tokens: Arc::new(config.auth.ingest_token.resolve()?),
            query_tokens: Arc::new(config.auth.query_token.resolve()?),
            config,
            store,
            metrics: Arc::new(Metrics::new()),
            started: Instant::now(),
            started_at: OffsetDateTime::now_utc(),
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
