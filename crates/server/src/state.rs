//! Shared request state.

use std::sync::Arc;
use std::time::Instant;

use telemetryd_core::{Config, Result, TokenSet};
use telemetryd_store::Store;
use time::OffsetDateTime;

use crate::metrics::Metrics;

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
}

impl AppState {
    pub fn new(config: Arc<Config>, store: Arc<Store>) -> Result<Self> {
        Ok(Self {
            ingest_tokens: Arc::new(config.auth.ingest_token.resolve()?),
            query_tokens: Arc::new(config.auth.query_token.resolve()?),
            config,
            store,
            metrics: Arc::new(Metrics::new()),
            started: Instant::now(),
            started_at: OffsetDateTime::now_utc(),
        })
    }

    pub fn uptime_seconds(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }
}
