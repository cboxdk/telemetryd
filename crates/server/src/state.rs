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
    /// Cbox ID token validation. Disabled unless an issuer is configured.
    pub oidc: Arc<crate::oidc::Oidc>,
    /// Relay client credentials, each carrying the app it is allowed to be (ADR-013).
    pub relay_clients: Arc<telemetryd_core::ClientTokens>,
    /// Forwarding upstream. `None` unless `relay.upstream` is set.
    pub relay: Option<Arc<crate::relay::Relay>>,
    /// Ingest requests in flight per client. Only ever holds clients with an active
    /// request, so it is bounded by the queue depth.
    in_flight: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
    queue_depth: usize,
    tail: broadcast::Sender<Arc<LogRecord>>,
}

impl AppState {
    /// Claim a slot for one ingest request, or `None` when the queue is full.
    ///
    /// The permit is held for the whole handler, blocking work included, so this
    /// bounds concurrent *work* rather than concurrent parsing.
    pub fn ingest_slot(&self) -> Option<IngestSlot> {
        self.ingest_slot_for(None)
    }

    /// As [`Self::ingest_slot`], but also bounding one client's share of the queue.
    ///
    /// The global depth alone lets a single client fill it and hand every other client
    /// a `429` — a retry loop shipped to a fleet does precisely that, through a
    /// mechanism working exactly as designed. `relay.max_queue_share` caps how much of
    /// the queue any one identity holds, so the rest always have room.
    ///
    /// The map of active clients cannot grow without bound, and not by construction of
    /// its own: an entry exists only while that client has a request in flight, and
    /// there can never be more of those than there are permits.
    pub fn ingest_slot_for(&self, identity: Option<&str>) -> Option<IngestSlot> {
        let permit = Arc::clone(&self.ingest_permits).try_acquire_owned().ok()?;

        let Some(app) = identity.filter(|_| self.config.relay.is_enabled()) else {
            return Some(IngestSlot {
                _permit: permit,
                app: None,
                in_flight: Arc::clone(&self.in_flight),
            });
        };

        let ceiling = self.config.relay.per_client_slots(self.queue_depth);
        {
            let mut in_flight = self
                .in_flight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let held = in_flight.entry(app.to_owned()).or_insert(0);
            if *held >= ceiling {
                if *held == 0 {
                    in_flight.remove(app);
                }
                self.metrics.incr(
                    "telemetryd_ingest_rejected_total",
                    &[("signal", "any"), ("reason", "client_share")],
                );
                return None;
            }
            *held += 1;
        }

        Some(IngestSlot {
            _permit: permit,
            app: Some(app.to_owned()),
            in_flight: Arc::clone(&self.in_flight),
        })
    }

    pub fn new(config: Arc<Config>, store: Arc<Store>) -> Result<Self> {
        let (tail, _) = broadcast::channel(TAIL_BUFFER);
        let queue_depth = usize::try_from(config.limits.ingest_queue_depth).unwrap_or(usize::MAX);
        let oidc = Arc::new(crate::oidc::Oidc::new(config.auth.oidc.clone()));

        let mut clients = Vec::with_capacity(config.relay.client.len());
        for client in &config.relay.client {
            clients.push((client.token.resolve_digest()?, client.app.clone()));
        }
        let relay_clients = Arc::new(telemetryd_core::ClientTokens::new(clients));

        let relay = config.relay.is_enabled().then(|| {
            Arc::new(crate::relay::Relay::new(
                config.relay.clone(),
                store.data_dir().root(),
            ))
        });
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
            oidc,
            relay_clients,
            relay,
            in_flight: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            queue_depth,
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

/// A claimed ingest slot. Releases the global permit and the client's share on drop,
/// including when the handler returns early or panics — the alternative is a counter
/// that only ever goes up and a client locked out of its own quota forever.
#[derive(Debug)]
pub struct IngestSlot {
    _permit: tokio::sync::OwnedSemaphorePermit,
    app: Option<String>,
    in_flight: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
}

impl Drop for IngestSlot {
    fn drop(&mut self) {
        let Some(app) = &self.app else {
            return;
        };
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(held) = in_flight.get_mut(app) {
            *held = held.saturating_sub(1);
            if *held == 0 {
                in_flight.remove(app);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use telemetryd_core::config::RelayConfig;

    /// The number that decides whether one client can lock the others out.
    #[test]
    fn a_share_never_rounds_down_to_refusing_everything() {
        let mut config = RelayConfig {
            upstream: "https://central.example.com".to_owned(),
            ..RelayConfig::default()
        };

        assert_eq!(config.per_client_slots(8192), 4096, "the default is half");

        // A share small enough to round to zero must still allow one request, or the
        // cap stops being a cap and becomes an outage.
        config.max_queue_share = 0.0001;
        assert_eq!(config.per_client_slots(100), 1);
        config.max_queue_share = 0.0;
        assert_eq!(config.per_client_slots(100), 1);

        // And a negative one, which no validator should have to catch.
        config.max_queue_share = -1.0;
        assert_eq!(config.per_client_slots(100), 1);

        // 1.0 is "off": one client may hold the whole queue, as it could before.
        config.max_queue_share = 1.0;
        assert_eq!(config.per_client_slots(100), 100);
        config.max_queue_share = 2.0;
        assert_eq!(config.per_client_slots(100), 100);
    }
}
