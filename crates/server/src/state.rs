//! Shared request state.

use std::sync::Arc;
use std::time::{Duration, Instant};

use telemetryd_core::{Config, Error, LogRecord, Result, TokenSet};
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

/// How many live tails may be open at once.
///
/// A tail holds its WebSocket for as long as the client likes and runs its query against
/// every record ingested, so each one is a standing cost on the write path. They were
/// unbounded and outside the query limiter: two thousand of them turned every ingested
/// line into two thousand pipeline evaluations. Sixty-four is far above what a team
/// watches at once.
pub const MAX_TAILS: usize = 64;

#[derive(Clone, Debug)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Arc<Store>,
    pub metrics: Arc<Metrics>,
    /// The static tokens and relay client credentials, swapped whole on `SIGHUP`.
    ///
    /// Resolved at startup and on reload, not per request: a `file:` token indirection is
    /// read then, and a token that cannot be resolved stops startup or fails the reload
    /// rather than failing every request. Behind a lock because a reload replaces them:
    /// they were fixed for the life of the process, so a leaked token removed from the
    /// file and reloaded kept working until someone thought to restart.
    credentials: Arc<std::sync::RwLock<Arc<Credentials>>>,
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
    /// The read side's equivalent of `ingest_permits`, and separate from it: a burst of
    /// dashboards must not be able to shut out writes, and a write burst must not close
    /// the API you would use to look at it.
    query_permits: Arc<Semaphore>,
    /// Counted apart from queries because one export costs roughly twenty-five of them.
    export_permits: Arc<Semaphore>,
    query_concurrency: usize,
    export_concurrency: usize,
    /// Cbox ID token validation. Disabled unless an issuer is configured.
    pub oidc: Arc<crate::oidc::Oidc>,
    /// Forwarding upstream. `None` unless `relay.upstream` is set.
    pub relay: Option<Arc<crate::relay::Relay>>,
    /// Ingest requests in flight per client. Only ever holds clients with an active
    /// request, so it is bounded by the queue depth.
    in_flight: Arc<std::sync::Mutex<std::collections::HashMap<String, usize>>>,
    queue_depth: usize,
    tail: broadcast::Sender<Arc<LogRecord>>,
    tail_permits: Arc<Semaphore>,
}

/// Every static credential the server checks, resolved from one configuration.
#[derive(Debug, PartialEq, Eq)]
pub struct Credentials {
    pub ingest: TokenSet,
    pub query: TokenSet,
    /// Guards `/status` and `/metrics`.
    ///
    /// Falls back to the query tokens when unset, which is what guarded them before
    /// the role existed — so a deployment that never heard of it keeps working.
    pub admin: TokenSet,
    /// Relay client credentials, each carrying the app it is allowed to be.
    pub relay_clients: telemetryd_core::ClientTokens,
}

impl Credentials {
    /// Read every token the configuration names, following `file:` indirections.
    pub fn resolve(config: &Config) -> Result<Self> {
        let mut clients = Vec::with_capacity(config.relay.client.len());
        for client in &config.relay.client {
            clients.push((client.token.resolve_digest()?, client.app.clone()));
        }
        Ok(Self {
            ingest: config.auth.ingest_token.resolve()?,
            query: config.auth.query_token.resolve()?,
            admin: if config.auth.admin_token.is_empty() {
                config.auth.query_token.resolve()?
            } else {
                config.auth.admin_token.resolve()?
            },
            relay_clients: telemetryd_core::ClientTokens::new(clients),
        })
    }

    /// Which credentials differ from `other`, by name and never by value.
    #[must_use]
    pub fn changed_from(&self, other: &Self) -> Vec<&'static str> {
        let mut changed = Vec::new();
        if self.ingest != other.ingest {
            changed.push("auth.ingest_token");
        }
        if self.query != other.query {
            changed.push("auth.query_token");
        }
        if self.admin != other.admin {
            changed.push("auth.admin_token");
        }
        if self.relay_clients != other.relay_clients {
            changed.push("relay.client");
        }
        changed
    }
}

impl AppState {
    /// The credentials in force now. A request holds the snapshot it started with, so a
    /// reload never changes the rules halfway through one.
    #[must_use]
    pub fn credentials(&self) -> Arc<Credentials> {
        Arc::clone(
            &self
                .credentials
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Whether some surface answers without any credential — the default instance.
    ///
    /// The same rule the guard applies: admin falls back to the query tokens, relay
    /// clients guard ingest, and Cbox ID guards everything.
    #[must_use]
    pub fn any_surface_open(&self) -> bool {
        if self.oidc.is_enabled() {
            return false;
        }
        let credentials = self.credentials();
        (credentials.ingest.is_empty() && credentials.relay_clients.is_empty())
            || credentials.query.is_empty()
            || credentials.admin.is_empty()
    }

    /// Put new credentials in force, returning which changed. Takes effect for the
    /// next request; one already admitted finishes under the old.
    pub fn replace_credentials(&self, fresh: Credentials) -> Vec<&'static str> {
        let mut current = self
            .credentials
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let changed = fresh.changed_from(current.as_ref());
        if !changed.is_empty() {
            *current = Arc::new(fresh);
        }
        changed
    }

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
        // Resolved once, here: `0` means "size it from the memory this process is allowed
        // to use", and that answer must not change under the instance while it runs.
        let query_concurrency =
            usize::try_from(config.limits.resolved_query_concurrency()).unwrap_or(64);
        let export_concurrency =
            usize::try_from(config.limits.resolved_export_concurrency()).unwrap_or(4);
        let oidc = Arc::new(crate::oidc::Oidc::new(config.auth.oidc.clone()));

        let credentials = Arc::new(std::sync::RwLock::new(Arc::new(Credentials::resolve(
            &config,
        )?)));

        let relay = config.relay.is_enabled().then(|| {
            Arc::new(crate::relay::Relay::new(
                config.relay.clone(),
                store.data_dir().root(),
            ))
        });
        Ok(Self {
            credentials,
            config,
            store,
            metrics: Arc::new(Metrics::new()),
            started: Instant::now(),
            started_at: OffsetDateTime::now_utc(),
            ingest_permits: Arc::new(Semaphore::new(queue_depth)),
            query_permits: Arc::new(Semaphore::new(query_concurrency)),
            export_permits: Arc::new(Semaphore::new(export_concurrency)),
            query_concurrency,
            export_concurrency,
            oidc,
            relay,
            in_flight: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            queue_depth,
            tail,
            tail_permits: Arc::new(Semaphore::new(MAX_TAILS)),
        })
    }

    /// Claim a slot for a read request, waiting briefly before giving up.
    ///
    /// # Why this waits at all
    ///
    /// It used to be `try_acquire`, refusing the moment every slot was busy, and the
    /// reasoning was that a waiting caller holds a connection and its share of a proxy's
    /// worker while it does so — while a `429` with `Retry-After` is something a client
    /// can act on.
    ///
    /// The second half turned out to be wrong about the client that matters. A dashboard
    /// opens all of its panels at once: a hundred and twenty-four queries arrive in one
    /// burst, sixteen run, and the rest were told to come back — which the dashboard
    /// renders as a hundred and eight errors. Nothing there acts on `Retry-After`; there
    /// is no queue on the client side to hold the work.
    ///
    /// A burst is exactly what a short wait absorbs. Queries against a warm store finish
    /// in tens of milliseconds, so a few seconds of queueing drains a burst many times
    /// that size, and the caller sees a slower panel instead of a broken one. Sustained
    /// overload still refuses, because the wait is bounded — the difference is that
    /// refusing now means "this instance is genuinely saturated" rather than "two requests
    /// arrived at the same moment".
    ///
    /// Bounded well under `server.request_timeout` so a queued request still has time to
    /// run: waiting until the timeout would trade a `429` for a `408`, which is not an
    /// improvement.
    pub async fn query_slot(&self, wait: Duration) -> Option<tokio::sync::OwnedSemaphorePermit> {
        // Fast path first: an uncontended slot must not pay for a timer.
        if let Ok(permit) = Arc::clone(&self.query_permits).try_acquire_owned() {
            return Some(permit);
        }
        tokio::time::timeout(wait, Arc::clone(&self.query_permits).acquire_owned())
            .await
            .ok()?
            .ok()
    }

    /// Record a query this instance refused, so the server keeps a trace of it.
    ///
    /// # Why this exists
    ///
    /// A refused query used to leave nothing behind. The client got a `400` with a
    /// perfectly good explanation, and the server kept no record that it had happened —
    /// so an operator looking at their own instance could not tell which query had failed,
    /// or that any had. Diagnosing one meant packet-capturing loopback traffic to read a
    /// request the server had already parsed and rejected.
    ///
    /// Ingest has had this from the start: every rejected record is counted by reason,
    /// named in the response and logged. The read side had the response and nothing else.
    ///
    /// `WARN`, because a refusal means someone is looking at a broken panel right now. The
    /// expression is included because the reason is meaningless without it, and truncated
    /// because a query is caller-controlled input.
    pub fn refused_query(&self, surface: &'static str, query: &str, error: &Error) {
        const MAX_LOGGED: usize = 400;
        let reason = error.code();
        let shown: String = query.chars().take(MAX_LOGGED).collect();
        tracing::warn!(
            surface,
            reason,
            query = %shown,
            truncated = query.chars().count() > MAX_LOGGED,
            "refused a query: {error}"
        );
        self.metrics.incr(
            "telemetryd_query_rejected_total",
            &[("surface", surface), ("reason", reason)],
        );
    }

    /// How long a read request will wait for a slot before being refused.
    ///
    /// Half the request timeout, so a request that waits still has the other half to run
    /// in. Not separately configurable: the number that matters is how long a client is
    /// willing to wait in total, and that is `server.request_timeout`.
    #[must_use]
    pub fn query_wait(&self) -> Duration {
        self.config.server.request_timeout / 2
    }

    pub fn export_slot(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.export_permits).try_acquire_owned().ok()
    }

    /// In force after `0` was resolved, for `/status` and `/metrics`.
    pub fn query_concurrency(&self) -> usize {
        self.query_concurrency
    }

    pub fn export_concurrency(&self) -> usize {
        self.export_concurrency
    }

    /// How many of each are running right now.
    pub fn queries_in_flight(&self) -> usize {
        self.query_concurrency
            .saturating_sub(self.query_permits.available_permits())
    }

    pub fn exports_in_flight(&self) -> usize {
        self.export_concurrency
            .saturating_sub(self.export_permits.available_permits())
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

    /// A slot for one live tail, or `None` when [`MAX_TAILS`] are open. Held for the life
    /// of the WebSocket.
    pub fn tail_slot(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.tail_permits).try_acquire_owned().ok()
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
