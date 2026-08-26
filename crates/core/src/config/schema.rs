//! The shape of `telemetryd.toml`: every section, its defaults, and the small
//! accessors that answer "is this configured" once instead of at each call site.
//!
//! Split out when `config.rs` passed 1,600 lines doing the schema, the environment
//! table, loading, validation and the operator messages at once. Everything here is
//! re-exported from the parent, so `telemetryd_core::config::X` is unchanged and this
//! is a move rather than an API change.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use bytesize::ByteSize;
use serde::{Deserialize, Serialize};

use crate::secret::{Secret, TokenSpecs};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub storage: StorageConfig,
    pub retention: RetentionConfig,
    pub limits: LimitsConfig,
    pub ingest: IngestConfig,
    pub log: LogConfig,
    /// Forwarding to a central instance. Off unless `upstream` is set.
    #[serde(default)]
    pub relay: RelayConfig,
    /// Who telemetryd trusts when it dials out.
    #[serde(default)]
    pub tls: TlsConfig,
}

/// Terminating TLS ourselves, for deployments with nowhere to put a proxy.
///
/// Unset means plain HTTP, which stays the default: anything at a public edge usually
/// has an ingress already, and two TLS implementations in one path is a second place
/// for policy to be wrong. Set it where there is no proxy and the traffic still
/// deserves encrypting — an internal relay, a container talking to a container.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ServerTlsConfig {
    /// PEM certificate chain, leaf first.
    #[serde(deserialize_with = "path_or_none")]
    pub cert_file: Option<PathBuf>,
    /// PEM private key, unencrypted — telemetryd cannot prompt at startup.
    #[serde(deserialize_with = "path_or_none")]
    pub key_file: Option<PathBuf>,
    /// Generate a self-signed certificate for these names, if none exists yet.
    ///
    /// A comma-separated list of the hostnames clients will connect as, e.g.
    /// `"telemetry.internal"`. Empty disables it. `localhost`, `127.0.0.1` and `::1`
    /// are always included, so a local test needs no list of its own.
    ///
    /// It is the names rather than a plain on/off switch because a certificate valid
    /// only for `localhost` is useless the moment a client connects by hostname — and
    /// that failure arrives as an opaque verification error, long after the setting
    /// that caused it.
    ///
    /// **This encrypts; it does not authenticate.** Clients have no way to know the
    /// certificate is yours, so they must be told to skip verification — and that
    /// setting outlives the certificate, leaving the deployment open to an active
    /// attacker even after a real one is installed. Worth it against passive capture,
    /// which is a real threat; not a substitute for an authority the clients trust.
    pub self_signed: String,
}

impl ServerTlsConfig {
    /// Whether TLS termination is switched on, by either route.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.cert_file.is_some() || self.key_file.is_some() || self.is_self_signed()
    }

    /// Whether telemetryd should generate its own certificate.
    #[must_use]
    pub fn is_self_signed(&self) -> bool {
        !self.self_signed.trim().is_empty()
    }

    /// How this instance terminates TLS, as one word for an operator or a label.
    ///
    /// Reported rather than inferred at each site: `/status` and `/metrics` answering
    /// the same question differently is worse than neither answering it.
    #[must_use]
    pub fn posture(&self) -> &'static str {
        if self.is_self_signed() {
            "self-signed"
        } else if self.is_enabled() {
            "certificate"
        } else {
            "off"
        }
    }

    /// The names to put in the generated certificate, loopback included.
    #[must_use]
    pub fn self_signed_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .self_signed
            .split(',')
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty())
            .collect();
        for local in ["localhost", "127.0.0.1", "::1"] {
            if !names.iter().any(|name| name == local) {
                names.push(local.to_owned());
            }
        }
        names
    }
}

/// Trust for outbound connections — the OIDC key fetch, relay shipping, transfer.
///
/// Inbound TLS is still terminated by a reverse proxy; this is only about
/// connections telemetryd makes.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct TlsConfig {
    /// A PEM bundle of certificate authorities to trust instead of the built-in set.
    ///
    /// Empty uses the roots compiled into the binary, which is right for the public
    /// internet. Set it when the issuer or the relay upstream is behind a private CA —
    /// the case relay mode is built for, where the upstream is internal infrastructure.
    ///
    /// **It replaces the built-in roots rather than adding to them.** Trusting exactly
    /// the authority that signs your internal hosts is tighter than trusting it *and*
    /// every public CA, and an instance configured this way is usually talking only to
    /// internal infrastructure. If you genuinely need both, the file is a bundle:
    /// concatenate the public roots you want alongside your own.
    #[serde(deserialize_with = "path_or_none")]
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngestConfig {
    /// Resource attributes promoted to *stream* labels, in addition to `app` and
    /// `level` which are always present.
    ///
    /// This list is the cardinality contract. Promoting every resource attribute
    /// would be the friendly-looking default and a trap: attributes like `host.id`,
    /// `process.pid` or `container.id` change per deploy or per process and would
    /// multiply streams without bound. Anything not listed here is still stored and
    /// still queryable through label filters — it just does not create a new stream.
    pub stream_labels: Vec<String>,

    /// Truncate an over-long log body instead of rejecting the record.
    ///
    /// Default is to truncate: a 300 KiB stack trace is usually the single most
    /// interesting line of the day, and dropping it entirely to enforce a size cap
    /// loses exactly the record someone is looking for. The truncation is marked in
    /// the body and counted, so it is never invisible.
    pub truncate_oversized_bodies: bool,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            stream_labels: [
                "service_name",
                "service_namespace",
                "service_version",
                "deployment_environment",
                "deployment_environment_name",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            truncate_oversized_bodies: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// One port serves ingest, query and the UI-facing APIs.
    pub listen: SocketAddr,
    /// Permit a non-loopback bind with no token configured.
    pub insecure: bool,
    /// Terminate TLS here rather than at a proxy. Unset = plain HTTP.
    #[serde(default)]
    pub tls: ServerTlsConfig,
    pub max_body_bytes: ByteSize,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub shutdown_grace: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 4319)),
            insecure: false,
            tls: ServerTlsConfig::default(),
            max_body_bytes: ByteSize::mib(16),
            request_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    /// **Write.** Guards `/v1/*` and `/api/v1/write`.
    pub ingest_token: TokenSpecs,
    /// **Read.** Guards the Prometheus, Loki and Tempo read APIs.
    pub query_token: TokenSpecs,
    /// **Admin.** Guards `/status` and `/metrics`.
    ///
    /// Separate from `query_token` because the two answer different questions.
    /// Reading telemetry tells you about your applications; `/status` and `/metrics`
    /// tell you about the *deployment* — every app name, its series count, its share
    /// of the disk, whether the instance is running unauthenticated. Handing that to
    /// everyone who may read logs is more than is usually meant.
    ///
    /// **Unset means `query_token` guards them**, which is what it did before this
    /// existed, so adding the role breaks no deployment. Setting it is opting into
    /// the tighter split.
    pub admin_token: TokenSpecs,
    /// Accept Cbox ID access tokens alongside the static ones.
    ///
    /// Unset means static tokens only, which is a complete answer for one team on one
    /// host. Setting an issuer turns it on.
    #[serde(default)]
    pub oidc: OidcConfig,
}

/// One client application allowed to write through a relay.
///
/// The token identifies the app; the app is not taken from the payload. That is the
/// whole point.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RelayClient {
    /// The `app` label every record from this credential is stamped with.
    pub app: String,
    /// Its ingest credential. Accepts `file:` and `env:` indirection like any other.
    pub token: Secret,
}

/// What to do when the series budget is full of series that are all still active.
///
/// Named like [`WhenFull`] and deliberately not merged with it: they answer the same
/// question about different resources, and the right answer differs — see
/// `LimitsConfig::when_series_full` for why a ring buffer of series is not the same
/// trade as a ring buffer of records.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WhenSeriesFull {
    /// Refuse the new series, loudly. The admitted set stays complete and stable.
    #[default]
    Refuse,
    /// Drop the least recently written series so the new one fits.
    EvictOldest,
}

/// What to do when the disk budget cannot be held because undelivered data is
/// protected from the reaper.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WhenFull {
    /// Keep accepting; lose the oldest undelivered telemetry.
    ///
    /// The default, because a relay in front of clients that cannot buffer should stay
    /// available — a phone has nowhere to put what we refuse.
    #[default]
    DropOldest,
    /// Stop accepting with `429`, and let clients that *can* buffer hold it.
    Reject,
}

/// Forwarding to a central instance, as a safe front door for untrusted clients.
///
/// Unset means telemetryd stores what it receives and sends nothing onward, which is
/// what it has always done.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct RelayConfig {
    /// Base URL of the instance that receives what this one accepts. Empty = off.
    pub upstream: String,
    /// Credential for `upstream`. This is the one the clients must never hold.
    pub token: Secret,
    /// Take the `app` label from the payload instead of from the credential.
    ///
    /// `false` in relay mode, and that is the security boundary: a client that picks
    /// its own `app` can impersonate any other, and everything downstream — alerts,
    /// dashboards, retention — is keyed on it.
    pub trust_client_identity: bool,
    /// What to do when undelivered data fills the disk budget.
    pub when_full: WhenFull,
    /// How often to look for sealed segments to ship.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// The largest request this relay will send upstream.
    ///
    /// A segment is far bigger than any receiver will accept in one request — the
    /// defaults are a 256 MiB segment against a 16 MiB body limit — so segments are
    /// split into requests of at most this size. Well under the default limit on
    /// purpose: a receiver whose own ceiling is lower still works, because a `413`
    /// halves the batch and retries rather than stalling delivery.
    pub max_request_bytes: ByteSize,
    /// The most of the ingest queue any one client may hold at once, as a fraction.
    ///
    /// `limits.ingest_queue_depth` is global, so without this one client can fill it
    /// and every other client gets `429` — a bad app version shipped to a fleet does
    /// exactly that, through a mechanism working as designed.
    ///
    /// A *share* rather than a request rate, because the identity here is the
    /// application and not the device: a million phones present one credential, so a
    /// requests-per-second ceiling would have to be guessed against fleet size and
    /// would throttle the whole fleet. A share needs no such number and scales with
    /// `ingest_queue_depth` on its own.
    ///
    /// `1.0` disables it. Below `1.0`, no client can lock the others out.
    pub max_queue_share: f64,
    /// Per-client ingest credentials, each bound to an app name.
    pub client: Vec<RelayClient>,
}

impl Default for RelayConfig {
    /// Spelled out rather than derived. `Default` would give a zero queue share —
    /// which rounds to one in-flight request per client — and a zero interval, and
    /// both would then need a second, hidden default at the point of use.
    fn default() -> Self {
        Self {
            upstream: String::new(),
            token: Secret::default(),
            trust_client_identity: false,
            when_full: WhenFull::default(),
            interval: Duration::from_secs(30),
            max_request_bytes: ByteSize::mib(4),
            max_queue_share: 0.5,
            client: Vec::new(),
        }
    }
}

impl RelayConfig {
    /// In-flight requests one client may hold, given the global queue depth.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn per_client_slots(&self, queue_depth: usize) -> usize {
        if self.max_queue_share >= 1.0 {
            return queue_depth;
        }
        // At least one, always: a share small enough to round to zero would refuse
        // every request rather than merely bounding one client's share of them.
        ((queue_depth as f64) * self.max_queue_share.max(0.0))
            .floor()
            .max(1.0) as usize
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !self.upstream.trim().is_empty()
    }
}

/// Validating tokens issued by a Cbox ID instance.
///
/// Tokens are checked **locally** against the issuer's published keys. telemetryd
/// never asks the provider about a token, because it is the thing you open when
/// something is broken: if the identity provider is unreachable, introspection would
/// mean you cannot read the logs that would tell you why. Keys are cached, so a
/// provider that goes down stops new tokens being *issued* while every token already
/// in hand keeps working.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct OidcConfig {
    /// The issuer URL, e.g. `https://id.example.com`. Empty disables the whole thing.
    ///
    /// Must match the token's `iss` exactly — it is what ties a token to the provider
    /// you meant rather than any provider.
    pub issuer: String,

    /// The `aud` a token must carry.
    ///
    /// Cbox ID always sets one (RFC 9068 §2.2 requires it): the requested resource, or
    /// the issuer itself when no resource was named. Leaving this empty accepts the
    /// issuer's own value, which is right for a single telemetryd. Set it when several
    /// resource servers share an issuer, so a token minted for one cannot be replayed
    /// at another.
    pub audience: String,

    /// Where the signing keys are published.
    ///
    /// Empty derives `{issuer}/.well-known/jwks.json`, which is where Cbox ID's
    /// `KeyManager` puts them. **Not every provider agrees**: Google publishes at
    /// `https://www.googleapis.com/oauth2/v3/certs`, nowhere near its issuer. Set this
    /// when the provider's discovery document names something else.
    ///
    /// A configured URL rather than fetching the discovery document, deliberately: the
    /// document exists to tell a client a path it cannot guess, and if you already know
    /// the path, reading it costs a second network dependency and a second thing to be
    /// down.
    pub jwks_url: String,

    /// The claim carrying granted scopes.
    ///
    /// `scope` is what OAuth specifies and what Cbox ID emits. Microsoft Entra uses
    /// `scp`. Accepted as a space-separated string or as an array of strings, because
    /// providers disagree about that too.
    pub scope_claim: String,

    /// Scope that grants each role. A token may hold several.
    pub scope_write: String,
    pub scope_read: String,
    pub scope_admin: String,

    /// How often to refetch the key set.
    ///
    /// A key rotation is also picked up immediately on a token whose `kid` is unknown,
    /// so this is the ceiling on staleness rather than the mechanism.
    #[serde(with = "humantime_serde")]
    pub refresh_interval: Duration,

    /// Tolerance for clock difference between this host and the issuer.
    #[serde(with = "humantime_serde")]
    pub clock_skew: Duration,
}

impl Default for OidcConfig {
    fn default() -> Self {
        Self {
            issuer: String::new(),
            audience: String::new(),
            jwks_url: String::new(),
            scope_claim: "scope".to_owned(),
            // Namespaced, so they cannot collide with another resource server's scopes
            // on a shared issuer.
            scope_write: "telemetry:write".to_owned(),
            scope_read: "telemetry:read".to_owned(),
            scope_admin: "telemetry:admin".to_owned(),
            refresh_interval: Duration::from_secs(3600),
            clock_skew: Duration::from_secs(60),
        }
    }
}

impl OidcConfig {
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !self.issuer.trim().is_empty()
    }

    /// Where the key set lives.
    ///
    /// Cbox ID publishes it at the well-known path its `KeyManager` documents.
    #[must_use]
    pub fn jwks_url(&self) -> String {
        if !self.jwks_url.trim().is_empty() {
            return self.jwks_url.trim().to_owned();
        }
        format!(
            "{}/.well-known/jwks.json",
            self.issuer.trim_end_matches('/')
        )
    }

    /// The audience a token must carry: the configured one, or the issuer.
    #[must_use]
    pub fn expected_audience(&self) -> &str {
        if self.audience.trim().is_empty() {
            self.issuer.trim_end_matches('/')
        } else {
            self.audience.trim()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageConfig {
    /// `None` selects the automatic location — see [`StorageConfig::resolve_data_dir`].
    ///
    /// An empty string is normalised to `None` on the way in and rendered as `""` on
    /// the way out, so `data_dir = ""` and an absent key are genuinely the same
    /// configuration rather than two that merely behave alike.
    #[serde(with = "optional_path")]
    pub data_dir: Option<PathBuf>,
    /// Hard ceiling across all signals. The reaper drops oldest-first to stay under it.
    pub disk_budget: ByteSize,
    pub segment_duration: DurationSetting,
    /// Seal a segment early if its buffer exceeds this, so memory is a configured
    /// number rather than a function of traffic.
    pub max_segment_bytes: ByteSize,
    pub wal_sync: WalSync,
    #[serde(with = "humantime_serde")]
    pub wal_sync_interval: Duration,
    pub compression: Compression,
    /// Threads a single query may use to scan sealed segments.
    ///
    /// **Defaults to 1 — off.** It was worth 1.27x when the allocator was the
    /// bottleneck; with mimalloc the sequential path got fast enough that four
    /// workers buy about 7% on an unbounded scan and nothing at all on a limited one.
    /// That is not enough to spend three extra cores by default on a machine that is
    /// also accepting writes. Raise it if you run wide analytical queries and have
    /// cores to spare. `0` picks a conservative fraction of the machine.
    pub query_parallelism: usize,
}

impl StorageConfig {
    /// Resolved worker ceiling: `0` means "choose for me".
    #[must_use]
    pub fn resolved_query_parallelism(&self) -> usize {
        if self.query_parallelism != 0 {
            return self.query_parallelism;
        }
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        // Half the machine, capped. Past a handful of workers a single query is
        // usually bound by disk rather than CPU anyway, and the rest of the cores
        // still have writes to accept.
        (cores / 2).clamp(1, 4)
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            disk_budget: ByteSize::gib(10),
            segment_duration: DurationSetting(Duration::from_secs(60 * 60)),
            max_segment_bytes: ByteSize::mib(256),
            wal_sync: WalSync::Interval,
            wal_sync_interval: Duration::from_millis(100),
            compression: Compression::Zstd,
            query_parallelism: 1,
        }
    }
}

/// Treats `""` and "absent" as the same value in both directions.
mod optional_path {
    use std::path::PathBuf;

    use serde::{Deserialize, Deserializer, Serializer};

    // `&Option<T>` is required here: serde's `with` attribute dictates the signature.
    #[allow(clippy::ref_option)]
    pub(super) fn serialize<S: Serializer>(
        value: &Option<PathBuf>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        ser.serialize_str(
            value
                .as_ref()
                .map_or("", |p| p.to_str().unwrap_or_default()),
        )
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        de: D,
    ) -> Result<Option<PathBuf>, D::Error> {
        let raw = Option::<String>::deserialize(de)?;
        Ok(raw
            .filter(|s| !s.trim().is_empty())
            .map(|s| PathBuf::from(s.trim())))
    }
}

impl StorageConfig {
    /// Resolution order:
    /// 1. an explicit `data_dir`
    /// 2. `./telemetryd-data` **if it already exists** — so a developer who ran once in
    ///    a project directory keeps hitting the same store
    /// 3. the platform data directory
    ///
    /// Never silently returns a *different* directory than a previous run in the same
    /// working directory, which is the trap a bare `./telemetryd-data` default sets.
    pub fn resolve_data_dir(&self) -> PathBuf {
        if let Some(dir) = &self.data_dir
            && !dir.as_os_str().is_empty()
        {
            return dir.clone();
        }
        let cwd_default = PathBuf::from("./telemetryd-data");
        if cwd_default.is_dir() {
            return cwd_default;
        }
        directories::ProjectDirs::from("dk", "cbox", "telemetryd")
            .map_or(cwd_default, |dirs| dirs.data_dir().to_path_buf())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WalSync {
    /// fsync every batch. Safest, and caps ingest at the device's sync rate.
    Always,
    /// fsync on a timer — bounded loss window, documented in the README.
    Interval,
    /// Never fsync explicitly. Test and benchmark use only.
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    Zstd,
    Snappy,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RetentionConfig {
    pub logs: DurationSetting,
    pub traces: DurationSetting,
    /// Longer than the rest by default: metrics cost ~1.3 bytes/sample, and
    /// week-over-week comparison is most of what dashboards are for.
    pub metrics: DurationSetting,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        const DAY: u64 = 24 * 60 * 60;
        Self {
            logs: DurationSetting(Duration::from_secs(7 * DAY)),
            traces: DurationSetting(Duration::from_secs(7 * DAY)),
            metrics: DurationSetting(Duration::from_secs(30 * DAY)),
        }
    }
}

impl RetentionConfig {
    pub(super) fn each(&self) -> [(&'static str, Duration); 3] {
        [
            ("retention.logs", self.logs.0),
            ("retention.traces", self.traces.0),
            ("retention.metrics", self.metrics.0),
        ]
    }

    pub fn longest(&self) -> Duration {
        self.each()
            .iter()
            .map(|(_, d)| *d)
            .max()
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LimitsConfig {
    /// The ceiling on distinct *active* series across every app, and in practice the
    /// memory ceiling for the process. `0` derives it from the memory this process is
    /// allowed to use.
    ///
    /// Measured against the release binary: 100,000 distinct metric series took a serving
    /// process from 11 MB to 138 MB, growing linearly at about 1.0 KB per series across
    /// the whole range. That measurement is what `SERIES_COST_BYTES` encodes, and why
    /// this can be derived rather than guessed.
    ///
    /// It was a constant — 100,000 — picked from that measurement on a developer laptop
    /// and then shipped to every machine, including a VPS with a fraction of the memory.
    /// The number a box can hold is a property of the box, and the box is right there to
    /// be asked. `query_concurrency` had been derived this way for a while; there was no
    /// reason for this one to be a guess.
    ///
    /// # `0` changed meaning
    ///
    /// It used to mean *unlimited*. Unbounded cardinality is how a log store dies, so an
    /// operator who wrote `0` here almost certainly wanted "stop making me pick a
    /// number", not "run without a ceiling". Deriving is that, and it fails safe.
    ///
    /// The first attempt at the cost measurement pushed 100,000 log records carrying
    /// 100,000 distinct *attribute* values. Log attributes are structured metadata rather
    /// than stream labels, so all 100,000 were one series, and what got measured was the
    /// cost of records. Series cardinality has to be driven through label sets — the
    /// instance's own `series_active` is what says whether it was.
    pub max_series: u64,
    /// The ceiling on distinct active series for any one app. `0` means the same as
    /// `max_series`, so it does not bind.
    ///
    /// # Why it does not bind by default
    ///
    /// It defaulted to a fifth of the global limit, and that was wrong in the case
    /// telemetryd is actually for. This limit exists to stop one noisy app consuming the
    /// budget the others need — a fairness property, and one that has no meaning at all
    /// when a deployment has one app, which is the common shape here. What it did instead
    /// was hand the sole user of a 100,000-series budget a 20,000-series ceiling.
    ///
    /// That is not a hypothetical. A single small Laravel site — 144 routes, request
    /// histograms across method, status and bucket — reached the per-app cap and had
    /// every subsequent log stream refused, on an instance using 0.3% of its disk.
    ///
    /// Lower it when several apps share an instance and one must not crowd out the rest.
    pub max_series_per_app: u64,
    /// Silence after which a series gives its slot in the budget back. `0` disables it.
    ///
    /// The budget used to count every series in every unexpired segment, so a slot was
    /// held for as long as the *data* survived — thirty days for metrics. Renaming a set
    /// of metrics cost double for a month, and on one deployment 93 dead names held half
    /// the budget while every new log stream was refused.
    ///
    /// An hour is sixty flushes of margin for a client exporting every minute, and short
    /// enough that a rename heals within a working session. A series that goes quiet and
    /// comes back is simply re-admitted; that costs nothing unless the budget is full, and
    /// if it is full, the slot was better spent on whatever was still running.
    pub series_idle_after: DurationSetting,
    /// What to do when the budget is full of series that are all still active.
    ///
    /// `refuse` keeps the admitted set complete and stable, and says so loudly.
    /// `evict_oldest` makes the budget a ring buffer: the least recently written series
    /// gives way, so a new one is never turned away.
    ///
    /// The default is `refuse`, and the reasoning is worth stating because the other
    /// answer is the intuitive one — it is what `relay.when_full` and the disk reaper both
    /// do. The difference is what gets freed. Overwriting the oldest *records* reclaims
    /// their memory immediately. Evicting the oldest *series* reclaims nothing on its own,
    /// because the producer does not know and keeps sending it; the series is simply
    /// re-admitted a moment later at the cost of another one. At 120,000 active series
    /// against a 100,000 budget that churns continuously, every series ends up with holes,
    /// and the stored dictionary still accumulates all 120,000 — so the ceiling stops
    /// bounding the thing it exists to bound, exactly when it is needed.
    ///
    /// Complete data for a bounded set is worth more to someone debugging than most of
    /// everything with gaps in unpredictable places. But that is a judgement about how
    /// people read telemetry, not a fact about the machine, so it is a setting rather than
    /// a decision made for you.
    pub when_series_full: WhenSeriesFull,
    pub max_labels_per_series: u32,
    pub max_label_name_bytes: u32,
    pub max_label_value_bytes: u32,
    pub max_log_line_bytes: ByteSize,
    pub max_attrs_per_record: u32,
    /// Backpressure threshold: a full queue returns 429 with `Retry-After` rather than
    /// buffering without bound.
    pub ingest_queue_depth: u32,

    /// Read requests that may run at once. `0` sizes it from the memory this process is
    /// allowed to use.
    ///
    /// # Why this exists
    ///
    /// Ingest has had backpressure since `ingest_queue_depth`; the read side had none at
    /// all, and a query's cost is not small. Measured against a 120,000-record store: a
    /// `query_range` returning 5,000 records costs about 4 MB while it runs, and 256 of
    /// them concurrently took the process from 11 MB to 982 MB. Latency degraded
    /// gracefully the whole way — the failure mode here is memory, not time, which is why
    /// a request timeout does not cover it.
    ///
    /// The only accidental bound was tokio's blocking pool at 512 threads, which is not a
    /// limit anybody chose.
    pub query_concurrency: u32,

    /// Exports that may run at once. Much smaller, because one is much larger.
    ///
    /// An export returns up to `MAX_LIMIT` records in a single response, and the store's
    /// scan collects them before the first byte is written. Measured at roughly 95 MB per
    /// concurrent request against a 120,000-record store — about 25 times a `query_range`
    /// — and 32 at once reached 3.0 GB. Sharing one budget with ordinary queries would
    /// mean picking a number that is either too tight for a dashboard or too loose for
    /// this, so they are counted separately.
    pub export_concurrency: u32,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_series: 0,
            max_series_per_app: 0,
            series_idle_after: DurationSetting(Duration::from_secs(60 * 60)),
            when_series_full: WhenSeriesFull::Refuse,
            max_labels_per_series: 60,
            max_label_name_bytes: 128,
            max_label_value_bytes: 2048,
            max_log_line_bytes: ByteSize::kib(256),
            max_attrs_per_record: 128,
            ingest_queue_depth: 8192,
            query_concurrency: 0,
            export_concurrency: 0,
        }
    }
}

/// What one series costs in a sealed segment's manifest, resident.
///
/// Every sealed segment carries a dictionary of the distinct streams it holds, and those
/// dictionaries stay in memory for as long as the segment does. Measured: 133 segments
/// over a 20,592-series store held 193 MB resident with nothing being queried and nothing
/// being ingested — 1.45 MB per segment, which is 20,592 label sets at about 70 bytes
/// each. The same 200,000 records kept in one unsealed buffer held 24 MB.
///
/// That is the term that took a server down, and it is invisible from every other number:
/// it is bounded by `storage.disk_budget` and by retention, not by memory, so a
/// configuration can quietly require more RAM than the machine has.
const MANIFEST_STREAM_BYTES: u64 = 70;

/// Share of the memory limit the resident segment manifests may take.
///
/// A third: they compete with the write buffer, the read budget and the series tracker,
/// and unlike all three this one cannot be shed under pressure — it is what "the store is
/// open" costs.
const MANIFEST_BUDGET_FRACTION: u64 = 3;

/// The most concurrent reads telemetryd will give itself.
///
/// It was 64, from a measurement of 4 MB per `query_range` against a 120,000-record
/// store. Both halves of that aged badly: a production store is much larger, and a
/// dashboard opening 124 panels at once arrives as one burst that will use every slot it
/// is offered. Sixty-four of them at the real cost is where 6.65 GB came from.
///
/// Sixteen is a deliberate trade. A burst past it waits — the limiter answers `429` with
/// `Retry-After`, the same answer a full ingest queue gives — and waiting is recoverable
/// in a way that being killed by the kernel is not. An operator with a large machine and
/// a measured appetite can still set `query_concurrency` outright.
///
/// What is *not* known, and is worth writing down rather than implying: the 6.65 GB was
/// never attributed to a specific allocation. Re-measured against a 200,000-record store,
/// 32 concurrent `query_range` calls peaked at 103 MB — about 3 MB each, comfortably
/// under `QUERY_COST_BYTES`. That store held minutes of data where the one that failed
/// held weeks, so the constant is defensible at the size it was measured and untested at
/// the size that matters. This ceiling and the unit's `MemoryMax` are both chosen to
/// bound a term whose size is still a guess.
const QUERY_CONCURRENCY_MAX: u32 = 16;

/// Roughly what a read request holds while it runs, from the measurements in
/// [`LimitsConfig::query_concurrency`] and [`LimitsConfig::export_concurrency`], rounded
/// up. Deliberately pessimistic: over-estimating costs throughput on a busy instance,
/// under-estimating costs the process.
const QUERY_COST_BYTES: u64 = 8 * 1024 * 1024;

const EXPORT_COST_BYTES: u64 = 160 * 1024 * 1024;

/// Share of the memory limit these two may hold between them.
///
/// The rest is for the write path, whose own ceiling is `storage.max_segment_bytes`, and
/// for the allocator, which does not hand pages back promptly — measured going from a
/// 982 MB peak to 855 MB three seconds later. A controller reading live memory would
/// have read that as continuing pressure, which is one of the reasons this is decided
/// once at startup instead.
const READ_BUDGET_FRACTION: u64 = 4;

/// What one counted series costs the process, all in.
///
/// Measured on the release binary rather than reasoned about: 100,000 distinct metric
/// series took a serving process from 11 MB to 138 MB, linearly across the range. Rounded
/// up, because erring high derives a smaller ceiling and a smaller ceiling is the safe
/// direction — it refuses visibly instead of being killed by the kernel.
const SERIES_COST_BYTES: u64 = 1_400;

/// Share of the memory limit the series budget may hold.
///
/// Smaller than the read budget's quarter, because series memory is *resident for the
/// life of the process* while a query's is transient. It also has to sit alongside the
/// write buffer, whose own ceiling is `storage.max_segment_bytes`. A sixteenth of 2 GiB
/// is 128 MiB, or roughly 91,000 series — close to the constant this replaces, which is
/// the reassuring part: on the machine the constant was measured on, the derivation
/// agrees with it, and on a smaller machine it does not.
const SERIES_BUDGET_FRACTION: u64 = 16;

/// Floor and ceiling on the derived series budget.
///
/// The floor keeps a small container usable rather than correct-and-useless: 10,000
/// series is a real Laravel app's request metrics.
///
/// The ceiling is 100,000 because that is the largest figure that has been *measured* —
/// a serving process at 138 MB — rather than extrapolated from a slope. A larger machine
/// would derive more, and the honest position is that nobody has run this at 300,000
/// series to find out what else grows there. Memory is also not the only cost: every
/// series is matcher work on every query, and `SERIES_COST_BYTES` was measured with one
/// record per series, where in production each carries thousands. Both of those push the
/// real cost up, and neither is in the arithmetic, so the arithmetic does not get to pick
/// a number past the one that was checked.
///
/// An operator who knows their box can set `max_series` outright; this is the ceiling on
/// what telemetryd will assume on its own.
const SERIES_BUDGET_MIN: u64 = 10_000;
const SERIES_BUDGET_MAX: u64 = 100_000;

/// What this configuration will cost in resident segment manifests once the store is
/// full, and what it is allowed to cost.
///
/// Worst case on purpose. The series count per segment is not knowable in advance, so the
/// ceiling is used — which is exactly the question being asked: *can this configuration,
/// at the limits it permits, fit in this machine?* A number that only reports the happy
/// path is the number that let a 7.5 GiB box run a configuration needing several times
/// that.
#[must_use]
pub fn manifest_memory(config: &Config) -> (u64, u64) {
    let segment = config.storage.segment_duration.get().as_secs().max(1);
    let segments: u64 = config
        .retention
        .each()
        .iter()
        .map(|(_, window)| window.as_secs() / segment)
        .sum();
    let series = config.limits.resolved_max_series();
    let implied = segments
        .saturating_mul(series)
        .saturating_mul(MANIFEST_STREAM_BYTES);
    (implied, memory_limit_bytes() / MANIFEST_BUDGET_FRACTION)
}

impl LimitsConfig {
    /// The global series ceiling in force, resolving `0`.
    #[must_use]
    pub fn resolved_max_series(&self) -> u64 {
        if self.max_series != 0 {
            return self.max_series;
        }
        let budget = memory_limit_bytes() / SERIES_BUDGET_FRACTION;
        (budget / SERIES_COST_BYTES).clamp(SERIES_BUDGET_MIN, SERIES_BUDGET_MAX)
    }

    /// The per-app ceiling in force. `0` means "whatever the global one is", so it does
    /// not bind until an operator lowers it deliberately.
    #[must_use]
    pub fn resolved_max_series_per_app(&self) -> u64 {
        if self.max_series_per_app != 0 {
            return self.max_series_per_app;
        }
        self.resolved_max_series()
    }

    /// The number of concurrent queries in force, resolving `0`.
    pub fn resolved_query_concurrency(&self) -> u32 {
        if self.query_concurrency != 0 {
            return self.query_concurrency;
        }
        derive_concurrency(QUERY_COST_BYTES, 4, QUERY_CONCURRENCY_MAX)
    }

    /// The number of concurrent exports in force, resolving `0`.
    pub fn resolved_export_concurrency(&self) -> u32 {
        if self.export_concurrency != 0 {
            return self.export_concurrency;
        }
        derive_concurrency(EXPORT_COST_BYTES, 1, 8)
    }
}

fn derive_concurrency(cost_bytes: u64, min: u32, max: u32) -> u32 {
    let budget = memory_limit_bytes() / READ_BUDGET_FRACTION;
    let derived = u32::try_from(budget / cost_bytes).unwrap_or(max);
    derived.clamp(min, max)
}

/// What this process is actually allowed to use, read once.
///
/// The cgroup limit first, because in a container the host's free memory is not the
/// number that gets you killed — and the container is a first-class way to run this. The
/// systemd unit sets `MemoryMax` for the same reason, so a service install lands here too.
///
/// # `MemTotal` is not ours, and assuming it was took down a server
///
/// The fallback used to return the host's total memory, and every derived limit was a
/// fraction of that. On a dedicated box that is merely aggressive. On the box this was
/// reported from — telemetryd sharing a 7.5 GiB VPS with MySQL, php-fpm, Redis, Typesense
/// and the website those serve — it sized itself as though it owned the machine, derived
/// 64 concurrent queries, and reached 6.65 GB resident. The load average hit 38 and
/// nothing on the server answered, telemetryd included.
///
/// A single-node observability tool is *usually* a guest on a machine that has a job. It
/// cannot tell a dedicated host from a shared one, so it assumes the answer that fails
/// safe: a quarter of what is installed. An operator who has given telemetryd the whole
/// box says so with `MemoryMax` in the unit, or by setting the limits outright — both of
/// which are read exactly, and neither of which is guessed.
/// Whether anything is actually stopping this process from taking the machine.
///
/// `false` means no cgroup limit is in force, so every derived limit is a guess about a
/// share of a host telemetryd does not own, and nothing but those guesses stands between
/// a heavy query burst and the OOM killer picking a victim — which may well be the
/// database next door rather than us.
#[must_use]
pub fn memory_is_capped() -> bool {
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Some(limit) = std::fs::read_to_string(path)
            .ok()
            .and_then(|text| text.split_whitespace().find_map(|w| w.parse::<u64>().ok()))
            && limit > 0
            && limit < u64::MAX / 2
        {
            return true;
        }
    }
    false
}

fn memory_limit_bytes() -> u64 {
    const ASSUMED: u64 = 2 * 1024 * 1024 * 1024;
    /// Share of a machine's installed memory to assume is ours when nothing says.
    const UNCONTAINED_SHARE: u64 = 4;

    let read_number = |path: &str| -> Option<u64> {
        std::fs::read_to_string(path)
            .ok()?
            .split_whitespace()
            .find_map(|word| word.parse::<u64>().ok())
    };

    // cgroup v2 writes the literal `max` when unlimited, which parses to nothing and
    // falls through — which is the behaviour we want.
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Some(limit) = read_number(path) {
            // cgroup v1 spells "unlimited" as a number near u64::MAX rather than a word.
            if limit > 0 && limit < u64::MAX / 2 {
                return limit;
            }
        }
    }

    // `MemTotal:  16305892 kB` — the first number on that line, in kibibytes.
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo")
        && let Some(line) = meminfo.lines().find(|line| line.starts_with("MemTotal:"))
        && let Some(kib) = line
            .split_whitespace()
            .nth(1)
            .and_then(|v| v.parse::<u64>().ok())
    {
        return (kib * 1024 / UNCONTAINED_SHARE).max(256 * 1024 * 1024);
    }

    ASSUMED
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    pub level: String,
    pub format: LogFormat,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
            format: LogFormat::Text,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

/// A duration that round-trips as a humantime string (`7d`, `100ms`).
///
/// A newtype rather than `#[serde(with = "humantime_serde")]` on each field, because
/// it also has to survive being *serialised* back out for `telemetryd validate` and
/// `/status` — as a readable `"7d"`, not `{ "secs": 604800, "nanos": 0 }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationSetting(pub Duration);

impl DurationSetting {
    pub fn get(self) -> Duration {
        self.0
    }
}

impl From<DurationSetting> for Duration {
    fn from(value: DurationSetting) -> Self {
        value.0
    }
}

impl Serialize for DurationSetting {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        ser.serialize_str(&humantime::format_duration(self.0).to_string())
    }
}

impl<'de> Deserialize<'de> for DurationSetting {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> std::result::Result<Self, D::Error> {
        humantime_serde::deserialize(de).map(Self)
    }
}

/// Deserialize a path, treating an empty string as "not set".
///
/// The schema documents every unset value as `""`, and the environment gives no way to
/// express "absent" other than an empty string. Mapping that to `None` here means the
/// question "is this configured" is answered by the type rather than by every call site
/// remembering to trim and compare — which is how `data_dir` has always worked, and
/// what the TLS paths should have done from the start.
fn path_or_none<'de, D>(deserializer: D) -> std::result::Result<Option<PathBuf>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<String>::deserialize(deserializer)?;
    Ok(raw
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty())
        .map(PathBuf::from))
}
