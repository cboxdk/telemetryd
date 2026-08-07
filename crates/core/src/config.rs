//! Configuration schema and the defaults → file → env → flags layering described in
//! ADR-003.
//!
//! The load-bearing property here is that **the empty configuration is a valid
//! configuration**: every field has a default, so `telemetryd serve` with no file, no
//! environment and no flags is a complete, supported setup. Everything else is an
//! override on top of that.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytesize::ByteSize;
use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::secret::TokenSpecs;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

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
    #[serde(default)]
    pub scrape: Vec<ScrapeConfig>,
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
    /// Permit a non-loopback bind with no token configured. See ADR-004.
    pub insecure: bool,
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
            max_body_bytes: ByteSize::mib(16),
            request_timeout: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AuthConfig {
    /// Guards `/v1/*` and `/api/v1/write`.
    pub ingest_token: TokenSpecs,
    /// Guards the Prometheus/Loki/Tempo read APIs, plus `/status` and `/metrics`.
    pub query_token: TokenSpecs,
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
    /// number rather than a function of traffic (ADR-001 D4).
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

/// Strip figment's internal profile name out of the key path it reports.
///
/// figment reports `key "default.retention.lgos"`, where `default` is its own profile
/// concept and appears nowhere in the user's file. Someone reading the error goes
/// looking for a `[default]` section, does not find one, and concludes the message is
/// about something else. Everything after the profile is the real path.
fn readable_figment_error(error: &figment::Error) -> String {
    error.to_string().replace("key \"default.", "key \"")
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
    /// Resolution order, per ADR-003:
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
    pub events: DurationSetting,
    /// Longer than the rest by default: metrics cost ~1.3 bytes/sample, and
    /// week-over-week comparison is most of what dashboards are for. See ADR-001 D3.
    pub metrics: DurationSetting,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        const DAY: u64 = 24 * 60 * 60;
        Self {
            logs: DurationSetting(Duration::from_secs(7 * DAY)),
            traces: DurationSetting(Duration::from_secs(7 * DAY)),
            events: DurationSetting(Duration::from_secs(7 * DAY)),
            metrics: DurationSetting(Duration::from_secs(30 * DAY)),
        }
    }
}

impl RetentionConfig {
    fn each(&self) -> [(&'static str, Duration); 4] {
        [
            ("retention.logs", self.logs.0),
            ("retention.traces", self.traces.0),
            ("retention.events", self.events.0),
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
    pub max_series: u64,
    pub max_series_per_app: u64,
    pub max_labels_per_series: u32,
    pub max_label_name_bytes: u32,
    pub max_label_value_bytes: u32,
    pub max_log_line_bytes: ByteSize,
    pub max_attrs_per_record: u32,
    pub max_spans_per_trace: u32,
    /// Backpressure threshold: a full queue returns 429 with `Retry-After` rather than
    /// buffering without bound.
    pub ingest_queue_depth: u32,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_series: 100_000,
            max_series_per_app: 20_000,
            max_labels_per_series: 60,
            max_label_name_bytes: 128,
            max_label_value_bytes: 2048,
            max_log_line_bytes: ByteSize::kib(256),
            max_attrs_per_record: 128,
            max_spans_per_trace: 10_000,
            ingest_queue_depth: 8192,
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScrapeConfig {
    pub job: String,
    pub targets: Vec<String>,
    #[serde(default = "default_scrape_interval")]
    pub interval: DurationSetting,
    #[serde(default = "default_scrape_timeout")]
    pub timeout: DurationSetting,
    #[serde(default = "default_scrape_path")]
    pub path: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

fn default_scrape_interval() -> DurationSetting {
    DurationSetting(Duration::from_secs(15))
}
fn default_scrape_timeout() -> DurationSetting {
    DurationSetting(Duration::from_secs(10))
}
fn default_scrape_path() -> String {
    "/metrics".to_owned()
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

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

/// Explicit env-var → config-path mapping.
///
/// figment's generic `Env::split("_")` cannot work here: it would turn
/// `TELEMETRYD_STORAGE_DATA_DIR` into `storage.data.dir`. A table is more typing but
/// buys the documented naming, plus the ability to detect a typo'd `TELEMETRYD_*`
/// variable instead of ignoring it silently.
const ENV_KEYS: &[(&str, &str)] = &[
    ("TELEMETRYD_SERVER_LISTEN", "server.listen"),
    ("TELEMETRYD_SERVER_INSECURE", "server.insecure"),
    ("TELEMETRYD_SERVER_MAX_BODY_BYTES", "server.max_body_bytes"),
    (
        "TELEMETRYD_SERVER_REQUEST_TIMEOUT",
        "server.request_timeout",
    ),
    ("TELEMETRYD_SERVER_SHUTDOWN_GRACE", "server.shutdown_grace"),
    ("TELEMETRYD_AUTH_INGEST_TOKEN", "auth.ingest_token"),
    ("TELEMETRYD_AUTH_QUERY_TOKEN", "auth.query_token"),
    ("TELEMETRYD_STORAGE_DATA_DIR", "storage.data_dir"),
    ("TELEMETRYD_STORAGE_DISK_BUDGET", "storage.disk_budget"),
    (
        "TELEMETRYD_STORAGE_SEGMENT_DURATION",
        "storage.segment_duration",
    ),
    (
        "TELEMETRYD_STORAGE_MAX_SEGMENT_BYTES",
        "storage.max_segment_bytes",
    ),
    ("TELEMETRYD_STORAGE_WAL_SYNC", "storage.wal_sync"),
    (
        "TELEMETRYD_STORAGE_WAL_SYNC_INTERVAL",
        "storage.wal_sync_interval",
    ),
    ("TELEMETRYD_STORAGE_COMPRESSION", "storage.compression"),
    (
        "TELEMETRYD_STORAGE_QUERY_PARALLELISM",
        "storage.query_parallelism",
    ),
    ("TELEMETRYD_RETENTION_LOGS", "retention.logs"),
    ("TELEMETRYD_RETENTION_TRACES", "retention.traces"),
    ("TELEMETRYD_RETENTION_EVENTS", "retention.events"),
    ("TELEMETRYD_RETENTION_METRICS", "retention.metrics"),
    ("TELEMETRYD_LIMITS_MAX_SERIES", "limits.max_series"),
    (
        "TELEMETRYD_LIMITS_MAX_SERIES_PER_APP",
        "limits.max_series_per_app",
    ),
    (
        "TELEMETRYD_LIMITS_MAX_LABELS_PER_SERIES",
        "limits.max_labels_per_series",
    ),
    (
        "TELEMETRYD_LIMITS_MAX_LABEL_NAME_BYTES",
        "limits.max_label_name_bytes",
    ),
    (
        "TELEMETRYD_LIMITS_MAX_LABEL_VALUE_BYTES",
        "limits.max_label_value_bytes",
    ),
    (
        "TELEMETRYD_LIMITS_MAX_LOG_LINE_BYTES",
        "limits.max_log_line_bytes",
    ),
    (
        "TELEMETRYD_LIMITS_MAX_ATTRS_PER_RECORD",
        "limits.max_attrs_per_record",
    ),
    (
        "TELEMETRYD_LIMITS_MAX_SPANS_PER_TRACE",
        "limits.max_spans_per_trace",
    ),
    (
        "TELEMETRYD_LIMITS_INGEST_QUEUE_DEPTH",
        "limits.ingest_queue_depth",
    ),
    (
        "TELEMETRYD_INGEST_TRUNCATE_OVERSIZED_BODIES",
        "ingest.truncate_oversized_bodies",
    ),
    ("TELEMETRYD_LOG_LEVEL", "log.level"),
    ("TELEMETRYD_LOG_FORMAT", "log.format"),
];

/// The config path a `TELEMETRYD_*` variable maps to, if any. Lets `telemetryd
/// validate` attribute each resolved value to the environment variable that set it.
pub fn env_var_path(var: &str) -> Option<&'static str> {
    ENV_KEYS
        .iter()
        .find(|(name, _)| *name == var)
        .map(|(_, path)| *path)
}

/// CLI-supplied overrides. Highest precedence; `None` means "not specified".
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub listen: Option<SocketAddr>,
    pub data_dir: Option<PathBuf>,
    pub insecure: Option<bool>,
    pub log_level: Option<String>,
}

/// A loaded configuration together with where its values came from — the provenance
/// is what makes `telemetryd validate` genuinely useful rather than a syntax check.
#[derive(Debug)]
pub struct Loaded {
    pub config: Config,
    pub config_file: Option<PathBuf>,
    pub env_overrides: Vec<&'static str>,
    pub flag_overrides: Vec<&'static str>,
    /// Non-fatal problems found while loading — returned rather than logged, because
    /// configuration is read *before* the logger it configures exists. The caller
    /// emits these once tracing is up, so they are never silently swallowed.
    pub warnings: Vec<String>,
}

impl Config {
    /// Load with the full precedence chain: defaults → file → env → flags.
    pub fn load(explicit_file: Option<&Path>, overrides: &Overrides) -> Result<Loaded> {
        let config_file = match explicit_file {
            // An explicitly requested file that is missing is an error; a missing
            // *discovered* file is not.
            Some(path) => {
                if !path.is_file() {
                    return Err(Error::ConfigUnreadable {
                        path: path.to_path_buf(),
                        source: std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "no such configuration file",
                        ),
                    });
                }
                Some(path.to_path_buf())
            }
            None => discover_config_file(),
        };

        let mut figment = Figment::new();
        if let Some(path) = &config_file {
            figment = figment.merge(Toml::file(path));
        }

        let mut env_overrides = Vec::new();
        for (var, path) in ENV_KEYS {
            if let Ok(value) = std::env::var(var)
                && !value.is_empty()
            {
                env_overrides.push(*var);
                figment = figment.merge(Env::raw().only(&[var]).map(move |_| (*path).into()));
            }
        }
        let warnings = unknown_env_var_warnings();

        let mut config: Config = figment
            .extract()
            .map_err(|err| Error::Config(readable_figment_error(&err)))?;

        let mut flag_overrides = Vec::new();
        if let Some(listen) = overrides.listen {
            config.server.listen = listen;
            flag_overrides.push("server.listen");
        }
        if let Some(dir) = &overrides.data_dir {
            config.storage.data_dir = Some(dir.clone());
            flag_overrides.push("storage.data_dir");
        }
        if let Some(insecure) = overrides.insecure {
            config.server.insecure = insecure;
            flag_overrides.push("server.insecure");
        }
        if let Some(level) = &overrides.log_level {
            config.log.level.clone_from(level);
            flag_overrides.push("log.level");
        }

        config.validate()?;

        Ok(Loaded {
            config,
            config_file,
            env_overrides,
            flag_overrides,
            warnings,
        })
    }

    /// Cross-field rules. Run at load time rather than at first use, so a bad
    /// configuration fails at startup instead of at 3am on the first query.
    pub fn validate(&self) -> Result<()> {
        // ADR-004: fail closed on an exposed bind.
        let exposed = !self.server.listen.ip().is_loopback();
        let unauthenticated = self.auth.ingest_token.is_empty() && self.auth.query_token.is_empty();
        if exposed && unauthenticated && !self.server.insecure {
            return Err(Error::Config(exposed_bind_message(self.server.listen)));
        }

        let segment = self.storage.segment_duration.get();
        if segment.is_zero() {
            return Err(Error::Config(
                "storage.segment_duration must be greater than zero".to_owned(),
            ));
        }
        for (name, value) in self.retention.each() {
            if value < segment {
                return Err(Error::Config(format!(
                    "{name} ({}) is shorter than storage.segment_duration ({}); \
                     retention is enforced by deleting whole segments, so data would be \
                     dropped before a segment is even sealed",
                    humantime::format_duration(value),
                    humantime::format_duration(segment),
                )));
            }
        }

        // The reaper deletes in segment-sized quanta. A budget that cannot hold a few
        // segments would have it deleting data as fast as it arrives.
        let floor = self.storage.max_segment_bytes.as_u64().saturating_mul(4);
        if self.storage.disk_budget.as_u64() < floor {
            return Err(Error::Config(format!(
                "storage.disk_budget ({}) must be at least 4x storage.max_segment_bytes ({}), \
                 i.e. {} or more",
                self.storage.disk_budget,
                self.storage.max_segment_bytes,
                ByteSize::b(floor),
            )));
        }

        if self.limits.ingest_queue_depth == 0 {
            return Err(Error::Config(
                "limits.ingest_queue_depth must be greater than zero".to_owned(),
            ));
        }
        if self.limits.max_series_per_app > self.limits.max_series {
            return Err(Error::Config(format!(
                "limits.max_series_per_app ({}) exceeds limits.max_series ({}), so the \
                 per-app cap could never be reached",
                self.limits.max_series_per_app, self.limits.max_series,
            )));
        }

        let mut jobs = std::collections::BTreeSet::new();
        for scrape in &self.scrape {
            if !jobs.insert(&scrape.job) {
                return Err(Error::Config(format!(
                    "duplicate scrape job name {:?}; job names must be unique",
                    scrape.job
                )));
            }
            if scrape.timeout.get() > scrape.interval.get() {
                return Err(Error::Config(format!(
                    "scrape job {:?} has timeout ({}) longer than interval ({})",
                    scrape.job,
                    humantime::format_duration(scrape.timeout.get()),
                    humantime::format_duration(scrape.interval.get()),
                )));
            }
        }

        Ok(())
    }
}

/// The message an operator sees when they expose telemetryd without a token.
///
/// It is long on purpose. This is the one error most likely to be hit by someone
/// trying the product for the first time, and "unauthorized bind" with no remedy
/// would send them straight to `--insecure` — the worst of the three fixes.
fn exposed_bind_message(listen: SocketAddr) -> String {
    // Written as an explicit line list rather than one continued string literal:
    // `\` continuations swallow the leading indentation of the next line, which
    // silently flattens the numbered steps that make this message readable.
    let lines = [
        format!("refusing to listen on {listen} with no authentication token configured."),
        String::new(),
        "This address is reachable from outside this machine, and telemetry data".to_owned(),
        "routinely contains emails, tokens and stack traces.".to_owned(),
        String::new(),
        "Pick one:".to_owned(),
        String::new(),
        "  1. Set a token (recommended):".to_owned(),
        format!("       TELEMETRYD_AUTH_INGEST_TOKEN={}", suggest_token()),
        format!("       TELEMETRYD_AUTH_QUERY_TOKEN={}", suggest_token()),
        String::new(),
        "  2. Bind to loopback and put a reverse proxy in front, which is also".to_owned(),
        "     how you get TLS (telemetryd does not terminate TLS — see ADR-004):".to_owned(),
        format!("       --listen 127.0.0.1:{}", listen.port()),
        String::new(),
        "  3. Accept the risk explicitly, e.g. on a trusted private network:".to_owned(),
        "       --insecure".to_owned(),
    ];
    lines.join("\n")
}

/// Generate a random token to paste into the error above.
///
/// Deliberately not a `rand` dependency — this is the only place in the binary that
/// needs randomness, and the OS entropy source is two lines away. The fallback path
/// exists so a hardened environment without `/dev/urandom` still gets *a* suggestion;
/// it is never used to protect anything, since the operator chooses their own token.
fn suggest_token() -> String {
    use std::hash::{BuildHasher, Hasher};

    let mut bytes = [0u8; 24];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut bytes))
        .is_err()
    {
        let state = std::collections::hash_map::RandomState::new();
        for chunk in bytes.chunks_mut(8) {
            let mut hasher = state.build_hasher();
            hasher.write_usize(chunk.as_ptr() as usize);
            hasher.write_u128(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos()),
            );
            chunk.copy_from_slice(&hasher.finish().to_le_bytes()[..chunk.len()]);
        }
    }
    bytes.iter().fold(String::with_capacity(48), |mut acc, b| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{b:02x}");
        acc
    })
}

fn discover_config_file() -> Option<PathBuf> {
    let mut candidates = vec![PathBuf::from("telemetryd.toml")];
    if let Some(dirs) = directories::ProjectDirs::from("dk", "cbox", "telemetryd") {
        candidates.push(dirs.config_dir().join("telemetryd.toml"));
    }
    candidates.push(PathBuf::from("/etc/telemetryd/telemetryd.toml"));
    candidates.into_iter().find(|p| p.is_file())
}

/// A `TELEMETRYD_*` variable that matches nothing is almost always a typo. Ignoring it
/// silently means the operator believes a setting is applied when it is not.
fn unknown_env_var_warnings() -> Vec<String> {
    let mut warnings: Vec<String> = std::env::vars()
        .map(|(key, _)| key)
        .filter(|key| key.starts_with("TELEMETRYD_"))
        .filter(|key| !ENV_KEYS.iter().any(|(var, _)| var == key))
        .map(|key| {
            format!(
                "unrecognised environment variable {key} has no effect \
                 (see docs/CONFIGURATION.md for the supported names)"
            )
        })
        .collect();
    warnings.sort();
    warnings
}

#[cfg(test)]
// `figment::Jail` closures return figment's own large error type; that is the test
// harness's shape, not ours.
#[allow(clippy::unwrap_used, clippy::result_large_err)]
mod tests {
    use super::*;

    fn day(n: u64) -> Duration {
        Duration::from_secs(n * 24 * 60 * 60)
    }

    #[test]
    fn empty_config_is_a_valid_config() {
        let config = Config::default();
        config.validate().unwrap();
        assert_eq!(config.server.listen.port(), 4319);
        assert!(config.server.listen.ip().is_loopback());
        assert_eq!(config.storage.disk_budget, ByteSize::gib(10));
        assert_eq!(config.retention.logs.get(), day(7));
        assert_eq!(config.retention.metrics.get(), day(30));
    }

    #[test]
    fn toml_overrides_defaults_and_leaves_the_rest_alone() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "telemetryd.toml",
                r#"
                [server]
                listen = "127.0.0.1:9999"

                [retention]
                logs = "3d"
                "#,
            )?;

            let loaded = Config::load(None, &Overrides::default()).unwrap();
            assert_eq!(loaded.config.server.listen.port(), 9999);
            assert_eq!(loaded.config.retention.logs.get(), day(3));
            // Untouched keys keep their defaults — partial files are the norm.
            assert_eq!(loaded.config.retention.metrics.get(), day(30));
            assert_eq!(loaded.config.storage.disk_budget, ByteSize::gib(10));
            assert!(loaded.config_file.is_some());
            Ok(())
        });
    }

    #[test]
    fn env_beats_file_and_flags_beat_env() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("telemetryd.toml", "[server]\nlisten = \"127.0.0.1:1111\"\n")?;
            jail.set_env("TELEMETRYD_SERVER_LISTEN", "127.0.0.1:2222");

            let from_env = Config::load(None, &Overrides::default()).unwrap();
            assert_eq!(from_env.config.server.listen.port(), 2222);
            assert_eq!(from_env.env_overrides, vec!["TELEMETRYD_SERVER_LISTEN"]);

            let overrides = Overrides {
                listen: Some("127.0.0.1:3333".parse().unwrap()),
                ..Overrides::default()
            };
            let from_flag = Config::load(None, &overrides).unwrap();
            assert_eq!(from_flag.config.server.listen.port(), 3333);
            assert_eq!(from_flag.flag_overrides, vec!["server.listen"]);
            Ok(())
        });
    }

    #[test]
    fn env_parses_non_string_scalars() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("TELEMETRYD_SERVER_INSECURE", "true");
            jail.set_env("TELEMETRYD_LIMITS_MAX_SERIES", "5000");
            jail.set_env("TELEMETRYD_LIMITS_MAX_SERIES_PER_APP", "500");
            jail.set_env("TELEMETRYD_RETENTION_LOGS", "12h");
            jail.set_env("TELEMETRYD_STORAGE_DISK_BUDGET", "2GiB");

            let loaded = Config::load(None, &Overrides::default()).unwrap();
            assert!(loaded.config.server.insecure);
            assert_eq!(loaded.config.limits.max_series, 5000);
            assert_eq!(
                loaded.config.retention.logs.get(),
                Duration::from_secs(12 * 3600)
            );
            assert_eq!(loaded.config.storage.disk_budget, ByteSize::gib(2));
            Ok(())
        });
    }

    #[test]
    fn a_typo_in_an_env_var_is_reported_not_ignored() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("TELEMETRYD_RETETNION_LOGS", "3d");
            let loaded = Config::load(None, &Overrides::default()).unwrap();

            assert!(
                loaded
                    .warnings
                    .iter()
                    .any(|w| w.contains("TELEMETRYD_RETETNION_LOGS")),
                "expected a warning naming the typo, got {:?}",
                loaded.warnings
            );
            // …but it must not change behaviour.
            assert_eq!(loaded.config.retention.logs.get(), day(7));
            Ok(())
        });
    }

    #[test]
    fn unknown_keys_are_rejected_rather_than_ignored() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("telemetryd.toml", "[retention]\nretetnion = \"3d\"\n")?;
            let err = Config::load(None, &Overrides::default())
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("retetnion"),
                "error should name the offending key: {err}"
            );
            Ok(())
        });
    }

    #[test]
    fn explicit_missing_config_file_is_an_error() {
        let err = Config::load(
            Some(Path::new("/nonexistent/telemetryd.toml")),
            &Overrides::default(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::ConfigUnreadable { .. }));
    }

    #[test]
    fn exposed_bind_without_a_token_is_refused_with_a_usable_message() {
        let mut config = Config::default();
        config.server.listen = "0.0.0.0:4319".parse().unwrap();

        let err = config.validate().unwrap_err().to_string();
        // The wildcard bind is the exact case that exposes people; it must not be
        // treated as "unspecified, probably fine".
        assert!(err.contains("refusing to listen"), "{err}");
        assert!(err.contains("TELEMETRYD_AUTH_INGEST_TOKEN"), "{err}");
        assert!(err.contains("--insecure"), "{err}");
        assert!(err.contains("127.0.0.1:4319"), "{err}");
    }

    #[test]
    fn exposed_bind_is_allowed_with_a_token_or_with_insecure() {
        let mut config = Config::default();
        config.server.listen = "0.0.0.0:4319".parse().unwrap();

        config.auth.ingest_token = serde_json::from_str(r#""a-token""#).unwrap();
        config.validate().unwrap();

        config.auth = AuthConfig::default();
        config.server.insecure = true;
        config.validate().unwrap();
    }

    #[test]
    fn loopback_needs_no_token() {
        let mut config = Config::default();
        for addr in ["127.0.0.1:4319", "[::1]:4319", "127.0.0.5:4319"] {
            config.server.listen = addr.parse().unwrap();
            config
                .validate()
                .unwrap_or_else(|e| panic!("{addr} should be fine: {e}"));
        }
    }

    #[test]
    fn retention_shorter_than_a_segment_is_refused() {
        let mut config = Config::default();
        config.retention.logs = DurationSetting(Duration::from_secs(60));
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("retention.logs"), "{err}");
        assert!(err.contains("segment_duration"), "{err}");
    }

    #[test]
    fn disk_budget_must_hold_several_segments() {
        let mut config = Config::default();
        config.storage.disk_budget = ByteSize::mib(64);
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("disk_budget"), "{err}");
    }

    #[test]
    fn per_app_series_cap_above_the_global_cap_is_refused() {
        let mut config = Config::default();
        config.limits.max_series_per_app = config.limits.max_series + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn duplicate_scrape_jobs_are_refused() {
        let mut config = Config::default();
        let job = |name: &str| ScrapeConfig {
            job: name.to_owned(),
            targets: vec!["127.0.0.1:9000".to_owned()],
            interval: default_scrape_interval(),
            timeout: default_scrape_timeout(),
            path: default_scrape_path(),
            labels: BTreeMap::new(),
        };
        config.scrape = vec![job("a"), job("a")];
        assert!(config.validate().is_err());

        config.scrape = vec![job("a"), job("b")];
        config.validate().unwrap();
    }

    #[test]
    fn durations_serialise_back_as_readable_strings() {
        let json = serde_json::to_string(&DurationSetting(day(7))).unwrap();
        assert_eq!(json, "\"7days\"");
        let round: DurationSetting = serde_json::from_str("\"7d\"").unwrap();
        assert_eq!(round.get(), day(7));
    }

    #[test]
    fn serialising_the_config_never_leaks_a_token() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("TELEMETRYD_AUTH_INGEST_TOKEN", "hunter2-do-not-log-me");
            let loaded = Config::load(None, &Overrides::default()).unwrap();
            assert!(!loaded.config.auth.ingest_token.is_empty());

            let dumped = serde_json::to_string(&loaded.config).unwrap();
            assert!(
                !dumped.contains("hunter2"),
                "token leaked into config dump: {dumped}"
            );
            let debugged = format!("{:?}", loaded.config);
            assert!(
                !debugged.contains("hunter2"),
                "token leaked into Debug: {debugged}"
            );
            Ok(())
        });
    }

    #[test]
    fn an_empty_data_dir_is_the_same_configuration_as_an_absent_one() {
        let explicit: StorageConfig = toml::from_str(r#"data_dir = """#).unwrap();
        assert_eq!(explicit.data_dir, None);
        assert_eq!(
            explicit.resolve_data_dir(),
            StorageConfig::default().resolve_data_dir()
        );

        // Whitespace counts as empty, so a stray env var does not become a literal path.
        let padded: StorageConfig = toml::from_str(r#"data_dir = "  ""#).unwrap();
        assert_eq!(padded.data_dir, None);

        let set: StorageConfig = toml::from_str(r#"data_dir = "/var/lib/telemetryd""#).unwrap();
        assert_eq!(set.data_dir, Some(PathBuf::from("/var/lib/telemetryd")));
    }

    #[test]
    fn data_dir_resolution_prefers_explicit_then_existing_cwd_dir() {
        let dir = tempfile::tempdir().unwrap();

        let mut storage = StorageConfig {
            data_dir: Some(dir.path().join("explicit")),
            ..StorageConfig::default()
        };
        assert_eq!(storage.resolve_data_dir(), dir.path().join("explicit"));

        // With nothing explicit we must not silently pick a *different* directory
        // than a previous run in this working directory.
        storage.data_dir = None;
        let resolved = storage.resolve_data_dir();
        assert!(resolved.is_absolute() || resolved == Path::new("./telemetryd-data"));
    }

    /// figment names its own profile in the key path, and the user's file has no such
    /// section. Someone reading `key "default.retention.lgos"` goes looking for a
    /// `[default]` table, does not find one, and stops trusting the message.
    #[test]
    fn a_bad_key_is_reported_by_the_path_the_user_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("telemetryd.toml");
        std::fs::write(&path, "[retention]\nlgos = \"7d\"\n").unwrap();

        let error = Config::load(Some(&path), &Overrides::default())
            .expect_err("a misspelt key must not load")
            .to_string();

        assert!(
            error.contains("retention.lgos"),
            "the real key should be named: {error}"
        );
        assert!(
            !error.contains("default.retention"),
            "figment's profile should not leak into the message: {error}"
        );
        // And it should still suggest what was expected.
        assert!(error.contains("logs"), "{error}");
    }
}
