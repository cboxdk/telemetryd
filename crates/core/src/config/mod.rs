//! Configuration schema and the defaults → file → env → flags layering described in
//! ADR-003.
//!
//! The load-bearing property here is that **the empty configuration is a valid
//! configuration**: every field has a default, so `telemetryd serve` with no file, no
//! environment and no flags is a complete, supported setup. Everything else is an
//! override on top of that.

use bytesize::ByteSize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use figment::Figment;
use figment::providers::{Env, Format, Toml};

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Strip figment's internal profile name out of the key path it reports.
///
/// figment reports `key "default.retention.lgos"`, where `default` is its own profile
/// concept and appears nowhere in the user's file. Someone reading the error goes
/// looking for a `[default]` section, does not find one, and concludes the message is
/// about something else. Everything after the profile is the real path.
fn readable_figment_error(error: &figment::Error) -> String {
    error.to_string().replace("key \"default.", "key \"")
}
mod env;
mod schema;

pub use env::env_var_path;
use env::{ENV_KEYS, unknown_env_var_warnings};
pub use schema::*;

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

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

    /// Relay mode's own checks, kept out of `validate` so neither grows past reading.
    fn validate_relay(&self) -> Result<()> {
        if !self.relay.is_enabled() {
            return Ok(());
        }

        let upstream = self.relay.upstream.trim();
        if !upstream.starts_with("https://") && !is_loopback(upstream) {
            return Err(Error::Config(format!(
                "relay.upstream must be https (got {upstream:?}). Everything this \
                 instance accepts is forwarded there, so plaintext exposes every \
                 record and the upstream credential with it. Loopback is allowed \
                 for testing."
            )));
        }

        for client in &self.relay.client {
            if client.app.trim().is_empty() {
                return Err(Error::Config(
                    "every relay.client needs a non-empty app: it is the identity \
                     stamped onto that client's records, and an empty one would \
                     make its telemetry indistinguishable from an unidentified \
                     producer's"
                        .to_owned(),
                ));
            }
            if client.token.is_empty() {
                return Err(Error::Config(format!(
                    "relay.client {:?} has no token. Without one it cannot be \
                     identified, which is the only thing a relay client is for",
                    client.app
                )));
            }
        }

        // A bare ingest token has no app attached to it, so there is nothing to
        // stamp. Silently trusting the payload for those writers would leave a
        // hole in exactly the boundary this mode exists to draw, so it is refused
        // rather than quietly special-cased.
        if !self.relay.trust_client_identity && !self.auth.ingest_token.is_empty() {
            return Err(Error::Config(
                "auth.ingest_token is set while relay.trust_client_identity is \
                 false. A bare ingest token carries no identity, so records \
                 arriving with it could only keep the app they claim — which is \
                 the impersonation this mode prevents. Move those writers to \
                 [[relay.client]] entries, or set trust_client_identity = true if \
                 every writer really is trusted."
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// TLS termination rules, kept out of `validate` so neither outgrows a reading.
    fn validate_server_tls(&self) -> Result<()> {
        let tls = &self.server.tls;
        // Both routes at once is ambiguous, and guessing which one the operator meant
        // is how an instance ends up serving a certificate nobody intended.
        if tls.is_self_signed() && (tls.cert_file.is_some() || tls.key_file.is_some()) {
            return Err(Error::Config(
                "server.tls.self_signed cannot be combined with cert_file or key_file. \
             Use the certificate you have, or generate one — not both."
                    .to_owned(),
            ));
        }
        if !tls.is_self_signed()
            && tls.is_enabled()
            && (tls.cert_file.is_none() || tls.key_file.is_none())
        {
            return Err(Error::Config(
                "server.tls needs both cert_file and key_file. One without the other \
             would leave telemetryd serving plain HTTP while looking configured \
             for TLS."
                    .to_owned(),
            ));
        }
        Ok(())
    }

    /// Cross-field rules. Run at load time rather than at first use, so a bad
    /// configuration fails at startup instead of at 3am on the first query.
    pub fn validate(&self) -> Result<()> {
        // Half a TLS configuration is always a mistake, and the failure it causes
        // otherwise is a server that quietly keeps speaking plain HTTP.
        self.validate_server_tls()?;

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

        if self.auth.oidc.is_enabled() {
            let issuer = self.auth.oidc.issuer.trim();
            // Keys fetched over plaintext can be substituted in flight, and whoever
            // substitutes them mints their own admin tokens. Loopback is exempt
            // because that is how it is tested.
            if !issuer.starts_with("https://") && !is_loopback(issuer) {
                return Err(Error::Config(format!(
                    "auth.oidc.issuer must be https (got {issuer:?}).\n\
                     telemetryd fetches the issuer's signing keys from it, so anyone \
                     able to answer that request over plaintext can mint tokens this \
                     instance will accept."
                )));
            }
            // The same reasoning covers an explicit key URL. It is the *only* thing
            // that decides which keys mint valid admin tokens, so it gets the same
            // rule as the issuer it replaces rather than a weaker one.
            let jwks = self.auth.oidc.jwks_url.trim();
            if !jwks.is_empty() && !jwks.starts_with("https://") && !is_loopback(jwks) {
                return Err(Error::Config(format!(
                    "auth.oidc.jwks_url must be https (got {jwks:?}).\n\
                     It replaces the path derived from the issuer, so anyone able to \
                     answer that request over plaintext can mint tokens this instance \
                     will accept."
                )));
            }
            if self.auth.oidc.scope_claim.trim().is_empty() {
                return Err(Error::Config(
                    "auth.oidc.scope_claim must name a claim (the default is \"scope\"); \
                     empty would match nothing and refuse every token"
                        .to_owned(),
                ));
            }
            for (name, scope) in [
                ("scope_write", &self.auth.oidc.scope_write),
                ("scope_read", &self.auth.oidc.scope_read),
                ("scope_admin", &self.auth.oidc.scope_admin),
            ] {
                if scope.trim().is_empty() || scope.contains(' ') {
                    return Err(Error::Config(format!(
                        "auth.oidc.{name} must be a single non-empty scope (got \
                         {scope:?}); scopes are matched whole against a \
                         space-separated claim"
                    )));
                }
            }
        }

        self.validate_relay()?;

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

        Ok(())
    }
}

/// Loopback is exempt from the https rules: it is how the OIDC paths are tested, and
/// a plaintext hop that never leaves the machine cannot be substituted in flight.
fn is_loopback(url: &str) -> bool {
    url.starts_with("http://127.0.0.1")
        || url.starts_with("http://localhost")
        || url.starts_with("http://[::1]")
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

#[cfg(test)]
// `figment::Jail` closures return figment's own large error type; that is the test
// harness's shape, not ours.
#[allow(clippy::unwrap_used, clippy::result_large_err)]
mod tests {
    use std::time::Duration;

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

    /// The key path is a guess about the provider unless it is stated, so the
    /// configured value has to win. Google publishes nowhere near its issuer.
    #[test]
    fn a_configured_key_url_wins_over_the_derived_one() {
        let mut oidc = OidcConfig {
            issuer: "https://accounts.google.com".to_owned(),
            ..OidcConfig::default()
        };
        assert_eq!(
            oidc.jwks_url(),
            "https://accounts.google.com/.well-known/jwks.json"
        );
        oidc.jwks_url = "https://www.googleapis.com/oauth2/v3/certs".to_owned();
        assert_eq!(
            oidc.jwks_url(),
            "https://www.googleapis.com/oauth2/v3/certs"
        );
    }

    /// An explicit key URL decides which keys mint admin tokens, so plaintext is
    /// refused there for the same reason it is refused on the issuer.
    #[test]
    fn a_plaintext_key_url_is_refused() {
        let config = |jwks: &str| -> Config {
            toml::from_str(&format!(
                "[auth]\nadmin_token = \"t\"\n[auth.oidc]\n\
                 issuer = \"https://acme.cboxid.com\"\njwks_url = \"{jwks}\"\n"
            ))
            .expect("the fixture parses")
        };

        let error = config("http://keys.example.com/jwks.json")
            .validate()
            .expect_err("plaintext keys must be refused");
        assert!(error.to_string().contains("jwks_url must be https"));

        // Loopback stays usable, because that is how this path is tested.
        config("http://127.0.0.1:9000/jwks.json")
            .validate()
            .expect("loopback keys are allowed");
    }

    /// Empty would match no claim at all and refuse every token — a silent lockout
    /// that looks like a signature problem.
    #[test]
    fn an_empty_scope_claim_is_refused() {
        let config: Config = toml::from_str(
            "[auth]\nadmin_token = \"t\"\n[auth.oidc]\n\
             issuer = \"https://acme.cboxid.com\"\nscope_claim = \"  \"\n",
        )
        .expect("the fixture parses");
        let error = config
            .validate()
            .expect_err("an empty claim name must be refused");
        assert!(error.to_string().contains("scope_claim"));
    }
}
