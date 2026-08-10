//! The environment-variable surface: which `TELEMETRYD_*` name maps to which
//! configuration path, and the warning for one that maps to nothing.
//!
//! Its own module because it is a list that only ever grows, and because it is the one
//! part of the configuration with no logic in it — a table and two lookups over it.

/// Explicit env-var → config-path mapping.
///
/// figment's generic `Env::split("_")` cannot work here: it would turn
/// `TELEMETRYD_STORAGE_DATA_DIR` into `storage.data.dir`. A table is more typing but
/// buys the documented naming, plus the ability to detect a typo'd `TELEMETRYD_*`
/// variable instead of ignoring it silently.
pub(super) const ENV_KEYS: &[(&str, &str)] = &[
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
    ("TELEMETRYD_AUTH_ADMIN_TOKEN", "auth.admin_token"),
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
        "TELEMETRYD_LIMITS_INGEST_QUEUE_DEPTH",
        "limits.ingest_queue_depth",
    ),
    (
        "TELEMETRYD_INGEST_TRUNCATE_OVERSIZED_BODIES",
        "ingest.truncate_oversized_bodies",
    ),
    // Cbox ID / OIDC. Absent until now, which meant the one deployment shape that
    // most needs env-only configuration — a container — could not turn SSO on at all
    // without baking a file into the image.
    ("TELEMETRYD_AUTH_OIDC_ISSUER", "auth.oidc.issuer"),
    ("TELEMETRYD_AUTH_OIDC_AUDIENCE", "auth.oidc.audience"),
    ("TELEMETRYD_AUTH_OIDC_JWKS_URL", "auth.oidc.jwks_url"),
    ("TELEMETRYD_AUTH_OIDC_SCOPE_CLAIM", "auth.oidc.scope_claim"),
    ("TELEMETRYD_AUTH_OIDC_SCOPE_WRITE", "auth.oidc.scope_write"),
    ("TELEMETRYD_AUTH_OIDC_SCOPE_READ", "auth.oidc.scope_read"),
    ("TELEMETRYD_AUTH_OIDC_SCOPE_ADMIN", "auth.oidc.scope_admin"),
    (
        "TELEMETRYD_AUTH_OIDC_REFRESH_INTERVAL",
        "auth.oidc.refresh_interval",
    ),
    ("TELEMETRYD_AUTH_OIDC_CLOCK_SKEW", "auth.oidc.clock_skew"),
    // Relay. `relay.client` is deliberately absent: it is a list of tables, and the
    // compact string encodings that would fit an env var are all worse than a mounted
    // file for something whose values are credentials anyway.
    ("TELEMETRYD_RELAY_UPSTREAM", "relay.upstream"),
    ("TELEMETRYD_RELAY_TOKEN", "relay.token"),
    (
        "TELEMETRYD_RELAY_TRUST_CLIENT_IDENTITY",
        "relay.trust_client_identity",
    ),
    ("TELEMETRYD_RELAY_WHEN_FULL", "relay.when_full"),
    ("TELEMETRYD_RELAY_INTERVAL", "relay.interval"),
    (
        "TELEMETRYD_RELAY_MAX_REQUEST_BYTES",
        "relay.max_request_bytes",
    ),
    ("TELEMETRYD_RELAY_MAX_QUEUE_SHARE", "relay.max_queue_share"),
    (
        "TELEMETRYD_SERVER_TLS_SELF_SIGNED",
        "server.tls.self_signed",
    ),
    ("TELEMETRYD_SERVER_TLS_CERT_FILE", "server.tls.cert_file"),
    ("TELEMETRYD_SERVER_TLS_KEY_FILE", "server.tls.key_file"),
    ("TELEMETRYD_TLS_CA_FILE", "tls.ca_file"),
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
/// A `TELEMETRYD_*` variable that matches nothing is almost always a typo. Ignoring it
/// silently means the operator believes a setting is applied when it is not.
pub(super) fn unknown_env_var_warnings() -> Vec<String> {
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
