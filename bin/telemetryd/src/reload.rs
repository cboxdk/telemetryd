//! Re-reading configuration on `SIGHUP`.
//!
//! Reload was originally declined,
//! on the grounds that a fast-starting process with a durable log makes restarting
//! cheap. That reasoning holds for most settings and does not hold for the ones people
//! actually need to change in a hurry: a disk filling at three in the morning is
//! fixed by shortening retention, and restarting the observability backend during an
//! incident means losing the live tail of the incident.
//!
//! So reload is deliberately narrow rather than general. Four things change:
//!
//! - the static credentials — `auth.ingest_token`, `auth.query_token`,
//!   `auth.admin_token` and `[[relay.client]]` — because the moment you need to revoke
//!   a leaked token is not a moment to restart the service
//! - `retention.*` — the windows per signal
//! - `storage.disk_budget` — the ceiling the reaper enforces
//! - `log.level` — because turning up logging should not require restarting the
//!   process whose behaviour you are trying to observe
//!
//! Everything else is refused **by name**. A reload that silently ignored half the
//! file would be worse than no reload at all: the operator would believe a setting had
//! taken effect when it had not, and would go looking for the problem somewhere else.

use telemetryd_core::Config;
use telemetryd_core::config::Overrides;
use telemetryd_server::AppState;
use telemetryd_server::state::Credentials;

use crate::logging::LevelHandle;

/// Settings that cannot change without reopening something, and what to say about each.
///
/// Listed explicitly rather than derived, so adding a config field forces a decision
/// about which side of the line it falls on instead of defaulting to silence.
fn immutable_differences(old: &Config, new: &Config) -> Vec<String> {
    let mut differences = Vec::new();

    let mut note = |name: &str, old: String, new: String| {
        if old != new {
            differences.push(format!("{name} ({old} -> {new})"));
        }
    };

    note(
        "server.listen",
        old.server.listen.to_string(),
        new.server.listen.to_string(),
    );
    note(
        "storage.data_dir",
        old.storage.resolve_data_dir().display().to_string(),
        new.storage.resolve_data_dir().display().to_string(),
    );
    note(
        "storage.max_segment_bytes",
        old.storage.max_segment_bytes.to_string(),
        new.storage.max_segment_bytes.to_string(),
    );
    note(
        "storage.segment_duration",
        format!("{}s", old.storage.segment_duration.get().as_secs()),
        format!("{}s", new.storage.segment_duration.get().as_secs()),
    );
    note(
        "storage.compression",
        format!("{:?}", old.storage.compression),
        format!("{:?}", new.storage.compression),
    );
    note(
        "storage.wal_sync",
        format!("{:?}", old.storage.wal_sync),
        format!("{:?}", new.storage.wal_sync),
    );
    note(
        "server.max_body_bytes",
        old.server.max_body_bytes.to_string(),
        new.server.max_body_bytes.to_string(),
    );
    // Cbox ID holds a key cache and a refresh task built for one issuer, and TLS a
    // listener built for one certificate; both are rebuilt only by a restart. Relay
    // forwarding holds its cursor and upstream for the life of the process. Compared by
    // everything that identifies them, so a change is named rather than lost.
    note(
        "auth.oidc",
        format!(
            "{} {} {}",
            old.auth.oidc.issuer, old.auth.oidc.audience, old.auth.oidc.jwks_url
        ),
        format!(
            "{} {} {}",
            new.auth.oidc.issuer, new.auth.oidc.audience, new.auth.oidc.jwks_url
        ),
    );
    note(
        "server.tls",
        format!("{:?}", old.server.tls),
        format!("{:?}", new.server.tls),
    );
    // Named without its values: an upstream URL can carry credentials in its userinfo.
    if old.relay.upstream != new.relay.upstream {
        differences.push("relay.upstream (changed)".to_owned());
    }

    differences
}

/// Re-read configuration and apply the parts that can change.
///
/// Never returns an error to the caller: a malformed file must leave a running server
/// running on its previous configuration. That is the whole reason reload is safe to
/// wire to a signal — the worst outcome is a log line saying nothing changed.
pub fn apply(
    config_file: Option<&std::path::Path>,
    overrides: &Overrides,
    current: &Config,
    state: &AppState,
    level: &LevelHandle,
) {
    let loaded = match Config::load(config_file, overrides) {
        Ok(loaded) => loaded,
        Err(error) => {
            tracing::error!(%error, "reload failed; keeping the running configuration");
            return;
        }
    };
    // Validated as the running process would be: the listen address cannot change
    // without a restart, so the exposed-bind rule is judged against the one in use. A
    // file that dropped the last token from a public listener is refused whole, rather
    // than opening the surface until someone noticed.
    let mut effective = loaded.config.clone();
    effective.server.listen = current.server.listen;
    if let Err(error) = effective.validate() {
        tracing::error!(%error, "reloaded configuration is invalid; keeping the running one");
        return;
    }
    for warning in &loaded.warnings {
        tracing::warn!("{warning}");
    }

    let mut changes = state.store.apply_retention_policy(&loaded.config);

    // Named, never valued: a token must not reach a log line even as a diff.
    match Credentials::resolve(&loaded.config) {
        Ok(fresh) => changes.extend(
            state
                .replace_credentials(fresh)
                .into_iter()
                .map(|name| format!("{name} replaced")),
        ),
        Err(error) => tracing::error!(
            %error,
            "the tokens could not be read; the ones in force stay in force"
        ),
    }

    if loaded.config.log.level != current.log.level {
        match level.set(&loaded.config.log.level) {
            Ok(()) => changes.push(format!(
                "log.level {} -> {}",
                current.log.level, loaded.config.log.level
            )),
            // RUST_LOG overrides the file at startup; say so rather than letting the
            // operator conclude the reload silently failed.
            Err(error) => tracing::warn!(%error, "log.level not applied"),
        }
    }

    for refused in immutable_differences(current, &loaded.config) {
        tracing::warn!(
            setting = %refused,
            "this setting changed in the file but cannot be applied to a running \
             process — restart telemetryd for it to take effect"
        );
    }

    if changes.is_empty() {
        tracing::info!("configuration reloaded; nothing reloadable had changed");
    } else {
        tracing::info!(changes = ?changes, "configuration reloaded");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn an_unchanged_configuration_reports_no_immutable_differences() {
        let config = Config::default();
        assert!(immutable_differences(&config, &config).is_empty());
    }

    #[test]
    fn a_changed_listen_address_is_named_rather_than_ignored() {
        let old = Config::default();
        let mut new = Config::default();
        new.server.listen = "0.0.0.0:9999".parse().unwrap();

        let differences = immutable_differences(&old, &new);
        assert_eq!(differences.len(), 1);
        assert!(
            differences[0].contains("server.listen"),
            "expected the setting named, got {differences:?}"
        );
        // The operator has to be able to see both values to know what was refused.
        assert!(differences[0].contains("9999"), "{differences:?}");
    }

    #[test]
    fn every_immutable_setting_is_detected() {
        // Guards the list itself: a field added to the immutable set without a `note`
        // call would silently become "reloadable" by omission.
        let old = Config::default();

        let mut segments = Config::default();
        segments.storage.max_segment_bytes = bytesize::ByteSize::mib(999);
        assert!(!immutable_differences(&old, &segments).is_empty());

        let mut body = Config::default();
        body.server.max_body_bytes = bytesize::ByteSize::mib(123);
        assert!(!immutable_differences(&old, &body).is_empty());

        let mut oidc = Config::default();
        oidc.auth.oidc.issuer = "https://id.example".to_owned();
        assert!(immutable_differences(&old, &oidc)[0].contains("auth.oidc"));

        let mut tls = Config::default();
        tls.server.tls.self_signed = "localhost".to_owned();
        assert!(immutable_differences(&old, &tls)[0].contains("server.tls"));
    }

    /// An upstream URL may carry credentials in its userinfo, so a changed one is named
    /// and its values are not.
    #[test]
    fn a_changed_upstream_is_named_without_its_credentials() {
        let old = Config::default();
        let mut new = Config::default();
        new.relay.upstream = "https://user:secret@upstream.example".to_owned();
        let differences = immutable_differences(&old, &new);
        assert_eq!(differences, ["relay.upstream (changed)"]);
    }
}
