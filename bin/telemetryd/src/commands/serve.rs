//! `telemetryd serve`

use std::sync::Arc;

use anyhow::Context;
use telemetryd_core::Config;
use telemetryd_core::config::Overrides;
use telemetryd_store::Store;

pub fn run(config_file: Option<&std::path::Path>, overrides: &Overrides) -> anyhow::Result<()> {
    let loaded = Config::load(config_file, overrides)?;
    let level = crate::logging::init(&loaded.config.log, &loaded.warnings)?;

    if let Some(path) = &loaded.config_file {
        tracing::info!(path = %path.display(), "loaded configuration file");
    }

    // Before anything can dial out: the OIDC key fetch happens during startup, and a
    // trust decision made after the first request would be no decision at all.
    if let Some(ca_file) = loaded.config.tls.ca_file.clone() {
        telemetryd_core::http::init_trust(&ca_file).map_err(|e| anyhow::anyhow!(e))?;
        tracing::info!(
            ca_file = %ca_file.display(),
            "outbound TLS verifies against this bundle only"
        );
    }

    let config = Arc::new(loaded.config);
    let store = Arc::new(open_store(&config)?);

    report_recovery(&store);
    warn_if_unbounded(&config);
    for note in config.memory_notes() {
        tracing::warn!("{note}");
    }

    // A multi-threaded runtime built here rather than via `#[tokio::main]`, so the
    // non-serving subcommands do not pay for a runtime they never use.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the tokio runtime")?;

    runtime.block_on(async {
        // SIGHUP re-reads the file and applies retention, the disk budget and the log
        // level. Anything else that changed is refused by name rather than ignored.
        spawn_reload_listener(
            config_file.map(std::path::Path::to_path_buf),
            overrides.clone(),
            Arc::clone(&config),
            Arc::clone(&store),
            level,
        );
        telemetryd_server::serve(config, Arc::clone(&store)).await
    })?;
    Ok(())
}

/// Watch for `SIGHUP` for the life of the process.
///
/// Unix only. Windows has no equivalent, and inventing one (a control socket, a file
/// watcher) is a larger surface than the feature is worth — telemetryd's deployment
/// story is a Linux service.
#[cfg(unix)]
fn spawn_reload_listener(
    config_file: Option<std::path::PathBuf>,
    overrides: Overrides,
    config: Arc<Config>,
    store: Arc<Store>,
    level: crate::logging::LevelHandle,
) {
    tokio::spawn(async move {
        let mut hangup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        {
            Ok(stream) => stream,
            Err(error) => {
                tracing::error!(%error, "could not install the SIGHUP handler; reload is unavailable");
                return;
            }
        };
        tracing::debug!("send SIGHUP to reload retention, the disk budget and the log level");

        while hangup.recv().await.is_some() {
            let (config_file, overrides) = (config_file.clone(), overrides.clone());
            let (config, store, level) = (Arc::clone(&config), Arc::clone(&store), level.clone());
            // The reload reads a file and walks no data, but it is still blocking I/O.
            let _ = tokio::task::spawn_blocking(move || {
                crate::reload::apply(config_file.as_deref(), &overrides, &config, &store, &level);
            })
            .await;
        }
    });
}

#[cfg(not(unix))]
fn spawn_reload_listener(
    _config_file: Option<std::path::PathBuf>,
    _overrides: Overrides,
    _config: Arc<Config>,
    _store: Arc<Store>,
    _level: crate::logging::LevelHandle,
) {
}

fn open_store(config: &Config) -> anyhow::Result<Store> {
    let data_dir = config.storage.resolve_data_dir();
    Store::open(config).with_context(|| {
        format!(
            "opening the data directory at {}\n\
             \n\
             If another telemetryd is already running against it, stop that one first \
             — a data directory has exactly one writer.",
            data_dir.display()
        )
    })
}

/// Say so at startup when nothing bounds this process's memory.
///
/// telemetryd sizes its query concurrency and series budget from what it believes it may
/// use. With a cgroup limit that belief is a fact. Without one it is a guess about a share
/// of a machine telemetryd does not own — and when the guess is wrong the kernel picks a
/// victim, which on a shared box is as likely to be the database as us.
///
/// This is a `WARN` rather than a refusal because plenty of correct deployments have no
/// cgroup: a developer laptop, a foreground process, a container run without limits. But
/// it is the difference between a service that degrades and a server that stops, so it
/// should never be something an operator discovers from `htop` at the wrong moment.
///
/// Skipped when the operator has pinned both limits by hand, which is the other way of
/// having decided this on purpose.
fn warn_if_unbounded(config: &Config) {
    if telemetryd_core::config::memory_is_capped() {
        return;
    }
    if config.limits.query_concurrency != 0 && config.limits.max_series != 0 {
        return;
    }
    tracing::warn!(
        query_concurrency = config.limits.resolved_query_concurrency(),
        max_series = config.limits.resolved_max_series(),
        "no memory limit is in force, so these were derived from a fraction of the \
         host's total memory rather than from a share this process actually owns. On a \
         machine shared with a database or a web server, set MemoryMax= in the systemd \
         unit (`sudo telemetryd service install` writes it), or pin limits.query_concurrency \
         and limits.max_series"
    );
}

/// Report what a previous crash cost us. Loudly, at `WARN`: a truncated write-ahead
/// log means records that were accepted over HTTP did not survive, and that should
/// never be something an operator has to go looking for.
fn report_recovery(store: &Store) {
    let status = match store.snapshot() {
        Ok(status) => status,
        Err(e) => {
            tracing::warn!(error = %e, "could not read storage status at startup");
            return;
        }
    };

    if status.logs.recovered_records > 0 {
        tracing::info!(
            records = status.logs.recovered_records,
            "recovered buffered records from the write-ahead log"
        );
    }
    for truncation in &status.wal_truncations {
        tracing::warn!(
            path = %truncation.path.display(),
            discarded_bytes = truncation.discarded_bytes,
            reason = ?truncation.reason,
            "a previous run did not shut down cleanly; records at the end of the \
             write-ahead log were not durable and have been discarded"
        );
    }
}
