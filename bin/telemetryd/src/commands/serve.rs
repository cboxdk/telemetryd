//! `telemetryd serve`

use std::sync::Arc;

use anyhow::Context;
use telemetryd_core::Config;
use telemetryd_core::config::Overrides;
use telemetryd_store::Store;

pub fn run(config_file: Option<&std::path::Path>, overrides: &Overrides) -> anyhow::Result<()> {
    let loaded = Config::load(config_file, overrides)?;
    crate::logging::init(&loaded.config.log, &loaded.warnings)?;

    if let Some(path) = &loaded.config_file {
        tracing::info!(path = %path.display(), "loaded configuration file");
    }

    let config = Arc::new(loaded.config);
    let store = Arc::new(open_store(&config)?);

    report_recovery(&store);

    // A multi-threaded runtime built here rather than via `#[tokio::main]`, so the
    // non-serving subcommands do not pay for a runtime they never use.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("building the tokio runtime")?;

    runtime.block_on(telemetryd_server::serve(config, Arc::clone(&store)))?;
    Ok(())
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
