//! Logging setup.
//!
//! Initialised *after* configuration is loaded, so `log.level` from the file actually
//! takes effect. Anything the loader wanted to say is buffered and replayed here.

use anyhow::Context;
use telemetryd_core::config::{LogConfig, LogFormat};
use tracing_subscriber::EnvFilter;

/// Install the global subscriber and replay any warnings collected during load.
pub fn init(config: &LogConfig, deferred_warnings: &[String]) -> anyhow::Result<()> {
    // RUST_LOG wins when set: an operator debugging a specific module should not have
    // to edit the config file to do it.
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::try_new(&config.level)
            .with_context(|| format!("log.level {:?} is not a valid filter", config.level))?,
    };

    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match config.format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Text => builder.init(),
    }

    for warning in deferred_warnings {
        tracing::warn!("{warning}");
    }
    Ok(())
}
