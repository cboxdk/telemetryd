//! Logging setup.
//!
//! Initialised *after* configuration is loaded, so `log.level` from the file actually
//! takes effect. Anything the loader wanted to say is buffered and replayed here.

use anyhow::Context;
use telemetryd_core::config::{LogConfig, LogFormat};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, reload};

/// Changes the log level of a running process.
///
/// Turning up logging is the first thing anyone does when a production problem
/// appears, and needing a restart to do it means restarting the process whose
/// behaviour you were trying to observe.
#[derive(Clone)]
pub struct LevelHandle(reload::Handle<EnvFilter, tracing_subscriber::Registry>);

impl std::fmt::Debug for LevelHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LevelHandle")
    }
}

impl LevelHandle {
    /// Swap the filter. Returns an error rather than panicking on an unparseable
    /// level: a bad value in a reloaded file must leave the running filter alone.
    pub fn set(&self, level: &str) -> anyhow::Result<()> {
        let filter = EnvFilter::try_new(level)
            .with_context(|| format!("{level:?} is not a valid filter"))?;
        self.0
            .reload(filter)
            .context("installing the new log filter")?;
        Ok(())
    }
}

/// Install the global subscriber and replay any warnings collected during load.
pub fn init(config: &LogConfig, deferred_warnings: &[String]) -> anyhow::Result<LevelHandle> {
    // RUST_LOG wins when set: an operator debugging a specific module should not have
    // to edit the config file to do it.
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::try_new(&config.level)
            .with_context(|| format!("log.level {:?} is not a valid filter", config.level))?,
    };

    let (filter, handle) = reload::Layer::new(filter);
    let registry = tracing_subscriber::registry().with(filter);
    match config.format {
        LogFormat::Json => registry
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
        LogFormat::Text => registry.with(tracing_subscriber::fmt::layer()).init(),
    }

    for warning in deferred_warnings {
        tracing::warn!("{warning}");
    }
    Ok(LevelHandle(handle))
}
