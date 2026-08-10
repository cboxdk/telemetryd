//! The four telemetry signals telemetryd stores.
//!
//! Logs, traces and events share identical storage machinery and differ only in their
//! Arrow schema; metrics use the separate chunk store. The distinction shows
//! up here as [`Signal::uses_record_store`].

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Signal {
    Logs,
    Traces,
    Metrics,
}

impl Signal {
    pub const ALL: [Self; 3] = [Self::Logs, Self::Traces, Self::Metrics];

    /// Directory name under `wal/` and `segments/`. Stable on disk — changing one of
    /// these is a storage format break.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Traces => "traces",
            Self::Metrics => "metrics",
        }
    }

    /// Whether this signal is stored as WAL → Parquet segments (as opposed to the
    /// metric chunk store).
    pub fn uses_record_store(self) -> bool {
        !matches!(self, Self::Metrics)
    }
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Signal {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|signal| signal.as_str() == s)
            .ok_or_else(|| crate::Error::BadRequest(format!("unknown signal {s:?}")))
    }
}
