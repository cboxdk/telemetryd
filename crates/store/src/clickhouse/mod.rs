//! ClickHouse as the store: one database every telemetryd in front of it shares.
//!
//! The local store keeps telemetry in files only the process that wrote them can read,
//! which is what makes it one machine. Here every instance writes to and reads from the
//! same tables, so any of them answers for all of the data and they scale out behind a
//! load balancer. Nothing is buffered between them: a batch is acknowledged once
//! ClickHouse has it, and is readable by every instance from then on.
//!
//! The tables are the OpenTelemetry Collector's ClickHouse layout — the one
//! `cboxdk/laravel-telemetry-store` and Grafana's ClickHouse plugin read — plus the
//! columns telemetryd needs to answer its own APIs exactly as the local store does. See
//! [`schema`].

mod client;
pub mod logs;
mod scan;
pub mod schema;
pub mod spans;
pub(crate) mod sql;

pub use client::{Client, Operation, Stats, StatsSnapshot};
pub use logs::ClickHouseLogs;
pub use spans::ClickHouseSpans;

use telemetryd_core::Result;
use telemetryd_core::config::ClickHouseConfig;

/// Everything kept in ClickHouse, behind one connection.
#[derive(Debug)]
pub struct ClickHouse {
    client: Client,
    pub logs: ClickHouseLogs,
    pub spans: ClickHouseSpans,
}

impl ClickHouse {
    /// Connect, and create what is missing when the configuration says to.
    ///
    /// # Errors
    /// When the password cannot be resolved or ClickHouse refuses the schema: a startup
    /// error, not a first failed request.
    pub fn open(config: &ClickHouseConfig) -> Result<Self> {
        let client = Client::new(config)?;
        if config.create_schema {
            schema::ensure(&client, config)?;
        }
        Ok(Self {
            logs: ClickHouseLogs::new(client.clone()),
            spans: ClickHouseSpans::new(client.clone()),
            client,
        })
    }

    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }
}
