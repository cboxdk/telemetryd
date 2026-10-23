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

mod check;
mod client;
pub mod logs;
pub mod metric_folds;
pub mod metrics;
pub mod metrics_schema;
mod scan;
pub mod schema;
pub mod spans;
pub(crate) mod sql;
mod traceql;

pub use client::{Client, Operation, Stats, StatsSnapshot};
pub use logs::ClickHouseLogs;
pub use metrics::ClickHouseMetrics;
pub use spans::ClickHouseSpans;

use telemetryd_core::Result;
use telemetryd_core::config::{ClickHouseConfig, RetentionConfig};

/// Everything kept in ClickHouse, behind one connection.
#[derive(Debug)]
pub struct ClickHouse {
    client: Client,
    pub logs: ClickHouseLogs,
    pub metrics: ClickHouseMetrics,
    pub spans: ClickHouseSpans,
}

impl ClickHouse {
    /// Connect, and create what is missing when the configuration says to.
    ///
    /// # Errors
    /// When the password cannot be resolved or ClickHouse refuses the schema: a startup
    /// error, not a first failed request.
    pub fn open(config: &ClickHouseConfig, retention: &RetentionConfig) -> Result<Self> {
        let client = Client::new(config)?;
        if config.create_schema {
            schema::ensure(&client, config, retention)?;
        } else {
            // Someone else manages the schema: find out now that it is not all there,
            // rather than from the first write refused.
            let missing = schema::missing(&client, config, retention)?;
            if !missing.is_empty() {
                return Err(telemetryd_core::Error::Config(format!(
                    "clickhouse.create_schema = false, and database {:?} lacks {}: create \
                     them with the statements `telemetryd clickhouse schema` prints, or let \
                     telemetryd create them",
                    client.database(),
                    missing.join(", ")
                )));
            }
        }
        Ok(Self {
            logs: ClickHouseLogs::new(client.clone()),
            metrics: ClickHouseMetrics::new(client.clone()),
            spans: ClickHouseSpans::new(client.clone()),
            client,
        })
    }

    #[must_use]
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Whether the configured account can do what an instance needs, checked one thing
    /// at a time, without changing anything: `(what was checked, what is wrong)`.
    #[must_use]
    pub fn check(
        config: &ClickHouseConfig,
        retention: &RetentionConfig,
    ) -> Vec<(String, Option<String>)> {
        check::run(config, retention)
    }

    /// What each app holds, from the streams summaries: series and rows exact, bytes
    /// apportioned by each app's share of a table's rows, as the local store does.
    ///
    /// # Errors
    /// When ClickHouse cannot be read.
    pub fn app_usage(&self) -> Result<Vec<crate::AppUsage>> {
        use std::collections::BTreeMap;
        let mut apps: BTreeMap<String, crate::AppUsage> = BTreeMap::new();
        for (table, summary, hash, rows) in [
            (schema::LOGS, schema::LOG_STREAMS, "StreamHash", "Rows"),
            (schema::TRACES, schema::TRACE_STREAMS, "StreamHash", "Rows"),
            (
                metrics_schema::SAMPLES,
                metrics_schema::SERIES,
                "SeriesHash",
                "Samples",
            ),
        ] {
            let mut bytes = 0u64;
            self.client.query_lines(
                &format!(
                    "SELECT sum(data_compressed_bytes) AS bytes FROM system.parts \
                     WHERE active AND database = currentDatabase() AND table = {}",
                    sql::quote(table)
                ),
                |line| {
                    let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
                    bytes = value
                        .get("bytes")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )?;
            let mut rows_here: Vec<(String, u64, u64)> = Vec::new();
            self.client.query_lines(
                &format!(
                    "SELECT ServiceName AS app, uniqExact({hash}) AS series, sum({rows}) AS rows \
                     FROM {summary} GROUP BY app"
                ),
                |line| {
                    let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
                    let number = |n: &str| {
                        value
                            .get(n)
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(0)
                    };
                    let app = value
                        .get("app")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    rows_here.push((app.to_owned(), number("series"), number("rows")));
                    Ok(std::ops::ControlFlow::Continue(()))
                },
            )?;
            let total: u64 = rows_here.iter().map(|(_, _, rows)| rows).sum();
            for (app, series, rows) in rows_here {
                let usage = apps.entry(app.clone()).or_insert_with(|| crate::AppUsage {
                    app,
                    ..crate::AppUsage::default()
                });
                usage.series += series;
                usage.rows += rows;
                if total > 0 {
                    usage.estimated_bytes +=
                        u64::try_from(u128::from(bytes) * u128::from(rows) / u128::from(total))
                            .unwrap_or(u64::MAX);
                }
            }
        }
        Ok(apps.into_values().collect())
    }

    /// Give the tables a changed retention, as a reload does.
    ///
    /// # Errors
    /// When ClickHouse refuses the change.
    pub fn apply_retention(
        &self,
        config: &ClickHouseConfig,
        retention: &RetentionConfig,
    ) -> Result<()> {
        if config.create_schema {
            schema::apply_ttl(&self.client, config, retention)?;
        } else {
            tracing::warn!(
                "retention changed with clickhouse.create_schema = false: telemetryd does not \
                 manage the tables, so their TTLs are as whoever created them set them"
            );
        }
        Ok(())
    }
}
