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
pub mod schema;
pub(crate) mod sql;

pub use client::Client;
pub use logs::ClickHouseLogs;
