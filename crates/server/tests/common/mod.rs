//! What the integration tests that run against ClickHouse share.

#![allow(dead_code, clippy::unwrap_used)]

use telemetryd_core::config::ClickHouseConfig;

/// A `[clickhouse]` section on a database of its own, or `None` when
/// `TELEMETRYD_TEST_CLICKHOUSE_URL=http://user:password@host:8123` is not set.
///
/// Each call names a fresh database, so tests never read each other's rows. The corpora
/// are dated, so the TTL is long: a month's would have ClickHouse drop them on insert.
pub(crate) fn clickhouse_under_test(name: &str) -> Option<ClickHouseConfig> {
    let raw = std::env::var("TELEMETRYD_TEST_CLICKHOUSE_URL").ok()?;
    let (scheme, rest) = raw.split_once("://").unwrap();
    let (credentials, host) = rest.rsplit_once('@').unwrap_or(("default:", rest));
    let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Some(ClickHouseConfig {
        url: format!("{scheme}://{host}"),
        database: format!("td_test_{name}_{unique}"),
        user: user.to_owned(),
        password: telemetryd_core::Secret::new(password),
        retention_days: 3_650,
        ..ClickHouseConfig::default()
    })
}
