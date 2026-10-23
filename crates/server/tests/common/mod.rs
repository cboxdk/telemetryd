//! What the integration tests that run against ClickHouse share.

#![allow(dead_code, clippy::unwrap_used)]

use std::time::Duration;

use telemetryd_core::config::{ClickHouseConfig, DurationSetting, RetentionConfig};

/// A `[clickhouse]` section on a database of its own, or `None` when
/// `TELEMETRYD_TEST_CLICKHOUSE_URL=http://user:password@host:8123` is not set.
///
/// Each call names a fresh database, so tests never read each other's rows. Pair it with
/// [`long_retention`]: the corpora are dated, and a week's TTL would have ClickHouse
/// drop them on insert.
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
        ..ClickHouseConfig::default()
    })
}

/// Say that a test needing `variable` did not run — or, when
/// `TELEMETRYD_TEST_CLICKHOUSE_REQUIRED=1` is set, as CI sets it, fail it: a job meant to
/// test against ClickHouse that tested nothing should not pass.
pub(crate) fn skip(variable: &str) {
    assert!(
        std::env::var("TELEMETRYD_TEST_CLICKHOUSE_REQUIRED").as_deref() != Ok("1"),
        "{variable} is not set, and TELEMETRYD_TEST_CLICKHOUSE_REQUIRED=1 says it must be"
    );
    eprintln!("{variable} is not set; skipped");
}

/// Ten years of everything: what a test against ClickHouse keeps its dated rows for.
pub(crate) fn long_retention() -> RetentionConfig {
    let ten_years = DurationSetting(Duration::from_secs(3650 * 86_400));
    RetentionConfig {
        logs: ten_years,
        traces: ten_years,
        metrics: ten_years,
    }
}
