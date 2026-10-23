//! What `telemetryd clickhouse check` checks: the account, the database, the tables, the
//! reads `/status` needs, the cluster and the storage policy — each on its own, so one
//! failure does not hide the next.

use std::ops::ControlFlow;

use telemetryd_core::config::{ClickHouseConfig, RetentionConfig};

use super::{Client, schema, sql};

type Checks = Vec<(String, Option<String>)>;

pub(super) fn run(config: &ClickHouseConfig, retention: &RetentionConfig) -> Checks {
    let mut checks = Vec::new();
    let client = match Client::new(config) {
        Ok(client) => client,
        Err(error) => {
            checks.push((
                "the password can be read".to_owned(),
                Some(error.to_string()),
            ));
            return checks;
        }
    };
    let server = client.in_database("default");
    let reached = server.probe("SELECT 1").err().map(|e| e.to_string());
    let unreachable = reached.is_some();
    checks.push((
        format!(
            "{} answers, and accepts {:?}",
            client.snapshot().url,
            config.user
        ),
        reached,
    ));
    if unreachable {
        return checks;
    }
    tables(&client, &server, config, retention, &mut checks);
    checks.push((
        "system.parts can be read, for what each app holds".to_owned(),
        server
            .probe("SELECT count() FROM system.parts WHERE 0")
            .err()
            .map(|e| e.to_string()),
    ));
    cluster(&server, config, &mut checks);
    policy(&server, config, &mut checks);
    checks
}

/// Whether `sql` returns a row.
fn any(server: &Client, sql: &str) -> bool {
    let mut found = false;
    let _ = server.query_lines(sql, |_| {
        found = true;
        Ok(ControlFlow::Break(()))
    });
    found
}

fn tables(
    client: &Client,
    server: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
    checks: &mut Checks,
) {
    let database = format!("database {:?} exists", config.database);
    if !any(
        server,
        &format!(
            "SELECT 1 AS one FROM system.databases WHERE name = {}",
            sql::quote(&config.database)
        ),
    ) {
        checks.push((
            database,
            (!config.create_schema)
                .then(|| "it does not, and clickhouse.create_schema = false".to_owned()),
        ));
        return;
    }
    checks.push((database, None));
    let what = "every table and view is there".to_owned();
    match schema::missing(client, config, retention) {
        Ok(missing) if missing.is_empty() => checks.push((what, None)),
        Ok(missing) => checks.push((
            what,
            Some(if config.create_schema {
                format!(
                    "missing {}; the next start creates them",
                    missing.join(", ")
                )
            } else {
                format!(
                    "missing {}, and clickhouse.create_schema = false",
                    missing.join(", ")
                )
            }),
        )),
        Err(error) => checks.push((
            "the tables can be listed".to_owned(),
            Some(error.to_string()),
        )),
    }
}

fn cluster(server: &Client, config: &ClickHouseConfig, checks: &mut Checks) {
    let cluster = config.cluster.trim();
    if cluster.is_empty() {
        return;
    }
    let known = any(
        server,
        &format!(
            "SELECT 1 AS one FROM system.clusters WHERE cluster = {} LIMIT 1",
            sql::quote(cluster)
        ),
    );
    checks.push((
        format!("cluster {cluster:?} is configured on the server"),
        (!known).then(|| "system.clusters does not list it".to_owned()),
    ));
}

fn policy(server: &Client, config: &ClickHouseConfig, checks: &mut Checks) {
    let policy = config.storage_policy.trim();
    if policy.is_empty() {
        return;
    }
    let mut volumes = Vec::new();
    let _ = server.query_lines(
        &format!(
            "SELECT volume_name AS v FROM system.storage_policies WHERE policy_name = {}",
            sql::quote(policy)
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            if let Some(v) = value.get("v").and_then(serde_json::Value::as_str) {
                volumes.push(v.to_owned());
            }
            Ok(ControlFlow::Continue(()))
        },
    );
    checks.push((
        format!("storage policy {policy:?} exists"),
        volumes
            .is_empty()
            .then(|| "system.storage_policies does not list it".to_owned()),
    ));
    if !config.cold_after.is_zero() && !volumes.is_empty() {
        checks.push((
            format!(
                "storage policy {policy:?} has volume {:?}",
                config.cold_volume
            ),
            (!volumes.contains(&config.cold_volume))
                .then(|| format!("its volumes are {}", volumes.join(", "))),
        ));
    }
}
