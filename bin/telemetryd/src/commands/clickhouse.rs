//! `telemetryd clickhouse` — for a deployment with ClickHouse as the store.
//!
//! `schema` prints every statement an instance would run to create and age its tables,
//! for a database whose schema someone else manages (`clickhouse.create_schema =
//! false`). `check` connects with the configured account and checks, one thing at a
//! time and changing nothing, that it can do what an instance needs: what to run before
//! a deployment rather than learn from its log.

use telemetryd_core::Config;
use telemetryd_core::config::Overrides;

#[derive(Debug, clap::Subcommand)]
pub enum ClickHouseAction {
    /// Print the statements that create and age the tables, for a DBA to run.
    Schema(crate::ConfigArgs),
    /// Check that the configured account can do what an instance needs.
    Check(crate::ConfigArgs),
}

pub fn run(action: &ClickHouseAction) -> anyhow::Result<()> {
    let (ClickHouseAction::Schema(args) | ClickHouseAction::Check(args)) = action;
    let overrides = Overrides {
        data_dir: args.data_dir.clone(),
        ..Overrides::default()
    };
    let config = Config::load(args.config.as_deref(), &overrides)?.config;
    if !config.clickhouse.is_enabled() {
        anyhow::bail!("this configuration has no [clickhouse] section: clickhouse.url is not set");
    }
    // The CA bundle `serve` trusts, so `check` reaches ClickHouse as an instance would.
    if let Some(ca_file) = &config.tls.ca_file {
        telemetryd_core::http::init_trust(ca_file).map_err(|e| anyhow::anyhow!(e))?;
    }
    match action {
        ClickHouseAction::Schema(_) => {
            print_schema(&config);
            Ok(())
        }
        ClickHouseAction::Check(_) => check(&config),
    }
}

fn print_schema(config: &Config) {
    println!(
        "-- telemetryd {}: the schema for database {:?}{}.",
        env!("CARGO_PKG_VERSION"),
        config.clickhouse.database,
        if config.clickhouse.cluster.trim().is_empty() {
            String::new()
        } else {
            format!(" on cluster {:?}", config.clickhouse.cluster)
        }
    );
    println!("-- Every statement is idempotent; run them in this order.\n");
    let cluster = config.clickhouse.cluster.trim();
    println!(
        "CREATE DATABASE IF NOT EXISTS `{}`{};\n",
        config.clickhouse.database.replace('`', "\\`"),
        if cluster.is_empty() {
            String::new()
        } else {
            format!(" ON CLUSTER '{}'", cluster.replace('\'', "\\'"))
        }
    );
    for statement in
        telemetryd_store::clickhouse::schema::statements(&config.clickhouse, &config.retention)
            .into_iter()
            .chain(telemetryd_store::clickhouse::schema::ttl_statements(
                &config.clickhouse,
                &config.retention,
            ))
    {
        println!("{statement};\n");
    }
}

fn check(config: &Config) -> anyhow::Result<()> {
    let checks =
        telemetryd_store::clickhouse::ClickHouse::check(&config.clickhouse, &config.retention);
    let mut failed = 0;
    for (what, problem) in &checks {
        match problem {
            None => println!("  ok    {what}"),
            Some(problem) => {
                failed += 1;
                println!("  FAIL  {what}\n        {problem}");
            }
        }
    }
    if failed > 0 {
        anyhow::bail!("{failed} of {} checks failed", checks.len());
    }
    println!("\nevery check passed");
    Ok(())
}
