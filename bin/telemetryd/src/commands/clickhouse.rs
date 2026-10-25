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
    Schema(SchemaArgs),
    /// Check that the configured account can do what an instance needs.
    Check(crate::ConfigArgs),
}

#[derive(Debug, clap::Args)]
pub struct SchemaArgs {
    #[command(flatten)]
    config: crate::ConfigArgs,
    /// Which layout to print. `auto` asks the server how many shards
    /// `clickhouse.cluster` has, as an instance does at startup; the others print
    /// without connecting.
    #[arg(long, value_enum, default_value_t = Shards::Auto)]
    shards: Shards,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Shards {
    Auto,
    /// One server, or a cluster of one shard: replicated tables.
    One,
    /// A cluster of several shards: each shard's own tables, Distributed over them.
    Many,
}

pub fn run(action: &ClickHouseAction) -> anyhow::Result<()> {
    let args = match action {
        ClickHouseAction::Schema(schema) => &schema.config,
        ClickHouseAction::Check(args) => args,
    };
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
        ClickHouseAction::Schema(schema) => {
            let sharded = match schema.shards {
                Shards::One => false,
                Shards::Many => true,
                Shards::Auto if config.clickhouse.cluster.trim().is_empty() => false,
                Shards::Auto => {
                    let client = telemetryd_store::clickhouse::Client::new(&config.clickhouse)?;
                    telemetryd_store::clickhouse::schema::sharded(&client, &config.clickhouse)
                        .map_err(|error| {
                            anyhow::anyhow!(
                                "{error}\n\nThe layout depends on how many shards the \
                                 cluster has; pass --shards one or --shards many to print \
                                 it without asking the server"
                            )
                        })?
                }
            };
            print_schema(&config, sharded);
            Ok(())
        }
        ClickHouseAction::Check(_) => check(&config),
    }
}

fn print_schema(config: &Config, sharded: bool) {
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
    for statement in telemetryd_store::clickhouse::schema::statements(
        &config.clickhouse,
        &config.retention,
        sharded,
    )
    .into_iter()
    .chain(telemetryd_store::clickhouse::schema::ttl_statements(
        &config.clickhouse,
        &config.retention,
        sharded,
    )) {
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
