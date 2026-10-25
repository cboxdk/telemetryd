//! `telemetryd clickhouse` — for a deployment with ClickHouse as the store.
//!
//! `schema` prints every statement an instance would run to create and age its tables,
//! for a database whose schema someone else manages (`clickhouse.create_schema =
//! false`). `check` connects with the configured account and checks, one thing at a
//! time and changing nothing, that it can do what an instance needs: what to run before
//! a deployment rather than learn from its log. `migrate` brings a database an older
//! version created up to this one's layout, and `import` moves a single node's data
//! directory into ClickHouse.

use telemetryd_core::Config;
use telemetryd_core::config::Overrides;

#[derive(Debug, clap::Subcommand)]
pub enum ClickHouseAction {
    /// Print the statements that create and age the tables, for a DBA to run.
    Schema(SchemaArgs),
    /// Check that the configured account can do what an instance needs.
    Check(crate::ConfigArgs),
    /// Bring a database an older telemetryd created up to this version's layout.
    ///
    /// Rebuilds tables laid out differently, while instances keep writing; drops
    /// objects earlier versions created and this one does not; adds missing indexes.
    /// Every step can be repeated, and a run that is interrupted continues where it
    /// stopped when run again.
    Migrate(MigrateArgs),
    /// Copy a stopped single node's data directory into ClickHouse.
    ///
    /// Reads the records themselves — sealed segments and the write-ahead log — and
    /// writes them as an instance would. Resumes where an interrupted run stopped.
    Import(ImportArgs),
}

#[derive(Debug, clap::Args)]
#[allow(clippy::struct_excessive_bools)] // command-line switches, each its own
pub struct MigrateArgs {
    #[command(flatten)]
    pub config: crate::ConfigArgs,
    /// Say what would be done and change nothing.
    #[arg(long)]
    pub dry_run: bool,
    /// Fill in rollups created after the rows they summarise: reads those rows once.
    #[arg(long)]
    pub backfill: bool,
    /// Index the partitions written before an index existed, a partition at a time,
    /// then drop the indexes it replaces.
    #[arg(long)]
    pub materialize_indexes: bool,
    /// Keep a rebuilt table's old copy, under its temporary name.
    #[arg(long)]
    pub keep_old: bool,
    /// Go ahead although another run holds the lease — one that died, not one still
    /// running: two at once would copy the same partitions.
    #[arg(long)]
    pub take_over: bool,
    /// How long one statement may take: a partition copied, a day filled in.
    #[arg(long, default_value = "6h", value_name = "DURATION")]
    pub statement_timeout: humantime::Duration,
    /// NDJSON events on stdout, for a program watching.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, clap::Args)]
pub struct ImportArgs {
    /// The configuration of the ClickHouse deployment to import into.
    #[command(flatten)]
    pub config: crate::ConfigArgs,
    /// The data directory of the single node to import from. It must be stopped.
    #[arg(long, value_name = "DIR")]
    pub from_data_dir: std::path::PathBuf,
    /// Which signals; all three when omitted.
    #[arg(long = "signal", value_enum, value_name = "SIGNAL")]
    pub signals: Vec<ImportSignal>,
    /// Only records this recent. Default: as far back as the destination keeps each
    /// signal — anything older ClickHouse would drop at once.
    #[arg(long, value_name = "DURATION")]
    pub since: Option<humantime::Duration>,
    /// Records per insert.
    #[arg(long, default_value_t = 50_000)]
    pub batch: usize,
    /// Start from the beginning even where an earlier run got further.
    #[arg(long)]
    pub restart: bool,
    /// NDJSON events on stdout, for a program watching.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ImportSignal {
    Logs,
    Traces,
    Metrics,
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
        ClickHouseAction::Migrate(migrate) => &migrate.config,
        ClickHouseAction::Import(import) => &import.config,
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
        ClickHouseAction::Migrate(args) => migrate(&config, args),
        ClickHouseAction::Import(args) => import(&config, args),
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

fn migrate(config: &Config, args: &MigrateArgs) -> anyhow::Result<()> {
    use telemetryd_store::clickhouse::migrate::{Event, Options};
    // A statement here copies a partition or reads a day of rows, not a query's worth.
    let clickhouse = telemetryd_core::config::ClickHouseConfig {
        timeout: args.statement_timeout.into(),
        ..config.clickhouse.clone()
    };
    let client = telemetryd_store::clickhouse::Client::new(&clickhouse)?;
    let options = Options {
        dry_run: args.dry_run,
        backfill: args.backfill,
        materialize_indexes: args.materialize_indexes,
        keep_old: args.keep_old,
        take_over: args.take_over,
    };
    let json = args.json;
    let mut pending = 0;
    telemetryd_store::clickhouse::migrate::run(
        &client,
        &clickhouse,
        &config.retention,
        options,
        &mut |event| {
            if json {
                println!("{}", serde_json::to_string(event).unwrap_or_default());
                return;
            }
            match event {
                Event::Found {
                    what,
                    object,
                    detail,
                } => println!("  found  {object}: {} — {detail}", what.replace('_', " ")),
                Event::Step {
                    object,
                    action,
                    detail,
                    seconds,
                } => println!(
                    "  done   {object}: {action}{}{} ({seconds:.1}s)",
                    if detail.is_empty() { "" } else { " — " },
                    detail
                ),
                Event::Skipped { object, reason } => println!("  left   {object}: {reason}"),
                Event::Done {
                    changed,
                    pending: left,
                } => {
                    pending = *left;
                    println!("\n{changed} steps taken, {left} things left to do");
                }
            }
        },
    )
    .map_err(|e| anyhow::anyhow!(e))?;
    if args.dry_run && pending > 0 && !json {
        println!("a dry run: nothing was changed. Run without --dry-run to do it.");
    }
    Ok(())
}

fn import(config: &Config, args: &ImportArgs) -> anyhow::Result<()> {
    use telemetryd_core::Signal;
    use telemetryd_store::clickhouse::import::ImportOptions;

    let source_dir = &args.from_data_dir;
    if !source_dir.join("VERSION").is_file() {
        anyhow::bail!(
            "{} is not a telemetryd data directory: it has no VERSION file",
            source_dir.display()
        );
    }
    // The source as the store it was, alone: no ClickHouse, its own directory. Opening
    // it takes its lock, so a node still running is refused rather than read mid-write.
    let source_config = Config {
        storage: telemetryd_core::config::StorageConfig {
            data_dir: Some(source_dir.clone()),
            ..telemetryd_core::config::StorageConfig::default()
        },
        ..Config::default()
    };
    let source = telemetryd_store::Store::open(&source_config).map_err(|e| {
        anyhow::anyhow!(
            "opening {}: {e}\n\nStop the node that uses it first: a data directory has \
             one user at a time.",
            source_dir.display()
        )
    })?;
    let destination =
        telemetryd_store::clickhouse::ClickHouse::open(&config.clickhouse, &config.retention)
            .map_err(|e| anyhow::anyhow!(e))?;

    let signals: Vec<Signal> = if args.signals.is_empty() {
        vec![Signal::Logs, Signal::Traces, Signal::Metrics]
    } else {
        args.signals
            .iter()
            .map(|s| match s {
                ImportSignal::Logs => Signal::Logs,
                ImportSignal::Traces => Signal::Traces,
                ImportSignal::Metrics => Signal::Metrics,
            })
            .collect()
    };
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    )
    .unwrap_or(u64::MAX);
    let source_name = std::fs::canonicalize(source_dir)
        .unwrap_or_else(|_| source_dir.clone())
        .display()
        .to_string();
    let json = args.json;
    let mut total = 0u64;
    for signal in signals {
        let keep = match signal {
            Signal::Logs => config.retention.logs.0,
            Signal::Traces => config.retention.traces.0,
            Signal::Metrics => config.retention.metrics.0,
        };
        let window = args.since.map_or(keep, |since| keep.min(since.into()));
        let start = now.saturating_sub(u64::try_from(window.as_nanos()).unwrap_or(u64::MAX));
        let Some((first, last)) =
            telemetryd_store::clickhouse::import::source_bounds(&source, signal)
                .map_err(|e| anyhow::anyhow!(e))?
        else {
            continue;
        };
        if first < start && !json {
            println!(
                "  {signal:?}: records older than {} are not imported — the destination \
                 keeps {} of them",
                humantime::format_rfc3339_seconds(
                    std::time::UNIX_EPOCH + std::time::Duration::from_nanos(start)
                ),
                humantime::format_duration(window)
            );
        }
        let options = ImportOptions {
            signals: vec![signal],
            start: first.max(start),
            end: last,
            batch: args.batch,
            source: source_name.clone(),
            restart: args.restart,
        };
        let summary = telemetryd_store::clickhouse::import::import(
            &source,
            &destination,
            &options,
            &mut |event| print_import_event(event, json),
        )
        .map_err(|e| anyhow::anyhow!(e))?;
        total += summary.logs + summary.traces + summary.metrics;
    }
    if !json {
        println!("\n{total} records imported");
    }
    Ok(())
}

fn print_import_event(event: &telemetryd_store::clickhouse::import::ImportEvent, json: bool) {
    use telemetryd_store::clickhouse::import::ImportEvent;
    if json {
        println!("{}", serde_json::to_string(event).unwrap_or_default());
        return;
    }
    let when = |nanos: u64| {
        humantime::format_rfc3339_seconds(
            std::time::UNIX_EPOCH + std::time::Duration::from_nanos(nanos),
        )
    };
    match event {
        ImportEvent::Start {
            signal, resumed, ..
        } => println!(
            "  {signal}: {}",
            if *resumed {
                "resuming where an earlier run stopped"
            } else {
                "starting"
            }
        ),
        // On stderr, redrawn: a meter, not a log.
        ImportEvent::Progress {
            signal,
            total,
            through,
            records_per_second,
            ..
        } => eprint!(
            "\r  {signal}: {total} records, through {}, {records_per_second:.0}/s   ",
            when(*through)
        ),
        ImportEvent::Done {
            signal,
            records,
            seconds,
        } => eprintln!("\n  {signal}: {records} records in {seconds:.1}s"),
    }
}
