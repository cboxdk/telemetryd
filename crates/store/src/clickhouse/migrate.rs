//! Bringing a database an older telemetryd created up to what this one creates.
//!
//! At startup an instance creates what is missing and leaves what exists alone, so a
//! table keeps the engine, keys and partitioning it was created with. This finds where
//! that differs from what [`schema::statements`] would create today, and puts it right
//! while instances keep writing:
//!
//! - **A table laid out differently is rebuilt**: created again under a temporary
//!   name, swapped in by `EXCHANGE TABLES`, and the old rows copied into it a partition
//!   at a time — through a staging table and `MOVE PARTITION`, so each partition
//!   arrives exactly once and no materialized view sees it a second time. See
//!   [`rebuild`].
//! - **Objects earlier versions created and this one does not** are dropped — the ones
//!   in [`STALE`], by name, and nothing else.
//! - **Indexes**: one this version declares and the table lacks is added; with
//!   `materialize_indexes`, partitions written before it existed are indexed, a
//!   partition at a time, and an index it replaces is dropped once they all are.
//! - **Rollups** created after the rows they summarise (`--backfill`) are filled in
//!   for those rows. See [`backfill`].
//!
//! What is to be done is found by asking ClickHouse rather than by comparing text: a
//! probe of each table, created from the statement an instance would run and dropped
//! again, says what engine, keys and columns that statement gives on this server.
//!
//! Every step is idempotent and records where it got to in `telemetryd_meta`, so a run
//! that is interrupted is run again and continues.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use serde::Serialize;
use telemetryd_core::config::{ClickHouseConfig, RetentionConfig};
use telemetryd_core::{Error, Result};

use super::Client;
use super::metric_folds::{read_meta, write_meta};
use super::schema::{self, Layout};
use super::sql::quote;

/// What a run may do beyond the plan.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_excessive_bools)] // independent switches, as the CLI has them
pub struct Options {
    /// Say what would be done, change nothing.
    pub dry_run: bool,
    /// Fill in rollups for rows older than the rollup.
    pub backfill: bool,
    /// Index the partitions written before an index existed, and drop what it replaces.
    pub materialize_indexes: bool,
    /// Keep the old table after a rebuild, under its temporary name.
    pub keep_old: bool,
    /// Go ahead although another run holds the lease: for one that died, said so by
    /// the operator.
    pub take_over: bool,
}

/// One thing found or done, for a person or a program watching.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// Something to be done: `what` is `create`, `rebuild`, `stale`, `add_index`,
    /// `drop_index`, `materialize_index`, `backfill` or `advice`.
    Found {
        what: &'static str,
        object: String,
        detail: String,
    },
    /// A step was taken.
    Step {
        object: String,
        action: String,
        detail: String,
        seconds: f64,
    },
    /// Something found and left, with why.
    Skipped { object: String, reason: String },
    /// The run is over: `changed` steps were taken, `pending` things left to do.
    Done { changed: usize, pending: usize },
}

/// The base tables other writers share — the Collector's layout, as the store package
/// creates it. Rebuilding one would rewrite someone else's table, so a layout that
/// differs is reported, not changed.
const SHARED: &[&str] = &[schema::LOGS, schema::TRACES];

/// Where a run records its progress: rebuilding it would swap that away mid-run.
const NEVER_REBUILT: &[&str] = &[schema::META];

/// Tables and views earlier versions of telemetryd created that this one does not:
/// dropped by name, views first. Never a pattern: a database may hold other things.
const STALE: &[(&str, &str)] = &[
    ("VIEW", "otel_trace_attribute_keys_mv"),
    ("TABLE", "otel_trace_attribute_keys"),
];

/// Indexes earlier versions created on a table this one still has, and whether one
/// is still in use until what replaces it covers every partition.
const STALE_INDEXES: &[StaleIndex] = &[
    StaleIndex {
        table: schema::LOGS,
        index: "idx_stream_value",
        until_replaced: false,
    },
    StaleIndex {
        table: schema::LOGS,
        index: "idx_body",
        until_replaced: true,
    },
];

#[derive(Debug, Clone, Copy)]
struct StaleIndex {
    table: &'static str,
    index: &'static str,
    /// Kept until every index the table declares covers every partition: it answers
    /// queries until then.
    until_replaced: bool,
}

/// How long after a swap a rebuild waits before copying: writes already on their way
/// to the table by its name land in the new one, and this lets them.
const SETTLE: Duration = Duration::from_secs(5);

/// Bring the database up to this version's layout.
///
/// # Errors
/// When ClickHouse refuses a step. The run can be repeated: it continues from where
/// this one stopped.
pub fn run(
    client: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
    options: Options,
    report: &mut dyn FnMut(&Event),
) -> Result<()> {
    // What an instance's startup creates — missing tables, views, columns — first, as
    // it would, and as safely run beside one: the rest is what startup leaves alone.
    if options.dry_run {
        for missing in schema::missing(client, config, retention)? {
            report(&Event::Found {
                what: "create",
                object: missing,
                detail: "missing; an instance's startup creates it".to_owned(),
            });
        }
    } else if config.create_schema {
        schema::ensure(client, config, retention)?;
    } else if let Some(missing) = schema::missing(client, config, retention)?.first() {
        return Err(Error::Config(format!(
            "clickhouse.create_schema = false, and the database lacks {missing}: create \
             what `telemetryd clickhouse schema` prints first"
        )));
    }
    let owner = format!("migrate-{}-{}", std::process::id(), now_nanos());
    if !options.dry_run {
        take_lease(client, &owner, options.take_over)?;
    }
    let outcome = (|| {
        let existing = tables(client)?;
        let expected = probe(client, config, retention, &existing)?;
        let mut tally = Tally::default();
        // Stale objects first: a stale view would otherwise keep feeding a stale table.
        tally.add(drop_stale(
            client,
            &Layout { config, retention },
            &existing,
            options,
            report,
        )?);
        tally.add(rebuilds(
            client, config, retention, &existing, &expected, options, report,
        )?);
        // Indexes after rebuilds, which create theirs whole.
        let existing = tables(client)?;
        tally.add(indexes(
            client,
            &Layout { config, retention },
            &existing,
            &expected,
            options,
            report,
        )?);
        // Rollups younger than their rows.
        tally.add(backfill(client, config, retention, options, report)?);
        report(&Event::Done {
            changed: tally.changed,
            pending: tally.pending,
        });
        Ok(())
    })();
    if !options.dry_run {
        // Released whatever happened, so a rerun need not wait out the lease.
        let _ = write_meta(client, LEASE, "");
    }
    outcome
}

/// Steps taken, and things found and left for another run.
#[derive(Debug, Default, Clone, Copy)]
struct Tally {
    changed: usize,
    pending: usize,
}

impl Tally {
    fn add(&mut self, other: Self) {
        self.changed += other.changed;
        self.pending += other.pending;
    }
}

fn drop_stale(
    client: &Client,
    layout: &Layout<'_>,
    existing: &BTreeMap<String, Table>,
    options: Options,
    report: &mut dyn FnMut(&Event),
) -> Result<Tally> {
    let mut tally = Tally::default();
    for (kind, name) in STALE {
        if !existing.contains_key(*name) {
            continue;
        }
        report(&Event::Found {
            what: "stale",
            object: (*name).to_owned(),
            detail: format!("a {} no version since creates", kind.to_lowercase()),
        });
        if options.dry_run {
            tally.pending += 1;
            continue;
        }
        let started = Instant::now();
        client.execute(&format!(
            "DROP {kind} IF EXISTS {name}{} SYNC",
            layout.on_cluster()
        ))?;
        tally.changed += 1;
        report(&step(name, "dropped", String::new(), started));
    }
    Ok(tally)
}

/// Rebuild every table laid out differently from what this version creates, and
/// finish any rebuild an earlier run left part way.
fn rebuilds(
    client: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
    existing: &BTreeMap<String, Table>,
    expected: &BTreeMap<String, Table>,
    options: Options,
    report: &mut dyn FnMut(&Event),
) -> Result<Tally> {
    let mut tally = Tally::default();
    for (name, want) in expected {
        let Some(have) = existing.get(name) else {
            continue;
        };
        let differences = differences(have, want);
        let unfinished = read_meta(client, &rebuild_key(name))?
            .is_some_and(|state| !state.is_empty() && !state.starts_with("done:"));
        if differences.is_empty() && !unfinished {
            continue;
        }
        if SHARED.contains(&name.as_str()) || NEVER_REBUILT.contains(&name.as_str()) {
            if !differences.is_empty() {
                report(&Event::Found {
                    what: "advice",
                    object: name.clone(),
                    detail: format!(
                        "laid out differently from what telemetryd creates ({}); another \
                         writer may own it, so it is left as it is. Queries are correct, \
                         and may read more than they would",
                        differences.join("; ")
                    ),
                });
            }
            continue;
        }
        let extra: Vec<&str> = have
            .columns
            .keys()
            .filter(|c| !want.columns.contains_key(*c))
            .map(String::as_str)
            .collect();
        if !extra.is_empty() {
            report(&Event::Skipped {
                object: name.clone(),
                reason: format!(
                    "it has columns telemetryd does not create ({}), which a rebuild would \
                     lose: left as it is",
                    extra.join(", ")
                ),
            });
            tally.pending += 1;
            continue;
        }
        report(&Event::Found {
            what: "rebuild",
            object: name.clone(),
            detail: if differences.is_empty() {
                "an earlier run stopped part way; continuing it".to_owned()
            } else {
                differences.join("; ")
            },
        });
        if options.dry_run {
            tally.pending += 1;
            continue;
        }
        tally.changed += rebuild(client, config, retention, name, want, options, report)?;
    }
    Ok(tally)
}

fn step(object: &str, action: &str, detail: String, started: Instant) -> Event {
    Event::Step {
        object: object.to_owned(),
        action: action.to_owned(),
        detail,
        seconds: started.elapsed().as_secs_f64(),
    }
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

// ---------------------------------------------------------------------------------
// One run at a time.

const LEASE: &str = "migrate_lease";
/// A lease is renewed by every step; one not renewed for this long was left by a run
/// that died.
const LEASE_FOR: u64 = 30 * 60 * 1_000_000_000;

fn take_lease(client: &Client, owner: &str, take_over: bool) -> Result<()> {
    let now = now_nanos();
    if !take_over
        && let Some(lease) = read_meta(client, LEASE)?
        && let Some((holder, until)) = lease.split_once('@')
        && holder != owner
        && until.parse::<u64>().unwrap_or(0) > now
    {
        return Err(Error::Config(format!(
            "another `telemetryd clickhouse migrate` ({holder}) is running against this \
             database; wait for it. If it died, its lease lapses half an hour after its \
             last step, or run with --take-over"
        )));
    }
    renew(client, owner)?;
    match read_meta(client, LEASE)?.and_then(|l| l.split_once('@').map(|(h, _)| h.to_owned())) {
        Some(holder) if holder == owner => Ok(()),
        _ => Err(Error::Config(
            "another `telemetryd clickhouse migrate` started at the same moment; run again"
                .to_owned(),
        )),
    }
}

fn renew(client: &Client, owner: &str) -> Result<()> {
    write_meta(
        client,
        LEASE,
        &format!("{owner}@{}", now_nanos().saturating_add(LEASE_FOR)),
    )
}

// ---------------------------------------------------------------------------------
// What the database holds, and what this version would create.

/// A table as ClickHouse describes it.
#[derive(Debug, Clone, Default)]
struct Table {
    engine: String,
    engine_full: String,
    partition_key: String,
    sorting_key: String,
    primary_key: String,
    /// Name → (type, default kind, default expression).
    columns: BTreeMap<String, (String, String, String)>,
    /// Name → (expression, type, granularity).
    indexes: BTreeMap<String, (String, String, u64)>,
}

fn text(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn number(value: &serde_json::Value, key: &str) -> u64 {
    value
        .get(key)
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(0)
}

/// Every table and view in the database, as ClickHouse describes it.
fn tables(client: &Client) -> Result<BTreeMap<String, Table>> {
    tables_named(client, None)
}

fn tables_named(client: &Client, only: Option<&[&str]>) -> Result<BTreeMap<String, Table>> {
    let filter = only.map_or(String::new(), |names| {
        let quoted: Vec<String> = names.iter().map(|n| quote(n)).collect();
        format!(" AND name IN ({})", quoted.join(", "))
    });
    let column_filter = only.map_or(String::new(), |names| {
        let quoted: Vec<String> = names.iter().map(|n| quote(n)).collect();
        format!(" AND table IN ({})", quoted.join(", "))
    });
    let mut tables: BTreeMap<String, Table> = BTreeMap::new();
    client.query_lines(
        &format!(
            "SELECT name, engine, engine_full, partition_key, sorting_key, primary_key \
             FROM system.tables WHERE database = currentDatabase(){filter}"
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            tables.insert(
                text(&value, "name"),
                Table {
                    engine: text(&value, "engine"),
                    engine_full: text(&value, "engine_full"),
                    partition_key: text(&value, "partition_key"),
                    sorting_key: text(&value, "sorting_key"),
                    primary_key: text(&value, "primary_key"),
                    ..Table::default()
                },
            );
            Ok(ControlFlow::Continue(()))
        },
    )?;
    client.query_lines(
        &format!(
            "SELECT table, name, type, default_kind, default_expression \
             FROM system.columns WHERE database = currentDatabase(){column_filter}"
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            if let Some(table) = tables.get_mut(&text(&value, "table")) {
                table.columns.insert(
                    text(&value, "name"),
                    (
                        text(&value, "type"),
                        text(&value, "default_kind"),
                        text(&value, "default_expression"),
                    ),
                );
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;
    client.query_lines(
        &format!(
            "SELECT table, name, expr, type_full, granularity \
             FROM system.data_skipping_indices WHERE database = currentDatabase(){column_filter}"
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            if let Some(table) = tables.get_mut(&text(&value, "table")) {
                table.indexes.insert(
                    text(&value, "name"),
                    (
                        text(&value, "expr"),
                        text(&value, "type_full"),
                        number(&value, "granularity"),
                    ),
                );
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;
    Ok(tables)
}

/// The suffix a probe table carries: never a name telemetryd uses otherwise.
const PROBE: &str = "__td_probe";

/// What this version would create for each table the database already has, from
/// ClickHouse itself: each created on this server under a probe name, unreplicated —
/// nothing for the cluster to do — described, and dropped.
fn probe(
    client: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
    existing: &BTreeMap<String, Table>,
) -> Result<BTreeMap<String, Table>> {
    let local = ClickHouseConfig {
        cluster: String::new(),
        ..config.clone()
    };
    let mut expected = BTreeMap::new();
    for name in schema::objects(&local, retention) {
        let Some(statement) = schema::create_statement(&local, retention, &name) else {
            continue;
        };
        if !statement.starts_with("CREATE TABLE ") || !existing.contains_key(&name) {
            continue;
        }
        let probe = format!("{name}{PROBE}");
        let create = renamed(&statement, &name, &probe, &local)?;
        client.execute(&format!("DROP TABLE IF EXISTS {probe} SYNC"))?;
        client.execute(&create)?;
        let described = tables_named(client, Some(&[probe.as_str()]));
        client.execute(&format!("DROP TABLE IF EXISTS {probe} SYNC"))?;
        if let Some(table) = described?.remove(&probe) {
            expected.insert(name, table);
        }
    }
    Ok(expected)
}

/// `statement`, the `CREATE TABLE IF NOT EXISTS` of `from`, creating `to` instead —
/// with a Keeper path of its own when replicated, since two tables cannot share one.
fn renamed(statement: &str, from: &str, to: &str, config: &ClickHouseConfig) -> Result<String> {
    let head = format!("CREATE TABLE IF NOT EXISTS {from}");
    let Some(rest) = statement.strip_prefix(&head) else {
        return Err(Error::Config(format!(
            "the statement for {from} does not begin as expected"
        )));
    };
    let mut renamed = format!("CREATE TABLE {to}{rest}");
    if !config.cluster.trim().is_empty() {
        let path = format!("/{{database}}/{from}'");
        if renamed.matches(&path).count() != 1 {
            return Err(Error::Config(format!(
                "the statement for {from} does not name its Keeper path as expected"
            )));
        }
        renamed = renamed.replacen(&path, &format!("/{{database}}/{to}'"), 1);
    }
    Ok(renamed)
}

/// The engine without `Replicated`, and its arguments without a replicated engine's
/// Keeper path and replica name: what decides how rows merge.
fn engine_kind(table: &Table) -> (String, Vec<String>) {
    let kind = table
        .engine
        .strip_prefix("Replicated")
        .unwrap_or(&table.engine)
        .to_owned();
    let arguments = table
        .engine_full
        .strip_prefix(&table.engine)
        .and_then(|rest| rest.strip_prefix('('))
        .and_then(|rest| rest.split_once(')'))
        .map(|(inside, _)| {
            inside
                .split(',')
                .map(str::trim)
                .filter(|a| !a.is_empty() && !a.starts_with('\''))
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    (kind, arguments)
}

/// How `have` differs from `want` in what a rebuild changes: engine, keys, column
/// types. Columns `want` adds are left to an instance's startup, which adds them.
fn differences(have: &Table, want: &Table) -> Vec<String> {
    let mut out = Vec::new();
    let (have_engine, want_engine) = (engine_kind(have), engine_kind(want));
    if have_engine != want_engine {
        out.push(format!(
            "engine {}{} rather than {}{}",
            have_engine.0,
            arguments(&have_engine.1),
            want_engine.0,
            arguments(&want_engine.1)
        ));
    }
    for (what, h, w) in [
        ("partitioned by", &have.partition_key, &want.partition_key),
        ("sorted by", &have.sorting_key, &want.sorting_key),
        ("primary key", &have.primary_key, &want.primary_key),
    ] {
        if h != w {
            out.push(format!("{what} ({h}) rather than ({w})"));
        }
    }
    for (column, (want_type, want_kind, want_default)) in &want.columns {
        if let Some((have_type, have_kind, have_default)) = have.columns.get(column)
            && (have_type != want_type || have_kind != want_kind || have_default != want_default)
        {
            out.push(format!(
                "column {column} is {have_type}{} rather than {want_type}{}",
                default_text(have_kind, have_default),
                default_text(want_kind, want_default)
            ));
        }
    }
    out
}

fn arguments(arguments: &[String]) -> String {
    if arguments.is_empty() {
        String::new()
    } else {
        format!("({})", arguments.join(", "))
    }
}

fn default_text(kind: &str, expression: &str) -> String {
    if kind.is_empty() {
        String::new()
    } else {
        format!(" {kind} {expression}")
    }
}

// ---------------------------------------------------------------------------------
// Rebuilding a table.

fn rebuild_key(table: &str) -> String {
    format!("migrate_rebuild:{table}")
}

/// Rebuild `table` in the layout `want` describes, without stopping writes to it.
///
/// 1. **Create** the new table under a temporary name, from the statement an instance
///    would run. For a table whose rows merge to the same answer however often they
///    arrive — a `ReplacingMergeTree`, or an `AggregatingMergeTree` of only `min`,
///    `max` and `any` — copy every row into it now, so nothing is missing once it is
///    swapped in.
/// 2. **Swap** it in with `EXCHANGE TABLES`. Inserts name the table, and materialized
///    views resolve their target by name too, so from here every write lands in the
///    new table; the old one, under the temporary name, holds what was written before.
/// 3. **Copy** the old rows a partition at a time: into a staging table laid out like
///    the new one, checked against the old partition — distinct keys and every summed
///    column — then `MOVE PARTITION` into the new table. A move adds parts without an
///    insert, so no view counts them again, and `telemetryd_meta` records each
///    partition staged and then moved, so an interrupted copy resumes without
///    repeating one.
/// 4. **Drop** the old table and the staging table, unless `keep_old`.
fn rebuild(
    client: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
    table: &str,
    want: &Table,
    options: Options,
    report: &mut dyn FnMut(&Event),
) -> Result<usize> {
    atomic_database(client)?;
    let layout = Layout { config, retention };
    let on = layout.on_cluster();
    let statement = schema::create_statement(config, retention, table)
        .ok_or_else(|| Error::Config(format!("telemetryd has no statement creating {table}")))?;
    let key = rebuild_key(table);
    let mut changed = 0;
    let state = read_meta(client, &key)?.unwrap_or_default();
    // A rebuild finished before is no reason not to start another: this one is for a
    // layout that changed since.
    let (phase, old) = match state.split_once(':') {
        Some((phase, old)) if !old.is_empty() && phase != "done" => {
            (phase.to_owned(), old.to_owned())
        }
        _ => (
            String::new(),
            format!("{table}__td_old_{}", now_nanos() / 1_000_000_000),
        ),
    };
    let staging = format!("{table}__td_stage");
    let owner = read_meta(client, LEASE)?
        .and_then(|l| l.split_once('@').map(|(h, _)| h.to_owned()))
        .unwrap_or_default();

    if phase.is_empty() || phase == "created" {
        if phase.is_empty() {
            let started = Instant::now();
            client.execute(&renamed(&statement, table, &old, config)?)?;
            write_meta(client, &key, &format!("created:{old}"))?;
            changed += 1;
            report(&step(
                table,
                "created",
                format!("the new layout, as {old}"),
                started,
            ));
        }
        let fresh = tables_named(client, Some(&[table, old.as_str()]))?;
        let swapped = fresh
            .get(table)
            .is_some_and(|t| differences(t, want).is_empty())
            && fresh
                .get(&old)
                .is_some_and(|t| !differences(t, want).is_empty());
        if !swapped {
            if idempotent(want) {
                let started = Instant::now();
                let copied = copy_all(client, table, &old, want)?;
                report(&step(
                    table,
                    "copied ahead",
                    format!("{copied} rows into {old} before the swap"),
                    started,
                ));
            }
            let started = Instant::now();
            client.execute(&format!("EXCHANGE TABLES {table} AND {old}{on}"))?;
            changed += 1;
            report(&step(
                table,
                "swapped",
                format!("writes now land in the new layout; the old rows are in {old}"),
                started,
            ));
            std::thread::sleep(SETTLE);
        }
        write_meta(client, &key, &format!("swapped:{old}"))?;
        renew(client, &owner)?;
    }

    {
        changed += copy_back(
            client, config, retention, table, &old, &staging, want, &owner, report,
        )?;
        if options.keep_old {
            client.execute(&format!("DROP TABLE IF EXISTS {staging}{on} SYNC"))?;
            report(&Event::Skipped {
                object: old.clone(),
                reason: "kept, as asked; drop it once you no longer want it".to_owned(),
            });
        } else {
            let started = Instant::now();
            client.execute(&format!("DROP TABLE IF EXISTS {old}{on} SYNC"))?;
            client.execute(&format!("DROP TABLE IF EXISTS {staging}{on} SYNC"))?;
            changed += 1;
            report(&step(table, "dropped the old table", old.clone(), started));
        }
        write_meta(client, &key, &format!("done:{old}"))?;
    }
    Ok(changed)
}

/// Refuse a database whose engine cannot swap two tables in one step.
fn atomic_database(client: &Client) -> Result<()> {
    let mut engine = String::new();
    client.query_lines(
        "SELECT engine FROM system.databases WHERE name = currentDatabase()",
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            engine = text(&value, "engine");
            Ok(ControlFlow::Break(()))
        },
    )?;
    if engine == "Atomic" {
        Ok(())
    } else {
        Err(Error::Config(format!(
            "database {:?} uses the {engine} engine, which cannot exchange two tables in \
             one step; a rebuild needs an Atomic database, ClickHouse's default",
            client.database()
        )))
    }
}

/// Whether rows of `table` give the same answer however many times each arrives.
fn idempotent(table: &Table) -> bool {
    let (kind, _) = engine_kind(table);
    match kind.as_str() {
        "ReplacingMergeTree" => true,
        "AggregatingMergeTree" => table.columns.values().all(|(kind, _, _)| {
            !kind.starts_with("SimpleAggregateFunction(")
                || ["min,", "max,", "any,", "anyLast,"]
                    .iter()
                    .any(|f| kind.starts_with(&format!("SimpleAggregateFunction({f}")))
        }),
        _ => false,
    }
}

/// The columns an insert into `want` writes, and that `from` has: everything but
/// what the table computes itself.
fn insertable(want: &Table, from: &Table) -> Vec<String> {
    want.columns
        .iter()
        .filter(|(name, (_, kind, _))| {
            (kind.is_empty() || kind == "DEFAULT") && from.columns.contains_key(*name)
        })
        .map(|(name, _)| format!("`{name}`"))
        .collect()
}

fn copy_all(client: &Client, from: &str, into: &str, want: &Table) -> Result<u64> {
    let source = tables_named(client, Some(&[from]))?
        .remove(from)
        .unwrap_or_default();
    let columns = insertable(want, &source).join(", ");
    let mut copied = 0;
    for (partition, rows) in partitions(client, from)? {
        client.execute(&format!(
            "INSERT INTO {into} ({columns}) SELECT {columns} FROM {from} \
             WHERE _partition_id = {}",
            quote(&partition)
        ))?;
        copied += rows;
    }
    Ok(copied)
}

/// A table's partitions and their rows, newest first: what is most likely read is
/// copied first.
fn partitions(client: &Client, table: &str) -> Result<Vec<(String, u64)>> {
    let mut out = Vec::new();
    client.query_lines(
        &format!(
            "SELECT partition_id AS id, sum(rows) AS rows \
             FROM system.parts WHERE active AND database = currentDatabase() AND table = {} \
             GROUP BY id ORDER BY max(max_time) DESC, max(max_date) DESC, id DESC",
            quote(table)
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            out.push((text(&value, "id"), number(&value, "rows")));
            Ok(ControlFlow::Continue(()))
        },
    )?;
    Ok(out)
}

fn partition_key(table: &str, old: &str, partition: &str) -> String {
    format!("migrate_partition:{table}:{old}:{partition}")
}

/// Step 3 of [`rebuild`]: every partition of `old` into `table`, exactly once.
#[allow(clippy::too_many_arguments)]
fn copy_back(
    client: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
    table: &str,
    old: &str,
    staging: &str,
    want: &Table,
    owner: &str,
    report: &mut dyn FnMut(&Event),
) -> Result<usize> {
    let layout = Layout { config, retention };
    let on = layout.on_cluster();
    let Some(source) = tables_named(client, Some(&[old]))?.remove(old) else {
        // Dropped by an earlier run that stopped before saying it was done.
        return Ok(0);
    };
    let replicated = !config.cluster.trim().is_empty();
    if replicated {
        client.execute(&format!("SYSTEM SYNC REPLICA {old}"))?;
    }
    let statement = schema::create_statement(config, retention, table)
        .ok_or_else(|| Error::Config(format!("telemetryd has no statement creating {table}")))?;
    let create_stage = renamed(&statement, table, staging, config)?;
    client.execute(&create_stage.replacen("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1))?;

    let columns = insertable(want, &source).join(", ");
    let check = Check::of(want);
    let expired = expired_before(retention, table);
    let mut changed = 0;
    for (partition, rows) in partitions(client, old)? {
        let marker = partition_key(table, old, &partition);
        let progress = read_meta(client, &marker)?.unwrap_or_default();
        if progress == "moved" {
            continue;
        }
        let started = Instant::now();
        if let Some((time, cutoff)) = &expired
            && newest(client, old, &partition, time)? < *cutoff
        {
            write_meta(client, &marker, "moved")?;
            report(&Event::Skipped {
                object: format!("{table} partition {partition}"),
                reason: "older than the retention; ClickHouse would drop it at once".to_owned(),
            });
            continue;
        }
        if progress != "staged" {
            // Empty after every move; anything there is a copy an interrupted run left
            // unchecked.
            if !partitions(client, staging)?.is_empty() {
                client.execute(&format!("TRUNCATE TABLE IF EXISTS {staging}{on} SYNC"))?;
            }
            client.execute(&format!(
                "INSERT INTO {staging} ({columns}) SELECT {columns} FROM {old} \
                 WHERE _partition_id = {}",
                quote(&partition)
            ))?;
            let (have, got) = (
                check.measure(client, old, Some(&partition))?,
                check.measure(client, staging, None)?,
            );
            if have != got {
                return Err(Error::Unavailable(format!(
                    "copying partition {partition} of {table} gave {got:?} where the old \
                     table holds {have:?}; nothing was moved, and the old table is kept as \
                     {old}. Run again to retry"
                )));
            }
            write_meta(client, &marker, "staged")?;
        }
        // Whatever the staging table holds is this partition's rows, checked: move all
        // of it, whichever of its partitions an earlier run had already moved.
        for (staged, _) in partitions(client, staging)? {
            client.execute(&format!(
                "ALTER TABLE {staging} MOVE PARTITION ID {} TO TABLE {table}",
                quote(&staged)
            ))?;
        }
        write_meta(client, &marker, "moved")?;
        renew(client, owner)?;
        changed += 1;
        report(&step(
            table,
            "copied",
            format!("partition {partition}, {rows} rows"),
            started,
        ));
    }
    Ok(changed)
}

/// For a table that ages, its time expression and the moment before which a row has
/// outlived the retention.
fn expired_before(retention: &RetentionConfig, table: &str) -> Option<(String, u64)> {
    let aging = schema::aging_of(table)?;
    let keep = match aging.signal {
        schema::Signal::Logs => retention.logs.0,
        schema::Signal::Traces => retention.traces.0,
        schema::Signal::Metrics => retention.metrics.0,
    };
    let now = now_nanos() / 1_000_000_000;
    Some((aging.time.to_owned(), now.saturating_sub(keep.as_secs())))
}

/// The latest time, in Unix seconds, of any row in a partition.
fn newest(client: &Client, table: &str, partition: &str, time: &str) -> Result<u64> {
    let mut newest = u64::MAX;
    client.query_lines(
        &format!(
            "SELECT toUnixTimestamp(max({time})) AS newest FROM {table} \
             WHERE _partition_id = {}",
            quote(partition)
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            newest = number(&value, "newest");
            Ok(ControlFlow::Break(()))
        },
    )?;
    Ok(newest)
}

/// What says two copies of a table's rows are the same rows, merged or not: how many
/// distinct sort keys, and the total of every summed column. A row count would not
/// do — a merge in either copy collapses rows that share a key.
struct Check {
    expressions: Vec<String>,
}

impl Check {
    fn of(table: &Table) -> Self {
        let mut expressions = vec![if table.sorting_key.is_empty() {
            "count()".to_owned()
        } else {
            format!("uniqExact(tuple({}))", table.sorting_key)
        }];
        let (kind, _) = engine_kind(table);
        if kind == "MergeTree" {
            expressions.push("count()".to_owned());
        }
        for (name, (kind, _, _)) in &table.columns {
            if kind.starts_with("SimpleAggregateFunction(sum,") {
                expressions.push(format!("toString(sum(`{name}`))"));
            } else if kind.starts_with("SimpleAggregateFunction(min,") {
                expressions.push(format!("toString(min(`{name}`))"));
            } else if kind.starts_with("SimpleAggregateFunction(max,") {
                expressions.push(format!("toString(max(`{name}`))"));
            }
        }
        Self { expressions }
    }

    fn measure(&self, client: &Client, table: &str, partition: Option<&str>) -> Result<String> {
        let filter = partition.map_or(String::new(), |p| {
            format!(" WHERE _partition_id = {}", quote(p))
        });
        let mut out = String::new();
        client.query_lines(
            &format!(
                "SELECT toString(tuple({})) AS measure FROM {table}{filter}",
                self.expressions.join(", ")
            ),
            |line| {
                let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
                out = text(&value, "measure");
                Ok(ControlFlow::Break(()))
            },
        )?;
        Ok(out)
    }
}

// ---------------------------------------------------------------------------------
// Indexes.

fn index_done_key(table: &str, index: &str, partition: &str) -> String {
    format!("migrate_index_done:{table}:{index}:{partition}")
}

/// Add the indexes a table lacks; with `materialize_indexes`, index what was written
/// before each existed and drop what it replaces.
///
/// A part written before an index existed lacks it until a merge or a `MATERIALIZE
/// INDEX` rewrites it, and ClickHouse does not say which parts lack which index. So
/// whatever adds an index to a table that exists records the moment in
/// `telemetryd_meta`, and a partition is indexed if it has a part older than that and
/// has not been indexed since — perhaps one a merge had already rewritten; never one
/// missed.
fn indexes(
    client: &Client,
    layout: &Layout<'_>,
    existing: &BTreeMap<String, Table>,
    expected: &BTreeMap<String, Table>,
    options: Options,
    report: &mut dyn FnMut(&Event),
) -> Result<Tally> {
    let mut tally = Tally::default();
    for (name, want) in expected {
        let Some(have) = existing.get(name) else {
            continue;
        };
        let declared = declared_indexes(client, layout, name, have, want, options, report)?;
        tally.add(declared);
        tally.add(stale_indexes(
            client,
            layout,
            name,
            have,
            want,
            declared.pending,
            options,
            report,
        )?);
    }
    Ok(tally)
}

/// The indexes `want` declares: added where missing, materialized where asked.
fn declared_indexes(
    client: &Client,
    layout: &Layout<'_>,
    name: &str,
    have: &Table,
    want: &Table,
    options: Options,
    report: &mut dyn FnMut(&Event),
) -> Result<Tally> {
    let on = layout.on_cluster();
    let now = now_nanos() / 1_000_000_000;
    let mut tally = Tally::default();
    for (index, (expr, kind, granularity)) in &want.indexes {
        match have.indexes.get(index) {
            Some(found) if found == &(expr.clone(), kind.clone(), *granularity) => {}
            found => {
                report(&Event::Found {
                    what: "add_index",
                    object: name.to_owned(),
                    detail: if found.is_some() {
                        format!("{index} is declared differently; telemetryd declares {kind}")
                    } else {
                        format!("{index} {kind} is missing")
                    },
                });
                // Redeclaring one drops what it holds: only when it will be rebuilt.
                if options.dry_run || found.is_some() && !options.materialize_indexes {
                    tally.pending += 1;
                    continue;
                }
                let started = Instant::now();
                if found.is_some() {
                    client.execute(&format!(
                        "ALTER TABLE {name}{on} DROP INDEX IF EXISTS {index}"
                    ))?;
                }
                client.execute(&format!(
                    "ALTER TABLE {name}{on} ADD INDEX IF NOT EXISTS {index} {expr} \
                     TYPE {kind} GRANULARITY {granularity}"
                ))?;
                // Every part from before now lacks it.
                write_meta(
                    client,
                    &schema::index_added_key(name, index),
                    &now.to_string(),
                )?;
                tally.changed += 1;
                report(&step(name, "added index", index.clone(), started));
            }
        }
        // Added to a table that already had rows, by an instance's startup or by an
        // earlier run, which records when; one with no such record was created with its
        // table, and every part has it.
        let Some(added) = read_meta(client, &schema::index_added_key(name, index))?
            .and_then(|v| v.parse::<u64>().ok())
        else {
            continue;
        };
        for partition in unindexed(client, name, index, added)? {
            if !options.materialize_indexes || options.dry_run {
                tally.pending += 1;
                report(&Event::Found {
                    what: "materialize_index",
                    object: name.to_owned(),
                    detail: format!("{index} in partition {partition}"),
                });
                continue;
            }
            let started = Instant::now();
            client.execute_with(
                &format!(
                    "ALTER TABLE {name} MATERIALIZE INDEX {index} IN PARTITION ID {}",
                    quote(&partition)
                ),
                &[("mutations_sync", "2")],
            )?;
            write_meta(client, &index_done_key(name, index, &partition), "1")?;
            tally.changed += 1;
            report(&step(
                name,
                "materialized index",
                format!("{index} in partition {partition}"),
                started,
            ));
        }
    }
    Ok(tally)
}

/// The indexes in [`STALE_INDEXES`] `have` still carries: dropped, the ones still in
/// use only once nothing the table declares is `outstanding`.
#[allow(clippy::too_many_arguments)]
fn stale_indexes(
    client: &Client,
    layout: &Layout<'_>,
    name: &str,
    have: &Table,
    want: &Table,
    outstanding: usize,
    options: Options,
    report: &mut dyn FnMut(&Event),
) -> Result<Tally> {
    let on = layout.on_cluster();
    let mut tally = Tally::default();
    for stale in STALE_INDEXES.iter().filter(|s| s.table == name) {
        if !have.indexes.contains_key(stale.index) || want.indexes.contains_key(stale.index) {
            continue;
        }
        if stale.until_replaced && outstanding > 0 {
            report(&Event::Skipped {
                object: format!("{name} index {}", stale.index),
                reason: "kept until the indexes that replace it cover every partition: run \
                         with --materialize-indexes"
                    .to_owned(),
            });
            tally.pending += 1;
            continue;
        }
        report(&Event::Found {
            what: "drop_index",
            object: name.to_owned(),
            detail: format!("{}, which no version since creates", stale.index),
        });
        if options.dry_run {
            tally.pending += 1;
            continue;
        }
        let started = Instant::now();
        client.execute(&format!(
            "ALTER TABLE {name}{on} DROP INDEX IF EXISTS {}",
            stale.index
        ))?;
        tally.changed += 1;
        report(&step(
            name,
            "dropped index",
            stale.index.to_owned(),
            started,
        ));
    }
    Ok(tally)
}

/// The partitions of `table` holding a part written before `index` was first seen, and
/// not indexed since.
fn unindexed(client: &Client, table: &str, index: &str, added: u64) -> Result<Vec<String>> {
    let mut candidates = Vec::new();
    client.query_lines(
        &format!(
            "SELECT DISTINCT partition_id AS id FROM system.parts \
             WHERE active AND database = currentDatabase() AND table = {} \
             AND toUnixTimestamp(modification_time) <= {added} ORDER BY id DESC",
            quote(table)
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            candidates.push(text(&value, "id"));
            Ok(ControlFlow::Continue(()))
        },
    )?;
    let mut out = Vec::new();
    for partition in candidates {
        if read_meta(client, &index_done_key(table, index, &partition))?.is_none() {
            out.push(partition);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------------
// Rollups.

/// A rollup kept by a materialized view, which holds every row only from when it was
/// created — the moment `telemetryd_meta` records under `marker`.
struct Rollup {
    marker: &'static str,
    table: &'static str,
    view: &'static str,
    base: &'static str,
    /// The rollup's time column, and the width of one of its buckets in nanoseconds.
    unit: &'static str,
    unit_nanos: u64,
    /// Buckets are `(start, end]`, as LogQL windows are, rather than `[start, end)`.
    open_start: bool,
    /// The column is a `DateTime`, rather than a count of buckets since the epoch.
    datetime: bool,
}

const ROLLUPS: &[Rollup] = &[
    Rollup {
        marker: schema::LOG_VOLUME_SINCE,
        table: schema::LOG_VOLUME,
        view: schema::LOG_VOLUME_VIEW,
        base: schema::LOGS,
        unit: "Second",
        unit_nanos: 1_000_000_000,
        open_start: true,
        datetime: false,
    },
    Rollup {
        marker: schema::LOG_VOLUME_MINUTES_SINCE,
        table: schema::LOG_VOLUME_MINUTES,
        view: schema::LOG_VOLUME_MINUTES_VIEW,
        base: schema::LOGS,
        unit: "Minute",
        unit_nanos: 60_000_000_000,
        open_start: true,
        datetime: false,
    },
    Rollup {
        marker: schema::TRACE_KEYS_SINCE,
        table: schema::TRACE_KEYS,
        view: schema::TRACE_KEYS_VIEW,
        base: schema::TRACES,
        unit: "Hour",
        unit_nanos: 3_600_000_000_000,
        open_start: false,
        datetime: true,
    },
];

const DAY_NANOS: u64 = 86_400_000_000_000;
/// How long after a rollup's marker a backfill may start: rows with times just before
/// it may still be arriving, and a backfill reading them as the view counts them would
/// count them twice.
const SETTLED: u64 = 10 * 60 * 1_000_000_000;

/// Fill each rollup in for the rows older than it, a day at a time, newest first.
///
/// Each day adds, per key, what the rows hold and the rollup does not: the rows
/// counted the way the view counts them, less what the rollup already has. A day done
/// twice adds nothing the second time, so an interrupted backfill is run again and
/// continues; and the marker moves back a day at a time, so queries use what is filled
/// in as soon as it is.
fn backfill(
    client: &Client,
    config: &ClickHouseConfig,
    retention: &RetentionConfig,
    options: Options,
    report: &mut dyn FnMut(&Event),
) -> Result<Tally> {
    let mut tally = Tally::default();
    let present = tables(client)?;
    for rollup in ROLLUPS {
        let Some(table) = present.get(rollup.table) else {
            continue;
        };
        if !present.contains_key(rollup.base) {
            continue;
        }
        let Some(since) = schema::rollup_start(client, rollup.marker)? else {
            continue;
        };
        if since == 0 {
            continue;
        }
        let Some(oldest) = oldest_row(client, rollup.base)? else {
            // Nothing older to fill in.
            if !options.dry_run {
                write_meta(client, rollup.marker, "0")?;
            }
            continue;
        };
        if oldest >= since {
            if !options.dry_run {
                write_meta(client, rollup.marker, "0")?;
            }
            continue;
        }
        report(&Event::Found {
            what: "backfill",
            object: rollup.table.to_owned(),
            detail: format!(
                "holds every row only from {}; {} has rows from {}",
                seconds_text(since),
                rollup.base,
                seconds_text(oldest)
            ),
        });
        if !options.backfill || options.dry_run {
            tally.pending += 1;
            continue;
        }
        if now_nanos() < since.saturating_add(SETTLED) {
            report(&Event::Skipped {
                object: rollup.table.to_owned(),
                reason: format!(
                    "created too recently to fill in safely; run again after {}",
                    seconds_text(since.saturating_add(SETTLED))
                ),
            });
            tally.pending += 1;
            continue;
        }
        let select = schema::view_select(config, retention, rollup.view)
            .ok_or_else(|| Error::Config(format!("telemetryd has no view {}", rollup.view)))?;
        if !config.cluster.trim().is_empty() {
            // Read what every replica has written, or a row another replica holds would
            // look missing here and be added again.
            client.execute(&format!("SYSTEM SYNC REPLICA {}", rollup.base))?;
            client.execute(&format!("SYSTEM SYNC REPLICA {}", rollup.table))?;
        }
        let owner = read_meta(client, LEASE)?
            .and_then(|l| l.split_once('@').map(|(h, _)| h.to_owned()))
            .unwrap_or_default();
        let mut high = since.div_ceil(rollup.unit_nanos) * rollup.unit_nanos;
        // A day `(low, high]` holds a row at `high` and not one at `low`, so with
        // buckets open at the start the day ending at the oldest row is one more.
        while high > oldest || rollup.open_start && high == oldest && high > 0 {
            let low = ((high - 1) / DAY_NANOS) * DAY_NANOS;
            let started = Instant::now();
            delta(client, rollup, table, &select, low, high)?;
            let complete = if rollup.open_start {
                low < oldest
            } else {
                low <= oldest
            };
            write_meta(
                client,
                rollup.marker,
                &if complete { 0 } else { low }.to_string(),
            )?;
            renew(client, &owner)?;
            tally.changed += 1;
            report(&step(
                rollup.table,
                "filled in",
                format!("{} to {}", seconds_text(low), seconds_text(high)),
                started,
            ));
            high = low;
        }
    }
    Ok(tally)
}

/// Add to `rollup` what the rows in `(low, high]` (or `[low, high)`) hold and it does
/// not, key by key.
fn delta(
    client: &Client,
    rollup: &Rollup,
    table: &Table,
    select: &str,
    low: u64,
    high: u64,
) -> Result<()> {
    let keys: Vec<String> = table
        .sorting_key
        .split(',')
        .map(|k| k.trim().to_owned())
        .filter(|k| !k.is_empty())
        .collect();
    let mut sums = Vec::new();
    let mut others = Vec::new();
    for (name, (kind, default_kind, _)) in &table.columns {
        if keys.contains(name) || !default_kind.is_empty() {
            continue;
        }
        if kind.starts_with("SimpleAggregateFunction(sum,") {
            sums.push(name.clone());
        } else {
            others.push(name.clone());
        }
    }
    let Some(first) = sums.first() else {
        return Err(Error::Config(format!(
            "{} sums nothing, so it cannot be filled in by difference",
            rollup.table
        )));
    };
    let (start, end) = (sql_nanos(low), sql_nanos(high));
    let base_range = if rollup.open_start {
        format!("Timestamp > {start} AND Timestamp <= {end}")
    } else {
        format!("Timestamp >= {start} AND Timestamp < {end}")
    };
    let (low_unit, high_unit) = (low / rollup.unit_nanos, high / rollup.unit_nanos);
    let rollup_range = if rollup.datetime {
        let per = rollup.unit_nanos / 1_000_000_000;
        format!(
            "{unit} >= toDateTime({}) AND {unit} < toDateTime({})",
            low_unit * per,
            high_unit * per,
            unit = rollup.unit
        )
    } else {
        format!(
            "{unit} >= {low_unit} AND {unit} < {high_unit}",
            unit = rollup.unit
        )
    };
    let restricted = restricted(select, rollup.base, &base_range)?;
    let using = keys.join(", ");
    let have: Vec<String> = sums.iter().map(|s| format!("sum({s}) AS {s}")).collect();
    let columns: Vec<String> = keys
        .iter()
        .chain(sums.iter())
        .chain(others.iter())
        .cloned()
        .collect();
    let values: Vec<String> = keys
        .iter()
        .map(|k| format!("b.{k}"))
        .chain(
            sums.iter()
                .map(|s| format!("if(b.{s} > r.{s}, b.{s} - r.{s}, 0)")),
        )
        .chain(others.iter().map(|o| format!("b.{o}")))
        .collect();
    client.execute(&format!(
        "INSERT INTO {table} ({columns}) \
         SELECT {values} FROM ({restricted}) AS b \
         LEFT JOIN (SELECT {using}, {have} FROM {table} WHERE {rollup_range} GROUP BY {using}) AS r \
         USING ({using}) WHERE b.{first} > r.{first}",
        table = rollup.table,
        columns = columns.join(", "),
        values = values.join(", "),
        have = have.join(", "),
    ))
}

/// A view's `SELECT`, reading only the rows of `base` that `condition` admits.
fn restricted(select: &str, base: &str, condition: &str) -> Result<String> {
    let mut found = 0;
    let mut out = String::with_capacity(select.len() + condition.len() + 64);
    let needle = format!("FROM {base}");
    let mut rest = select;
    while let Some(at) = rest.find(&needle) {
        let after = &rest[at + needle.len()..];
        let ends = after
            .chars()
            .next()
            .is_none_or(|c| !(c.is_ascii_alphanumeric() || c == '_'));
        out.push_str(&rest[..at]);
        if ends {
            found += 1;
            let _ = write!(
                out,
                "FROM (SELECT * FROM {base} WHERE {condition}) AS {base}"
            );
        } else {
            out.push_str(&needle);
        }
        rest = after;
    }
    out.push_str(rest);
    if found == 1 {
        Ok(out)
    } else {
        Err(Error::Config(format!(
            "the view's query reads {base} {found} times, not once"
        )))
    }
}

/// The earliest row of a table, in nanoseconds.
fn oldest_row(client: &Client, table: &str) -> Result<Option<u64>> {
    let mut oldest = None;
    client.query_lines(
        &format!(
            "SELECT toUnixTimestamp64Nano(min(Timestamp)) AS oldest, count() > 0 AS any \
             FROM {table}"
        ),
        |line| {
            let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            if number(&value, "any") > 0 {
                oldest = Some(number(&value, "oldest"));
            }
            Ok(ControlFlow::Break(()))
        },
    )?;
    Ok(oldest)
}

fn sql_nanos(nanos: u64) -> String {
    format!("fromUnixTimestamp64Nano(toInt64({nanos}), 'UTC')")
}

/// A moment, as an operator reads it: `2026-10-25T14:00:00Z`.
fn seconds_text(nanos: u64) -> String {
    let seconds = nanos / 1_000_000_000;
    let (days, rest) = (seconds / 86_400, seconds % 86_400);
    // Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
    let z = i64::try_from(days).unwrap_or(i64::MAX - 719_468) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3_600,
        rest % 3_600 / 60,
        rest % 60
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_view_reads_only_the_rows_asked_for() {
        let select = "SELECT a FROM (SELECT a FROM otel_logs)\nGROUP BY a";
        assert_eq!(
            restricted(select, "otel_logs", "x = 1").unwrap(),
            "SELECT a FROM (SELECT a FROM (SELECT * FROM otel_logs WHERE x = 1) AS otel_logs)\nGROUP BY a"
        );
        // A longer name sharing the prefix is not the table.
        assert!(restricted("SELECT 1 FROM otel_logs_x", "otel_logs", "x").is_err());
        assert!(
            restricted(
                "SELECT 1 FROM otel_traces\nARRAY JOIN k",
                "otel_traces",
                "x"
            )
            .is_ok()
        );
    }

    #[test]
    fn a_rename_gives_a_replicated_table_its_own_keeper_path() {
        let config = ClickHouseConfig {
            cluster: "td".to_owned(),
            ..ClickHouseConfig::default()
        };
        let statement = "CREATE TABLE IF NOT EXISTS t ON CLUSTER 'td' (x UInt8) ENGINE = \
                         ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/t', '{replica}') ORDER BY x";
        let renamed = renamed(statement, "t", "t__td_old_1", &config).unwrap();
        assert!(renamed.starts_with("CREATE TABLE t__td_old_1 ON CLUSTER"));
        assert!(renamed.contains("/{database}/t__td_old_1'"));
        assert!(!renamed.contains("/{database}/t'"));
    }

    #[test]
    fn engines_compare_without_replication_or_keeper_paths() {
        let table = |engine: &str, full: &str| Table {
            engine: engine.to_owned(),
            engine_full: full.to_owned(),
            ..Table::default()
        };
        let local = table(
            "ReplacingMergeTree",
            "ReplacingMergeTree(Version) PARTITION BY x",
        );
        let replicated = table(
            "ReplicatedReplacingMergeTree",
            "ReplicatedReplacingMergeTree('/p/{shard}/t', '{replica}', Version) ORDER BY x",
        );
        assert_eq!(engine_kind(&local), engine_kind(&replicated));
        let plain = table("MergeTree", "MergeTree PARTITION BY x");
        assert_ne!(engine_kind(&local), engine_kind(&plain));
    }

    #[test]
    fn only_tables_whose_rows_merge_to_one_answer_are_copied_ahead() {
        let mut table = Table {
            engine: "AggregatingMergeTree".to_owned(),
            ..Table::default()
        };
        table.columns.insert(
            "Start".to_owned(),
            (
                "SimpleAggregateFunction(min, DateTime64(9, 'UTC'))".to_owned(),
                String::new(),
                String::new(),
            ),
        );
        assert!(idempotent(&table));
        table.columns.insert(
            "Rows".to_owned(),
            (
                "SimpleAggregateFunction(sum, UInt64)".to_owned(),
                String::new(),
                String::new(),
            ),
        );
        assert!(!idempotent(&table));
    }
}
