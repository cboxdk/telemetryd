//! Hourly and daily counter summaries: what makes a 90-day `rate` read a few rows per
//! series instead of every sample.
//!
//! # Made by telemetryd, with the local store's arithmetic
//!
//! A summary is a [`StreamFold`]: the same struct a sealed local segment carries, filled
//! by the same [`StreamFold::add`] from the samples in time order and joined by the same
//! [`StreamFold::merge_later`]. ClickHouse stores them; it does not compute them, because
//! a counter's increase across a reset is an ordered, sample-by-sample rule no aggregate
//! function applies, and a view sees an insert at a time rather than an hour.
//!
//! [`roll_up`] makes them: hours that closed a few minutes ago, a day's worth per read,
//! and a day's summary from its 24 hours once they are all made. Every instance runs it;
//! a lease in `telemetryd_meta` keeps two from doing the same work at once, and if they
//! do anyway a fold rewritten is the same fold, kept once by its table's `Version`.
//!
//! # Trusted only when complete
//!
//! Late data — an agent replaying after an outage — lands in an hour already summarised.
//! `telemetryd_sample_counts`, kept by a view on every insert, counts what each hour now
//! holds; a fold counts what it saw. A fold can only have seen fewer, so where the two
//! sums over a query's series agree for a bucket every fold in it is complete, and where
//! they do not the bucket is read from the samples instead. [`roll_up`] also looks back
//! over the last three days for such buckets and makes them again.
//!
//! # Answering a window
//!
//! A window `(start, end]` is cut into whole days, whole hours and what is left at its
//! ends, in time order. The ends and any bucket that is not complete are read as
//! samples — one read for every window of a query together — and each series' pieces
//! are joined in order, exactly as the local store joins its segments' summaries with
//! the edges it folds from rows. The answer agrees with folding every sample to within
//! floating-point rounding, as the local store's own summaries do.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::ControlFlow;

use serde::{Deserialize, Serialize};
use telemetryd_core::{Error, Labels, Result};

use super::Client;
use super::metrics_schema::{COUNTS, DAILY, FOLDS, HOUR_NANOS, HOURLY, SAMPLES, SERIES, bucket_of};
use super::schema::META;
use super::sql;
use crate::folds::StreamFold;
use crate::metric_backend::Folded;

/// How long after an hour ends before it is summarised: samples are still arriving for
/// it until a sender's batch interval and retries have passed.
const SETTLE_NANOS: u64 = 5 * 60 * 1_000_000_000;

/// How far back [`roll_up`] looks for hours late data has made incomplete.
const REPAIR_HOURS: u64 = 72;

/// Hours one [`roll_up`] pass may summarise, so a first pass over months of existing
/// data is spread over many and none holds the lease for long.
const HOURS_PER_PASS: u64 = 24 * 7;

/// How long a lease to summarise is held.
const LEASE_NANOS: u64 = 10 * 60 * 1_000_000_000;

/// Samples one [`fold_windows`] reads beside the summaries before it declines.
const MAX_RAW_SAMPLES: usize = 20_000_000;

/// Rows sent per insert while summarising.
const ROWS_PER_INSERT: usize = 100_000;

const HOURS_CURSOR: &str = "metric_folds_next_hour";
const DAYS_CURSOR: &str = "metric_folds_next_day";
const LEASE: &str = "metric_folds_lease";

/// A summary's numbers as the table stores them: bit patterns, so a NaN or an
/// infinity in a sum or an increase survives as it is.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Bits {
    seen: u64,
    first_nanos: u64,
    last_nanos: u64,
    first_value: u64,
    last_value: u64,
    increase: u64,
    sum: u64,
    compensation: u64,
    min: u64,
    max: u64,
}

impl From<&StreamFold> for Bits {
    fn from(fold: &StreamFold) -> Self {
        Self {
            seen: fold.seen,
            first_nanos: fold.first_nanos,
            last_nanos: fold.last_nanos,
            first_value: fold.first_value.to_bits(),
            last_value: fold.last_value.to_bits(),
            increase: fold.increase.to_bits(),
            sum: fold.sum.to_bits(),
            compensation: fold.compensation.to_bits(),
            min: fold.min.to_bits(),
            max: fold.max.to_bits(),
        }
    }
}

impl From<Bits> for StreamFold {
    fn from(bits: Bits) -> Self {
        Self {
            seen: bits.seen,
            first_nanos: bits.first_nanos,
            last_nanos: bits.last_nanos,
            first_value: f64::from_bits(bits.first_value),
            last_value: f64::from_bits(bits.last_value),
            increase: f64::from_bits(bits.increase),
            sum: f64::from_bits(bits.sum),
            compensation: f64::from_bits(bits.compensation),
            min: f64::from_bits(bits.min),
            max: f64::from_bits(bits.max),
            gauges: true,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct FoldRow<'a> {
    metric_name: &'a str,
    tier: u8,
    bucket: u64,
    series_hash: u64,
    starts: u64,
    #[serde(flatten)]
    bits: Bits,
    version: u64,
}

/// Fold rows written in batches.
struct Writer<'a> {
    client: &'a Client,
    body: Vec<u8>,
    rows: usize,
    written: usize,
}

impl<'a> Writer<'a> {
    fn new(client: &'a Client) -> Self {
        Self {
            client,
            body: Vec::new(),
            rows: 0,
            written: 0,
        }
    }

    fn push(&mut self, row: &FoldRow<'_>) -> Result<()> {
        serde_json::to_writer(&mut self.body, row)
            .map_err(|e| Error::Unavailable(format!("encoding a fold: {e}")))?;
        self.body.push(b'\n');
        self.rows += 1;
        if self.rows >= ROWS_PER_INSERT {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows > 0 {
            self.client.insert_bulk(FOLDS, &self.body)?;
            self.written += self.rows;
        }
        self.body.clear();
        self.rows = 0;
        Ok(())
    }
}

pub(super) fn read_meta(client: &Client, name: &str) -> Result<Option<String>> {
    let mut value = None;
    client.query_lines(
        &format!(
            "SELECT argMax(Value, Updated) AS value, count() AS n FROM {META} WHERE Name = {}",
            sql::quote(name)
        ),
        |line| {
            let row: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            if row
                .get("n")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
                > 0
            {
                value = row
                    .get("value")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;
    Ok(value)
}

pub(super) fn write_meta(client: &Client, name: &str, value: &str) -> Result<()> {
    client.execute(&format!(
        "INSERT INTO {META} SELECT {}, {}, now64(9)",
        sql::quote(name),
        sql::quote(value)
    ))
}

/// Every metric name with samples in the hours, for numbering them in a fixed-width row.
fn metric_names(client: &Client, first_hour: u64, last_hour: u64) -> Result<Vec<String>> {
    let mut names = Vec::new();
    client.query_lines(
        &format!(
            "SELECT DISTINCT MetricName AS name FROM {SERIES} \
             WHERE Day >= toDate(toDateTime({}, 'UTC')) AND Day <= toDate(toDateTime({}, 'UTC'))",
            first_hour * 3600,
            (last_hour + 1) * 3600
        ),
        |line| {
            names.push(super::scan::field(line, "name")?);
            Ok(ControlFlow::Continue(()))
        },
    )?;
    Ok(names)
}

/// Summarise every series' samples per hour, for the hours `first..=last`, from one
/// read of them — or only the series in `only`, which the read then reaches by the
/// samples table's sort key instead of reading the day.
fn roll_hours(
    client: &Client,
    (first, last): (u64, u64),
    only: Option<&BTreeSet<(String, u64)>>,
    version: u64,
) -> Result<usize> {
    let names: Vec<String> = match only {
        Some(pairs) => pairs
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        None => metric_names(client, first, last)?,
    };
    if names.is_empty() {
        return Ok(0);
    }
    let quoted: Vec<String> = names.iter().map(|n| sql::quote(n)).collect();
    let series = only.map_or(String::new(), |pairs| {
        let pairs: Vec<String> = pairs
            .iter()
            .map(|(name, hash)| format!("({}, {hash})", sql::quote(name)))
            .collect();
        format!(" AND (MetricName, SeriesHash) IN ({})", pairs.join(", "))
    });
    // A name's position, from one: `UInt32`, so no number of names wraps it, and `0` —
    // a name that is not in the list — is a row skipped, not one filed under another.
    let query = format!(
        "SELECT toUInt32(indexOf([{}], MetricName)), SeriesHash, \
         toUInt64(toUnixTimestamp64Nano(Timestamp)), Value, Special FROM {SAMPLES} \
         WHERE Timestamp > {} AND Timestamp <= {}{series} \
         ORDER BY MetricName, SeriesHash, Timestamp",
        quoted.join(", "),
        sql::nanos(first * HOUR_NANOS),
        sql::nanos((last + 1) * HOUR_NANOS),
    );
    let mut writer = Writer::new(client);
    let mut current: Option<(u32, u64, u64)> = None;
    let mut fold = StreamFold::default();
    let emit = |key: (u32, u64, u64), fold: &StreamFold, writer: &mut Writer<'_>| {
        if fold.seen == 0 {
            return Ok(());
        }
        let Some(name) = usize::try_from(key.0)
            .ok()
            .and_then(|i| i.checked_sub(1))
            .and_then(|i| names.get(i))
        else {
            return Ok(());
        };
        writer.push(&FoldRow {
            metric_name: name,
            tier: HOURLY,
            bucket: key.2,
            series_hash: key.1,
            starts: key.2 * 3600,
            bits: Bits::from(fold),
            version,
        })
    };
    client.query_rows(&query, 4 + 8 + 8 + 8 + 1, |row| {
        let read = |at: usize| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&row[at..at + 8]);
            u64::from_le_bytes(bytes)
        };
        let name = u32::from_le_bytes([row[0], row[1], row[2], row[3]]);
        if name == 0 {
            return Ok(ControlFlow::Continue(()));
        }
        let hash = read(4);
        let nanos = read(12);
        let value = super::metrics::joined_value(f64::from_bits(read(20)), row[28]);
        let key = (name, hash, bucket_of(nanos, HOUR_NANOS));
        if current != Some(key) {
            if let Some(previous) = current {
                emit(previous, &fold, &mut writer)?;
            }
            current = Some(key);
            fold = StreamFold::default();
        }
        fold.add(nanos, value);
        Ok(ControlFlow::Continue(()))
    })?;
    if let Some(previous) = current {
        emit(previous, &fold, &mut writer)?;
    }
    writer.flush()?;
    Ok(writer.written)
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct StoredFold {
    metric_name: String,
    series_hash: u64,
    bucket: u64,
    #[serde(flatten)]
    bits: Bits,
}

/// Join each series' hourly summaries of the days `first..=last` into one per day.
fn roll_days(client: &Client, first: u64, last: u64, version: u64) -> Result<usize> {
    let query = format!(
        "SELECT MetricName, SeriesHash, Bucket, Seen, FirstNanos, LastNanos, FirstValue, \
         LastValue, Increase, Sum, Compensation, Min, Max FROM {FOLDS} FINAL \
         WHERE Tier = {HOURLY} AND Bucket >= {} AND Bucket < {} \
         ORDER BY MetricName, SeriesHash, Bucket",
        first * 24,
        (last + 1) * 24
    );
    let mut writer = Writer::new(client);
    let mut current: Option<(String, u64, u64)> = None;
    let mut day = StreamFold::default();
    let emit = |key: &(String, u64, u64), fold: &StreamFold, writer: &mut Writer<'_>| {
        if fold.seen == 0 {
            return Ok(());
        }
        writer.push(&FoldRow {
            metric_name: &key.0,
            tier: DAILY,
            bucket: key.2,
            series_hash: key.1,
            starts: key.2 * 86_400,
            bits: Bits::from(fold),
            version,
        })
    };
    let mut refused = None;
    client.query_lines(&query, |line| {
        let row: StoredFold = serde_json::from_slice(line)
            .map_err(|e| Error::Unavailable(format!("reading a fold from ClickHouse: {e}")))?;
        let key = (row.metric_name, row.series_hash, row.bucket / 24);
        if current.as_ref() != Some(&key) {
            if let Some(previous) = &current
                && let Err(error) = emit(previous, &day, &mut writer)
            {
                refused = Some(error);
                return Ok(ControlFlow::Break(()));
            }
            current = Some(key);
            day = StreamFold::default();
        }
        day.merge_later(&StreamFold::from(row.bits));
        Ok(ControlFlow::Continue(()))
    })?;
    if let Some(error) = refused {
        return Err(error);
    }
    if let Some(previous) = &current {
        emit(previous, &day, &mut writer)?;
    }
    writer.flush()?;
    Ok(writer.written)
}

/// Hours in `first..=last` whose summaries no longer count every sample they hold.
fn incomplete_hours(client: &Client, first: u64, last: u64) -> Result<Vec<u64>> {
    let query = format!(
        "SELECT Hour AS hour FROM \
         (SELECT Hour, sum(Samples) AS n FROM {COUNTS} WHERE Hour >= {first} AND Hour <= {last} GROUP BY Hour) AS c \
         LEFT JOIN \
         (SELECT Bucket AS Hour, sum(Seen) AS s FROM {FOLDS} FINAL \
          WHERE Tier = {HOURLY} AND Bucket >= {first} AND Bucket <= {last} GROUP BY Hour) AS f \
         USING (Hour) WHERE n != s ORDER BY hour"
    );
    let mut hours = Vec::new();
    client.query_lines(&query, |line| {
        let row: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
        if let Some(hour) = row.get("hour").and_then(serde_json::Value::as_u64) {
            hours.push(hour);
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(hours)
}

/// Series repaired one by one, at most, per day: past this many a day's hours are
/// summarised whole again, which is one read of the day rather than a long list.
const REPAIR_SERIES: usize = 5_000;

/// The series whose summaries of these hours no longer count every sample they hold.
fn incomplete_series(client: &Client, hours: &[u64]) -> Result<Vec<(u64, String, u64)>> {
    if hours.is_empty() {
        return Ok(Vec::new());
    }
    let list: Vec<String> = hours.iter().map(u64::to_string).collect();
    let list = list.join(", ");
    let query = format!(
        "SELECT Hour AS hour, MetricName AS name, SeriesHash AS hash FROM \
         (SELECT MetricName, SeriesHash, Hour, sum(Samples) AS n FROM {COUNTS} \
          WHERE Hour IN ({list}) GROUP BY MetricName, SeriesHash, Hour) AS c \
         LEFT JOIN \
         (SELECT MetricName, SeriesHash, toUInt32(Bucket) AS Hour, argMax(Seen, Version) AS s \
          FROM {FOLDS} WHERE Tier = {HOURLY} AND Bucket IN ({list}) \
          GROUP BY MetricName, SeriesHash, Bucket) AS f \
         USING (MetricName, SeriesHash, Hour) WHERE n != s"
    );
    let mut out = Vec::new();
    client.query_lines(&query, |line| {
        let row: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
        let hour = row.get("hour").and_then(serde_json::Value::as_u64);
        let name = row.get("name").and_then(serde_json::Value::as_str);
        let hash = row.get("hash").and_then(serde_json::Value::as_u64);
        if let (Some(hour), Some(name), Some(hash)) = (hour, name, hash) {
            out.push((hour, name.to_owned(), hash));
        }
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(out)
}

/// A day's hours to make again, and the series to make them for.
type Repairs = (BTreeSet<u64>, BTreeSet<(String, u64)>);

/// Make again what late data broke in `hours`, a day at a time: only the series whose
/// counts moved where they are few, the day's hours whole where they are many.
fn repair(client: &Client, hours: &[u64], version: u64) -> Result<(usize, BTreeSet<u64>)> {
    let mut by_day: BTreeMap<u64, Repairs> = BTreeMap::new();
    for (hour, name, hash) in incomplete_series(client, hours)? {
        let day = by_day.entry(hour / 24).or_default();
        day.0.insert(hour);
        day.1.insert((name, hash));
    }
    let mut written = 0;
    for (hours, series) in by_day.values() {
        let (Some(&first), Some(&last)) = (hours.first(), hours.last()) else {
            continue;
        };
        let only = (series.len() <= REPAIR_SERIES).then_some(series);
        written += roll_hours(client, (first, last), only, version)?;
    }
    Ok((written, by_day.into_keys().collect()))
}

/// Take the lease to summarise, or learn that another instance holds it.
///
/// Written, then read back: the newest write wins, so of two instances writing at once
/// only the one that reads its own name goes on.
fn take_lease(client: &Client, now: u64, owner: &str) -> Result<bool> {
    if let Some(lease) = read_meta(client, LEASE)?
        && let Some((holder, until)) = lease.split_once('@')
        && holder != owner
        && until.parse::<u64>().unwrap_or(0) > now
    {
        return Ok(false);
    }
    write_meta(client, LEASE, &format!("{owner}@{}", now + LEASE_NANOS))?;
    Ok(read_meta(client, LEASE)?
        .and_then(|lease| lease.split_once('@').map(|(holder, _)| holder == owner))
        .unwrap_or(false))
}

/// What one [`roll_up`] pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RollUp {
    pub hours: u64,
    pub repaired_hours: u64,
    pub days: u64,
    pub folds_written: usize,
}

/// Summarise what has settled since the last pass, mend what late data broke, and join
/// complete days. `owner` names this instance in the lease.
///
/// # Errors
/// When ClickHouse cannot be read or written; the next pass starts where this one
/// stopped.
pub fn roll_up(client: &Client, now: u64, owner: &str) -> Result<RollUp> {
    let mut done = RollUp::default();
    if !take_lease(client, now, owner)? {
        return Ok(done);
    }

    let Some(ready) = (now.saturating_sub(SETTLE_NANOS) / HOUR_NANOS).checked_sub(1) else {
        return Ok(done);
    };
    let next = match read_meta(client, HOURS_CURSOR)?.and_then(|v| v.parse::<u64>().ok()) {
        Some(next) => next,
        None => match oldest_hour(client)? {
            Some(oldest) => oldest,
            None => return Ok(done),
        },
    };
    let mut cursor = next;
    let budget_end = next.saturating_add(HOURS_PER_PASS);
    while cursor <= ready && cursor < budget_end {
        // A day at a time: one read of the samples table's day partition.
        let last = ready.min((cursor / 24) * 24 + 23).min(budget_end - 1);
        done.folds_written += roll_hours(client, (cursor, last), None, now)?;
        done.hours += last - cursor + 1;
        cursor = last + 1;
        write_meta(client, HOURS_CURSOR, &cursor.to_string())?;
        // Renewed as it goes: a pass over weeks of backlog outlasts one lease.
        write_meta(client, LEASE, &format!("{owner}@{}", now + LEASE_NANOS))?;
    }

    let mut repaired_days = BTreeSet::new();
    if cursor > 0 {
        let from = cursor.saturating_sub(REPAIR_HOURS);
        let hours = incomplete_hours(client, from, cursor - 1)?;
        done.repaired_hours = hours.len() as u64;
        let (written, days) = repair(client, &hours, now)?;
        done.folds_written += written;
        repaired_days = days;
    }

    // A day is complete once all of its hours are summarised.
    let complete_until = cursor / 24;
    let next_day = match read_meta(client, DAYS_CURSOR)?.and_then(|v| v.parse::<u64>().ok()) {
        Some(day) => day,
        None => next / 24,
    };
    if next_day < complete_until {
        done.folds_written += roll_days(client, next_day, complete_until - 1, now)?;
        done.days += complete_until - next_day;
        write_meta(client, DAYS_CURSOR, &complete_until.to_string())?;
    }
    for day in repaired_days.into_iter().filter(|d| *d < complete_until) {
        done.folds_written += roll_days(client, day, day, now)?;
    }
    Ok(done)
}

/// Have the summaries made again from `hour` on, by whichever instance next holds the
/// lease: for samples written into hours already summarised — an import of history,
/// most often. A cursor already earlier is left where it is.
///
/// # Errors
/// When ClickHouse cannot be read or written.
pub fn refold_from(client: &Client, hour: u64) -> Result<()> {
    if let Some(next) = read_meta(client, HOURS_CURSOR)?.and_then(|v| v.parse::<u64>().ok())
        && next > hour
    {
        write_meta(client, HOURS_CURSOR, &hour.to_string())?;
    }
    let day = hour / 24;
    if let Some(next) = read_meta(client, DAYS_CURSOR)?.and_then(|v| v.parse::<u64>().ok())
        && next > day
    {
        write_meta(client, DAYS_CURSOR, &day.to_string())?;
    }
    Ok(())
}

fn oldest_hour(client: &Client) -> Result<Option<u64>> {
    let mut oldest = None;
    client.query_lines(
        &format!(
            "SELECT toUInt64(toUnixTimestamp64Nano(min(MinTimestamp))) AS oldest, \
             count() AS n FROM {SERIES}"
        ),
        |line| {
            let row: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
            if row
                .get("n")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
                > 0
            {
                oldest = row
                    .get("oldest")
                    .and_then(serde_json::Value::as_u64)
                    .map(|nanos| bucket_of(nanos, HOUR_NANOS));
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;
    Ok(oldest)
}

/// One series' summaries by tier and bucket, each with the `Version` it was read at.
type Versioned = BTreeMap<(u8, u64), (u64, StreamFold)>;

/// One piece of a window, in time order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Piece {
    /// Samples in `(start, end]`.
    Raw(u64, u64),
    Hour(u64),
    Day(u64),
}

/// The series a query reads, as `metrics.rs` resolved them.
pub(super) struct Reading<'a> {
    pub series: &'a HashMap<u64, Labels>,
    /// `MetricName IN (…) AND SeriesHash IN (…)`.
    pub condition: String,
}

/// Bucket sums for `tier` over the query's series, `(counted, folded)` per bucket, for
/// the buckets in `runs` — inclusive `(first, last)` bucket numbers.
///
/// A fold made again is a second row until the table merges; each series' bucket is
/// counted at its newest `Version` only, or the two would add up to more than either
/// saw, and an incomplete bucket could look complete.
fn completeness(
    client: &Client,
    reading: &Reading<'_>,
    tier: u8,
    runs: &[(u64, u64)],
) -> Result<HashMap<u64, (u64, u64)>> {
    let mut sums: HashMap<u64, (u64, u64)> = HashMap::new();
    if runs.is_empty() {
        return Ok(sums);
    }
    let per = if tier == DAILY { 24 } else { 1 };
    let hours: Vec<String> = runs
        .iter()
        .map(|(first, last)| format!("(Hour >= {} AND Hour < {})", first * per, (last + 1) * per))
        .collect();
    let buckets: Vec<String> = runs
        .iter()
        .map(|(first, last)| format!("(Bucket >= {first} AND Bucket <= {last})"))
        .collect();
    let counted = format!(
        "SELECT intDiv(Hour, {per}) AS bucket, sum(Samples) AS n FROM {COUNTS} \
         WHERE {} AND ({}) GROUP BY bucket",
        reading.condition,
        hours.join(" OR ")
    );
    client.query_lines(&counted, |line| {
        let row: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
        let number = |k: &str| row.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0);
        sums.entry(number("bucket")).or_default().0 += number("n");
        Ok(ControlFlow::Continue(()))
    })?;
    let folded = format!(
        "SELECT bucket, sum(seen) AS n FROM \
         (SELECT Bucket AS bucket, argMax(Seen, Version) AS seen FROM {FOLDS} \
          WHERE {} AND Tier = {tier} AND ({}) GROUP BY MetricName, SeriesHash, Bucket) \
         GROUP BY bucket",
        reading.condition,
        buckets.join(" OR ")
    );
    client.query_lines(&folded, |line| {
        let row: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
        let number = |k: &str| row.get(k).and_then(serde_json::Value::as_u64).unwrap_or(0);
        sums.entry(number("bucket")).or_default().1 += number("n");
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(sums)
}

/// Cut `(start, end]` into pieces, in time order.
fn plan(
    (start, end): (u64, u64),
    hour_complete: &dyn Fn(u64) -> bool,
    day_complete: &dyn Fn(u64) -> bool,
) -> Vec<Piece> {
    let mut pieces = Vec::new();
    let raw = |pieces: &mut Vec<Piece>, from: u64, to: u64| {
        if from >= to {
            return;
        }
        if let Some(Piece::Raw(_, last)) = pieces.last_mut()
            && *last == from
        {
            *last = to;
            return;
        }
        pieces.push(Piece::Raw(from, to));
    };
    let first = start.div_ceil(HOUR_NANOS);
    let last = end / HOUR_NANOS;
    if first >= last {
        raw(&mut pieces, start, end);
        return pieces;
    }
    raw(&mut pieces, start, first * HOUR_NANOS);
    let mut hour = first;
    while hour < last {
        if hour % 24 == 0 && hour + 24 <= last && day_complete(hour / 24) {
            pieces.push(Piece::Day(hour / 24));
            hour += 24;
        } else if hour_complete(hour) {
            pieces.push(Piece::Hour(hour));
            hour += 1;
        } else {
            raw(&mut pieces, hour * HOUR_NANOS, (hour + 1) * HOUR_NANOS);
            hour += 1;
        }
    }
    raw(&mut pieces, last * HOUR_NANOS, end);
    pieces
}

/// `ranges` merged where they touch or overlap.
fn merged(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    ranges.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (from, to) in ranges {
        match out.last_mut() {
            Some(last) if from <= last.1 => last.1 = last.1.max(to),
            _ => out.push((from, to)),
        }
    }
    out
}

/// Consecutive bucket numbers as `(first, last)` runs.
fn runs(buckets: &BTreeSet<u64>) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &bucket in buckets {
        match out.last_mut() {
            Some(last) if bucket == last.1 + 1 => last.1 = bucket,
            _ => out.push((bucket, bucket)),
        }
    }
    out
}

/// Every window's summaries, from the hourly and daily ones where they are complete
/// and from the samples elsewhere; `None` when that would read most of the samples
/// anyway, and the caller's ordinary read is the better one.
///
/// # Errors
/// When ClickHouse cannot be read.
#[allow(clippy::too_many_lines)]
pub(super) fn fold_windows(
    client: &Client,
    reading: &Reading<'_>,
    windows: &[(u64, u64)],
    gauges: bool,
) -> Result<Option<Vec<Folded>>> {
    let mut out: Vec<Folded> = vec![Vec::new(); windows.len()];
    if windows.is_empty() || reading.series.is_empty() {
        return Ok(Some(out));
    }
    let low = windows.iter().map(|w| w.0).min().unwrap_or(0);
    let high = windows.iter().map(|w| w.1).max().unwrap_or(0);
    let (first_hour, last_hour) = (
        low.div_ceil(HOUR_NANOS),
        (high / HOUR_NANOS).saturating_sub(1),
    );
    if first_hour > last_hour {
        return Ok(None);
    }
    // Whether summaries could help at all, before asking which are complete: a chart of
    // five-minute windows holds no whole hour, and costs nothing more here.
    let hopeful: Vec<Vec<Piece>> = windows
        .iter()
        .map(|&window| plan(window, &|_| true, &|_| true))
        .collect();
    let hopeful_raw: u64 = merged(
        hopeful
            .iter()
            .flatten()
            .filter_map(|piece| match *piece {
                Piece::Raw(from, to) => Some((from, to)),
                _ => None,
            })
            .collect(),
    )
    .iter()
    .map(|(a, b)| b - a)
    .sum();
    if !hopeful
        .iter()
        .flatten()
        .any(|p| !matches!(p, Piece::Raw(..)))
        || hopeful_raw > (high - low) / 4
    {
        return Ok(None);
    }
    let complete = |sums: &HashMap<u64, (u64, u64)>, bucket: u64| {
        sums.get(&bucket)
            .is_none_or(|(counted, folded)| counted == folded)
    };
    // Days first: where a whole day's summary is complete, its hours are never asked
    // about. Hours are checked only where a plan would use one — a window's ragged ends,
    // and the days that are not complete.
    let (first_day, last_day) = (first_hour.div_ceil(24), (last_hour + 1) / 24);
    let days = if first_day < last_day {
        completeness(client, reading, DAILY, &[(first_day, last_day - 1)])?
    } else {
        HashMap::new()
    };
    let day_ok = |day: u64| complete(&days, day) && days.get(&day).is_some_and(|(n, _)| *n > 0);
    let wanted: BTreeSet<u64> = windows
        .iter()
        .flat_map(|&window| plan(window, &|_| true, &day_ok))
        .filter_map(|piece| match piece {
            Piece::Hour(hour) => Some(hour),
            _ => None,
        })
        .collect();
    let hours = completeness(client, reading, HOURLY, &runs(&wanted))?;
    let plans: Vec<Vec<Piece>> = windows
        .iter()
        .map(|&window| plan(window, &|hour| complete(&hours, hour), &day_ok))
        .collect();

    let mut raw_ranges = Vec::new();
    let (mut hours_used, mut days_used) = (BTreeSet::new(), BTreeSet::new());
    for piece in plans.iter().flatten() {
        match *piece {
            Piece::Raw(from, to) => raw_ranges.push((from, to)),
            Piece::Hour(hour) => {
                hours_used.insert(hour);
            }
            Piece::Day(day) => {
                days_used.insert(day);
            }
        }
    }
    let raw_ranges = merged(raw_ranges);
    let raw_span: u64 = raw_ranges.iter().map(|(a, b)| b - a).sum();
    if (hours_used.is_empty() && days_used.is_empty()) || raw_span > (high - low) / 4 {
        return Ok(None);
    }

    // The samples the summaries do not cover, every series' in time order — up to a
    // ceiling, past which the caller's own read, which counts against the query's
    // sample limit as it goes, is the one to make.
    let mut samples: HashMap<u64, Vec<(u64, f64)>> = HashMap::new();
    let mut held = 0usize;
    let mut too_many = false;
    if !raw_ranges.is_empty() {
        let time: Vec<String> = raw_ranges
            .iter()
            .map(|&(from, to)| {
                format!(
                    "(Timestamp > {} AND Timestamp <= {})",
                    sql::nanos(from),
                    sql::nanos(to)
                )
            })
            .collect();
        let query = format!(
            "SELECT SeriesHash, toUInt64(toUnixTimestamp64Nano(Timestamp)), Value, Special, Kind \
             FROM {SAMPLES} WHERE {} AND ({}) ORDER BY MetricName, SeriesHash, Timestamp",
            reading.condition,
            time.join(" OR ")
        );
        client.query_rows(&query, 8 + 8 + 8 + 1 + 1, |row| {
            let read = |at: usize| {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&row[at..at + 8]);
                u64::from_le_bytes(bytes)
            };
            let value = super::metrics::joined_value(f64::from_bits(read(16)), row[24]);
            samples.entry(read(0)).or_default().push((read(8), value));
            held += 1;
            if held > MAX_RAW_SAMPLES {
                too_many = true;
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        })?;
        if too_many {
            return Ok(None);
        }
    }

    // The summaries the plans use, a series at a time.
    let mut folds: HashMap<u64, Versioned> = HashMap::new();
    let mut tiers = Vec::new();
    for (first, last) in runs(&hours_used) {
        tiers.push(format!(
            "(Tier = {HOURLY} AND Bucket >= {first} AND Bucket <= {last})"
        ));
    }
    for (first, last) in runs(&days_used) {
        tiers.push(format!(
            "(Tier = {DAILY} AND Bucket >= {first} AND Bucket <= {last})"
        ));
    }
    // A counter's rate needs neither the sum nor the extremes: a third less to send.
    let gauge_columns = if gauges {
        "Sum, Compensation, Min, Max"
    } else {
        "toUInt64(0), toUInt64(0), toUInt64(0), toUInt64(0)"
    };
    let query = format!(
        "SELECT Tier, toUInt32(Bucket), SeriesHash, Seen, FirstNanos, LastNanos, FirstValue, \
         LastValue, Increase, {gauge_columns}, Version FROM {FOLDS} WHERE {} AND ({})",
        reading.condition,
        tiers.join(" OR ")
    );
    // Not `FINAL`, which costs more than the read: a fold made again is a second row
    // until the table merges, and the newer one is kept here.
    client.query_rows(&query, 1 + 4 + 8 + 11 * 8, |row| {
        let read = |at: usize| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&row[at..at + 8]);
            u64::from_le_bytes(bytes)
        };
        let tier = row[0];
        let bucket = u64::from(u32::from_le_bytes([row[1], row[2], row[3], row[4]]));
        let hash = read(5);
        let field = |i: usize| read(13 + i * 8);
        let fold = StreamFold::from(Bits {
            seen: field(0),
            first_nanos: field(1),
            last_nanos: field(2),
            first_value: field(3),
            last_value: field(4),
            increase: field(5),
            sum: field(6),
            compensation: field(7),
            min: field(8),
            max: field(9),
        });
        let version = field(10);
        let slot = folds.entry(hash).or_default().entry((tier, bucket));
        match slot {
            std::collections::btree_map::Entry::Vacant(vacant) => {
                vacant.insert((version, fold));
            }
            std::collections::btree_map::Entry::Occupied(mut held) if held.get().0 <= version => {
                held.insert((version, fold));
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
        Ok(ControlFlow::Continue(()))
    })?;
    // As the local store declines a counter summary with a NaN folded in.
    if !gauges
        && folds
            .values()
            .flat_map(BTreeMap::values)
            .any(|(_, fold)| fold.seen > 0 && (fold.increase.is_nan() || fold.last_value.is_nan()))
    {
        return Ok(None);
    }

    let mut series: Vec<u64> = folds.keys().chain(samples.keys()).copied().collect();
    series.sort_unstable();
    series.dedup();
    for hash in series {
        let Some(labels) = reading.series.get(&hash) else {
            continue;
        };
        let own_folds = folds.get(&hash);
        let own_samples = samples.get(&hash).map_or(&[][..], Vec::as_slice);
        for (window, pieces) in plans.iter().enumerate() {
            let mut joined = StreamFold::default();
            for piece in pieces {
                let part = match *piece {
                    Piece::Raw(from, to) => {
                        let lo = own_samples.partition_point(|(t, _)| *t <= from);
                        let hi = own_samples.partition_point(|(t, _)| *t <= to);
                        let mut part = StreamFold::default();
                        for &(t, v) in &own_samples[lo..hi] {
                            part.add(t, v);
                        }
                        part
                    }
                    Piece::Hour(hour) => own_folds
                        .and_then(|f| f.get(&(HOURLY, hour)))
                        .map(|(_, fold)| *fold)
                        .unwrap_or_default(),
                    Piece::Day(day) => own_folds
                        .and_then(|f| f.get(&(DAILY, day)))
                        .map(|(_, fold)| *fold)
                        .unwrap_or_default(),
                };
                if part.seen == 0 {
                    continue;
                }
                if !joined.precedes(part.first_nanos) {
                    return Ok(None);
                }
                joined.merge_later(&part);
            }
            if joined.seen > 0 {
                out[window].push((labels.clone(), joined));
            }
        }
    }
    for folded in &mut out {
        folded.sort_by(|a, b| a.0.cmp(&b.0));
    }
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: u64 = HOUR_NANOS;

    #[test]
    fn a_window_is_cut_into_its_whole_days_hours_and_ends() {
        let all = |_| true;
        // From half past the hour before a day to ten past an hour into the next day.
        let start = 23 * H + H / 2;
        let end = 49 * H + H / 6;
        let pieces = plan((start, end), &all, &all);
        assert_eq!(
            pieces,
            vec![
                Piece::Raw(start, 24 * H),
                Piece::Day(1),
                Piece::Hour(48),
                Piece::Raw(49 * H, end),
            ]
        );
    }

    #[test]
    fn an_incomplete_hour_is_read_and_joins_its_neighbours_reads() {
        let pieces = plan((0, 3 * H), &|hour| hour != 1, &|_| false);
        assert_eq!(
            pieces,
            vec![Piece::Hour(0), Piece::Raw(H, 2 * H), Piece::Hour(2)]
        );
        let pieces = plan((H / 2, 2 * H), &|hour| hour != 1, &|_| false);
        assert_eq!(pieces, vec![Piece::Raw(H / 2, 2 * H)]);
    }

    #[test]
    fn a_day_is_used_only_when_whole_and_complete() {
        let pieces = plan((0, 48 * H), &|_| true, &|day| day == 1);
        assert_eq!(pieces.len(), 25);
        assert_eq!(pieces[24], Piece::Day(1));
    }
}
