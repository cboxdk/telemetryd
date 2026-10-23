//! Metric samples in `telemetryd_samples`, behind the same interface as the local
//! store. See [`super::metrics_schema`] for the tables.
//!
//! # What runs where
//!
//! A selector's matchers are resolved against `telemetryd_series` first — the day's
//! series, a few thousand rows, matched in Rust by the matcher code every store uses —
//! and the samples table is then read for exactly those series: by metric name and
//! series hash, which are its sort key, inside the query's windows only. Samples come
//! back in `RowBinary`, a series at a time and in time order, and are handed over as
//! the local store hands its runs.
//!
//! Counter summaries (`fold_window`) are declined: the engine then folds the samples
//! itself, which is exact. A rollup that answered them would have to reproduce the
//! local store's floating-point order of operations to give the same digits.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::ControlFlow;

use serde::{Deserialize, Serialize};
use telemetryd_core::metric::STALE_MARKER_BITS;
use telemetryd_core::{
    APP_LABEL, Error, LabelMatcher, Labels, METRIC_NAME_LABEL, MetricKind, MetricSample, Result,
    matches_all,
};

use super::Client;
use super::metrics_schema::{
    INGEST, NAN, NEGATIVE_INFINITY, POSITIVE_INFINITY, SAMPLES, SERIES, STALE, VALUE,
};
use super::scan::labels_of;
use super::sql;
use crate::folds::StreamFold;
use crate::metric_backend::MetricStore;
use crate::metrics::SeriesRun;
use crate::records::Scan;
use crate::topk::Order;

/// Windows sent to ClickHouse one condition each; past this many the span from the
/// first to the last is read and the samples between windows dropped here.
const MAX_WINDOWS: usize = 512;

/// One sample as `RowBinary` sends it: series hash, nanoseconds, value, special, kind.
const ROW: usize = 8 + 8 + 8 + 1 + 1;

/// The metric tables.
#[derive(Debug, Clone)]
pub struct ClickHouseMetrics {
    client: Client,
}

/// Series a selector selects, resolved exactly.
struct Selected {
    series: HashMap<u64, Labels>,
    names: BTreeSet<String>,
}

impl Selected {
    fn condition(&self) -> String {
        let names: Vec<String> = self.names.iter().map(|n| sql::quote(n)).collect();
        let hashes: Vec<String> = self.series.keys().map(u64::to_string).collect();
        format!(
            "MetricName IN ({}) AND SeriesHash IN ({})",
            names.join(", "),
            hashes.join(", ")
        )
    }
}

fn kind_code(kind: MetricKind) -> u8 {
    match kind {
        MetricKind::Gauge => 0,
        MetricKind::Counter => 1,
        MetricKind::Histogram => 2,
        MetricKind::Summary => 3,
        MetricKind::Unknown => 4,
    }
}

fn kind_of(code: u8) -> MetricKind {
    match code {
        0 => MetricKind::Gauge,
        1 => MetricKind::Counter,
        2 => MetricKind::Histogram,
        3 => MetricKind::Summary,
        _ => MetricKind::Unknown,
    }
}

/// A value as two columns: itself where JSON can spell it, and what it is where not.
fn split_value(value: f64) -> (f64, u8) {
    if value.to_bits() == STALE_MARKER_BITS {
        (0.0, STALE)
    } else if value.is_nan() {
        (0.0, NAN)
    } else if value == f64::INFINITY {
        (0.0, POSITIVE_INFINITY)
    } else if value == f64::NEG_INFINITY {
        (0.0, NEGATIVE_INFINITY)
    } else {
        (value, VALUE)
    }
}

fn joined_value(value: f64, special: u8) -> f64 {
    match special {
        STALE => f64::from_bits(STALE_MARKER_BITS),
        NAN => f64::NAN,
        POSITIVE_INFINITY => f64::INFINITY,
        NEGATIVE_INFINITY => f64::NEG_INFINITY,
        _ => value,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct WriteRow<'a> {
    timestamp: u64,
    metric_name: &'a str,
    service_name: &'a str,
    series: &'a Labels,
    kind: u8,
    value: f64,
    special: u8,
}

/// The days a range touches, as `Day` conditions on the series table.
fn days(start_nanos: u64, end_nanos: u64) -> String {
    format!(
        "Day >= toDate(toDateTime({})) AND Day <= toDate(toDateTime({}))",
        start_nanos / 1_000_000_000,
        end_nanos.min(u64::from(u32::MAX) * 1_000_000_000) / 1_000_000_000
    )
}

fn series_range(start_nanos: u64, end_nanos: u64) -> String {
    format!(
        "{} AND MaxTimestamp >= {} AND MinTimestamp <= {}",
        days(start_nanos, end_nanos),
        sql::nanos(start_nanos),
        sql::nanos(end_nanos)
    )
}

fn windows_condition(windows: &[(u64, u64)]) -> String {
    let span = |(start, end): (u64, u64)| {
        format!(
            "(Timestamp >= {} AND Timestamp <= {})",
            sql::nanos(start),
            sql::nanos(end)
        )
    };
    if windows.len() > MAX_WINDOWS {
        let first = windows.first().map_or(0, |w| w.0);
        let last = windows.last().map_or(0, |w| w.1);
        return span((first, last));
    }
    windows
        .iter()
        .map(|&w| span(w))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn read_u64(row: &[u8], at: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&row[at..at + 8]);
    u64::from_le_bytes(bytes)
}

/// One decoded sample row.
struct SampleRow {
    hash: u64,
    nanos: u64,
    value: f64,
    kind: MetricKind,
}

fn decode(row: &[u8]) -> SampleRow {
    let hash = read_u64(row, 0);
    let nanos = read_u64(row, 8);
    let value = f64::from_bits(read_u64(row, 16));
    SampleRow {
        hash,
        nanos,
        value: joined_value(value, row[24]),
        kind: kind_of(row[25]),
    }
}

const SAMPLE_COLUMNS: &str =
    "SeriesHash, toUInt64(toUnixTimestamp64Nano(Timestamp)), Value, Special, Kind";

impl ClickHouseMetrics {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Write samples. Stored, and readable by every instance, once this returns.
    ///
    /// # Errors
    /// [`Error::Unavailable`] when ClickHouse did not take them; a sender retries the
    /// whole batch.
    pub fn append(&self, samples: &[MetricSample]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let mut body = Vec::with_capacity(samples.len() * 256);
        for sample in samples {
            let (value, special) = split_value(sample.value);
            let row = WriteRow {
                timestamp: sample.timestamp_nanos,
                metric_name: sample.series.get(METRIC_NAME_LABEL).unwrap_or_default(),
                service_name: sample.series.get(APP_LABEL).unwrap_or_default(),
                series: &sample.series,
                kind: kind_code(sample.kind),
                value,
                special,
            };
            serde_json::to_writer(&mut body, &row)
                .map_err(|e| Error::Unavailable(format!("encoding a metric sample: {e}")))?;
            body.push(b'\n');
        }
        self.client.insert(INGEST, &body)?;
        self.client.stats().count_rows(samples.len());
        Ok(())
    }

    /// The series with samples in the range that the matchers select.
    fn select(&self, start: u64, end: u64, matchers: &[LabelMatcher]) -> Result<Selected> {
        let mut conditions = vec![series_range(start, end)];
        conditions.extend(matchers.iter().map(|m| {
            if m.name == METRIC_NAME_LABEL {
                sql::matcher("Series", "MetricName", &rename_app(m))
            } else {
                sql::matcher("Series", "ServiceName", m)
            }
        }));
        let query = format!(
            "SELECT SeriesHash AS hash, any(Series) AS labels, any(MetricName) AS name \
             FROM {SERIES} WHERE {} GROUP BY SeriesHash",
            conditions.join(" AND ")
        );
        let mut selected = Selected {
            series: HashMap::new(),
            names: BTreeSet::new(),
        };
        self.client.query_lines(&query, |line| {
            #[derive(Deserialize)]
            struct SeriesRow {
                hash: u64,
                labels: BTreeMap<String, String>,
                name: String,
            }
            let row: SeriesRow = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!("reading a series from ClickHouse: {e}"))
            })?;
            let labels = labels_of(row.labels);
            if matches_all(matchers, &labels) {
                selected.series.insert(row.hash, labels);
                selected.names.insert(row.name);
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(selected)
    }

    /// Every sample of the selected series in `time`, a series at a time and in time
    /// order within it — the samples table's own order.
    fn read_series(
        &self,
        selected: &Selected,
        time: &str,
        order_by_time: bool,
        mut each: impl FnMut(SampleRow) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        if selected.series.is_empty() {
            return Ok(());
        }
        let order = if order_by_time {
            "Timestamp, MetricName, SeriesHash"
        } else {
            "MetricName, SeriesHash, Timestamp"
        };
        let query = format!(
            "SELECT {SAMPLE_COLUMNS} FROM {SAMPLES} WHERE {} AND ({time}) ORDER BY {order}",
            selected.condition()
        );
        self.client.query_rows(&query, ROW, |row| each(decode(row)))
    }
}

/// A `__name__` matcher, aimed at the `MetricName` column: [`sql::matcher`] reads the
/// column it is given for `app`, so the matcher is renamed to that.
fn rename_app(m: &LabelMatcher) -> LabelMatcher {
    let mut renamed = m.clone();
    APP_LABEL.clone_into(&mut renamed.name);
    renamed
}

impl MetricStore for ClickHouseMetrics {
    fn scan_series(
        &self,
        windows: &[(u64, u64)],
        matchers: &[LabelMatcher],
        visit: &mut dyn FnMut(SeriesRun<'_>) -> ControlFlow<()>,
    ) -> Result<bool> {
        let (Some(&(start, _)), Some(&(_, end))) = (windows.first(), windows.last()) else {
            return Ok(true);
        };
        let selected = self.select(start, end, matchers)?;
        let inside = |at: u64| {
            let i = windows.partition_point(|(_, stop)| *stop < at);
            windows.get(i).is_some_and(|(from, _)| *from <= at)
        };
        let mut current: Option<u64> = None;
        let mut timestamps: Vec<u64> = Vec::new();
        let mut values: Vec<f64> = Vec::new();
        let mut stopped = false;
        let mut flush = |hash: u64, timestamps: &mut Vec<u64>, values: &mut Vec<f64>| {
            let mut outcome = ControlFlow::Continue(());
            if let Some(series) = selected.series.get(&hash)
                && !timestamps.is_empty()
            {
                outcome = visit(SeriesRun {
                    source: 0,
                    series,
                    timestamps,
                    values,
                });
            }
            timestamps.clear();
            values.clear();
            outcome
        };
        self.read_series(&selected, &windows_condition(windows), false, |row| {
            if current != Some(row.hash) {
                if let Some(previous) = current
                    && flush(previous, &mut timestamps, &mut values).is_break()
                {
                    stopped = true;
                    return Ok(ControlFlow::Break(()));
                }
                current = Some(row.hash);
            }
            if inside(row.nanos) {
                timestamps.push(row.nanos);
                values.push(row.value);
            }
            Ok(ControlFlow::Continue(()))
        })?;
        if stopped {
            return Ok(false);
        }
        if let Some(previous) = current
            && flush(previous, &mut timestamps, &mut values).is_break()
        {
            return Ok(false);
        }
        Ok(true)
    }

    fn fold_window(
        &self,
        _start_nanos: u64,
        _end_nanos: u64,
        _matchers: &[LabelMatcher],
        _whole_reads_allowed: usize,
        _gauges: bool,
    ) -> Result<Option<Vec<(Labels, StreamFold)>>> {
        Ok(None)
    }

    fn query_bounded(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&MetricSample) -> bool + Sync),
        ceiling: usize,
    ) -> Result<Vec<MetricSample>> {
        let mut out = Vec::new();
        if start_nanos > end_nanos {
            return Ok(out);
        }
        let selected = self.select(start_nanos, end_nanos, matchers)?;
        let time = format!(
            "Timestamp >= {} AND Timestamp <= {}",
            sql::nanos(start_nanos),
            sql::nanos(end_nanos)
        );
        let mut over = false;
        self.read_series(&selected, &time, true, |row| {
            let Some(series) = selected.series.get(&row.hash) else {
                return Ok(ControlFlow::Continue(()));
            };
            let sample = MetricSample {
                timestamp_nanos: row.nanos,
                series: series.clone(),
                value: row.value,
                kind: row.kind,
            };
            if extra(&sample) {
                out.push(sample);
                if ceiling != 0 && out.len() > ceiling {
                    over = true;
                    return Ok(ControlFlow::Break(()));
                }
            }
            Ok(ControlFlow::Continue(()))
        })?;
        if over {
            return Err(Error::BadRequest(format!(
                "this query matched more than {ceiling} records, which is more than one query \
                 is allowed to hold at once"
            )));
        }
        Ok(out)
    }

    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: &(dyn Fn(&MetricSample) -> bool + Sync),
    ) -> Result<Vec<MetricSample>> {
        let (start, end) = (request.start_nanos, request.end_nanos);
        let mut out = Vec::new();
        if start > end {
            return Ok(out);
        }
        let selected = self.select(start, end, matchers)?;
        if selected.series.is_empty() {
            return Ok(out);
        }
        let direction = match request.order {
            Order::Ascending => "ASC",
            Order::Descending => "DESC",
        };
        let read = |limit: Option<usize>, out: &mut Vec<MetricSample>| -> Result<usize> {
            out.clear();
            let mut rows = 0usize;
            let query = format!(
                "SELECT {SAMPLE_COLUMNS} FROM {SAMPLES} WHERE {} AND Timestamp >= {} \
                 AND Timestamp <= {} ORDER BY Timestamp {direction}{}",
                selected.condition(),
                sql::nanos(start),
                sql::nanos(end),
                limit.map_or(String::new(), |n| format!(" LIMIT {n}")),
            );
            self.client.query_rows(&query, ROW, |row| {
                rows += 1;
                let row = decode(row);
                let Some(series) = selected.series.get(&row.hash) else {
                    return Ok(ControlFlow::Continue(()));
                };
                let sample = MetricSample {
                    timestamp_nanos: row.nanos,
                    series: series.clone(),
                    value: row.value,
                    kind: row.kind,
                };
                if extra(&sample) {
                    out.push(sample);
                    super::scan::over_ceiling(&request, out.len())?;
                    if request.limit != 0 && out.len() >= request.limit {
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            })?;
            Ok(rows)
        };
        let limit = (request.limit != 0).then_some(request.limit);
        let rows = read(limit, &mut out)?;
        // The predicate turned some away and the limit was not reached: read on.
        if limit.is_some_and(|n| rows == n && out.len() < n) {
            read(None, &mut out)?;
        }
        Ok(out)
    }

    fn label_names(&self, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>> {
        let query = format!(
            "SELECT DISTINCT arrayJoin(mapKeys(Series)) AS name FROM {SERIES} \
             WHERE {} ORDER BY name",
            series_range(start_nanos, end_nanos)
        );
        let mut names = Vec::new();
        self.client.query_lines(&query, |line| {
            names.push(super::scan::field(line, "name")?);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(names)
    }

    fn label_values(&self, name: &str, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>> {
        let query = format!(
            "SELECT DISTINCT Series[{key}] AS value FROM {SERIES} \
             WHERE {range} AND mapContains(Series, {key}) ORDER BY value",
            key = sql::quote(name),
            range = series_range(start_nanos, end_nanos)
        );
        let mut values = Vec::new();
        self.client.query_lines(&query, |line| {
            values.push(super::scan::field(line, "value")?);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(values)
    }

    fn streams(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>> {
        let selected = self.select(start_nanos, end_nanos, matchers)?;
        let seen: BTreeSet<Labels> = selected.series.into_values().collect();
        Ok(seen.into_iter().collect())
    }

    fn oldest_sample_nanos(&self) -> Result<Option<u64>> {
        let mut oldest = None;
        self.client.query_lines(
            &format!(
                "SELECT toUInt64(toUnixTimestamp64Nano(min(MinTimestamp))) AS oldest, \
                 count() AS n FROM {SERIES}"
            ),
            |line| {
                let value: serde_json::Value = serde_json::from_slice(line).unwrap_or_default();
                if value
                    .get("n")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
                    > 0
                {
                    oldest = value.get("oldest").and_then(serde_json::Value::as_u64);
                }
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(oldest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_value_survives_the_split() {
        for value in [
            1.5,
            -0.0,
            f64::from_bits(STALE_MARKER_BITS),
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
        ] {
            let (stored, special) = split_value(value);
            let back = joined_value(stored, special);
            if value.is_nan() {
                assert!(back.is_nan());
                assert_eq!(
                    back.to_bits() == STALE_MARKER_BITS,
                    value.to_bits() == STALE_MARKER_BITS
                );
            } else {
                assert_eq!(back.to_bits(), value.to_bits());
            }
        }
    }

    const NANOS_PER_DAY: u64 = 86_400_000_000_000;

    #[test]
    fn days_cover_the_range() {
        assert_eq!(
            days(0, NANOS_PER_DAY),
            "Day >= toDate(toDateTime(0)) AND Day <= toDate(toDateTime(86400))"
        );
    }
}
