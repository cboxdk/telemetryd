//! Reading a signal's table the way the local store reads its segments: by stream, by
//! time range, newest or oldest first, within a limit.
//!
//! Shared by logs and traces, which differ in their columns and their records but not
//! in how a query reaches them:
//!
//! 1. **Streams first.** The query's stream matchers are resolved against the signal's
//!    streams summary — a few rows per stream per hour — by the matcher code every store
//!    uses, so the table is read with `StreamHash IN (…)` and `ServiceName IN (…)`
//!    rather than conditions on a map: a `UInt64` column instead of a map column, and an
//!    app list the sort key prunes on even when the query named no app.
//! 2. **Windows from the end.** A limited scan reads windows from the end it starts at,
//!    the first sized from how densely the selected streams wrote there, each four times
//!    the last. The table is sorted by time within an app, so a window costs what it
//!    holds: the newest hundred rows of a day read seconds of it.
//! 3. **Keyset pages within a window**, so a window holding more than a page is read in
//!    pages that neither skip nor repeat rows sharing a timestamp.
//!
//! The record predicate always runs on what comes back, so every condition sent to
//! ClickHouse may select too much and never too little.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::ControlFlow;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use telemetryd_core::{Error, LabelMatcher, Labels, Result, matches_all};

use super::Client;
use super::sql;
use crate::records::Scan;
use crate::topk::Order;

/// More streams than this and listing their hashes stops paying: the matchers go into
/// the query as conditions on the stream map instead.
const MAX_LISTED_STREAMS: usize = 10_000;

/// Candidate rows a limited scan's first window is sized to hold, per row it needs.
const WINDOW_ROWS_PER_RESULT: u64 = 4;

/// Rows asked for per round trip when a scan still needs `wanted`.
///
/// The record predicate runs here, not in ClickHouse, so a page can come back with rows
/// it rejects, and then another is read. Asking for more than is still needed would be
/// cheaper in round trips and dearer in the case that matters: ClickHouse stops reading
/// once it has a page, and a filter that matches a few hundred lines a day made it read
/// the whole day looking for a page twice the limit.
fn page_size(wanted: usize) -> usize {
    wanted.clamp(100, 10_000)
}

/// A row with a timestamp, as every signal's rows have: `t`, in nanoseconds.
pub(super) trait Timed: DeserializeOwned {
    fn nanos(&self) -> u64;
}

/// The streams a query selects, resolved exactly.
pub(super) struct Selected {
    pub(super) streams: HashMap<u64, Labels>,
    pub(super) services: BTreeSet<String>,
}

impl Selected {
    pub(super) fn condition(&self) -> String {
        if self.streams.is_empty() {
            return "0".to_owned();
        }
        let services: Vec<String> = self.services.iter().map(|s| sql::quote(s)).collect();
        let hashes: Vec<String> = self.streams.keys().map(u64::to_string).collect();
        // One app as `=`, not `IN`: with the sort key's first column fixed, ClickHouse
        // reads a time-ordered query in order and stops at its limit, rather than sorting
        // every row of the window.
        let service = match services.as_slice() {
            [one] => format!("ServiceName = {one}"),
            many => format!("ServiceName IN ({})", many.join(", ")),
        };
        format!("{service} AND StreamHash IN ({})", hashes.join(", "))
    }
}

/// A signal's table and the summary its streams resolve against.
pub(super) struct Table<'a> {
    pub(super) client: &'a Client,
    pub(super) name: &'static str,
    pub(super) streams: &'static str,
    /// What a row is read as, beginning `toUnixTimestamp64Nano(Timestamp) AS t`.
    pub(super) columns: &'static str,
}

impl Table<'_> {
    /// The streams with data in the range that the matchers select, or `None` for more
    /// than [`MAX_LISTED_STREAMS`] of them.
    pub(super) fn select(
        &self,
        start: u64,
        end: u64,
        matchers: &[LabelMatcher],
    ) -> Result<Option<Selected>> {
        let mut conditions = vec![stream_range(start, end)];
        conditions.extend(
            matchers
                .iter()
                .map(|m| sql::matcher("Stream", "ServiceName", m)),
        );
        let query = format!(
            "SELECT StreamHash AS hash, any(Stream) AS labels, any(ServiceName) AS service \
             FROM {} WHERE {} GROUP BY StreamHash LIMIT {}",
            self.streams,
            conditions.join(" AND "),
            MAX_LISTED_STREAMS + 1
        );
        let mut selected = Selected {
            streams: HashMap::new(),
            services: BTreeSet::new(),
        };
        let mut listed = 0usize;
        self.client.query_lines(&query, |line| {
            #[derive(Deserialize)]
            struct StreamRow {
                hash: u64,
                labels: BTreeMap<String, String>,
                service: String,
            }
            let row: StreamRow = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!("reading a stream from ClickHouse: {e}"))
            })?;
            listed += 1;
            let labels = labels_of(row.labels);
            if !labels.is_empty() && matches_all(matchers, &labels) {
                selected.streams.insert(row.hash, labels);
                selected.services.insert(row.service);
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok((listed <= MAX_LISTED_STREAMS).then_some(selected))
    }

    /// Rows the selected streams hold per hour of the range, as `(hour, rows)`.
    pub(super) fn hourly_rows(
        &self,
        start: u64,
        end: u64,
        selected: &Selected,
    ) -> Result<Vec<(u64, u64)>> {
        let query = format!(
            "SELECT toUInt64(toUnixTimestamp(Hour)) AS hour, sum(Rows) AS rows \
             FROM {} WHERE {} AND {} GROUP BY hour ORDER BY hour",
            self.streams,
            stream_range(start, end),
            selected.condition()
        );
        let mut hours = Vec::new();
        self.client.query_lines(&query, |line| {
            let value: serde_json::Value = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!("reading hourly rows from ClickHouse: {e}"))
            })?;
            let number = |name: &str| {
                value
                    .get(name)
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0)
            };
            hours.push((number("hour").saturating_mul(1_000_000_000), number("rows")));
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(hours)
    }

    pub(super) fn read<R: DeserializeOwned>(
        &self,
        conditions: &str,
        order: Order,
        limit: Option<usize>,
        mut each: impl FnMut(R) -> Result<ControlFlow<()>>,
    ) -> Result<()> {
        let direction = match order {
            Order::Descending => "DESC",
            Order::Ascending => "ASC",
        };
        // In the sort key's own terms — `(ServiceName, toDateTime(Timestamp))` — so that
        // an app fixed by the conditions lets ClickHouse read in order; the order is the
        // timestamp's either way.
        let mut query = format!(
            "SELECT {} FROM {} WHERE {conditions} \
             ORDER BY toDateTime(Timestamp) {direction}, Timestamp {direction}",
            self.columns, self.name
        );
        if let Some(limit) = limit {
            query = format!("{query} LIMIT {limit}");
        }
        self.client.query_lines(&query, |line| {
            let row: R = serde_json::from_slice(line).map_err(|e| {
                Error::Unavailable(format!(
                    "ClickHouse returned a {} row this cannot read: {e}",
                    self.name
                ))
            })?;
            each(row)
        })
    }

    fn rows<R: DeserializeOwned>(
        &self,
        conditions: &str,
        order: Order,
        limit: Option<usize>,
    ) -> Result<Vec<R>> {
        let mut rows = Vec::new();
        self.read(conditions, order, limit, |row| {
            rows.push(row);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(rows)
    }

    /// The scan's records: those in its range matching `base` — its stream and other
    /// conditions, everything but the time — that `accepts` takes, in its order and
    /// within its limit and ceiling.
    pub(super) fn scan<R: Timed, T>(
        &self,
        request: &Scan<'_>,
        base: &str,
        selected: Option<&Selected>,
        convert: &dyn Fn(R) -> T,
        accepts: &dyn Fn(&T) -> bool,
    ) -> Result<Vec<T>> {
        let (start, end) = (request.start_nanos, request.end_nanos);
        let mut out = Vec::new();
        if start > end || selected.is_some_and(|s| s.streams.is_empty()) {
            return Ok(out);
        }

        if request.limit == 0 {
            let mut failed = None;
            let conditions = format!("{} AND {base}", time_condition((start, true), (end, true)));
            self.read(&conditions, request.order, None, |row: R| {
                let record = convert(row);
                if accepts(&record) {
                    out.push(record);
                    if let Err(error) = over_ceiling(request, out.len()) {
                        failed = Some(error);
                        return Ok(ControlFlow::Break(()));
                    }
                }
                Ok(ControlFlow::Continue(()))
            })?;
            return failed.map_or(Ok(out), Err);
        }

        let hours = match selected {
            Some(selected) if selected.services.len() > 1 => {
                self.hourly_rows(start, end, selected)?
            }
            _ => Vec::new(),
        };
        let need = (request.limit as u64)
            .saturating_mul(WINDOW_ROWS_PER_RESULT)
            .max(1_000);
        // With one app the read is in the sort key's order, and ClickHouse stops once it
        // has the page: one window, the whole range, costs what the answer costs. With
        // several it sorts what a window holds, so windows keep that small.
        let one_app = selected.is_some_and(|s| s.services.len() == 1);
        let mut width = if one_app {
            end - start
        } else {
            first_window(&hours, request.order, need, end - start)
        };
        let mut edge = match request.order {
            Order::Descending => end,
            Order::Ascending => start,
        };
        loop {
            let (window, last) = match request.order {
                Order::Descending => {
                    let low = edge.saturating_sub(width);
                    if low <= start {
                        (time_condition((start, true), (edge, true)), true)
                    } else {
                        (time_condition((low, false), (edge, true)), false)
                    }
                }
                Order::Ascending => {
                    let high = edge.saturating_add(width);
                    if high >= end {
                        (time_condition((edge, true), (end, true)), true)
                    } else {
                        (time_condition((edge, true), (high, false)), false)
                    }
                }
            };
            if self.read_window(
                &format!("{window} AND {base}"),
                request,
                convert,
                accepts,
                &mut out,
            )? || last
            {
                return Ok(out);
            }
            edge = match request.order {
                Order::Descending => edge.saturating_sub(width),
                Order::Ascending => edge.saturating_add(width),
            };
            width = width.saturating_mul(4);
        }
    }

    /// Accept rows from one window into `out` until it holds the scan's limit; `true`
    /// once it does.
    ///
    /// Keyset pagination on the timestamp. A page that ends partway through one timestamp
    /// leaves the rest of it for the next page, which starts at that timestamp again:
    /// dropping the last timestamp's rows from this page and asking from it inclusively
    /// means rows that share a timestamp are neither skipped nor read twice. A page that
    /// is one timestamp throughout reads that timestamp whole.
    fn read_window<R: Timed, T>(
        &self,
        window: &str,
        request: &Scan<'_>,
        convert: &dyn Fn(R) -> T,
        accepts: &dyn Fn(&T) -> bool,
        out: &mut Vec<T>,
    ) -> Result<bool> {
        let mut bound: Option<String> = None;
        loop {
            let page = page_size(request.limit.saturating_sub(out.len()));
            let conditions = match &bound {
                Some(bound) => format!("{window} AND {bound}"),
                None => window.to_owned(),
            };
            let mut rows: Vec<R> = self.rows(&conditions, request.order, Some(page))?;
            let full = rows.len() == page;
            let mut next = None;
            if full {
                let last = rows.last().map_or(0, Timed::nanos);
                let first = rows.first().map_or(0, Timed::nanos);
                if first == last {
                    rows = self.rows(
                        &format!("{window} AND Timestamp = {}", sql::nanos(last)),
                        request.order,
                        None,
                    )?;
                    next = match request.order {
                        Order::Descending => last.checked_sub(1).map(|t| ("<=", t)),
                        Order::Ascending => last.checked_add(1).map(|t| (">=", t)),
                    };
                } else {
                    rows.retain(|r| r.nanos() != last);
                    next = Some((
                        match request.order {
                            Order::Descending => "<=",
                            Order::Ascending => ">=",
                        },
                        last,
                    ));
                }
            }
            for row in rows {
                let record = convert(row);
                if accepts(&record) {
                    out.push(record);
                    over_ceiling(request, out.len())?;
                    if out.len() >= request.limit {
                        return Ok(true);
                    }
                }
            }
            match next {
                Some((op, at)) if full => {
                    bound = Some(format!("Timestamp {op} {}", sql::nanos(at)));
                }
                _ => return Ok(false),
            }
        }
    }
}

/// How the stream matchers reach the table: as hashes, or as map conditions.
pub(super) fn stream_condition(selected: Option<&Selected>, matchers: &[LabelMatcher]) -> String {
    match selected {
        Some(selected) => selected.condition(),
        None if matchers.is_empty() => "1".to_owned(),
        None => matchers
            .iter()
            .map(|m| sql::matcher("Stream", "ServiceName", m))
            .collect::<Vec<_>>()
            .join(" AND "),
    }
}

/// `Timestamp` between two bounds, each inclusive or not.
pub(super) fn time_condition(
    (low, low_inclusive): (u64, bool),
    (high, high_inclusive): (u64, bool),
) -> String {
    format!(
        "Timestamp {} {} AND Timestamp {} {}",
        if low_inclusive { ">=" } else { ">" },
        sql::nanos(low),
        if high_inclusive { "<=" } else { "<" },
        sql::nanos(high),
    )
}

/// The summary's rows that can hold data in the range. Each row records the first and
/// last timestamp it covers, so this is exact to the stream, not the hour.
pub(super) fn stream_range(start_nanos: u64, end_nanos: u64) -> String {
    format!(
        "Hour >= toStartOfHour({start}) AND Hour <= {end} \
         AND MaxTimestamp >= {start} AND MinTimestamp <= {end}",
        start = sql::nanos(start_nanos),
        end = sql::nanos(end_nanos),
    )
}

/// The span a limited scan's first window covers, from how densely the selected streams
/// wrote at the end it starts from: wide enough to hold the candidates `need` rows ask
/// for, if the rate held, and never narrower than a second or wider than the range.
pub(super) fn first_window(hours: &[(u64, u64)], order: Order, need: u64, span: u64) -> u64 {
    const HOUR: u64 = 3_600_000_000_000;
    let at_the_end = match order {
        Order::Descending => hours.iter().rev().find(|(_, rows)| *rows > 0),
        Order::Ascending => hours.iter().find(|(_, rows)| *rows > 0),
    };
    let window = at_the_end.map_or(60_000_000_000, |(_, rows)| {
        u64::try_from(u128::from(need) * u128::from(HOUR) / u128::from(*rows)).unwrap_or(u64::MAX)
    });
    // Not `clamp`: a range shorter than a second would make its bounds cross.
    window.max(1_000_000_000).min(span.max(1))
}

pub(super) fn over_ceiling(request: &Scan<'_>, collected: usize) -> Result<()> {
    if request.abort_over != 0 && collected > request.abort_over {
        return Err(Error::BadRequest(format!(
            "this query matched more than {} records, which is more than one query is \
             allowed to hold at once",
            request.abort_over
        )));
    }
    Ok(())
}

pub(super) fn labels_of(map: BTreeMap<String, String>) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in map {
        labels.insert(name, value);
    }
    labels
}

pub(super) fn field(line: &[u8], name: &str) -> Result<String> {
    let value: serde_json::Value = serde_json::from_slice(line)
        .map_err(|e| Error::Unavailable(format!("reading {name} from ClickHouse: {e}")))?;
    Ok(value
        .get(name)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3_600_000_000_000;

    #[test]
    fn the_first_window_holds_what_the_last_hour_wrote_at_its_rate() {
        // 3.6 million rows an hour is a thousand a second: 4,000 rows is four seconds.
        let hours = [(0, 7), (HOUR, 3_600_000)];
        assert_eq!(
            first_window(&hours, Order::Descending, 4_000, 2 * HOUR),
            4_000_000_000
        );
        // From the other end the first hour is the one that counts.
        assert_eq!(first_window(&hours, Order::Ascending, 7, 2 * HOUR), HOUR);
    }

    #[test]
    fn the_first_window_never_exceeds_the_range_even_under_a_second() {
        assert_eq!(first_window(&[], Order::Descending, 100, 0), 1);
        assert_eq!(first_window(&[(0, 1)], Order::Descending, 100, 500), 500);
    }
}
