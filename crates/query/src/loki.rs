//! The Loki-compatible query API.
//!
//! Response shapes match what Loki emits, because the contract is that
//! `laravel-telemetry-ui` works against telemetryd unchanged (`COMPATIBILITY.md`).
//! That includes the details that look like noise — timestamps as *strings* of
//! nanoseconds, `resultType`, the `stats` object — because a client that parses them
//! strictly will otherwise break.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use telemetryd_core::{Error, Labels, LogRecord, Result};
use telemetryd_store::RecordStore;
use telemetryd_store::logs::LogSchema;

use crate::logql::{self, LogQuery};

/// Default entry limit, matching Loki.
pub const DEFAULT_LIMIT: usize = 100;
/// Ceiling on `limit`, so one query cannot try to materialise the whole store.
pub const MAX_LIMIT: usize = 5_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// Newest first. Loki's default, and what a log viewer wants.
    #[default]
    Backward,
    Forward,
}

/// A parsed and validated `query_range` request.
#[derive(Debug, Clone)]
pub struct QueryRangeRequest {
    pub query: LogQuery,
    pub start_nanos: u64,
    pub end_nanos: u64,
    pub limit: usize,
    pub direction: Direction,
}

/// Raw query-string parameters, before validation.
#[derive(Debug, Default, Deserialize)]
pub struct QueryRangeParams {
    pub query: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<String>,
    pub direction: Option<String>,
    pub since: Option<String>,
    /// Accepted and ignored — they only affect metric queries, which are out of the
    /// subset. Rejecting them would break clients that always send them.
    pub step: Option<String>,
    pub interval: Option<String>,
}

/// Parameters shared by `labels`, `label/{name}/values` and `series`.
#[derive(Debug, Default, Deserialize)]
pub struct RangeParams {
    pub start: Option<String>,
    pub end: Option<String>,
    pub since: Option<String>,
    /// `series` takes one or more selectors under the repeated `match[]` parameter.
    #[serde(rename = "match[]", default)]
    pub matches: Vec<String>,
    pub query: Option<String>,
}

impl QueryRangeRequest {
    /// Validate raw parameters into a runnable request.
    pub fn from_params(params: &QueryRangeParams, now_nanos: u64) -> Result<Self> {
        let raw = params
            .query
            .as_deref()
            .filter(|q| !q.trim().is_empty())
            .ok_or_else(|| Error::BadRequest("the `query` parameter is required".to_owned()))?;

        let query = logql::parse(raw)?;
        let (start_nanos, end_nanos) = resolve_range(
            params.start.as_deref(),
            params.end.as_deref(),
            params.since.as_deref(),
            now_nanos,
        )?;

        let limit = match params.limit.as_deref() {
            None | Some("") => DEFAULT_LIMIT,
            Some(raw) => raw
                .trim()
                .parse::<usize>()
                .map_err(|_| Error::BadRequest(format!("`limit` must be a number, got {raw:?}")))?,
        };
        if limit == 0 {
            return Err(Error::BadRequest(
                "`limit` must be greater than zero".to_owned(),
            ));
        }
        let limit = limit.min(MAX_LIMIT);

        let direction = match params.direction.as_deref() {
            None | Some("" | "backward") => Direction::Backward,
            Some("forward") => Direction::Forward,
            Some(other) => {
                return Err(Error::BadRequest(format!(
                    "`direction` must be `forward` or `backward`, got {other:?}"
                )));
            }
        };

        Ok(Self {
            query,
            start_nanos,
            end_nanos,
            limit,
            direction,
        })
    }
}

/// Resolve a start/end pair, honouring `since` and Loki's defaults.
pub fn resolve_range(
    start: Option<&str>,
    end: Option<&str>,
    since: Option<&str>,
    now_nanos: u64,
) -> Result<(u64, u64)> {
    let end_nanos = if let Some(raw) = end.filter(|s| !s.is_empty()) {
        parse_time(raw)?
    } else {
        now_nanos
    };

    let start_nanos = if let Some(raw) = start.filter(|s| !s.is_empty()) {
        parse_time(raw)?
    } else {
        let window = if let Some(raw) = since.filter(|s| !s.is_empty()) {
            parse_duration_nanos(raw)?
        } else {
            // Loki's default lookback.
            6 * 3_600_000_000_000
        };
        end_nanos.saturating_sub(window)
    };

    if start_nanos > end_nanos {
        return Err(Error::BadRequest(
            "`start` must not be after `end`".to_owned(),
        ));
    }
    Ok((start_nanos, end_nanos))
}

/// Parse a Loki timestamp.
///
/// Loki accepts three forms and clients use all of them: nanosecond epoch as a
/// (possibly very long) integer, RFC3339, and a float of unix seconds. Supporting only
/// the first would break Grafana-style clients; supporting only RFC3339 would break
/// the UI's own paging, which round-trips the nanosecond values we return.
pub fn parse_time(raw: &str) -> Result<u64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(Error::BadRequest("empty timestamp".to_owned()));
    }

    // Integer: unix nanoseconds, or a shorter unit we can identify by magnitude.
    if raw.bytes().all(|b| b.is_ascii_digit()) {
        let value: u64 = raw
            .parse()
            .map_err(|_| Error::BadRequest(format!("timestamp {raw:?} is out of range")))?;
        return Ok(scale_to_nanos(value));
    }

    // Float seconds, e.g. 1750000000.123.
    if let Ok(seconds) = raw.parse::<f64>()
        && seconds >= 0.0
        && seconds.is_finite()
    {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        return Ok((seconds * 1e9) as u64);
    }

    parse_rfc3339(raw)
}

/// Interpret a bare integer timestamp by magnitude.
///
/// Same reasoning as the ingest path: unix seconds, millis, micros and nanos do not
/// overlap for any date between 2001 and 2100, so a client sending seconds gets the
/// range it meant rather than an empty result set from 1970.
fn scale_to_nanos(value: u64) -> u64 {
    const MIN_SECONDS: u64 = 978_307_200;
    const MAX_SECONDS: u64 = 4_102_444_800;

    if value >= MIN_SECONDS * 1_000_000_000 {
        value
    } else if value >= MIN_SECONDS * 1_000_000 {
        value * 1_000
    } else if value >= MIN_SECONDS * 1_000 {
        value * 1_000_000
    } else if (MIN_SECONDS..MAX_SECONDS).contains(&value) {
        value * 1_000_000_000
    } else {
        value
    }
}

/// Minimal RFC3339 parsing — no chrono dependency for one date format.
fn parse_rfc3339(raw: &str) -> Result<u64> {
    let invalid = || Error::BadRequest(format!("{raw:?} is not a valid RFC3339 timestamp"));

    let (date, rest) = raw.split_once('T').ok_or_else(invalid)?;
    let mut parts = date.split('-');
    let year: i64 = parts
        .next()
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let month: i64 = parts
        .next()
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let day: i64 = parts
        .next()
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(invalid());
    }

    // Strip the zone; we only accept UTC, which is all Loki clients send.
    let time = rest
        .trim_end_matches('Z')
        .split(['+'])
        .next()
        .ok_or_else(invalid)?;
    let time = match time.rfind('-') {
        Some(index) if index > 0 => &time[..index],
        _ => time,
    };

    let (hms, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut hms_parts = hms.split(':');
    let hour: i64 = hms_parts
        .next()
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let minute: i64 = hms_parts
        .next()
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let second: i64 = hms_parts
        .next()
        .unwrap_or("0")
        .parse()
        .map_err(|_| invalid())?;

    let mut nanos_fraction = 0u64;
    if !fraction.is_empty() {
        let digits: String = fraction.chars().take_while(char::is_ascii_digit).collect();
        let padded = format!("{digits:0<9}");
        nanos_fraction = padded[..9].parse().map_err(|_| invalid())?;
    }

    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if seconds < 0 {
        return Err(Error::BadRequest(format!(
            "{raw:?} is before the unix epoch, which telemetryd does not store"
        )));
    }
    Ok(u64::try_from(seconds).unwrap_or(0) * 1_000_000_000 + nanos_fraction)
}

/// Days since 1970-01-01 from a civil date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn parse_duration_nanos(raw: &str) -> Result<u64> {
    match crate::lexer::tokenize(raw)?.first().map(|s| &s.token) {
        Some(crate::lexer::Token::Duration(nanos)) => Ok(*nanos),
        _ => Err(Error::BadRequest(format!(
            "{raw:?} is not a duration (try `1h`, `30m`, `5s`)"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct LokiResponse<T> {
    pub status: &'static str,
    pub data: T,
}

impl<T> LokiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            status: "success",
            data,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StreamsData {
    #[serde(rename = "resultType")]
    pub result_type: &'static str,
    pub result: Vec<StreamResult>,
    pub stats: Stats,
}

#[derive(Debug, Serialize)]
pub struct StreamResult {
    pub stream: BTreeMap<String, String>,
    pub values: Vec<Entry>,
}

/// One log entry on the wire.
///
/// Loki entries are `[timestamp, line]` or `[timestamp, line, structuredMetadata]`.
/// The third element is where per-record attributes belong: promoting them to stream
/// labels would explode the index, and dropping them would make `order.id` and
/// `trace_id` invisible in a client that reads them from there — which
/// `laravel-telemetry-ui` does. The timestamp is a string because a JSON number loses
/// nanosecond precision in JavaScript.
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Entry {
    Plain([String; 2]),
    WithMetadata(String, String, BTreeMap<String, String>),
}

impl Entry {
    pub fn new(timestamp_nanos: u64, line: String, metadata: BTreeMap<String, String>) -> Self {
        if metadata.is_empty() {
            // Match Loki, which omits the element entirely rather than sending {}.
            Self::Plain([timestamp_nanos.to_string(), line])
        } else {
            Self::WithMetadata(timestamp_nanos.to_string(), line, metadata)
        }
    }

    pub fn timestamp(&self) -> &str {
        match self {
            Self::Plain([ts, _]) | Self::WithMetadata(ts, _, _) => ts,
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct Stats {
    pub summary: StatsSummary,
}

#[derive(Debug, Default, Serialize)]
pub struct StatsSummary {
    #[serde(rename = "totalLinesProcessed")]
    pub total_lines_processed: u64,
    #[serde(rename = "totalEntriesReturned")]
    pub total_entries_returned: u64,
    #[serde(rename = "execTime")]
    pub exec_time: f64,
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Run a `query_range` against the log store.
pub fn query_range(
    store: &RecordStore<LogSchema>,
    request: &QueryRangeRequest,
) -> Result<LokiResponse<StreamsData>> {
    let started = std::time::Instant::now();
    let scanned = std::sync::atomic::AtomicU64::new(0);

    let filter = |record: &LogRecord| {
        scanned.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Label filters see stream labels *and* record attributes; see
        // `LogQuery::evaluate`.
        let mut base = record.stream.clone();
        for (name, value) in record.attributes.iter() {
            base.insert(name, value);
        }
        request.query.evaluate(&record.body, &base)
    };

    let mut records = store.query(
        request.start_nanos,
        request.end_nanos,
        &request.query.matchers,
        &filter,
    )?;

    // Sort before limiting: a `backward` query must return the newest N entries in the
    // range, not the first N the storage layer happened to hand back.
    match request.direction {
        Direction::Backward => {
            records.sort_by_key(|r| std::cmp::Reverse(r.timestamp_nanos));
        }
        Direction::Forward => records.sort_by_key(|r| r.timestamp_nanos),
    }
    records.truncate(request.limit);

    let returned = records.len() as u64;
    let result = group_into_streams(records, request.direction);

    Ok(LokiResponse::success(StreamsData {
        result_type: "streams",
        result,
        stats: Stats {
            summary: StatsSummary {
                total_lines_processed: scanned.load(std::sync::atomic::Ordering::Relaxed),
                total_entries_returned: returned,
                exec_time: started.elapsed().as_secs_f64(),
            },
        },
    }))
}

/// Group records by their stream label set, preserving order within each stream.
fn group_into_streams(records: Vec<LogRecord>, direction: Direction) -> Vec<StreamResult> {
    let mut grouped: BTreeMap<Labels, Vec<Entry>> = BTreeMap::new();
    for record in records {
        let metadata: BTreeMap<String, String> = record
            .attributes
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        grouped
            .entry(record.stream.clone())
            .or_default()
            .push(Entry::new(record.timestamp_nanos, record.body, metadata));
    }

    grouped
        .into_iter()
        .map(|(stream, mut values)| {
            // Entries within a stream follow the requested direction, which is what
            // clients rely on for paging.
            match direction {
                Direction::Backward => {
                    values.sort_by(|a, b| b.timestamp().cmp(a.timestamp()));
                }
                Direction::Forward => values.sort_by(|a, b| a.timestamp().cmp(b.timestamp())),
            }
            StreamResult {
                stream: stream
                    .iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect(),
                values,
            }
        })
        .collect()
}

/// `/loki/api/v1/labels`
pub fn label_names(
    store: &RecordStore<LogSchema>,
    start_nanos: u64,
    end_nanos: u64,
) -> LokiResponse<Vec<String>> {
    LokiResponse::success(store.label_names(start_nanos, end_nanos))
}

/// `/loki/api/v1/label/{name}/values`
pub fn label_values(
    store: &RecordStore<LogSchema>,
    name: &str,
    start_nanos: u64,
    end_nanos: u64,
) -> Result<LokiResponse<Vec<String>>> {
    Ok(LokiResponse::success(store.label_values(
        name,
        start_nanos,
        end_nanos,
    )?))
}

/// `/loki/api/v1/series`
pub fn series(
    store: &RecordStore<LogSchema>,
    selectors: &[String],
    start_nanos: u64,
    end_nanos: u64,
) -> Result<LokiResponse<Vec<BTreeMap<String, String>>>> {
    let mut seen: std::collections::BTreeSet<Labels> = std::collections::BTreeSet::new();

    if selectors.is_empty() {
        seen.extend(store.streams(start_nanos, end_nanos, &[])?);
    } else {
        for selector in selectors {
            let query = logql::parse(selector)?;
            seen.extend(store.streams(start_nanos, end_nanos, &query.matchers)?);
        }
    }

    Ok(LokiResponse::success(
        seen.into_iter()
            .map(|labels| {
                labels
                    .iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect()
            })
            .collect(),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const NOW: u64 = 1_750_000_000_000_000_000;

    #[test]
    fn nanosecond_epoch_timestamps_parse() {
        assert_eq!(parse_time("1750000000000000000").unwrap(), NOW);
    }

    #[test]
    fn shorter_units_are_identified_by_magnitude() {
        // A client sending seconds should get the range it meant, not 1970.
        assert_eq!(parse_time("1750000000").unwrap(), NOW);
        assert_eq!(parse_time("1750000000000").unwrap(), NOW);
        assert_eq!(parse_time("1750000000000000").unwrap(), NOW);
    }

    #[test]
    fn float_seconds_parse() {
        assert_eq!(parse_time("1750000000.5").unwrap(), NOW + 500_000_000);
    }

    #[test]
    fn rfc3339_parses_with_and_without_fractions() {
        // 1750000000 epoch seconds is 2025-06-15T15:06:40Z.
        assert_eq!(parse_time("2025-06-15T15:06:40Z").unwrap(), NOW);
        assert_eq!(
            parse_time("2025-06-15T15:06:40.5Z").unwrap(),
            NOW + 500_000_000
        );
        assert_eq!(
            parse_time("2025-06-15T15:06:40.123456789Z").unwrap(),
            NOW + 123_456_789
        );
        // Round-trips against the value the API itself hands back.
        assert_eq!(parse_time("1750000000000000000").unwrap(), NOW);
    }

    #[test]
    fn a_malformed_timestamp_is_a_clean_client_error() {
        for raw in ["", "not-a-time", "2025-13-45T99:99:99Z"] {
            let err = parse_time(raw).unwrap_err();
            assert!(matches!(err, Error::BadRequest(_)), "{raw}");
        }
    }

    #[test]
    fn the_range_defaults_to_the_last_six_hours() {
        let (start, end) = resolve_range(None, None, None, NOW).unwrap();
        assert_eq!(end, NOW);
        assert_eq!(start, NOW - 6 * 3_600_000_000_000);
    }

    #[test]
    fn since_sets_the_lookback_window() {
        let (start, end) = resolve_range(None, None, Some("30m"), NOW).unwrap();
        assert_eq!(end, NOW);
        assert_eq!(start, NOW - 1_800_000_000_000);
    }

    #[test]
    fn an_inverted_range_is_refused() {
        let err = resolve_range(Some("1750000000"), Some("1740000000"), None, NOW).unwrap_err();
        assert!(err.to_string().contains("must not be after"), "{err}");
    }

    #[test]
    fn query_is_required() {
        let err = QueryRangeRequest::from_params(&QueryRangeParams::default(), NOW).unwrap_err();
        assert!(
            err.to_string().contains("`query` parameter is required"),
            "{err}"
        );
    }

    #[test]
    fn limit_defaults_clamps_and_validates() {
        let base = |limit: Option<&str>| QueryRangeParams {
            query: Some(r#"{app="x"}"#.to_owned()),
            limit: limit.map(str::to_owned),
            ..QueryRangeParams::default()
        };

        assert_eq!(
            QueryRangeRequest::from_params(&base(None), NOW)
                .unwrap()
                .limit,
            DEFAULT_LIMIT
        );
        assert_eq!(
            QueryRangeRequest::from_params(&base(Some("50")), NOW)
                .unwrap()
                .limit,
            50
        );
        // Clamped rather than refused, so a client asking for a million still works.
        assert_eq!(
            QueryRangeRequest::from_params(&base(Some("999999")), NOW)
                .unwrap()
                .limit,
            MAX_LIMIT
        );
        assert!(QueryRangeRequest::from_params(&base(Some("0")), NOW).is_err());
        assert!(QueryRangeRequest::from_params(&base(Some("abc")), NOW).is_err());
    }

    #[test]
    fn direction_defaults_to_backward() {
        let params = QueryRangeParams {
            query: Some(r#"{app="x"}"#.to_owned()),
            ..QueryRangeParams::default()
        };
        let request = QueryRangeRequest::from_params(&params, NOW).unwrap();
        assert_eq!(request.direction, Direction::Backward);
    }

    #[test]
    fn an_unknown_direction_is_refused_by_name() {
        let params = QueryRangeParams {
            query: Some(r#"{app="x"}"#.to_owned()),
            direction: Some("sideways".to_owned()),
            ..QueryRangeParams::default()
        };
        let err = QueryRangeRequest::from_params(&params, NOW).unwrap_err();
        assert!(err.to_string().contains("sideways"), "{err}");
    }

    #[test]
    fn step_and_interval_are_accepted_and_ignored() {
        // Clients send these unconditionally; refusing them would break the UI for no
        // benefit, since they only affect metric queries.
        let params = QueryRangeParams {
            query: Some(r#"{app="x"}"#.to_owned()),
            step: Some("60".to_owned()),
            interval: Some("10".to_owned()),
            ..QueryRangeParams::default()
        };
        assert!(QueryRangeRequest::from_params(&params, NOW).is_ok());
    }

    #[test]
    fn days_from_civil_matches_known_epochs() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 3, 1), 11017);
        assert_eq!(days_from_civil(2025, 6, 15), 20254);
    }
}
