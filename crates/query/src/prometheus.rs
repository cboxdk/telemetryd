//! The Prometheus-compatible query API.
//!
//! Shapes and units follow `PrometheusSource`: `start`/`end`/`time` are
//! **seconds**, values are `[unix_seconds_as_number, "value_as_string"]`, and
//! `/api/v1/status/buildinfo` exists because the UI probes it first and shows a
//! degraded backend without it.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use telemetryd_core::{Error, LabelMatcher, Labels, Result};
use telemetryd_store::RecordStore;
use telemetryd_store::metrics::MetricSchema;

use crate::promeval::{Snapshot, Value};
use crate::promql::{self, Expr};

/// Ceiling on points per series in a range query, so one request cannot ask for a
/// million steps. Prometheus uses the same idea.
pub const MAX_STEPS: usize = 11_000;

#[derive(Debug, Default, Deserialize)]
pub struct InstantParams {
    pub query: Option<String>,
    pub time: Option<String>,
    pub timeout: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct RangeParams {
    pub query: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub step: Option<String>,
    pub timeout: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct MetaParams {
    pub start: Option<String>,
    pub end: Option<String>,
    #[serde(rename = "match[]")]
    pub matches: Option<String>,
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct PromResponse<T> {
    pub status: &'static str,
    pub data: T,
}

impl<T> PromResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            status: "success",
            data,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct InstantData {
    #[serde(rename = "resultType")]
    pub result_type: &'static str,
    pub result: Vec<InstantResult>,
}

#[derive(Debug, Serialize)]
pub struct InstantResult {
    pub metric: BTreeMap<String, String>,
    /// `[seconds, "value"]` — the value is a string, as Prometheus sends it.
    pub value: (f64, String),
}

#[derive(Debug, Serialize)]
pub struct RangeData {
    #[serde(rename = "resultType")]
    pub result_type: &'static str,
    pub result: Vec<RangeResult>,
}

#[derive(Debug, Serialize)]
pub struct RangeResult {
    pub metric: BTreeMap<String, String>,
    pub values: Vec<(f64, String)>,
}

#[derive(Debug, Serialize)]
pub struct BuildInfo {
    pub version: &'static str,
    pub revision: &'static str,
    pub branch: &'static str,
    #[serde(rename = "buildUser")]
    pub build_user: &'static str,
    #[serde(rename = "buildDate")]
    pub build_date: &'static str,
    #[serde(rename = "goVersion")]
    pub go_version: &'static str,
}

/// `GET /api/v1/status/buildinfo`
///
/// The UI's primary probe. Answering it truthfully — telemetryd's own version, and a
/// `goVersion` that says what this actually is — beats either omitting the endpoint
/// (every connection check shows degraded) or impersonating a Prometheus build.
pub fn build_info() -> PromResponse<BuildInfo> {
    PromResponse::success(BuildInfo {
        version: telemetryd_core::VERSION,
        revision: "unknown",
        branch: "main",
        build_user: "telemetryd",
        build_date: "",
        go_version: "n/a (telemetryd is not Prometheus)",
    })
}

/// Render a float the way Prometheus does.
fn format_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() {
        if value > 0.0 { "+Inf" } else { "-Inf" }.to_owned()
    } else {
        // `{}` gives the shortest representation that round-trips, which is what
        // Prometheus emits and what a chart library expects to parse.
        format!("{value}")
    }
}

#[allow(clippy::cast_precision_loss)]
fn to_seconds(nanos: u64) -> f64 {
    nanos as f64 / 1e9
}

fn labels_to_map(labels: &Labels) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

// ---------------------------------------------------------------------------
// Parameter parsing
// ---------------------------------------------------------------------------

/// Prometheus timestamps are Unix **seconds**, possibly fractional.
pub fn parse_time(raw: &str) -> Result<u64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(Error::BadRequest("empty timestamp".to_owned()));
    }
    if let Ok(seconds) = raw.parse::<f64>()
        && seconds >= 0.0
        && seconds.is_finite()
    {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        return Ok((seconds * 1e9) as u64);
    }
    // RFC3339 is also accepted by Prometheus.
    crate::loki::parse_time(raw)
}

/// The instants a range query is evaluated at: `start`, then every `step` up to `end`.
///
/// Computed once and used by both the load and the step loop, so they cannot disagree —
/// that disagreement once indexed past the end of an array and took the server down.
/// Each instant is `start + i·step` rather than a running sum: a step near `u64::MAX`
/// used to wrap the running sum back below `end`, and the loop then counted down one
/// nanosecond at a time until memory ran out. And the grid is Prometheus's own: it stops
/// at the last step that fits, rather than adding an extra point at an unaligned `end`.
fn step_grid(start: u64, end: u64, step_nanos: u64) -> Result<Vec<u64>> {
    let intervals = (end - start) / step_nanos.max(1);
    let steps = usize::try_from(intervals)
        .ok()
        .and_then(|n| n.checked_add(1))
        .filter(|n| *n <= MAX_STEPS)
        .ok_or_else(|| {
            Error::BadRequest(format!(
                "that range and step would need {} points per series; the limit is \
                 {MAX_STEPS}. Widen `step` or narrow the range.",
                intervals.saturating_add(1)
            ))
        })?;
    Ok((0..steps as u64).map(|i| start + i * step_nanos).collect())
}

/// `step` is seconds, or a duration string.
pub fn parse_step(raw: &str) -> Result<Duration> {
    let raw = raw.trim();
    if let Ok(seconds) = raw.parse::<f64>() {
        if seconds <= 0.0 || !seconds.is_finite() {
            return Err(Error::BadRequest(
                "`step` must be a positive number of seconds".to_owned(),
            ));
        }
        // `try_`, not the plain form: `step=1e300` is a finite positive number and the
        // plain conversion panics on it, which in a release build ends the process.
        return Duration::try_from_secs_f64(seconds).map_err(|_| {
            Error::BadRequest(format!("`step` of {raw} seconds is too large to represent"))
        });
    }
    match crate::lexer::tokenize(raw)?.first().map(|s| &s.token) {
        Some(crate::lexer::Token::Duration(nanos)) if *nanos > 0 => {
            Ok(Duration::from_nanos(*nanos))
        }
        _ => Err(Error::BadRequest(format!(
            "`step` must be a positive duration, got {raw:?}"
        ))),
    }
}

fn required_query(query: Option<&String>) -> Result<Expr> {
    let raw = query
        .map(String::as_str)
        .filter(|q| !q.trim().is_empty())
        .ok_or_else(|| Error::BadRequest("the `query` parameter is required".to_owned()))?;
    promql::parse(raw)
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// `GET,POST /api/v1/query`
pub fn instant(
    store: &RecordStore<MetricSchema>,
    params: &InstantParams,
    now_nanos: u64,
    max_samples: u64,
) -> Result<PromResponse<InstantData>> {
    let expr = required_query(params.query.as_ref())?;
    let at = match params.time.as_deref().filter(|s| !s.is_empty()) {
        Some(raw) => parse_time(raw)?,
        None => now_nanos,
    };

    let snapshot = Snapshot::load(store, &expr, at, at, max_samples)?;
    let vector = match snapshot.eval(&expr, at)? {
        Value::Vector(vector) => vector,
        Value::Scalar(value) => crate::promeval::InstantVector {
            samples: vec![(Labels::new(), value)],
        },
    };

    let result = vector
        .samples
        .into_iter()
        .map(|(labels, value)| InstantResult {
            metric: labels_to_map(&labels),
            value: (to_seconds(at), format_value(value)),
        })
        .collect();

    Ok(PromResponse::success(InstantData {
        result_type: "vector",
        result,
    }))
}

/// `GET,POST /api/v1/query_range`
pub fn range(
    store: &RecordStore<MetricSchema>,
    params: &RangeParams,
    now_nanos: u64,
    max_samples: u64,
) -> Result<PromResponse<RangeData>> {
    let expr = required_query(params.query.as_ref())?;

    let end = match params.end.as_deref().filter(|s| !s.is_empty()) {
        Some(raw) => parse_time(raw)?,
        None => now_nanos,
    };
    let start = match params.start.as_deref().filter(|s| !s.is_empty()) {
        Some(raw) => parse_time(raw)?,
        None => end.saturating_sub(3_600_000_000_000),
    };
    if start > end {
        return Err(Error::BadRequest(
            "`start` must not be after `end`".to_owned(),
        ));
    }

    let step = match params.step.as_deref().filter(|s| !s.is_empty()) {
        Some(raw) => parse_step(raw)?,
        None => Duration::from_secs(60),
    };
    let step_nanos = u64::try_from(step.as_nanos()).unwrap_or(u64::MAX).max(1);
    let timestamps = step_grid(start, end, step_nanos)?;

    // Load once, evaluate every step from memory.
    let mut snapshot = Snapshot::load_at(store, &expr, &timestamps, max_samples)?;
    // Range-vector calls turned inside out: each series walked once with a sliding
    // window, instead of every series bisected at every step. A no-op after a folded
    // load, which already produced values for these points.
    snapshot.prepare(&expr, &timestamps);

    // Keyed by label set so a series' points stay together across steps. A hash map
    // rather than an ordered one: this is entered once per result series per step — for a
    // six-hour chart at a minute's resolution, hundreds of thousands of times — and an
    // ordered lookup pays for comparing whole label sets on every one. Order is restored
    // below, where it costs one sort instead of one per insert.
    let mut series: HashMap<Labels, Vec<(f64, String)>> = HashMap::new();
    for &at in &timestamps {
        if let Value::Vector(vector) = snapshot.eval(&expr, at)? {
            for (labels, value) in vector.samples {
                series
                    .entry(labels)
                    .or_default()
                    .push((to_seconds(at), format_value(value)));
            }
        }
    }

    let mut series: Vec<(Labels, Vec<(f64, String)>)> = series.into_iter().collect();
    series.sort_by(|(a, _), (b, _)| a.cmp(b));
    let result = series
        .into_iter()
        .map(|(labels, values)| RangeResult {
            metric: labels_to_map(&labels),
            values,
        })
        .collect();

    Ok(PromResponse::success(RangeData {
        result_type: "matrix",
        result,
    }))
}

/// `GET /api/v1/labels`
pub fn label_names(
    store: &RecordStore<MetricSchema>,
    start_nanos: u64,
    end_nanos: u64,
) -> PromResponse<Vec<String>> {
    PromResponse::success(store.label_names(start_nanos, end_nanos))
}

/// `GET /api/v1/label/{name}/values`
pub fn label_values(
    store: &RecordStore<MetricSchema>,
    name: &str,
    start_nanos: u64,
    end_nanos: u64,
) -> Result<PromResponse<Vec<String>>> {
    Ok(PromResponse::success(store.label_values(
        name,
        start_nanos,
        end_nanos,
    )?))
}

/// `GET /api/v1/series`
pub fn series(
    store: &RecordStore<MetricSchema>,
    selectors: &[String],
    start_nanos: u64,
    end_nanos: u64,
) -> Result<PromResponse<Vec<BTreeMap<String, String>>>> {
    let mut seen: std::collections::BTreeSet<Labels> = std::collections::BTreeSet::new();

    if selectors.is_empty() {
        seen.extend(store.streams(start_nanos, end_nanos, &[])?);
    } else {
        for selector in selectors {
            let matchers = matchers_of(selector)?;
            seen.extend(store.streams(start_nanos, end_nanos, &matchers)?);
        }
    }

    Ok(PromResponse::success(
        seen.iter().map(labels_to_map).collect(),
    ))
}

/// Extract the matchers from a bare selector, for `match[]`.
fn matchers_of(selector: &str) -> Result<Vec<LabelMatcher>> {
    match promql::parse(selector)? {
        Expr::Selector(selector) => Ok(selector.matchers),
        _ => Err(Error::BadRequest(format!(
            "`match[]` takes a series selector, not an expression: {selector:?}"
        ))),
    }
}

/// Resolve a metadata endpoint's time range, defaulting to the last hour.
pub fn meta_range(params: &MetaParams, now_nanos: u64) -> Result<(u64, u64)> {
    let end = match params.end.as_deref().filter(|s| !s.is_empty()) {
        Some(raw) => parse_time(raw)?,
        None => now_nanos,
    };
    let start = match params.start.as_deref().filter(|s| !s.is_empty()) {
        Some(raw) => parse_time(raw)?,
        None => end.saturating_sub(3_600_000_000_000),
    };
    Ok((start, end))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// `step=1e300` is a finite positive number, and converting it used to panic; in a
    /// release build that ended the process. It is a 400 now.
    #[test]
    fn a_step_too_large_to_represent_is_refused() {
        assert!(parse_step("1e300").is_err());
        assert!(parse_step("1e11").is_ok(), "large but representable");
    }

    /// A step near `u64::MAX` wrapped the running sum back below `end`, and the loop then
    /// counted down one nanosecond at a time until memory ran out. The grid is computed as
    /// `start + i·step` now, and a count past the limit is refused before allocating.
    #[test]
    fn the_step_grid_cannot_wrap_or_run_away() {
        let start = 1_700_000_000_000_000_000u64;
        let end = start + 1_000_000_000;
        assert_eq!(step_grid(start, end, u64::MAX).unwrap(), vec![start]);
        assert!(step_grid(0, 1_000_000_000_000_000_000, 1).is_err());

        // Prometheus's grid: the last point is the last whole step, not an extra one at
        // an unaligned end.
        assert_eq!(step_grid(0, 25, 10).unwrap(), vec![0, 10, 20]);
        assert_eq!(step_grid(0, 20, 10).unwrap(), vec![0, 10, 20]);
    }

    const NOW: u64 = 1_750_000_000_000_000_000;

    #[test]
    fn prometheus_timestamps_are_seconds() {
        assert_eq!(parse_time("1750000000").unwrap(), NOW);
        assert_eq!(parse_time("1750000000.5").unwrap(), NOW + 500_000_000);
    }

    #[test]
    fn rfc3339_is_accepted_too() {
        assert_eq!(parse_time("2025-06-15T15:06:40Z").unwrap(), NOW);
    }

    #[test]
    fn step_accepts_seconds_and_durations() {
        assert_eq!(parse_step("60").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_step("1m").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_step("15s").unwrap(), Duration::from_secs(15));
        assert_eq!(parse_step("0.5").unwrap(), Duration::from_millis(500));
    }

    #[test]
    fn a_non_positive_step_is_refused() {
        for raw in ["0", "-5", "abc", ""] {
            assert!(parse_step(raw).is_err(), "{raw}");
        }
    }

    #[test]
    fn values_render_the_way_prometheus_renders_them() {
        assert_eq!(format_value(1.0), "1");
        assert_eq!(format_value(1.5), "1.5");
        assert_eq!(format_value(0.0), "0");
        // NaN and the infinities are real Prometheus signals, not errors.
        assert_eq!(format_value(f64::NAN), "NaN");
        assert_eq!(format_value(f64::INFINITY), "+Inf");
        assert_eq!(format_value(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn query_is_required() {
        assert!(required_query(None).is_err());
        assert!(required_query(Some(&String::new())).is_err());
        assert!(required_query(Some(&"   ".to_owned())).is_err());
    }

    #[test]
    fn build_info_is_honest_about_what_this_is() {
        // Answering the probe matters; pretending to be a Prometheus build does not.
        let json = serde_json::to_value(build_info()).unwrap();
        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["version"], telemetryd_core::VERSION);
        assert!(
            json["data"]["goVersion"]
                .as_str()
                .unwrap()
                .contains("not Prometheus"),
            "{json}"
        );
    }

    #[test]
    fn match_takes_a_selector_not_an_expression() {
        assert_eq!(matchers_of(r#"up{app="checkout"}"#).unwrap().len(), 2);

        let err = matchers_of("sum(up)").unwrap_err();
        assert!(err.to_string().contains("series selector"), "{err}");
    }

    #[test]
    fn the_instant_response_shape_matches_prometheus() {
        let response = PromResponse::success(InstantData {
            result_type: "vector",
            result: vec![InstantResult {
                metric: [("app".to_owned(), "checkout".to_owned())]
                    .into_iter()
                    .collect(),
                value: (1_750_000_000.0, "42".to_owned()),
            }],
        });
        let json = serde_json::to_value(&response).unwrap();

        assert_eq!(json["status"], "success");
        assert_eq!(json["data"]["resultType"], "vector");
        assert_eq!(json["data"]["result"][0]["metric"]["app"], "checkout");
        // A number then a string, in that order.
        assert!(json["data"]["result"][0]["value"][0].is_number());
        assert_eq!(json["data"]["result"][0]["value"][1], "42");
    }

    #[test]
    fn the_range_response_shape_matches_prometheus() {
        let response = PromResponse::success(RangeData {
            result_type: "matrix",
            result: vec![RangeResult {
                metric: BTreeMap::new(),
                values: vec![(1_750_000_000.0, "1".to_owned())],
            }],
        });
        let json = serde_json::to_value(&response).unwrap();

        assert_eq!(json["data"]["resultType"], "matrix");
        assert!(json["data"]["result"][0]["values"][0][0].is_number());
        assert!(json["data"]["result"][0]["values"][0][1].is_string());
    }
}
