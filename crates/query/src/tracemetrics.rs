//! TraceQL metrics: `{ status = error } | rate() by (resource.service.name)`, answered
//! on `/api/metrics/query_range` and `/api/metrics/query` in Tempo's shapes.
//!
//! Computed from the stored spans at query time, as Tempo's own query path computes
//! them — checked against Tempo in `tempo_conformance.rs`. Points sit on whole steps, the
//! start rounded down to one and the end up, and each counts the spans that started in
//! the step before it. `rate` is spans per second of the step, `count_over_time` spans per step, the `*_over_time` family reads
//! a field — `duration` in seconds — and `quantile_over_time` answers one series per
//! quantile under the label `p`. `histogram_over_time` counts spans into power-of-two
//! buckets under `__bucket`, as Tempo does. Quantiles here are exact rather than read off
//! Tempo's log-2 histogram, so they can differ from Tempo's by up to a bucket's width.

use std::collections::{BTreeMap, HashMap};

use serde::Deserialize;
use telemetryd_core::span::SpanRecord;
use telemetryd_core::{Error, Result};
use telemetryd_store::RecordStore;
use telemetryd_store::spans::SpanSchema;

use crate::traceql::{self, MetricsFunction, MetricsStage, TraceQuery};

/// Series one query may answer with, as Tempo's default `max_response_series`.
pub const MAX_SERIES: usize = 1_000;
/// Steps one range may be cut into.
pub const MAX_STEPS: u64 = 11_000;

/// `GET /api/metrics/query_range` and `/api/metrics/query` parameters.
#[derive(Debug, Default, Deserialize)]
pub struct MetricsParams {
    pub q: Option<String>,
    /// Unix seconds or nanoseconds, or RFC 3339.
    pub start: Option<String>,
    pub end: Option<String>,
    pub since: Option<String>,
    /// A duration (`30s`) or seconds.
    pub step: Option<String>,
}

/// A validated metrics request: the query, its window, and the step.
#[derive(Debug, Clone)]
pub struct MetricsRequest {
    pub query: TraceQuery,
    pub start_nanos: u64,
    pub end_nanos: u64,
    pub step_nanos: u64,
}

impl MetricsRequest {
    /// # Errors
    /// A `400` for a query without a metrics function, or an unreadable window or step.
    pub fn from_params(params: &MetricsParams, now_nanos: u64, instant: bool) -> Result<Self> {
        let query = traceql::parse(params.q.as_deref().unwrap_or_default())?;
        if query.metrics.is_none() {
            return Err(Error::BadRequest(
                "a metrics query ends in a metrics function, like \
                 { status = error } | rate() by (resource.service.name); /api/search \
                 answers a search"
                    .to_owned(),
            ));
        }
        let end_nanos = match params.end.as_deref().filter(|s| !s.trim().is_empty()) {
            Some(raw) => crate::loki::parse_time(raw)?,
            None => now_nanos,
        };
        let start_nanos =
            if let Some(raw) = params.start.as_deref().filter(|s| !s.trim().is_empty()) {
                crate::loki::parse_time(raw)?
            } else {
                let since = match params.since.as_deref().filter(|s| !s.trim().is_empty()) {
                    Some(raw) => duration_nanos(raw)?,
                    None => 3_600_000_000_000,
                };
                end_nanos.saturating_sub(since)
            };
        if start_nanos >= end_nanos {
            return Err(Error::BadRequest("`start` must be before `end`".to_owned()));
        }
        let range = end_nanos - start_nanos;
        if instant {
            return Ok(Self {
                query,
                start_nanos,
                end_nanos,
                step_nanos: range,
            });
        }
        let step_nanos = match params.step.as_deref().filter(|s| !s.trim().is_empty()) {
            Some(raw) => duration_nanos(raw)?,
            // A hundred points across the range, at least a second apart.
            None => (range / 100).max(1_000_000_000),
        };
        if step_nanos == 0 || range.div_ceil(step_nanos) > MAX_STEPS {
            return Err(Error::BadRequest(format!(
                "that range and step make more than {MAX_STEPS} points; widen `step`"
            )));
        }
        // Tempo's grid: whole steps, the start rounded down and the end up, one step
        // before the first point, which covers the step before it.
        let first = start_nanos - start_nanos % step_nanos;
        let last = end_nanos.div_ceil(step_nanos).saturating_mul(step_nanos);
        let (start_nanos, end_nanos) = (first.saturating_sub(step_nanos), last);
        Ok(Self {
            query,
            start_nanos,
            end_nanos,
            step_nanos,
        })
    }

    fn stage(&self) -> &MetricsStage {
        // Checked when the request was built.
        self.query
            .metrics
            .as_ref()
            .unwrap_or_else(|| unreachable!("a metrics request has a metrics stage"))
    }

    fn steps(&self) -> usize {
        usize::try_from((self.end_nanos - self.start_nanos).div_ceil(self.step_nanos))
            .unwrap_or(usize::MAX)
    }
}

/// `30s`, `1m30s`, or a number of seconds.
fn duration_nanos(raw: &str) -> Result<u64> {
    crate::prometheus::parse_step(raw).map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// What one series gathered in one step.
#[derive(Debug, Default, Clone)]
struct Bucket {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
    values: Vec<f64>,
    histogram: BTreeMap<u64, u64>,
}

/// A series' identity: the `by` labels, in the order they were written.
type Key = Vec<(String, String)>;

/// One answer series: labels, and `(step start, value)` points.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    pub labels: Vec<(String, LabelValue)>,
    pub points: Vec<(u64, f64)>,
}

/// A label's value, typed as Tempo types it: text, or a number for `p` and `__bucket`.
#[derive(Debug, Clone, PartialEq)]
pub enum LabelValue {
    Text(String),
    Number(f64),
}

/// Evaluate a metrics request over the stored spans.
///
/// # Errors
/// A `400` for more than [`MAX_SERIES`] series; storage errors as they come.
pub fn evaluate(store: &RecordStore<SpanSchema>, request: &MetricsRequest) -> Result<Vec<Series>> {
    let stage = request.stage();
    let steps = request.steps();
    let mut series: HashMap<Key, Vec<Bucket>> = HashMap::new();
    let mut refused = None;
    let gathered = std::sync::Mutex::new((&mut series, &mut refused));
    let visit = |span: &SpanRecord| {
        if !request.query.matches(span) {
            return false;
        }
        let key: Key = stage
            .by
            .iter()
            .filter_map(|(written, field)| field.text(span).map(|v| (written.clone(), v)))
            .collect();
        let value = stage.field.as_ref().map(|field| field.number(span));
        let step = usize::try_from(
            span.start_nanos.saturating_sub(request.start_nanos) / request.step_nanos,
        )
        .unwrap_or(usize::MAX)
        .min(steps - 1);
        let mut guard = gathered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (series, refused) = &mut *guard;
        if refused.is_some() {
            return false;
        }
        if !series.contains_key(&key) && series.len() >= MAX_SERIES {
            **refused = Some(Error::BadRequest(format!(
                "more than {MAX_SERIES} series; group by fewer fields or filter the spans"
            )));
            return false;
        }
        let bucket = &mut series
            .entry(key)
            .or_insert_with(|| vec![Bucket::default(); steps])[step];
        match value {
            // A span without the field read counts for nothing here.
            Some(None) => {}
            Some(Some(value)) => bucket.add(stage.function, value),
            None => bucket.count += 1,
        }
        false
    };
    let scan = telemetryd_store::Scan::range(request.start_nanos, request.end_nanos - 1);
    store.scan(scan, &[], &visit)?;
    drop(gathered);
    if let Some(error) = refused {
        return Err(error);
    }
    Ok(answer(stage, request, series))
}

impl Bucket {
    fn add(&mut self, function: MetricsFunction, value: f64) {
        if self.count == 0 || value < self.min {
            self.min = value;
        }
        if self.count == 0 || value > self.max {
            self.max = value;
        }
        self.count += 1;
        self.sum += value;
        match function {
            MetricsFunction::QuantileOverTime => self.values.push(value),
            MetricsFunction::HistogramOverTime => {
                *self
                    .histogram
                    .entry(power_of_two_bucket(value))
                    .or_default() += 1;
            }
            _ => {}
        }
    }
}

/// The power-of-two upper bound a value falls under, as the bits of an `f64`, so the
/// buckets sort and key exactly.
fn power_of_two_bucket(value: f64) -> u64 {
    let bound = if value <= 0.0 {
        0.0
    } else {
        // Clamped first: an f64's exponent fits an i32 many times over.
        #[allow(clippy::cast_possible_truncation)]
        let exponent = value.log2().ceil().clamp(-1074.0, 1024.0) as i32;
        2f64.powi(exponent)
    };
    bound.to_bits()
}

/// Turn the buckets into series, each function its own way.
fn answer(
    stage: &MetricsStage,
    request: &MetricsRequest,
    series: HashMap<Key, Vec<Bucket>>,
) -> Vec<Series> {
    #[allow(clippy::cast_precision_loss)]
    let step_seconds = request.step_nanos as f64 / 1e9;
    // A step's point is at its end: it counts what started in the step before it. An
    // instant answer is one step whose point is its window's end.
    let at = |i: usize| request.start_nanos + (i as u64 + 1) * request.step_nanos;
    // With no `by`, Tempo names the one series after its function.
    let text = |key: &Key| -> Vec<(String, LabelValue)> {
        if stage.by.is_empty() {
            return vec![(
                "__name__".to_owned(),
                LabelValue::Text(stage.function.name().to_owned()),
            )];
        }
        key.iter()
            .map(|(k, v)| (k.clone(), LabelValue::Text(v.clone())))
            .collect()
    };
    let mut out = Vec::new();
    for (key, buckets) in series {
        match stage.function {
            MetricsFunction::QuantileOverTime => {
                for &q in &stage.quantiles {
                    let points = buckets
                        .iter()
                        .enumerate()
                        .filter(|(_, b)| !b.values.is_empty())
                        .map(|(i, b)| {
                            let mut values = b.values.clone();
                            (at(i), crate::promfn::quantile(q, &mut values))
                        })
                        .collect();
                    let mut labels = text(&key);
                    labels.push(("p".to_owned(), LabelValue::Number(q)));
                    out.push(Series { labels, points });
                }
            }
            MetricsFunction::HistogramOverTime => {
                let mut by_bound: BTreeMap<u64, Vec<(u64, f64)>> = BTreeMap::new();
                for (i, bucket) in buckets.iter().enumerate() {
                    for (&bound, &count) in &bucket.histogram {
                        #[allow(clippy::cast_precision_loss)]
                        by_bound
                            .entry(bound)
                            .or_default()
                            .push((at(i), count as f64));
                    }
                }
                for (bound, points) in by_bound {
                    let mut labels = text(&key);
                    labels.push((
                        "__bucket".to_owned(),
                        LabelValue::Number(f64::from_bits(bound)),
                    ));
                    out.push(Series { labels, points });
                }
            }
            function => {
                #[allow(clippy::cast_precision_loss)]
                let points = buckets
                    .iter()
                    .enumerate()
                    .filter_map(|(i, b)| {
                        let value = match function {
                            // Counts are dense: a step with no span is a zero.
                            MetricsFunction::Rate => b.count as f64 / step_seconds,
                            MetricsFunction::CountOverTime => b.count as f64,
                            _ if b.count == 0 => return None,
                            MetricsFunction::SumOverTime => b.sum,
                            MetricsFunction::AvgOverTime => b.sum / b.count as f64,
                            MetricsFunction::MinOverTime => b.min,
                            _ => b.max,
                        };
                        Some((at(i), value))
                    })
                    .collect();
                out.push(Series {
                    labels: text(&key),
                    points,
                });
            }
        }
    }
    out.sort_by(|a, b| format!("{:?}", a.labels).cmp(&format!("{:?}", b.labels)));
    if let Some((top, k)) = stage.rank {
        out = rank(out, top, k);
    }
    out
}

/// `topk(k)` / `bottomk(k)`: at each step, only the `k` highest (or lowest) series keep
/// their point there.
fn rank(series: Vec<Series>, top: bool, k: usize) -> Vec<Series> {
    let mut by_time: BTreeMap<u64, Vec<(usize, f64)>> = BTreeMap::new();
    for (index, s) in series.iter().enumerate() {
        for &(t, v) in &s.points {
            by_time.entry(t).or_default().push((index, v));
        }
    }
    let mut kept: Vec<Vec<(u64, f64)>> = vec![Vec::new(); series.len()];
    for (t, mut points) in by_time {
        points.sort_by(|a, b| {
            let order = a.1.total_cmp(&b.1);
            if top { order.reverse() } else { order }
        });
        for (index, v) in points.into_iter().take(k) {
            kept[index].push((t, v));
        }
    }
    series
        .into_iter()
        .zip(kept)
        .filter(|(_, points)| !points.is_empty())
        .map(|(s, points)| Series {
            labels: s.labels,
            points,
        })
        .collect()
}

fn label_json(labels: &[(String, LabelValue)]) -> serde_json::Value {
    serde_json::Value::Array(
        labels
            .iter()
            .map(|(key, value)| match value {
                LabelValue::Text(text) => {
                    serde_json::json!({"key": key, "value": {"stringValue": text}})
                }
                LabelValue::Number(number) => {
                    serde_json::json!({"key": key, "value": {"doubleValue": number}})
                }
            })
            .collect(),
    )
}

/// Prometheus's text form of the labels, which Tempo sends alongside as `promLabels`.
fn prom_labels(labels: &[(String, LabelValue)]) -> String {
    let inner = labels
        .iter()
        .map(|(key, value)| match value {
            LabelValue::Text(text) => format!("{key}={text:?}"),
            LabelValue::Number(number) => format!("{key}=\"{number}\""),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{inner}}}")
}

/// Tempo's `QueryRangeResponse` in JSON.
#[must_use]
pub fn range_json(series: &[Series]) -> serde_json::Value {
    let series: Vec<_> = series
        .iter()
        .map(|s| {
            serde_json::json!({
                "labels": label_json(&s.labels),
                "promLabels": prom_labels(&s.labels),
                "samples": s.points.iter().map(|(t, v)| serde_json::json!({
                    "timestampMs": (t / 1_000_000).to_string(),
                    "value": v,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::json!({ "series": series, "metrics": {} })
}

/// Tempo's `QueryInstantResponse` in JSON: one value per series over the whole window.
#[must_use]
pub fn instant_json(series: &[Series]) -> serde_json::Value {
    let series: Vec<_> = series
        .iter()
        .filter_map(|s| {
            let (_, value) = s.points.first()?;
            Some(serde_json::json!({
                "labels": label_json(&s.labels),
                "promLabels": prom_labels(&s.labels),
                "value": value,
            }))
        })
        .collect();
    serde_json::json!({ "series": series, "metrics": {} })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_fall_under_power_of_two_bounds() {
        let bound = |v: f64| f64::from_bits(power_of_two_bucket(v));
        assert!((bound(0.3) - 0.5).abs() < f64::EPSILON);
        assert!((bound(1.0) - 1.0).abs() < f64::EPSILON);
        assert!((bound(3.0) - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn topk_keeps_the_highest_at_each_step() {
        let s = |name: &str, points: Vec<(u64, f64)>| Series {
            labels: vec![("x".into(), LabelValue::Text(name.into()))],
            points,
        };
        let ranked = rank(
            vec![
                s("a", vec![(0, 1.0), (1, 5.0)]),
                s("b", vec![(0, 3.0), (1, 2.0)]),
            ],
            true,
            1,
        );
        assert_eq!(ranked[0].points, vec![(1, 5.0)]);
        assert_eq!(ranked[1].points, vec![(0, 3.0)]);
    }
}
