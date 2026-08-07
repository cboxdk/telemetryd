//! PromQL evaluation.
//!
//! # Shape
//!
//! Storage is read **once** for the whole query, not once per step. A one-hour range at
//! a 15-second step is 240 evaluations; re-reading segments for each would make a chart
//! cost 240 scans instead of one. Samples are loaded for the union of every selector's
//! matchers over `[start - lookback, end]`, grouped into series, and every step is then
//! evaluated from memory.
//!
//! Timestamps and counts convert to `f64` throughout: PromQL is defined over floats,
//! and the values involved — nanosecond durations within a query window, sample counts
//! — are far below the 2^53 boundary where that would matter.
#![allow(clippy::cast_precision_loss)]

use std::collections::BTreeMap;
use std::time::Duration;

use telemetryd_core::{Error, LabelMatcher, Labels, MetricSample, Result};
use telemetryd_store::RecordStore;
use telemetryd_store::metrics::MetricSchema;

use crate::promql::{AggregateOp, BinaryOp, Expr, Function, Grouping, Selector};

const NANOS_PER_SECOND: f64 = 1e9;

/// One series' samples, ascending by time.
#[derive(Debug, Clone)]
pub struct Series {
    pub labels: Labels,
    pub samples: Vec<(u64, f64)>,
}

/// An instant vector: one value per series at one moment.
#[derive(Debug, Clone, Default)]
pub struct InstantVector {
    pub samples: Vec<(Labels, f64)>,
}

/// What an expression evaluates to.
#[derive(Debug, Clone)]
pub enum Value {
    Scalar(f64),
    Vector(InstantVector),
}

impl Value {
    fn into_vector(self) -> InstantVector {
        match self {
            Self::Vector(vector) => vector,
            // A bare scalar in vector position has no labels; Prometheus treats it as a
            // single unlabelled sample.
            Self::Scalar(value) => InstantVector {
                samples: vec![(Labels::new(), value)],
            },
        }
    }
}

/// Series loaded once and evaluated many times.
#[derive(Debug, Default)]
pub struct Snapshot {
    series: Vec<Series>,
}

impl Snapshot {
    /// Load everything the expression could need.
    pub fn load(
        store: &RecordStore<MetricSchema>,
        expr: &Expr,
        start_nanos: u64,
        end_nanos: u64,
    ) -> Result<Self> {
        let lookback = expr.required_lookback();
        let from = start_nanos.saturating_sub(duration_nanos(lookback));

        // One read covering every selector. A query's selectors usually differ only by
        // `offset`, so reading their union once and filtering per selector in memory is
        // both simpler and fewer segment opens than reading each separately.
        let mut wanted: Vec<LabelMatcher> = Vec::new();
        for selector in expr.selectors() {
            if selector.matchers.len() == 1 {
                wanted.clone_from(&selector.matchers);
                break;
            }
            if wanted.is_empty() {
                wanted.clone_from(&selector.matchers);
            }
        }
        // Only matchers common to every selector can be pushed down safely.
        let pushdown: Vec<LabelMatcher> = wanted
            .into_iter()
            .filter(|matcher| {
                expr.selectors()
                    .iter()
                    .all(|selector| selector.matchers.contains(matcher))
            })
            .collect();

        let samples = store.query(from, end_nanos, &pushdown, &|_| true)?;

        let mut by_series: BTreeMap<Labels, Vec<(u64, f64)>> = BTreeMap::new();
        for sample in samples {
            by_series
                .entry(sample.series.clone())
                .or_default()
                .push((sample.timestamp_nanos, sample.value));
        }

        let series = by_series
            .into_iter()
            .map(|(labels, mut samples)| {
                samples.sort_by_key(|(ts, _)| *ts);
                Series { labels, samples }
            })
            .collect();

        Ok(Self { series })
    }

    /// Build directly from samples, for testing and for the in-memory path.
    pub fn from_samples(samples: Vec<MetricSample>) -> Self {
        let mut by_series: BTreeMap<Labels, Vec<(u64, f64)>> = BTreeMap::new();
        for sample in samples {
            by_series
                .entry(sample.series.clone())
                .or_default()
                .push((sample.timestamp_nanos, sample.value));
        }
        Self {
            series: by_series
                .into_iter()
                .map(|(labels, mut samples)| {
                    samples.sort_by_key(|(ts, _)| *ts);
                    Series { labels, samples }
                })
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }

    /// Evaluate at one instant.
    pub fn eval(&self, expr: &Expr, at_nanos: u64) -> Result<Value> {
        match expr {
            Expr::Number(value) => Ok(Value::Scalar(*value)),
            Expr::Selector(selector) => Ok(Value::Vector(self.instant(selector, at_nanos))),
            Expr::Negate(inner) => Ok(match self.eval(inner, at_nanos)? {
                Value::Scalar(value) => Value::Scalar(-value),
                Value::Vector(mut vector) => {
                    for (_, value) in &mut vector.samples {
                        *value = -*value;
                    }
                    Value::Vector(vector)
                }
            }),
            Expr::Call { function, args } => self.eval_call(*function, args, at_nanos),
            Expr::Aggregation {
                op,
                grouping,
                inner,
            } => {
                let vector = self.eval(inner, at_nanos)?.into_vector();
                Ok(Value::Vector(aggregate(*op, grouping, &vector)))
            }
            Expr::Binary { op, left, right } => {
                let left = self.eval(left, at_nanos)?;
                let right = self.eval(right, at_nanos)?;
                Ok(binary(*op, left, right))
            }
        }
    }

    fn eval_call(&self, function: Function, args: &[Expr], at_nanos: u64) -> Result<Value> {
        match function {
            Function::Rate | Function::Increase => {
                let Expr::Selector(selector) = &args[0] else {
                    return Err(Error::BadRequest(format!(
                        "`{}` needs a range vector, e.g. {}(metric[5m])",
                        function.as_str(),
                        function.as_str()
                    )));
                };
                let Some(range) = selector.range else {
                    return Err(Error::BadRequest(format!(
                        "`{}` needs a range selector like `metric[5m]`",
                        function.as_str()
                    )));
                };
                Ok(Value::Vector(self.rate(
                    selector,
                    range,
                    at_nanos,
                    function == Function::Rate,
                )))
            }
            Function::HistogramQuantile => {
                let quantile = match self.eval(&args[0], at_nanos)? {
                    Value::Scalar(value) => value,
                    Value::Vector(_) => {
                        return Err(Error::BadRequest(
                            "histogram_quantile needs a scalar quantile as its first argument"
                                .to_owned(),
                        ));
                    }
                };
                let vector = self.eval(&args[1], at_nanos)?.into_vector();
                Ok(Value::Vector(histogram_quantile(quantile, &vector)))
            }
            Function::ClampMin | Function::ClampMax => {
                let bound = match self.eval(&args[1], at_nanos)? {
                    Value::Scalar(value) => value,
                    Value::Vector(_) => {
                        return Err(Error::BadRequest(format!(
                            "`{}` needs a scalar bound as its second argument",
                            function.as_str()
                        )));
                    }
                };
                let mut vector = self.eval(&args[0], at_nanos)?.into_vector();
                for (_, value) in &mut vector.samples {
                    *value = if function == Function::ClampMin {
                        value.max(bound)
                    } else {
                        value.min(bound)
                    };
                }
                Ok(Value::Vector(vector))
            }
            Function::Abs => {
                let mut vector = self.eval(&args[0], at_nanos)?.into_vector();
                for (_, value) in &mut vector.samples {
                    *value = value.abs();
                }
                Ok(Value::Vector(vector))
            }
        }
    }

    /// The most recent sample per matching series within the lookback window.
    fn instant(&self, selector: &Selector, at_nanos: u64) -> InstantVector {
        let at = at_nanos.saturating_sub(duration_nanos(selector.offset.unwrap_or(Duration::ZERO)));
        let floor = at.saturating_sub(duration_nanos(crate::promql::DEFAULT_LOOKBACK));

        let mut samples = Vec::new();
        for series in self.matching(selector) {
            if let Some((_, value)) = series
                .samples
                .iter()
                .rev()
                .find(|(ts, _)| *ts <= at && *ts > floor)
            {
                samples.push((strip_name(&series.labels), *value));
            }
        }
        InstantVector { samples }
    }

    /// `rate` and `increase` over a range window.
    ///
    /// Counter resets are handled the way Prometheus does: a drop between consecutive
    /// samples means the process restarted, so the new value is the increase rather
    /// than a negative delta. Without this every deploy would show as a large negative
    /// rate.
    ///
    /// The rate is computed over the span actually **observed** (last sample minus
    /// first), not over the nominal window. This matters: a range selector is
    /// half-open, `(t-range, t]`, so a series scraped exactly on the window boundary
    /// contributes one fewer interval than it looks like it should. Dividing by the
    /// nominal window in that case reports half the true rate — a number that is
    /// wrong in the ordinary case, not just the sparse one.
    ///
    /// `increase` is then that rate extrapolated across the window, which is what
    /// Prometheus reports and what makes `increase(x[1h])` comparable between series
    /// scraped at different intervals.
    fn rate(
        &self,
        selector: &Selector,
        range: Duration,
        at_nanos: u64,
        per_second: bool,
    ) -> InstantVector {
        let at = at_nanos.saturating_sub(duration_nanos(selector.offset.unwrap_or(Duration::ZERO)));
        let floor = at.saturating_sub(duration_nanos(range));

        let mut samples = Vec::new();
        for series in self.matching(selector) {
            let window: Vec<(u64, f64)> = series
                .samples
                .iter()
                .copied()
                .filter(|(ts, _)| *ts > floor && *ts <= at)
                .collect();

            // One point cannot describe a change.
            if window.len() < 2 {
                continue;
            }

            let mut increase = 0.0;
            for pair in window.windows(2) {
                let (previous, current) = (pair[0].1, pair[1].1);
                increase += if current < previous {
                    current
                } else {
                    current - previous
                };
            }

            let observed_nanos = window[window.len() - 1].0.saturating_sub(window[0].0);
            if observed_nanos == 0 {
                // Every sample shares a timestamp; there is no elapsed time to divide
                // by, and inventing one would report an arbitrary rate.
                continue;
            }
            let observed_seconds = observed_nanos as f64 / NANOS_PER_SECOND;
            let per_second_rate = increase / observed_seconds;

            let value = if per_second {
                per_second_rate
            } else {
                per_second_rate * (duration_nanos(range) as f64 / NANOS_PER_SECOND)
            };
            samples.push((strip_name(&series.labels), value));
        }
        InstantVector { samples }
    }

    fn matching<'a>(&'a self, selector: &'a Selector) -> impl Iterator<Item = &'a Series> {
        self.series
            .iter()
            .filter(move |series| telemetryd_core::matches_all(&selector.matchers, &series.labels))
    }
}

/// `__name__` is dropped from results, as Prometheus does once a function or
/// aggregation has been applied — the value is no longer that metric.
fn strip_name(labels: &Labels) -> Labels {
    let mut out = labels.clone();
    out.remove(telemetryd_core::METRIC_NAME_LABEL);
    out
}

fn aggregate(op: AggregateOp, grouping: &Grouping, vector: &InstantVector) -> InstantVector {
    let mut groups: BTreeMap<Labels, Vec<f64>> = BTreeMap::new();

    for (labels, value) in &vector.samples {
        let key = match grouping {
            Grouping::All => Labels::new(),
            Grouping::By(names) => names
                .iter()
                .filter_map(|name| labels.get(name).map(|v| (name.clone(), v.to_owned())))
                .collect(),
            Grouping::Without(names) => labels
                .iter()
                .filter(|(name, _)| !names.iter().any(|excluded| excluded == name))
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
        };
        groups.entry(key).or_default().push(*value);
    }

    let samples = groups
        .into_iter()
        .map(|(labels, values)| {
            let value = match op {
                AggregateOp::Sum => values.iter().sum(),
                AggregateOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
                AggregateOp::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
                AggregateOp::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                AggregateOp::Count => values.len() as f64,
            };
            (labels, value)
        })
        .collect();

    InstantVector { samples }
}

fn binary(op: BinaryOp, left: Value, right: Value) -> Value {
    match (op, left, right) {
        // Vector union: the left side wins, the right fills gaps. This is what makes
        // the UI's `sel offset 5m or sel * 0` yield zero instead of nothing.
        (BinaryOp::Or, left, right) => {
            let left = left.into_vector();
            let right = right.into_vector();
            let mut samples = left.samples;
            for (labels, value) in right.samples {
                if !samples.iter().any(|(existing, _)| *existing == labels) {
                    samples.push((labels, value));
                }
            }
            Value::Vector(InstantVector { samples })
        }
        (op, Value::Scalar(a), Value::Scalar(b)) => Value::Scalar(apply(op, a, b)),
        (op, Value::Vector(mut vector), Value::Scalar(scalar)) => {
            for (_, value) in &mut vector.samples {
                *value = apply(op, *value, scalar);
            }
            Value::Vector(vector)
        }
        (op, Value::Scalar(scalar), Value::Vector(mut vector)) => {
            for (_, value) in &mut vector.samples {
                *value = apply(op, scalar, *value);
            }
            Value::Vector(vector)
        }
        // Vector-to-vector: match on identical label sets, as PromQL's default
        // one-to-one matching does. Series present on only one side drop out.
        (op, Value::Vector(left), Value::Vector(right)) => {
            let samples = left
                .samples
                .into_iter()
                .filter_map(|(labels, value)| {
                    right
                        .samples
                        .iter()
                        .find(|(other, _)| *other == labels)
                        .map(|(_, other)| (labels, apply(op, value, *other)))
                })
                .collect();
            Value::Vector(InstantVector { samples })
        }
    }
}

fn apply(op: BinaryOp, a: f64, b: f64) -> f64 {
    match op {
        BinaryOp::Add => a + b,
        BinaryOp::Sub => a - b,
        BinaryOp::Mul => a * b,
        BinaryOp::Div => a / b,
        BinaryOp::Mod => a % b,
        BinaryOp::Pow => a.powf(b),
        BinaryOp::Or => a,
    }
}

/// Linear interpolation over cumulative histogram buckets.
fn histogram_quantile(quantile: f64, vector: &InstantVector) -> InstantVector {
    if !(0.0..=1.0).contains(&quantile) {
        // Prometheus returns +Inf/-Inf outside [0,1]; matching that beats erroring on
        // a dashboard that briefly computes a nonsense quantile.
        let value = if quantile < 0.0 {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
        return InstantVector {
            samples: vector
                .samples
                .iter()
                .map(|(labels, _)| (without_le(labels), value))
                .collect(),
        };
    }

    // Group buckets by everything except `le`.
    let mut histograms: BTreeMap<Labels, Vec<(f64, f64)>> = BTreeMap::new();
    for (labels, count) in &vector.samples {
        let Some(le) = labels.get("le") else { continue };
        let bound = if le == "+Inf" {
            f64::INFINITY
        } else {
            match le.parse::<f64>() {
                Ok(value) => value,
                Err(_) => continue,
            }
        };
        histograms
            .entry(without_le(labels))
            .or_default()
            .push((bound, *count));
    }

    let samples = histograms
        .into_iter()
        .filter_map(|(labels, mut buckets)| {
            buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            let total = buckets.last()?.1;
            if total <= 0.0 {
                return None;
            }

            let wanted = quantile * total;
            let mut previous_bound = 0.0;
            let mut previous_count = 0.0;

            for (bound, count) in buckets {
                if count >= wanted {
                    if bound.is_infinite() {
                        // The last finite bound is the best answer available.
                        return Some((labels, previous_bound));
                    }
                    let span = count - previous_count;
                    let position = if span > 0.0 {
                        (wanted - previous_count) / span
                    } else {
                        0.0
                    };
                    return Some((labels, previous_bound + (bound - previous_bound) * position));
                }
                previous_bound = bound;
                previous_count = count;
            }
            Some((labels, previous_bound))
        })
        .collect();

    InstantVector { samples }
}

fn without_le(labels: &Labels) -> Labels {
    let mut out = labels.clone();
    out.remove("le");
    out.remove(telemetryd_core::METRIC_NAME_LABEL);
    out
}

fn duration_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
// Exact float comparison is deliberate here: these are small values that round-trip
// through f64 without loss, and the assertion is that they arrived unchanged.
#[allow(clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::*;
    use telemetryd_core::MetricKind;
    use telemetryd_core::metric::METRIC_NAME_LABEL;

    const T0: u64 = 1_750_000_000_000_000_000;
    const SECOND: u64 = 1_000_000_000;

    fn sample(name: &str, app: &str, ts: u64, value: f64) -> MetricSample {
        let mut series = Labels::new();
        series.insert(METRIC_NAME_LABEL, name);
        series.insert("app", app);
        MetricSample {
            timestamp_nanos: ts,
            series,
            value,
            kind: MetricKind::Counter,
        }
    }

    fn eval(snapshot: &Snapshot, query: &str, at: u64) -> InstantVector {
        snapshot
            .eval(&crate::promql::parse(query).unwrap(), at)
            .unwrap()
            .into_vector()
    }

    fn value_for(vector: &InstantVector, app: &str) -> Option<f64> {
        vector
            .samples
            .iter()
            .find(|(labels, _)| labels.get("app") == Some(app))
            .map(|(_, value)| *value)
    }

    #[test]
    fn an_instant_selector_returns_the_latest_sample_per_series() {
        let snapshot = Snapshot::from_samples(vec![
            sample("up", "checkout", T0, 1.0),
            sample("up", "checkout", T0 + 30 * SECOND, 2.0),
            sample("up", "cart", T0, 5.0),
        ]);

        let vector = eval(&snapshot, "up", T0 + 60 * SECOND);
        assert_eq!(vector.samples.len(), 2);
        assert_eq!(value_for(&vector, "checkout"), Some(2.0));
        assert_eq!(value_for(&vector, "cart"), Some(5.0));
    }

    #[test]
    fn a_sample_older_than_the_lookback_is_stale() {
        let snapshot = Snapshot::from_samples(vec![sample("up", "checkout", T0, 1.0)]);
        // Default lookback is 5 minutes.
        assert!(eval(&snapshot, "up", T0 + 299 * SECOND).samples.len() == 1);
        assert!(eval(&snapshot, "up", T0 + 400 * SECOND).samples.is_empty());
    }

    #[test]
    fn matchers_select_series() {
        let snapshot = Snapshot::from_samples(vec![
            sample("up", "checkout", T0, 1.0),
            sample("up", "cart", T0, 2.0),
        ]);
        let vector = eval(&snapshot, r#"up{app="checkout"}"#, T0);
        assert_eq!(vector.samples.len(), 1);
        assert_eq!(value_for(&vector, "checkout"), Some(1.0));
    }

    #[test]
    fn rate_is_computed_over_the_observed_span_not_the_nominal_window() {
        // A range selector is half-open, so the sample sitting exactly on the window
        // start is excluded. Dividing by the nominal 60s here would report 0.5/sec for
        // a counter that plainly advances at 1/sec.
        let snapshot = Snapshot::from_samples(vec![
            sample("requests", "checkout", T0, 0.0),
            sample("requests", "checkout", T0 + 30 * SECOND, 30.0),
            sample("requests", "checkout", T0 + 60 * SECOND, 60.0),
        ]);

        let vector = eval(&snapshot, "rate(requests[60s])", T0 + 60 * SECOND);
        assert!(
            (value_for(&vector, "checkout").unwrap() - 1.0).abs() < 1e-9,
            "got {:?}",
            value_for(&vector, "checkout")
        );
    }

    #[test]
    fn increase_extrapolates_the_rate_across_the_window() {
        // 1/sec observed, asked for a 120s window, so 120.
        let snapshot = Snapshot::from_samples(vec![
            sample("requests", "checkout", T0, 0.0),
            sample("requests", "checkout", T0 + 30 * SECOND, 30.0),
            sample("requests", "checkout", T0 + 60 * SECOND, 60.0),
        ]);
        let vector = eval(&snapshot, "increase(requests[120s])", T0 + 60 * SECOND);
        assert!(
            (value_for(&vector, "checkout").unwrap() - 120.0).abs() < 1e-6,
            "got {:?}",
            value_for(&vector, "checkout")
        );
    }

    #[test]
    fn samples_sharing_one_timestamp_yield_no_rate() {
        // No elapsed time to divide by; inventing one would report an arbitrary rate.
        let snapshot = Snapshot::from_samples(vec![
            sample("requests", "checkout", T0, 1.0),
            sample("requests", "checkout", T0, 2.0),
        ]);
        assert!(
            eval(&snapshot, "rate(requests[60s])", T0)
                .samples
                .is_empty()
        );
    }

    #[test]
    fn a_counter_reset_is_not_a_negative_rate() {
        // A deploy resets the counter. Without reset handling this reads as a large
        // negative rate on every restart.
        let snapshot = Snapshot::from_samples(vec![
            sample("requests", "checkout", T0, 100.0),
            sample("requests", "checkout", T0 + 30 * SECOND, 150.0),
            sample("requests", "checkout", T0 + 40 * SECOND, 10.0),
            sample("requests", "checkout", T0 + 60 * SECOND, 25.0),
        ]);

        // A window wide enough to hold all four samples: +50, then the reset
        // contributes its post-reset value of 10, then +15. That is 75 over 60s.
        let rate = value_for(
            &eval(&snapshot, "rate(requests[70s])", T0 + 60 * SECOND),
            "checkout",
        )
        .unwrap();
        assert!((rate - 75.0 / 60.0).abs() < 1e-9, "got {rate}");
        assert!(rate > 0.0, "a reset must never produce a negative rate");
    }

    #[test]
    fn a_single_sample_yields_no_rate() {
        let snapshot = Snapshot::from_samples(vec![sample("requests", "checkout", T0, 5.0)]);
        assert!(
            eval(&snapshot, "rate(requests[60s])", T0)
                .samples
                .is_empty()
        );
    }

    #[test]
    fn aggregations_group_correctly() {
        let snapshot = Snapshot::from_samples(vec![
            sample("up", "checkout", T0, 1.0),
            sample("up", "cart", T0, 2.0),
            sample("up", "billing", T0, 4.0),
        ]);

        assert_eq!(eval(&snapshot, "sum(up)", T0).samples[0].1, 7.0);
        assert_eq!(eval(&snapshot, "count(up)", T0).samples[0].1, 3.0);
        assert_eq!(eval(&snapshot, "min(up)", T0).samples[0].1, 1.0);
        assert_eq!(eval(&snapshot, "max(up)", T0).samples[0].1, 4.0);
        assert!((eval(&snapshot, "avg(up)", T0).samples[0].1 - 7.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn by_and_without_partition_the_same_way() {
        let snapshot = Snapshot::from_samples(vec![
            sample("up", "checkout", T0, 1.0),
            sample("up", "checkout", T0 + SECOND, 3.0),
            sample("up", "cart", T0, 2.0),
        ]);

        let by = eval(&snapshot, "sum by (app) (up)", T0 + SECOND);
        assert_eq!(by.samples.len(), 2);
        assert_eq!(value_for(&by, "checkout"), Some(3.0));

        let without = eval(&snapshot, "sum without (nothing) (up)", T0 + SECOND);
        assert_eq!(
            without.samples.len(),
            2,
            "grouping on a label nobody has keeps them apart"
        );
    }

    #[test]
    fn scalar_arithmetic_applies_to_every_series() {
        let snapshot = Snapshot::from_samples(vec![
            sample("up", "checkout", T0, 2.0),
            sample("up", "cart", T0, 3.0),
        ]);
        let vector = eval(&snapshot, "up * 60", T0);
        assert_eq!(value_for(&vector, "checkout"), Some(120.0));
        assert_eq!(value_for(&vector, "cart"), Some(180.0));
    }

    #[test]
    fn the_uis_counter_increase_form_yields_zero_rather_than_nothing() {
        // clamp_min(sel - (sel offset 5m or sel * 0), 0)
        // With no older sample, `or sel * 0` supplies zero — without it the series
        // would simply be absent and the chart would be empty rather than flat.
        let snapshot = Snapshot::from_samples(vec![sample("requests", "checkout", T0, 42.0)]);

        let query = "clamp_min(requests - (requests offset 5m or requests * 0), 0)";
        let vector = eval(&snapshot, query, T0);

        assert_eq!(vector.samples.len(), 1, "the series must be present");
        assert_eq!(value_for(&vector, "checkout"), Some(42.0));
    }

    #[test]
    fn offset_reads_the_earlier_value() {
        let snapshot = Snapshot::from_samples(vec![
            sample("requests", "checkout", T0, 10.0),
            sample("requests", "checkout", T0 + 300 * SECOND, 50.0),
        ]);

        let now = eval(&snapshot, "requests", T0 + 300 * SECOND);
        assert_eq!(value_for(&now, "checkout"), Some(50.0));

        let earlier = eval(&snapshot, "requests offset 5m", T0 + 300 * SECOND);
        assert_eq!(value_for(&earlier, "checkout"), Some(10.0));

        // …and the difference is the increase over that window.
        let delta = eval(
            &snapshot,
            "requests - (requests offset 5m)",
            T0 + 300 * SECOND,
        );
        assert_eq!(value_for(&delta, "checkout"), Some(40.0));
    }

    #[test]
    fn clamp_min_floors_negative_values() {
        let snapshot = Snapshot::from_samples(vec![sample("gauge", "checkout", T0, -5.0)]);
        let vector = eval(&snapshot, "clamp_min(gauge, 0)", T0);
        assert_eq!(value_for(&vector, "checkout"), Some(0.0));
    }

    #[test]
    fn histogram_quantile_interpolates_between_buckets() {
        let bucket = |le: &str, count: f64| {
            let mut series = Labels::new();
            series.insert(METRIC_NAME_LABEL, "latency_bucket");
            series.insert("app", "checkout");
            series.insert("le", le);
            MetricSample {
                timestamp_nanos: T0,
                series,
                value: count,
                kind: MetricKind::Histogram,
            }
        };

        // Cumulative: 50 under 0.1, 90 under 0.5, 100 total.
        let snapshot = Snapshot::from_samples(vec![
            bucket("0.1", 50.0),
            bucket("0.5", 90.0),
            bucket("+Inf", 100.0),
        ]);

        let p50 = eval(&snapshot, "histogram_quantile(0.5, latency_bucket)", T0);
        assert!(
            (p50.samples[0].1 - 0.1).abs() < 1e-9,
            "got {:?}",
            p50.samples
        );

        // p90 lands exactly on the 0.5 bucket boundary.
        let p90 = eval(&snapshot, "histogram_quantile(0.9, latency_bucket)", T0);
        assert!(
            (p90.samples[0].1 - 0.5).abs() < 1e-9,
            "got {:?}",
            p90.samples
        );

        // The `le` label is dropped from the result, as Prometheus does.
        assert!(p50.samples[0].0.get("le").is_none());
        assert_eq!(p50.samples[0].0.get("app"), Some("checkout"));
    }

    #[test]
    fn the_metric_name_is_dropped_from_results() {
        // Once a function or aggregation has run, the value is no longer that metric.
        let snapshot = Snapshot::from_samples(vec![sample("up", "checkout", T0, 1.0)]);
        let vector = eval(&snapshot, "up", T0);
        assert!(vector.samples[0].0.get("__name__").is_none());
        assert_eq!(vector.samples[0].0.get("app"), Some("checkout"));
    }

    #[test]
    fn an_empty_snapshot_evaluates_to_an_empty_vector() {
        let snapshot = Snapshot::default();
        for query in ["up", "rate(up[5m])", "sum(up)", "clamp_min(up, 0)"] {
            assert!(eval(&snapshot, query, T0).samples.is_empty(), "{query}");
        }
    }

    #[test]
    fn vector_to_vector_arithmetic_matches_on_labels() {
        let snapshot = Snapshot::from_samples(vec![
            sample("a", "checkout", T0, 10.0),
            sample("b", "checkout", T0, 4.0),
            // No matching `b` for cart, so it drops out.
            sample("a", "cart", T0, 7.0),
        ]);

        let vector = eval(&snapshot, "a - b", T0);
        assert_eq!(vector.samples.len(), 1);
        assert_eq!(value_for(&vector, "checkout"), Some(6.0));
    }
}
