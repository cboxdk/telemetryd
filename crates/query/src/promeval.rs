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

use std::collections::HashMap;
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
///
/// # What is precomputed, and why it has to be
///
/// A range query evaluates the same expression at every step — 361 of them for six hours
/// at a minute's resolution. Three costs used to be paid inside that loop that do not
/// depend on the step at all:
///
/// - **Which series a selector matches.** Re-filtered every step: 361 x 2,886 matcher
///   evaluations for one panel, over a set that cannot change.
/// - **Stripping `__name__` from the result labels.** `Labels` shares its map, so cloning
///   is a pointer — but *removing* a key copies the map. Once per series per step.
/// - **Finding the window of a range selector.** A linear scan of the whole series,
///   collected into a fresh `Vec`: 361 x 2,886 x 360 comparisons and a million
///   allocations.
///
/// All three are now done once at load. What remains inside the loop is a binary search
/// over sorted samples and the arithmetic itself.
#[derive(Debug, Default)]
pub struct Snapshot {
    series: Vec<Series>,
    /// `series[i].labels` without `__name__`, ready to be handed to a result.
    stripped: Vec<Labels>,
    /// For each distinct selector in the expression, the series it matches.
    resolved: Vec<(Vec<telemetryd_core::LabelMatcher>, Vec<usize>)>,
    /// For each grouping the expression aggregates by, where each series lands.
    ///
    /// Keyed by [`Labels::storage_id`] rather than by the label set: every step is handed
    /// the *same* shared label set for a given series, so identity answers the question
    /// that comparing values was answering a million times over. Profiling a six-hour
    /// panel put `memcmp` at the top, and this is what it was.
    grouped: Vec<GroupIndex>,
}

/// Where every series lands under one grouping, worked out once.
#[derive(Debug)]
struct GroupIndex {
    grouping: Grouping,
    /// `storage_id` of a series' labels to the index of its group in `keys`.
    of_series: HashMap<usize, usize>,
    keys: Vec<Labels>,
}

impl Snapshot {
    /// Load everything the expression could need.
    /// `max_samples` is a ceiling to refuse at, not a top-N. Zero means unbounded, which
    /// is what the store did before this existed and what took a server down: a query with
    /// no `limit` — PromQL has no notion of one — loading every sample for every matching
    /// series across its window, three times over as it regrouped them.
    pub fn load(
        store: &RecordStore<MetricSchema>,
        expr: &Expr,
        start_nanos: u64,
        end_nanos: u64,
        max_samples: u64,
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

        // One more than allowed, so a full collector is unambiguously an overflow rather
        // than a query that happened to fit exactly.
        let ceiling = usize::try_from(max_samples.saturating_add(1)).unwrap_or(usize::MAX);
        let samples = store.query_bounded(
            from,
            end_nanos,
            &pushdown,
            &|_| true,
            if max_samples == 0 { 0 } else { ceiling },
        )?;
        if max_samples != 0 && samples.len() as u64 > max_samples {
            return Err(Error::BadRequest(format!(
                "this query would load more than {max_samples} samples into memory at \
                 once. PromQL is evaluated from a single read of the whole window, so the \
                 cost is every sample of every matching series — narrow the time range, \
                 add label matchers, or raise limits.max_query_samples"
            )));
        }

        let mut by_series: HashMap<Labels, Vec<(u64, f64)>> = HashMap::new();
        for sample in samples {
            by_series
                .entry(sample.series.clone())
                .or_default()
                .push((sample.timestamp_nanos, sample.value));
        }

        // Sorted explicitly. Grouping moved from a BTreeMap to a HashMap for speed, and
        // with it went the ordering callers had been getting for free — an unstable order
        // makes identical queries disagree and a dashboard reshuffle between refreshes.
        let mut series: Vec<Series> = by_series
            .into_iter()
            .map(|(labels, mut samples)| {
                samples.sort_by_key(|(ts, _)| *ts);
                Series { labels, samples }
            })
            .collect();
        series.sort_by(|a, b| a.labels.cmp(&b.labels));

        // Resolve every selector against the loaded series once. A query has a handful
        // of selectors; the loop inside `eval` has hundreds of steps.
        let stripped: Vec<Labels> = series.iter().map(|s| strip_name(&s.labels)).collect();
        let mut resolved: Vec<(Vec<telemetryd_core::LabelMatcher>, Vec<usize>)> = Vec::new();
        for selector in expr.selectors() {
            if resolved.iter().any(|(m, _)| *m == selector.matchers) {
                continue;
            }
            let members = series
                .iter()
                .enumerate()
                .filter(|(_, s)| telemetryd_core::matches_all(&selector.matchers, &s.labels))
                .map(|(i, _)| i)
                .collect();
            resolved.push((selector.matchers.clone(), members));
        }

        // Every grouping the expression aggregates by, resolved against the series once.
        let mut grouped: Vec<GroupIndex> = Vec::new();
        for grouping in expr.groupings() {
            if grouped.iter().any(|index| index.grouping == grouping) {
                continue;
            }
            let mut of_series = HashMap::new();
            let mut keys: Vec<Labels> = Vec::new();
            for labels in &stripped {
                let key = group_key(labels, &grouping);
                let position = keys.iter().position(|existing| *existing == key);
                let position = position.unwrap_or_else(|| {
                    keys.push(key);
                    keys.len() - 1
                });
                of_series.insert(labels.storage_id(), position);
            }
            grouped.push(GroupIndex {
                grouping,
                of_series,
                keys,
            });
        }

        Ok(Self {
            series,
            stripped,
            resolved,
            grouped,
        })
    }

    /// Build directly from samples, for testing and for the in-memory path.
    pub fn from_samples(samples: Vec<MetricSample>) -> Self {
        let mut by_series: HashMap<Labels, Vec<(u64, f64)>> = HashMap::new();
        for sample in samples {
            by_series
                .entry(sample.series.clone())
                .or_default()
                .push((sample.timestamp_nanos, sample.value));
        }
        let mut series: Vec<Series> = by_series
            .into_iter()
            .map(|(labels, mut samples)| {
                samples.sort_by_key(|(ts, _)| *ts);
                Series { labels, samples }
            })
            .collect();
        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        let stripped = series.iter().map(|s| strip_name(&s.labels)).collect();
        // No expression to resolve against here, so selectors fall back to filtering.
        // This path is the in-memory one and evaluates a handful of steps at most.
        Self {
            series,
            stripped,
            resolved: Vec::new(),
            grouped: Vec::new(),
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
                param,
                inner,
            } => {
                let vector = self.eval(inner, at_nanos)?.into_vector();
                if op.selects_elements() {
                    // `topk(k, …)`: `k` is a scalar, and a vector there is a query error
                    // rather than something to coerce — `topk(sum(x), y)` means nothing.
                    let k = match param.as_deref() {
                        Some(expr) => match self.eval(expr, at_nanos)? {
                            Value::Scalar(k) => k,
                            Value::Vector(_) => {
                                return Err(Error::BadRequest(format!(
                                    "`{}` needs a number as its first argument, like \
                                     `{}(5, …)`",
                                    op.as_str(),
                                    op.as_str()
                                )));
                            }
                        },
                        None => 0.0,
                    };
                    return Ok(Value::Vector(select_elements(*op, grouping, &vector, k)));
                }
                Ok(Value::Vector(self.aggregate(*op, grouping, &vector)))
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
        for index in self.matching(selector) {
            let series = &self.series[index];
            if let Some((_, value)) = series
                .samples
                .iter()
                .rev()
                .find(|(ts, _)| *ts <= at && *ts > floor)
            {
                samples.push((self.stripped[index].clone(), *value));
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
        for index in self.matching(selector) {
            let series = &self.series[index];
            // Samples are sorted by time, so `(floor, at]` is a contiguous slice: found by
            // two binary searches rather than a scan of the whole series, and used in
            // place rather than copied. This ran once per series per step — for a
            // six-hour panel, 361 x 2,886 scans of 360 samples each, and as many
            // allocations.
            let start = series.samples.partition_point(|(ts, _)| *ts <= floor);
            let end = series.samples.partition_point(|(ts, _)| *ts <= at);
            let window = &series.samples[start..end];

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
            samples.push((self.stripped[index].clone(), value));
        }
        InstantVector { samples }
    }

    /// Indices of the series a selector matches, resolved at load.
    ///
    /// Falls back to filtering when a selector was not seen at load — which cannot happen
    /// for an expression evaluated against its own snapshot, but a wrong answer here
    /// would be silent, and an empty result is not the kind of thing to risk on an
    /// invariant that lives in another function.
    fn matching(&self, selector: &Selector) -> Vec<usize> {
        if let Some((_, members)) = self
            .resolved
            .iter()
            .find(|(matchers, _)| *matchers == selector.matchers)
        {
            return members.clone();
        }
        self.series
            .iter()
            .enumerate()
            .filter(|(_, series)| telemetryd_core::matches_all(&selector.matchers, &series.labels))
            .map(|(i, _)| i)
            .collect()
    }
}

/// `__name__` is dropped from results, as Prometheus does once a function or
/// aggregation has been applied — the value is no longer that metric.
fn strip_name(labels: &Labels) -> Labels {
    let mut out = labels.clone();
    out.remove(telemetryd_core::METRIC_NAME_LABEL);
    out
}

/// Hash of the group a sample falls into, without building the group's label set.
///
/// `sum by (le, route)` over a range query asks this question once per sample per step —
/// on a 2,886-series histogram over 360 steps, a million times. Materialising a `Labels`
/// each time means a million `BTreeMap` allocations to answer a question whose answer is
/// almost always "the group I built on the previous sample". Hashing the selected pairs
/// directly costs no allocation, and the label set is built only when a group is new.
///
/// A hash is not an identity, so [`group_matches`] confirms the hit. That check is also
/// allocation-free: it compares the projected values in place.
fn group_hash(labels: &Labels, grouping: &Grouping) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    match grouping {
        Grouping::All => {}
        Grouping::By(names) => {
            // Sorted, so `by (a, b)` and `by (b, a)` are the same group — which they are.
            let mut selected: Vec<(&str, &str)> = names
                .iter()
                .filter_map(|name| labels.get(name).map(|value| (name.as_str(), value)))
                .collect();
            selected.sort_unstable();
            for pair in selected {
                pair.hash(&mut hasher);
            }
        }
        Grouping::Without(names) => {
            for (name, value) in labels.iter() {
                if !names.iter().any(|excluded| excluded == name) {
                    (name, value).hash(&mut hasher);
                }
            }
        }
    }
    hasher.finish()
}

/// Whether `labels` belongs to the group `key` describes, without building either.
fn group_matches(labels: &Labels, key: &Labels, grouping: &Grouping) -> bool {
    match grouping {
        Grouping::All => true,
        Grouping::By(names) => names.iter().all(|name| labels.get(name) == key.get(name)),
        Grouping::Without(names) => {
            let kept = |set: &Labels| {
                set.iter()
                    .filter(|(name, _)| !names.iter().any(|excluded| excluded == name))
                    .count()
            };
            kept(labels) == key.len()
                && labels
                    .iter()
                    .filter(|(name, _)| !names.iter().any(|excluded| excluded == name))
                    .all(|(name, value)| key.get(name) == Some(value))
        }
    }
}

/// The group key a sample falls into under `by`/`without`.
fn group_key(labels: &Labels, grouping: &Grouping) -> Labels {
    match grouping {
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
    }
}

/// `topk` and `bottomk`: keep the k best elements of each group, labels intact.
///
/// Unlike every other aggregation here, the result is a *subset of the input* rather than
/// one value per group — which is the whole point, since a panel asking for the five
/// slowest routes wants to know which five. Grouping decides within which set the
/// selection happens, not what the answer is labelled with.
///
/// NaN values are dropped rather than ordered. They are not comparable, so any ordering
/// of them is arbitrary, and a NaN sorting to the top of a "slowest routes" panel is a
/// wrong answer that looks like a real one.
fn select_elements(
    op: AggregateOp,
    grouping: &Grouping,
    vector: &InstantVector,
    k: f64,
) -> InstantVector {
    if !k.is_finite() || k < 1.0 {
        return InstantVector::default();
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let keep = k.floor().min(f64::from(u32::MAX)) as usize;

    let mut groups: HashMap<Labels, Vec<(Labels, f64)>> = HashMap::new();
    for (labels, value) in &vector.samples {
        if value.is_nan() {
            continue;
        }
        groups
            .entry(group_key(labels, grouping))
            .or_default()
            .push((labels.clone(), *value));
    }

    let mut samples = Vec::new();
    for (_, mut members) in groups {
        members.sort_by(|(a_labels, a), (b_labels, b)| {
            let ordered = match op {
                AggregateOp::BottomK => a.partial_cmp(b),
                _ => b.partial_cmp(a),
            };
            // Ties broken by label set so the answer is stable between identical calls;
            // an unstable order makes a dashboard flicker between equal values.
            ordered
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a_labels.cmp(b_labels))
        });
        members.truncate(keep);
        samples.extend(members);
    }
    // Groups come out of the map in no particular order; the selection within each group
    // is already ordered, so this only settles the groups against each other.
    samples.sort_by(|(a_labels, a), (b_labels, b)| {
        let ordered = match op {
            AggregateOp::BottomK => a.partial_cmp(b),
            _ => b.partial_cmp(a),
        };
        ordered
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a_labels.cmp(b_labels))
    });

    InstantVector { samples }
}

impl Snapshot {
    /// `sum`/`avg`/`min`/`max`/`count`, using the precomputed group index when the
    /// samples came from this snapshot's series.
    ///
    /// A sample whose label set this snapshot does not recognise — one built by a binary
    /// operation, say — falls through to grouping it the slow way. Correct either way;
    /// the index is a shortcut for the common case, not a requirement.
    fn aggregate(
        &self,
        op: AggregateOp,
        grouping: &Grouping,
        vector: &InstantVector,
    ) -> InstantVector {
        let Some(index) = self.grouped.iter().find(|g| g.grouping == *grouping) else {
            return aggregate(op, grouping, vector);
        };

        let mut values: Vec<Vec<f64>> = vec![Vec::new(); index.keys.len()];
        let mut spilled: InstantVector = InstantVector::default();
        for (labels, value) in &vector.samples {
            match index.of_series.get(&labels.storage_id()) {
                Some(&group) => values[group].push(*value),
                None => spilled.samples.push((labels.clone(), *value)),
            }
        }

        let mut samples: Vec<(Labels, f64)> = values
            .into_iter()
            .enumerate()
            .filter(|(_, group)| !group.is_empty())
            .map(|(position, group)| (index.keys[position].clone(), reduce(op, &group)))
            .collect();
        if !spilled.samples.is_empty() {
            samples.extend(aggregate(op, grouping, &spilled).samples);
        }
        samples.sort_by(|(a, _), (b, _)| a.cmp(b));
        InstantVector { samples }
    }
}

/// Reduce one group to a single value.
fn reduce(op: AggregateOp, values: &[f64]) -> f64 {
    match op {
        AggregateOp::Sum => values.iter().sum(),
        #[allow(clippy::cast_precision_loss)]
        AggregateOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
        AggregateOp::Min => values.iter().copied().fold(f64::INFINITY, f64::min),
        AggregateOp::Max => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        #[allow(clippy::cast_precision_loss)]
        AggregateOp::Count => values.len() as f64,
        // Handled by `select_elements`, which keeps each element rather than reducing a
        // group to one number.
        AggregateOp::TopK | AggregateOp::BottomK => f64::NAN,
    }
}

fn aggregate(op: AggregateOp, grouping: &Grouping, vector: &InstantVector) -> InstantVector {
    // Keyed by hash with the label set carried alongside, so the common case — a sample
    // joining a group that already exists — allocates nothing. A bucket holds a list
    // because a hash is not an identity; `group_matches` picks the right member.
    let mut groups: HashMap<u64, Vec<(Labels, Vec<f64>)>> = HashMap::new();

    for (labels, value) in &vector.samples {
        let bucket = groups.entry(group_hash(labels, grouping)).or_default();
        match bucket
            .iter_mut()
            .find(|(key, _)| group_matches(labels, key, grouping))
        {
            Some((_, values)) => values.push(*value),
            None => bucket.push((group_key(labels, grouping), vec![*value])),
        }
    }
    let groups = groups.into_values().flatten();

    let mut samples: Vec<(Labels, f64)> = groups
        .into_iter()
        .map(|(labels, values)| (labels, reduce(op, &values)))
        .collect();
    samples.sort_by(|(a, _), (b, _)| a.cmp(b));

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
    let mut histograms: HashMap<Labels, Vec<(f64, f64)>> = HashMap::new();
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

    let mut samples: Vec<(Labels, f64)> = histograms
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
    samples.sort_by(|(a, _), (b, _)| a.cmp(b));

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
#[allow(clippy::unwrap_used)]
mod group_index_tests {
    use super::*;
    use crate::promql::Grouping;
    use telemetryd_core::MetricSample;

    fn sample(route: &str, le: &str, value: f64) -> MetricSample {
        let mut labels = Labels::new();
        labels.insert("__name__", "probe");
        labels.insert("route", route);
        labels.insert("le", le);
        MetricSample {
            series: labels,
            timestamp_nanos: 1_000_000_000,
            value,
            kind: telemetryd_core::MetricKind::Gauge,
        }
    }

    /// The index is keyed by label-set identity, so anything it does not recognise has to
    /// fall through to grouping the slow way rather than being dropped. A silently
    /// missing series is the worst outcome available here.
    #[test]
    fn samples_the_index_does_not_know_are_still_grouped() {
        let snapshot = Snapshot::from_samples(vec![sample("/a", "1", 1.0), sample("/b", "1", 2.0)]);

        // `from_samples` builds no index, so this is the fallback path end to end.
        let mut foreign = Labels::new();
        foreign.insert("route", "/c");
        let vector = InstantVector {
            samples: vec![(foreign, 5.0)],
        };
        let out = snapshot.aggregate(
            AggregateOp::Sum,
            &Grouping::By(vec!["route".to_owned()]),
            &vector,
        );
        assert_eq!(out.samples.len(), 1);
        assert!((out.samples[0].1 - 5.0).abs() < f64::EPSILON);
        assert_eq!(out.samples[0].0.get("route"), Some("/c"));
    }

    /// Mixed input: some samples known to the index, some not. Both have to appear, and
    /// samples of the same group must land together whichever path they took.
    #[test]
    fn known_and_unknown_samples_end_up_in_the_same_answer() {
        let snapshot = Snapshot::from_samples(vec![sample("/a", "1", 1.0)]);
        let mut known = snapshot.stripped[0].clone();
        known.remove("le");
        let mut other = Labels::new();
        other.insert("route", "/z");

        let vector = InstantVector {
            samples: vec![(known, 3.0), (other, 4.0)],
        };
        let out = snapshot.aggregate(
            AggregateOp::Sum,
            &Grouping::By(vec!["route".to_owned()]),
            &vector,
        );
        let routes: Vec<&str> = out
            .samples
            .iter()
            .map(|(labels, _)| labels.get("route").unwrap())
            .collect();
        assert_eq!(routes, vec!["/a", "/z"]);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod grouping_tests {
    use super::*;
    use crate::promql::Grouping;

    fn labelled(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (k, v) in pairs {
            labels.insert(*k, *v);
        }
        labels
    }

    /// The hash exists to avoid building the key; it is only sound if it agrees with the
    /// key that would have been built. Same group must hash the same, different groups
    /// must (in practice) not — and `group_matches` is what makes a clash harmless.
    #[test]
    fn the_hash_agrees_with_the_key_it_replaces() {
        let by = Grouping::By(vec!["route".to_owned(), "le".to_owned()]);
        let a = labelled(&[("route", "/x"), ("le", "5"), ("pod", "one")]);
        let b = labelled(&[("route", "/x"), ("le", "5"), ("pod", "two")]);
        let c = labelled(&[("route", "/y"), ("le", "5"), ("pod", "one")]);

        assert_eq!(group_key(&a, &by), group_key(&b, &by), "same group");
        assert_eq!(group_hash(&a, &by), group_hash(&b, &by));
        assert!(group_matches(&b, &group_key(&a, &by), &by));

        assert_ne!(group_key(&a, &by), group_key(&c, &by), "different group");
        assert!(!group_matches(&c, &group_key(&a, &by), &by));
    }

    /// `by (a, b)` and `by (b, a)` are the same grouping, so they must hash alike — the
    /// projected pairs are sorted before hashing for exactly this reason.
    #[test]
    fn the_order_of_by_names_does_not_change_the_group() {
        let one = Grouping::By(vec!["a".to_owned(), "b".to_owned()]);
        let other = Grouping::By(vec!["b".to_owned(), "a".to_owned()]);
        let labels = labelled(&[("a", "1"), ("b", "2")]);
        assert_eq!(group_hash(&labels, &one), group_hash(&labels, &other));
        assert_eq!(group_key(&labels, &one), group_key(&labels, &other));
    }

    /// `without` is the awkward direction: the key is everything *except* the named
    /// labels, so a sample carrying an extra label belongs to a different group even
    /// though every label the key does have matches.
    #[test]
    fn without_grouping_distinguishes_an_extra_label() {
        let without = Grouping::Without(vec!["le".to_owned()]);
        let key = group_key(&labelled(&[("route", "/x"), ("le", "5")]), &without);

        assert!(group_matches(
            &labelled(&[("route", "/x"), ("le", "9")]),
            &key,
            &without
        ));
        assert!(
            !group_matches(
                &labelled(&[("route", "/x"), ("pod", "one"), ("le", "5")]),
                &key,
                &without
            ),
            "an extra label outside the exclusion list is a different group"
        );
        assert!(!group_matches(
            &labelled(&[("route", "/y"), ("le", "5")]),
            &key,
            &without
        ));
    }

    /// Everything collapses into one group, so anything matches it.
    #[test]
    fn grouping_over_everything_is_one_group() {
        let all = Grouping::All;
        let key = group_key(&labelled(&[("a", "1")]), &all);
        assert!(key.is_empty());
        assert!(group_matches(&labelled(&[("b", "2")]), &key, &all));
        assert_eq!(
            group_hash(&labelled(&[("a", "1")]), &all),
            group_hash(&labelled(&[("b", "2")]), &all)
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod topk_tests {
    use super::*;
    use crate::promql::Grouping;

    fn labelled(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (k, v) in pairs {
            labels.insert(*k, *v);
        }
        labels
    }

    fn vector(rows: &[(&str, f64)]) -> InstantVector {
        InstantVector {
            samples: rows
                .iter()
                .map(|(route, value)| (labelled(&[("route", route)]), *value))
                .collect(),
        }
    }

    /// The answer is a subset of the input with its own labels, not one reduced value.
    /// A panel asking for the slowest routes wants to know *which* routes.
    #[test]
    fn topk_keeps_the_largest_elements_and_their_labels() {
        let picked = select_elements(
            AggregateOp::TopK,
            &Grouping::All,
            &vector(&[("/a", 1.0), ("/b", 9.0), ("/c", 5.0)]),
            2.0,
        );

        let routes: Vec<&str> = picked
            .samples
            .iter()
            .map(|(labels, _)| labels.get("route").unwrap())
            .collect();
        assert_eq!(routes, vec!["/b", "/c"], "largest first, labels intact");
        assert!((picked.samples[0].1 - 9.0).abs() < f64::EPSILON);
    }

    #[test]
    fn bottomk_takes_the_other_end() {
        let picked = select_elements(
            AggregateOp::BottomK,
            &Grouping::All,
            &vector(&[("/a", 1.0), ("/b", 9.0), ("/c", 5.0)]),
            1.0,
        );
        assert_eq!(picked.samples.len(), 1);
        assert!((picked.samples[0].1 - 1.0).abs() < f64::EPSILON);
    }

    /// Grouping decides *within which set* the selection happens — one winner per group,
    /// not one winner overall.
    #[test]
    fn grouping_selects_within_each_group() {
        let samples = vec![
            (labelled(&[("app", "a"), ("route", "/x")]), 1.0),
            (labelled(&[("app", "a"), ("route", "/y")]), 7.0),
            (labelled(&[("app", "b"), ("route", "/z")]), 3.0),
        ];
        let picked = select_elements(
            AggregateOp::TopK,
            &Grouping::By(vec!["app".to_owned()]),
            &InstantVector { samples },
            1.0,
        );
        assert_eq!(picked.samples.len(), 2, "one per app");
        let mut values: Vec<f64> = picked.samples.iter().map(|(_, v)| *v).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert!((values[0] - 3.0).abs() < f64::EPSILON);
        assert!((values[1] - 7.0).abs() < f64::EPSILON);
    }

    /// NaN is not comparable, so ordering it is arbitrary — and a NaN at the top of a
    /// "slowest routes" panel is a wrong answer wearing the shape of a real one.
    #[test]
    fn not_a_number_is_dropped_rather_than_ordered() {
        let picked = select_elements(
            AggregateOp::TopK,
            &Grouping::All,
            &vector(&[("/a", f64::NAN), ("/b", 2.0)]),
            5.0,
        );
        assert_eq!(picked.samples.len(), 1);
        assert!((picked.samples[0].1 - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_count_below_one_selects_nothing() {
        for k in [0.0, -3.0, f64::NAN] {
            let picked = select_elements(
                AggregateOp::TopK,
                &Grouping::All,
                &vector(&[("/a", 1.0)]),
                k,
            );
            assert!(picked.samples.is_empty(), "k = {k}");
        }
    }
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
        assert_eq!(eval(&snapshot, "up", T0 + 299 * SECOND).samples.len(), 1);
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
