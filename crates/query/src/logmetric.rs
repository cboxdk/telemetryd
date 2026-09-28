//! LogQL metric queries: `rate({app="x"} |= "error" [5m])`,
//! `sum by (level) (count_over_time({app="x"}[1m]))`,
//! `quantile_over_time(0.99, {app="x"} | logfmt | unwrap duration(took) [5m]) by (route)`.
//!
//! A metric query is two languages. Its leaves — range aggregations over log streams —
//! are LogQL's own. Everything above them — aggregation by label, binary operators with
//! `on`/`ignoring`/`group_left` matching, `bool`, `topk`, `label_replace`, `vector` — is
//! PromQL's grammar with PromQL's semantics. So the leaves are parsed and evaluated here,
//! each becomes a placeholder series holding its value at every step, and the PromQL
//! evaluator answers the rest: one implementation of vector matching, not two that drift.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::{Mutex, PoisonError};

use telemetryd_core::{Error, LabelMatcher, Labels, LogRecord, MatchOp, MetricKind, Result};
use telemetryd_core::{MetricSample, metric::STALE_MARKER_BITS};
use telemetryd_store::RecordStore;
use telemetryd_store::logs::LogSchema;

use crate::lexer::{Token, tokenize};
use crate::logql::{LabelPredicate, LogQuery, Parser, Stage};
use crate::promeval::{InstantVector, Snapshot, Value};
use crate::promql::{self, AggregateOp, Expr, Function, Grouping, Selector};

/// The name a range aggregation's placeholder series carries, followed by its index.
const PLACEHOLDER: &str = "__logql_range_";

/// How many series one range aggregation, or the whole answer, may hold. A range
/// aggregation keeps every label a line carries — structured metadata included, as in
/// Loki — so `count_over_time` over lines that each carry a trace id is one series per
/// line. Refusing names the fix; answering would hold them all.
pub const MAX_SERIES: usize = 10_000;

/// How many extracted samples one query may hold before it is refused.
pub const MAX_SAMPLES: usize = 10_000_000;

/// A parsed metric query: the PromQL expression over placeholders, and what each
/// placeholder stands for.
#[derive(Debug, Clone)]
pub struct MetricQuery {
    pub expr: Expr,
    pub ranges: Vec<RangeAggregation>,
}

/// One LogQL range aggregation, `count_over_time({app="x"} |= "error" [5m])`.
#[derive(Debug, Clone)]
pub struct RangeAggregation {
    pub function: RangeFunction,
    /// The φ of `quantile_over_time`.
    pub parameter: Option<f64>,
    pub query: LogQuery,
    pub unwrap: Option<Unwrap>,
    pub range_nanos: u64,
    pub offset_nanos: u64,
    /// `by`/`without` written on the aggregation, or taken from a `sum` directly above
    /// one whose value sums across series — which is what keeps
    /// `sum by (level) (count_over_time(…))` from holding one series per trace id.
    pub grouping: Option<Grouping>,
}

/// `| unwrap took`, `| unwrap duration(took)`, `| unwrap bytes(size)`, and the label
/// filters that may follow it.
#[derive(Debug, Clone)]
pub struct Unwrap {
    pub label: String,
    pub conversion: Conversion,
    pub filters: Vec<LabelPredicate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conversion {
    Number,
    /// A Go duration, as seconds.
    Duration,
    Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFunction {
    CountOverTime,
    Rate,
    BytesOverTime,
    BytesRate,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    StdvarOverTime,
    StddevOverTime,
    QuantileOverTime,
    FirstOverTime,
    LastOverTime,
    AbsentOverTime,
    RateCounter,
}

const RANGE_FUNCTIONS: &[(&str, RangeFunction)] = &[
    ("count_over_time", RangeFunction::CountOverTime),
    ("rate", RangeFunction::Rate),
    ("bytes_over_time", RangeFunction::BytesOverTime),
    ("bytes_rate", RangeFunction::BytesRate),
    ("sum_over_time", RangeFunction::SumOverTime),
    ("avg_over_time", RangeFunction::AvgOverTime),
    ("min_over_time", RangeFunction::MinOverTime),
    ("max_over_time", RangeFunction::MaxOverTime),
    ("stdvar_over_time", RangeFunction::StdvarOverTime),
    ("stddev_over_time", RangeFunction::StddevOverTime),
    ("quantile_over_time", RangeFunction::QuantileOverTime),
    ("first_over_time", RangeFunction::FirstOverTime),
    ("last_over_time", RangeFunction::LastOverTime),
    ("absent_over_time", RangeFunction::AbsentOverTime),
    ("rate_counter", RangeFunction::RateCounter),
];

impl RangeFunction {
    fn named(name: &str) -> Option<Self> {
        RANGE_FUNCTIONS
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, f)| *f)
    }

    fn name(self) -> &'static str {
        RANGE_FUNCTIONS
            .iter()
            .find(|(_, f)| *f == self)
            .map_or("?", |(n, _)| n)
    }

    /// Whether the function reads an unwrapped value: `Some(true)` it must,
    /// `Some(false)` it must not, `None` either — `rate` counts lines or sums values.
    fn unwraps(self) -> Option<bool> {
        match self {
            Self::CountOverTime | Self::BytesOverTime | Self::BytesRate | Self::AbsentOverTime => {
                Some(false)
            }
            Self::Rate => None,
            _ => Some(true),
        }
    }

    /// Whether Loki accepts `by`/`without` on the aggregation itself.
    fn groups(self) -> bool {
        matches!(
            self,
            Self::AvgOverTime
                | Self::MinOverTime
                | Self::MaxOverTime
                | Self::StddevOverTime
                | Self::StdvarOverTime
                | Self::QuantileOverTime
                | Self::FirstOverTime
                | Self::LastOverTime
        )
    }

    /// Whether summing the function's per-series values equals applying it to the
    /// series' samples merged — the condition for grouping before extraction.
    fn sums(self) -> bool {
        matches!(
            self,
            Self::CountOverTime
                | Self::Rate
                | Self::BytesOverTime
                | Self::BytesRate
                | Self::SumOverTime
        )
    }
}

/// Whether a query is a metric query rather than a log query: a log query is a stream
/// selector and begins with one.
#[must_use]
pub fn is_metric(query: &str) -> bool {
    let query = query.trim_start();
    !query.is_empty() && !query.starts_with('{')
}

/// Parse a LogQL metric query.
///
/// # Errors
/// A `400` naming what is wrong, or `Unsupported` for PromQL that LogQL does not have.
pub fn parse(input: &str) -> Result<MetricQuery> {
    if input.contains(PLACEHOLDER) {
        return Err(Error::BadRequest(format!(
            "`{PLACEHOLDER}` is reserved; a LogQL metric query cannot name it"
        )));
    }
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(Error::BadRequest("empty LogQL query".to_owned()));
    }

    // Each range aggregation becomes a placeholder series in the text PromQL parses.
    let mut rewritten = String::with_capacity(input.len());
    let mut copied = 0;
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let function = match &tokens[i].token {
            Token::Ident(name) => RangeFunction::named(name),
            _ => None,
        };
        let Some(function) =
            function.filter(|_| tokens.get(i + 1).map(|s| &s.token) == Some(&Token::LeftParen))
        else {
            i += 1;
            continue;
        };
        let mut parser = Parser::new(input, tokens.clone());
        parser.pos = i + 2;
        let range = range_aggregation(&mut parser, function)?;
        let end = parser
            .tokens
            .get(parser.pos)
            .map_or(input.len(), |s| s.offset);
        rewritten.push_str(&input[copied..tokens[i].offset]);
        let _ = write!(rewritten, " {PLACEHOLDER}{} ", ranges.len());
        copied = end;
        i = parser.pos;
        ranges.push(range);
    }
    rewritten.push_str(&input[copied..]);

    let expr = promql::parse(&rewritten).map_err(|error| match error {
        Error::BadRequest(message) => Error::BadRequest(message.replace(&rewritten, input)),
        other => other,
    })?;
    validate(&expr, ranges.len())?;
    push_down(&expr, &mut ranges);
    Ok(MetricQuery { expr, ranges })
}

/// `count_over_time(` has been read; the rest of the call, and a `by`/`without` after it.
fn range_aggregation(p: &mut Parser, function: RangeFunction) -> Result<RangeAggregation> {
    let parameter = if function == RangeFunction::QuantileOverTime {
        let Some(Token::Number(value)) = p.peek().cloned() else {
            return Err(p.unexpected("the quantile, like 0.99, before the log range"));
        };
        p.pos += 1;
        p.expect(&Token::Comma, "`,` after the quantile")?;
        Some(value)
    } else {
        None
    };

    let wrapped = p.peek() == Some(&Token::LeftParen);
    if wrapped {
        p.pos += 1;
    }
    let matchers = p.parse_selector()?;
    // The range may come straight after the selector or after the pipeline.
    let mut range = bracketed_range(p)?;
    let mut stages = Vec::new();
    let mut unwrap: Option<Unwrap> = None;
    while starts_stage(p) {
        if p.peek() == Some(&Token::Pipe)
            && matches!(p.tokens.get(p.pos + 1).map(|s| &s.token), Some(Token::Ident(w)) if w == "unwrap")
        {
            if unwrap.is_some() {
                return Err(Error::BadRequest(
                    "a range aggregation unwraps once".to_owned(),
                ));
            }
            p.pos += 2;
            unwrap = Some(unwrap_expression(p)?);
            continue;
        }
        let stage = p.parse_stage()?;
        match (&mut unwrap, stage) {
            (None, stage) => stages.push(stage),
            (Some(unwrap), Stage::Label(predicate)) => unwrap.filters.push(predicate),
            (Some(_), _) => {
                return Err(Error::BadRequest(
                    "only label filters may follow `| unwrap`, like `| __error__=\"\"`".to_owned(),
                ));
            }
        }
    }
    if wrapped {
        p.expect(&Token::RightParen, "`)` closing the log expression")?;
    }
    if range.is_none() {
        range = bracketed_range(p)?;
    }
    let Some(range_nanos) = range else {
        return Err(p.unexpected("a range like [5m] after the log selector"));
    };
    let offset_nanos = offset(p)?;
    p.expect(&Token::RightParen, "`)` closing the range aggregation")?;
    let grouping = if matches!(p.peek(), Some(Token::Ident(w)) if w == "by" || w == "without") {
        Some(grouping(p)?)
    } else {
        None
    };

    let name = function.name();
    match (function.unwraps(), &unwrap) {
        (Some(true), None) => {
            return Err(Error::BadRequest(format!(
                "`{name}` reads a value from each line; unwrap one, like \
                 {name}({{app=\"x\"}} | logfmt | unwrap duration(took) [5m])"
            )));
        }
        (Some(false), Some(_)) => {
            return Err(Error::BadRequest(format!(
                "`{name}` counts lines and takes no `| unwrap`; use sum_over_time or rate"
            )));
        }
        _ => {}
    }
    if grouping.is_some() && !function.groups() {
        return Err(Error::BadRequest(format!(
            "grouping not allowed for {name} aggregation; group it from outside, like \
             sum by (…) ({name}(…))"
        )));
    }
    Ok(RangeAggregation {
        function,
        parameter,
        query: LogQuery { matchers, stages },
        unwrap,
        range_nanos,
        offset_nanos,
        grouping,
    })
}

fn starts_stage(p: &Parser) -> bool {
    matches!(
        p.peek(),
        Some(
            Token::Pipe
                | Token::LineContains
                | Token::LineRegex
                | Token::NotEqual
                | Token::RegexNotMatch
        )
    )
}

/// `[5m]`, if the next token opens one.
fn bracketed_range(p: &mut Parser) -> Result<Option<u64>> {
    if p.peek() != Some(&Token::LeftBracket) {
        return Ok(None);
    }
    p.pos += 1;
    let Some(Token::Duration(nanos)) = p.peek().cloned() else {
        return Err(p.unexpected("a duration like 5m inside `[…]`"));
    };
    p.pos += 1;
    p.expect(&Token::RightBracket, "`]`")?;
    if nanos == 0 {
        return Err(Error::BadRequest(
            "a range must be longer than zero".to_owned(),
        ));
    }
    Ok(Some(nanos))
}

fn offset(p: &mut Parser) -> Result<u64> {
    if !matches!(p.peek(), Some(Token::Ident(w)) if w == "offset") {
        return Ok(0);
    }
    p.pos += 1;
    let Some(Token::Duration(nanos)) = p.peek().cloned() else {
        return Err(p.unexpected("a duration after `offset`"));
    };
    p.pos += 1;
    Ok(nanos)
}

/// What follows `| unwrap`: a label, or a conversion of one.
fn unwrap_expression(p: &mut Parser) -> Result<Unwrap> {
    let name = p.expect_ident("a label to unwrap")?;
    let conversion = match name.as_str() {
        "duration" | "duration_seconds" => Some(Conversion::Duration),
        "bytes" => Some(Conversion::Bytes),
        _ => None,
    };
    let (label, conversion) = match conversion {
        Some(conversion) if p.peek() == Some(&Token::LeftParen) => {
            p.pos += 1;
            let label = p.expect_ident("the label to convert")?;
            p.expect(&Token::RightParen, "`)` after the label")?;
            (label, conversion)
        }
        _ => (name, Conversion::Number),
    };
    Ok(Unwrap {
        label,
        conversion,
        filters: Vec::new(),
    })
}

/// `by (a, b)` or `without (a)`.
fn grouping(p: &mut Parser) -> Result<Grouping> {
    let keyword = p.expect_ident("`by` or `without`")?;
    p.expect(&Token::LeftParen, "`(` after the grouping keyword")?;
    let mut labels = Vec::new();
    while p.peek() != Some(&Token::RightParen) {
        labels.push(p.expect_ident("a label name")?);
        if p.peek() == Some(&Token::Comma) {
            p.pos += 1;
        }
    }
    p.pos += 1;
    Ok(if keyword == "by" {
        Grouping::By(labels)
    } else {
        Grouping::Without(labels)
    })
}

/// The index a placeholder selector stands for.
fn placeholder(selector: &Selector) -> Option<usize> {
    let [matcher] = selector.matchers.as_slice() else {
        return None;
    };
    if selector.range.is_some()
        || selector.at.is_some()
        || selector.offset != promql::Offset::default()
        || matcher.name != "__name__"
        || matcher.op != MatchOp::Equal
    {
        return None;
    }
    matcher.value.strip_prefix(PLACEHOLDER)?.parse().ok()
}

/// Refuse the PromQL that LogQL does not have: series selectors, subqueries, and the
/// functions and aggregations outside LogQL's set.
fn validate(expr: &Expr, ranges: usize) -> Result<()> {
    match expr {
        Expr::Number(_) | Expr::String(_) => Ok(()),
        Expr::Selector(selector) => match placeholder(selector) {
            Some(index) if index < ranges => Ok(()),
            _ => Err(Error::BadRequest(
                "a LogQL metric query aggregates log streams, like \
                 rate({app=\"x\"}[5m]); a bare series name is PromQL — ask the \
                 Prometheus API"
                    .to_owned(),
            )),
        },
        Expr::Call { function, args } => {
            if !matches!(
                function,
                Function::Vector | Function::LabelReplace | Function::Sort | Function::SortDesc
            ) {
                let name = promql::FUNCTIONS
                    .iter()
                    .find(|(_, f)| f == function)
                    .map_or("?", |(n, _)| n);
                return Err(Error::unsupported_with_hint(
                    format!("`{name}` in LogQL"),
                    "it is a PromQL function; LogQL has vector, label_replace, sort and \
                     sort_desc",
                ));
            }
            args.iter().try_for_each(|arg| validate(arg, ranges))
        }
        Expr::Aggregation {
            op, param, inner, ..
        } => {
            if !matches!(
                op,
                AggregateOp::Sum
                    | AggregateOp::Avg
                    | AggregateOp::Min
                    | AggregateOp::Max
                    | AggregateOp::Count
                    | AggregateOp::TopK
                    | AggregateOp::BottomK
                    | AggregateOp::Stddev
                    | AggregateOp::Stdvar
            ) {
                return Err(Error::unsupported(format!(
                    "the `{op:?}` aggregation in LogQL"
                )));
            }
            if let Some(param) = param {
                validate(param, ranges)?;
            }
            validate(inner, ranges)
        }
        Expr::Binary { left, right, .. } => {
            validate(left, ranges)?;
            validate(right, ranges)
        }
        Expr::Negate(inner) => validate(inner, ranges),
        Expr::Subquery(_) => Err(Error::unsupported("subqueries in LogQL")),
    }
}

/// Give a `sum` directly over a summing range aggregation its grouping, so the
/// aggregation groups as it extracts instead of holding every series it would sum away.
fn push_down(expr: &Expr, ranges: &mut [RangeAggregation]) {
    expr.walk(&mut |node| {
        if let Expr::Aggregation {
            op: AggregateOp::Sum,
            grouping,
            param: None,
            inner,
        } = node
            && let Expr::Selector(selector) = inner.as_ref()
            && let Some(range) = placeholder(selector).and_then(|i| ranges.get_mut(i))
            && range.grouping.is_none()
            && range.function.sums()
        {
            range.grouping = Some(grouping.clone());
        }
    });
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// What one range aggregation extracted: each series and its samples in time order.
type Extracted = Vec<(Labels, Vec<(u64, f64)>)>;

#[derive(Default)]
struct Collector {
    index: HashMap<Labels, usize>,
    series: Extracted,
    samples: usize,
    lines: u64,
    refused: Option<Error>,
}

impl Collector {
    fn push(&mut self, labels: Labels, timestamp: u64, value: f64) {
        if self.refused.is_some() {
            return;
        }
        let next = self.series.len();
        let slot = *self.index.entry(labels.clone()).or_insert(next);
        if slot == next {
            if next >= MAX_SERIES {
                self.refused = Some(too_many_series());
                return;
            }
            self.series.push((labels, Vec::new()));
        }
        self.series[slot].1.push((timestamp, value));
        self.samples += 1;
        if self.samples > MAX_SAMPLES {
            self.refused = Some(Error::BadRequest(format!(
                "this query extracts more than {MAX_SAMPLES} samples; narrow the range or \
                 add a line filter"
            )));
        }
    }
}

fn too_many_series() -> Error {
    Error::BadRequest(format!(
        "maximum of series ({MAX_SERIES}) reached for a single query; aggregate with \
         sum by (…) — every label a line carries, structured metadata included, makes \
         its own series"
    ))
}

/// A metric query's value at each step, and how many lines were read to get it.
///
/// # Errors
/// A `400` for a pipeline error in a series, or a query over the series or sample limits.
pub fn evaluate(
    store: &RecordStore<LogSchema>,
    query: &MetricQuery,
    steps: &[u64],
) -> Result<(Vec<Value>, u64)> {
    let mut samples = Vec::new();
    let mut lines = 0;
    for (index, range) in query.ranges.iter().enumerate() {
        let (series, read) = range.extract(store, steps)?;
        lines += read;
        pipeline_error(&series)?;
        range.emit(index, &series, steps, &mut samples)?;
    }
    let mut snapshot = Snapshot::from_samples(samples);
    snapshot.prepare(&query.expr, steps);
    let values = steps
        .iter()
        .map(|&at| snapshot.eval(&query.expr, at).map(without_placeholders))
        .collect::<Result<Vec<_>>>()?;
    Ok((values, lines))
}

fn without_placeholders(value: Value) -> Value {
    match value {
        Value::Scalar(value) => Value::Scalar(value),
        Value::Vector(vector) => Value::Vector(InstantVector {
            samples: vector
                .samples
                .into_iter()
                .map(|(mut labels, value)| {
                    if labels
                        .get("__name__")
                        .is_some_and(|n| n.starts_with(PLACEHOLDER))
                    {
                        labels.remove("__name__");
                    }
                    (labels, value)
                })
                .collect(),
        }),
    }
}

/// Loki refuses a metric over lines its pipeline could not process, rather than count
/// them silently; the message is Loki's, so the fix it names is the same.
fn pipeline_error(series: &Extracted) -> Result<()> {
    let Some((labels, _)) = series
        .iter()
        .find(|(labels, _)| labels.get("__error__").is_some_and(|e| !e.is_empty()))
    else {
        return Ok(());
    };
    let error = labels.get("__error__").unwrap_or_default();
    let shown = labels
        .iter()
        .map(|(k, v)| format!("{k}={v:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    Err(Error::BadRequest(format!(
        "pipeline error: '{error}' for series: '{{{shown}}}'. Use a label filter to \
         intentionally skip this error. (e.g | __error__!=\"{error}\"). To skip all \
         potential errors you can match empty errors.(e.g __error__=\"\") The label filter \
         can also be specified after unwrap. (e.g | unwrap latency | __error__=\"\" )"
    )))
}

impl RangeAggregation {
    /// The window a step reads: `(floor, top]`, offset applied.
    fn window(&self, at: u64) -> (u64, u64) {
        let top = at.saturating_sub(self.offset_nanos);
        (top.saturating_sub(self.range_nanos), top)
    }

    /// Read every line any step's window covers, once, into series.
    fn extract(&self, store: &RecordStore<LogSchema>, steps: &[u64]) -> Result<(Extracted, u64)> {
        let (Some(&first), Some(&last)) = (steps.first(), steps.last()) else {
            return Ok((Vec::new(), 0));
        };
        let (floor, _) = self.window(first);
        let (_, top) = self.window(last);
        let collector = Mutex::new(Collector::default());
        // The predicate is the visitor: it records the line and declines it, so the scan
        // holds nothing and memory follows the series, not the lines.
        let visit = |record: &LogRecord| {
            let sample = self.sample(record);
            let mut collector = collector.lock().unwrap_or_else(PoisonError::into_inner);
            collector.lines += 1;
            if let Some((labels, value)) = sample {
                collector.push(labels, record.timestamp_nanos, value);
            }
            false
        };
        let prefilter = crate::loki::build_line_prefilter(&self.query);
        let mut scan = telemetryd_store::Scan::range(floor.saturating_add(1), top);
        if let Some(prefilter) = prefilter.as_ref() {
            scan = scan.columns(prefilter);
        }
        if let Some(required) = self.query.required_substring() {
            scan = scan.required_text(required);
        }
        store.scan(scan, &self.query.matchers, &visit)?;

        let collector = collector
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(error) = collector.refused {
            return Err(error);
        }
        let mut series = collector.series;
        for (_, samples) in &mut series {
            samples.sort_by_key(|(timestamp, _)| *timestamp);
        }
        Ok((series, collector.lines))
    }

    /// A line's series and value, or `None` if the pipeline drops it.
    fn sample(&self, record: &LogRecord) -> Option<(Labels, f64)> {
        let base = record.filter_labels();
        let (bytes, mut labels) = if self.query.has_parser_stage() {
            let processed = self.query.process(&record.body, &base, &record.stream)?;
            (processed.line.len(), processed.labels)
        } else {
            if !self.query.evaluate(&record.body, &base, &record.stream) {
                return None;
            }
            (record.body.len(), base)
        };
        #[allow(clippy::cast_precision_loss)]
        let value = match &self.unwrap {
            Some(unwrap) => {
                let value = match unwrap.read(&mut labels) {
                    Some(value) => value,
                    // Loki's `avg_over_time(…) by (…)` is a sum over a count, and the
                    // count reads no label: a line without it lowers the average, and one
                    // its pipeline could not parse refuses the query. Loki answers so.
                    None if self.function == RangeFunction::AvgOverTime
                        && self.grouping.is_some() =>
                    {
                        MISSING
                    }
                    // Without the label there is no sample, as in Loki.
                    None => return None,
                };
                if !unwrap.filters.iter().all(|f| f.apply(&mut labels)) {
                    return None;
                }
                value
            }
            None if matches!(
                self.function,
                RangeFunction::BytesOverTime | RangeFunction::BytesRate
            ) =>
            {
                bytes as f64
            }
            None => 1.0,
        };
        Some((self.series_labels(&labels), value))
    }

    /// The labels a line's sample is kept under: every label it carries that is a valid
    /// series label, grouped when the aggregation groups — except that an error survives
    /// grouping, so it can still refuse the query.
    fn series_labels(&self, labels: &Labels) -> Labels {
        let failed = labels.get("__error__").is_some_and(|e| !e.is_empty());
        labels
            .iter()
            .filter(|(name, value)| !value.is_empty() && is_label_name(name))
            .filter(|(name, _)| match &self.grouping {
                None => true,
                // A `sum` with no labels at all keeps none — not even an error, which is
                // why Loki answers `sum(count_over_time({…} | json [5m]))` over lines
                // that did not parse, and refuses the same thing `by` a label.
                Some(Grouping::All) => false,
                Some(_) if failed && name.starts_with("__error") => true,
                Some(Grouping::By(names)) => names.iter().any(|n| n == name),
                Some(Grouping::Without(names)) => !names.iter().any(|n| n == name),
            })
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect()
    }

    /// Each series' value at each step, as placeholder samples — with a staleness marker
    /// where a series stops, so the next step does not look back and find it again.
    fn emit(
        &self,
        index: usize,
        series: &Extracted,
        steps: &[u64],
        out: &mut Vec<MetricSample>,
    ) -> Result<()> {
        let name = format!("{PLACEHOLDER}{index}");
        let sample = |labels: &Labels, at: u64, value: f64| MetricSample {
            timestamp_nanos: at,
            series: labels.clone(),
            value,
            kind: MetricKind::Gauge,
        };
        if self.function == RangeFunction::AbsentOverTime {
            let mut labels = absent_labels(&self.query.matchers);
            labels.insert("__name__", name);
            let mut present = false;
            for &at in steps {
                let (floor, top) = self.window(at);
                if series
                    .iter()
                    .all(|(_, s)| in_window(s, floor, top).is_empty())
                {
                    out.push(sample(&labels, at, 1.0));
                    present = true;
                } else if present {
                    out.push(sample(&labels, at, f64::from_bits(STALE_MARKER_BITS)));
                    present = false;
                }
            }
            return Ok(());
        }
        for (labels, samples) in series {
            let mut labels = labels.clone();
            labels.insert("__name__", name.clone());
            let mut present = false;
            for &at in steps {
                let (floor, top) = self.window(at);
                match self.value(in_window(samples, floor, top), floor, top) {
                    Some(value) => {
                        out.push(sample(&labels, at, value));
                        present = true;
                    }
                    None if present => {
                        out.push(sample(&labels, at, f64::from_bits(STALE_MARKER_BITS)));
                        present = false;
                    }
                    None => {}
                }
            }
            if out.len() > MAX_SAMPLES {
                return Err(Error::BadRequest(format!(
                    "this query would hold more than {MAX_SAMPLES} points; widen `step`, \
                     narrow the range, or aggregate with sum by (…)"
                )));
            }
        }
        Ok(())
    }

    /// The function over one window's samples, as Loki computes it.
    #[allow(clippy::cast_precision_loss)]
    fn value(&self, window: &[(u64, f64)], floor: u64, top: u64) -> Option<f64> {
        let (first, last) = (window.first()?, window.last()?);
        let seconds = self.range_nanos as f64 / 1e9;
        let count = window.len() as f64;
        let sum = || window.iter().map(|(_, v)| v).sum::<f64>();
        Some(match self.function {
            RangeFunction::CountOverTime => count,
            RangeFunction::Rate if self.unwrap.is_none() => count / seconds,
            RangeFunction::Rate | RangeFunction::BytesRate => sum() / seconds,
            RangeFunction::BytesOverTime | RangeFunction::SumOverTime => sum(),
            RangeFunction::AvgOverTime if self.grouping.is_some() => {
                // Loki's sum over count; see `sample`.
                let total: f64 = window
                    .iter()
                    .filter(|(_, v)| !is_missing(*v))
                    .map(|(_, v)| v)
                    .sum();
                total / count
            }
            RangeFunction::AvgOverTime => {
                let mut mean = 0.0;
                for (n, (_, v)) in window.iter().enumerate() {
                    mean += (v - mean) / (n + 1) as f64;
                }
                mean
            }
            RangeFunction::MinOverTime => extreme(window, |v, m| v < m),
            RangeFunction::MaxOverTime => extreme(window, |v, m| v > m),
            RangeFunction::StdvarOverTime => variance(window),
            RangeFunction::StddevOverTime => variance(window).sqrt(),
            RangeFunction::QuantileOverTime => {
                let mut values: Vec<f64> = window.iter().map(|(_, v)| *v).collect();
                crate::promfn::quantile(self.parameter.unwrap_or(f64::NAN), &mut values)
            }
            RangeFunction::FirstOverTime => first.1,
            RangeFunction::LastOverTime => last.1,
            RangeFunction::RateCounter => return crate::promeval::counter_rate(window, floor, top),
            RangeFunction::AbsentOverTime => 1.0,
        })
    }
}

impl Unwrap {
    /// The unwrapped value, taking the label out of the series: `None` when the line has
    /// no such label. A value that does not read marks the line `SampleExtractionErr`,
    /// as Loki does.
    fn read(&self, labels: &mut Labels) -> Option<f64> {
        let raw = labels.remove(&self.label)?;
        let parsed = match self.conversion {
            Conversion::Number => raw.trim().parse::<f64>().ok(),
            Conversion::Duration => crate::logstage::parse_duration(&raw),
            Conversion::Bytes => crate::logstage::parse_bytes(&raw),
        };
        Some(parsed.unwrap_or_else(|| {
            if labels.get("__error__").is_none_or(str::is_empty) {
                labels.insert("__error__", "SampleExtractionErr");
                // Go's words, as Loki reports them.
                labels.insert(
                    "__error_details__",
                    match self.conversion {
                        Conversion::Duration => format!("time: invalid duration {raw:?}"),
                        _ => format!("strconv.ParseFloat: parsing {raw:?}: invalid syntax"),
                    },
                );
            }
            0.0
        }))
    }
}

/// A line `avg_over_time(…) by (…)` counts without a value; see `sample`. A NaN no
/// parse produces, so a real NaN in the data stays a value.
const MISSING: f64 = f64::from_bits(0x7ff8_0000_dead_beef);

fn is_missing(value: f64) -> bool {
    value.to_bits() == MISSING.to_bits()
}

/// The samples in `(floor, top]`.
fn in_window(samples: &[(u64, f64)], floor: u64, top: u64) -> &[(u64, f64)] {
    let start = samples.partition_point(|(t, _)| *t <= floor);
    let end = samples.partition_point(|(t, _)| *t <= top);
    &samples[start..end.max(start)]
}

fn extreme(window: &[(u64, f64)], better: impl Fn(f64, f64) -> bool) -> f64 {
    window.iter().fold(
        f64::NAN,
        |m, (_, v)| {
            if m.is_nan() || better(*v, m) { *v } else { m }
        },
    )
}

#[allow(clippy::cast_precision_loss)]
fn variance(window: &[(u64, f64)]) -> f64 {
    let (mut mean, mut aux) = (0.0, 0.0);
    for (n, (_, v)) in window.iter().enumerate() {
        let delta = v - mean;
        mean += delta / (n + 1) as f64;
        aux += delta * (v - mean);
    }
    aux / window.len() as f64
}

/// The labels `absent_over_time` answers with: the selector's equality matchers, where a
/// name has only one.
fn absent_labels(matchers: &[LabelMatcher]) -> Labels {
    matchers
        .iter()
        .filter(|m| m.op == MatchOp::Equal)
        .filter(|m| matchers.iter().filter(|o| o.name == m.name).count() == 1)
        .map(|m| (m.name.clone(), m.value.clone()))
        .collect()
}

fn is_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// Loki's answer to a metric `query_range`: a matrix, or a scalar drawn as a series.
///
/// # Errors
/// As [`evaluate`], and for an answer over [`MAX_SERIES`].
pub fn answer_range(
    store: &RecordStore<LogSchema>,
    query: &MetricQuery,
    steps: &[u64],
) -> Result<serde_json::Value> {
    let started = std::time::Instant::now();
    let (values, lines) = evaluate(store, query, steps)?;
    let mut series: HashMap<Labels, Vec<(f64, String)>> = HashMap::new();
    let mut points = 0u64;
    for (&at, value) in steps.iter().zip(values) {
        let samples = match value {
            Value::Vector(vector) => vector.samples,
            Value::Scalar(value) => vec![(Labels::new(), value)],
        };
        for (labels, value) in samples {
            points += 1;
            series.entry(labels).or_default().push((
                crate::prometheus::to_seconds(at),
                crate::prometheus::format_value(value),
            ));
        }
        if series.len() > MAX_SERIES {
            return Err(too_many_series());
        }
    }
    let mut series: Vec<_> = series.into_iter().collect();
    series.sort_by(|(a, _), (b, _)| a.cmp(b));
    let result: Vec<_> = series
        .into_iter()
        .map(|(labels, values)| serde_json::json!({ "metric": labels, "values": values }))
        .collect();
    Ok(success(
        "matrix",
        &serde_json::Value::Array(result),
        lines,
        points,
        started,
    ))
}

/// Loki's answer to a metric instant query: a vector, or a scalar.
///
/// # Errors
/// As [`evaluate`].
pub fn answer_instant(
    store: &RecordStore<LogSchema>,
    query: &MetricQuery,
    at: u64,
) -> Result<serde_json::Value> {
    use crate::prometheus::{format_value, to_seconds};
    let started = std::time::Instant::now();
    let (mut values, lines) = evaluate(store, query, &[at])?;
    let (kind, result, points) = match values.pop() {
        Some(Value::Scalar(value)) => (
            "scalar",
            serde_json::json!([to_seconds(at), format_value(value)]),
            1,
        ),
        Some(Value::Vector(vector)) => {
            let points = vector.samples.len() as u64;
            let mut samples = vector.samples;
            samples.sort_by(|(a, _), (b, _)| a.cmp(b));
            let result: Vec<_> = samples
                .into_iter()
                .map(|(labels, value)| {
                    serde_json::json!({
                        "metric": labels,
                        "value": [to_seconds(at), format_value(value)],
                    })
                })
                .collect();
            ("vector", serde_json::Value::Array(result), points)
        }
        None => ("vector", serde_json::json!([]), 0),
    };
    Ok(success(kind, &result, lines, points, started))
}

fn success(
    kind: &str,
    result: &serde_json::Value,
    lines: u64,
    points: u64,
    started: std::time::Instant,
) -> serde_json::Value {
    serde_json::json!({
        "status": "success",
        "data": {
            "resultType": kind,
            "result": result,
            "stats": { "summary": {
                "totalLinesProcessed": lines,
                "totalEntriesReturned": points,
                "execTime": started.elapsed().as_secs_f64(),
            }},
        },
    })
}

/// Loki's default step for a range: a 250th of it, at least a second.
#[must_use]
pub fn default_step_nanos(start_nanos: u64, end_nanos: u64) -> u64 {
    let seconds = (end_nanos.saturating_sub(start_nanos) / 1_000_000_000) / 250;
    seconds.max(1) * 1_000_000_000
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn a_log_query_is_not_a_metric_query() {
        assert!(!is_metric(r#"{app="x"} |= "a""#));
        assert!(!is_metric(r#"  {app="x"}"#));
        assert!(is_metric(r#"rate({app="x"}[5m])"#));
        assert!(is_metric("vector(1)+vector(1)"));
    }

    #[test]
    fn the_range_may_follow_the_selector_or_the_pipeline() {
        for query in [
            r#"count_over_time({app="x"} |= "error" [5m])"#,
            r#"count_over_time({app="x"}[5m] |= "error")"#,
            r#"count_over_time(({app="x"} |= "error")[5m])"#,
        ] {
            let parsed = parse(query).unwrap();
            let [range] = parsed.ranges.as_slice() else {
                panic!("{query}");
            };
            assert_eq!(range.range_nanos, 300_000_000_000, "{query}");
            assert_eq!(range.query.stages.len(), 1, "{query}");
        }
    }

    #[test]
    fn unwrap_reads_conversions_and_trailing_filters() {
        let parsed = parse(
            r#"quantile_over_time(0.99, {app="x"} | logfmt | unwrap duration_seconds(took) | __error__="" [5m] offset 1h) by (route)"#,
        )
        .unwrap();
        let range = &parsed.ranges[0];
        assert_eq!(range.parameter, Some(0.99));
        assert_eq!(range.offset_nanos, 3_600_000_000_000);
        let unwrap = range.unwrap.as_ref().unwrap();
        assert_eq!(unwrap.label, "took");
        assert_eq!(unwrap.conversion, Conversion::Duration);
        assert_eq!(unwrap.filters.len(), 1);
        assert!(matches!(&range.grouping, Some(Grouping::By(l)) if l == &["route"]));
    }

    #[test]
    fn a_sum_groups_what_it_sums_but_not_what_it_does_not() {
        let parsed = parse(r#"sum by (level) (count_over_time({app="x"}[1m]))"#).unwrap();
        assert!(matches!(&parsed.ranges[0].grouping, Some(Grouping::By(l)) if l == &["level"]));
        // max_over_time's per-series maxima do not sum.
        let parsed =
            parse(r#"sum by (level) (max_over_time({app="x"} | logfmt | unwrap took [1m]))"#)
                .unwrap();
        assert!(parsed.ranges[0].grouping.is_none());
        // Nor does a count of series.
        let parsed = parse(r#"count(count_over_time({app="x"}[1m]))"#).unwrap();
        assert!(parsed.ranges[0].grouping.is_none());
    }

    #[test]
    fn windows_are_left_open() {
        let samples = [(10, 1.0), (20, 2.0), (30, 3.0)];
        assert_eq!(in_window(&samples, 10, 30), &samples[1..]);
        assert_eq!(in_window(&samples, 0, 10), &samples[..1]);
        assert!(in_window(&samples, 30, 40).is_empty());
    }

    #[test]
    fn the_placeholder_cannot_be_named() {
        assert!(parse("sum(__logql_range_0)").is_err());
    }
}
