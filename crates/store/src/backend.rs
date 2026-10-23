//! What the query engines need from a store, whichever store it is.
//!
//! The engines used to take the local [`RecordStore`] by type. With ClickHouse as an
//! alternative they take these traits instead, and each backend answers them its own
//! way: the local store from segments on disk, ClickHouse with SQL. The contract is the
//! local store's — same records, same order, same limits, same errors — so an answer
//! does not depend on which one is configured.

use telemetryd_core::span::{SpanKind, SpanRecord, SpanStatus};
use telemetryd_core::{LabelMatcher, Labels, LogRecord, Result};

use crate::logs::LogSchema;
use crate::records::{RecordStore, Scan};
use crate::spans::SpanSchema;

/// A record predicate, applied after the store's own selection.
pub type RecordFilter<'a, R> = &'a (dyn Fn(&R) -> bool + Sync);

/// A line filter on the stored body, as the query wrote it: `|= "text"` or
/// `!= "text"`.
///
/// Carried down as data so a backend can turn it into its own condition — the local
/// store has the Arrow prefilter for the same thing, ClickHouse a `LIKE`. Like that
/// prefilter it may select too much, never too little: the record predicate still runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyFilter {
    pub contains: bool,
    pub text: String,
}

/// Lines and bytes per stream group per time bucket: what `count_over_time`, `rate`,
/// `bytes_over_time` and `bytes_rate` need, without reading the lines.
///
/// Buckets are `(anchor + k·width, anchor + (k+1)·width]`, the same half-open shape as
/// a LogQL window, so a window whose bounds fall on bucket bounds holds exactly the
/// lines of the buckets inside it.
#[derive(Debug, Clone)]
pub struct BucketRequest<'a> {
    /// Lines after this, exclusive — also the grid's origin.
    pub anchor_nanos: u64,
    /// Lines up to this, inclusive.
    pub until_nanos: u64,
    pub width_nanos: u64,
    pub matchers: &'a [LabelMatcher],
    pub body_filters: &'a [BodyFilter],
    /// Stream labels to group by; empty for one group of everything.
    pub group_by: &'a [String],
    /// Whether bytes are wanted as well as lines; reading the bodies is most of the
    /// cost of counting them.
    pub bytes: bool,
}

/// One group's `(bucket, lines, bytes)`, in no particular order.
pub type BucketCounts = Vec<(u64, u64, u64)>;

/// What a store counted.
#[derive(Debug, Default)]
pub struct Buckets {
    /// Per group, its `group_by` values and `(bucket, lines, bytes)`.
    pub groups: Vec<(Vec<String>, BucketCounts)>,
    /// Lines whose group the store could not tell from its stream labels alone — an
    /// attribute that would stand in for a grouped label, or a row with no stream — to
    /// be grouped by the caller, one by one, as if they had been scanned.
    pub individual: Vec<LogRecord>,
    /// Lines counted, for the response's statistics.
    pub lines: u64,
}

/// Logs, as the Loki API and LogQL read them.
pub trait LogStore: Send + Sync {
    /// Count lines into buckets in the store, if it can; `None` when it cannot and the
    /// caller should scan. The local store cannot: its segments are read either way.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn buckets(&self, request: &BucketRequest<'_>) -> Result<Option<Buckets>> {
        let _ = request;
        Ok(None)
    }

    /// Records in the scan's range whose stream matches, that `extra` accepts, in the
    /// scan's order and within its limit.
    ///
    /// # Errors
    /// As [`RecordStore::scan`]: a ceiling exceeded, or a store that cannot be read.
    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: RecordFilter<'_, LogRecord>,
    ) -> Result<Vec<LogRecord>>;

    /// Distinct stream label names with data in the range.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn label_names(&self, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>>;

    /// Distinct values of one stream label in the range.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn label_values(&self, name: &str, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>>;

    /// Distinct streams in the range that match.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn streams(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>>;
}

impl LogStore for RecordStore<LogSchema> {
    fn scan(
        &self,
        request: Scan<'_>,
        matchers: &[LabelMatcher],
        extra: RecordFilter<'_, LogRecord>,
    ) -> Result<Vec<LogRecord>> {
        Self::scan(self, request, matchers, extra)
    }

    fn label_names(&self, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>> {
        Ok(Self::label_names(self, start_nanos, end_nanos))
    }

    fn label_values(&self, name: &str, start_nanos: u64, end_nanos: u64) -> Result<Vec<String>> {
        Self::label_values(self, name, start_nanos, end_nanos)
    }

    fn streams(
        &self,
        start_nanos: u64,
        end_nanos: u64,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<Labels>> {
        Self::streams(self, start_nanos, end_nanos, matchers)
    }
}

/// What a TraceQL condition reads, as data a store can turn into its own conditions.
#[derive(Debug, Clone, PartialEq)]
pub enum SpanField {
    Name,
    Status,
    StatusMessage,
    Kind,
    Duration,
    /// `span.x`: the span's attributes, which hold its resource's under `resource.`.
    Span(String),
    /// `resource.x`: a stream label, or a resource attribute.
    Resource(String),
    /// `.x`: either.
    Unscoped(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanOp {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SpanValue {
    Text(String),
    Number(f64),
    Nanos(u64),
    Status(SpanStatus),
    Kind(SpanKind),
    Nil,
}

/// One condition every span a query wants must meet.
///
/// Like a [`BodyFilter`] it is a hint: a store may select by it, too loosely if it must,
/// and the query's own predicate still decides each span.
#[derive(Debug, Clone, PartialEq)]
pub struct SpanCondition {
    pub field: SpanField,
    pub op: SpanOp,
    pub value: SpanValue,
}

/// What a TraceQL metrics query aggregates per step: spans counted, or their duration.
#[derive(Debug, Clone, PartialEq)]
pub enum SpanAggregation {
    /// `rate`, `count_over_time`.
    Count,
    /// `sum_`, `avg_`, `min_`, `max_over_time(duration)`: count, sum, min and max of the
    /// duration in seconds.
    Duration,
    /// `quantile_over_time(duration, …)`, by Prometheus's method.
    Quantiles(Vec<f64>),
    /// `histogram_over_time(duration)`: spans per power-of-two upper bound in seconds.
    Histogram,
}

/// A TraceQL metrics query, for a store that can aggregate it itself.
#[derive(Debug, Clone)]
pub struct SpanAggregateRequest<'a> {
    /// Spans starting from here, inclusive…
    pub start_nanos: u64,
    /// …to here, exclusive.
    pub end_nanos: u64,
    pub step_nanos: u64,
    /// A span past the last step's end counts in the last step, as the caller counts it.
    pub steps: usize,
    pub conditions: &'a [SpanCondition],
    /// What the series are keyed by, in order; a span without one of them has no value
    /// for it.
    pub group_by: &'a [SpanField],
    pub aggregation: &'a SpanAggregation,
}

/// One group's aggregate in one step.
#[derive(Debug, Clone, Default)]
pub struct SpanAggregateRow {
    pub key: Vec<Option<String>>,
    pub step: usize,
    pub count: u64,
    /// Seconds, for [`SpanAggregation::Duration`].
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub quantiles: Vec<f64>,
    /// `(upper bound's f64 bits, spans)`.
    pub histogram: Vec<(u64, u64)>,
}

/// What a store aggregated, and the spans it could not decide on, for the caller to
/// evaluate and add.
#[derive(Debug, Default)]
pub struct SpanAggregates {
    pub rows: Vec<SpanAggregateRow>,
    pub individual: Vec<SpanRecord>,
}

/// Spans, as the Tempo API and TraceQL read them.
pub trait SpanStore: Send + Sync {
    /// Aggregate a TraceQL metrics query in the store, if it can; `None` to scan.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn aggregate(&self, request: &SpanAggregateRequest<'_>) -> Result<Option<SpanAggregates>> {
        let _ = request;
        Ok(None)
    }

    /// Spans in the scan's range, that `extra` accepts, in its order and within its
    /// limit. `conditions` are what `extra` requires, for a store that can select by
    /// them.
    ///
    /// # Errors
    /// As [`RecordStore::scan`].
    fn scan(
        &self,
        request: Scan<'_>,
        conditions: &[SpanCondition],
        extra: RecordFilter<'_, SpanRecord>,
    ) -> Result<Vec<SpanRecord>>;

    /// Every span of one trace, whenever it happened.
    ///
    /// # Errors
    /// When the store cannot be read.
    fn trace(&self, trace_id: &str) -> Result<Vec<SpanRecord>>;

    /// The spans of these traces within the scan.
    ///
    /// # Errors
    /// As [`Self::scan`].
    fn of_traces(&self, trace_ids: &[&str], request: Scan<'_>) -> Result<Vec<SpanRecord>>;

    /// The traces a search considers: those with a span in the scan that `extra`
    /// accepts. Among them, at least the `want` that started latest, where there are that
    /// many; a store may return more, and the caller ranks them.
    ///
    /// The default reads the scan's spans and returns every trace among them, which is
    /// what the local store does: its scan stops at the scan's limit.
    ///
    /// # Errors
    /// As [`Self::scan`].
    fn search_traces(
        &self,
        request: Scan<'_>,
        conditions: &[SpanCondition],
        extra: RecordFilter<'_, SpanRecord>,
        want: usize,
    ) -> Result<Vec<String>> {
        let _ = want;
        let mut ids: Vec<String> = self
            .scan(request, conditions, extra)?
            .into_iter()
            .map(|span| span.trace_id)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }
}

impl SpanStore for RecordStore<SpanSchema> {
    fn scan(
        &self,
        request: Scan<'_>,
        _conditions: &[SpanCondition],
        extra: RecordFilter<'_, SpanRecord>,
    ) -> Result<Vec<SpanRecord>> {
        Self::scan(self, request, &[], extra)
    }

    fn trace(&self, trace_id: &str) -> Result<Vec<SpanRecord>> {
        // The whole retention window, but not the whole store: `exact_key` lets each
        // segment's Bloom filter answer "this trace is definitely not here" without any
        // I/O.
        let mut request = Scan::range(0, u64::MAX).exact_key(trace_id);
        request.order = crate::topk::Order::Ascending;
        Self::scan(self, request, &[], &|span: &SpanRecord| {
            span.trace_id == trace_id
        })
    }

    fn of_traces(&self, trace_ids: &[&str], request: Scan<'_>) -> Result<Vec<SpanRecord>> {
        let wanted: std::collections::HashSet<&str> = trace_ids.iter().copied().collect();
        Self::scan(self, request, &[], &|span: &SpanRecord| {
            wanted.contains(span.trace_id.as_str())
        })
    }
}
