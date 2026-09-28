//! OTLP/HTTP JSON metrics decoding.
//!
//! OTLP and Prometheus model metrics differently, and the mapping is the interesting
//! part:
//!
//! - **Names are dotted in OTLP** (`http.server.duration`) and must be
//!   `[a-zA-Z_:][a-zA-Z0-9_:]*` in Prometheus. Here — unlike `remote_write`, where an
//!   invalid name is refused — the dotted form is the convention, so it is rewritten
//!   once, explicitly, at this boundary. That is the whole difference: a Prometheus
//!   producer chose its name, an OTLP producer followed a convention we translate.
//! - **Histograms are not cumulative buckets on the wire.** OTLP sends per-bucket
//!   counts with explicit bounds; Prometheus expects a cumulative `_bucket` series with
//!   an `le` label. The running total is built here so `histogram_quantile` works.
//! - **Sums carry monotonicity**, which becomes counter vs gauge.

use std::collections::HashMap;

use serde::Deserialize;
use telemetryd_core::Labels;
use telemetryd_core::config::{IngestConfig, LimitsConfig};
use telemetryd_core::metric::{METRIC_NAME_LABEL, MetricKind, MetricSample};
use telemetryd_core::record::{APP_LABEL, UNKNOWN_APP, sanitize_label_name};

use crate::logs::normalize_timestamp;
use crate::otlp::{FlexU64, InstrumentationScope, KeyValue, Resource, extend_labels};
use crate::{Decoded, RejectReason, Rejection};

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MetricsData {
    #[serde(alias = "resource_metrics")]
    pub resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ResourceMetrics {
    pub resource: Option<Resource>,
    #[serde(alias = "scope_metrics")]
    pub scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ScopeMetrics {
    pub scope: Option<InstrumentationScope>,
    pub metrics: Vec<MetricJson>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MetricJson {
    pub name: String,
    pub unit: String,
    pub gauge: Option<NumberData>,
    pub sum: Option<SumData>,
    pub histogram: Option<HistogramData>,
    /// Counted, not decoded: telemetryd does not store these, and says so per point.
    #[serde(alias = "exponential_histogram")]
    pub exponential_histogram: Option<UncountedPoints>,
    pub summary: Option<UncountedPoints>,
}

/// The data points of a metric type telemetryd refuses, counted so the refusal can say
/// how many.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct UncountedPoints {
    #[serde(alias = "data_points")]
    pub data_points: Vec<serde::de::IgnoredAny>,
}

/// OTLP's `AggregationTemporality`: 1 is delta, 2 cumulative, 0 unspecified.
pub const TEMPORALITY_DELTA: i32 = 1;

fn temporality(raw: &crate::otlp::FlexEnum) -> Option<i32> {
    raw.resolve(|name| match name {
        "AGGREGATION_TEMPORALITY_DELTA" => Some(1),
        "AGGREGATION_TEMPORALITY_CUMULATIVE" => Some(2),
        "AGGREGATION_TEMPORALITY_UNSPECIFIED" => Some(0),
        _ => None,
    })
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NumberData {
    #[serde(alias = "data_points")]
    pub data_points: Vec<NumberPoint>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SumData {
    #[serde(alias = "data_points")]
    pub data_points: Vec<NumberPoint>,
    #[serde(alias = "is_monotonic")]
    pub is_monotonic: bool,
    #[serde(alias = "aggregation_temporality")]
    pub aggregation_temporality: crate::otlp::FlexEnum,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NumberPoint {
    #[serde(alias = "time_unix_nano")]
    pub time_unix_nano: FlexU64,
    pub attributes: Vec<KeyValue>,
    #[serde(alias = "as_double")]
    #[serde(deserialize_with = "crate::otlp::flex_f64")]
    pub as_double: Option<f64>,
    #[serde(alias = "as_int")]
    pub as_int: crate::otlp::FlexI64,
}

impl NumberPoint {
    #[allow(clippy::cast_precision_loss)]
    fn value(&self) -> f64 {
        self.as_double
            .or_else(|| self.as_int.get().map(|v| v as f64))
            .unwrap_or(0.0)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HistogramData {
    #[serde(alias = "data_points")]
    pub data_points: Vec<HistogramPoint>,
    #[serde(alias = "aggregation_temporality")]
    pub aggregation_temporality: crate::otlp::FlexEnum,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HistogramPoint {
    #[serde(alias = "time_unix_nano")]
    pub time_unix_nano: FlexU64,
    pub attributes: Vec<KeyValue>,
    pub count: FlexU64,
    #[serde(deserialize_with = "crate::otlp::flex_f64")]
    pub sum: Option<f64>,
    #[serde(alias = "bucket_counts")]
    pub bucket_counts: Vec<FlexU64>,
    #[serde(alias = "explicit_bounds")]
    pub explicit_bounds: Vec<f64>,
}

/// Everything a metrics decode needs beyond the payload.
#[derive(Debug, Clone, Copy)]
pub struct MetricContext<'a> {
    pub limits: &'a LimitsConfig,
    pub ingest: &'a IngestConfig,
    pub now_nanos: u64,
    /// The memory requests in flight share, which decoding draws from.
    pub pool: Option<&'a std::sync::Arc<crate::pool::MemoryPool>>,
}

/// Rewrite an OTLP metric name into a valid Prometheus name.
///
/// Done here and only here. `remote_write` refuses an invalid name instead, because
/// there the producer chose it and renaming would make their dashboards query a series
/// that does not exist.
pub fn prometheus_name(otlp_name: &str) -> String {
    let mut out = String::with_capacity(otlp_name.len());
    for (index, ch) in otlp_name.chars().enumerate() {
        let valid = if index == 0 {
            ch.is_ascii_alphabetic() || ch == '_' || ch == ':'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'
        };
        out.push(if valid { ch } else { '_' });
    }
    if out.is_empty() { "_".to_owned() } else { out }
}

/// UCUM units, as Prometheus spells them in a metric name.
///
/// From the OpenTelemetry-to-Prometheus compatibility specification. Anything not listed
/// is left alone rather than guessed at: an unknown unit appended verbatim would produce
/// a name nobody queries, which is the failure this whole function exists to end.
const UNITS: &[(&str, &str)] = &[
    ("d", "days"),
    ("h", "hours"),
    ("min", "minutes"),
    ("s", "seconds"),
    ("ms", "milliseconds"),
    ("us", "microseconds"),
    ("ns", "nanoseconds"),
    ("By", "bytes"),
    ("KiBy", "kibibytes"),
    ("MiBy", "mebibytes"),
    ("GiBy", "gibibytes"),
    ("TiBy", "tibibytes"),
    ("KBy", "kilobytes"),
    ("MBy", "megabytes"),
    ("GBy", "gigabytes"),
    ("TBy", "terabytes"),
    ("%", "percent"),
    ("Cel", "celsius"),
    ("Hz", "hertz"),
    ("V", "volts"),
    ("A", "amperes"),
    ("J", "joules"),
    ("W", "watts"),
];

/// Assemble the name a Prometheus client will actually ask for.
///
/// # The bug this closes
///
/// The unit arrived on every OTLP metric and was read into a field nothing used, and
/// monotonic sums were stored under their bare name. So `http.server.request.duration`
/// with `unit: "ms"` was stored as `http_server_request_duration`, while every query
/// written to the convention asks for `http_server_request_duration_milliseconds`. The
/// request succeeds and matches nothing, so a dashboard shows `0` rather than an error —
/// measured against a real deployment, 60 of the 64 metric names its UI asks for did not
/// exist, and 914 successful queries had returned nothing.
///
/// Two rules, both from the OpenTelemetry-to-Prometheus specification:
///
/// - the unit becomes part of the name, spelled out — `ms` → `milliseconds`
/// - a monotonic sum gets `_total`, which is what makes it a counter to a reader
///
/// `1` is dimensionless and adds nothing; a unit already present in the name is not
/// repeated, because producers that follow the convention themselves would otherwise get
/// `_seconds_seconds`.
fn prometheus_metric_name(otlp_name: &str, unit: &str, counter: bool) -> String {
    let mut name = prometheus_name(otlp_name);

    let unit = unit.trim();
    if !unit.is_empty()
        && unit != "1"
        && let Some((_, word)) = UNITS.iter().find(|(ucum, _)| *ucum == unit)
        && !name.ends_with(word)
    {
        name.push('_');
        name.push_str(word);
    }

    // Suffix order matters: `_total` goes last, so a counter in seconds is
    // `x_seconds_total` and not `x_total_seconds`.
    if counter && !name.ends_with("_total") {
        name.push_str("_total");
    }
    name
}

/// Decode an `ExportMetricsServiceRequest`.
pub fn decode(
    body: &[u8],
    ctx: MetricContext<'_>,
) -> Result<Decoded<MetricSample>, serde_json::Error> {
    crate::json_objects_within(body)?;
    let data: MetricsData = serde_json::from_slice(body)?;
    Ok(convert_data(&data, ctx))
}

/// Convert an already-parsed payload. See [`crate::logs::convert_data`] for why.
pub fn convert_data(data: &MetricsData, ctx: MetricContext<'_>) -> Decoded<MetricSample> {
    let mut decoded = Decoded::bounded(ctx.limits).drawing_from(ctx.pool);

    for resource_metrics in &data.resource_metrics {
        let mut resource_labels = Labels::new();
        if let Some(resource) = &resource_metrics.resource {
            extend_labels(&mut resource_labels, &resource.attributes);
        }
        // Every series belongs to an app, so retention and quotas never see a missing
        // tenant.
        let app = resource_labels
            .get(APP_LABEL)
            .or_else(|| resource_labels.get("service_name"))
            .unwrap_or(UNKNOWN_APP)
            .to_owned();

        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                convert_metric(metric, &resource_labels, &app, ctx, &mut decoded);
            }
        }
    }

    decoded
}

fn convert_metric(
    metric: &MetricJson,
    resource: &Labels,
    app: &str,
    ctx: MetricContext<'_>,
    decoded: &mut Decoded<MetricSample>,
) {
    if metric.name.trim().is_empty() {
        decoded.refuse(Rejection::new(
            RejectReason::MissingMetricName,
            "metric has no name".to_owned(),
        ));
        return;
    }
    if let Some(gauge) = &metric.gauge {
        // A gauge is never a counter, so it never takes `_total`.
        let name = prometheus_metric_name(&metric.name, &metric.unit, false);
        for point in &gauge.data_points {
            push_number(&name, MetricKind::Gauge, point, resource, app, ctx, decoded);
        }
    }
    if let Some(sum) = &metric.sum {
        if temporality(&sum.aggregation_temporality) == Some(TEMPORALITY_DELTA) {
            refuse_delta(&metric.name, sum.data_points.len(), decoded);
            return;
        }
        // Monotonic is the only thing that makes a sum a counter — and the only thing
        // that earns `_total`.
        let kind = if sum.is_monotonic {
            MetricKind::Counter
        } else {
            MetricKind::Gauge
        };
        let name = prometheus_metric_name(&metric.name, &metric.unit, sum.is_monotonic);
        for point in &sum.data_points {
            push_number(&name, kind, point, resource, app, ctx, decoded);
        }
    }
    if let Some(histogram) = &metric.histogram {
        if temporality(&histogram.aggregation_temporality) == Some(TEMPORALITY_DELTA) {
            refuse_delta(&metric.name, histogram.data_points.len(), decoded);
            return;
        }
        // `_count`, `_sum` and `_bucket` are the counter-ish suffixes here; `_total` is
        // not part of the histogram convention.
        let name = prometheus_metric_name(&metric.name, &metric.unit, false);
        let mut derived = HistogramSeries::default();
        for point in &histogram.data_points {
            push_histogram(&name, point, (resource, app), ctx, &mut derived, decoded);
        }
    }
    for (kind, points) in [
        ("exponential histogram", &metric.exponential_histogram),
        ("summary", &metric.summary),
    ] {
        let Some(points) = points else { continue };
        for _ in &points.data_points {
            decoded.refuse(Rejection::new(
                RejectReason::UnsupportedMetricType,
                format!(
                    "{kind} {:?} is not stored; export it as an explicit-bucket histogram",
                    metric.name
                ),
            ));
        }
    }
}

/// Refuse every point of a delta-temporality metric, naming the fix.
fn refuse_delta(name: &str, points: usize, decoded: &mut Decoded<MetricSample>) {
    for _ in 0..points {
        decoded.refuse(Rejection::new(
            RejectReason::DeltaTemporality,
            format!(
                "{name:?} uses delta temporality, which is not stored; configure the \
                 exporter for cumulative (OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=cumulative)"
            ),
        ));
    }
}

fn push_number(
    name: &str,
    kind: MetricKind,
    point: &NumberPoint,
    resource: &Labels,
    app: &str,
    ctx: MetricContext<'_>,
    decoded: &mut Decoded<MetricSample>,
) {
    let timestamp_nanos = resolve_time(point.time_unix_nano, ctx, decoded);

    let mut series = base_series(name, resource, app, ctx);
    add_attributes(&mut series, &point.attributes);
    identify(&mut series, resource);

    if let Err(rejection) = check_limits(&series, ctx) {
        decoded.refuse(rejection);
        return;
    }

    decoded.keep(MetricSample {
        timestamp_nanos,
        series,
        value: point.value(),
        kind,
    });
}

/// The series one histogram's points expand into, kept across its data points.
///
/// A histogram point becomes a series per bucket and one each for its sum and count, and
/// the next point of the same series — the same attributes a scrape interval later —
/// becomes the same ones again. Built afresh, each is a copy of every label, and an
/// export carrying a minute of fifty route histograms builds fifty thousand of them to
/// arrive at 850. Here each is built once per payload and shared after.
#[derive(Default)]
struct HistogramSeries {
    /// Keyed by the point's own series and its bucket bounds, bit for bit.
    by_base: HashMap<(Labels, Vec<u64>), std::sync::Arc<Expanded>>,
}

/// One histogram series expanded: a label set per bucket, then the sum's and count's.
struct Expanded {
    buckets: Vec<Labels>,
    sum: Labels,
    count: Labels,
}

impl HistogramSeries {
    fn expand(
        &mut self,
        name: &str,
        base: Labels,
        point: &HistogramPoint,
    ) -> std::sync::Arc<Expanded> {
        let bounds: Vec<u64> = point.explicit_bounds.iter().map(|b| b.to_bits()).collect();
        let key = (base, bounds);
        if let Some(expanded) = self.by_base.get(&key)
            && expanded.buckets.len() >= point.bucket_counts.len()
        {
            return std::sync::Arc::clone(expanded);
        }
        let base = &key.0;
        let with = |metric: String, le: Option<String>| {
            let mut series = base.clone();
            series.insert(METRIC_NAME_LABEL, metric);
            if let Some(le) = le {
                series.insert("le", le);
            }
            series
        };
        // `bucket_counts` has one more entry than `explicit_bounds`: the last is the
        // overflow bucket, which is `+Inf`.
        let buckets = (0..point
            .bucket_counts
            .len()
            .max(point.explicit_bounds.len() + 1))
            .map(|index| {
                let bound = point
                    .explicit_bounds
                    .get(index)
                    .map_or_else(|| "+Inf".to_owned(), |value| format_bound(*value));
                with(format!("{name}_bucket"), Some(bound))
            })
            .collect();
        let expanded = std::sync::Arc::new(Expanded {
            buckets,
            sum: with(format!("{name}_sum"), None),
            count: with(format!("{name}_count"), None),
        });
        self.by_base.insert(key, std::sync::Arc::clone(&expanded));
        expanded
    }
}

/// Expand an OTLP histogram into the `_bucket` / `_sum` / `_count` series Prometheus
/// expects, with cumulative bucket counts.
fn push_histogram(
    name: &str,
    point: &HistogramPoint,
    (resource, app): (&Labels, &str),
    ctx: MetricContext<'_>,
    derived: &mut HistogramSeries,
    decoded: &mut Decoded<MetricSample>,
) {
    let timestamp_nanos = resolve_time(point.time_unix_nano, ctx, decoded);

    let mut base = base_series(name, resource, app, ctx);
    add_attributes(&mut base, &point.attributes);
    identify(&mut base, resource);
    if let Err(rejection) = check_limits(&base, ctx) {
        decoded.refuse(rejection);
        return;
    }
    let expanded = derived.expand(name, base, point);

    // OTLP sends per-bucket counts; Prometheus wants a running total.
    let mut cumulative = 0u64;
    for (count, series) in point.bucket_counts.iter().zip(&expanded.buckets) {
        cumulative = cumulative.saturating_add(count.get().unwrap_or(0));
        #[allow(clippy::cast_precision_loss)]
        decoded.keep(MetricSample {
            timestamp_nanos,
            series: series.shared_with(),
            value: cumulative as f64,
            kind: MetricKind::Histogram,
        });
    }

    if let Some(sum) = point.sum {
        decoded.keep(MetricSample {
            timestamp_nanos,
            series: expanded.sum.shared_with(),
            value: sum,
            kind: MetricKind::Counter,
        });
    }

    if let Some(count) = point.count.get() {
        #[allow(clippy::cast_precision_loss)]
        decoded.keep(MetricSample {
            timestamp_nanos,
            series: expanded.count.shared_with(),
            value: count as f64,
            kind: MetricKind::Counter,
        });
    }
}

/// Render a bucket bound the way Prometheus writes `le`.
fn format_bound(value: f64) -> String {
    if value.is_infinite() {
        return if value > 0.0 { "+Inf" } else { "-Inf" }.to_owned();
    }
    format!("{value}")
}

fn base_series(name: &str, resource: &Labels, app: &str, ctx: MetricContext<'_>) -> Labels {
    let mut series = Labels::new();
    series.insert(METRIC_NAME_LABEL, name);
    series.insert(APP_LABEL, app);

    for promoted in &ctx.ingest.stream_labels {
        let promoted = sanitize_label_name(promoted);
        if let Some(value) = resource.get(&promoted) {
            series.insert(promoted, value);
        }
    }
    series
}

/// `job` and `instance`, as Prometheus's own OTLP receiver sets them: `job` from
/// `service.namespace` and `service.name`, `instance` from `service.instance.id`.
/// Set last, over any data-point attribute of the same name, as Prometheus sets them.
///
/// # Why
///
/// A series was its name, its app and the promoted resource labels — none of which tell
/// two hosts of one app apart. cbox.dk runs on two servers exporting the same counters,
/// and their samples became one series whose value jumped between the two totals every
/// minute: each jump read as a counter reset, and `increase()` reported seven thousand
/// cache warms an hour where there were none.
///
/// `instance` falls back to `host.name` when a producer sends no `service.instance.id`,
/// which laravel-telemetry does not. Prometheus sets no `instance` then, and merges the
/// hosts into one series exactly as this did; the one label more is the difference
/// between two counters and a number that means nothing.
fn identify(series: &mut Labels, resource: &Labels) {
    if let Some(service) = resource.get("service_name") {
        let job = match resource.get("service_namespace") {
            Some(namespace) => format!("{namespace}/{service}"),
            None => service.to_owned(),
        };
        series.insert("job", job);
    }
    if let Some(instance) = resource
        .get("service_instance_id")
        .or_else(|| resource.get("host_name"))
    {
        series.insert("instance", instance);
    }
}

fn add_attributes(series: &mut Labels, attributes: &[KeyValue]) {
    // Data-point attributes are series labels here, not free-form data, so they are
    // sanitised like any other label name.
    extend_labels(series, attributes);
}

/// Resolve a data point's timestamp, falling back to arrival time.
fn resolve_time(raw: FlexU64, ctx: MetricContext<'_>, decoded: &mut Decoded<MetricSample>) -> u64 {
    match raw.get().filter(|v| *v > 0).and_then(normalize_timestamp) {
        Some((nanos, unit)) => {
            if unit != crate::logs::TimeUnit::Nanos {
                decoded.rescaled_timestamps += 1;
            }
            nanos
        }
        None => ctx.now_nanos,
    }
}

fn check_limits(series: &Labels, ctx: MetricContext<'_>) -> Result<(), Rejection> {
    if series.len() > ctx.limits.max_labels_per_series as usize {
        return Err(Rejection::new(
            RejectReason::TooManyLabels,
            format!(
                "{} labels exceeds max_labels_per_series ({})",
                series.len(),
                ctx.limits.max_labels_per_series
            ),
        ));
    }
    for (name, value) in series.iter() {
        if name.len() > ctx.limits.max_label_name_bytes as usize {
            return Err(Rejection::new(
                RejectReason::LabelNameTooLong,
                format!("label name {name:?} exceeds max_label_name_bytes"),
            ));
        }
        if value.len() > ctx.limits.max_label_value_bytes as usize {
            return Err(Rejection::new(
                RejectReason::LabelValueTooLong,
                format!("value of label {name:?} exceeds max_label_value_bytes"),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
// Exact float comparison is deliberate here: these are small values that round-trip
// through f64 without loss, and the assertion is that they arrived unchanged.
#[allow(clippy::unwrap_used, clippy::float_cmp)]
mod tests {
    use super::*;

    const NOW: u64 = 1_750_000_000_000_000_000;

    fn decode_str(json: &str) -> Decoded<MetricSample> {
        let limits = LimitsConfig::default();
        let ingest = IngestConfig::default();
        decode(
            json.as_bytes(),
            MetricContext {
                pool: None,
                limits: &limits,
                ingest: &ingest,
                now_nanos: NOW,
            },
        )
        .unwrap()
    }

    fn find<'a>(decoded: &'a Decoded<MetricSample>, name: &str) -> Vec<&'a MetricSample> {
        decoded
            .records
            .iter()
            .filter(|s| s.name() == name)
            .collect()
    }

    #[test]
    fn otlp_dotted_names_become_valid_prometheus_names() {
        // Rewritten here because the dotted form is the OTLP convention — unlike
        // remote_write, where a name the producer chose is refused rather than renamed.
        assert_eq!(
            prometheus_name("http.server.duration"),
            "http_server_duration"
        );
        assert_eq!(prometheus_name("queue-depth"), "queue_depth");
        assert_eq!(prometheus_name("1st"), "_st");
        assert_eq!(prometheus_name("already_fine"), "already_fine");
        assert_eq!(prometheus_name(""), "_");
    }

    #[test]
    fn a_gauge_decodes_with_its_attributes_as_labels() {
        let decoded = decode_str(
            r#"{"resourceMetrics":[{
                "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},
                "scopeMetrics":[{"metrics":[{
                    "name":"queue.depth",
                    "gauge":{"dataPoints":[{
                        "timeUnixNano":"1750000000000000000",
                        "asDouble":12.5,
                        "attributes":[{"key":"queue","value":{"stringValue":"emails"}}]
                    }]}
                }]}]
            }]}"#,
        );

        assert_eq!(decoded.records.len(), 1);
        let sample = &decoded.records[0];
        assert_eq!(sample.name(), "queue_depth");
        assert_eq!(sample.app(), "checkout");
        assert_eq!(sample.series.get("queue"), Some("emails"));
        assert_eq!(sample.value, 12.5);
        assert_eq!(sample.kind, MetricKind::Gauge);
    }

    #[test]
    fn monotonicity_decides_counter_versus_gauge() {
        let counter = decode_str(
            r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{
                "name":"requests","sum":{"isMonotonic":true,"dataPoints":[
                    {"timeUnixNano":"1750000000000000000","asInt":"7"}]}}]}]}]}"#,
        );
        assert_eq!(counter.records[0].kind, MetricKind::Counter);
        assert_eq!(counter.records[0].value, 7.0);

        let gauge = decode_str(
            r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{
                "name":"in_flight","sum":{"isMonotonic":false,"dataPoints":[
                    {"timeUnixNano":"1750000000000000000","asInt":"3"}]}}]}]}]}"#,
        );
        assert_eq!(gauge.records[0].kind, MetricKind::Gauge);
    }

    #[test]
    fn a_histogram_becomes_cumulative_buckets_plus_sum_and_count() {
        // OTLP sends per-bucket counts; histogram_quantile needs a running total.
        let decoded = decode_str(
            r#"{"resourceMetrics":[{
                "resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},
                "scopeMetrics":[{"metrics":[{
                    "name":"http.duration",
                    "histogram":{"dataPoints":[{
                        "timeUnixNano":"1750000000000000000",
                        "count":"10",
                        "sum":4.5,
                        "bucketCounts":["5","3","2"],
                        "explicitBounds":[0.1,0.5]
                    }]}
                }]}]
            }]}"#,
        );

        let buckets = find(&decoded, "http_duration_bucket");
        assert_eq!(buckets.len(), 3, "two bounds plus the +Inf overflow bucket");

        let value_at = |le: &str| {
            buckets
                .iter()
                .find(|s| s.series.get("le") == Some(le))
                .map(|s| s.value)
        };
        // Cumulative: 5, then 5+3, then 5+3+2.
        assert_eq!(value_at("0.1"), Some(5.0));
        assert_eq!(value_at("0.5"), Some(8.0));
        assert_eq!(value_at("+Inf"), Some(10.0));

        assert_eq!(find(&decoded, "http_duration_sum")[0].value, 4.5);
        assert_eq!(find(&decoded, "http_duration_count")[0].value, 10.0);
    }

    /// A histogram's next point of the same series reuses the label sets the first one
    /// built — one allocation shared, not a copy of every label per bucket per point —
    /// while a point with other bounds gets its own.
    #[test]
    fn points_of_one_histogram_series_share_their_label_sets() {
        let point = |ts: &str, bounds: &str, counts: &str| {
            format!(
                r#"{{"timeUnixNano":"{ts}","count":"3","sum":1.5,
                    "bucketCounts":[{counts}],"explicitBounds":[{bounds}],
                    "attributes":[{{"key":"route","value":{{"stringValue":"/a"}}}}]}}"#
            )
        };
        let decoded = decode_str(&format!(
            r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[{{
                "name":"latency","histogram":{{"aggregationTemporality":2,"dataPoints":[{},{},{}]}}
            }}]}}]}}]}}"#,
            point("1750000000000000000", "0.1,0.5", r#""1","1","1""#),
            point("1750000030000000000", "0.1,0.5", r#""2","2","2""#),
            point("1750000060000000000", "0.1,1", r#""1","1","1""#),
        ));
        let buckets = find(&decoded, "latency_bucket");
        assert_eq!(buckets.len(), 9);
        // The same series a point later: the same allocation, and the same labels.
        assert!(buckets[0].series.shares_storage_with(&buckets[3].series));
        assert_eq!(buckets[3].value, 2.0);
        // Other bounds are other series.
        assert_eq!(buckets[1].series.get("le"), Some("0.5"));
        assert_eq!(buckets[7].series.get("le"), Some("1"));
        assert_eq!(buckets[8].series.get("le"), Some("+Inf"));
        assert_eq!(find(&decoded, "latency_count").len(), 3);
    }

    #[test]
    fn histogram_buckets_keep_their_data_point_attributes() {
        let decoded = decode_str(
            r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{
                "name":"latency",
                "histogram":{"dataPoints":[{
                    "timeUnixNano":"1750000000000000000",
                    "count":"1","bucketCounts":["1"],
                    "attributes":[{"key":"route","value":{"stringValue":"/api"}}]
                }]}}]}]}]}"#,
        );
        let bucket = find(&decoded, "latency_bucket")[0];
        assert_eq!(bucket.series.get("route"), Some("/api"));
        assert_eq!(bucket.series.get("le"), Some("+Inf"));
    }

    #[test]
    fn one_metric_can_carry_several_data_points() {
        let decoded = decode_str(
            r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{
                "name":"up","gauge":{"dataPoints":[
                    {"timeUnixNano":"1750000000000000000","asDouble":1,
                     "attributes":[{"key":"instance","value":{"stringValue":"a"}}]},
                    {"timeUnixNano":"1750000000000000000","asDouble":0,
                     "attributes":[{"key":"instance","value":{"stringValue":"b"}}]}
                ]}}]}]}]}"#,
        );
        assert_eq!(decoded.records.len(), 2);
        assert_ne!(decoded.records[0].series, decoded.records[1].series);
    }

    fn with_resource(resource: &str) -> Decoded<MetricSample> {
        decode_str(&format!(
            r#"{{"resourceMetrics":[{{"resource":{{"attributes":[{resource}]}},
                "scopeMetrics":[{{"metrics":[{{"name":"warms","unit":"1","sum":{{
                    "isMonotonic":true,"aggregationTemporality":2,"dataPoints":[{{
                    "timeUnixNano":"1750000000000000000","asInt":"124",
                    "attributes":[{{"key":"job","value":{{"stringValue":"ignored"}}}}]
                }}]}}}}]}}]}}]}}"#
        ))
    }

    fn attribute(key: &str, value: &str) -> String {
        format!(r#"{{"key":"{key}","value":{{"stringValue":"{value}"}}}}"#)
    }

    /// Two hosts of one app exporting the same counter are two series, told apart by
    /// `instance` as Prometheus's OTLP receiver tells them apart. They were one series,
    /// whose value jumped between the hosts' totals and read as a reset every minute.
    #[test]
    fn two_hosts_of_one_app_are_two_series() {
        let series = |host: &str| {
            let decoded = with_resource(
                &[
                    attribute("service.name", "cbox-web"),
                    attribute("host.name", host),
                ]
                .join(","),
            );
            decoded.records[0].series.clone()
        };
        let (one, two) = (series("web01"), series("web02"));
        assert_ne!(one, two);
        assert_eq!(one.get("instance"), Some("web01"));
        // Set over the data point's own `job`, as Prometheus sets it.
        assert_eq!(one.get("job"), Some("cbox-web"));
    }

    #[test]
    fn job_and_instance_follow_prometheus() {
        let decoded = with_resource(
            &[
                attribute("service.name", "api"),
                attribute("service.namespace", "shop"),
                attribute("service.instance.id", "7f3a"),
                attribute("host.name", "web01"),
            ]
            .join(","),
        );
        let series = &decoded.records[0].series;
        assert_eq!(series.get("job"), Some("shop/api"));
        // The instance id outranks the host.
        assert_eq!(series.get("instance"), Some("7f3a"));

        // Nothing to name them from, nothing named.
        let decoded = with_resource("");
        assert_eq!(decoded.records[0].series.get("job"), Some("ignored"));
        assert_eq!(decoded.records[0].series.get("instance"), None);
    }

    #[test]
    fn a_metric_without_a_name_is_rejected() {
        let decoded = decode_str(
            r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{
                "gauge":{"dataPoints":[{"timeUnixNano":"1750000000000000000","asDouble":1}]}}]}]}]}"#,
        );
        assert!(decoded.records.is_empty());
        assert_eq!(
            decoded.rejections[0].reason,
            RejectReason::MissingMetricName
        );
    }

    #[test]
    fn a_missing_timestamp_falls_back_to_arrival() {
        let decoded = decode_str(
            r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{
                "name":"up","gauge":{"dataPoints":[{"asDouble":1}]}}]}]}]}"#,
        );
        assert_eq!(decoded.records[0].timestamp_nanos, NOW);
    }

    #[test]
    fn a_metric_name_carries_its_unit_and_a_counter_says_total() {
        // Measured against a real deployment before this existed: 60 of the 64 metric
        // names the UI asks for did not exist, and 914 successful queries had returned
        // nothing. A name that is merely wrong produces `200` with an empty result, so a
        // dashboard shows `0` instead of an error and nothing anywhere says why.
        assert_eq!(
            prometheus_metric_name("http.server.request.duration", "ms", false),
            "http_server_request_duration_milliseconds"
        );
        assert_eq!(
            prometheus_metric_name("cache.operations", "1", true),
            "cache_operations_total",
            "`1` is dimensionless and adds nothing to the name"
        );
        assert_eq!(
            prometheus_metric_name("worker.memory", "By", false),
            "worker_memory_bytes"
        );

        // Order: a counter measured in seconds is `_seconds_total`, never
        // `_total_seconds`.
        assert_eq!(
            prometheus_metric_name("job.time", "s", true),
            "job_time_seconds_total"
        );

        // A producer already following the convention must not be doubled up.
        assert_eq!(
            prometheus_metric_name("queue_wait_seconds", "s", false),
            "queue_wait_seconds"
        );
        assert_eq!(
            prometheus_metric_name("requests_total", "1", true),
            "requests_total"
        );

        // An unknown unit is left off rather than guessed at: appending it verbatim
        // would produce a name nobody queries, which is the failure being fixed.
        assert_eq!(
            prometheus_metric_name("odd.thing", "furlongs", false),
            "odd_thing"
        );
        assert_eq!(
            prometheus_metric_name("plain.gauge", "", false),
            "plain_gauge"
        );
    }

    #[test]
    fn timestamps_in_the_wrong_unit_are_corrected_and_counted() {
        let decoded = decode_str(
            r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{
                "name":"up","gauge":{"dataPoints":[
                    {"timeUnixNano":"1750000000","asDouble":1}]}}]}]}]}"#,
        );
        assert_eq!(decoded.records[0].timestamp_nanos, NOW);
        assert_eq!(decoded.rescaled_timestamps, 1);
    }

    #[test]
    fn snake_case_payloads_decode_identically() {
        let decoded = decode_str(
            r#"{"resource_metrics":[{"scope_metrics":[{"metrics":[{
                "name":"up","sum":{"is_monotonic":true,"data_points":[
                    {"time_unix_nano":"1750000000000000000","as_int":"5"}]}}]}]}]}"#,
        );
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].value, 5.0);
        assert_eq!(decoded.records[0].kind, MetricKind::Counter);
    }

    #[test]
    fn an_empty_payload_is_valid_and_yields_nothing() {
        for json in ["{}", r#"{"resourceMetrics":[]}"#] {
            let decoded = decode_str(json);
            assert!(decoded.records.is_empty(), "{json}");
            assert!(decoded.rejections.is_empty(), "{json}");
        }
    }

    #[test]
    fn bucket_bounds_render_the_way_prometheus_writes_le() {
        assert_eq!(format_bound(0.1), "0.1");
        assert_eq!(format_bound(1.0), "1");
        assert_eq!(format_bound(f64::INFINITY), "+Inf");
    }

    /// Delta sums were stored as if cumulative — each point read as a counter reset,
    /// `increase` of 5, 3, 4 came out 120 instead of 12 — and summaries and exponential
    /// histograms vanished with a 200. Each is now refused per point, visibly.
    #[test]
    fn what_cannot_be_stored_is_refused_where_the_sender_sees_it() {
        let limits = LimitsConfig::default();
        let ingest = IngestConfig::default();
        let ctx = MetricContext {
            pool: None,
            limits: &limits,
            ingest: &ingest,
            now_nanos: 1_750_000_000_000_000_000,
        };
        let point = r#"{"timeUnixNano":"1750000000000000000","asDouble":5}"#;
        let json = format!(
            r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
              {{"name":"delta_n","sum":{{"aggregationTemporality":1,"isMonotonic":true,"dataPoints":[{point},{point}]}}}},
              {{"name":"delta_s","sum":{{"aggregationTemporality":"AGGREGATION_TEMPORALITY_DELTA","isMonotonic":true,"dataPoints":[{point}]}}}},
              {{"name":"cumulative","sum":{{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{point}]}}}},
              {{"name":"quantiles","summary":{{"dataPoints":[{{}},{{}}]}}}},
              {{"name":"expo","exponentialHistogram":{{"dataPoints":[{{}}]}}}}
            ]}}]}}]}}"#
        );
        let decoded = decode(json.as_bytes(), ctx).unwrap();
        assert_eq!(
            decoded.records.len(),
            1,
            "only the cumulative sum is stored"
        );
        let count = |reason: RejectReason| {
            decoded
                .rejections
                .iter()
                .filter(|r| r.reason == reason)
                .count()
        };
        assert_eq!(count(RejectReason::DeltaTemporality), 3);
        assert_eq!(count(RejectReason::UnsupportedMetricType), 3);
    }

    /// OTLP/JSON spells the doubles JSON cannot hold as strings. Read as plain numbers
    /// they failed the whole batch — every point in it, for one undefined gauge.
    #[test]
    fn nan_and_infinity_arrive_as_strings_and_are_kept() {
        let decoded = decode_str(
            r#"{"resourceMetrics":[{"scopeMetrics":[{"metrics":[{"name":"g","gauge":{"dataPoints":[
              {"timeUnixNano":"1750000000000000000","asDouble":"NaN"},
              {"timeUnixNano":"1750000000000000001","asDouble":"Infinity"},
              {"timeUnixNano":"1750000000000000002","asDouble":"-Infinity"},
              {"timeUnixNano":"1750000000000000003","asDouble":2.5}
            ]}}]}]}]}"#,
        );
        let values: Vec<f64> = decoded.records.iter().map(|s| s.value).collect();
        assert_eq!(values.len(), 4, "{:?}", decoded.rejections);
        assert!(values[0].is_nan());
        assert_eq!(&values[1..], &[f64::INFINITY, f64::NEG_INFINITY, 2.5]);
    }
}
