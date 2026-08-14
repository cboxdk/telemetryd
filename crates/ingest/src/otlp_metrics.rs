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
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NumberPoint {
    #[serde(alias = "time_unix_nano")]
    pub time_unix_nano: FlexU64,
    pub attributes: Vec<KeyValue>,
    #[serde(alias = "as_double")]
    pub as_double: Option<f64>,
    #[serde(alias = "as_int")]
    pub as_int: FlexU64,
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
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct HistogramPoint {
    #[serde(alias = "time_unix_nano")]
    pub time_unix_nano: FlexU64,
    pub attributes: Vec<KeyValue>,
    pub count: FlexU64,
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
    let data: MetricsData = serde_json::from_slice(body)?;
    Ok(convert_data(&data, ctx))
}

/// Convert an already-parsed payload. See [`crate::logs::convert_data`] for why.
pub fn convert_data(data: &MetricsData, ctx: MetricContext<'_>) -> Decoded<MetricSample> {
    let mut decoded = Decoded::default();

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
        decoded.rejections.push(Rejection::new(
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
        // `_count`, `_sum` and `_bucket` are the counter-ish suffixes here; `_total` is
        // not part of the histogram convention.
        let name = prometheus_metric_name(&metric.name, &metric.unit, false);
        for point in &histogram.data_points {
            push_histogram(&name, point, resource, app, ctx, decoded);
        }
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

    if let Err(rejection) = check_limits(&series, ctx) {
        decoded.rejections.push(rejection);
        return;
    }

    decoded.records.push(MetricSample {
        timestamp_nanos,
        series,
        value: point.value(),
        kind,
    });
}

/// Expand an OTLP histogram into the `_bucket` / `_sum` / `_count` series Prometheus
/// expects, with cumulative bucket counts.
fn push_histogram(
    name: &str,
    point: &HistogramPoint,
    resource: &Labels,
    app: &str,
    ctx: MetricContext<'_>,
    decoded: &mut Decoded<MetricSample>,
) {
    let timestamp_nanos = resolve_time(point.time_unix_nano, ctx, decoded);

    let mut base = base_series(name, resource, app, ctx);
    add_attributes(&mut base, &point.attributes);
    if let Err(rejection) = check_limits(&base, ctx) {
        decoded.rejections.push(rejection);
        return;
    }

    // OTLP sends per-bucket counts; Prometheus wants a running total.
    let mut cumulative = 0u64;
    for (index, count) in point.bucket_counts.iter().enumerate() {
        cumulative = cumulative.saturating_add(count.get().unwrap_or(0));

        // `bucket_counts` has one more entry than `explicit_bounds`: the last is the
        // overflow bucket, which is `+Inf`.
        let bound = point
            .explicit_bounds
            .get(index)
            .map_or_else(|| "+Inf".to_owned(), |value| format_bound(*value));

        let mut series = base.clone();
        series.insert(METRIC_NAME_LABEL, format!("{name}_bucket"));
        series.insert("le", bound);

        #[allow(clippy::cast_precision_loss)]
        decoded.records.push(MetricSample {
            timestamp_nanos,
            series,
            value: cumulative as f64,
            kind: MetricKind::Histogram,
        });
    }

    if let Some(sum) = point.sum {
        let mut series = base.clone();
        series.insert(METRIC_NAME_LABEL, format!("{name}_sum"));
        decoded.records.push(MetricSample {
            timestamp_nanos,
            series,
            value: sum,
            kind: MetricKind::Counter,
        });
    }

    if let Some(count) = point.count.get() {
        let mut series = base;
        series.insert(METRIC_NAME_LABEL, format!("{name}_count"));
        #[allow(clippy::cast_precision_loss)]
        decoded.records.push(MetricSample {
            timestamp_nanos,
            series,
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
}
