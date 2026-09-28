//! Span metrics and service graphs, derived from spans as they arrive — what Tempo's
//! metrics generator writes to a Prometheus, written here to telemetryd's own metrics.
//!
//! Grafana's service graph and its RED table read these series by name, so the names,
//! labels and default buckets are Tempo's:
//!
//! - `traces_spanmetrics_calls_total` and `traces_spanmetrics_latency_{bucket,sum,count}`
//!   by `service`, `span_name`, `span_kind`, `status_code`
//! - `traces_service_graph_request_total`, `…_request_failed_total`,
//!   `…_request_server_seconds_*` and `…_request_client_seconds_*` by `client`, `server`
//!   and `connection_type`, and `traces_service_graph_unpaired_spans_total`
//!
//! An edge is a client (or producer) span and the server (or consumer) span whose parent
//! it is, from two services. A client span whose server never reports — a database, a
//! third party — becomes an edge to a virtual node named by `peer.service`, `db.name` or
//! `db.system` once it has waited; a root server span is an edge from `user`. Counters are
//! cumulative from startup, as a scraped process's are: a restart is a counter reset,
//! which `rate` already reads correctly.

use std::collections::HashMap;

use telemetryd_core::Labels;
use telemetryd_core::metric::{METRIC_NAME_LABEL, MetricKind, MetricSample};
use telemetryd_core::span::{SpanKind, SpanRecord, SpanStatus};

/// Tempo's default span-metrics latency buckets, in seconds.
pub const SPAN_METRICS_BUCKETS: &[f64] = &[
    0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.02, 2.05, 4.10,
];
/// Tempo's default service-graph latency buckets, in seconds.
pub const SERVICE_GRAPH_BUCKETS: &[f64] = &[0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8];

/// Series one generator keeps, of each kind. Past it, new series are dropped and counted
/// rather than let a span name carrying an id grow memory without bound.
pub const MAX_SERIES: usize = 10_000;
/// Spans waiting for their other half, at most.
const MAX_PENDING: usize = 100_000;
/// A series nothing has touched for this long is no longer written, as Tempo's
/// `stale_duration`.
const STALE_NANOS: u64 = 15 * 60 * 1_000_000_000;

/// Which of Tempo's processors run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Processors {
    pub span_metrics: bool,
    pub service_graphs: bool,
}

#[derive(Debug, Clone)]
struct Histogram {
    buckets: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    fn new(bounds: &[f64]) -> Self {
        Self {
            buckets: vec![0; bounds.len()],
            sum: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, bounds: &[f64], seconds: f64) {
        for (bucket, bound) in self.buckets.iter_mut().zip(bounds) {
            if seconds <= *bound {
                *bucket += 1;
            }
        }
        self.sum += seconds;
        self.count += 1;
    }
}

#[derive(Debug, Clone)]
struct SpanSeries {
    calls: u64,
    latency: Histogram,
    touched: u64,
}

#[derive(Debug, Clone)]
struct Edge {
    requests: u64,
    failed: u64,
    server: Histogram,
    client: Histogram,
    touched: u64,
}

/// One half of an edge, waiting for the other.
#[derive(Debug, Clone)]
struct Half {
    service: String,
    seconds: f64,
    failed: bool,
    messaging: bool,
    /// What a virtual node would be called, and whether it is a database.
    virtual_node: Option<(String, bool)>,
    arrived: u64,
}

/// The generator's state. Fed spans with [`Generator::observe`]; asked for samples with
/// [`Generator::collect`].
#[derive(Debug, Default)]
pub struct Generator {
    processors: Processors,
    wait_nanos: u64,
    spans: HashMap<Labels, SpanSeries>,
    edges: HashMap<Labels, Edge>,
    unpaired: HashMap<Labels, (u64, u64)>,
    /// Client halves by `(trace, span)`, and whether one has been paired yet: a producer
    /// can have several consumers, so a client waits out its time either way.
    clients: HashMap<(String, String), (Half, bool)>,
    /// Server halves by `(trace, parent)`, every one: consumers of one producer share it.
    servers: HashMap<(String, String), Vec<Half>>,
    waiting: usize,
    /// Series refused for being past [`MAX_SERIES`].
    pub dropped: u64,
}

fn seconds(span: &SpanRecord) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let nanos = span.duration_nanos() as f64;
    nanos / 1e9
}

fn kind_name(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Internal => "SPAN_KIND_INTERNAL",
        SpanKind::Server => "SPAN_KIND_SERVER",
        SpanKind::Client => "SPAN_KIND_CLIENT",
        SpanKind::Producer => "SPAN_KIND_PRODUCER",
        SpanKind::Consumer => "SPAN_KIND_CONSUMER",
        SpanKind::Unspecified => "SPAN_KIND_UNSPECIFIED",
    }
}

fn status_name(status: SpanStatus) -> &'static str {
    match status {
        SpanStatus::Ok => "STATUS_CODE_OK",
        SpanStatus::Error => "STATUS_CODE_ERROR",
        SpanStatus::Unset => "STATUS_CODE_UNSET",
    }
}

fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

impl Generator {
    #[must_use]
    pub fn new(processors: Processors, wait_nanos: u64) -> Self {
        Self {
            processors,
            wait_nanos,
            ..Self::default()
        }
    }

    /// Account for spans as they arrive.
    pub fn observe(&mut self, spans: &[SpanRecord], now_nanos: u64) {
        for span in spans {
            if self.processors.span_metrics {
                self.span_metrics(span, now_nanos);
            }
            if self.processors.service_graphs {
                self.service_graph(span, now_nanos);
            }
        }
    }

    fn span_metrics(&mut self, span: &SpanRecord, now: u64) {
        let key = labels(&[
            ("service", span.service_name()),
            ("span_name", &span.name),
            ("span_kind", kind_name(span.kind)),
            ("status_code", status_name(span.status)),
        ]);
        if !self.spans.contains_key(&key) && self.spans.len() >= MAX_SERIES {
            self.dropped += 1;
            return;
        }
        let series = self.spans.entry(key).or_insert_with(|| SpanSeries {
            calls: 0,
            latency: Histogram::new(SPAN_METRICS_BUCKETS),
            touched: now,
        });
        series.calls += 1;
        series.latency.observe(SPAN_METRICS_BUCKETS, seconds(span));
        series.touched = now;
    }

    fn service_graph(&mut self, span: &SpanRecord, now: u64) {
        let half = || Half {
            service: span.service_name().to_owned(),
            seconds: seconds(span),
            failed: span.status == SpanStatus::Error,
            messaging: matches!(span.kind, SpanKind::Producer | SpanKind::Consumer),
            virtual_node: virtual_node(span),
            arrived: now,
        };
        match span.kind {
            SpanKind::Client | SpanKind::Producer => {
                let key = (span.trace_id.clone(), span.span_id.clone());
                let client = half();
                let servers = self.servers.remove(&key).unwrap_or_default();
                self.waiting = self.waiting.saturating_sub(servers.len());
                for server in &servers {
                    self.pair(&client, server, now);
                }
                if self.waiting < MAX_PENDING {
                    self.waiting += 1;
                    self.clients.insert(key, (client, !servers.is_empty()));
                } else if servers.is_empty() {
                    self.dropped += 1;
                }
            }
            SpanKind::Server | SpanKind::Consumer => {
                let Some(parent) = &span.parent_span_id else {
                    // A root: the caller is not instrumented — a person, or something
                    // outside. Tempo draws it as `user`, timed by the server's span.
                    let server = half();
                    self.edge(
                        "user",
                        &server.service,
                        "virtual_node",
                        &server,
                        &server,
                        now,
                    );
                    return;
                };
                let key = (span.trace_id.clone(), parent.clone());
                if let Some((client, paired)) = self.clients.get_mut(&key) {
                    *paired = true;
                    let client = client.clone();
                    self.pair(&client, &half(), now);
                } else if self.waiting < MAX_PENDING {
                    self.waiting += 1;
                    self.servers.entry(key).or_default().push(half());
                } else {
                    self.dropped += 1;
                }
            }
            _ => {}
        }
    }

    fn pair(&mut self, client: &Half, server: &Half, now: u64) {
        let connection = if client.messaging || server.messaging {
            "messaging_system"
        } else {
            ""
        };
        self.edge(
            &client.service,
            &server.service,
            connection,
            client,
            server,
            now,
        );
    }

    fn edge(
        &mut self,
        client: &str,
        server: &str,
        connection_type: &str,
        client_half: &Half,
        server_half: &Half,
        now: u64,
    ) {
        let key = labels(&[
            ("client", client),
            ("server", server),
            ("connection_type", connection_type),
        ]);
        if !self.edges.contains_key(&key) && self.edges.len() >= MAX_SERIES {
            self.dropped += 1;
            return;
        }
        let edge = self.edges.entry(key).or_insert_with(|| Edge {
            requests: 0,
            failed: 0,
            server: Histogram::new(SERVICE_GRAPH_BUCKETS),
            client: Histogram::new(SERVICE_GRAPH_BUCKETS),
            touched: now,
        });
        edge.requests += 1;
        edge.failed += u64::from(client_half.failed || server_half.failed);
        edge.client
            .observe(SERVICE_GRAPH_BUCKETS, client_half.seconds);
        edge.server
            .observe(SERVICE_GRAPH_BUCKETS, server_half.seconds);
        edge.touched = now;
    }

    /// Settle halves that have waited long enough, then every live series as samples at
    /// `now_nanos`.
    pub fn collect(&mut self, now_nanos: u64) -> Vec<MetricSample> {
        self.expire(now_nanos);
        let fresh = |touched: u64| now_nanos.saturating_sub(touched) < STALE_NANOS;
        self.spans.retain(|_, s| fresh(s.touched));
        self.edges.retain(|_, e| fresh(e.touched));

        let mut out = Vec::new();
        for (key, series) in &self.spans {
            push(
                &mut out,
                "traces_spanmetrics_calls_total",
                key,
                &[],
                series.calls,
                now_nanos,
            );
            histogram(
                &mut out,
                "traces_spanmetrics_latency",
                key,
                SPAN_METRICS_BUCKETS,
                &series.latency,
                now_nanos,
            );
        }
        for (key, edge) in &self.edges {
            push(
                &mut out,
                "traces_service_graph_request_total",
                key,
                &[],
                edge.requests,
                now_nanos,
            );
            push(
                &mut out,
                "traces_service_graph_request_failed_total",
                key,
                &[],
                edge.failed,
                now_nanos,
            );
            for (name, h) in [
                ("traces_service_graph_request_server_seconds", &edge.server),
                ("traces_service_graph_request_client_seconds", &edge.client),
            ] {
                histogram(&mut out, name, key, SERVICE_GRAPH_BUCKETS, h, now_nanos);
            }
        }
        for (key, (count, _)) in &self.unpaired {
            push(
                &mut out,
                "traces_service_graph_unpaired_spans_total",
                key,
                &[],
                *count,
                now_nanos,
            );
        }
        out
    }

    /// Halves whose other half never came: a client with somewhere to point becomes an
    /// edge to a virtual node; the rest are counted unpaired.
    fn expire(&mut self, now: u64) {
        let wait = self.wait_nanos;
        let due = move |half: &Half| now.saturating_sub(half.arrived) >= wait;
        let expired: Vec<(Half, bool)> = self
            .clients
            .extract_if(|_, (half, _)| due(half))
            .map(|(_, client)| client)
            .collect();
        let servers: Vec<Half> = self
            .servers
            .extract_if(|_, halves| halves.first().is_some_and(due))
            .flat_map(|(_, halves)| halves)
            .collect();
        self.waiting = self.waiting.saturating_sub(expired.len() + servers.len());
        // A client that met its server has said all it had to.
        let clients = expired
            .into_iter()
            .filter_map(|(client, paired)| (!paired).then_some(client));
        for client in clients {
            if let Some((node, database)) = client.virtual_node.clone() {
                let connection = if database { "database" } else { "virtual_node" };
                let server = Half {
                    service: node.clone(),
                    ..client.clone()
                };
                self.edge(&client.service, &node, connection, &client, &server, now);
            } else {
                self.count_unpaired("client", &client.service, now);
            }
        }
        for server in servers {
            self.count_unpaired("server", &server.service, now);
        }
    }

    fn count_unpaired(&mut self, side: &str, service: &str, now: u64) {
        let key = labels(&[(side, service)]);
        if self.unpaired.contains_key(&key) || self.unpaired.len() < MAX_SERIES {
            let entry = self.unpaired.entry(key).or_insert((0, now));
            entry.0 += 1;
            entry.1 = now;
        }
    }
}

/// What a client span's uninstrumented callee is called: `peer.service`, else the
/// database's name or system — and whether it is a database.
fn virtual_node(span: &SpanRecord) -> Option<(String, bool)> {
    let get = |name: &str| span.attributes.get(name).filter(|v| !v.is_empty());
    if let Some(peer) = get("peer.service") {
        return Some((peer.to_owned(), get("db.system").is_some()));
    }
    get("db.name")
        .or_else(|| get("db.namespace"))
        .or_else(|| get("db.system"))
        .map(|name| (name.to_owned(), true))
}

fn push(
    out: &mut Vec<MetricSample>,
    name: &str,
    key: &Labels,
    extra: &[(&str, &str)],
    value: u64,
    at: u64,
) {
    let mut series = key.clone();
    series.insert(METRIC_NAME_LABEL, name);
    for (k, v) in extra {
        series.insert(*k, *v);
    }
    #[allow(clippy::cast_precision_loss)]
    out.push(MetricSample {
        timestamp_nanos: at,
        series,
        value: value as f64,
        kind: MetricKind::Counter,
    });
}

fn histogram(
    out: &mut Vec<MetricSample>,
    name: &str,
    key: &Labels,
    bounds: &[f64],
    h: &Histogram,
    at: u64,
) {
    let bucket = format!("{name}_bucket");
    let start = out.len();
    for (bound, count) in bounds.iter().zip(&h.buckets) {
        push(out, &bucket, key, &[("le", &bound.to_string())], *count, at);
    }
    push(out, &bucket, key, &[("le", "+Inf")], h.count, at);
    for sample in &mut out[start..] {
        sample.kind = MetricKind::Histogram;
    }
    push(out, &format!("{name}_count"), key, &[], h.count, at);
    let mut series = key.clone();
    series.insert(METRIC_NAME_LABEL, format!("{name}_sum"));
    out.push(MetricSample {
        timestamp_nanos: at,
        series,
        value: h.sum,
        kind: MetricKind::Counter,
    });
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::float_cmp)]

    use super::*;

    const SECOND: u64 = 1_000_000_000;

    fn span(
        service: &str,
        id: &str,
        parent: Option<&str>,
        kind: SpanKind,
        millis: u64,
        attributes: &[(&str, &str)],
    ) -> SpanRecord {
        let mut stream = Labels::new();
        stream.insert("service_name", service);
        stream.insert("app", service);
        SpanRecord {
            trace_id: "t1".into(),
            span_id: id.into(),
            parent_span_id: parent.map(str::to_owned),
            name: format!("{service} op"),
            kind,
            start_nanos: 0,
            end_nanos: millis * 1_000_000,
            status: SpanStatus::Unset,
            status_message: String::new(),
            stream,
            attributes: labels(attributes),
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    fn value(samples: &[MetricSample], name: &str, pairs: &[(&str, &str)]) -> Option<f64> {
        samples
            .iter()
            .find(|s| {
                s.series.get(METRIC_NAME_LABEL) == Some(name)
                    && pairs.iter().all(|(k, v)| s.series.get(k) == Some(*v))
            })
            .map(|s| s.value)
    }

    fn both() -> Generator {
        Generator::new(
            Processors {
                span_metrics: true,
                service_graphs: true,
            },
            10 * SECOND,
        )
    }

    #[test]
    fn a_client_and_its_server_are_one_edge_in_either_order() {
        let mut g = both();
        g.observe(&[span("web", "b", Some("a"), SpanKind::Server, 80, &[])], 0);
        g.observe(&[span("gateway", "a", None, SpanKind::Client, 100, &[])], 0);
        let samples = g.collect(SECOND);
        let edge = [("client", "gateway"), ("server", "web")];
        assert_eq!(
            value(&samples, "traces_service_graph_request_total", &edge),
            Some(1.0)
        );
        assert_eq!(
            value(
                &samples,
                "traces_service_graph_request_server_seconds_sum",
                &edge
            ),
            Some(0.08)
        );
        assert_eq!(
            value(
                &samples,
                "traces_service_graph_request_client_seconds_bucket",
                &[("client", "gateway"), ("le", "0.1")]
            ),
            Some(1.0)
        );
    }

    #[test]
    fn an_uninstrumented_callee_is_a_virtual_node_once_it_has_waited() {
        let mut g = both();
        g.observe(
            &[span(
                "web",
                "c",
                Some("b"),
                SpanKind::Client,
                5,
                &[("db.system", "mysql")],
            )],
            0,
        );
        assert!(
            value(
                &g.collect(SECOND),
                "traces_service_graph_request_total",
                &[]
            )
            .is_none()
        );
        let samples = g.collect(11 * SECOND);
        assert_eq!(
            value(
                &samples,
                "traces_service_graph_request_total",
                &[
                    ("client", "web"),
                    ("server", "mysql"),
                    ("connection_type", "database")
                ]
            ),
            Some(1.0)
        );
    }

    #[test]
    fn a_root_server_span_is_called_by_user_and_a_lone_server_is_unpaired() {
        let mut g = both();
        g.observe(&[span("web", "a", None, SpanKind::Server, 50, &[])], 0);
        g.observe(
            &[span("api", "z", Some("gone"), SpanKind::Server, 50, &[])],
            0,
        );
        let samples = g.collect(11 * SECOND);
        assert_eq!(
            value(
                &samples,
                "traces_service_graph_request_total",
                &[("client", "user"), ("server", "web")]
            ),
            Some(1.0)
        );
        assert_eq!(
            value(
                &samples,
                "traces_service_graph_unpaired_spans_total",
                &[("server", "api")]
            ),
            Some(1.0)
        );
    }

    #[test]
    fn every_consumer_of_one_message_is_an_edge() {
        let mut g = both();
        g.observe(&[span("shop", "p", None, SpanKind::Producer, 1, &[])], 0);
        g.observe(
            &[
                span("mailer", "c1", Some("p"), SpanKind::Consumer, 5, &[]),
                span("audit", "c2", Some("p"), SpanKind::Consumer, 5, &[]),
            ],
            0,
        );
        let samples = g.collect(11 * SECOND);
        for server in ["mailer", "audit"] {
            assert_eq!(
                value(
                    &samples,
                    "traces_service_graph_request_total",
                    &[
                        ("client", "shop"),
                        ("server", server),
                        ("connection_type", "messaging_system")
                    ]
                ),
                Some(1.0),
                "{server}"
            );
        }
        assert!(
            value(&samples, "traces_service_graph_unpaired_spans_total", &[]).is_none(),
            "a paired producer is not unpaired when its wait ends"
        );
    }

    #[test]
    fn span_metrics_count_calls_and_bucket_latency() {
        let mut g = both();
        g.observe(
            &[
                span("web", "a", None, SpanKind::Server, 3, &[]),
                span("web", "b", None, SpanKind::Server, 300, &[]),
            ],
            0,
        );
        let samples = g.collect(SECOND);
        let key = [("service", "web"), ("span_kind", "SPAN_KIND_SERVER")];
        assert_eq!(
            value(&samples, "traces_spanmetrics_calls_total", &key),
            Some(2.0)
        );
        assert_eq!(
            value(
                &samples,
                "traces_spanmetrics_latency_bucket",
                &[("service", "web"), ("le", "0.004")]
            ),
            Some(1.0)
        );
        assert_eq!(
            value(
                &samples,
                "traces_spanmetrics_latency_bucket",
                &[("service", "web"), ("le", "+Inf")]
            ),
            Some(2.0)
        );
        // Untouched for longer than the stale window, a series stops being written.
        assert!(g.collect(16 * 60 * SECOND).is_empty());
    }
}
