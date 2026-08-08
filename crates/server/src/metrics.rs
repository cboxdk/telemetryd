//! telemetryd's own metrics, in Prometheus text exposition format.
//!
//! Hand-rolled rather than pulling in a metrics facade: the surface is a few dozen
//! counters and gauges, the exposition format is stable and simple, and the dependency
//! budget is a product constraint here (one static binary, no surprises). If this ever
//! needs histograms with exemplars, revisit.
//!
//! The counters declared here are half the "degrade loudly" contract from the brief —
//! every rejection path increments one, so a limit being hit is observable rather than
//! inferred from missing data.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::RwLock;

/// A metric name plus its sorted label set.
type Series = (&'static str, Vec<(String, String)>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Counter,
    Gauge,
}

#[derive(Debug, Clone, Copy)]
pub struct Descriptor {
    pub name: &'static str,
    pub kind: Kind,
    pub help: &'static str,
}

/// Every metric telemetryd exposes. Declared up front so `/metrics` emits `# HELP`
/// and `# TYPE` even for a counter that has not been incremented yet — a missing
/// series and a zero series mean very different things to an alerting rule.
pub const DESCRIPTORS: &[Descriptor] = &[
    Descriptor {
        name: "telemetryd_build_info",
        kind: Kind::Gauge,
        help: "Build metadata; always 1, carried by its labels",
    },
    Descriptor {
        name: "telemetryd_uptime_seconds",
        kind: Kind::Gauge,
        help: "Seconds since this process started serving",
    },
    Descriptor {
        name: "telemetryd_http_requests_total",
        kind: Kind::Counter,
        help: "HTTP requests handled, by route, method and status class",
    },
    Descriptor {
        name: "telemetryd_auth_failures_total",
        kind: Kind::Counter,
        help: "Requests rejected for a missing or invalid bearer token, by surface",
    },
    Descriptor {
        name: "telemetryd_ingest_rejected_total",
        kind: Kind::Counter,
        help: "Records rejected at ingest, by signal and reason — never a silent drop",
    },
    Descriptor {
        name: "telemetryd_disk_used_bytes",
        kind: Kind::Gauge,
        help: "Bytes on disk in the data directory, by subtree",
    },
    Descriptor {
        name: "telemetryd_disk_budget_bytes",
        kind: Kind::Gauge,
        help: "Configured disk budget; the reaper drops oldest-first to stay under it",
    },
    Descriptor {
        name: "telemetryd_storage_over_budget",
        kind: Kind::Gauge,
        help: "1 when disk usage exceeds the configured budget",
    },
    Descriptor {
        name: "telemetryd_ingest_accepted_total",
        kind: Kind::Counter,
        help: "Records accepted at ingest, by signal",
    },
    Descriptor {
        name: "telemetryd_ingest_timestamps_rescaled_total",
        kind: Kind::Counter,
        help: "Records whose timestamp was in the wrong unit and was corrected — a producer bug",
    },
    Descriptor {
        name: "telemetryd_ingest_bodies_truncated_total",
        kind: Kind::Counter,
        help: "Log bodies that exceeded max_log_line_bytes and were truncated",
    },
    Descriptor {
        name: "telemetryd_records_buffered",
        kind: Kind::Gauge,
        help: "Records held in the in-memory buffer, not yet sealed into a segment",
    },
    Descriptor {
        name: "telemetryd_records_appended_total",
        kind: Kind::Counter,
        help: "Records appended since start, by signal",
    },
    Descriptor {
        name: "telemetryd_segments",
        kind: Kind::Gauge,
        help: "Sealed segments on disk, by signal",
    },
    Descriptor {
        name: "telemetryd_segment_rows",
        kind: Kind::Gauge,
        help: "Rows held in sealed segments, by signal",
    },
    Descriptor {
        name: "telemetryd_segments_sealed_total",
        kind: Kind::Counter,
        help: "Segments sealed since start, by signal",
    },
    Descriptor {
        name: "telemetryd_oidc_keys",
        kind: Kind::Gauge,
        help: "Cbox ID signing keys currently cached — zero means every Cbox ID token is being refused",
    },
    Descriptor {
        name: "telemetryd_relay_pending_segments",
        kind: Kind::Gauge,
        help: "Sealed segments not yet accepted upstream, by signal — the number to alert on, since delivered totals only ever rise",
    },
    Descriptor {
        name: "telemetryd_relay_segments_delivered_total",
        kind: Kind::Counter,
        help: "Segments accepted upstream since start",
    },
    Descriptor {
        name: "telemetryd_relay_records_delivered_total",
        kind: Kind::Counter,
        help: "Records accepted upstream since start",
    },
    Descriptor {
        name: "telemetryd_relay_failures_total",
        kind: Kind::Counter,
        help: "Failed delivery attempts since start. Retried from the cursor, not lost",
    },
    Descriptor {
        name: "telemetryd_relay_identity_overridden_total",
        kind: Kind::Counter,
        help: "Records whose claimed app was replaced by the credential's, by app — a client that keeps claiming another app is misconfigured or probing",
    },
    Descriptor {
        name: "telemetryd_app_series",
        kind: Kind::Gauge,
        help: "Distinct series per app — what limits.max_series_per_app is enforced against",
    },
    Descriptor {
        name: "telemetryd_app_rows",
        kind: Kind::Gauge,
        help: "Rows in sealed segments per app",
    },
    Descriptor {
        name: "telemetryd_app_bytes_estimate",
        kind: Kind::Gauge,
        help: "Disk per app, apportioned by row share — an estimate, since a segment mixes apps",
    },
    Descriptor {
        name: "telemetryd_series_active",
        kind: Kind::Gauge,
        help: "Distinct series counted against limits.max_series — the number that decides when ingest starts refusing new ones",
    },
    Descriptor {
        name: "telemetryd_series_rejected_total",
        kind: Kind::Counter,
        help: "Records refused because their series would exceed a cardinality cap",
    },
    Descriptor {
        name: "telemetryd_series_limit",
        kind: Kind::Gauge,
        help: "Configured limits.max_series, so the active count can be alerted on as a ratio",
    },
    Descriptor {
        name: "telemetryd_segments_unreadable_total",
        kind: Kind::Counter,
        help: "Query reads skipped because a segment file is damaged — non-zero means data was lost",
    },
    Descriptor {
        name: "telemetryd_query_segments_scanned_total",
        kind: Kind::Counter,
        help: "Segments opened and decoded to answer queries — the number to watch when queries slow down",
    },
    Descriptor {
        name: "telemetryd_query_segments_pruned_total",
        kind: Kind::Counter,
        help: "Segments queries skipped with no I/O, via time range, label index, Bloom filter or limit cutoff",
    },
    Descriptor {
        name: "telemetryd_retention_deleted_total",
        kind: Kind::Counter,
        help: "Segments deleted by retention, by reason (age or disk_budget)",
    },
    Descriptor {
        name: "telemetryd_tail_subscribers",
        kind: Kind::Gauge,
        help: "Live-tail WebSocket clients currently connected",
    },
    Descriptor {
        name: "telemetryd_tail_connections_total",
        kind: Kind::Counter,
        help: "Live-tail connections opened since start",
    },
    Descriptor {
        name: "telemetryd_tail_disconnects_total",
        kind: Kind::Counter,
        help: "Live-tail connections closed since start",
    },
    Descriptor {
        name: "telemetryd_tail_dropped_total",
        kind: Kind::Counter,
        help: "Entries a live-tail client missed by falling behind the buffer",
    },
    Descriptor {
        name: "telemetryd_wal_segments",
        kind: Kind::Gauge,
        help: "Write-ahead log segments on disk, by signal",
    },
    Descriptor {
        name: "telemetryd_wal_records_total",
        kind: Kind::Counter,
        help: "Records appended to the write-ahead log since start, by signal",
    },
    Descriptor {
        name: "telemetryd_wal_unsynced_records",
        kind: Kind::Gauge,
        help: "Records written but not yet fsynced, by signal",
    },
    Descriptor {
        name: "telemetryd_wal_truncations_total",
        kind: Kind::Counter,
        help: "Torn write-ahead log tails repaired at startup; non-zero means a crash cost records",
    },
];

/// Process-lifetime counters.
#[derive(Debug, Default)]
pub struct Metrics {
    counters: RwLock<BTreeMap<Series, u64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn incr(&self, name: &'static str, labels: &[(&str, &str)]) {
        self.add(name, labels, 1);
    }

    pub fn add(&self, name: &'static str, labels: &[(&str, &str)], by: u64) {
        let key = (name, normalise(labels));
        let mut counters = self
            .counters
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *counters.entry(key).or_insert(0) += by;
    }

    pub fn get(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        let key = normalise(labels);
        self.counters
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|((n, l), _)| *n == name && *l == key)
            .map_or(0, |(_, v)| *v)
    }

    /// Render the exposition, merging live gauge samples supplied by the caller.
    ///
    /// Gauges are passed in rather than stored because their truth lives elsewhere —
    /// disk usage comes from a directory walk, uptime from the clock. Caching them
    /// here would let `/metrics` report a number that is no longer true.
    #[allow(clippy::cast_precision_loss)] // exposition format is f64; counters stay far below 2^53
    pub fn render(&self, gauges: &[Sample]) -> String {
        let counters = self
            .counters
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut out = String::with_capacity(4096);
        for descriptor in DESCRIPTORS {
            let mut lines: Vec<String> = counters
                .iter()
                .filter(|((name, _), _)| *name == descriptor.name)
                .map(|((name, labels), value)| format_line(name, labels, *value as f64))
                .collect();
            lines.extend(
                gauges
                    .iter()
                    .filter(|sample| sample.name == descriptor.name)
                    .map(|sample| format_line(sample.name, &sample.labels, sample.value)),
            );

            let _ = writeln!(out, "# HELP {} {}", descriptor.name, descriptor.help);
            let _ = writeln!(
                out,
                "# TYPE {} {}",
                descriptor.name,
                match descriptor.kind {
                    Kind::Counter => "counter",
                    Kind::Gauge => "gauge",
                }
            );
            for line in lines {
                out.push_str(&line);
                out.push('\n');
            }
        }
        out
    }
}

/// One gauge reading, produced at scrape time.
#[derive(Debug, Clone)]
pub struct Sample {
    pub name: &'static str,
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

impl Sample {
    pub fn new(name: &'static str, labels: &[(&str, &str)], value: f64) -> Self {
        Self {
            name,
            labels: normalise(labels),
            value,
        }
    }
}

fn normalise(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut labels: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    labels.sort();
    labels
}

fn format_line(name: &str, labels: &[(String, String)], value: f64) -> String {
    let mut line = String::from(name);
    if !labels.is_empty() {
        line.push('{');
        for (i, (key, value)) in labels.iter().enumerate() {
            if i > 0 {
                line.push(',');
            }
            let _ = write!(line, "{key}=\"{}\"", escape(value));
        }
        line.push('}');
    }
    let _ = write!(line, " {value}");
    line
}

/// Escape a label value per the Prometheus text format. Skipped escaping here would
/// produce a silently unparseable scrape the first time an app name contains a quote.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_per_label_set() {
        let metrics = Metrics::new();
        metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "logs"), ("reason", "too_large")],
        );
        metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "logs"), ("reason", "too_large")],
        );
        metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "logs"), ("reason", "cardinality")],
        );

        assert_eq!(
            metrics.get(
                "telemetryd_ingest_rejected_total",
                &[("signal", "logs"), ("reason", "too_large")]
            ),
            2
        );
        assert_eq!(
            metrics.get(
                "telemetryd_ingest_rejected_total",
                &[("reason", "cardinality"), ("signal", "logs")]
            ),
            1,
            "label order must not create a distinct series"
        );
    }

    #[test]
    fn every_declared_metric_gets_help_and_type_even_when_unused() {
        let rendered = Metrics::new().render(&[]);
        for descriptor in DESCRIPTORS {
            assert!(
                rendered.contains(&format!("# HELP {} ", descriptor.name)),
                "missing HELP for {}",
                descriptor.name
            );
            assert!(
                rendered.contains(&format!("# TYPE {} ", descriptor.name)),
                "missing TYPE for {}",
                descriptor.name
            );
        }
    }

    #[test]
    fn label_values_are_escaped() {
        let metrics = Metrics::new();
        metrics.incr(
            "telemetryd_http_requests_total",
            &[("route", "we\"ird\\path")],
        );
        let rendered = metrics.render(&[]);
        assert!(
            rendered.contains(r#"route="we\"ird\\path""#),
            "quotes and backslashes must be escaped: {rendered}"
        );
    }

    #[test]
    fn gauges_are_rendered_under_their_declared_type() {
        let rendered = Metrics::new().render(&[
            Sample::new("telemetryd_disk_used_bytes", &[("kind", "wal")], 4096.0),
            Sample::new("telemetryd_uptime_seconds", &[], 1.5),
        ]);
        assert!(rendered.contains("telemetryd_disk_used_bytes{kind=\"wal\"} 4096"));
        assert!(rendered.contains("telemetryd_uptime_seconds 1.5"));
    }
}
