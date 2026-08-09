//! Every metric `/metrics` describes must be able to carry a value.
//!
//! The exposition format is self-describing: each metric ships a `# HELP` and a
//! `# TYPE` line so a scraper knows what it is looking at. That is a promise, and five
//! metrics were breaking it — declared in the descriptor table, documented in the
//! output, and never given a value by any code path:
//!
//! - `telemetryd_wal_unsynced_records`, which is how much a power cut would cost right
//!   now and precisely the sort of thing you would build an alert on
//! - `telemetryd_wal_segments` and `telemetryd_wal_records_total`
//! - `telemetryd_query_segments_scanned_total` and `..._pruned_total`, the pair that
//!   says whether segment pruning is earning its keep
//!
//! The data existed in all five cases. They were simply never wired to the exporter, so
//! anyone graphing them got an empty panel and no error — the failure mode of a metric
//! that does not exist is indistinguishable from one that is always zero.
//!
//! This is the same shape as `config_is_wired`: a grep, not a proof. It cannot tell
//! whether a value is *correct*, only that something somewhere emits it, which is the
//! state all five were not in.

use std::collections::BTreeSet;
use std::path::Path;

fn read(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{relative} should be readable: {e}"))
}

/// Metric names from the descriptor table.
fn declared() -> BTreeSet<String> {
    let source = read("crates/server/src/metrics.rs");
    let mut names = BTreeSet::new();
    for line in source.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("name: \"") else {
            continue;
        };
        if let Some(name) = rest.split('"').next() {
            names.insert(name.to_owned());
        }
    }
    assert!(
        names.len() > 20,
        "expected to find the descriptor table, found {} names",
        names.len()
    );
    names
}

/// Everywhere a metric name is used outside the descriptor table itself.
fn emitting_source() -> String {
    let mut source = String::new();
    for file in [
        "crates/server/src/routes.rs",
        "crates/server/src/ingest.rs",
        "crates/server/src/auth.rs",
        "crates/server/src/state.rs",
        "crates/server/src/tail.rs",
        "crates/server/src/loki.rs",
        "crates/server/src/tempo.rs",
        "crates/server/src/prometheus.rs",
        "crates/server/src/relay.rs",
        "crates/server/src/maintenance.rs",
        "crates/server/src/lib.rs",
    ] {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(file);
        if let Ok(text) = std::fs::read_to_string(path) {
            source.push_str(&text);
        }
    }
    assert!(!source.is_empty(), "no server source was read");
    source
}

#[test]
fn every_described_metric_is_emitted_somewhere() {
    let emitting = emitting_source();

    let dead: Vec<String> = declared()
        .into_iter()
        .filter(|name| !emitting.contains(name.as_str()))
        .collect();

    assert!(
        dead.is_empty(),
        "these metrics are described in /metrics output but nothing ever gives them a \
         value, so a scraper sees HELP and TYPE for a number that cannot exist:\n  {}\n\
         Either emit them or delete the descriptor — an advertised metric that is \
         permanently absent is worse than one that was never promised.",
        dead.join("\n  ")
    );
}
