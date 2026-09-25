//! Label names and values drawn from a set of matched streams.
//!
//! Shared by the Prometheus and Loki metadata endpoints, which scope the same way —
//! Prometheus with `match[]`, Loki with `query` — and must answer the same way.

use std::collections::BTreeSet;

use telemetryd_core::Labels;

/// Every label name any of the streams carries, sorted.
#[must_use]
pub fn names_of(streams: &BTreeSet<Labels>) -> Vec<String> {
    let mut names: BTreeSet<&str> = BTreeSet::new();
    for labels in streams {
        names.extend(labels.iter().map(|(name, _)| name));
    }
    names.into_iter().map(str::to_owned).collect()
}

/// Every value `name` takes across the streams, sorted.
#[must_use]
pub fn values_of(streams: &BTreeSet<Labels>, name: &str) -> Vec<String> {
    let values: BTreeSet<&str> = streams
        .iter()
        .filter_map(|labels| labels.get(name))
        .collect();
    values.into_iter().map(str::to_owned).collect()
}
