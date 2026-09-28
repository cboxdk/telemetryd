//! What `/api/v1/query` answers for shapes other than a plain vector, against a store.

#![allow(clippy::unwrap_used)]

use telemetryd_core::config::Config;
use telemetryd_core::{Labels, MetricKind, MetricSample};
use telemetryd_query::prometheus::{InstantParams, instant};
use telemetryd_store::Store;

const SECOND: u64 = 1_000_000_000;
const T0: u64 = 1_790_000_000 * SECOND;

fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    for route in ["/a", "/b"] {
        let samples: Vec<MetricSample> = (0..40u64)
            .map(|i| {
                let mut series = Labels::new();
                series.insert("__name__", "requests_total");
                series.insert("route", route);
                MetricSample {
                    series,
                    timestamp_nanos: T0 + i * 30 * SECOND,
                    #[allow(clippy::cast_precision_loss)]
                    value: (i * 10) as f64,
                    kind: MetricKind::Counter,
                }
            })
            .collect();
        store.metrics().append(&samples).unwrap();
    }
    (dir, store)
}

fn ask(store: &Store, query: &str, at_secs: u64) -> serde_json::Value {
    let params = InstantParams {
        query: Some(query.to_owned()),
        time: Some(at_secs.to_string()),
        timeout: None,
    };
    serde_json::to_value(instant(store.metrics(), &params, 0, 0).unwrap()).unwrap()
}

/// `x[5m]` at one instant is each series' raw samples in `(t - 5m, t]`, name and all, as
/// Prometheus answers it. It used to come back as a vector of one value per series.
#[test]
fn a_range_selector_is_answered_with_its_samples() {
    let (_dir, store) = store();
    let at = T0 / SECOND + 600;
    let json = ask(&store, "requests_total[5m]", at);
    assert_eq!(json["data"]["resultType"], "matrix");
    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0]["metric"]["__name__"], "requests_total");
    assert_eq!(result[0]["metric"]["route"], "/a");
    let values = result[0]["values"].as_array().unwrap();
    // Left-open: the sample exactly five minutes back is outside, the one at `at` inside.
    assert_eq!(values.len(), 10);
    assert_eq!(values[0][0], serde_json::json!(at - 270));
    assert_eq!(values[9], serde_json::json!([at, "200"]));
}

#[test]
fn a_subquery_is_answered_with_its_points() {
    let (_dir, store) = store();
    let at = T0 / SECOND + 600;
    let json = ask(&store, "sum(requests_total)[2m:1m]", at);
    assert_eq!(json["data"]["resultType"], "matrix");
    let values = json["data"]["result"][0]["values"].as_array().unwrap();
    // Points sit on multiples of the step in absolute time, as Prometheus places them:
    // `at` is twenty seconds past a minute, so the last is twenty seconds before it.
    assert_eq!(values.len(), 2);
    assert_eq!(values[1], serde_json::json!([at - 20, "380"]));
}

#[test]
fn a_function_of_a_range_selector_is_still_a_vector() {
    let (_dir, store) = store();
    let json = ask(&store, "increase(requests_total[5m])", T0 / SECOND + 600);
    assert_eq!(json["data"]["resultType"], "vector");
}
