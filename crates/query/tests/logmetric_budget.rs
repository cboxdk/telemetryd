//! A metric LogQL query stops when its time is up, rather than holding a query slot for
//! an answer nobody is waiting for any more.

#![allow(clippy::unwrap_used)]

use telemetryd_core::config::Config;
use telemetryd_core::{Labels, LogRecord, Severity};
use telemetryd_query::logmetric::{self, Deadline};
use telemetryd_store::Store;

#[test]
fn a_query_past_its_deadline_is_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    let base = 1_790_000_000_000_000_000u64;
    let mut stream = Labels::new();
    stream.insert("app", "x");
    let records: Vec<LogRecord> = (0..100u64)
        .map(|i| LogRecord {
            timestamp_nanos: base + i * 1_000_000_000,
            stream: stream.clone(),
            severity: Severity::Info,
            severity_text: String::new(),
            body: format!("line {i}"),
            attributes: Labels::new(),
            trace_id: None,
            span_id: None,
        })
        .collect();
    store.logs().append(&records).unwrap();

    let query = logmetric::parse(r#"count_over_time({app="x"}[1m])"#).unwrap();
    let steps: Vec<u64> = (0..5000u64).map(|i| base + i * 1_000_000_000).collect();
    let answered = logmetric::answer_range(store.logs(), &query, &steps, Deadline(None));
    assert!(answered.is_ok(), "{answered:?}");

    let past = Deadline(Some(std::time::Instant::now()));
    let error = logmetric::answer_range(store.logs(), &query, &steps, past).unwrap_err();
    assert!(error.to_string().contains("request_timeout"), "{error}");
}
