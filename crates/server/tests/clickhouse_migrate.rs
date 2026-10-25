//! `telemetryd clickhouse migrate` and `import`, against a real ClickHouse.
//!
//! - A database laid out by an earlier version — the trace-id index a plain
//!   `MergeTree`, folds and sample counts partitioned by month — is rebuilt with every
//!   row kept exactly once, though the first run is interrupted part way; a second run
//!   finishes it and a third does nothing.
//! - A rollup created after the rows it summarises is filled in for them, exactly once,
//!   including rows with old timestamps it had already counted, and though the first
//!   run is interrupted.
//! - Objects earlier versions created are dropped by name, and nothing else is.
//! - An index a table lacks is added, and the partitions written before it are indexed.
//! - A single node's data directory, imported, answers as the node did.
//!
//! Run when `TELEMETRYD_TEST_CLICKHOUSE_URL` is set; the rebuild also on a cluster when
//! `TELEMETRYD_TEST_CLICKHOUSE_CLUSTER` and `TELEMETRYD_TEST_CLICKHOUSE_REPLICAS` are.

#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use std::fmt::Write as _;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use telemetryd_core::Config;
use telemetryd_core::config::{ClickHouseConfig, StorageConfig};
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use telemetryd_store::clickhouse::migrate::{self, Event, Options};
use telemetryd_store::clickhouse::{ClickHouse, Client};
use tower::ServiceExt;

mod common;
use common::{clickhouse_under_test, long_retention};

const SECOND: u64 = 1_000_000_000;
const HOUR: u64 = 3_600 * SECOND;
const DAY: u64 = 24 * HOUR;

fn now() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    )
    .unwrap()
}

/// The first value a query answers, as text.
fn one(client: &Client, sql: &str) -> String {
    let mut out = String::new();
    client
        .query_lines(&format!("SELECT toString(({sql})) AS v"), |line| {
            let value: Value = serde_json::from_slice(line).unwrap();
            value["v"].as_str().unwrap_or_default().clone_into(&mut out);
            Ok(std::ops::ControlFlow::Break(()))
        })
        .unwrap();
    out
}

/// Run a migration, stopping it — as a crash would, mid-run — at the first event
/// `stop` picks.
fn run_until(
    client: &Client,
    config: &ClickHouseConfig,
    options: Options,
    stop: impl Fn(&Event) -> bool,
) -> Vec<Event> {
    let mut events = Vec::new();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        migrate::run(client, config, &long_retention(), options, &mut |event| {
            events.push(event.clone());
            assert!(!stop(event), "stopped here, as a crash would");
        })
    }));
    if let Ok(result) = outcome {
        result.unwrap();
    }
    events
}

fn finished(events: &[Event]) -> (usize, usize) {
    events
        .iter()
        .find_map(|e| match e {
            Event::Done { changed, pending } => Some((*changed, *pending)),
            _ => None,
        })
        .expect("the run finished")
}

fn take_over() -> Options {
    Options {
        take_over: true,
        ..Options::default()
    }
}

// ---------------------------------------------------------------------------------
// Rebuilding tables an earlier version laid out.

/// An engine clause as an earlier version wrote it: replicated on a cluster.
fn old_engine(kind: &str, table: &str, cluster: bool, version: bool) -> String {
    let version = if version { "Version" } else { "" };
    if cluster {
        let sep = if version.is_empty() { "" } else { ", " };
        format!(
            "Replicated{kind}('/clickhouse/tables/{{shard}}/{{database}}/{table}', '{{replica}}'{sep}{version})"
        )
    } else {
        format!("{kind}({version})")
    }
}

/// Lay out the trace-id index, the folds and the sample counts as earlier versions did,
/// and fill them: spans and samples through the store, folds as an instance writes them.
fn old_layout(clickhouse: &ClickHouse, config: &ClickHouseConfig, days: u64) {
    let client = clickhouse.client();
    let cluster = !config.cluster.is_empty();
    let on = if cluster {
        format!(" ON CLUSTER '{}'", config.cluster)
    } else {
        String::new()
    };
    for table in [
        "otel_traces_trace_id_ts",
        "telemetryd_folds",
        "telemetryd_sample_counts",
    ] {
        client
            .execute(&format!("DROP TABLE IF EXISTS {table}{on} SYNC"))
            .unwrap();
    }
    client
        .execute(&format!(
            "CREATE TABLE otel_traces_trace_id_ts{on} (
    TraceId String CODEC(ZSTD(1)),
    Start DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1)),
    End DateTime64(9, 'UTC') CODEC(Delta(8), ZSTD(1))
) ENGINE = {}
PARTITION BY toDate(Start)
ORDER BY (TraceId, toUnixTimestamp(Start))",
            old_engine("MergeTree", "otel_traces_trace_id_ts", cluster, false)
        ))
        .unwrap();
    client
        .execute(&format!(
            "CREATE TABLE telemetryd_sample_counts{on} (
    MetricName LowCardinality(String),
    Hour UInt32,
    SeriesHash UInt64,
    Samples SimpleAggregateFunction(sum, UInt64)
) ENGINE = {}
PARTITION BY intDiv(Hour, 24 * 30)
ORDER BY (MetricName, Hour, SeriesHash)",
            old_engine(
                "AggregatingMergeTree",
                "telemetryd_sample_counts",
                cluster,
                false
            )
        ))
        .unwrap();
    client
        .execute(&format!(
            "CREATE TABLE telemetryd_folds{on} (
    MetricName LowCardinality(String),
    Tier UInt8,
    Bucket UInt32,
    SeriesHash UInt64,
    Starts DateTime CODEC(Delta(4), ZSTD(1)),
    Seen UInt64, FirstNanos UInt64, LastNanos UInt64, FirstValue UInt64, LastValue UInt64,
    Increase UInt64, Sum UInt64, Compensation UInt64, Min UInt64, Max UInt64,
    Version UInt64
) ENGINE = {}
PARTITION BY (Tier, toYYYYMM(Starts))
ORDER BY (MetricName, Tier, Bucket, SeriesHash)",
            old_engine("ReplacingMergeTree", "telemetryd_folds", cluster, true)
        ))
        .unwrap();

    // `days` back to now: several old partitions, and many new.
    let start = now() - days * DAY;
    let spans: Vec<telemetryd_core::span::SpanRecord> = (0..days * 36)
        .map(|i| {
            let at = start + i * 40 * 60 * SECOND;
            let mut stream = telemetryd_core::Labels::new();
            stream.insert("app", "geo");
            telemetryd_core::span::SpanRecord {
                trace_id: format!("{:032x}", i / 3 + 1),
                span_id: format!("{:016x}", i + 1),
                parent_span_id: None,
                name: "GET /geocode".to_owned(),
                kind: telemetryd_core::span::SpanKind::Server,
                start_nanos: at,
                end_nanos: at + 20_000_000,
                status: telemetryd_core::span::SpanStatus::Ok,
                status_message: String::new(),
                stream,
                attributes: telemetryd_core::Labels::new(),
                events: Vec::new(),
                links: Vec::new(),
            }
        })
        .collect();
    clickhouse.spans.append(&spans).unwrap();
    let samples: Vec<telemetryd_core::metric::MetricSample> = (0..days * 288)
        .map(|i| {
            let mut series = telemetryd_core::Labels::new();
            series.insert("__name__", "http_requests_total");
            series.insert("app", "geo");
            series.insert("route", ["/a", "/b", "/c"][usize::try_from(i % 3).unwrap()]);
            telemetryd_core::metric::MetricSample {
                timestamp_nanos: start + i * 5 * 60 * SECOND,
                series,
                value: f64::from(u32::try_from(i).unwrap()),
                kind: telemetryd_core::metric::MetricKind::Counter,
            }
        })
        .collect();
    clickhouse.metrics.append(&samples).unwrap();
    let mut folds = String::new();
    for i in 0..days * 24 {
        let hour = (start / HOUR) + i;
        for (tier, bucket) in [(1u8, hour), (2u8, hour / 24)] {
            let starts = bucket * if tier == 1 { 3_600 } else { 86_400 };
            let _ = writeln!(
                folds,
                "{{\"MetricName\":\"http_requests_total\",\"Tier\":{tier},\"Bucket\":{bucket},\
                 \"SeriesHash\":{},\"Starts\":{starts},\"Seen\":{i},\"FirstNanos\":1,\
                 \"LastNanos\":2,\"FirstValue\":3,\"LastValue\":4,\"Increase\":5,\"Sum\":6,\
                 \"Compensation\":0,\"Min\":7,\"Max\":8,\"Version\":{}}}",
                i % 5,
                i % 3
            );
        }
    }
    client
        .insert_bulk("telemetryd_folds", folds.as_bytes())
        .unwrap();
}

/// What the three tables hold, merged however they are: the same before and after a
/// rebuild that kept every row exactly once.
fn fingerprint(client: &Client) -> Vec<String> {
    vec![
        one(
            client,
            "SELECT tuple(count(), sum(cityHash64(TraceId, s, e))) FROM \
             (SELECT TraceId, min(Start) s, max(End) e FROM otel_traces_trace_id_ts GROUP BY TraceId)",
        ),
        one(
            client,
            "SELECT tuple(count(), sum(cityHash64(MetricName, Tier, Bucket, SeriesHash, v))) FROM \
             (SELECT MetricName, Tier, Bucket, SeriesHash, argMax(tuple(Seen, Min, Max, \
             toUnixTimestamp(Starts)), Version) v FROM telemetryd_folds GROUP BY ALL)",
        ),
        one(
            client,
            "SELECT tuple(count(), sum(cityHash64(MetricName, Hour, SeriesHash, s))) FROM \
             (SELECT MetricName, Hour, SeriesHash, sum(Samples) s FROM telemetryd_sample_counts \
             GROUP BY ALL)",
        ),
    ]
}

fn layout_of(client: &Client, table: &str) -> String {
    one(
        client,
        &format!(
            "SELECT concat(engine, ' | ', partition_key, ' | ', sorting_key) FROM system.tables \
             WHERE database = currentDatabase() AND name = '{table}'"
        ),
    )
}

fn rebuilds_keep_every_row_once(
    config: &ClickHouseConfig,
    reader: Option<&ClickHouseConfig>,
    days: u64,
) {
    let clickhouse = ClickHouse::open(config, &long_retention()).unwrap();
    old_layout(&clickhouse, config, days);
    let client = clickhouse.client();
    let before = fingerprint(client);
    assert!(layout_of(client, "telemetryd_folds").contains("toYYYYMM"));

    // Looked at, nothing changed.
    let dry = run_until(
        client,
        config,
        Options {
            dry_run: true,
            ..Options::default()
        },
        |_| false,
    );
    let found: Vec<String> = dry
        .iter()
        .filter_map(|e| match e {
            Event::Found {
                what: "rebuild",
                object,
                ..
            } => Some(object.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        found,
        [
            "otel_traces_trace_id_ts",
            "telemetryd_folds",
            "telemetryd_sample_counts"
        ]
    );
    assert_eq!(fingerprint(client), before);

    // Interrupted after the sample counts' first partition is copied back: the table
    // whose rows are summed, so one copied twice would show.
    let events = run_until(client, config, Options::default(), |e| {
        matches!(e, Event::Step { object, action, .. }
            if object == "telemetryd_sample_counts" && action == "copied")
    });
    assert!(events.iter().all(|e| !matches!(e, Event::Done { .. })));

    // Continued: every row once.
    let events = run_until(client, config, take_over(), |_| false);
    let (changed, pending) = finished(&events);
    assert!(changed > 0);
    assert_eq!(pending, 0, "{events:?}");
    assert_eq!(fingerprint(client), before);
    assert_eq!(
        layout_of(client, "telemetryd_folds").trim_start_matches("Replicated"),
        "ReplacingMergeTree | (Tier, toDate(Starts)) | MetricName, Tier, Bucket, SeriesHash"
    );
    assert!(
        layout_of(client, "otel_traces_trace_id_ts")
            .contains("AggregatingMergeTree | toDate(Start) | TraceId"),
    );
    assert_eq!(
        one(
            client,
            "SELECT count() FROM system.tables WHERE database = currentDatabase() \
             AND name LIKE '%\\_\\_td\\_%'"
        ),
        "0",
        "nothing temporary is left"
    );

    // Done: nothing to do.
    let events = run_until(client, config, Options::default(), |_| false);
    assert_eq!(finished(&events), (0, 0), "{events:?}");

    // Writes still land, and reach the views' new targets.
    let mut stream = telemetryd_core::Labels::new();
    stream.insert("app", "geo");
    let span = telemetryd_core::span::SpanRecord {
        trace_id: "f".repeat(32),
        span_id: "e".repeat(16),
        parent_span_id: None,
        name: "after".to_owned(),
        kind: telemetryd_core::span::SpanKind::Server,
        start_nanos: now(),
        end_nanos: now() + 1,
        status: telemetryd_core::span::SpanStatus::Ok,
        status_message: String::new(),
        stream,
        attributes: telemetryd_core::Labels::new(),
        events: Vec::new(),
        links: Vec::new(),
    };
    clickhouse.spans.append(&[span]).unwrap();
    assert_eq!(
        one(
            client,
            &format!(
                "SELECT count() FROM otel_traces_trace_id_ts WHERE TraceId = '{}'",
                "f".repeat(32)
            )
        ),
        "1"
    );

    // Another replica holds the same.
    if let Some(reader) = reader {
        let other = Client::new(reader).unwrap();
        for table in [
            "otel_traces_trace_id_ts",
            "telemetryd_folds",
            "telemetryd_sample_counts",
        ] {
            other
                .execute(&format!("SYSTEM SYNC REPLICA {table}"))
                .unwrap();
        }
        // The trace-id index has gained the span written since; the rest is as before.
        assert_eq!(fingerprint(&other)[1..], before[1..]);
    }
}

#[test]
fn an_old_layout_is_rebuilt_with_every_row_once() {
    let Some(config) = clickhouse_under_test("migrate_rebuild") else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_URL");
        return;
    };
    // Across three monthly partitions.
    rebuilds_keep_every_row_once(&config, None, 70);
}

#[test]
fn an_old_layout_is_rebuilt_across_a_cluster() {
    let (Ok(cluster), Ok(urls)) = (
        std::env::var("TELEMETRYD_TEST_CLICKHOUSE_CLUSTER"),
        std::env::var("TELEMETRYD_TEST_CLICKHOUSE_REPLICAS"),
    ) else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_CLUSTER");
        return;
    };
    let database = format!("td_test_migrate_cluster_{}", now());
    let configs: Vec<ClickHouseConfig> = urls
        .split(',')
        .map(|raw| {
            let (scheme, rest) = raw.trim().split_once("://").unwrap();
            let (credentials, host) = rest.rsplit_once('@').unwrap_or(("default:", rest));
            let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
            ClickHouseConfig {
                url: format!("{scheme}://{host}"),
                database: database.clone(),
                user: user.to_owned(),
                password: telemetryd_core::Secret::new(password),
                cluster: cluster.clone(),
                ..ClickHouseConfig::default()
            }
        })
        .collect();
    // Shorter: every partition moved on a cluster is a replicated operation.
    rebuilds_keep_every_row_once(&configs[0], configs.get(1), 12);
}

// ---------------------------------------------------------------------------------
// Rollups, objects and indexes, through an instance.

fn instance(dir: &std::path::Path, clickhouse: ClickHouseConfig) -> axum::Router {
    let config = Config {
        storage: StorageConfig {
            data_dir: Some(dir.to_path_buf()),
            ..StorageConfig::default()
        },
        clickhouse,
        retention: long_retention(),
        ..Config::default()
    };
    config.validate().unwrap();
    let store = Arc::new(Store::open(&config).unwrap());
    router(AppState::new(Arc::new(config), store).unwrap())
}

fn kv(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

async fn post(app: &axum::Router, path: &str, body: &Value) {
    let request = Request::post(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{path}");
}

/// Lines and spans every `step` from `from` for `count` steps.
async fn send(app: &axum::Router, from: u64, step: u64, count: u64, tag: &str) {
    let lines: Vec<Value> = (0..count)
        .map(|i| {
            json!({
                "timeUnixNano": (from + i * step).to_string(),
                "severityNumber": if i % 4 == 0 { 17 } else { 9 },
                "body": {"stringValue": format!("{tag} {i} status={}", 200 + i % 3)},
                "attributes": if i % 5 == 0 { vec![kv("env", "prod")] } else { vec![] },
            })
        })
        .collect();
    post(
        app,
        "/v1/logs",
        &json!({"resourceLogs": [{
            "resource": {"attributes": [kv("service.name", "geo")]},
            "scopeLogs": [{"logRecords": lines}],
        }]}),
    )
    .await;
    let spans: Vec<Value> = (0..count)
        .map(|i| {
            json!({
                "traceId": format!("{:032x}", from / SECOND + i),
                "spanId": format!("{:016x}", i + 1),
                "name": "GET /geocode",
                "kind": 2,
                "startTimeUnixNano": (from + i * step).to_string(),
                "endTimeUnixNano": (from + i * step + 1_000_000).to_string(),
                "attributes": [kv(["http.route", "db.rows", "user.id"][usize::try_from(i % 3).unwrap()], "x")],
            })
        })
        .collect();
    post(
        app,
        "/v1/traces",
        &json!({"resourceSpans": [{
            "resource": {"attributes": [kv("service.name", "geo")]},
            "scopeSpans": [{"spans": spans}],
        }]}),
    )
    .await;
}

/// A rollup's rows, summed per key: what its view would hold had it always existed.
fn rollup(client: &Client, table: &str, keys: &str, sums: &str) -> String {
    one(
        client,
        &format!(
            "SELECT tuple(count(), sum(cityHash64({keys}, {sums}))) FROM \
             (SELECT {keys}, {} FROM {table} GROUP BY {keys})",
            sums.split(", ")
                .map(|s| format!("sum({s}) AS {s}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    )
}

/// The same, computed from the rows by the view's own query.
fn from_rows(
    client: &Client,
    config: &ClickHouseConfig,
    view: &str,
    keys: &str,
    sums: &str,
) -> String {
    let select =
        telemetryd_store::clickhouse::schema::view_select(config, &long_retention(), view).unwrap();
    one(
        client,
        &format!("SELECT tuple(count(), sum(cityHash64({keys}, {sums}))) FROM ({select})"),
    )
}

const ROLLUPS: [(&str, &str, &str, &str, &str); 3] = [
    (
        "otel_log_volume",
        "otel_log_volume_mv",
        "log_volume_since",
        "ServiceName, Second, StreamHash, LabelsHash",
        "Lines, Bytes",
    ),
    (
        "otel_log_volume_1m",
        "otel_log_volume_1m_mv",
        "log_volume_1m_since",
        "ServiceName, Minute, StreamHash, LabelsHash",
        "Lines, Bytes",
    ),
    (
        "otel_trace_keys",
        "otel_trace_keys_mv",
        "trace_keys_since",
        "ServiceName, Hour, Scope, Key",
        "Spans",
    ),
];

#[tokio::test]
async fn a_rollup_younger_than_its_rows_is_filled_in_once() {
    let Some(config) = clickhouse_under_test("migrate_backfill") else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_URL");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let app = instance(dir.path(), config.clone());
    let client = Client::new(&config).unwrap();
    let start = (now() / DAY - 5) * DAY;
    send(&app, start, 7 * 60 * SECOND, 800, "old").await;

    // As if the rollups had been created two days ago: empty, then holding what the
    // view counted from then on.
    let created = start + 3 * DAY + 5 * HOUR;
    for (table, view, marker, _, _) in ROLLUPS {
        client.execute(&format!("TRUNCATE TABLE {table}")).unwrap();
        let select =
            telemetryd_store::clickhouse::schema::view_select(&config, &long_retention(), view)
                .unwrap();
        let base = if table == "otel_trace_keys" {
            "otel_traces"
        } else {
            "otel_logs"
        };
        let restricted = select.replace(
            &format!("FROM {base}"),
            &format!(
                "FROM (SELECT * FROM {base} WHERE Timestamp >= \
                 fromUnixTimestamp64Nano(toInt64({created}))) AS {base}"
            ),
        );
        client
            .execute(&format!("INSERT INTO {table} {restricted}"))
            .unwrap();
        client
            .execute(&format!(
                "INSERT INTO telemetryd_meta VALUES ('{marker}', '{created}', now64(9))"
            ))
            .unwrap();
    }
    // Rows with old times written after the rollups were: counted by the view already,
    // so they must not be counted again.
    send(
        &app,
        start + DAY + 11 * SECOND,
        13 * 60 * SECOND,
        50,
        "late",
    )
    .await;

    for (table, view, _, keys, sums) in ROLLUPS {
        assert_ne!(
            rollup(&client, table, keys, sums),
            from_rows(&client, &config, view, keys, sums),
            "{table} starts incomplete"
        );
    }

    // Without --backfill: found, left.
    let events = run_until(&client, &config, Options::default(), |_| false);
    let backfills = events
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::Found {
                    what: "backfill",
                    ..
                }
            )
        })
        .count();
    assert_eq!(backfills, 3, "{events:?}");

    // Interrupted after the first day.
    let options = Options {
        backfill: true,
        ..Options::default()
    };
    run_until(
        &client,
        &config,
        options,
        |e| matches!(e, Event::Step { action, .. } if action == "filled in"),
    );
    let events = run_until(
        &client,
        &config,
        Options {
            take_over: true,
            ..options
        },
        |_| false,
    );
    assert_eq!(finished(&events).1, 0, "{events:?}");
    for (table, view, marker, keys, sums) in ROLLUPS {
        assert_eq!(
            rollup(&client, table, keys, sums),
            from_rows(&client, &config, view, keys, sums),
            "{table} holds every row once"
        );
        assert_eq!(
            telemetryd_store::clickhouse::schema::rollup_start(&client, marker).unwrap(),
            Some(0),
            "{table} is complete from the start"
        );
    }
    // Again: nothing.
    let events = run_until(&client, &config, options, |_| false);
    assert_eq!(finished(&events), (0, 0), "{events:?}");
}

#[tokio::test]
async fn stale_objects_go_by_name_and_missing_indexes_are_built() {
    let Some(config) = clickhouse_under_test("migrate_objects") else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_URL");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let app = instance(dir.path(), config.clone());
    let client = Client::new(&config).unwrap();
    let start = (now() / DAY - 3) * DAY;
    send(&app, start, 9 * 60 * SECOND, 400, "x").await;

    // What an earlier version created, and something of the operator's own beside it.
    client
        .execute(
            "CREATE TABLE otel_trace_attribute_keys (Hour DateTime, ServiceName String, \
             Scope String, Key String, Spans SimpleAggregateFunction(sum, UInt64)) \
             ENGINE = AggregatingMergeTree ORDER BY (Hour, ServiceName, Scope, Key)",
        )
        .unwrap();
    client
        .execute(
            "CREATE MATERIALIZED VIEW otel_trace_attribute_keys_mv TO otel_trace_attribute_keys \
             AS SELECT toStartOfHour(Timestamp) AS Hour, ServiceName, 'span' AS Scope, k AS Key, \
             count() AS Spans FROM otel_traces ARRAY JOIN mapKeys(SpanAttributes) AS k \
             GROUP BY Hour, ServiceName, Scope, Key",
        )
        .unwrap();
    client
        .execute("CREATE TABLE otel_trace_attribute_keys_mine (x UInt8) ENGINE = Memory")
        .unwrap();
    client
        .execute(
            "ALTER TABLE otel_logs ADD INDEX idx_stream_value mapValues(Stream) \
             TYPE bloom_filter(0.01) GRANULARITY 1",
        )
        .unwrap();
    // An index the table should have and lacks, as one added after its rows would.
    client
        .execute("ALTER TABLE otel_logs DROP INDEX idx_trace_id")
        .unwrap();

    let events = run_until(&client, &config, Options::default(), |_| false);
    let names = one(
        &client,
        "SELECT arraySort(groupArray(name)) FROM system.tables \
         WHERE database = currentDatabase() AND name LIKE 'otel_trace_attribute_keys%'",
    );
    assert_eq!(names, "['otel_trace_attribute_keys_mine']", "{events:?}");
    let indexes = one(
        &client,
        "SELECT arraySort(groupArray(name)) FROM system.data_skipping_indices \
         WHERE database = currentDatabase() AND table = 'otel_logs'",
    );
    assert!(indexes.contains("idx_trace_id"), "{indexes}");
    assert!(!indexes.contains("idx_stream_value"), "{indexes}");
    let unindexed = events
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::Found {
                    what: "materialize_index",
                    ..
                }
            )
        })
        .count();
    assert!(unindexed > 0, "the old partitions lack it: {events:?}");

    let events = run_until(
        &client,
        &config,
        Options {
            materialize_indexes: true,
            ..Options::default()
        },
        |_| false,
    );
    assert_eq!(finished(&events).1, 0, "{events:?}");
    let events = run_until(
        &client,
        &config,
        Options {
            dry_run: true,
            ..Options::default()
        },
        |_| false,
    );
    assert_eq!(finished(&events), (0, 0), "{events:?}");
    // And the index answers: the lookup reads one granule's worth, not the table.
    let trace = format!("{:032x}", start / SECOND + 7);
    assert_eq!(
        one(
            &client,
            &format!(
                "SELECT count() FROM otel_logs WHERE TraceId = '{trace}' \
                 SETTINGS force_data_skipping_indices = 'idx_trace_id'"
            )
        ),
        "0"
    );
}

// ---------------------------------------------------------------------------------
// A single node, imported.

async fn ask(app: &axum::Router, path: &str) -> Value {
    let request = Request::get(path).body(Body::empty()).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let mut value: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    if let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) {
        data.remove("stats");
    }
    if let Some(metrics) = value.get_mut("metrics").and_then(Value::as_object_mut) {
        metrics.remove("inspectedSpans");
    }
    value
}

fn encode(raw: &str) -> String {
    raw.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[tokio::test]
async fn a_single_node_imported_answers_as_it_did() {
    let Some(config) = clickhouse_under_test("migrate_import") else {
        common::skip("TELEMETRYD_TEST_CLICKHOUSE_URL");
        return;
    };
    let local_dir = tempfile::tempdir().unwrap();
    let start = (now() / HOUR - 6) * HOUR;
    let local = instance(local_dir.path(), ClickHouseConfig::default());
    send(&local, start, 11 * SECOND, 1_500, "node").await;
    // Many samples at one timestamp, as a scrape writes them: an import that paged by
    // count would cut through them.
    let points: Vec<Value> = (0..3_000u64)
        .map(|i| {
            json!({
                "timeUnixNano": (start + (i / 1_000) * 60 * SECOND).to_string(),
                "asDouble": f64::from(u32::try_from(i).unwrap()),
                "attributes": [kv("instance", &format!("i{}", i % 1_000))],
            })
        })
        .collect();
    post(
        &local,
        "/v1/metrics",
        &json!({"resourceMetrics": [{
            "resource": {"attributes": [kv("service.name", "geo")]},
            "scopeMetrics": [{"metrics": [{
                "name": "queue.depth",
                "gauge": {"dataPoints": points},
            }]}],
        }]}),
    )
    .await;

    let (from, to) = (start / SECOND, start / SECOND + 6 * 3_600);
    let paths = [
        format!(
            "/loki/api/v1/query_range?query={}&start={from}&end={to}&limit=300",
            encode(r#"{app="geo", level="error"}"#)
        ),
        format!(
            "/loki/api/v1/query_range?query={}&start={from}&end={to}&step=600",
            encode(r#"sum by (level, env) (count_over_time({app="geo"}[10m]))"#)
        ),
        format!("/loki/api/v1/labels?start={from}&end={to}"),
        format!(
            "/api/search?q={}&start={from}&end={to}&limit=50",
            encode("{ span.db.rows = \"x\" }")
        ),
        format!("/api/traces/{:032x}", start / SECOND + 42),
        format!(
            "/api/v1/query_range?query={}&start={from}&end={to}&step=60",
            encode("count(queue_depth)")
        ),
        format!(
            "/api/v1/query?query={}&time={}",
            encode("sum(queue_depth)"),
            start / SECOND + 150
        ),
    ];
    let mut want = Vec::new();
    for path in &paths {
        want.push(ask(&local, path).await);
    }
    drop(local);

    // The node stopped; its directory imported.
    let source_config = Config {
        storage: StorageConfig {
            data_dir: Some(local_dir.path().to_path_buf()),
            ..StorageConfig::default()
        },
        ..Config::default()
    };
    let source = Store::open(&source_config).unwrap();
    let destination = ClickHouse::open(&config, &long_retention()).unwrap();
    let options = telemetryd_store::clickhouse::import::ImportOptions {
        signals: vec![
            telemetryd_core::Signal::Logs,
            telemetryd_core::Signal::Traces,
            telemetryd_core::Signal::Metrics,
        ],
        start: 0,
        end: now(),
        batch: 700,
        source: "test-node".to_owned(),
        restart: false,
    };
    let summary =
        telemetryd_store::clickhouse::import::import(&source, &destination, &options, &mut |_| {})
            .unwrap();
    assert_eq!(
        (summary.logs, summary.traces, summary.metrics),
        (1_500, 1_500, 3_000)
    );
    // Run again: it resumes past everything, and writes nothing twice.
    let again =
        telemetryd_store::clickhouse::import::import(&source, &destination, &options, &mut |_| {})
            .unwrap();
    assert_eq!((again.logs, again.traces, again.metrics), (0, 0, 0));
    drop(source);

    let cluster_dir = tempfile::tempdir().unwrap();
    let remote = instance(cluster_dir.path(), config);
    for (path, want) in paths.iter().zip(&want) {
        assert_ne!(*want, Value::Null, "{path}");
        assert_eq!(&ask(&remote, path).await, want, "{path}");
    }
}
