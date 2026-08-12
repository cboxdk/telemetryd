//! In-process tests against the composed router.
//!
//! No socket is bound and no CLI is involved — the router is the composition root
//!, which keeps these fast enough to run on every commit and makes them the
//! natural home for the M4 contract tests.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::Value;
use telemetryd_core::Config;
use telemetryd_core::config::StorageConfig;
use telemetryd_server::{AppState, router};
use telemetryd_store::Store;
use tower::ServiceExt;

struct Harness {
    router: axum::Router,
    state: AppState,
    _tmp: tempfile::TempDir,
}

impl Harness {
    fn new(configure: impl FnOnce(&mut Config)) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config {
            storage: StorageConfig {
                data_dir: Some(tmp.path().join("data")),
                ..StorageConfig::default()
            },
            ..Config::default()
        };
        configure(&mut config);
        config.validate().unwrap();

        let store = Arc::new(Store::open(&config).unwrap());
        let state = AppState::new(Arc::new(config), store).unwrap();
        Self {
            router: router(state.clone()),
            state,
            _tmp: tmp,
        }
    }

    async fn get(&self, path: &str) -> (StatusCode, Vec<(String, String)>, String) {
        self.request(Request::get(path).body(Body::empty()).unwrap())
            .await
    }

    async fn get_with_token(
        &self,
        path: &str,
        token: &str,
    ) -> (StatusCode, Vec<(String, String)>, String) {
        self.request(
            Request::get(path)
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
    }

    async fn request(&self, request: Request<Body>) -> (StatusCode, Vec<(String, String)>, String) {
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_owned(),
                    v.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, headers, String::from_utf8_lossy(&body).into_owned())
    }
}

fn with_query_token(config: &mut Config) {
    config.auth.query_token = serde_json::from_str(r#""query-secret""#).unwrap();
}

fn with_ingest_token(config: &mut Config) {
    config.auth.ingest_token = serde_json::from_str(r#""ingest-secret""#).unwrap();
}

// ---------------------------------------------------------------------------
// health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn healthz_is_open_even_when_tokens_are_configured() {
    let harness = Harness::new(|config| {
        with_query_token(config);
        with_ingest_token(config);
    });

    let (status, _, body) = harness.get("/healthz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.trim(), "ok");
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_reports_the_shape_operators_and_the_cli_depend_on() {
    let harness = Harness::new(|_| {});
    let (status, _, body) = harness.get("/status").await;
    assert_eq!(status, StatusCode::OK);

    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(json["storage_format_version"], 1);
    assert_eq!(json["insecure"], false);
    assert_eq!(json["auth"]["ingest"], "disabled");
    assert_eq!(json["auth"]["query"], "disabled");
    assert!(json["uptime_seconds"].is_number());
    assert!(json["started_at"].as_str().unwrap().contains('T'));

    // Storage facts the reaper and the disk budget are judged by.
    assert_eq!(
        json["storage"]["disk_budget_bytes"],
        10 * 1024 * 1024 * 1024_u64
    );
    assert_eq!(json["storage"]["over_budget"], false);
    assert!(json["storage"]["data_dir"].is_string());
    assert!(
        json["storage"]["logs"].is_object(),
        "missing log storage stats"
    );
    assert!(
        json["storage"]["retention"].is_object(),
        "missing the retention report"
    );
    assert_eq!(json["storage"]["logs"]["segments"], 0);
    assert_eq!(json["storage"]["logs"]["buffered_records"], 0);

    assert_eq!(json["retention"]["logs"], "7days");
    assert_eq!(json["retention"]["metrics"], "30days");
}

/// Every metric name is a contract with someone's dashboard.
///
/// The test above this one pins seven names by hand out of forty. The other
/// thirty-three could be renamed or dropped and nothing would fail — the same gap
/// `/status` had, on a surface where the consumer is an alert that will simply stop
/// firing rather than error.
///
/// Deriving this list from `DESCRIPTORS` would prove only that the exporter matches its
/// own declaration; removing a metric from both would still pass. So it is written out,
/// and shortening it has to be a deliberate act.
#[tokio::test]
async fn the_exported_metric_names_cannot_change_by_accident() {
    let harness = Harness::new(|_| {});
    let (_, _, body) = harness.get("/metrics").await;

    let mut exported: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix("# HELP "))
        .filter_map(|rest| rest.split_whitespace().next())
        .collect();
    exported.sort_unstable();
    exported.dedup();

    let expected = [
        "telemetryd_app_bytes_estimate",
        "telemetryd_app_rows",
        "telemetryd_app_series",
        "telemetryd_auth_failures_total",
        "telemetryd_build_info",
        "telemetryd_disk_budget_bytes",
        "telemetryd_disk_used_bytes",
        "telemetryd_http_requests_total",
        "telemetryd_ingest_accepted_total",
        "telemetryd_ingest_bodies_truncated_total",
        "telemetryd_ingest_rejected_total",
        "telemetryd_ingest_timestamps_rescaled_total",
        "telemetryd_oidc_keys",
        "telemetryd_query_segments_pruned_total",
        "telemetryd_query_segments_scanned_total",
        "telemetryd_records_appended_total",
        "telemetryd_records_buffered",
        "telemetryd_relay_failures_total",
        "telemetryd_relay_identity_overridden_total",
        "telemetryd_relay_pending_segments",
        "telemetryd_relay_records_delivered_total",
        "telemetryd_relay_segments_delivered_total",
        "telemetryd_retention_deleted_total",
        "telemetryd_segment_rows",
        "telemetryd_segments",
        "telemetryd_segments_sealed_total",
        "telemetryd_segments_unreadable_total",
        "telemetryd_series_active",
        "telemetryd_series_limit",
        "telemetryd_series_rejected_total",
        "telemetryd_storage_over_budget",
        "telemetryd_tail_connections_total",
        "telemetryd_tail_disconnects_total",
        "telemetryd_tail_dropped_total",
        "telemetryd_tail_subscribers",
        "telemetryd_uptime_seconds",
        "telemetryd_wal_records_total",
        "telemetryd_wal_segments",
        "telemetryd_wal_truncations_total",
        "telemetryd_wal_unsynced_records",
    ];
    assert_eq!(
        exported, expected,
        "the exported metric set changed. Adding one is a line here; removing or \
         renaming one silently stops every alert and panel built on it."
    );
}

/// `/status` is a contract, and it had none.
///
/// The query APIs are frozen in COMPATIBILITY.md and contract-tested against the
/// client's own connector source. The *operational* surface — what dashboards, alerts
/// and `telemetryd status` read — was asserted only field by field, so removing a field
/// no test happened to mention passed silently. That is not hypothetical: `milestone`
/// was removed in 0.25.0 and nothing failed, because the assertion that would have
/// caught it was deleted in the same change as part of the removal.
///
/// Asserting the whole key set makes a removal deliberate. Adding one is a one-line
/// edit here; taking one away is a decision someone has to write down.
#[tokio::test]
async fn the_status_key_set_cannot_change_by_accident() {
    let harness = Harness::new(|_| {});
    let (_, _, body) = harness.get("/status").await;
    let json: Value = serde_json::from_str(&body).unwrap();

    let mut keys: Vec<&str> = json
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();

    // `relay` is absent unless relay mode is on, which is its own assertion elsewhere.
    let expected = [
        "apps",
        "auth",
        "insecure",
        "limits",
        "listen",
        "retention",
        "started_at",
        "storage",
        "storage_format_version",
        "tls",
        "uptime_seconds",
        "version",
    ];
    assert_eq!(
        keys, expected,
        "the /status key set changed. If that was intended, update this list — and \
         treat it as a breaking change for anything reading the field that went."
    );
}

#[tokio::test]
async fn status_never_contains_a_token_value() {
    let harness = Harness::new(|config| {
        config.auth.query_token = serde_json::from_str(r#""do-not-leak-me""#).unwrap();
    });

    let (status, _, body) = harness.get_with_token("/status", "do-not-leak-me").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !body.contains("do-not-leak-me"),
        "token leaked into /status: {body}"
    );

    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["auth"]["query"], "enabled");
}

// ---------------------------------------------------------------------------
// auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_surface_rejects_missing_and_wrong_tokens() {
    let harness = Harness::new(with_query_token);

    for path in [
        "/status",
        "/metrics",
        "/api/v1/query",
        "/loki/api/v1/labels",
    ] {
        let (status, headers, body) = harness.get(path).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{path} should require a token"
        );

        // Without this header a client cannot tell *how* to authenticate.
        let challenge = headers.iter().find(|(k, _)| k == "www-authenticate");
        assert!(
            challenge.is_some(),
            "{path} must send a WWW-Authenticate challenge"
        );

        let json: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["error"]["code"], "unauthorized");

        let (status, _, _) = harness.get_with_token(path, "wrong-token").await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{path} accepted a wrong token"
        );
    }

    let (status, _, _) = harness.get_with_token("/status", "query-secret").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn ingest_and_query_tokens_are_independent() {
    let harness = Harness::new(|config| {
        with_query_token(config);
        with_ingest_token(config);
    });

    // The query token must not open the ingest surface, or the split is decorative.
    let (status, _, _) = harness.get_with_token("/v1/logs", "query-secret").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _, _) = harness.get_with_token("/status", "ingest-secret").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Each opens its own.
    let (status, _, _) = harness.get_with_token("/status", "query-secret").await;
    assert_eq!(status, StatusCode::OK);
    // /v1/logs is implemented, so a correct token gets past auth (405 for GET).
    let (status, _, _) = harness.get_with_token("/v1/logs", "ingest-secret").await;
    assert_ne!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn an_unguarded_surface_stays_open_when_only_the_other_is_configured() {
    let harness = Harness::new(with_ingest_token);

    let (status, _, _) = harness.get("/status").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "query surface has no token, so it stays open"
    );

    let (status, _, _) = harness.get("/v1/logs").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn token_rotation_accepts_both_old_and_new() {
    let harness = Harness::new(|config| {
        config.auth.query_token = serde_json::from_str(r#"["old-token", "new-token"]"#).unwrap();
    });

    for token in ["old-token", "new-token"] {
        let (status, _, _) = harness.get_with_token("/status", token).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{token} should be accepted during rotation"
        );
    }
    let (status, _, _) = harness.get_with_token("/status", "retired-token").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// contracted-but-unbuilt endpoints
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_contracted_endpoint_is_implemented() {
    // Nothing in the contract answers 501 any more. If a future build registers a
    // placeholder, this catches it rather than letting it ship silently.
    let harness = Harness::new(|_| {});

    for path in [
        "/v1/logs",
        "/v1/traces",
        "/v1/metrics",
        "/api/v1/write",
        "/loki/api/v1/query_range?query=%7Bapp%3D%22x%22%7D",
        "/loki/api/v1/labels",
        "/loki/api/v1/label/app/values",
        "/loki/api/v1/series",
        "/api/traces/abc123",
        "/api/search",
        "/api/search/tags",
        "/api/v2/search/tag/app/values",
        "/api/v1/status/buildinfo",
        "/api/v1/query?query=up",
        "/api/v1/query_range?query=up",
        "/api/v1/labels",
        "/api/v1/label/app/values",
        "/api/v1/series",
    ] {
        let (status, _, _) = harness.get(path).await;
        assert_ne!(
            status,
            StatusCode::NOT_IMPLEMENTED,
            "{path} still answers 501"
        );
        // A 404 from `/api/traces/{id}` means "no such trace", which is the correct
        // answer on an empty store — it is a routed endpoint either way.
        if !path.starts_with("/api/traces/") {
            assert_ne!(status, StatusCode::NOT_FOUND, "{path} is not routed");
        }
    }
}

// ---------------------------------------------------------------------------
// the index
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_index_never_names_a_route_that_does_not_exist() {
    // The whole justification for building `/` from a table rather than writing prose:
    // an index that drifts from the router sends people to a 404 with confidence. This
    // walks what the page renders against what the server actually answers.
    let harness = Harness::new(|_| {});

    for surface in telemetryd_server::index::SURFACES {
        for route in surface.routes {
            for path in route.paths {
                let concrete = path
                    .replace("{trace_id}", "abc123")
                    .replace("{name}", "app");
                let (status, _, _) = harness.get(&concrete).await;
                // 401 and 405 both prove the route exists; only 404 means the index is
                // pointing at nothing. `/api/traces/{id}` answers 404 for an unknown
                // trace on an empty store, which is a real answer from a real route.
                if !concrete.starts_with("/api/traces/") {
                    assert_ne!(
                        status,
                        StatusCode::NOT_FOUND,
                        "the index lists {path}, which is not routed"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn the_index_is_open_and_says_nothing_about_the_deployment() {
    let harness = Harness::new(|config| {
        with_query_token(config);
        with_ingest_token(config);
    });

    let (status, headers, body) = harness.get("/").await;
    assert_eq!(status, StatusCode::OK, "the index must not need a token");

    // curl sends `*/*`; it should not receive markup.
    let content_type = headers
        .iter()
        .find(|(name, _)| name == "content-type")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    assert!(content_type.starts_with("text/plain"), "{content_type}");
    assert!(body.contains("/loki/api/v1/query_range"), "{body}");

    // Open endpoint. It names the product, not the deployment.
    assert!(!body.contains("query-secret"), "the index printed a token");
    assert!(!body.contains("ingest-secret"), "the index printed a token");
}

#[tokio::test]
async fn a_client_without_a_token_can_still_tell_this_is_telemetryd() {
    // The failure this closes: pointed at a URL it has no credential for, a discovery
    // client got 401 from every guarded route and reported "nothing answered" about a
    // telemetryd that was answering perfectly well. Refusal and silence looked the same.
    let harness = Harness::new(|config| {
        with_query_token(config);
        with_ingest_token(config);
    });

    let (status, headers, body) = harness
        .request(
            Request::get("/")
                .header(header::ACCEPT, "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        headers
            .iter()
            .any(|(name, value)| name == "content-type" && value.starts_with("application/json"))
    );

    let json: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(json["product"], "telemetryd");
    assert_eq!(json["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(json["storage_format_version"], 1);
    assert_eq!(
        json["signals"],
        serde_json::json!(["logs", "metrics", "traces"])
    );

    // Identity, not inventory: nothing here describes what this instance holds.
    for forbidden in ["apps", "storage", "retention", "listen", "uptime_seconds"] {
        assert!(
            json.get(forbidden).is_none(),
            "the open identity document exposed {forbidden}"
        );
    }
    assert!(
        !body.contains("query-secret"),
        "the identity leaked a token"
    );
}

#[tokio::test]
async fn a_browser_gets_html() {
    let harness = Harness::new(|_| {});
    let (status, headers, body) = harness
        .request(
            Request::get("/")
                .header(header::ACCEPT, "text/html,application/xhtml+xml,*/*;q=0.8")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        headers
            .iter()
            .any(|(name, value)| name == "content-type" && value.starts_with("text/html"))
    );
    assert!(body.starts_with("<!doctype html>"), "{body}");
    // Self-contained: an observability backend must not fetch a stylesheet, a font or a
    // script from someone else's server on its own landing page.
    for external in ["<script", "src=\"http", "href=\"http://", "@import"] {
        assert!(!body.contains(external), "the index page loads {external}");
    }
}

#[tokio::test]
async fn a_genuinely_unknown_route_is_still_a_404() {
    let harness = Harness::new(|_| {});
    let (status, _, _) = harness.get("/no/such/endpoint").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ingest_endpoints_accept_post() {
    let harness = Harness::new(|_| {});
    for path in ["/v1/logs", "/v1/traces", "/v1/metrics"] {
        let (status, _, _) = harness
            .request(
                Request::post(path)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{path}");
    }
}

// ---------------------------------------------------------------------------
// self-observability
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metrics_exposition_is_well_formed_prometheus_text() {
    let harness = Harness::new(|_| {});
    let (status, headers, body) = harness.get("/metrics").await;
    assert_eq!(status, StatusCode::OK);

    let content_type = headers.iter().find(|(k, _)| k == "content-type").unwrap();
    assert!(content_type.1.starts_with("text/plain"), "{content_type:?}");

    for metric in [
        "telemetryd_build_info",
        "telemetryd_uptime_seconds",
        "telemetryd_disk_used_bytes",
        "telemetryd_disk_budget_bytes",
        "telemetryd_segments",
        "telemetryd_ingest_rejected_total",
        "telemetryd_retention_deleted_total",
    ] {
        assert!(
            body.contains(&format!("# HELP {metric} ")),
            "missing HELP for {metric}"
        );
        assert!(
            body.contains(&format!("# TYPE {metric} ")),
            "missing TYPE for {metric}"
        );
    }

    // Labels are sorted, so `tls` precedes `version`. The TLS posture rides on
    // `build_info` rather than a gauge of its own because it describes the deployment
    // rather than measuring anything — and `off` is a value rather than an absent
    // series, so an alert can say "nothing should be serving plaintext" without having
    // to guess what a missing series means.
    assert!(
        body.contains(&format!(
            r#"telemetryd_build_info{{tls="off",version="{}"}} 1"#,
            env!("CARGO_PKG_VERSION")
        )),
        "build_info must carry both the version and the TLS posture: {body}"
    );
    assert!(body.contains(r#"telemetryd_segments{signal="logs"}"#));

    // Every non-comment line must parse as `name[{labels}] value`.
    for line in body
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let (_, value) = line
            .rsplit_once(' ')
            .unwrap_or_else(|| panic!("malformed line: {line}"));
        value
            .parse::<f64>()
            .unwrap_or_else(|_| panic!("non-numeric sample value in: {line}"));
    }
}

#[tokio::test]
async fn request_counters_are_keyed_by_matched_route_not_raw_path() {
    let harness = Harness::new(|_| {});

    // A caller must not be able to mint unbounded label cardinality in our own
    // metrics by varying the path — the exact failure telemetryd caps for its users.
    for id in 0..5 {
        harness.get(&format!("/api/traces/trace-{id}")).await;
    }
    let (_, _, body) = harness.get("/metrics").await;

    let series: Vec<&str> = body
        .lines()
        .filter(|line| line.starts_with("telemetryd_http_requests_total{"))
        .filter(|line| line.contains("/api/traces/"))
        .collect();

    assert_eq!(
        series.len(),
        1,
        "expected one collapsed series, got: {series:?}"
    );
    assert!(
        series[0].contains(r#"route="/api/traces/{trace_id}""#),
        "{}",
        series[0]
    );
    assert!(series[0].ends_with(" 5"), "{}", series[0]);
}

#[tokio::test]
async fn auth_failures_are_counted_per_surface() {
    let harness = Harness::new(with_query_token);

    // `/status` is the admin surface, `/loki/api/v1/labels` the query one. They are
    // counted apart so a dashboard can tell "someone is probing the read API" from
    // "someone is probing the operational one".
    harness.get("/status").await;
    harness.get_with_token("/status", "nope").await;
    harness.get_with_token("/loki/api/v1/labels", "nope").await;

    let (_, _, body) = harness.get_with_token("/metrics", "query-secret").await;
    let counted = |surface: &str| {
        body.lines()
            .find(|line| {
                line.starts_with("telemetryd_auth_failures_total{")
                    && line.contains(&format!("surface=\"{surface}\""))
            })
            .unwrap_or_else(|| panic!("no auth failures counted for {surface} in:\n{body}"))
            .to_owned()
    };

    assert!(counted("admin").ends_with(" 2"), "{}", counted("admin"));
    assert!(counted("query").ends_with(" 1"), "{}", counted("query"));
}

/// `server.max_body_bytes` has to be the limit that applies.
///
/// axum's extractors carry their own 2 MB default, and it silently won: a 16 MiB
/// setting rejected anything past 2 MiB, and raising the setting changed nothing. An
/// app batching its logs would hit 413 and the operator would tune a value with no
/// effect — the same class of bug as a memory bound that does not bound memory.
#[tokio::test]
async fn the_configured_body_limit_is_the_one_that_applies() {
    let harness = Harness::new(|config| {
        config.server.max_body_bytes = bytesize::ByteSize::mib(8);
    });

    // Comfortably past axum's 2 MB default, comfortably inside the configured 8 MiB.
    let records: Vec<Value> = (0..30_000)
        .map(|i| {
            serde_json::json!({
                "timeUnixNano": (1_760_000_000_000_000_000u64 + i).to_string(),
                "severityText": "INFO",
                "body": {"stringValue": "x".repeat(80)},
                "attributes": [],
            })
        })
        .collect();
    let payload = serde_json::json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeLogs": [{"logRecords": records}],
        }]
    })
    .to_string();

    assert!(
        payload.len() > 4 * 1024 * 1024,
        "the payload has to exceed axum's default to test anything, got {} bytes",
        payload.len()
    );

    let request = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload))
        .unwrap();
    let (status, _, body) = harness.request(request).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        &body[..body.len().min(200)]
    );
}

/// And a body past the configured limit is still refused, with a status a client can act on.
#[tokio::test]
async fn a_body_past_the_configured_limit_is_refused() {
    let harness = Harness::new(|config| {
        config.server.max_body_bytes = bytesize::ByteSize::mib(1);
    });

    let request = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("x".repeat(2 * 1024 * 1024)))
        .unwrap();
    let (status, _, _) = harness.request(request).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

/// The cardinality cap has to be enforced, not merely configured.
///
/// `limits.max_series` was validated and documented long before it did anything: 400
/// distinct series were stored against a configured cap of 50, with nothing rejected
/// and nothing logged. Unbounded cardinality is how a log store dies, so this is the
/// limit that most needs to be real.
#[tokio::test]
async fn a_new_series_past_the_cap_is_refused_and_reported() {
    let harness = Harness::new(|config| {
        config.limits.max_series = 5;
        config.limits.max_series_per_app = 5;
    });

    let mut accepted = 0;
    let mut refused = 0;
    let mut last_message = String::new();

    for i in 0..20 {
        let payload = serde_json::json!({
            "resourceLogs": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": format!("svc-{i}")}}
                ]},
                "scopeLogs": [{"logRecords": [{
                    "timeUnixNano": (1_786_000_000_000_000_000u64 + i).to_string(),
                    "severityText": "INFO",
                    "body": {"stringValue": "x"},
                    "attributes": [],
                }]}],
            }]
        })
        .to_string();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload))
            .unwrap();
        let (status, _, body) = harness.request(request).await;
        assert_eq!(status, StatusCode::OK, "ingest must not fail outright");

        let parsed: Value = serde_json::from_str(&body).unwrap();
        if let Some(partial) = parsed.get("partialSuccess") {
            refused += 1;
            last_message = partial["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
        } else {
            accepted += 1;
        }
    }

    assert_eq!(accepted, 5, "exactly the cap should have been admitted");
    assert_eq!(refused, 15);

    // Refusals have to be actionable: the producer needs to know which limit and what
    // to do, or a silent drop is indistinguishable from a bug on their side.
    assert!(
        last_message.contains("max_series"),
        "the limit should be named: {last_message}"
    );
    assert!(
        last_message.contains("raise the limit"),
        "the message should say what to do: {last_message}"
    );
}

/// A series already being stored keeps working once the cap is reached.
#[tokio::test]
async fn an_existing_series_still_ingests_at_the_cap() {
    let harness = Harness::new(|config| {
        config.limits.max_series = 1;
        config.limits.max_series_per_app = 1;
    });

    let payload = |i: u64| {
        serde_json::json!({
            "resourceLogs": [{
                "resource": {"attributes": [
                    {"key": "service.name", "value": {"stringValue": "checkout"}}
                ]},
                "scopeLogs": [{"logRecords": [{
                    "timeUnixNano": (1_786_000_000_000_000_000u64 + i).to_string(),
                    "severityText": "INFO",
                    "body": {"stringValue": "x"},
                    "attributes": [],
                }]}],
            }]
        })
        .to_string()
    };

    // Ten batches on the same series, against a cap of one. Refusing these would turn
    // a labelling mistake into an outage of the telemetry that still works.
    for i in 0..10 {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload(i)))
            .unwrap();
        let (status, _, body) = harness.request(request).await;
        assert_eq!(status, StatusCode::OK);
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert!(
            parsed.get("partialSuccess").is_none(),
            "an existing series must keep ingesting: {body}"
        );
    }
}

/// `limits.ingest_queue_depth` has to reject, not queue.
///
/// It documented itself as "a full queue returns 429 with Retry-After rather than
/// buffering without bound" and enforced nothing — the 429 mapping and the Retry-After
/// header both existed already; only the thing that triggers them was missing.
///
/// Rejecting is the point. Queueing the excess would be the unbounded buffering the
/// setting exists to prevent, moved somewhere an operator cannot see it.
#[tokio::test]
async fn a_full_ingest_queue_is_refused_with_retry_after() {
    let harness = Harness::new(|config| {
        config.limits.ingest_queue_depth = 1;
    });

    let payload = serde_json::json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}}
            ]},
            "scopeLogs": [{"logRecords": [{
                "timeUnixNano": "1786000000000000000",
                "severityText": "INFO",
                "body": {"stringValue": "x"},
                "attributes": [],
            }]}],
        }]
    })
    .to_string();

    let request = || {
        Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.clone()))
            .unwrap()
    };

    // Hold the only permit, then offer a second request.
    let held = harness.state.ingest_slot().expect("the first slot is free");
    let (status, headers, _) = harness.request(request()).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert!(
        headers
            .iter()
            .any(|(name, value)| name == "retry-after" && value == "1"),
        "a rejected producer needs to be told when to come back, got {headers:?}"
    );

    // And once the slot frees, ingest resumes rather than staying wedged.
    drop(held);
    let (status, _, _) = harness.request(request()).await;
    assert_eq!(status, StatusCode::OK);
}

/// The admin token separates "may read telemetry" from "may see the deployment".
///
/// `/status` and `/metrics` list every app name, its series count and its share of the
/// disk, and say whether the instance is running unauthenticated. That is a narrower
/// audience than everyone who may read logs.
#[tokio::test]
async fn the_admin_surface_is_separate_from_the_read_surface() {
    let harness = Harness::new(|config| {
        config.auth.query_token = serde_json::from_str(r#""read-token""#).unwrap();
        config.auth.admin_token = serde_json::from_str(r#""admin-token""#).unwrap();
    });

    // The read token opens the query APIs and nothing else.
    let (status, _, _) = harness
        .get_with_token("/loki/api/v1/labels", "read-token")
        .await;
    assert_eq!(status, StatusCode::OK);

    for path in ["/status", "/metrics"] {
        let (status, _, _) = harness.get_with_token(path, "read-token").await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{path} should not open to a read-only token"
        );
        let (status, _, _) = harness.get_with_token(path, "admin-token").await;
        assert_eq!(status, StatusCode::OK, "{path} should open to admin");
    }

    // And admin is not a superset: it does not silently grant reads.
    let (status, _, _) = harness
        .get_with_token("/loki/api/v1/labels", "admin-token")
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// A configuration written before the admin role existed keeps working unchanged.
#[tokio::test]
async fn without_an_admin_token_the_query_token_still_opens_status() {
    let harness = Harness::new(with_query_token);

    for path in ["/status", "/metrics"] {
        let (status, _, _) = harness.get_with_token(path, "query-secret").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{path} must stay reachable for deployments that never set an admin token"
        );
    }
    let (status, _, _) = harness.get("/status").await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "and still not open to nobody"
    );
}
