//! OTLP/HTTP ingest handlers.

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use telemetryd_core::Error;
use telemetryd_ingest::logs::{self, DecodeContext};
use telemetryd_ingest::otlp_metrics;
use telemetryd_ingest::remote_write;
use telemetryd_ingest::traces;

use crate::error::ApiError;
use crate::state::AppState;

/// OTLP's partial-success envelope.
///
/// The mechanism that lets one 2 MB log body not cost the other 499 records in the
/// batch: the request succeeds, and the response says exactly how many were refused
/// and why. Counts are strings because proto3 JSON encodes int64 that way, and a
/// client parsing this strictly will reject a bare number.
#[derive(Debug, Default, Serialize)]
pub struct OtlpResponse {
    #[serde(rename = "partialSuccess", skip_serializing_if = "Option::is_none")]
    pub partial_success: Option<PartialSuccess>,
}

#[derive(Debug, Serialize)]
pub struct PartialSuccess {
    #[serde(rename = "rejectedLogRecords", skip_serializing_if = "Option::is_none")]
    pub rejected_log_records: Option<String>,
    #[serde(rename = "rejectedSpans", skip_serializing_if = "Option::is_none")]
    pub rejected_spans: Option<String>,
    #[serde(rename = "rejectedDataPoints", skip_serializing_if = "Option::is_none")]
    pub rejected_data_points: Option<String>,
    #[serde(rename = "errorMessage")]
    pub error_message: String,
}

/// `POST /v1/logs`
pub async fn otlp_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Bound concurrent ingest. A rejected request is a signal the producer can act on
    // — back off, batch harder — where an accepted one that queues behind a hundred
    // others is the unbounded buffering `limits.ingest_queue_depth` exists to prevent.
    let Some(_slot) = state.ingest_slot() else {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "logs"), ("reason", "queue_full")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };

    reject_protobuf(&headers)?;

    let mut decoded = {
        let limits = state.config.limits.clone();
        let ingest = state.config.ingest.clone();
        let now = telemetryd_store::now_nanos();
        logs::decode(
            &body,
            DecodeContext {
                limits: &limits,
                ingest: &ingest,
                now_nanos: now,
            },
        )
        .map_err(|e| {
            state.metrics.incr(
                "telemetryd_ingest_rejected_total",
                &[("signal", "logs"), ("reason", "malformed_json")],
            );
            Error::BadRequest(format!("could not decode the OTLP logs payload: {e}"))
        })?
    };

    for rejection in &decoded.rejections {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "logs"), ("reason", rejection.reason.as_str())],
        );
    }
    if decoded.rescaled_timestamps > 0 {
        state.metrics.add(
            "telemetryd_ingest_timestamps_rescaled_total",
            &[("signal", "logs")],
            decoded.rescaled_timestamps,
        );
    }
    if decoded.truncated_bodies > 0 {
        state.metrics.add(
            "telemetryd_ingest_bodies_truncated_total",
            &[("signal", "logs")],
            decoded.truncated_bodies,
        );
    }

    if !decoded.records.is_empty() {
        // Live tail before storage: a subscriber should see a line the moment it is
        // accepted, and the fan-out must not be able to fail the write.
        state.publish_tail(&decoded.records);

        let store = std::sync::Arc::clone(&state.store);
        let records = decoded.records.clone();

        // The store is synchronous and fsyncs; running it on the async runtime would
        // stall every other connection on this worker.
        let admitted = tokio::task::spawn_blocking(move || store.append_logs(&records))
            .await
            .map_err(|e| Error::Config(format!("ingest task panicked: {e}")))??;
        decoded.note_series_rejections(admitted.rejected, admitted.reason);
        let accepted = admitted.stored as u64;

        state.metrics.add(
            "telemetryd_ingest_accepted_total",
            &[("signal", "logs")],
            accepted,
        );
    }

    let response = OtlpResponse {
        partial_success: decoded.rejection_summary().map(|message| PartialSuccess {
            rejected_log_records: Some(decoded.rejected().to_string()),
            rejected_spans: None,
            rejected_data_points: None,
            error_message: message,
        }),
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}

/// `POST /v1/traces`
pub async fn otlp_traces(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Bound concurrent ingest. A rejected request is a signal the producer can act on
    // — back off, batch harder — where an accepted one that queues behind a hundred
    // others is the unbounded buffering `limits.ingest_queue_depth` exists to prevent.
    let Some(_slot) = state.ingest_slot() else {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "traces"), ("reason", "queue_full")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };

    reject_protobuf(&headers)?;

    let mut decoded = {
        let limits = state.config.limits.clone();
        let ingest = state.config.ingest.clone();
        let now = telemetryd_store::now_nanos();
        traces::decode(&body, traces::context(&limits, &ingest, now)).map_err(|e| {
            state.metrics.incr(
                "telemetryd_ingest_rejected_total",
                &[("signal", "traces"), ("reason", "malformed_json")],
            );
            Error::BadRequest(format!("could not decode the OTLP traces payload: {e}"))
        })?
    };

    for rejection in &decoded.rejections {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "traces"), ("reason", rejection.reason.as_str())],
        );
    }
    if decoded.rescaled_timestamps > 0 {
        state.metrics.add(
            "telemetryd_ingest_timestamps_rescaled_total",
            &[("signal", "traces")],
            decoded.rescaled_timestamps,
        );
    }

    if !decoded.records.is_empty() {
        let store = std::sync::Arc::clone(&state.store);
        let records = decoded.records.clone();

        let admitted = tokio::task::spawn_blocking(move || store.append_spans(&records))
            .await
            .map_err(|e| Error::Config(format!("ingest task panicked: {e}")))??;
        decoded.note_series_rejections(admitted.rejected, admitted.reason);
        let accepted = admitted.stored as u64;

        state.metrics.add(
            "telemetryd_ingest_accepted_total",
            &[("signal", "traces")],
            accepted,
        );
    }

    let response = OtlpResponse {
        partial_success: decoded.rejection_summary().map(|message| PartialSuccess {
            rejected_log_records: None,
            rejected_spans: Some(decoded.rejected().to_string()),
            rejected_data_points: None,
            error_message: message,
        }),
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}

/// `POST /v1/metrics`
pub async fn otlp_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Bound concurrent ingest. A rejected request is a signal the producer can act on
    // — back off, batch harder — where an accepted one that queues behind a hundred
    // others is the unbounded buffering `limits.ingest_queue_depth` exists to prevent.
    let Some(_slot) = state.ingest_slot() else {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "metrics"), ("reason", "queue_full")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };

    reject_protobuf(&headers)?;

    let decoded = {
        let limits = state.config.limits.clone();
        let ingest = state.config.ingest.clone();
        let now = telemetryd_store::now_nanos();
        otlp_metrics::decode(
            &body,
            otlp_metrics::MetricContext {
                limits: &limits,
                ingest: &ingest,
                now_nanos: now,
            },
        )
        .map_err(|e| {
            state.metrics.incr(
                "telemetryd_ingest_rejected_total",
                &[("signal", "metrics"), ("reason", "malformed_json")],
            );
            Error::BadRequest(format!("could not decode the OTLP metrics payload: {e}"))
        })?
    };

    store_samples(&state, decoded).await
}

/// `POST /api/v1/write` — Prometheus remote_write.
pub async fn remote_write(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Bound concurrent ingest. A rejected request is a signal the producer can act on
    // — back off, batch harder — where an accepted one that queues behind a hundred
    // others is the unbounded buffering `limits.ingest_queue_depth` exists to prevent.
    let Some(_slot) = state.ingest_slot() else {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "metrics"), ("reason", "queue_full")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };

    let decoded = {
        let limits = state.config.limits.clone();
        remote_write::decode(
            &body,
            remote_write::WriteContext {
                limits: &limits,
                default_app: telemetryd_core::record::UNKNOWN_APP,
            },
        )
        .inspect_err(|_| {
            state.metrics.incr(
                "telemetryd_ingest_rejected_total",
                &[("signal", "metrics"), ("reason", "malformed_protobuf")],
            );
        })?
    };

    // remote_write has no partial-success envelope; Prometheus expects 204 on success
    // and treats anything else as a failure worth retrying.
    let response = store_samples(&state, decoded).await?;
    if response.status().is_success() {
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    Ok(response)
}

/// Shared tail of both metric ingest paths.
async fn store_samples(
    state: &AppState,
    mut decoded: telemetryd_ingest::Decoded<telemetryd_core::MetricSample>,
) -> Result<Response, ApiError> {
    for rejection in &decoded.rejections {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "metrics"), ("reason", rejection.reason.as_str())],
        );
    }
    if decoded.rescaled_timestamps > 0 {
        state.metrics.add(
            "telemetryd_ingest_timestamps_rescaled_total",
            &[("signal", "metrics")],
            decoded.rescaled_timestamps,
        );
    }

    if !decoded.records.is_empty() {
        let store = std::sync::Arc::clone(&state.store);
        let records = decoded.records.clone();

        let admitted = tokio::task::spawn_blocking(move || store.append_samples(&records))
            .await
            .map_err(|e| Error::Config(format!("ingest task panicked: {e}")))??;
        decoded.note_series_rejections(admitted.rejected, admitted.reason);
        let accepted = admitted.stored as u64;

        state.metrics.add(
            "telemetryd_ingest_accepted_total",
            &[("signal", "metrics")],
            accepted,
        );
    }

    let response = OtlpResponse {
        partial_success: decoded.rejection_summary().map(|message| PartialSuccess {
            rejected_log_records: None,
            rejected_spans: None,
            rejected_data_points: Some(decoded.rejected().to_string()),
            error_message: message,
        }),
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}

/// OTLP/HTTP also defines a protobuf encoding. We speak JSON, and saying so beats a
/// parse error full of binary.
fn reject_protobuf(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.starts_with("application/x-protobuf")
        || content_type.starts_with("application/protobuf")
    {
        return Err(Error::unsupported_with_hint(
            "OTLP/HTTP protobuf encoding",
            "send OTLP/HTTP with JSON encoding (Content-Type: application/json)",
        )
        .into());
    }
    Ok(())
}

/// Whether a request looks like OTLP JSON.
pub fn is_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_none_or(|value| value.starts_with("application/json") || value.is_empty())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn headers(content_type: &str) -> HeaderMap {
        let mut map = HeaderMap::new();
        if !content_type.is_empty() {
            map.insert(
                header::CONTENT_TYPE,
                axum::http::HeaderValue::from_str(content_type).unwrap(),
            );
        }
        map
    }

    #[test]
    fn protobuf_payloads_are_named_not_parsed() {
        let err = reject_protobuf(&headers("application/x-protobuf")).unwrap_err();
        assert!(matches!(err.0, Error::Unsupported { .. }));
        assert!(err.0.to_string().contains("protobuf"));
    }

    #[test]
    fn json_and_missing_content_types_are_accepted() {
        assert!(reject_protobuf(&headers("application/json")).is_ok());
        assert!(reject_protobuf(&headers("application/json; charset=utf-8")).is_ok());
        assert!(reject_protobuf(&headers("")).is_ok());
        assert!(is_json(&headers("application/json")));
        assert!(
            is_json(&headers("")),
            "a missing content-type defaults to JSON"
        );
    }

    #[test]
    fn the_partial_success_envelope_matches_otlps_shape() {
        let response = OtlpResponse {
            partial_success: Some(PartialSuccess {
                rejected_log_records: Some("3".to_owned()),
                rejected_spans: None,
                rejected_data_points: None,
                error_message: "3 record(s) rejected".to_owned(),
            }),
        };
        let json = serde_json::to_value(&response).unwrap();

        assert_eq!(json["partialSuccess"]["rejectedLogRecords"], "3");
        assert!(json["partialSuccess"].get("rejectedSpans").is_none());
        assert!(
            json["partialSuccess"]["rejectedLogRecords"].is_string(),
            "OTLP encodes int64 as a string; a number breaks strict clients"
        );
    }

    #[test]
    fn a_clean_batch_omits_partial_success_entirely() {
        let json = serde_json::to_value(OtlpResponse::default()).unwrap();
        assert_eq!(json, serde_json::json!({}));
    }
}
