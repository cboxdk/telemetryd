//! OTLP/HTTP ingest handlers.

use std::borrow::Cow;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use telemetryd_core::Error;
use telemetryd_ingest::compression::{self, Encoding};
use telemetryd_ingest::logs::{self, DecodeContext};
use telemetryd_ingest::otlp_metrics;
use telemetryd_ingest::remote_write;
use telemetryd_ingest::traces;

use crate::auth::ClientIdentity;
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

/// Overwrite what a client claimed to be with what its credential says it is.
///
/// The security boundary of relay mode. `app` arrives in the payload, which
/// means the least trusted party picks it — fine when every writer is something you
/// deployed, and not fine when the writer is a mobile binary anyone can extract a
/// token from. Every alert, dashboard and retention rule downstream is keyed on this
/// label.
///
/// Does nothing unless relay mode is on with `trust_client_identity = false`, and
/// nothing when the request carried no identity — an unidentified writer cannot be
/// stamped, and config validation refuses that combination at startup rather than
/// leaving it to be discovered here.
fn stamp<'a, I>(state: &AppState, identity: Option<&ClientIdentity>, streams: I)
where
    I: Iterator<Item = &'a mut telemetryd_core::Labels>,
{
    if state.config.relay.trust_client_identity || !state.config.relay.is_enabled() {
        return;
    }
    let Some(identity) = identity else {
        return;
    };
    let mut stamped = 0u64;
    for stream in streams {
        if stream.get(telemetryd_core::record::APP_LABEL) != Some(identity.app.as_str()) {
            stamped += 1;
        }
        stream.insert(
            telemetryd_core::record::APP_LABEL.to_owned(),
            identity.app.clone(),
        );
    }
    if stamped > 0 {
        // Worth counting: a client that keeps claiming someone else's app is either
        // misconfigured or probing, and either way you want to see it before it is a
        // support ticket.
        state.metrics.add(
            "telemetryd_relay_identity_overridden_total",
            &[("app", identity.app.as_str())],
            stamped,
        );
    }
}

/// `POST /v1/logs`
pub async fn otlp_logs(
    State(state): State<AppState>,
    identity: Option<axum::Extension<ClientIdentity>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let identity = identity.map(|axum::Extension(identity)| identity);
    // Bound concurrent ingest. A rejected request is a signal the producer can act on
    // — back off, batch harder — where an accepted one that queues behind a hundred
    // others is the unbounded buffering `limits.ingest_queue_depth` exists to prevent.
    let Some(_slot) = state.ingest_slot_for(identity.as_ref().map(|i| i.app.as_str())) else {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "logs"), ("reason", "queue_full")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };

    let encoding = encoding(&headers);
    let body = decompress(&state, &headers, &body, "logs", &[])?;

    let mut decoded = {
        let limits = state.config.limits.clone();
        let ingest = state.config.ingest.clone();
        let now = telemetryd_store::now_nanos();
        let ctx = DecodeContext {
            limits: &limits,
            ingest: &ingest,
            now_nanos: now,
        };
        match encoding {
            // One conversion for both encodings: the protobuf decoder produces the same
            // structs the JSON one does, so limits, rejections and counters below cannot
            // differ by how the batch arrived.
            Wire::Protobuf => telemetryd_ingest::otlp_protobuf::logs(&body)
                .map(|data| logs::convert_data(&data, ctx))
                .map_err(|e| {
                    reject(&state, "logs", "malformed_protobuf");
                    Error::BadRequest(format!("could not decode the OTLP logs payload: {e}"))
                })?,
            Wire::Json => logs::decode(&body, ctx).map_err(|e| {
                reject(&state, "logs", "malformed_json");
                Error::BadRequest(format!("could not decode the OTLP logs payload: {e}"))
            })?,
        }
    };

    // Before the tail, before storage, before the per-app counters: everything
    // downstream must see the identity the credential proved, never the one the
    // payload asked for.
    stamp(
        &state,
        identity.as_ref(),
        decoded.records.iter_mut().map(|record| &mut record.stream),
    );

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
    identity: Option<axum::Extension<ClientIdentity>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let identity = identity.map(|axum::Extension(identity)| identity);
    // Bound concurrent ingest. A rejected request is a signal the producer can act on
    // — back off, batch harder — where an accepted one that queues behind a hundred
    // others is the unbounded buffering `limits.ingest_queue_depth` exists to prevent.
    let Some(_slot) = state.ingest_slot_for(identity.as_ref().map(|i| i.app.as_str())) else {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "traces"), ("reason", "queue_full")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };

    let encoding = encoding(&headers);
    let body = decompress(&state, &headers, &body, "traces", &[])?;

    let mut decoded = {
        let limits = state.config.limits.clone();
        let ingest = state.config.ingest.clone();
        let now = telemetryd_store::now_nanos();
        let ctx = traces::context(&limits, &ingest, now);
        match encoding {
            Wire::Protobuf => telemetryd_ingest::otlp_protobuf::traces(&body)
                .map(|data| traces::convert_data(&data, ctx))
                .map_err(|e| {
                    reject(&state, "traces", "malformed_protobuf");
                    Error::BadRequest(format!("could not decode the OTLP traces payload: {e}"))
                })?,
            Wire::Json => traces::decode(&body, ctx).map_err(|e| {
                reject(&state, "traces", "malformed_json");
                Error::BadRequest(format!("could not decode the OTLP traces payload: {e}"))
            })?,
        }
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

    stamp(
        &state,
        identity.as_ref(),
        decoded.records.iter_mut().map(|record| &mut record.stream),
    );

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
    identity: Option<axum::Extension<ClientIdentity>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let identity = identity.map(|axum::Extension(identity)| identity);
    // Bound concurrent ingest. A rejected request is a signal the producer can act on
    // — back off, batch harder — where an accepted one that queues behind a hundred
    // others is the unbounded buffering `limits.ingest_queue_depth` exists to prevent.
    let Some(_slot) = state.ingest_slot_for(identity.as_ref().map(|i| i.app.as_str())) else {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "metrics"), ("reason", "queue_full")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };

    let encoding = encoding(&headers);
    let body = decompress(&state, &headers, &body, "metrics", &[])?;

    let decoded = {
        let limits = state.config.limits.clone();
        let ingest = state.config.ingest.clone();
        let now = telemetryd_store::now_nanos();
        let ctx = otlp_metrics::MetricContext {
            limits: &limits,
            ingest: &ingest,
            now_nanos: now,
        };
        match encoding {
            Wire::Protobuf => telemetryd_ingest::otlp_protobuf::metrics(&body)
                .map(|data| otlp_metrics::convert_data(&data, ctx))
                .map_err(|e| {
                    reject(&state, "metrics", "malformed_protobuf");
                    Error::BadRequest(format!("could not decode the OTLP metrics payload: {e}"))
                })?,
            Wire::Json => otlp_metrics::decode(&body, ctx).map_err(|e| {
                reject(&state, "metrics", "malformed_json");
                Error::BadRequest(format!("could not decode the OTLP metrics payload: {e}"))
            })?,
        }
    };

    let mut decoded = decoded;
    stamp(
        &state,
        identity.as_ref(),
        decoded.records.iter_mut().map(|sample| &mut sample.series),
    );
    store_samples(&state, decoded).await
}

/// `POST /api/v1/write` — Prometheus remote_write.
pub async fn remote_write(
    State(state): State<AppState>,
    identity: Option<axum::Extension<ClientIdentity>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let identity = identity.map(|axum::Extension(identity)| identity);
    // Bound concurrent ingest. A rejected request is a signal the producer can act on
    // — back off, batch harder — where an accepted one that queues behind a hundred
    // others is the unbounded buffering `limits.ingest_queue_depth` exists to prevent.
    let Some(_slot) = state.ingest_slot_for(identity.as_ref().map(|i| i.app.as_str())) else {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "metrics"), ("reason", "queue_full")],
        );
        return Err(telemetryd_core::Error::Overloaded.into());
    };

    // Prometheus sends `Content-Encoding: snappy`, and that snappy is the payload's
    // own framing rather than a transport coding — `remote_write::decode` owns it. So
    // it passes through here untouched, while a gzip added by a proxy in front of us
    // is still undone.
    let body = decompress(
        &state,
        &headers,
        &body,
        "metrics",
        compression::REMOTE_WRITE_PASSTHROUGH,
    )?;

    let decoded = {
        let limits = state.config.limits.clone();
        remote_write::decode(
            &body,
            remote_write::WriteContext {
                limits: &limits,
                default_app: telemetryd_core::record::UNKNOWN_APP,
                max_decompressed: usize::try_from(state.config.server.max_body_bytes.as_u64())
                    .unwrap_or(usize::MAX),
            },
        )
        .inspect_err(|_| {
            state.metrics.incr(
                "telemetryd_ingest_rejected_total",
                &[("signal", "metrics"), ("reason", "malformed_protobuf")],
            );
        })?
    };

    let mut decoded = decoded;
    stamp(
        &state,
        identity.as_ref(),
        decoded.records.iter_mut().map(|sample| &mut sample.series),
    );

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

/// Undo `Content-Encoding` before the body reaches a decoder.
///
/// OTLP/HTTP makes gzip part of the specification, and every OpenTelemetry SDK
/// compresses batches past some size threshold — so a server that ignores the header
/// is not "missing an optimisation", it is broken for every batch that carries data
/// while still answering 200 to the empty one a health check sends.
///
/// The cap is `server.max_body_bytes`, the same number `RequestBodyLimitLayer`
/// enforces on an uncompressed body, so the two paths agree: a client refused for
/// sending 20 MiB of JSON is refused for sending the 30 KB of gzip that becomes it,
/// and gets the same 413. Decompression happens while holding the ingest slot, which
/// bounds how many of these buffers can exist at once.
fn decompress<'a>(
    state: &AppState,
    headers: &HeaderMap,
    body: &'a Bytes,
    signal: &'static str,
    already_handled: &[&str],
) -> Result<Cow<'a, [u8]>, ApiError> {
    let Some(value) = headers.get(header::CONTENT_ENCODING) else {
        return Ok(Cow::Borrowed(body));
    };
    let Ok(value) = value.to_str() else {
        reject(state, signal, "unsupported_encoding");
        return Err(
            Error::BadRequest("the Content-Encoding header is not valid text".to_owned()).into(),
        );
    };

    let encoding = Encoding::parse(value, already_handled).inspect_err(|_| {
        reject(state, signal, "unsupported_encoding");
    })?;
    if encoding == Encoding::Identity {
        return Ok(Cow::Borrowed(body));
    }

    let max_body =
        usize::try_from(state.config.server.max_body_bytes.as_u64()).unwrap_or(usize::MAX);
    let decoded = compression::decode(encoding, body, max_body).inspect_err(|e| {
        let reason = if matches!(e, Error::LimitExceeded { .. }) {
            "decompressed_body_too_large"
        } else {
            "malformed_encoding"
        };
        reject(state, signal, reason);
    })?;

    if let Cow::Owned(bytes) = &decoded {
        tracing::debug!(
            signal,
            encoding = encoding.as_str(),
            compressed_bytes = body.len(),
            decompressed_bytes = bytes.len(),
            "decompressed an ingest body"
        );
    }
    Ok(decoded)
}

fn reject(state: &AppState, signal: &'static str, reason: &'static str) {
    state.metrics.incr(
        "telemetryd_ingest_rejected_total",
        &[("signal", signal), ("reason", reason)],
    );
}

/// Which of OTLP/HTTP's two encodings a request used.
///
/// Both are served. Protobuf is the default in every official OpenTelemetry SDK, so a
/// JSON-only backend rejects a stock exporter on every batch; JSON is what
/// `cboxdk/laravel-telemetry` sends and keeps a C extension off the client's path. The
/// two decode into the same structs and share one conversion, so the encoding decides
/// how a batch is parsed and nothing else about how it is treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wire {
    Json,
    Protobuf,
}

/// An absent `Content-Type` is read as JSON.
///
/// It is what a hand-written `curl` omits, and guessing binary for a request with no
/// declared type would turn a typo into an unreadable parse error.
fn encoding(headers: &HeaderMap) -> Wire {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.starts_with("application/x-protobuf")
        || content_type.starts_with("application/protobuf")
    {
        Wire::Protobuf
    } else {
        Wire::Json
    }
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
    fn protobuf_payloads_are_decoded_rather_than_named() {
        // This test used to assert the opposite: that a protobuf content type produced
        // an `Unsupported` error naming the encoding. That was the correct behaviour
        // while only JSON was served, and it is what made every stock OpenTelemetry SDK
        // — all of which default to `http/protobuf` — store nothing.
        assert_eq!(encoding(&headers("application/x-protobuf")), Wire::Protobuf);
        assert_eq!(encoding(&headers("application/protobuf")), Wire::Protobuf);
        assert_eq!(
            encoding(&headers("application/x-protobuf; charset=binary")),
            Wire::Protobuf
        );
    }

    #[test]
    fn json_and_missing_content_types_are_accepted() {
        assert_eq!(encoding(&headers("application/json")), Wire::Json);
        assert_eq!(
            encoding(&headers("application/json; charset=utf-8")),
            Wire::Json
        );
        // An absent type is JSON, not binary: it is what a hand-written curl omits, and
        // guessing binary would turn a typo into an unreadable parse error.
        assert_eq!(encoding(&headers("")), Wire::Json);
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
