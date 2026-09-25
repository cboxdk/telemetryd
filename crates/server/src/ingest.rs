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

impl OtlpResponse {
    /// The same answer as an `Export*ServiceResponse` protobuf.
    ///
    /// The three signals' responses share a shape — `partial_success` at field 1, and in
    /// it the rejected count at field 1 and the message at field 2 — so one encoder
    /// serves all of them.
    fn to_protobuf(&self) -> Vec<u8> {
        fn varint(out: &mut Vec<u8>, mut value: u64) {
            loop {
                let byte = u8::try_from(value & 0x7f).unwrap_or(0);
                value >>= 7;
                if value == 0 {
                    out.push(byte);
                    return;
                }
                out.push(byte | 0x80);
            }
        }
        let Some(partial) = &self.partial_success else {
            return Vec::new();
        };
        let rejected: u64 = [
            &partial.rejected_log_records,
            &partial.rejected_spans,
            &partial.rejected_data_points,
        ]
        .iter()
        .find_map(|count| count.as_deref())
        .and_then(|count| count.parse().ok())
        .unwrap_or(0);
        let mut inner = Vec::new();
        if rejected > 0 {
            inner.push(0x08);
            varint(&mut inner, rejected);
        }
        if !partial.error_message.is_empty() {
            inner.push(0x12);
            varint(&mut inner, partial.error_message.len() as u64);
            inner.extend_from_slice(partial.error_message.as_bytes());
        }
        let mut out = vec![0x0a];
        varint(&mut out, inner.len() as u64);
        out.extend(inner);
        out
    }
}

/// Answer an OTLP export in the encoding it arrived in.
///
/// The OTLP/HTTP specification says the response MUST use the request's content type.
/// Protobuf requests were answered in JSON; the Collector and the Go SDK happen to
/// tolerate that, and a stricter client would read the partial success as garbage.
fn answer(wire: Wire, response: &OtlpResponse) -> Response {
    match wire {
        Wire::Json => (StatusCode::OK, Json(response)).into_response(),
        Wire::Protobuf => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/x-protobuf")],
            response.to_protobuf(),
        )
            .into_response(),
    }
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

/// Count every rejection this batch produced, by reason.
///
/// Called after the store has been asked to append, because that is when the full set is
/// known: the decoder's rejections are present from the start, and a record turned away
/// by the series limit is added only once the store reports it. Counting earlier — which
/// is what this used to do — silently omitted the entire second category.
fn count_rejections<T>(
    state: &AppState,
    signal: &'static str,
    decoded: &telemetryd_ingest::Decoded<T>,
) {
    for rejection in &decoded.rejections {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", signal), ("reason", rejection.reason.as_str())],
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

    within_budget(&state, "logs", &decoded)?;

    // Before the tail, before storage, before the per-app counters: everything
    // downstream must see the identity the credential proved, never the one the
    // payload asked for.
    stamp(
        &state,
        identity.as_ref(),
        decoded.records.iter_mut().map(|record| &mut record.stream),
    );

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
        let admitted = crate::fatal::storage(
            tokio::task::spawn_blocking(move || store.append_logs(&records)).await,
            "appending logs",
        )??;
        decoded.note_series_rejections(admitted.rejected, admitted.reason);

        // Counted here, after the store has spoken, rather than before it.
        //
        // The loop used to sit above this call, so it saw only the rejections the decoder
        // had produced. A record refused by the *series limit* is added by the line above
        // — after the counting had already happened — so those never reached
        // `telemetryd_ingest_rejected_total` at all. Measured on a real deployment:
        // `telemetryd_series_rejected_total` at 4,397 and the reason-labelled counter
        // empty, while every log record was being turned away. `partialSuccess` said so
        // on every response and nothing anyone monitors did, which is why it took an hour
        // to find something the server knew immediately.
        count_rejections(&state, "logs", &decoded);
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

    Ok(answer(encoding, &response))
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

    within_budget(&state, "traces", &decoded)?;

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

        let admitted = crate::fatal::storage(
            tokio::task::spawn_blocking(move || store.append_spans(&records)).await,
            "appending spans",
        )??;
        decoded.note_series_rejections(admitted.rejected, admitted.reason);

        // Counted here, after the store has spoken, rather than before it.
        //
        // The loop used to sit above this call, so it saw only the rejections the decoder
        // had produced. A record refused by the *series limit* is added by the line above
        // — after the counting had already happened — so those never reached
        // `telemetryd_ingest_rejected_total` at all. Measured on a real deployment:
        // `telemetryd_series_rejected_total` at 4,397 and the reason-labelled counter
        // empty, while every log record was being turned away. `partialSuccess` said so
        // on every response and nothing anyone monitors did, which is why it took an hour
        // to find something the server knew immediately.
        count_rejections(&state, "traces", &decoded);
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

    Ok(answer(encoding, &response))
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

    within_budget(&state, "metrics", &decoded)?;
    let mut decoded = decoded;
    stamp(
        &state,
        identity.as_ref(),
        decoded.records.iter_mut().map(|sample| &mut sample.series),
    );
    Ok(answer(
        encoding,
        &store_samples(&state, decoded).await?.response,
    ))
}

/// `POST /api/v1/write` — Prometheus remote_write.
pub async fn remote_write(
    State(state): State<AppState>,
    identity: Option<axum::Extension<ClientIdentity>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    let identity = identity.map(|axum::Extension(identity)| identity);
    if is_remote_write_v2(&headers) {
        reject(&state, "metrics", "remote_write_v2");
        return Ok((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(serde_json::json!({ "error": {
                "code": "unsupported_media_type",
                "message": "Remote-Write 2.0 is not supported; send Remote-Write 1.0 \
                            (protobuf_message: prometheus.WriteRequest)",
            }})),
        )
            .into_response());
    }
    // Bound concurrent ingest. A rejected request is a signal the producer can act on
    // — back off, batch harder — where an accepted one that queues behind a hundred
    // others is the unbounded buffering `limits.ingest_queue_depth` exists to prevent.
    let Some(_slot) = state.ingest_slot_for(identity.as_ref().map(|i| i.app.as_str())) else {
        state.metrics.incr(
            "telemetryd_ingest_rejected_total",
            &[("signal", "metrics"), ("reason", "queue_full")],
        );
        // 503 where OTLP gets 429. Prometheus retries a 429 only when
        // `retry_on_http_429` is set, and otherwise drops the samples as if they were
        // malformed; every 5xx is retried. The same `Retry-After` rides along.
        let mut busy = ApiError::from(telemetryd_core::Error::Overloaded).into_response();
        *busy.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        return Ok(busy);
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

    within_budget(&state, "metrics", &decoded)?;
    let mut decoded = decoded;
    stamp(
        &state,
        identity.as_ref(),
        decoded.records.iter_mut().map(|sample| &mut sample.series),
    );

    // remote_write has no partial-success envelope. A batch where some samples were
    // stored is a 204 — the refusals are counted in our metrics, and a 4xx would make
    // the sender drop the samples that were fine. A batch where *none* was stored is a
    // 400: it used to be a 204 too, so Prometheus's own failure counter stayed at zero
    // while every sample was being turned away.
    let stored = store_samples(&state, decoded).await?;
    if stored.accepted == 0 && stored.rejected > 0 {
        return Err(Error::BadRequest(format!(
            "none of the {} samples were stored: {}",
            stored.rejected,
            stored.summary.unwrap_or_default()
        ))
        .into());
    }
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Whether this is a Remote-Write 2.0 request, which telemetryd does not decode.
///
/// Its message uses different protobuf fields, so decoding it as 1.0 skips every series
/// and answers 204 having stored nothing. The 2.0 specification asks a receiver that
/// cannot read it to answer 415, so a sender can fall back to 1.0.
fn is_remote_write_v2(headers: &HeaderMap) -> bool {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let version = headers
        .get("x-prometheus-remote-write-version")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    content_type.contains("io.prometheus.write.v2") || version.trim().starts_with('2')
}

/// Shared tail of both metric ingest paths.
async fn store_samples(
    state: &AppState,
    mut decoded: telemetryd_ingest::Decoded<telemetryd_core::MetricSample>,
) -> Result<StoredSamples, ApiError> {
    if decoded.rescaled_timestamps > 0 {
        state.metrics.add(
            "telemetryd_ingest_timestamps_rescaled_total",
            &[("signal", "metrics")],
            decoded.rescaled_timestamps,
        );
    }

    let mut stored = 0;
    if !decoded.records.is_empty() {
        let store = std::sync::Arc::clone(&state.store);
        let records = decoded.records.clone();

        let admitted = crate::fatal::storage(
            tokio::task::spawn_blocking(move || store.append_samples(&records)).await,
            "appending metric samples",
        )??;
        stored = admitted.stored;
        decoded.note_series_rejections(admitted.rejected, admitted.reason);

        // Counted here, after the store has spoken, rather than before it.
        //
        // The loop used to sit above this call, so it saw only the rejections the decoder
        // had produced. A record refused by the *series limit* is added by the line above
        // — after the counting had already happened — so those never reached
        // `telemetryd_ingest_rejected_total` at all. Measured on a real deployment:
        // `telemetryd_series_rejected_total` at 4,397 and the reason-labelled counter
        // empty, while every log record was being turned away. `partialSuccess` said so
        // on every response and nothing anyone monitors did, which is why it took an hour
        // to find something the server knew immediately.
        count_rejections(state, "metrics", &decoded);
        let accepted = admitted.stored as u64;

        state.metrics.add(
            "telemetryd_ingest_accepted_total",
            &[("signal", "metrics")],
            accepted,
        );
    }

    let summary = decoded.rejection_summary();
    let response = OtlpResponse {
        partial_success: summary.clone().map(|message| PartialSuccess {
            rejected_log_records: None,
            rejected_spans: None,
            rejected_data_points: Some(decoded.rejected().to_string()),
            error_message: message,
        }),
    };

    Ok(StoredSamples {
        response,
        accepted: stored,
        rejected: decoded.rejected(),
        summary,
    })
}

/// What storing a batch of samples came to: the OTLP answer, and the counts the
/// remote_write answer is decided from.
struct StoredSamples {
    response: OtlpResponse,
    /// What the store kept — not what survived decoding, which still includes samples
    /// the series limit then refused.
    accepted: usize,
    rejected: usize,
    summary: Option<String>,
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

/// Refuse a request whose decoded records outgrew `limits.max_decoded_bytes`.
///
/// Refused whole, with `413`: what was kept is incomplete by then, and storing part of a
/// batch while acknowledging it would be a silent loss. A client that sees `413` can split
/// the batch; one that sees `200` has no reason to look.
fn within_budget<T>(
    state: &AppState,
    signal: &'static str,
    decoded: &telemetryd_ingest::Decoded<T>,
) -> Result<(), Error> {
    if !decoded.over_budget() {
        return Ok(());
    }
    reject(state, signal, "decoded_too_large");
    Err(Error::LimitExceeded {
        limit: "limits.max_decoded_bytes",
        detail: format!(
            "this {signal} request expanded to more than {} once decoded; send smaller \
             batches, or fewer attributes shared across many records",
            state.config.limits.max_decoded_bytes
        ),
    })
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
