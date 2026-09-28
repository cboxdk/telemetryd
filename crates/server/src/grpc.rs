//! OTLP/gRPC ingest: the three `Export` methods, answered by the OTLP/HTTP handlers.
//!
//! An OTLP/gRPC request is the OTLP/HTTP protobuf request in other clothes: the same
//! `Export*ServiceRequest` message, behind a five-byte frame, on HTTP/2, with the
//! outcome in trailers instead of the status line. So nothing here decodes telemetry.
//! Each call is unframed and handed to the main router as `POST /v1/logs` (or traces,
//! or metrics) with `Content-Type: application/x-protobuf` — through the same token
//! check, ingest slots, limits, decompression and `partialSuccess` — and the answer is
//! framed back. A second decoding path is how two paths come to disagree about what
//! was accepted.
//!
//! Served over HTTP/2 with prior knowledge on a port of its own (`server.grpc_listen`),
//! or over TLS with `h2` negotiated when `server.tls` is on. Only unary calls exist in
//! OTLP, so there is no streaming to support.

use std::fmt::Write as _;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::Response;
use http_body_util::BodyExt;
use tower::ServiceExt;

/// The router the calls are answered by, and the largest frame accepted.
#[derive(Clone)]
pub struct Gateway {
    inner: Router,
    max_message: usize,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("max_message", &self.max_message)
            .finish_non_exhaustive()
    }
}

/// gRPC status codes this gateway answers with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Code {
    Ok = 0,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    Unauthenticated = 16,
}

impl Code {
    /// The status an OTLP/HTTP answer means, chosen so an exporter retries exactly what
    /// OTLP says it should: `UNAVAILABLE` for overload and storage trouble, never for a
    /// batch that would be refused again.
    fn of(status: StatusCode) -> Self {
        match status.as_u16() {
            200..=299 => Self::Ok,
            400 => Self::InvalidArgument,
            401 => Self::Unauthenticated,
            403 => Self::PermissionDenied,
            404 | 405 | 501 => Self::Unimplemented,
            // Resending the same oversized batch cannot succeed.
            413 => Self::ResourceExhausted,
            429 | 502 | 503 => Self::Unavailable,
            504 => Self::DeadlineExceeded,
            _ => Self::Internal,
        }
    }
}

/// The router the gRPC port serves: every path is one unary `Export` call, or refused.
pub fn router(inner: Router, max_body_bytes: usize) -> Router {
    Router::new().fallback(export).with_state(Gateway {
        inner,
        max_message: max_body_bytes,
    })
}

/// The OTLP/HTTP route a gRPC method is answered by.
fn route(path: &str) -> Option<&'static str> {
    match path {
        "/opentelemetry.proto.collector.logs.v1.LogsService/Export" => Some("/v1/logs"),
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export" => Some("/v1/traces"),
        "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export" => Some("/v1/metrics"),
        _ => None,
    }
}

/// One unary call.
async fn export(State(gateway): State<Gateway>, request: Request) -> Response {
    match call(&gateway, request).await {
        Ok(response) => response,
        Err((code, message)) => trailers_only(code, &message),
    }
}

async fn call(gateway: &Gateway, request: Request) -> Result<Response, (Code, String)> {
    let Some(path) = route(request.uri().path()) else {
        return Err((
            Code::Unimplemented,
            format!(
                "{} is not an OTLP export method; telemetryd serves the logs, trace and \
                 metrics Export calls",
                request.uri().path()
            ),
        ));
    };
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_type.starts_with("application/grpc") {
        return Err((
            Code::InvalidArgument,
            format!("content-type must be application/grpc, not {content_type:?}"),
        ));
    }

    let (parts, body) = request.into_parts();
    // One frame: a flag byte, a four-byte length, the message.
    let limit = gateway.max_message.saturating_add(5);
    let bytes = axum::body::to_bytes(body, limit).await.map_err(|_| {
        (
            Code::ResourceExhausted,
            format!(
                "the message is larger than server.max_body_bytes ({} bytes)",
                gateway.max_message
            ),
        )
    })?;
    let (compressed, message) = unframe(&bytes)?;

    let mut inner = Request::post(path)
        .header(header::CONTENT_TYPE, "application/x-protobuf")
        .body(Body::from(message))
        .map_err(|e| (Code::Internal, e.to_string()))?;
    // Metadata is headers: the bearer token, and whatever else the handlers read.
    for (name, value) in &parts.headers {
        if forwarded(name) {
            inner.headers_mut().append(name.clone(), value.clone());
        }
    }
    if compressed {
        let encoding = parts
            .headers
            .get("grpc-encoding")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("identity");
        if !matches!(encoding, "gzip" | "deflate" | "zstd") {
            return Err((
                Code::Unimplemented,
                format!("grpc-encoding {encoding:?} is not supported; use gzip or zstd"),
            ));
        }
        inner.headers_mut().insert(
            header::CONTENT_ENCODING,
            HeaderValue::from_str(encoding).map_err(|e| (Code::Internal, e.to_string()))?,
        );
    }
    // The client identity a TLS connection proved travels with the request.
    *inner.extensions_mut() = parts.extensions;

    let answer = gateway
        .inner
        .clone()
        .oneshot(inner)
        .await
        .map_err(|e| (Code::Internal, e.to_string()))?;
    let status = answer.status();
    let body = answer
        .into_body()
        .collect()
        .await
        .map_err(|e| (Code::Internal, e.to_string()))?
        .to_bytes();
    match Code::of(status) {
        Code::Ok => Ok(framed(&body)),
        code => Err((code, message_of(&body, status))),
    }
}

/// The message inside one uncompressed-or-compressed frame.
fn unframe(bytes: &[u8]) -> Result<(bool, Bytes), (Code, String)> {
    let malformed = |why: &str| {
        (
            Code::InvalidArgument,
            format!("malformed gRPC frame: {why}"),
        )
    };
    let [flag, a, b, c, d, rest @ ..] = bytes else {
        return Err(malformed("shorter than its five-byte header"));
    };
    let length = u32::from_be_bytes([*a, *b, *c, *d]) as usize;
    if rest.len() != length {
        return Err(malformed(&format!(
            "the header says {length} bytes and {} followed; one message per call",
            rest.len()
        )));
    }
    match flag {
        0 => Ok((false, Bytes::copy_from_slice(rest))),
        1 => Ok((true, Bytes::copy_from_slice(rest))),
        _ => Err(malformed("the compression flag is neither 0 nor 1")),
    }
}

/// Headers passed on to the OTLP/HTTP handler: metadata, not HTTP/2 or gRPC framing.
fn forwarded(name: &HeaderName) -> bool {
    let name = name.as_str();
    !(name.starts_with("grpc-")
        || name.starts_with(':')
        || matches!(
            name,
            "content-type" | "content-length" | "content-encoding" | "te" | "host"
        ))
}

/// The error message an OTLP/HTTP answer carried, for `grpc-message`.
fn message_of(body: &[u8], status: StatusCode) -> String {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|json| {
            json.pointer("/error/message")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| status.to_string())
}

fn grpc_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/grpc"),
    );
    headers.insert(
        "grpc-accept-encoding",
        HeaderValue::from_static("gzip,deflate,zstd,identity"),
    );
}

/// A successful call: the response message in a frame, `grpc-status: 0` in trailers.
fn framed(message: &[u8]) -> Response {
    let mut frame = Vec::with_capacity(message.len() + 5);
    frame.push(0);
    frame.extend_from_slice(
        &u32::try_from(message.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    frame.extend_from_slice(message);
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let frames = futures_util::stream::iter([
        Ok::<_, std::convert::Infallible>(hyper::body::Frame::data(Bytes::from(frame))),
        Ok(hyper::body::Frame::trailers(trailers)),
    ]);
    let mut response = Response::new(Body::new(http_body_util::StreamBody::new(frames)));
    grpc_headers(response.headers_mut());
    response
}

/// A failed call, as gRPC's "trailers-only" response: status and message in the headers.
fn trailers_only(code: Code, message: &str) -> Response {
    let mut response = Response::new(Body::empty());
    grpc_headers(response.headers_mut());
    let headers = response.headers_mut();
    headers.insert("grpc-status", HeaderValue::from(u16::from(code as u8)));
    if let Ok(value) = HeaderValue::from_str(&percent_encode(message)) {
        headers.insert("grpc-message", value);
    }
    response
}

/// `grpc-message` is percent-encoded: everything outside printable ASCII, and `%`.
fn percent_encode(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    for byte in message.bytes() {
        if (0x20..=0x7e).contains(&byte) && byte != b'%' {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn a_frame_carries_one_message() {
        assert_eq!(
            unframe(&[0, 0, 0, 0, 2, 7, 8]).unwrap(),
            (false, Bytes::from_static(&[7, 8]))
        );
        assert!(unframe(&[1, 0, 0, 0, 0]).unwrap().0);
        assert!(unframe(&[0, 0, 0]).is_err());
        assert!(unframe(&[0, 0, 0, 0, 3, 1]).is_err());
        assert!(unframe(&[2, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn statuses_map_to_what_otlp_retries() {
        assert_eq!(Code::of(StatusCode::OK), Code::Ok);
        assert_eq!(Code::of(StatusCode::BAD_REQUEST), Code::InvalidArgument);
        assert_eq!(Code::of(StatusCode::UNAUTHORIZED), Code::Unauthenticated);
        assert_eq!(Code::of(StatusCode::TOO_MANY_REQUESTS), Code::Unavailable);
        assert_eq!(Code::of(StatusCode::SERVICE_UNAVAILABLE), Code::Unavailable);
        assert_eq!(
            Code::of(StatusCode::PAYLOAD_TOO_LARGE),
            Code::ResourceExhausted
        );
    }

    #[test]
    fn messages_are_percent_encoded() {
        assert_eq!(percent_encode("50% ok"), "50%25 ok");
        assert_eq!(percent_encode("æ"), "%C3%A6");
    }
}
