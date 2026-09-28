//! Loki's push API — what promtail, Grafana Alloy, Fluent Bit, Vector and the Docker
//! driver send.
//!
//! Two encodings reach `/loki/api/v1/push`: snappy-compressed protobuf
//! (`logproto.PushRequest`), which every shipper uses by default, and JSON
//! (`{"streams":[{"stream":{…},"values":[["<ns>","line",{…}]]}]}`).
//!
//! A pushed stream's labels are its identity, so they stay stream labels here, under
//! the same limits OTLP streams meet. Two are added the way Loki adds them: `app`, and
//! `service_name` when the stream has none, from the first of the labels Loki looks at
//! for a service; and `level`, from a label or else from the line itself. A line's
//! structured metadata becomes its attributes, except `trace_id` and `span_id`, which
//! are the record's own fields — so a shipped line links to its trace as an OTLP one
//! does.

use serde::Deserialize;
use telemetryd_core::record::{APP_LABEL, LEVEL_LABEL, sanitize_label_name};
use telemetryd_core::{Error, Labels, LogRecord, Result, Severity};

use crate::logs::DecodeContext;
use crate::protobuf::{Reader, WireType};
use crate::{Decoded, RejectReason, Rejection};

/// Labels Loki reads a stream's service from, in the order it reads them.
const SERVICE_LABELS: &[&str] = &[
    "service_name",
    "service",
    "app",
    "application",
    "name",
    "app_kubernetes_io_name",
    "container",
    "container_name",
    "k8s_container_name",
    "component",
    "workload",
    "job",
];

/// Labels a level may already sit in.
const LEVEL_LABELS: &[&str] = &["level", "detected_level", "severity", "lvl", "loglevel"];

/// One pushed line before it becomes a record.
struct Entry {
    timestamp_nanos: u64,
    line: String,
    metadata: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct PushJson {
    #[serde(default)]
    streams: Vec<StreamJson>,
}

#[derive(Deserialize)]
struct StreamJson {
    #[serde(default)]
    stream: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    values: Vec<Vec<serde_json::Value>>,
}

/// Decode a JSON push body.
///
/// # Errors
/// A body that is not a push request, or a line whose timestamp is not a number.
pub fn decode_json(body: &[u8], ctx: DecodeContext<'_>) -> Result<Decoded<LogRecord>> {
    crate::json_objects_within(body)
        .map_err(|e| Error::BadRequest(format!("could not decode the Loki push body: {e}")))?;
    let push: PushJson = serde_json::from_slice(body)
        .map_err(|e| Error::BadRequest(format!("could not decode the Loki push body: {e}")))?;
    let mut decoded = Decoded::bounded(ctx.limits).drawing_from(ctx.pool);
    for stream in push.streams {
        let mut entries = Vec::with_capacity(stream.values.len());
        for value in stream.values {
            let (Some(timestamp), Some(line)) = (value.first(), value.get(1)) else {
                decoded.refuse(Rejection::new(
                    RejectReason::InvalidTimestamp,
                    "a pushed value needs a timestamp and a line".to_owned(),
                ));
                continue;
            };
            let Some(timestamp_nanos) = timestamp
                .as_str()
                .and_then(|t| t.parse::<u64>().ok())
                .or_else(|| timestamp.as_u64())
            else {
                decoded.refuse(Rejection::new(
                    RejectReason::InvalidTimestamp,
                    format!("{timestamp} is not a timestamp in nanoseconds"),
                ));
                continue;
            };
            let metadata = value
                .get(2)
                .and_then(serde_json::Value::as_object)
                .map(|object| {
                    object
                        .iter()
                        .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_owned())))
                        .collect()
                })
                .unwrap_or_default();
            entries.push(Entry {
                timestamp_nanos,
                line: line.as_str().unwrap_or_default().to_owned(),
                metadata,
            });
        }
        let labels: Vec<(String, String)> = stream.stream.into_iter().collect();
        keep_stream(&labels, entries, ctx, &mut decoded);
    }
    Ok(decoded)
}

/// Decode a snappy-compressed protobuf push body.
///
/// # Errors
/// Snappy or protobuf that does not decode, or a declared size past `max_decompressed`.
pub fn decode_protobuf(
    compressed: &[u8],
    max_decompressed: usize,
    ctx: DecodeContext<'_>,
) -> Result<Decoded<LogRecord>> {
    // The declared size is checked before anything is allocated for it; see
    // `remote_write::decode`, which met the same eight-byte request for gigabytes.
    if let Ok(claimed) = snap::raw::decompress_len(compressed)
        && claimed > max_decompressed
    {
        return Err(Error::LimitExceeded {
            limit: "server.max_body_bytes",
            detail: format!(
                "the push body declares {claimed} uncompressed bytes, past the \
                 {max_decompressed} byte limit"
            ),
        });
    }
    let body = snap::raw::Decoder::new()
        .decompress_vec(compressed)
        .map_err(|e| Error::BadRequest(format!("the Loki push body is not valid snappy: {e}")))?;
    let mut decoded = Decoded::bounded(ctx.limits).drawing_from(ctx.pool);
    // What the entries may become while a stream is gathered, before any is charged to
    // `decoded`: an empty entry is two bytes on the wire and a fifty-six byte `Entry`, so
    // a body within its limit could build half a gigabyte of them.
    let mut budget = usize::try_from(ctx.limits.max_decoded_bytes.as_u64()).unwrap_or(usize::MAX);
    let mut request = Reader::new(&body);
    while let Some((field, wire)) = request.next_field()? {
        if field == 1 && wire == WireType::LengthDelimited {
            let (labels, entries) = read_stream(request.message()?, &mut budget)?;
            let labels = parse_label_string(&labels).map_err(|e| {
                Error::BadRequest(format!(
                    "a pushed stream's labels {labels:?} do not parse: {e}"
                ))
            })?;
            keep_stream(&labels, entries, ctx, &mut decoded);
        } else {
            request.skip(wire)?;
        }
    }
    Ok(decoded)
}

/// `StreamAdapter`: `labels` (1) and `entries` (2).
fn read_stream(mut stream: Reader<'_>, budget: &mut usize) -> Result<(String, Vec<Entry>)> {
    let (mut labels, mut entries) = (String::new(), Vec::new());
    while let Some((field, wire)) = stream.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => stream.string()?.clone_into(&mut labels),
            (2, WireType::LengthDelimited) => {
                let entry = read_entry(stream.message()?)?;
                let size = std::mem::size_of::<Entry>()
                    + entry.line.len()
                    + entry
                        .metadata
                        .iter()
                        .map(|(k, v)| std::mem::size_of::<(String, String)>() + k.len() + v.len())
                        .sum::<usize>();
                *budget = budget
                    .checked_sub(size)
                    .ok_or_else(|| Error::LimitExceeded {
                        limit: "limits.max_decoded_bytes",
                        detail: "the push body expands past what one request may decode to"
                            .to_owned(),
                    })?;
                entries.push(entry);
            }
            _ => stream.skip(wire)?,
        }
    }
    Ok((labels, entries))
}

/// `EntryAdapter`: `timestamp` (1), `line` (2), `structuredMetadata` (3).
fn read_entry(mut entry: Reader<'_>) -> Result<Entry> {
    let mut out = Entry {
        timestamp_nanos: 0,
        line: String::new(),
        metadata: Vec::new(),
    };
    while let Some((field, wire)) = entry.next_field()? {
        match (field, wire) {
            (1, WireType::LengthDelimited) => {
                let mut timestamp = entry.message()?;
                let (mut seconds, mut nanos) = (0u64, 0u64);
                while let Some((field, wire)) = timestamp.next_field()? {
                    match (field, wire) {
                        (1, WireType::Varint) => seconds = timestamp.varint()?,
                        (2, WireType::Varint) => nanos = timestamp.varint()?,
                        _ => timestamp.skip(wire)?,
                    }
                }
                out.timestamp_nanos = seconds.saturating_mul(1_000_000_000).saturating_add(nanos);
            }
            (2, WireType::LengthDelimited) => entry.string()?.clone_into(&mut out.line),
            (3, WireType::LengthDelimited) => {
                let mut pair = entry.message()?;
                let (mut name, mut value) = (String::new(), String::new());
                while let Some((field, wire)) = pair.next_field()? {
                    match (field, wire) {
                        (1, WireType::LengthDelimited) => pair.string()?.clone_into(&mut name),
                        (2, WireType::LengthDelimited) => pair.string()?.clone_into(&mut value),
                        _ => pair.skip(wire)?,
                    }
                }
                out.metadata.push((name, value));
            }
            _ => entry.skip(wire)?,
        }
    }
    Ok(out)
}

/// `{a="1", b="two"}` — how a protobuf push spells a stream's labels.
fn parse_label_string(text: &str) -> std::result::Result<Vec<(String, String)>, String> {
    let inner = text
        .trim()
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or("labels are written as {name=\"value\", …}")?;
    let mut labels = Vec::new();
    let mut chars = inner.chars().peekable();
    loop {
        while chars.next_if(|c| *c == ',' || c.is_whitespace()).is_some() {}
        let name: String = std::iter::from_fn(|| chars.next_if(|c| *c != '=')).collect();
        if name.trim().is_empty() {
            return Ok(labels);
        }
        if chars.next() != Some('=') || chars.next() != Some('"') {
            return Err(format!("label {name:?} needs =\"value\""));
        }
        let mut value = String::new();
        loop {
            match chars.next() {
                Some('\\') => match chars.next() {
                    Some('n') => value.push('\n'),
                    Some('t') => value.push('\t'),
                    Some(other) => value.push(other),
                    None => return Err("an escape runs off the end".to_owned()),
                },
                Some('"') => break,
                Some(c) => value.push(c),
                None => return Err(format!("the value of {name:?} is not closed")),
            }
        }
        labels.push((name.trim().to_owned(), value));
    }
}

/// Turn one pushed stream's entries into records, or refuse them with a reason.
fn keep_stream(
    pushed: &[(String, String)],
    entries: Vec<Entry>,
    ctx: DecodeContext<'_>,
    decoded: &mut Decoded<LogRecord>,
) {
    let mut labels = Labels::new();
    for (name, value) in pushed {
        if !value.is_empty() {
            labels.insert(sanitize_label_name(name), value.clone());
        }
    }
    let service = SERVICE_LABELS
        .iter()
        .find_map(|name| labels.get(name).map(str::to_owned));
    if let Some(service) = &service {
        if labels.get("service_name").is_none() {
            labels.insert("service_name", service.clone());
        }
        if labels.get(APP_LABEL).is_none() {
            labels.insert(APP_LABEL, service.clone());
        }
    }
    let labelled = LEVEL_LABELS
        .iter()
        .find_map(|name| labels.get(name))
        .map(Severity::from_text)
        .filter(|s| *s != Severity::Unknown);

    if let Err(rejection) = check_labels(&labels, ctx) {
        for _ in &entries {
            decoded.refuse(rejection.clone());
        }
        return;
    }

    for entry in entries {
        match record(&labels, labelled, entry, ctx, decoded) {
            Ok(record) => decoded.keep(record),
            Err(rejection) => decoded.refuse(rejection),
        }
    }
}

fn record(
    labels: &Labels,
    labelled: Option<Severity>,
    entry: Entry,
    ctx: DecodeContext<'_>,
    decoded: &mut Decoded<LogRecord>,
) -> std::result::Result<LogRecord, Rejection> {
    let severity = labelled.unwrap_or_else(|| detect_level(&entry.line));
    let mut stream = labels.clone();
    if stream.get(APP_LABEL).is_none() {
        stream.insert(APP_LABEL, telemetryd_core::record::UNKNOWN_APP);
    }
    stream.insert(LEVEL_LABEL, severity.as_str());

    let mut line = entry.line;
    let max = usize::try_from(ctx.limits.max_log_line_bytes.as_u64()).unwrap_or(usize::MAX);
    if line.len() > max {
        if !ctx.ingest.truncate_oversized_bodies {
            return Err(Rejection::new(
                RejectReason::BodyTooLarge,
                format!(
                    "log line of {} bytes exceeds max_log_line_bytes",
                    line.len()
                ),
            ));
        }
        crate::logs::truncate_marked(&mut line, max);
        decoded.truncated_bodies += 1;
    }

    let (mut trace_id, mut span_id) = (None, None);
    let mut attributes = Labels::new();
    for (name, value) in entry.metadata {
        match name.as_str() {
            // An id that is one becomes the record's, padded as OTLP's are. One that is
            // not — all zeros, not hex — stays metadata as it was sent, as Loki keeps it.
            "trace_id" | "traceID" | "traceId" => match crate::otlp::normalize_trace_id(&value) {
                Some(id) => trace_id = Some(id),
                None => attributes.insert(name, value),
            },
            "span_id" | "spanID" | "spanId" => match crate::otlp::normalize_span_id(&value) {
                Some(id) => span_id = Some(id),
                None => attributes.insert(name, value),
            },
            _ => attributes.insert(name, value),
        }
    }
    if attributes.len() > ctx.limits.max_attrs_per_record as usize {
        return Err(Rejection::new(
            RejectReason::TooManyAttributes,
            format!(
                "{} structured metadata entries exceeds max_attrs_per_record ({})",
                attributes.len(),
                ctx.limits.max_attrs_per_record
            ),
        ));
    }

    Ok(LogRecord {
        timestamp_nanos: if entry.timestamp_nanos == 0 {
            ctx.now_nanos
        } else {
            entry.timestamp_nanos
        },
        stream,
        severity,
        severity_text: String::new(),
        body: line,
        attributes,
        trace_id,
        span_id,
    })
}

fn check_labels(labels: &Labels, ctx: DecodeContext<'_>) -> std::result::Result<(), Rejection> {
    // `app` and `level` join the pushed labels, so they count.
    if labels.len() + 2 > ctx.limits.max_labels_per_series as usize {
        return Err(Rejection::new(
            RejectReason::TooManyLabels,
            format!(
                "{} stream labels exceeds max_labels_per_series ({})",
                labels.len() + 2,
                ctx.limits.max_labels_per_series
            ),
        ));
    }
    for (name, value) in labels.iter() {
        if name.len() > ctx.limits.max_label_name_bytes as usize {
            return Err(Rejection::new(
                RejectReason::LabelNameTooLong,
                format!("label name {name:?} exceeds max_label_name_bytes"),
            ));
        }
        if value.len() > ctx.limits.max_label_value_bytes as usize {
            return Err(Rejection::new(
                RejectReason::LabelValueTooLong,
                format!("value of label {name:?} exceeds max_label_value_bytes"),
            ));
        }
    }
    Ok(())
}

/// A line's level when no label says it: a `level=`/`"level":` field, else the first
/// level word in the line — how Loki detects one.
fn detect_level(line: &str) -> Severity {
    let head: String = line
        .chars()
        .take(1024)
        .collect::<String>()
        .to_ascii_lowercase();
    for key in ["level", "lvl", "severity", "loglevel"] {
        for spelled in [
            format!("{key}="),
            format!("\"{key}\":"),
            format!("\"{key}\": "),
        ] {
            if let Some(position) = head.find(&spelled) {
                let value: String = head[position + spelled.len()..]
                    .trim_start_matches(['"', ' '])
                    .chars()
                    .take_while(char::is_ascii_alphabetic)
                    .collect();
                let severity = Severity::from_text(&value);
                if severity != Severity::Unknown {
                    return severity;
                }
            }
        }
    }
    for (word, severity) in [
        ("fatal", Severity::Fatal),
        ("critical", Severity::Fatal),
        ("panic", Severity::Fatal),
        ("error", Severity::Error),
        ("err", Severity::Error),
        ("warning", Severity::Warn),
        ("warn", Severity::Warn),
        ("debug", Severity::Debug),
        ("trace", Severity::Trace),
        ("info", Severity::Info),
    ] {
        if head
            .split(|c: char| !c.is_ascii_alphanumeric())
            .any(|token| token == word)
        {
            return severity;
        }
    }
    Severity::Unknown
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use telemetryd_core::config::{IngestConfig, LimitsConfig};

    fn context<'a>(limits: &'a LimitsConfig, ingest: &'a IngestConfig) -> DecodeContext<'a> {
        DecodeContext {
            limits,
            ingest,
            now_nanos: 1_750_000_000_000_000_000,
            pool: None,
        }
    }

    /// Empty entries are two bytes each on the wire and far more once gathered; a body
    /// of them is refused when what it gathers passes the decode budget, not after.
    #[test]
    fn a_push_of_tiny_entries_cannot_outgrow_its_budget() {
        let labels = br#"{job="x"}"#;
        let mut stream = vec![0x0a, u8::try_from(labels.len()).unwrap()];
        stream.extend_from_slice(labels);
        for _ in 0..10_000 {
            stream.extend_from_slice(&[0x12, 0x00]);
        }
        let mut body = vec![0x0a];
        let mut length = stream.len();
        while length >= 0x80 {
            body.push(u8::try_from(length & 0x7f).unwrap() | 0x80);
            length >>= 7;
        }
        body.push(u8::try_from(length).unwrap());
        body.extend_from_slice(&stream);
        let compressed = snap::raw::Encoder::new().compress_vec(&body).unwrap();
        let limits = LimitsConfig {
            max_decoded_bytes: bytesize::ByteSize::kib(64),
            ..LimitsConfig::default()
        };
        let ingest = IngestConfig::default();
        let error = decode_protobuf(&compressed, 1 << 20, context(&limits, &ingest)).unwrap_err();
        assert!(error.to_string().contains("max_decoded_bytes"), "{error}");
    }

    #[test]
    fn a_json_push_becomes_records() {
        let (limits, ingest) = (LimitsConfig::default(), IngestConfig::default());
        let body = br#"{"streams":[{"stream":{"job":"varlogs","filename":"/var/log/app.log"},
            "values":[["1750000000000000000","GET /checkout 500 error: timeout"],
                      ["1750000001000000000","level=info msg=ok",{"trace_id":"ABCDEF0123456789ABCDEF0123456789","user":"42"}]]}]}"#;
        let decoded = decode_json(body, context(&limits, &ingest)).unwrap();
        assert_eq!(decoded.records.len(), 2);
        let first = &decoded.records[0];
        assert_eq!(first.stream.get("app"), Some("varlogs"));
        assert_eq!(first.stream.get("service_name"), Some("varlogs"));
        assert_eq!(first.stream.get("filename"), Some("/var/log/app.log"));
        assert_eq!(first.severity, Severity::Error);
        let second = &decoded.records[1];
        assert_eq!(second.severity, Severity::Info);
        assert_eq!(
            second.trace_id.as_deref(),
            Some("abcdef0123456789abcdef0123456789")
        );
        assert_eq!(second.attributes.get("user"), Some("42"));
        assert!(second.attributes.get("trace_id").is_none());
    }

    #[test]
    fn a_protobuf_push_becomes_records() {
        fn field(tag: u8, payload: &[u8]) -> Vec<u8> {
            let mut out = vec![tag];
            out.push(u8::try_from(payload.len()).unwrap());
            out.extend_from_slice(payload);
            out
        }
        let timestamp = [0x08, 0x80, 0xa4, 0xdc, 0xc3, 0x06, 0x10, 0x05]; // seconds, nanos 5
        let pair = [field(0x0a, b"span_id"), field(0x12, b"00F067AA0BA902B7")].concat();
        let entry = [
            field(0x0a, &timestamp),
            field(0x12, b"warning: disk"),
            field(0x1a, &pair),
        ]
        .concat();
        let stream = [
            field(0x0a, br#"{app="checkout", env="prod"}"#),
            field(0x12, &entry),
        ]
        .concat();
        let request = field(0x0a, &stream);
        let compressed = snap::raw::Encoder::new().compress_vec(&request).unwrap();
        let (limits, ingest) = (LimitsConfig::default(), IngestConfig::default());
        let decoded = decode_protobuf(&compressed, 1 << 20, context(&limits, &ingest)).unwrap();
        let record = &decoded.records[0];
        assert_eq!(record.body, "warning: disk");
        assert_eq!(record.severity, Severity::Warn);
        assert_eq!(record.stream.get("env"), Some("prod"));
        assert_eq!(record.span_id.as_deref(), Some("00f067aa0ba902b7"));
        assert_eq!(record.timestamp_nanos % 1_000_000_000, 5);
    }

    #[test]
    fn stream_label_strings_parse_with_escapes() {
        assert_eq!(
            parse_label_string(r#"{a="1", b="x\"y", c="line\nbreak"}"#).unwrap(),
            vec![
                ("a".to_owned(), "1".to_owned()),
                ("b".to_owned(), "x\"y".to_owned()),
                ("c".to_owned(), "line\nbreak".to_owned()),
            ]
        );
        assert!(parse_label_string("a=1").is_err());
        assert!(parse_label_string(r#"{a="1"#).is_err());
    }

    #[test]
    fn a_level_is_found_in_the_line() {
        assert_eq!(detect_level("level=warn msg=slow"), Severity::Warn);
        assert_eq!(
            detect_level(r#"{"level":"error","msg":"x"}"#),
            Severity::Error
        );
        assert_eq!(
            detect_level("2024-01-01 ERROR something broke"),
            Severity::Error
        );
        assert_eq!(
            detect_level("terrible but no level word"),
            Severity::Unknown
        );
    }
}
