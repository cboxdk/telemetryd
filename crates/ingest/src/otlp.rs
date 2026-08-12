//! Shared OTLP/HTTP JSON wire types.
//!
//! OTLP JSON is proto3 JSON, and real producers exercise more of that spec's latitude
//! than the happy-path examples suggest. Three things bite in practice, and all three
//! are handled here rather than in each signal's decoder:
//!
//! 1. **64-bit integers are strings.** proto3 JSON encodes `int64`/`uint64` as decimal
//!    strings to survive JavaScript's 2^53 limit — so `timeUnixNano` arrives as
//!    `"1544712660300000000"`. Some producers send a bare number anyway. Both are
//!    accepted ([`FlexU64`]).
//! 2. **Field names come in two spellings.** proto3 JSON parsers accept both the
//!    lowerCamelCase JSON name and the original snake_case proto field name, so
//!    `timeUnixNano` and `time_unix_nano` are both valid. Every field carries an alias.
//! 3. **Enums come as numbers or names.** `severityNumber` may be `17` or
//!    `"SEVERITY_NUMBER_ERROR"` ([`FlexEnum`]).
//!
//! Being strict about any of these would reject data that is legitimately OTLP.

use serde::Deserialize;
use telemetryd_core::Labels;
use telemetryd_core::record::sanitize_label_name;

/// A `uint64` that may arrive as a JSON string or a JSON number.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(untagged)]
pub enum FlexU64 {
    #[default]
    Absent,
    Number(u64),
    /// Kept as a string until parsed so a malformed value degrades to `None` rather
    /// than failing the whole batch.
    Text(FlexText),
}

impl FlexU64 {
    pub fn get(self) -> Option<u64> {
        match self {
            Self::Absent => None,
            Self::Number(n) => Some(n),
            Self::Text(t) => t.0,
        }
    }
}

/// Wrapper that parses a decimal string at deserialize time and stores `None` on
/// failure, so one bad field never costs a whole request.
#[derive(Debug, Clone, Copy, Default)]
pub struct FlexText(pub Option<u64>);

impl<'de> Deserialize<'de> for FlexText {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(de)?;
        Ok(Self(raw.trim().parse::<u64>().ok()))
    }
}

/// An enum field that may arrive as a number or as its proto name.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(untagged)]
pub enum FlexEnum {
    #[default]
    Absent,
    Number(i32),
    Name(String),
}

impl FlexEnum {
    /// Resolve to the numeric value, mapping proto enum names through `lookup`.
    pub fn resolve(&self, lookup: impl Fn(&str) -> Option<i32>) -> Option<i32> {
        match self {
            Self::Absent => None,
            Self::Number(n) => Some(*n),
            Self::Name(name) => name.parse::<i32>().ok().or_else(|| lookup(name)),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Resource {
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct InstrumentationScope {
    pub name: String,
    pub version: String,
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValue {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub value: Option<AnyValue>,
}

/// OTLP's `AnyValue` union. Exactly one field is expected to be set.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AnyValue {
    #[serde(alias = "string_value")]
    pub string_value: Option<String>,
    #[serde(alias = "bool_value")]
    pub bool_value: Option<bool>,
    #[serde(alias = "int_value")]
    pub int_value: FlexU64,
    #[serde(alias = "double_value")]
    pub double_value: Option<f64>,
    #[serde(alias = "array_value")]
    pub array_value: Option<ArrayValue>,
    #[serde(alias = "kvlist_value")]
    pub kvlist_value: Option<KvList>,
    /// Base64 in the wire format; passed through unchanged rather than decoded, since
    /// a label value has to be text either way.
    #[serde(alias = "bytes_value")]
    pub bytes_value: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ArrayValue {
    pub values: Vec<AnyValue>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct KvList {
    pub values: Vec<KeyValue>,
}

impl AnyValue {
    /// Flatten to the string form used for label values and log bodies.
    ///
    /// Composite values render as JSON rather than as a debug format, so a structured
    /// body stays machine-readable — `| json` downstream can still parse it.
    pub fn to_text(&self) -> Option<String> {
        if let Some(value) = &self.string_value {
            return Some(value.clone());
        }
        if let Some(value) = self.bool_value {
            return Some(value.to_string());
        }
        if let Some(value) = self.int_value.get() {
            return Some(value.to_string());
        }
        if let Some(value) = self.double_value {
            return Some(format_double(value));
        }
        if let Some(value) = &self.bytes_value {
            return Some(value.clone());
        }
        if self.array_value.is_some() || self.kvlist_value.is_some() {
            return Some(self.to_json().to_string());
        }
        None
    }

    /// JSON projection of a composite value, preserving nesting.
    fn to_json(&self) -> serde_json::Value {
        use serde_json::Value;

        if let Some(value) = &self.string_value {
            return Value::String(value.clone());
        }
        if let Some(value) = self.bool_value {
            return Value::Bool(value);
        }
        if let Some(value) = self.int_value.get() {
            return Value::Number(value.into());
        }
        if let Some(value) = self.double_value {
            return serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number);
        }
        if let Some(value) = &self.bytes_value {
            return Value::String(value.clone());
        }
        if let Some(array) = &self.array_value {
            return Value::Array(array.values.iter().map(Self::to_json).collect());
        }
        if let Some(list) = &self.kvlist_value {
            return Value::Object(
                list.values
                    .iter()
                    .map(|kv| {
                        (
                            kv.key.clone(),
                            kv.value.as_ref().map_or(Value::Null, Self::to_json),
                        )
                    })
                    .collect(),
            );
        }
        Value::Null
    }
}

/// Render a float without a trailing `.0`, so `200.0` becomes `"200"`.
///
/// Matters because attribute values become label values, and `{status="200"}` not
/// matching `{status="200.0"}` is a confusing failure to debug.
fn format_double(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

/// Fold OTLP attributes into a label set, sanitising names.
///
/// Used for the resource and scope attributes that become *stream labels*, which must
/// be valid Loki/Prometheus label names.
///
/// Later keys win, which is what lets scope attributes override resource attributes
/// and record attributes override both — narrowest scope closest to the data.
pub fn extend_labels(target: &mut Labels, attributes: &[KeyValue]) {
    extend(target, attributes, true);
}

/// Fold OTLP attributes in **verbatim**, preserving the producer's key spelling.
///
/// Used for per-record and per-span attributes, which are data rather than label
/// names. Rewriting `exception.type` to `exception_type` would mean a trace view shows
/// a key nobody sent; queries reach either spelling through
/// [`telemetryd_core::Labels::get_relaxed`].
pub fn extend_attributes(target: &mut Labels, attributes: &[KeyValue]) {
    extend(target, attributes, false);
}

/// Keep the resource and scope attributes that did **not** become stream labels, as
/// record attributes under the producer's own key spelling.
///
/// # The bug this fixes
///
/// Resource attributes were read only to build stream labels, and `ingest.stream_labels`
/// promotes five names. Everything else — `k8s.pod.name`, `host.name`, `cloud.region`,
/// `container.id`, every attribute a non-Laravel sender puts in `resource` — was
/// discarded. Not stored and not a label: absent from the store, absent from
/// `/api/v1/export`, and absent from `partialSuccess`, so nothing said it had happened.
/// Measured by sending `k8s.pod.name` and finding zero occurrences of it anywhere.
///
/// The cardinality argument for keeping them out of *stream identity* is sound and is
/// untouched. There was never an argument for deleting them, because record attributes
/// have been stored all along at no cardinality cost — this is the same treatment.
///
/// # Two rules
///
/// A record attribute of the same name wins: it is the narrower scope, closest to the
/// data, which is the precedence the rest of this decoder already uses.
///
/// A name already promoted to a stream label is skipped rather than duplicated. It is
/// visible as a label, and storing it twice would inflate every record to say the same
/// thing — but the comparison is against the *sanitised* name, since `service.version`
/// is the label `service_version` and they are one attribute, not two.
pub fn keep_unpromoted(target: &mut Labels, inherited: &Labels, stream: &Labels) {
    for (name, value) in inherited.iter() {
        if stream.get(&sanitize_label_name(name)).is_some() || target.get(name).is_some() {
            continue;
        }
        target.insert(name.to_owned(), value.to_owned());
    }
}

fn extend(target: &mut Labels, attributes: &[KeyValue], sanitize: bool) {
    for kv in attributes {
        if kv.key.is_empty() {
            continue;
        }
        let Some(text) = kv.value.as_ref().and_then(AnyValue::to_text) else {
            continue;
        };
        if sanitize {
            target.insert(sanitize_label_name(&kv.key), text);
        } else {
            target.insert(kv.key.clone(), text);
        }
    }
}

/// Normalise a hex trace or span id.
///
/// An all-zero id is the OTLP encoding for "absent"; treating it as a real id would
/// make every unsampled record look like it belonged to trace `000…0`.
pub fn normalize_id(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.bytes().all(|b| b == b'0') {
        return None;
    }
    if !trimmed.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn any(json: &str) -> AnyValue {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn int64_arrives_as_a_string_or_a_number() {
        // proto3 JSON encodes int64 as a string; some producers send a number anyway.
        let as_string: FlexU64 = serde_json::from_str(r#""1544712660300000000""#).unwrap();
        assert_eq!(as_string.get(), Some(1_544_712_660_300_000_000));

        let as_number: FlexU64 = serde_json::from_str("1544712660300000000").unwrap();
        assert_eq!(as_number.get(), Some(1_544_712_660_300_000_000));

        // A malformed value degrades to None rather than failing the batch.
        let junk: FlexU64 = serde_json::from_str(r#""not-a-number""#).unwrap();
        assert_eq!(junk.get(), None);
    }

    #[test]
    fn enums_arrive_as_numbers_or_proto_names() {
        let lookup = |name: &str| (name == "SEVERITY_NUMBER_ERROR").then_some(17);

        let numeric: FlexEnum = serde_json::from_str("17").unwrap();
        assert_eq!(numeric.resolve(lookup), Some(17));

        let named: FlexEnum = serde_json::from_str(r#""SEVERITY_NUMBER_ERROR""#).unwrap();
        assert_eq!(named.resolve(lookup), Some(17));

        let unknown: FlexEnum = serde_json::from_str(r#""SEVERITY_NUMBER_MADE_UP""#).unwrap();
        assert_eq!(unknown.resolve(lookup), None);
    }

    #[test]
    fn any_value_flattens_every_variant() {
        assert_eq!(any(r#"{"stringValue":"hi"}"#).to_text().unwrap(), "hi");
        assert_eq!(any(r#"{"boolValue":true}"#).to_text().unwrap(), "true");
        assert_eq!(any(r#"{"intValue":"42"}"#).to_text().unwrap(), "42");
        assert_eq!(any(r#"{"doubleValue":1.5}"#).to_text().unwrap(), "1.5");
        // Whole floats lose the .0, so {status="200"} matches what people expect.
        assert_eq!(any(r#"{"doubleValue":200.0}"#).to_text().unwrap(), "200");
        assert_eq!(any("{}").to_text(), None);
    }

    #[test]
    fn composite_values_render_as_json_not_debug_output() {
        let array = any(r#"{"arrayValue":{"values":[{"stringValue":"a"},{"intValue":"2"}]}}"#);
        assert_eq!(array.to_text().unwrap(), r#"["a",2]"#);

        let map = any(r#"{"kvlistValue":{"values":[{"key":"k","value":{"stringValue":"v"}}]}}"#);
        assert_eq!(map.to_text().unwrap(), r#"{"k":"v"}"#);

        // Nesting survives, so `| json` downstream can still walk it.
        let nested = any(
            r#"{"kvlistValue":{"values":[{"key":"o","value":{"arrayValue":{"values":[{"intValue":"1"}]}}}]}}"#,
        );
        assert_eq!(nested.to_text().unwrap(), r#"{"o":[1]}"#);
    }

    #[test]
    fn snake_case_field_names_are_accepted_too() {
        // proto3 JSON permits the original proto field names.
        let value = any(r#"{"string_value":"snake"}"#);
        assert_eq!(value.to_text().unwrap(), "snake");
    }

    #[test]
    fn attribute_keys_are_sanitised_and_later_scopes_win() {
        let attrs: Vec<KeyValue> = serde_json::from_str(
            r#"[{"key":"service.name","value":{"stringValue":"checkout"}},
                {"key":"http.status_code","value":{"intValue":"200"}}]"#,
        )
        .unwrap();

        let mut labels = Labels::new();
        labels.insert("service_name", "placeholder");
        extend_labels(&mut labels, &attrs);

        assert_eq!(labels.get("service_name"), Some("checkout"));
        assert_eq!(labels.get("http_status_code"), Some("200"));
    }

    #[test]
    fn attributes_without_a_value_or_key_are_skipped() {
        let attrs: Vec<KeyValue> = serde_json::from_str(
            r#"[{"key":"empty"},{"key":"","value":{"stringValue":"x"}},{"key":"ok","value":{"stringValue":"y"}}]"#,
        )
        .unwrap();
        let mut labels = Labels::new();
        extend_labels(&mut labels, &attrs);

        assert_eq!(labels.len(), 1);
        assert_eq!(labels.get("ok"), Some("y"));
    }

    #[test]
    fn all_zero_ids_mean_absent_not_a_real_trace() {
        assert_eq!(normalize_id("00000000000000000000000000000000"), None);
        assert_eq!(normalize_id(""), None);
        assert_eq!(normalize_id("   "), None);
        assert_eq!(normalize_id("not-hex-at-all"), None);
        assert_eq!(
            normalize_id("4BF92F3577B34DA6A3CE929D0E0E4736").unwrap(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }
}
