//! TraceQL conditions and fields as SQL over `otel_traces`.
//!
//! A condition is stated twice:
//!
//! - **necessary** — every span TraceQL would match meets it. A search selects by it
//!   and lets the query's own evaluation decide each span that comes back.
//! - **sufficient** — every span that meets it, TraceQL matches. An aggregate counts
//!   those in ClickHouse; the spans that meet the necessary form but not the
//!   sufficient one are handed back to be decided one by one.
//!
//! Both follow how the span's attributes are looked up in Rust (see
//! `SpanRecord::resource_attribute` and `Labels::get_relaxed`): an exact key decides
//! on its own, so a span that has the attribute is decided by that one value; only
//! where no exact key is there is a key that sanitises alike looked for, and that is
//! matched on its letters — sanitising never changes an ASCII letter — which is
//! necessary but not sufficient. Numbers held as text compare as numbers; only plain
//! decimals are taken as parsing the same way in ClickHouse as in Rust.

use telemetryd_core::record::sanitize_label_name;
use telemetryd_core::span::{RESOURCE_ATTRIBUTE_PREFIX, SpanKind, SpanStatus};

use super::sql::quote;
use crate::backend::{SpanCondition, SpanField, SpanOp, SpanValue};

/// `f64::EPSILON`, which equality of numbers is within.
const EPSILON: &str = "2.220446049250313e-16";

/// A decimal both parsers read the same.
const PLAIN: &str = r"'^-?(0|[1-9][0-9]*)(\\.[0-9]+)?$'";

/// A condition's two SQL forms.
#[derive(Debug, Clone)]
pub(super) struct Stated {
    pub necessary: String,
    pub sufficient: String,
}

impl Stated {
    fn exact(sql: String) -> Self {
        Self {
            necessary: sql.clone(),
            sufficient: sql,
        }
    }
}

pub(super) const KINDS: [SpanKind; 6] = [
    SpanKind::Unspecified,
    SpanKind::Internal,
    SpanKind::Server,
    SpanKind::Client,
    SpanKind::Producer,
    SpanKind::Consumer,
];

pub(super) const STATUSES: [SpanStatus; 3] = [SpanStatus::Unset, SpanStatus::Ok, SpanStatus::Error];

/// Kind as the Collector and the store package write it.
pub(super) fn kind_name(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Unspecified => "Unspecified",
        SpanKind::Internal => "Internal",
        SpanKind::Server => "Server",
        SpanKind::Client => "Client",
        SpanKind::Producer => "Producer",
        SpanKind::Consumer => "Consumer",
    }
}

pub(super) fn status_name(status: SpanStatus) -> &'static str {
    match status {
        SpanStatus::Unset => "Unset",
        SpanStatus::Ok => "Ok",
        SpanStatus::Error => "Error",
    }
}

fn float(number: f64) -> String {
    if number.is_finite() {
        format!("{number:?}")
    } else if number.is_nan() {
        "nan".to_owned()
    } else if number > 0.0 {
        "inf".to_owned()
    } else {
        "-inf".to_owned()
    }
}

fn comparison(op: SpanOp) -> &'static str {
    match op {
        SpanOp::Greater => ">",
        SpanOp::GreaterEqual => ">=",
        SpanOp::Less => "<",
        SpanOp::LessEqual => "<=",
        SpanOp::NotEqual => "!=",
        SpanOp::Equal | SpanOp::Regex | SpanOp::NotRegex => "=",
    }
}

fn letters(text: &str) -> String {
    text.chars().filter(char::is_ascii_alphabetic).collect()
}

fn letters_sql(column: &str) -> String {
    format!("replaceRegexpAll({column}, '[^a-zA-Z]', '')")
}

/// Whether ClickHouse's RE2 matches `pattern` exactly as Rust's `regex` does.
///
/// Both read UTF-8 a character at a time, fold case alike under `(?i)`, and — with the
/// `(?s)` both sides compile it under — let `.` match a newline. Where they part is
/// the Perl classes: `\w`, `\d`, `\s` and `\b` are Unicode in Rust and ASCII in
/// RE2, so `\w+` matches `José` in one and not the other. A pattern using any of them
/// is not decided in ClickHouse.
pub(super) fn same_in_re2(pattern: &str) -> bool {
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == '\\'
            && matches!(
                chars.next(),
                Some('w' | 'W' | 'd' | 'D' | 's' | 'S' | 'b' | 'B')
            )
        {
            return false;
        }
    }
    true
}

fn anchored(pattern: &str) -> String {
    quote(&format!("^(?s:{pattern})$"))
}

/// What a value that is there must be, as SQL over `v`, for each form. `None` when the
/// comparison never holds on a present text value.
fn present_value(op: SpanOp, value: &SpanValue, v: &str) -> Option<Stated> {
    // A text right-hand side of an ordered comparison is read as a number, and one that
    // is not a number makes the comparison false, as `value_as_number` does.
    let number = match value {
        SpanValue::Number(n) => Some(*n),
        #[allow(clippy::cast_precision_loss)]
        SpanValue::Nanos(n) => Some(*n as f64),
        SpanValue::Text(text)
            if !matches!(
                op,
                SpanOp::Equal | SpanOp::NotEqual | SpanOp::Regex | SpanOp::NotRegex
            ) =>
        {
            Some(text.parse::<f64>().ok()?)
        }
        _ => None,
    };
    let plain = format!("match({v}, {PLAIN})");
    let parsed = format!("toFloat64OrNull({v})");
    Some(match (op, value, number) {
        (SpanOp::Equal, SpanValue::Text(text), _) => {
            Stated::exact(format!("{v} = {}", quote(text)))
        }
        (SpanOp::NotEqual, SpanValue::Text(text), _) => {
            Stated::exact(format!("{v} != {}", quote(text)))
        }
        (SpanOp::Regex, SpanValue::Text(pattern), _) if same_in_re2(pattern) => {
            Stated::exact(format!("match({v}, {})", anchored(pattern)))
        }
        (SpanOp::NotRegex, SpanValue::Text(pattern), _) if same_in_re2(pattern) => {
            Stated::exact(format!("NOT match({v}, {})", anchored(pattern)))
        }
        // A pattern the two engines read differently: any present value may match, and
        // none is certain to.
        (SpanOp::Regex | SpanOp::NotRegex, SpanValue::Text(_), _) => Stated {
            necessary: "1".to_owned(),
            sufficient: "0".to_owned(),
        },
        (SpanOp::Equal, _, Some(n)) => {
            let equal = format!("abs({parsed} - {}) < {EPSILON}", float(n));
            Stated {
                necessary: format!("(NOT {plain} OR {equal})"),
                sufficient: format!("({plain} AND {equal})"),
            }
        }
        (SpanOp::NotEqual, _, Some(n)) => {
            let equal = format!("abs({parsed} - {}) < {EPSILON}", float(n));
            Stated {
                necessary: format!("NOT ({plain} AND {equal})"),
                sufficient: format!("({plain} AND NOT {equal})"),
            }
        }
        (SpanOp::Greater | SpanOp::GreaterEqual | SpanOp::Less | SpanOp::LessEqual, _, Some(n)) => {
            let holds = format!("{parsed} {} {}", comparison(op), float(n));
            Stated {
                necessary: format!("(NOT {plain} OR {holds})"),
                sufficient: format!("({plain} AND {holds})"),
            }
        }
        // `!=` against something no text equals: every present value differs.
        (SpanOp::NotEqual, _, None) => Stated::exact("1".to_owned()),
        _ => return None,
    })
}

/// One step of a lookup, tried in order.
enum Arm {
    /// Where `present` holds, `value` is the field's value and decides.
    Value { present: String, value: String },
    /// Where `present` holds, a key a relaxed lookup might take is there, and whether
    /// it does is not known here: necessarily possible, not sufficiently certain.
    Unknown { present: String },
}

/// A lookup as both forms: each arm in turn, and where none applies the field is
/// absent, which only `= nil` matches.
fn lookup(arms: &[Arm], op: SpanOp, value: &SpanValue) -> Stated {
    let nil = *value == SpanValue::Nil;
    let mut necessary = Vec::new();
    let mut sufficient = Vec::new();
    for arm in arms {
        match arm {
            Arm::Value {
                present,
                value: expr,
            } => {
                let (n, s) = if nil {
                    // `= nil` is false wherever a key is there; `!= nil` true.
                    let holds = if op == SpanOp::NotEqual { "1" } else { "0" };
                    (holds.to_owned(), holds.to_owned())
                } else {
                    present_value(op, value, expr).map_or(("0".to_owned(), "0".to_owned()), |s| {
                        (s.necessary, s.sufficient)
                    })
                };
                necessary.push(format!("{present}, {n}"));
                sufficient.push(format!("{present}, {s}"));
            }
            Arm::Unknown { present } => {
                necessary.push(format!("{present}, 1"));
                sufficient.push(format!("{present}, 0"));
            }
        }
    }
    let absent = absent(op, value);
    Stated {
        necessary: format!("multiIf({}, {absent})", necessary.join(", ")),
        sufficient: format!("multiIf({}, {absent})", sufficient.join(", ")),
    }
}

/// What a lookup yields where no key is there, absent for certain: true only for
/// `= nil`.
fn absent(op: SpanOp, value: &SpanValue) -> &'static str {
    if *value == SpanValue::Nil && op == SpanOp::Equal {
        "1"
    } else {
        "0"
    }
}

/// Keys of `map` a relaxed lookup of `name` might find, under `prefix`.
fn candidate(map: &str, prefix: &str, name: &str) -> String {
    let key = if prefix.is_empty() {
        letters_sql("k")
    } else {
        format!("concat({}, {})", quote(&letters(prefix)), letters_sql("k"))
    };
    format!(
        "arrayExists(k -> {key} = {}, mapKeys({map}))",
        quote(&letters(name))
    )
}

/// The exact arms of `span.x`: the span's own attribute, then its resource's under
/// `resource.`, as `(present, value)`.
fn span_arms(name: &str) -> Vec<(String, String)> {
    let key = quote(name);
    let mut arms = vec![(
        format!("mapContains(SpanAttributes, {key})"),
        format!("SpanAttributes[{key}]"),
    )];
    if let Some(stripped) = name.strip_prefix(RESOURCE_ATTRIBUTE_PREFIX) {
        let stripped = quote(stripped);
        arms.push((
            format!("mapContains(ResourceAttributes, {stripped})"),
            format!("ResourceAttributes[{stripped}]"),
        ));
    }
    arms
}

fn values(arms: Vec<(String, String)>) -> impl Iterator<Item = Arm> {
    arms.into_iter()
        .map(|(present, value)| Arm::Value { present, value })
}

fn span_candidates(name: &str) -> String {
    format!(
        "({} OR {})",
        candidate("SpanAttributes", "", name),
        candidate("ResourceAttributes", RESOURCE_ATTRIBUTE_PREFIX, name)
    )
}

/// The arms of `resource.x`: the stream label it sanitises to, then the resource
/// attribute itself. Both exact.
fn resource_arms(name: &str) -> Vec<(String, String)> {
    let label = quote(&sanitize_label_name(name));
    let key = quote(name);
    vec![
        (
            format!("mapContains(Stream, {label})"),
            format!("Stream[{label}]"),
        ),
        (
            format!("mapContains(ResourceAttributes, {key})"),
            format!("ResourceAttributes[{key}]"),
        ),
    ]
}

/// Every attribute key and stream label the spans a query reads can carry, when known:
/// what turns a relaxed lookup — a key that sanitises alike — into the list of keys it
/// could find, each looked up exactly, in the order Rust's sorted map tries them.
#[derive(Debug, Default)]
pub(super) struct Keys {
    pub span: std::collections::BTreeSet<String>,
    pub resource: std::collections::BTreeSet<String>,
    pub stream: std::collections::BTreeSet<String>,
}

/// The keys besides `name` itself that `Labels::get_relaxed(name)` could take from a
/// span's attributes, in the order it tries them: the span's own and its resource's
/// under `resource.`, as one sorted map holds them.
fn relaxed_span_arms(name: &str, keys: &Keys) -> Vec<(String, String)> {
    let wanted = sanitize_label_name(name);
    let mut found: Vec<(String, &'static str, &str)> = keys
        .span
        .iter()
        .map(|k| (k.clone(), "SpanAttributes", k.as_str()))
        .chain(keys.resource.iter().map(|k| {
            (
                format!("{RESOURCE_ATTRIBUTE_PREFIX}{k}"),
                "ResourceAttributes",
                k.as_str(),
            )
        }))
        .filter(|(full, _, _)| full != name && sanitize_label_name(full) == wanted)
        .collect();
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
        .into_iter()
        .map(|(_, map, key)| {
            let key = quote(key);
            (
                format!("mapContains({map}, {key})"),
                format!("{map}[{key}]"),
            )
        })
        .collect()
}

/// The stream labels besides `name` that `Labels::get_relaxed(name)` could take.
fn relaxed_stream_arms(name: &str, keys: &Keys) -> Vec<(String, String)> {
    let wanted = sanitize_label_name(name);
    keys.stream
        .iter()
        .filter(|k| k.as_str() != name && sanitize_label_name(k) == wanted)
        .map(|k| {
            let key = quote(k);
            (
                format!("mapContains(Stream, {key})"),
                format!("Stream[{key}]"),
            )
        })
        .collect()
}

/// `span.x`'s lookup: the exact keys, then the relaxed ones — listed when the keys are
/// known, otherwise unknown wherever a key with the same letters is there.
fn span_lookup(name: &str, keys: Option<&Keys>) -> Vec<Arm> {
    let mut arms: Vec<Arm> = values(span_arms(name)).collect();
    match keys {
        Some(keys) => arms.extend(values(relaxed_span_arms(name, keys))),
        None => arms.push(Arm::Unknown {
            present: span_candidates(name),
        }),
    }
    arms
}

/// `.x`'s lookup: the span's attributes, relaxed; then the resource, exactly; then the
/// stream, relaxed.
fn unscoped_lookup(name: &str, keys: Option<&Keys>) -> Vec<Arm> {
    let stream_key = quote(name);
    let mut arms = span_lookup(name, keys);
    arms.extend(values(resource_arms(name)));
    arms.push(Arm::Value {
        present: format!("mapContains(Stream, {stream_key})"),
        value: format!("Stream[{stream_key}]"),
    });
    match keys {
        Some(keys) => arms.extend(values(relaxed_stream_arms(name, keys))),
        None => arms.push(Arm::Unknown {
            present: candidate("Stream", "", name),
        }),
    }
    arms
}

/// Both forms of one TraceQL condition, with the keys the spans can carry if known.
pub(super) fn condition(c: &SpanCondition, keys: Option<&Keys>) -> Stated {
    match &c.field {
        SpanField::Name => text_intrinsic("SpanName", c),
        SpanField::StatusMessage => text_intrinsic("StatusMessage", c),
        SpanField::Duration => Stated::exact(duration(c)),
        SpanField::Status => {
            let wanted = match &c.value {
                SpanValue::Status(status) => Some(*status),
                SpanValue::Text(text) => STATUSES
                    .into_iter()
                    .find(|s| s.as_str().eq_ignore_ascii_case(text)),
                _ => return Stated::exact("0".to_owned()),
            };
            Stated::exact(equality("StatusCode", c.op, wanted.map(status_name)))
        }
        SpanField::Kind => {
            let wanted = match &c.value {
                SpanValue::Kind(kind) => Some(*kind),
                SpanValue::Text(text) => KINDS
                    .into_iter()
                    .find(|k| k.as_str().eq_ignore_ascii_case(text)),
                _ => return Stated::exact("0".to_owned()),
            };
            Stated::exact(equality("SpanKind", c.op, wanted.map(kind_name)))
        }
        SpanField::Span(name) => with_presence(
            lookup(&span_lookup(name, keys), c.op, &c.value),
            c,
            &["SpanAttributes", "ResourceAttributes"],
        ),
        SpanField::Resource(name) => {
            let arms: Vec<Arm> = values(resource_arms(name)).collect();
            with_presence(
                lookup(&arms, c.op, &c.value),
                c,
                &["Stream", "ResourceAttributes"],
            )
        }
        SpanField::Unscoped(name) => with_presence(
            lookup(&unscoped_lookup(name, keys), c.op, &c.value),
            c,
            &["SpanAttributes", "ResourceAttributes", "Stream"],
        ),
    }
}

/// For equality with a text value, that it is among some map's values at all, in
/// front of the necessary form: implied by it, and what a Bloom filter index on
/// `mapValues` skips granules by.
fn with_presence(mut stated: Stated, c: &SpanCondition, maps: &[&str]) -> Stated {
    if let (SpanOp::Equal, SpanValue::Text(text)) = (c.op, &c.value) {
        let any: Vec<String> = maps
            .iter()
            .map(|map| format!("has(mapValues({map}), {})", quote(text)))
            .collect();
        stated.necessary = format!("(({}) AND {})", any.join(" OR "), stated.necessary);
    }
    stated
}

fn text_intrinsic(column: &str, c: &SpanCondition) -> Stated {
    if c.value == SpanValue::Nil {
        // An intrinsic is always there.
        return Stated::exact(if c.op == SpanOp::NotEqual { "1" } else { "0" }.to_owned());
    }
    present_value(c.op, &c.value, column).unwrap_or_else(|| Stated::exact("0".to_owned()))
}

fn duration(c: &SpanCondition) -> String {
    let nanos = match &c.value {
        #[allow(clippy::cast_precision_loss)]
        SpanValue::Nanos(nanos) => *nanos as f64,
        SpanValue::Number(number) => *number,
        _ => return "0".to_owned(),
    };
    match c.op {
        SpanOp::Equal => format!("abs(toFloat64(Duration) - {}) < {EPSILON}", float(nanos)),
        SpanOp::NotEqual => format!("abs(toFloat64(Duration) - {}) >= {EPSILON}", float(nanos)),
        SpanOp::Regex | SpanOp::NotRegex => "0".to_owned(),
        op => format!("toFloat64(Duration) {} {}", comparison(op), float(nanos)),
    }
}

fn equality(column: &str, op: SpanOp, wanted: Option<&str>) -> String {
    match (op, wanted) {
        (SpanOp::Equal, Some(name)) => format!("{column} = {}", quote(name)),
        (SpanOp::NotEqual, Some(name)) => format!("{column} != {}", quote(name)),
        // A status or kind nobody has: equal to nothing, unequal to everything.
        (SpanOp::NotEqual, None) => "1".to_owned(),
        _ => "0".to_owned(),
    }
}

/// A field's value as SQL, for grouping, with the condition under which it is the
/// value Rust would read: `None` for a field ClickHouse cannot read exactly.
pub(super) fn group_value(field: &SpanField, keys: Option<&Keys>) -> Option<(String, String)> {
    let chain = |arms: &[Arm]| -> String {
        let steps: Vec<String> = arms
            .iter()
            .filter_map(|arm| match arm {
                Arm::Value { present, value } => Some(format!("{present}, {value}")),
                Arm::Unknown { .. } => None,
            })
            .collect();
        format!("multiIf({}, NULL)", steps.join(", "))
    };
    Some(match field {
        SpanField::Name => ("SpanName".to_owned(), "1".to_owned()),
        SpanField::StatusMessage => ("StatusMessage".to_owned(), "1".to_owned()),
        SpanField::Status => ("lower(StatusCode)".to_owned(), "1".to_owned()),
        SpanField::Kind => ("lower(SpanKind)".to_owned(), "1".to_owned()),
        SpanField::Resource(name) => {
            let arms: Vec<Arm> = values(resource_arms(name)).collect();
            (chain(&arms), "1".to_owned())
        }
        SpanField::Span(name) => {
            let arms = span_lookup(name, keys);
            let decided = if keys.is_some() {
                "1".to_owned()
            } else {
                // The value is certain where an exact key is there, or no key with the
                // same letters is.
                let present: Vec<String> = span_arms(name).into_iter().map(|(p, _)| p).collect();
                format!(
                    "({} OR NOT {})",
                    present.join(" OR "),
                    span_candidates(name)
                )
            };
            (chain(&arms), decided)
        }
        // Without the keys, a relaxed stream lookup has no exact form.
        SpanField::Unscoped(name) => {
            keys?;
            (chain(&unscoped_lookup(name, keys)), "1".to_owned())
        }
        SpanField::Duration => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pattern_with_a_perl_class_is_not_decided_in_clickhouse() {
        assert!(same_in_re2("GET /(geo|re).*"));
        assert!(same_in_re2(r"a\.b\\w"));
        assert!(!same_in_re2(r"\w+"));
        assert!(!same_in_re2(r"id=\d{3}"));
        assert!(!same_in_re2(r"\bx"));
    }
}
