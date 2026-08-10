//! The TraceQL subset.
//!
//! Scoped precisely to what `laravel-telemetry-ui`'s `TraceqlCompiler` emits
//!: a single spanset of `&&`-joined conditions, optionally followed by
//! `| select(...)`.
//!
//! ```text
//! { resource.service.name = "checkout" && status = error && duration > 100ms }
//! ```
//!
//! As with LogQL, the input is parsed in full so that a construct outside the subset —
//! `||`, a second spanset, `count() > 2` — can be named rather than reported as a
//! syntax error.

use regex::Regex;
use telemetryd_core::record::sanitize_label_name;
use telemetryd_core::span::{SpanKind, SpanRecord, SpanStatus};
use telemetryd_core::{Error, Result};

use crate::lexer::{Spanned, Token, tokenize};

/// A parsed TraceQL query.
#[derive(Debug, Clone, Default)]
pub struct TraceQuery {
    /// `&&`-joined; all must hold for a span to match. Empty means "every span",
    /// which is what `{}` means and what the UI sends for an unfiltered search.
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone)]
pub struct Condition {
    pub field: Field,
    pub op: CompareOp,
    pub value: Value,
    regex: Option<Regex>,
}

/// What a condition reads from a span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Field {
    /// `name`, `status`, `duration`, `kind` — properties of the span itself.
    Intrinsic(Intrinsic),
    /// `resource.service.name` — a stream label.
    Resource(String),
    /// `span.http.status_code` — a span attribute.
    Span(String),
    /// `.foo` or a bare `foo` — look in span attributes, then resource labels.
    Unscoped(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intrinsic {
    Name,
    Status,
    Duration,
    Kind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Equal,
    NotEqual,
    Regex,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Number(f64),
    /// A duration literal, in nanoseconds.
    Duration(u64),
    Status(SpanStatus),
    Kind(SpanKind),
    /// `nil` — the attribute is absent.
    Nil,
}

/// Parse a TraceQL query.
pub fn parse(input: &str) -> Result<TraceQuery> {
    let trimmed = input.trim();
    // The UI sends `{}` when nothing is filtered; an empty string means the same.
    if trimmed.is_empty() {
        return Ok(TraceQuery::default());
    }

    let tokens = tokenize(trimmed)?;
    Parser {
        input: trimmed,
        tokens,
        pos: 0,
    }
    .parse_query()
}

struct Parser<'a> {
    input: &'a str,
    tokens: Vec<Spanned>,
    pos: usize,
}

impl Parser<'_> {
    fn parse_query(mut self) -> Result<TraceQuery> {
        self.expect(&Token::LeftBrace, "a spanset, e.g. { status = error }")?;

        let mut conditions = Vec::new();
        if self.peek() == Some(&Token::RightBrace) {
            self.pos += 1;
        } else {
            loop {
                conditions.push(self.parse_condition()?);
                match self.peek() {
                    // Accept the canonical `&&` and the word `and`.
                    Some(Token::AndAnd) => self.pos += 1,
                    Some(Token::Ident(word)) if word == "and" => self.pos += 1,
                    Some(Token::OrOr) => {
                        return Err(Error::unsupported_with_hint(
                            "TraceQL `||` between conditions",
                            "issue one search per alternative, or filter client-side",
                        ));
                    }
                    Some(Token::RightBrace) => {
                        self.pos += 1;
                        break;
                    }
                    _ => return Err(self.unexpected("`&&` or `}`")),
                }
            }
        }

        // `| select(a, b)` is accepted and ignored: telemetryd always returns the
        // matched spans in full, so a projection changes nothing about the result.
        if self.peek() == Some(&Token::Pipe) {
            self.pos += 1;
            let name = self.expect_ident("`select` after `|`")?;
            if name != "select" {
                return Err(Error::unsupported(format!("TraceQL `| {name}`")));
            }
            self.skip_balanced_parens()?;
        }

        if self.pos < self.tokens.len() {
            // A second spanset, `||`, or an aggregate — all real TraceQL, none of it
            // in the subset.
            return Err(Error::unsupported_with_hint(
                "TraceQL beyond a single spanset",
                "telemetryd supports one `{ … }` spanset of `&&`-joined conditions",
            ));
        }

        Ok(TraceQuery { conditions })
    }

    fn parse_condition(&mut self) -> Result<Condition> {
        let field = self.parse_field()?;
        let op = self.parse_op()?;
        let value = self.parse_value(&field)?;

        let regex = if op == CompareOp::Regex {
            let Value::Str(pattern) = &value else {
                return Err(Error::BadRequest(
                    "`=~` needs a quoted pattern on the right-hand side".to_owned(),
                ));
            };
            Some(Regex::new(pattern).map_err(|e| {
                Error::BadRequest(format!("invalid regular expression {pattern:?}: {e}"))
            })?)
        } else {
            None
        };

        Ok(Condition {
            field,
            op,
            value,
            regex,
        })
    }

    /// Field paths are dotted: `resource.service.name`, `span.http.status_code`. The
    /// lexer keeps them as one identifier.
    fn parse_field(&mut self) -> Result<Field> {
        let path = self.expect_ident("a field name")?;

        // A leading `.` is TraceQL's explicit "unscoped attribute" marker, and it is
        // checked before the intrinsics on purpose: `name` is the span's name, while
        // `.name` is an attribute that a producer happened to call "name". Collapsing
        // the two would answer a different question than the one asked.
        if let Some(attribute) = path.strip_prefix('.') {
            return Ok(Field::Unscoped(attribute.to_owned()));
        }

        Ok(match path.split_once('.') {
            Some(("resource", rest)) => Field::Resource(sanitize_label_name(rest)),
            // Span attributes keep the producer's spelling; `get_relaxed` accepts either.
            Some(("span", rest)) => Field::Span(rest.to_owned()),
            _ => match path.as_str() {
                "name" => Field::Intrinsic(Intrinsic::Name),
                "status" => Field::Intrinsic(Intrinsic::Status),
                "duration" => Field::Intrinsic(Intrinsic::Duration),
                "kind" => Field::Intrinsic(Intrinsic::Kind),
                // `rootName` / `rootServiceName` are trace-level, not span-level, and
                // reporting them as unknown attributes would silently match nothing.
                "rootName" | "rootServiceName" | "traceDuration" => {
                    return Err(Error::unsupported_with_hint(
                        format!("TraceQL trace-level intrinsic `{path}`"),
                        "filter on the span-level equivalent (`name`, `resource.service.name`, `duration`)",
                    ));
                }
                other => Field::Unscoped(other.to_owned()),
            },
        })
    }

    fn parse_op(&mut self) -> Result<CompareOp> {
        let op = match self.peek() {
            Some(Token::Equal) => CompareOp::Equal,
            Some(Token::NotEqual) => CompareOp::NotEqual,
            Some(Token::RegexMatch) => CompareOp::Regex,
            Some(Token::Greater) => CompareOp::Greater,
            Some(Token::GreaterEqual) => CompareOp::GreaterEqual,
            Some(Token::Less) => CompareOp::Less,
            Some(Token::LessEqual) => CompareOp::LessEqual,
            Some(Token::RegexNotMatch) => {
                return Err(Error::unsupported_with_hint(
                    "TraceQL `!~`",
                    "use `=~` with a negated pattern, or filter client-side",
                ));
            }
            _ => return Err(self.unexpected("a comparison operator")),
        };
        self.pos += 1;
        Ok(op)
    }

    fn parse_value(&mut self, field: &Field) -> Result<Value> {
        let value = match self.peek().cloned() {
            Some(Token::String(text)) => Value::Str(text),
            Some(Token::Number(number)) => Value::Number(number),
            Some(Token::Duration(nanos)) => Value::Duration(nanos),
            Some(Token::Ident(word)) => match word.as_str() {
                "nil" => Value::Nil,
                // Bare enum words: `status = error`, `kind = server`.
                other => match field {
                    Field::Intrinsic(Intrinsic::Status) => {
                        SpanStatus::from_otlp_name(&other.to_ascii_uppercase())
                            .map(Value::Status)
                            .ok_or_else(|| {
                                Error::BadRequest(format!(
                                    "{other:?} is not a span status (use ok, error or unset)"
                                ))
                            })?
                    }
                    Field::Intrinsic(Intrinsic::Kind) => {
                        SpanKind::from_otlp_name(&other.to_ascii_uppercase())
                            .map(Value::Kind)
                            .ok_or_else(|| {
                                Error::BadRequest(format!("{other:?} is not a span kind"))
                            })?
                    }
                    _ => Value::Str(other.to_owned()),
                },
            },
            _ => return Err(self.unexpected("a value")),
        };
        self.pos += 1;
        Ok(value)
    }

    fn skip_balanced_parens(&mut self) -> Result<()> {
        self.expect(&Token::LeftParen, "`(` after `select`")?;
        let mut depth = 1;
        while depth > 0 {
            match self.peek() {
                Some(Token::LeftParen) => depth += 1,
                Some(Token::RightParen) => depth -= 1,
                None => return Err(self.unexpected("`)`")),
                _ => {}
            }
            self.pos += 1;
        }
        Ok(())
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|s| &s.token)
    }

    fn expect(&mut self, expected: &Token, description: &str) -> Result<()> {
        if self.peek() == Some(expected) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.unexpected(description))
        }
    }

    fn expect_ident(&mut self, description: &str) -> Result<String> {
        match self.peek() {
            Some(Token::Ident(name)) => {
                let name = name.clone();
                self.pos += 1;
                Ok(name)
            }
            _ => Err(self.unexpected(description)),
        }
    }

    fn unexpected(&self, expected: &str) -> Error {
        match self.tokens.get(self.pos) {
            Some(spanned) => Error::BadRequest(format!(
                "expected {expected} but found `{}` at position {} in {:?}",
                spanned.token, spanned.offset, self.input
            )),
            None => Error::BadRequest(format!(
                "expected {expected} but the query ended: {:?}",
                self.input
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

impl TraceQuery {
    /// Whether a span satisfies every condition.
    pub fn matches(&self, span: &SpanRecord) -> bool {
        self.conditions.iter().all(|c| c.matches(span))
    }

    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }
}

impl Condition {
    pub fn matches(&self, span: &SpanRecord) -> bool {
        match &self.field {
            Field::Intrinsic(Intrinsic::Duration) => self.compare_number(
                #[allow(clippy::cast_precision_loss)]
                {
                    span.duration_nanos() as f64
                },
                self.value_as_nanos(),
            ),
            Field::Intrinsic(Intrinsic::Status) => {
                let actual = span.status;
                match &self.value {
                    Value::Status(expected) => self.compare_equality(actual == *expected),
                    Value::Str(text) => {
                        self.compare_equality(actual.as_str().eq_ignore_ascii_case(text))
                    }
                    _ => false,
                }
            }
            Field::Intrinsic(Intrinsic::Kind) => {
                let actual = span.kind;
                match &self.value {
                    Value::Kind(expected) => self.compare_equality(actual == *expected),
                    Value::Str(text) => {
                        self.compare_equality(actual.as_str().eq_ignore_ascii_case(text))
                    }
                    _ => false,
                }
            }
            Field::Intrinsic(Intrinsic::Name) => self.compare_text(Some(span.name.as_str())),
            Field::Resource(name) => self.compare_text(span.stream.get(name)),
            Field::Span(name) => self.compare_text(span.attributes.get_relaxed(name)),
            // Unscoped: span attributes take precedence, then resource labels — the
            // narrower scope wins, as in TraceQL.
            //
            // Both lookups are relaxed. Attributes keep the producer's spelling while
            // stream labels are sanitized, so `.service.name` has to reach the
            // `service_name` label — the dotted form is what a person actually types.
            Field::Unscoped(name) => self.compare_text(
                span.attributes
                    .get_relaxed(name)
                    .or_else(|| span.stream.get_relaxed(name)),
            ),
        }
    }

    /// Compare against a text-valued field, which may be absent.
    fn compare_text(&self, actual: Option<&str>) -> bool {
        // `= nil` and `!= nil` are how TraceQL tests for presence.
        if self.value == Value::Nil {
            return match self.op {
                CompareOp::Equal => actual.is_none(),
                CompareOp::NotEqual => actual.is_some(),
                _ => false,
            };
        }

        let Some(actual) = actual else {
            // An absent attribute matches only a negative comparison, mirroring the
            // label-matcher rule that an absent label is the empty string.
            return matches!(self.op, CompareOp::NotEqual);
        };

        match self.op {
            // Equality accepts a number on the right as well as a string. OTLP int
            // attributes are stored as label text, so `= 500` and `= "500"` have to
            // mean the same thing or half the obvious queries silently match nothing.
            CompareOp::Equal | CompareOp::NotEqual => {
                let equal = match &self.value {
                    Value::Str(text) => actual == text,
                    _ => actual.parse::<f64>().is_ok_and(|number| {
                        self.value_as_number()
                            .is_some_and(|expected| (number - expected).abs() < f64::EPSILON)
                    }),
                };
                if self.op == CompareOp::Equal {
                    equal
                } else {
                    !equal
                }
            }
            CompareOp::Regex => self.regex.as_ref().is_some_and(|r| r.is_match(actual)),
            // Ordered comparison against a text field: parse and compare, which is how
            // `span.http.status_code > 499` works when the attribute is a string.
            _ => actual
                .parse::<f64>()
                .is_ok_and(|number| self.compare_number(number, self.value_as_number())),
        }
    }

    fn compare_equality(&self, equal: bool) -> bool {
        match self.op {
            CompareOp::Equal => equal,
            CompareOp::NotEqual => !equal,
            _ => false,
        }
    }

    fn compare_number(&self, actual: f64, expected: Option<f64>) -> bool {
        let Some(expected) = expected else {
            return false;
        };
        match self.op {
            CompareOp::Equal => (actual - expected).abs() < f64::EPSILON,
            CompareOp::NotEqual => (actual - expected).abs() >= f64::EPSILON,
            CompareOp::Greater => actual > expected,
            CompareOp::GreaterEqual => actual >= expected,
            CompareOp::Less => actual < expected,
            CompareOp::LessEqual => actual <= expected,
            CompareOp::Regex => false,
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn value_as_number(&self) -> Option<f64> {
        match &self.value {
            Value::Number(number) => Some(*number),
            Value::Duration(nanos) => Some(*nanos as f64),
            Value::Str(text) => text.parse().ok(),
            _ => None,
        }
    }

    /// A duration comparison always works in nanoseconds, so `duration > 100ms` and
    /// `duration > 100000000` mean the same thing.
    #[allow(clippy::cast_precision_loss)]
    fn value_as_nanos(&self) -> Option<f64> {
        match &self.value {
            Value::Duration(nanos) => Some(*nanos as f64),
            Value::Number(number) => Some(*number),
            _ => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use telemetryd_core::Labels;

    fn span() -> SpanRecord {
        let mut stream = Labels::new();
        stream.insert("app", "checkout");
        stream.insert("service_name", "checkout");

        // As ingest stores them: the producer's own key spelling.
        let mut attributes = Labels::new();
        attributes.insert("http.method", "POST");
        attributes.insert("http.status_code", "500");

        SpanRecord {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_owned(),
            span_id: "00f067aa0ba902b7".to_owned(),
            parent_span_id: None,
            name: "POST /checkout".to_owned(),
            kind: SpanKind::Server,
            start_nanos: 1_750_000_000_000_000_000,
            end_nanos: 1_750_000_000_150_000_000,
            status: SpanStatus::Error,
            status_message: String::new(),
            stream,
            attributes,
            events: Vec::new(),
        }
    }

    #[test]
    fn an_empty_spanset_matches_everything() {
        for input in ["{}", "", "  "] {
            let query = parse(input).unwrap();
            assert!(query.is_empty(), "{input}");
            assert!(query.matches(&span()), "{input}");
        }
    }

    #[test]
    fn the_uis_compiled_form_parses_and_matches() {
        // Exactly what TraceqlCompiler emits.
        let query = parse(
            r#"{ resource.service.name = "checkout" && status = error && duration > 100ms }"#,
        )
        .unwrap();
        assert_eq!(query.conditions.len(), 3);
        assert!(query.matches(&span()));
    }

    #[test]
    fn resource_and_span_scopes_read_different_places() {
        assert!(
            parse(r#"{ resource.service.name = "checkout" }"#)
                .unwrap()
                .matches(&span())
        );
        assert!(
            !parse(r#"{ span.service.name = "checkout" }"#)
                .unwrap()
                .matches(&span())
        );

        assert!(
            parse(r#"{ span.http.method = "POST" }"#)
                .unwrap()
                .matches(&span())
        );
        assert!(
            !parse(r#"{ resource.http.method = "POST" }"#)
                .unwrap()
                .matches(&span())
        );
    }

    #[test]
    fn unscoped_fields_prefer_span_attributes() {
        assert!(
            parse(r#"{ http.method = "POST" }"#)
                .unwrap()
                .matches(&span())
        );
        assert!(
            parse(r#"{ service_name = "checkout" }"#)
                .unwrap()
                .matches(&span())
        );
    }

    /// The dotted form is what TraceQL actually specifies for an unscoped attribute,
    /// and it is what a person types into a search box. It used to be a lexer error:
    /// `.` could continue an identifier but never start one, so every canonical
    /// TraceQL attribute filter came back as a 400 while the non-standard bare form
    /// worked. Found by querying a released build rather than by a unit test.
    #[test]
    fn the_leading_dot_attribute_form_parses_and_matches() {
        // A span attribute, reached by the producer's own dotted spelling.
        assert!(
            parse(r#"{ .http.method = "POST" }"#)
                .unwrap()
                .matches(&span())
        );
        assert!(
            !parse(r#"{ .http.method = "GET" }"#)
                .unwrap()
                .matches(&span())
        );

        // A resource label, which ingest sanitized to `service_name`. The dotted
        // name still has to reach it.
        assert!(
            parse(r#"{ .service.name = "checkout" }"#)
                .unwrap()
                .matches(&span())
        );

        // An attribute nobody set matches nothing rather than erroring.
        assert!(!parse(r#"{ .nonexistent = "x" }"#).unwrap().matches(&span()));
    }

    /// `name` is the span's name; `.name` is an attribute a producer happened to call
    /// "name". Collapsing them would answer a different question than the one asked.
    #[test]
    fn a_leading_dot_means_attribute_not_intrinsic() {
        let mut s = span();
        s.attributes.insert("name", "not-the-span-name");

        assert!(parse(r#"{ name = "POST /checkout" }"#).unwrap().matches(&s));
        assert!(
            !parse(r#"{ .name = "POST /checkout" }"#)
                .unwrap()
                .matches(&s)
        );
        assert!(
            parse(r#"{ .name = "not-the-span-name" }"#)
                .unwrap()
                .matches(&s)
        );
    }

    #[test]
    fn status_is_compared_as_an_enum_not_a_string() {
        assert!(parse("{ status = error }").unwrap().matches(&span()));
        assert!(!parse("{ status = ok }").unwrap().matches(&span()));
        assert!(parse("{ status != ok }").unwrap().matches(&span()));

        // Unset must not satisfy either ok or error.
        let mut unset = span();
        unset.status = SpanStatus::Unset;
        assert!(!parse("{ status = error }").unwrap().matches(&unset));
        assert!(!parse("{ status = ok }").unwrap().matches(&unset));
    }

    #[test]
    fn kind_is_compared_as_an_enum() {
        assert!(parse("{ kind = server }").unwrap().matches(&span()));
        assert!(!parse("{ kind = client }").unwrap().matches(&span()));
    }

    #[test]
    fn durations_compare_in_nanoseconds_whatever_the_literal() {
        assert!(parse("{ duration > 100ms }").unwrap().matches(&span()));
        assert!(parse("{ duration >= 150ms }").unwrap().matches(&span()));
        assert!(!parse("{ duration > 200ms }").unwrap().matches(&span()));
        assert!(parse("{ duration < 1s }").unwrap().matches(&span()));
        // A bare number is nanoseconds.
        assert!(parse("{ duration > 100000000 }").unwrap().matches(&span()));
    }

    #[test]
    fn numeric_comparison_works_against_string_attributes() {
        // OTLP int attributes become string label values, so `> 499` has to parse.
        assert!(
            parse("{ span.http.status_code > 499 }")
                .unwrap()
                .matches(&span())
        );
        assert!(
            !parse("{ span.http.status_code > 500 }")
                .unwrap()
                .matches(&span())
        );
        assert!(
            parse("{ span.http.status_code = 500 }")
                .unwrap()
                .matches(&span())
        );
    }

    #[test]
    fn regex_conditions_work() {
        assert!(parse(r#"{ name =~ "POST.*" }"#).unwrap().matches(&span()));
        assert!(!parse(r#"{ name =~ "GET.*" }"#).unwrap().matches(&span()));
    }

    #[test]
    fn nil_tests_presence() {
        assert!(parse("{ span.missing = nil }").unwrap().matches(&span()));
        assert!(
            !parse("{ span.http.method = nil }")
                .unwrap()
                .matches(&span())
        );
        assert!(
            parse("{ span.http.method != nil }")
                .unwrap()
                .matches(&span())
        );
    }

    #[test]
    fn an_absent_attribute_matches_only_a_negative_comparison() {
        assert!(
            parse(r#"{ span.missing != "x" }"#)
                .unwrap()
                .matches(&span())
        );
        assert!(!parse(r#"{ span.missing = "x" }"#).unwrap().matches(&span()));
    }

    #[test]
    fn select_is_accepted_and_ignored() {
        // telemetryd always returns the matched spans in full, so a projection changes
        // nothing — but refusing it would break a query the UI legitimately sends.
        let query =
            parse("{ status = error } | select(span.http.method, resource.service.name)").unwrap();
        assert_eq!(query.conditions.len(), 1);
        assert!(query.matches(&span()));
    }

    #[test]
    fn conditions_are_joined_with_and() {
        let query = parse(r#"{ status = error && span.http.method = "GET" }"#).unwrap();
        assert!(!query.matches(&span()), "every condition must hold");
    }

    #[test]
    fn constructs_outside_the_subset_are_named() {
        let cases = [
            (r#"{ status = error } && { name = "x" }"#, "single spanset"),
            ("{ status = error } | count() > 2", "count"),
            (r#"{ name !~ "x" }"#, "!~"),
            ("{ rootServiceName = \"x\" }", "rootServiceName"),
        ];
        for (query, needle) in cases {
            let err = parse(query).unwrap_err();
            assert!(
                matches!(err, Error::Unsupported { .. }),
                "{query} should be Unsupported, got {err:?}"
            );
            assert!(err.to_string().contains(needle), "{query}: {err}");
        }
    }

    #[test]
    fn malformed_queries_are_client_errors() {
        for query in ["{", "{ status", "{ status = }", "{ = error }", "}{"] {
            let err = parse(query).unwrap_err();
            assert!(
                matches!(err, Error::BadRequest(_) | Error::Unsupported { .. }),
                "{query} produced {err:?}"
            );
        }
    }

    #[test]
    fn an_invalid_regex_is_a_clean_client_error() {
        let err = parse(r#"{ name =~ "[unclosed" }"#).unwrap_err();
        assert!(matches!(err, Error::BadRequest(_)));
    }
}
