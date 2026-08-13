//! The LogQL subset.
//!
//! The query is **parsed in full** and then lowered to what telemetryd can execute.
//! That is the whole reason a user hitting a subset boundary gets
//! "`| line_format` is not supported by telemetryd" instead of "syntax error" — the
//! parser has to recognise the construct in order to name it.
//!
//! Supported: stream selectors, line filters (`|=`, `!=`, `|~`, `!~`), the `json` and
//! `logfmt` parsers, and label filters. See `COMPATIBILITY.md`.

use regex::Regex;
use telemetryd_core::{Error, LabelMatcher, Labels, MatchOp, Result};

use crate::lexer::{Spanned, Token, tokenize};

/// A parsed and lowered log query.
#[derive(Debug, Clone)]
pub struct LogQuery {
    /// The stream selector. Never empty — an unselective query would scan everything.
    pub matchers: Vec<LabelMatcher>,
    pub stages: Vec<Stage>,
}

#[derive(Debug, Clone)]
pub enum Stage {
    Line(LineFilter),
    /// Parse the line as JSON and merge its fields into the label set.
    Json,
    /// Parse the line as logfmt and merge its fields into the label set.
    Logfmt,
    Label(LabelPredicate),
}

/// A label filter stage: one or more matchers combined with `and` / `or`.
///
/// LogQL allows `| status="500" or status="503"` in a single stage, and
/// `laravel-telemetry-ui` generates exactly that. Supporting only a bare matcher would
/// turn an ordinary UI query into a syntax error.
#[derive(Debug, Clone)]
pub enum LabelPredicate {
    Match(LabelMatcher),
    And(Box<LabelPredicate>, Box<LabelPredicate>),
    Or(Box<LabelPredicate>, Box<LabelPredicate>),
}

impl LabelPredicate {
    pub fn matches(&self, labels: &Labels) -> bool {
        match self {
            Self::Match(matcher) => matcher.matches(labels),
            Self::And(left, right) => left.matches(labels) && right.matches(labels),
            Self::Or(left, right) => left.matches(labels) || right.matches(labels),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LineFilter {
    pub op: LineOp,
    pub pattern: String,
    regex: Option<Regex>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineOp {
    Contains,
    NotContains,
    Matches,
    NotMatches,
}

impl LogQuery {
    /// A substring that every matching line must contain, if the pipeline states one.
    ///
    /// Used to skip whole segments via their trigram index, so being wrong here drops
    /// results silently. Only a positive `|=` qualifies:
    ///
    /// - `!=` and `!~` are satisfied by lines that lack the pattern, so they say
    ///   nothing about what a segment must hold
    /// - `|~` is a regular expression; its literal text is not necessarily a substring
    ///   of the lines it matches
    ///
    /// The first qualifying filter is enough — one necessary condition prunes as
    /// soundly as several, and stopping there keeps the rule easy to check.
    #[must_use]
    pub fn required_substring(&self) -> Option<&str> {
        self.stages.iter().find_map(|stage| match stage {
            Stage::Line(filter) if filter.op == LineOp::Contains => Some(filter.pattern.as_str()),
            _ => None,
        })
    }
}

impl LineFilter {
    fn new(op: LineOp, pattern: String) -> Result<Self> {
        let regex = match op {
            LineOp::Matches | LineOp::NotMatches => Some(Regex::new(&pattern).map_err(|e| {
                Error::BadRequest(format!("invalid regular expression {pattern:?}: {e}"))
            })?),
            _ => None,
        };
        Ok(Self { op, pattern, regex })
    }

    /// Test a log line.
    ///
    /// Unlike label matchers, line-filter regexes are **not** anchored: `|~ "err"`
    /// means "the line contains something matching err", which is what makes it
    /// useful for searching free text.
    pub fn matches(&self, line: &str) -> bool {
        match self.op {
            LineOp::Contains => line.contains(&self.pattern),
            LineOp::NotContains => !line.contains(&self.pattern),
            LineOp::Matches => self.regex.as_ref().is_some_and(|r| r.is_match(line)),
            LineOp::NotMatches => !self.regex.as_ref().is_some_and(|r| r.is_match(line)),
        }
    }
}

/// Parse a LogQL log-selector query.
pub fn parse(input: &str) -> Result<LogQuery> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(Error::BadRequest("empty LogQL query".to_owned()));
    }
    Parser::new(input, tokens).parse_log_query()
}

struct Parser<'a> {
    input: &'a str,
    tokens: Vec<Spanned>,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str, tokens: Vec<Spanned>) -> Self {
        Self {
            input,
            tokens,
            pos: 0,
        }
    }

    fn parse_log_query(mut self) -> Result<LogQuery> {
        // A metric query starts with a function call or an aggregation. Detecting it
        // here is what turns `rate({app="x"}[5m])` into a named, actionable error.
        self.reject_metric_query()?;

        let matchers = self.parse_selector()?;
        let mut stages = Vec::new();
        while self.pos < self.tokens.len() {
            stages.push(self.parse_stage()?);
        }

        Ok(LogQuery { matchers, stages })
    }

    fn reject_metric_query(&self) -> Result<()> {
        let Some(Token::Ident(name)) = self.tokens.first().map(|s| &s.token) else {
            return Ok(());
        };
        // `sum by (app) (rate(...))` and `rate({...}[5m])` both start with an ident.
        let hint = match name.as_str() {
            "rate" | "count_over_time" | "bytes_rate" | "bytes_over_time" | "sum_over_time"
            | "avg_over_time" | "max_over_time" | "min_over_time" | "quantile_over_time"
            | "stdvar_over_time" | "stddev_over_time" | "first_over_time" | "last_over_time"
            | "absent_over_time" => Some(
                "aggregate the results client-side, or use the Prometheus API for pre-aggregated metrics",
            ),
            "sum" | "avg" | "min" | "max" | "count" | "topk" | "bottomk" | "stddev" | "stdvar" => {
                Some(
                    "aggregations over log streams are not available; select the streams and aggregate client-side",
                )
            }
            _ => None,
        };
        if let Some(hint) = hint {
            return Err(Error::unsupported_with_hint(
                format!("LogQL metric query `{name}`"),
                hint,
            ));
        }
        Ok(())
    }

    fn parse_selector(&mut self) -> Result<Vec<LabelMatcher>> {
        self.expect(
            &Token::LeftBrace,
            "a stream selector, e.g. {app=\"checkout\"}",
        )?;

        let mut matchers = Vec::new();
        if self.peek() == Some(&Token::RightBrace) {
            self.pos += 1;
            return Err(Error::BadRequest(
                "the stream selector {} matches every stream; add at least one matcher \
                 so the query does not scan the whole store"
                    .to_owned(),
            ));
        }

        loop {
            let name = self.expect_ident("a label name")?;
            let op = self.expect_match_op()?;
            let value = self.expect_string("a quoted label value")?;
            matchers.push(LabelMatcher::new(name, op, value)?);

            match self.peek() {
                Some(Token::Comma) => {
                    self.pos += 1;
                    // Trailing comma before the brace is accepted.
                    if self.peek() == Some(&Token::RightBrace) {
                        self.pos += 1;
                        break;
                    }
                }
                Some(Token::RightBrace) => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(self.unexpected("`,` or `}`")),
            }
        }

        if matchers.iter().all(|m| !m.is_selective()) {
            return Err(Error::BadRequest(
                "the stream selector needs at least one matcher that requires a value; \
                 a selector built only from negative or match-everything matchers would \
                 scan the whole store"
                    .to_owned(),
            ));
        }

        Ok(matchers)
    }

    fn parse_stage(&mut self) -> Result<Stage> {
        match self.peek().cloned() {
            Some(Token::LineContains) => self.line_filter(LineOp::Contains),
            Some(Token::LineRegex) => self.line_filter(LineOp::Matches),
            Some(Token::NotEqual) => self.line_filter(LineOp::NotContains),
            Some(Token::RegexNotMatch) => self.line_filter(LineOp::NotMatches),
            Some(Token::Pipe) => {
                self.pos += 1;
                self.parse_pipe_stage()
            }
            Some(_) => Err(self.unexpected("a line filter (`|=`, `!=`, `|~`, `!~`) or `|`")),
            None => Err(self.unexpected("a pipeline stage")),
        }
    }

    fn line_filter(&mut self, op: LineOp) -> Result<Stage> {
        self.pos += 1;
        let pattern = self.expect_string("a quoted pattern after a line filter")?;
        Ok(Stage::Line(LineFilter::new(op, pattern)?))
    }

    fn parse_pipe_stage(&mut self) -> Result<Stage> {
        let name = self.expect_ident("a parser or label filter after `|`")?;

        match name.as_str() {
            "json" => {
                // `| json foo="bar.baz"` selects specific fields; we support only the
                // bare form, and saying which is better than failing vaguely.
                if matches!(self.peek(), Some(Token::Ident(_))) {
                    return Err(Error::unsupported_with_hint(
                        "LogQL `json` with explicit field expressions",
                        "use bare `| json`, then filter with a label filter",
                    ));
                }
                Ok(Stage::Json)
            }
            "logfmt" => Ok(Stage::Logfmt),

            // Recognised specifically so the error can name them.
            "line_format" | "label_format" => Err(Error::unsupported_with_hint(
                format!("LogQL `| {name}`"),
                "formatting is not applied server-side; render the line client-side",
            )),
            "unwrap" => Err(Error::unsupported_with_hint(
                "LogQL `| unwrap`",
                "unwrap only feeds metric queries, which telemetryd does not run over logs",
            )),
            "pattern" | "regexp" => Err(Error::unsupported_with_hint(
                format!("LogQL `| {name}` parser"),
                "use `| json` or `| logfmt`, or filter the line with `|~`",
            )),
            "drop" | "keep" => Err(Error::unsupported_with_hint(
                format!("LogQL `| {name}`"),
                "select the labels you want client-side",
            )),
            "decolorize" | "distinct" | "ip" => {
                Err(Error::unsupported(format!("LogQL `| {name}`")))
            }

            // Anything else in this position is a label filter.
            _ => {
                let first = self.label_matcher(name)?;
                Ok(Stage::Label(self.label_predicate_tail(first)?))
            }
        }
    }

    /// Parse one `name op "value"` matcher, given the already-consumed name.
    fn label_matcher(&mut self, name: String) -> Result<LabelPredicate> {
        let op = self.expect_match_op()?;
        let value = self.expect_string("a quoted value in a label filter")?;
        Ok(LabelPredicate::Match(LabelMatcher::new(name, op, value)?))
    }

    /// Extend a matcher with any `and` / `or` continuation.
    ///
    /// `and` binds tighter than `or`, as in LogQL, so `a or b and c` is `a or (b and c)`.
    fn label_predicate_tail(&mut self, first: LabelPredicate) -> Result<LabelPredicate> {
        let mut left = self.label_predicate_and(first)?;

        while matches!(self.peek(), Some(Token::Ident(word)) if word == "or") {
            self.pos += 1;
            let name = self.expect_ident("a label name after `or`")?;
            let next = self.label_matcher(name)?;
            let right = self.label_predicate_and(next)?;
            left = LabelPredicate::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn label_predicate_and(&mut self, first: LabelPredicate) -> Result<LabelPredicate> {
        let mut left = first;
        while matches!(self.peek(), Some(Token::Ident(word)) if word == "and") {
            self.pos += 1;
            let name = self.expect_ident("a label name after `and`")?;
            let right = self.label_matcher(name)?;
            left = LabelPredicate::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // -- token helpers -----------------------------------------------------

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

    fn expect_string(&mut self, description: &str) -> Result<String> {
        match self.peek() {
            Some(Token::String(value)) => {
                let value = value.clone();
                self.pos += 1;
                Ok(value)
            }
            _ => Err(self.unexpected(description)),
        }
    }

    fn expect_match_op(&mut self) -> Result<MatchOp> {
        let op = match self.peek() {
            Some(Token::Equal) => MatchOp::Equal,
            Some(Token::NotEqual) => MatchOp::NotEqual,
            Some(Token::RegexMatch) => MatchOp::Regex,
            Some(Token::RegexNotMatch) => MatchOp::NotRegex,
            // Numeric comparisons are a real LogQL feature we do not run.
            Some(Token::Greater | Token::GreaterEqual | Token::Less | Token::LessEqual) => {
                return Err(Error::unsupported_with_hint(
                    "LogQL numeric label filters (`>`, `>=`, `<`, `<=`)",
                    "compare as strings with `=` or `=~`, or filter client-side",
                ));
            }
            _ => return Err(self.unexpected("a matcher operator (`=`, `!=`, `=~`, `!~`)")),
        };
        self.pos += 1;
        Ok(op)
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

impl LogQuery {
    /// Run the pipeline against one line.
    ///
    /// `base` is the label set available to label filters before any parser stage:
    /// the stream labels plus the record's own attributes. Exposing record attributes
    /// without requiring a parser stage is a deliberate superset of Loki — the data is
    /// already structured, so making a user write `| json` to reach it would be
    /// theatre. Documented in `COMPATIBILITY.md`.
    /// What a parser stage pulled out of this line, for a record that already matched.
    ///
    /// # Why this is a second pass
    ///
    /// [`Self::evaluate`] builds the same set to answer label filters and then drops it,
    /// because it runs inside the scan on every candidate row and returning a `Labels`
    /// per row would allocate for records that are about to be rejected. This runs only
    /// on the records actually being returned — at most `limit` of them — so the cost is
    /// bounded by the response rather than by the search.
    ///
    /// # The bug this closes
    ///
    /// `| json` and `| logfmt` were filter-only: they parsed the body to decide whether a
    /// record matched, and nothing about the extraction reached the caller. Asking for
    /// `| json | level="error"` returned the line and a stream whose `level` label said
    /// `info` — telemetryd's severity-derived label, not the one the filter matched on. A
    /// user could not see the value their own query had selected, and the answer looked
    /// like it contradicted the question.
    ///
    /// Returns an empty set when the query has no parser stage, so a caller can merge it
    /// unconditionally.
    pub fn extracted(&self, line: &str) -> Labels {
        let mut labels = Labels::new();
        for stage in &self.stages {
            match stage {
                Stage::Json => merge_json(&mut labels, line),
                Stage::Logfmt => merge_logfmt(&mut labels, line),
                Stage::Line(_) | Stage::Label(_) => {}
            }
        }
        labels
    }

    /// Whether this query parses the line at all, so a caller can skip the second pass.
    pub fn has_parser_stage(&self) -> bool {
        self.stages
            .iter()
            .any(|stage| matches!(stage, Stage::Json | Stage::Logfmt))
    }

    pub fn evaluate(&self, line: &str, base: &Labels) -> bool {
        let mut extracted: Option<Labels> = None;

        for stage in &self.stages {
            match stage {
                Stage::Line(filter) => {
                    if !filter.matches(line) {
                        return false;
                    }
                }
                Stage::Json => {
                    let labels = extracted.get_or_insert_with(|| base.clone());
                    merge_json(labels, line);
                }
                Stage::Logfmt => {
                    let labels = extracted.get_or_insert_with(|| base.clone());
                    merge_logfmt(labels, line);
                }
                Stage::Label(predicate) => {
                    let labels = extracted.as_ref().unwrap_or(base);
                    if !predicate.matches(labels) {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Whether any stage can reject a line. A selector-only query skips per-line work.
    pub fn has_filters(&self) -> bool {
        self.stages
            .iter()
            .any(|s| matches!(s, Stage::Line(_) | Stage::Label(_)))
    }
}

/// Merge a JSON object's fields into the label set, flattening nested paths with `_`
/// as Loki does.
fn merge_json(labels: &mut Labels, line: &str) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
        // A line that is not JSON simply contributes no fields; it is not an error.
        // Log streams are rarely homogeneous, and failing the query because one line
        // out of a million is plain text would make `| json` unusable.
        return;
    };
    flatten_json(labels, "", &value);
}

fn flatten_json(labels: &mut Labels, prefix: &str, value: &serde_json::Value) {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let name = if prefix.is_empty() {
                    telemetryd_core::record::sanitize_label_name(key)
                } else {
                    format!(
                        "{prefix}_{}",
                        telemetryd_core::record::sanitize_label_name(key)
                    )
                };
                flatten_json(labels, &name, child);
            }
        }
        Value::Null => {}
        Value::String(text) => {
            if !prefix.is_empty() {
                labels.insert(prefix, text.clone());
            }
        }
        other => {
            if !prefix.is_empty() {
                labels.insert(prefix, other.to_string());
            }
        }
    }
}

/// Merge logfmt (`key=value key2="quoted value"`) into the label set.
fn merge_logfmt(labels: &mut Labels, line: &str) {
    let bytes = line.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' && !bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if key_start == i {
            i += 1;
            continue;
        }
        let key = &line[key_start..i];

        if i >= bytes.len() || bytes[i] != b'=' {
            // A bare token is a flag; logfmt treats it as an empty value.
            labels.insert(telemetryd_core::record::sanitize_label_name(key), "");
            continue;
        }
        i += 1;

        let value = if i < bytes.len() && bytes[i] == b'"' {
            i += 1;
            let mut out = String::new();
            while i < bytes.len() && bytes[i] != b'"' {
                // Always advance by whole characters. Stepping a single byte past a
                // multi-byte lead byte would leave `i` inside a UTF-8 sequence and the
                // next slice would panic — reachable from any log line.
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    i += 1;
                    let escaped = line[i..].chars().next().unwrap_or('\u{fffd}');
                    i += escaped.len_utf8();
                    out.push(match escaped {
                        'n' => '\n',
                        't' => '\t',
                        other => other,
                    });
                } else {
                    let c = line[i..].chars().next().unwrap_or('\u{fffd}');
                    out.push(c);
                    i += c.len_utf8();
                }
            }
            i += 1;
            out
        } else {
            let start = i;
            while i < bytes.len() && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            line[start..i].to_owned()
        };

        labels.insert(telemetryd_core::record::sanitize_label_name(key), value);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    // -- selectors ---------------------------------------------------------

    #[test]
    fn parses_a_selector_with_every_matcher_operator() {
        let query =
            parse(r#"{app="checkout", env!="dev", level=~"err.*", pod!~"canary.*"}"#).unwrap();
        assert_eq!(query.matchers.len(), 4);
        assert!(query.stages.is_empty());

        let base = labels(&[("app", "checkout"), ("level", "error")]);
        assert!(telemetryd_core::matches_all(&query.matchers, &base));
    }

    #[test]
    fn a_trailing_comma_is_accepted() {
        assert_eq!(parse(r#"{app="x",}"#).unwrap().matchers.len(), 1);
    }

    #[test]
    fn an_empty_selector_is_refused_with_a_reason() {
        let err = parse("{}").unwrap_err().to_string();
        assert!(err.contains("matches every stream"), "{err}");
    }

    #[test]
    fn a_selector_that_cannot_prune_is_refused() {
        // {app!="x"} matches streams with no app label at all, so it selects
        // everything — running it would scan the whole store.
        let err = parse(r#"{app!="x"}"#).unwrap_err().to_string();
        assert!(
            err.contains("at least one matcher that requires a value"),
            "{err}"
        );
    }

    #[test]
    fn a_missing_selector_is_a_clear_error() {
        let err = parse(r#"app="x""#).unwrap_err().to_string();
        assert!(err.contains("stream selector"), "{err}");
    }

    // -- line filters ------------------------------------------------------

    #[test]
    fn line_filters_parse_and_evaluate() {
        let query =
            parse(r#"{app="x"} |= "payment" != "test" |~ "declin(ed|e)" !~ "retry""#).unwrap();
        assert_eq!(query.stages.len(), 4);

        let base = labels(&[("app", "x")]);
        assert!(query.evaluate("payment declined for order 9912", &base));
        assert!(
            !query.evaluate("payment declined test", &base),
            "!= should reject"
        );
        assert!(!query.evaluate("order created", &base), "|= should reject");
        assert!(
            !query.evaluate("payment declined retry", &base),
            "!~ should reject"
        );
    }

    #[test]
    fn line_filter_regexes_are_not_anchored() {
        // Unlike label matchers: |~ "err" means the line *contains* a match.
        let query = parse(r#"{app="x"} |~ "err""#).unwrap();
        assert!(query.evaluate("an error occurred", &labels(&[])));
    }

    #[test]
    fn an_invalid_line_filter_regex_is_a_clean_client_error() {
        let err = parse(r#"{app="x"} |~ "[unclosed""#).unwrap_err();
        assert!(matches!(err, Error::BadRequest(_)));
        assert!(err.to_string().contains("[unclosed"));
    }

    // -- parsers and label filters ----------------------------------------

    #[test]
    fn json_parser_extracts_and_flattens_fields() {
        let query = parse(r#"{app="x"} | json | user_id="42""#).unwrap();
        let base = labels(&[("app", "x")]);

        assert!(query.evaluate(r#"{"user":{"id":"42"},"msg":"hi"}"#, &base));
        assert!(!query.evaluate(r#"{"user":{"id":"43"}}"#, &base));
    }

    #[test]
    fn json_numbers_and_booleans_become_label_values() {
        let query = parse(r#"{app="x"} | json | status="200""#).unwrap();
        assert!(query.evaluate(r#"{"status":200}"#, &labels(&[])));

        let flag = parse(r#"{app="x"} | json | ok="true""#).unwrap();
        assert!(flag.evaluate(r#"{"ok":true}"#, &labels(&[])));
    }

    #[test]
    fn a_non_json_line_contributes_nothing_rather_than_failing() {
        // Log streams are rarely homogeneous; one plain-text line must not break the
        // query for the other million.
        let query = parse(r#"{app="x"} | json | level="error""#).unwrap();
        assert!(!query.evaluate("this is not json", &labels(&[])));
        // …and a line that does parse still matches.
        assert!(query.evaluate(r#"{"level":"error"}"#, &labels(&[])));
    }

    #[test]
    fn logfmt_parser_handles_quoted_and_bare_values() {
        let query = parse(r#"{app="x"} | logfmt | method="GET""#).unwrap();
        assert!(query.evaluate("method=GET path=/api status=200", &labels(&[])));

        let quoted = parse(r#"{app="x"} | logfmt | msg="hello world""#).unwrap();
        assert!(quoted.evaluate(r#"level=info msg="hello world""#, &labels(&[])));
    }

    #[test]
    fn label_filters_see_record_attributes_without_a_parser_stage() {
        // telemetryd extension: OTLP records are already structured, so requiring a
        // `| json` to reach their attributes would be theatre.
        let query = parse(r#"{app="x"} | order_id="9912""#).unwrap();
        let base = labels(&[("app", "x"), ("order_id", "9912")]);
        assert!(query.evaluate("anything", &base));

        let miss = parse(r#"{app="x"} | order_id="1""#).unwrap();
        assert!(!miss.evaluate("anything", &base));
    }

    #[test]
    fn label_filters_accept_regex_operators() {
        let query = parse(r#"{app="x"} | route=~"/api/.*""#).unwrap();
        assert!(query.evaluate("x", &labels(&[("route", "/api/orders")])));
        assert!(!query.evaluate("x", &labels(&[("route", "/health")])));
    }

    // -- the subset boundary ----------------------------------------------

    #[test]
    fn metric_queries_are_named_not_rejected_as_syntax_errors() {
        for query in [
            r#"rate({app="x"}[5m])"#,
            r#"count_over_time({app="x"}[1h])"#,
            r#"sum by (app) (rate({app="x"}[5m]))"#,
        ] {
            let err = parse(query).unwrap_err();
            assert!(
                matches!(err, Error::Unsupported { .. }),
                "{query} should be Unsupported, got {err:?}"
            );
            let body = serde_json::to_value(err.to_body()).unwrap();
            assert_eq!(body["error"]["code"], "unsupported_feature");
            assert!(
                body["error"]["hint"].is_string(),
                "{query} should suggest an alternative"
            );
        }
    }

    #[test]
    fn unsupported_pipeline_stages_name_themselves() {
        let cases = [
            (r#"{app="x"} | line_format "{{.msg}}""#, "line_format"),
            (r#"{app="x"} | label_format foo="bar""#, "label_format"),
            (r#"{app="x"} | unwrap duration"#, "unwrap"),
            (r#"{app="x"} | pattern "<_> foo""#, "pattern"),
            (r#"{app="x"} | regexp "(?P<a>.*)""#, "regexp"),
            (r#"{app="x"} | drop foo"#, "drop"),
            (r#"{app="x"} | keep foo"#, "keep"),
            (r#"{app="x"} | decolorize"#, "decolorize"),
        ];
        for (query, feature) in cases {
            let err = parse(query).unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{query}");
            assert!(
                err.to_string().contains(feature),
                "{query} should name {feature}, said: {err}"
            );
        }
    }

    #[test]
    fn numeric_label_filters_are_named() {
        let err = parse(r#"{app="x"} | status > 400"#).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }));
        assert!(err.to_string().contains("numeric label filters"), "{err}");
    }

    #[test]
    fn every_unsupported_error_links_the_compatibility_doc() {
        let err = parse(r#"{app="x"} | unwrap duration"#).unwrap_err();
        let body = serde_json::to_value(err.to_body()).unwrap();
        assert!(
            body["error"]["docs"]
                .as_str()
                .unwrap()
                .ends_with("COMPATIBILITY.md"),
            "{body}"
        );
    }

    // -- robustness --------------------------------------------------------

    #[test]
    fn an_empty_query_is_refused() {
        assert!(parse("").is_err());
        assert!(parse("   ").is_err());
    }

    #[test]
    fn has_filters_reports_whether_per_line_work_is_needed() {
        assert!(!parse(r#"{app="x"}"#).unwrap().has_filters());
        assert!(!parse(r#"{app="x"} | json"#).unwrap().has_filters());
        assert!(parse(r#"{app="x"} |= "a""#).unwrap().has_filters());
        assert!(parse(r#"{app="x"} | json | a="b""#).unwrap().has_filters());
    }

    #[test]
    fn logfmt_flattening_sanitises_field_names() {
        let query = parse(r#"{app="x"} | logfmt | http_status="200""#).unwrap();
        assert!(query.evaluate("http.status=200", &labels(&[])));
    }

    #[test]
    fn json_flattening_sanitises_and_joins_nested_names() {
        let query = parse(r#"{app="x"} | json | http_status_code="500""#).unwrap();
        assert!(query.evaluate(r#"{"http":{"status.code":500}}"#, &labels(&[])));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod compatibility_tests {
    //! Cases taken from what `cboxdk/laravel-telemetry-ui`'s `LogqlCompiler` actually
    //! emits. These are the contract, so they are pinned separately from the tests
    //! that cover the language in general.

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn the_uis_default_selector_parses() {
        // LogqlCompiler falls back to this when no stream matcher is given.
        let query = parse(r#"{service_name=~".+"}"#).unwrap();
        assert_eq!(query.matchers.len(), 1);
        assert!(
            query.matchers[0].is_selective(),
            "`.+` requires a value, so it must not be treated as match-everything"
        );
    }

    #[test]
    fn label_filters_combine_with_and() {
        let query = parse(r#"{app="x"} | status="500" and method="GET""#).unwrap();

        assert!(query.evaluate("l", &labels(&[("status", "500"), ("method", "GET")])));
        assert!(!query.evaluate("l", &labels(&[("status", "500"), ("method", "POST")])));
        assert!(!query.evaluate("l", &labels(&[("status", "200"), ("method", "GET")])));
    }

    #[test]
    fn label_filters_combine_with_or() {
        let query = parse(r#"{app="x"} | status="500" or status="503""#).unwrap();

        assert!(query.evaluate("l", &labels(&[("status", "500")])));
        assert!(query.evaluate("l", &labels(&[("status", "503")])));
        assert!(!query.evaluate("l", &labels(&[("status", "200")])));
    }

    #[test]
    fn and_binds_tighter_than_or() {
        // `a or b and c` is `a or (b and c)`, as in LogQL.
        let query = parse(r#"{app="x"} | a="1" or b="2" and c="3""#).unwrap();

        assert!(query.evaluate("l", &labels(&[("a", "1")])));
        assert!(query.evaluate("l", &labels(&[("b", "2"), ("c", "3")])));
        assert!(
            !query.evaluate("l", &labels(&[("b", "2")])),
            "b alone must not satisfy `b and c`"
        );
    }

    #[test]
    fn a_long_or_chain_parses() {
        let query = parse(r#"{app="x"} | s="1" or s="2" or s="3" or s="4""#).unwrap();
        for value in ["1", "2", "3", "4"] {
            assert!(query.evaluate("l", &labels(&[("s", value)])), "{value}");
        }
        assert!(!query.evaluate("l", &labels(&[("s", "5")])));
    }

    #[test]
    fn mixed_operators_in_a_filter_chain_work() {
        let query = parse(r#"{app="x"} | route=~"/api/.*" and status!="200""#).unwrap();
        assert!(query.evaluate("l", &labels(&[("route", "/api/orders"), ("status", "500")])));
        assert!(!query.evaluate("l", &labels(&[("route", "/api/orders"), ("status", "200")])));
    }
}
