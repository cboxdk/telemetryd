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
use crate::logstage::{Pattern, Template};

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
    /// A regular expression's named groups become labels.
    Regexp(Regex),
    /// A `pattern` parser's captures become labels.
    Pattern(Pattern),
    /// ANSI colour escapes are taken out of the line.
    Decolorize,
    /// The line is replaced by a template's rendering.
    LineFormat(Template),
    /// Labels are renamed or set from templates.
    LabelFormat(Vec<(String, LabelSource)>),
    /// Labels are removed — all of a name, or where a matcher holds.
    Drop(Vec<Selection>),
    /// Only these labels are kept.
    Keep(Vec<Selection>),
    Label(LabelPredicate),
}

/// Where a `label_format` label's value comes from.
#[derive(Debug, Clone)]
pub enum LabelSource {
    /// `new=old`: the old label's value, which the old label gives up.
    Rename(String),
    /// `new="{{.a}}-{{.b}}"`.
    Template(Template),
}

/// A `drop`/`keep` entry: a label name, or a name with a matcher on its value.
#[derive(Debug, Clone)]
pub struct Selection {
    pub name: String,
    pub matcher: Option<LabelMatcher>,
}

impl Selection {
    fn selects(&self, labels: &Labels) -> bool {
        self.matcher
            .as_ref()
            .map_or(labels.get(&self.name).is_some(), |m| m.matches(labels))
    }
}

/// What a line has become after the pipeline: its text, the labels it carries, and
/// which of those the pipeline itself made.
#[derive(Debug, Clone)]
pub struct Processed {
    pub line: String,
    pub labels: Labels,
    pub extracted: Labels,
}

/// A label filter stage: one or more matchers combined with `and` / `or`.
///
/// LogQL allows `| status="500" or status="503"` in a single stage, and
/// `laravel-telemetry-ui` generates exactly that. Supporting only a bare matcher would
/// turn an ordinary UI query into a syntax error.
#[derive(Debug, Clone)]
pub enum LabelPredicate {
    Match(LabelMatcher),
    /// `status >= 500`, `duration > 250ms`, `size < 1MB`: the label read as a number,
    /// a duration or a byte size.
    Compare(Comparison),
    And(Box<LabelPredicate>, Box<LabelPredicate>),
    Or(Box<LabelPredicate>, Box<LabelPredicate>),
}

impl LabelPredicate {
    /// Whether the labels pass, for a predicate of string matchers only — the fast
    /// path, which may not write `__error__`. See `apply` for the general case.
    pub fn matches(&self, labels: &Labels) -> bool {
        match self {
            Self::Match(matcher) => matcher.matches(labels),
            Self::Compare(_) => {
                let mut labels = labels.clone();
                self.apply(&mut labels)
            }
            Self::And(left, right) => left.matches(labels) && right.matches(labels),
            Self::Or(left, right) => left.matches(labels) || right.matches(labels),
        }
    }

    /// Whether the labels pass, recording a value that is no number in `__error__`.
    ///
    /// As in Loki: a missing label fails a comparison; a value that does not read as
    /// the literal's kind keeps the line and marks it `LabelFilterErr`; and a line that
    /// already carries an error passes every comparison, so only `__error__` filters
    /// decide its fate. `or` does not try its right side once the left has passed.
    pub fn apply(&self, labels: &mut Labels) -> bool {
        match self {
            Self::Match(matcher) => matcher.matches(labels),
            Self::Compare(comparison) => comparison.apply(labels),
            Self::And(left, right) => left.apply(labels) && right.apply(labels),
            Self::Or(left, right) => left.apply(labels) || right.apply(labels),
        }
    }

    fn compares(&self) -> bool {
        match self {
            Self::Match(_) => false,
            Self::Compare(_) => true,
            Self::And(left, right) | Self::Or(left, right) => left.compares() || right.compares(),
        }
    }
}

/// A numeric label filter: which label, how it compares, and to what.
#[derive(Debug, Clone)]
pub struct Comparison {
    pub name: String,
    pub op: CompareOp,
    pub threshold: Threshold,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

/// The literal a label is compared with. Its kind decides how the label is read:
/// `500` as a number, `250ms` as a Go duration, `20MB` as a byte size.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Threshold {
    Number(f64),
    /// Seconds.
    Duration(f64),
    Bytes(f64),
}

impl Comparison {
    fn apply(&self, labels: &mut Labels) -> bool {
        if labels
            .get("__error__")
            .is_some_and(|error| !error.is_empty())
        {
            return true;
        }
        let Some(value) = labels.get(&self.name) else {
            return false;
        };
        let (read, threshold, kind) = match self.threshold {
            Threshold::Number(n) => (value.trim().parse::<f64>().ok(), n, "a number"),
            Threshold::Duration(d) => (crate::logstage::parse_duration(value), d, "a duration"),
            Threshold::Bytes(b) => (crate::logstage::parse_bytes(value), b, "a byte size"),
        };
        let Some(read) = read else {
            let details = format!("{value:?} is not {kind}");
            labels.insert("__error__", "LabelFilterErr");
            labels.insert("__error_details__", details);
            return true;
        };
        #[allow(clippy::float_cmp)]
        match self.op {
            CompareOp::Equal => read == threshold,
            CompareOp::NotEqual => read != threshold,
            CompareOp::Greater => read > threshold,
            CompareOp::GreaterEqual => read >= threshold,
            CompareOp::Less => read < threshold,
            CompareOp::LessEqual => read <= threshold,
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
        self.raw_line_filters()
            .find(|filter| filter.op == LineOp::Contains)
            .map(|filter| filter.pattern.as_str())
    }

    /// The line filters that see the line as it was stored: those before any stage that
    /// rewrites it. Only these may be checked against storage — one after `line_format`
    /// matches the rewritten line, and pushed down it would drop lines that pass.
    pub fn raw_line_filters(&self) -> impl Iterator<Item = &LineFilter> {
        self.stages
            .iter()
            .take_while(|stage| !matches!(stage, Stage::LineFormat(_) | Stage::Decolorize))
            .filter_map(|stage| match stage {
                Stage::Line(filter) => Some(filter),
                _ => None,
            })
    }
}

impl LineFilter {
    fn new(op: LineOp, pattern: String) -> Result<Self> {
        let regex = match op {
            LineOp::Matches | LineOp::NotMatches => Some(
                telemetryd_core::matcher::compile_regex(&pattern).map_err(|e| {
                    Error::BadRequest(format!("invalid regular expression {pattern:?}: {e}"))
                })?,
            ),
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

pub(crate) struct Parser<'a> {
    input: &'a str,
    pub(crate) tokens: Vec<Spanned>,
    pub(crate) pos: usize,
}

impl<'a> Parser<'a> {
    pub(crate) fn new(input: &'a str, tokens: Vec<Spanned>) -> Self {
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

    /// A metric query where a log query belongs — a tail, a label lookup — is refused
    /// by name: `query` and `query_range` answer it, this route cannot.
    fn reject_metric_query(&self) -> Result<()> {
        let token = |i: usize| self.tokens.get(i).map(|s| &s.token);
        let call = matches!(token(0), Some(Token::Ident(_)))
            && matches!(token(1), Some(Token::LeftParen))
            || matches!(token(1), Some(Token::Ident(w)) if w == "by" || w == "without");
        if !call {
            return Ok(());
        }
        Err(Error::BadRequest(format!(
            "{:?} is a metric query; /loki/api/v1/query and /loki/api/v1/query_range \
             answer it, but this route takes a log selector like {{app=\"x\"}}",
            self.input
        )))
    }

    pub(crate) fn parse_selector(&mut self) -> Result<Vec<LabelMatcher>> {
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

    pub(crate) fn parse_stage(&mut self) -> Result<Stage> {
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

            "regexp" => {
                let pattern = self.expect_string("a quoted regular expression after `regexp`")?;
                let regex = telemetryd_core::matcher::compile_regex(&pattern).map_err(|e| {
                    Error::BadRequest(format!("invalid regular expression {pattern:?}: {e}"))
                })?;
                if regex.capture_names().flatten().next().is_none() {
                    return Err(Error::BadRequest(format!(
                        "the regexp {pattern:?} names no group, like (?P<status>\\d+)"
                    )));
                }
                Ok(Stage::Regexp(regex))
            }
            "pattern" => {
                let pattern = self.expect_string("a quoted pattern after `pattern`")?;
                Ok(Stage::Pattern(Pattern::parse(&pattern)?))
            }
            "decolorize" => Ok(Stage::Decolorize),
            "line_format" => {
                let template = self.expect_string("a quoted template after `line_format`")?;
                Ok(Stage::LineFormat(Template::parse(&template)?))
            }
            "label_format" => Ok(Stage::LabelFormat(self.label_format_items()?)),
            "drop" => Ok(Stage::Drop(self.selections()?)),
            "keep" => Ok(Stage::Keep(self.selections()?)),
            "unwrap" => Err(Error::BadRequest(
                "`| unwrap` belongs inside a metric query, like \
                 sum_over_time({app=\"x\"} | unwrap duration [5m])"
                    .to_owned(),
            )),
            "distinct" | "ip" => Err(Error::unsupported(format!("LogQL `| {name}`"))),

            // Anything else in this position is a label filter.
            _ => {
                let first = self.label_matcher(name)?;
                Ok(Stage::Label(self.label_predicate_tail(first)?))
            }
        }
    }

    /// `new=old, other="{{.a}}"` after `label_format`.
    fn label_format_items(&mut self) -> Result<Vec<(String, LabelSource)>> {
        let mut items = Vec::new();
        loop {
            let name = self.expect_ident("a label name in `label_format`")?;
            self.expect(&Token::Equal, "`=`")?;
            let source = match self.peek().cloned() {
                Some(Token::String(template)) => {
                    self.pos += 1;
                    LabelSource::Template(Template::parse(&template)?)
                }
                Some(Token::Ident(old)) => {
                    self.pos += 1;
                    LabelSource::Rename(old)
                }
                _ => return Err(self.unexpected("a label name or a quoted template")),
            };
            items.push((name, source));
            if self.peek() == Some(&Token::Comma) {
                self.pos += 1;
            } else {
                return Ok(items);
            }
        }
    }

    /// `a, b, c="x"` after `drop` or `keep`.
    fn selections(&mut self) -> Result<Vec<Selection>> {
        let mut selections = Vec::new();
        loop {
            let name = self.expect_ident("a label name")?;
            let matcher = if matches!(
                self.peek(),
                Some(
                    Token::Equal
                        | Token::EqualEqual
                        | Token::NotEqual
                        | Token::RegexMatch
                        | Token::RegexNotMatch
                )
            ) {
                let op = self.expect_match_op()?;
                let value = self.expect_string("a quoted value")?;
                Some(LabelMatcher::new(name.clone(), op, value)?)
            } else {
                None
            };
            selections.push(Selection { name, matcher });
            if self.peek() == Some(&Token::Comma) {
                self.pos += 1;
            } else {
                return Ok(selections);
            }
        }
    }

    /// Parse one `name op "value"` matcher or `name op 500` comparison, given the
    /// already-consumed name.
    fn label_matcher(&mut self, name: String) -> Result<LabelPredicate> {
        let op = match self.peek() {
            Some(Token::Greater) => Some(CompareOp::Greater),
            Some(Token::GreaterEqual) => Some(CompareOp::GreaterEqual),
            Some(Token::Less) => Some(CompareOp::Less),
            Some(Token::LessEqual) => Some(CompareOp::LessEqual),
            Some(Token::Equal | Token::EqualEqual) if self.literal_follows() => {
                Some(CompareOp::Equal)
            }
            Some(Token::NotEqual) if self.literal_follows() => Some(CompareOp::NotEqual),
            _ => None,
        };
        if let Some(op) = op {
            self.pos += 1;
            let threshold = self.threshold()?;
            return Ok(LabelPredicate::Compare(Comparison {
                name,
                op,
                threshold,
            }));
        }
        let op = self.expect_match_op()?;
        let value = self.expect_string("a quoted value in a label filter")?;
        Ok(LabelPredicate::Match(LabelMatcher::new(name, op, value)?))
    }

    /// Whether the token after the operator is an unquoted literal, which makes `=` a
    /// numeric comparison rather than a string match.
    fn literal_follows(&self) -> bool {
        let next = |i: usize| self.tokens.get(self.pos + i).map(|s| &s.token);
        match next(1) {
            Some(Token::Number(_) | Token::Duration(_) | Token::Bytes(_)) => true,
            Some(Token::Minus) => matches!(next(2), Some(Token::Number(_))),
            _ => false,
        }
    }

    /// The literal a comparison is made against: a number, a duration or a byte size.
    fn threshold(&mut self) -> Result<Threshold> {
        let negative = self.peek() == Some(&Token::Minus);
        if negative {
            self.pos += 1;
        }
        let threshold = match self.peek() {
            Some(Token::Number(n)) => Threshold::Number(if negative { -n } else { *n }),
            #[allow(clippy::cast_precision_loss)]
            Some(Token::Duration(nanos)) if !negative => Threshold::Duration(*nanos as f64 / 1e9),
            #[allow(clippy::cast_precision_loss)]
            Some(Token::Bytes(bytes)) if !negative => Threshold::Bytes(*bytes as f64),
            _ => {
                return Err(self.unexpected(
                    "a number, a duration like `250ms` or a size like `20MB` to compare with",
                ));
            }
        };
        self.pos += 1;
        Ok(threshold)
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

    pub(crate) fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos).map(|s| &s.token)
    }

    pub(crate) fn expect(&mut self, expected: &Token, description: &str) -> Result<()> {
        if self.peek() == Some(expected) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.unexpected(description))
        }
    }

    pub(crate) fn expect_ident(&mut self, description: &str) -> Result<String> {
        match self.peek() {
            Some(Token::Ident(name)) => {
                let name = name.clone();
                self.pos += 1;
                Ok(name)
            }
            _ => Err(self.unexpected(description)),
        }
    }

    pub(crate) fn expect_string(&mut self, description: &str) -> Result<String> {
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
            Some(Token::Equal | Token::EqualEqual) => MatchOp::Equal,
            Some(Token::NotEqual) => MatchOp::NotEqual,
            Some(Token::RegexMatch) => MatchOp::Regex,
            Some(Token::RegexNotMatch) => MatchOp::NotRegex,
            _ => return Err(self.unexpected("a matcher operator (`=`, `!=`, `=~`, `!~`)")),
        };
        self.pos += 1;
        Ok(op)
    }

    pub(crate) fn unexpected(&self, expected: &str) -> Error {
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
    /// Whether this query parses or rewrites the line at all, so a caller can skip the
    /// second pass that builds what is shown.
    pub fn has_parser_stage(&self) -> bool {
        self.stages.iter().any(|stage| match stage {
            Stage::Line(_) => false,
            // A comparison may write `__error__`, which later stages and the response see.
            Stage::Label(predicate) => predicate.compares(),
            _ => true,
        })
    }

    /// Whether a line passes every stage. `base` is what a label filter sees before any
    /// parser runs — the stream labels and the record's attributes; `stream` is the
    /// stream labels alone, which decide whether a parsed field is renamed.
    pub fn evaluate(&self, line: &str, base: &Labels, stream: &Labels) -> bool {
        // Only filters: nothing to build, and nothing to clone. This is almost every
        // query, and it runs once per scanned line.
        if !self.has_parser_stage() {
            return self.stages.iter().all(|stage| match stage {
                Stage::Line(filter) => filter.matches(line),
                Stage::Label(predicate) => predicate.matches(base),
                _ => true,
            });
        }
        self.process(line, base, stream).is_some()
    }

    /// Run the pipeline over a line: `None` if a filter drops it, otherwise the line as
    /// the stages left it and the labels it carries.
    pub fn process(&self, line: &str, base: &Labels, stream: &Labels) -> Option<Processed> {
        let mut out = Processed {
            line: line.to_owned(),
            labels: base.clone(),
            extracted: Labels::new(),
        };
        for stage in &self.stages {
            match stage {
                Stage::Line(filter) => {
                    if !filter.matches(&out.line) {
                        return None;
                    }
                }
                Stage::Label(predicate) => {
                    if !predicate.apply(&mut out.labels) {
                        return None;
                    }
                    if let Some(error) = out.labels.get("__error__") {
                        let error = error.to_owned();
                        out.extracted.insert("__error__", error);
                        if let Some(details) = out.labels.get("__error_details__") {
                            let details = details.to_owned();
                            out.extracted.insert("__error_details__", details);
                        }
                    }
                }
                Stage::Json | Stage::Logfmt | Stage::Regexp(_) | Stage::Pattern(_) => {
                    let parsed = stage.parse(&out.line);
                    absorb(&mut out.labels, &parsed, stream);
                    absorb(&mut out.extracted, &parsed, stream);
                }
                Stage::Decolorize => out.line = crate::logstage::decolorize(&out.line),
                Stage::LineFormat(template) => {
                    out.line = template.render(&out.labels, &out.line);
                }
                Stage::LabelFormat(items) => {
                    for (name, source) in items {
                        let value = match source {
                            LabelSource::Rename(old) => {
                                let value = out.labels.get(old).unwrap_or("").to_owned();
                                out.labels.remove(old);
                                out.extracted.remove(old);
                                value
                            }
                            LabelSource::Template(template) => {
                                template.render(&out.labels, &out.line)
                            }
                        };
                        out.labels.insert(name.clone(), value.clone());
                        out.extracted.insert(name.clone(), value);
                    }
                }
                Stage::Drop(selections) => {
                    for selection in selections {
                        if selection.selects(&out.labels) {
                            out.labels.remove(&selection.name);
                            out.extracted.remove(&selection.name);
                        }
                    }
                }
                Stage::Keep(selections) => {
                    let kept: Vec<String> = out
                        .labels
                        .names()
                        .filter(|name| {
                            selections
                                .iter()
                                .any(|s| s.name == *name && s.selects(&out.labels))
                        })
                        .map(str::to_owned)
                        .collect();
                    out.labels = out
                        .labels
                        .iter()
                        .filter(|(name, _)| kept.iter().any(|k| k == name))
                        .map(|(k, v)| (k.to_owned(), v.to_owned()))
                        .collect();
                    out.extracted = out
                        .extracted
                        .iter()
                        .filter(|(name, _)| kept.iter().any(|k| k == name))
                        .map(|(k, v)| (k.to_owned(), v.to_owned()))
                        .collect();
                }
            }
        }
        Some(out)
    }

    /// Whether any stage can reject a line. A selector-only query skips per-line work.
    pub fn has_filters(&self) -> bool {
        self.stages
            .iter()
            .any(|s| matches!(s, Stage::Line(_) | Stage::Label(_)))
    }
}

impl Stage {
    /// The fields a parser stage pulls out of a line. Empty for the other stages.
    fn parse(&self, line: &str) -> Labels {
        let mut labels = Labels::new();
        match self {
            Self::Json => merge_json(&mut labels, line),
            Self::Logfmt => merge_logfmt(&mut labels, line),
            Self::Regexp(regex) => {
                if let Some(captures) = regex.captures(line) {
                    for name in regex.capture_names().flatten() {
                        if let Some(value) = captures.name(name) {
                            labels.insert(
                                telemetryd_core::record::sanitize_label_name(name),
                                value.as_str(),
                            );
                        }
                    }
                }
            }
            Self::Pattern(pattern) => {
                for (name, value) in pattern.captures(line) {
                    labels.insert(name, value);
                }
            }
            _ => {}
        }
        labels
    }
}

/// Fold parsed fields into a label set the way Loki does.
///
/// A field whose name a stream label already has is kept as `<name>_extracted`, and the
/// stream label stands. It used to overwrite it for filtering while the response said
/// `_extracted` — so `| json | app="x"` selected on the line's `app` and returned records
/// whose shown `app` was something else. Any other field — including one an attribute
/// already has — takes the name: a parsed label outranks structured metadata in Loki.
fn absorb(labels: &mut Labels, parsed: &Labels, stream: &Labels) {
    for (name, value) in parsed.iter() {
        if stream.get(name).is_some() {
            labels.insert(format!("{name}_extracted"), value);
        } else {
            labels.insert(name, value);
        }
    }
}

/// Merge a JSON object's fields into the label set, flattening nested paths with `_`
/// as Loki does.
///
/// A line that is not a JSON object contributes no fields and `__error__` =
/// `JSONParserErr`, as in Loki. It is not a query failure — log streams are rarely
/// homogeneous — but the label is how `| json | __error__=""` drops such lines, the
/// filter Grafana's query builder adds, and how `| __error__!=""` finds them. It was
/// never set, so the one kept plain-text lines and the other found nothing.
fn merge_json(labels: &mut Labels, line: &str) {
    match serde_json::from_str::<serde_json::Value>(line) {
        Ok(value @ serde_json::Value::Object(_)) => flatten_json(labels, "", &value),
        Ok(_) => {
            labels.insert("__error__", "JSONParserErr");
            labels.insert("__error_details__", "the line is not a JSON object");
        }
        Err(error) => {
            labels.insert("__error__", "JSONParserErr");
            labels.insert("__error_details__", error.to_string());
        }
    }
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
        assert!(query.evaluate("payment declined for order 9912", &base, &base));
        assert!(
            !query.evaluate("payment declined test", &base, &base),
            "!= should reject"
        );
        assert!(
            !query.evaluate("order created", &base, &base),
            "|= should reject"
        );
        assert!(
            !query.evaluate("payment declined retry", &base, &base),
            "!~ should reject"
        );
    }

    #[test]
    fn line_filter_regexes_are_not_anchored() {
        // Unlike label matchers: |~ "err" means the line *contains* a match.
        let query = parse(r#"{app="x"} |~ "err""#).unwrap();
        assert!(query.evaluate("an error occurred", &labels(&[]), &labels(&[])));
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

        assert!(query.evaluate(r#"{"user":{"id":"42"},"msg":"hi"}"#, &base, &base));
        assert!(!query.evaluate(r#"{"user":{"id":"43"}}"#, &base, &base));
    }

    #[test]
    fn json_numbers_and_booleans_become_label_values() {
        let query = parse(r#"{app="x"} | json | status="200""#).unwrap();
        assert!(query.evaluate(r#"{"status":200}"#, &labels(&[]), &labels(&[])));

        let flag = parse(r#"{app="x"} | json | ok="true""#).unwrap();
        assert!(flag.evaluate(r#"{"ok":true}"#, &labels(&[]), &labels(&[])));
    }

    #[test]
    fn a_non_json_line_contributes_nothing_rather_than_failing() {
        // Log streams are rarely homogeneous; one plain-text line must not break the
        // query for the other million.
        let query = parse(r#"{app="x"} | json | level="error""#).unwrap();
        assert!(!query.evaluate("this is not json", &labels(&[]), &labels(&[])));
        // …and a line that does parse still matches.
        assert!(query.evaluate(r#"{"level":"error"}"#, &labels(&[]), &labels(&[])));
    }

    #[test]
    fn logfmt_parser_handles_quoted_and_bare_values() {
        let query = parse(r#"{app="x"} | logfmt | method="GET""#).unwrap();
        assert!(query.evaluate(
            "method=GET path=/api status=200",
            &labels(&[]),
            &labels(&[])
        ));

        let quoted = parse(r#"{app="x"} | logfmt | msg="hello world""#).unwrap();
        assert!(quoted.evaluate(
            r#"level=info msg="hello world""#,
            &labels(&[]),
            &labels(&[])
        ));
    }

    #[test]
    fn label_filters_see_record_attributes_without_a_parser_stage() {
        // telemetryd extension: OTLP records are already structured, so requiring a
        // `| json` to reach their attributes would be theatre.
        let query = parse(r#"{app="x"} | order_id="9912""#).unwrap();
        let base = labels(&[("app", "x"), ("order_id", "9912")]);
        assert!(query.evaluate("anything", &base, &base));

        let miss = parse(r#"{app="x"} | order_id="1""#).unwrap();
        assert!(!miss.evaluate("anything", &base, &base));
    }

    #[test]
    fn label_filters_accept_regex_operators() {
        let query = parse(r#"{app="x"} | route=~"/api/.*""#).unwrap();
        assert!(query.evaluate("x", &labels(&[("route", "/api/orders")]), &Labels::new()));
        assert!(!query.evaluate("x", &labels(&[("route", "/health")]), &Labels::new()));
    }

    // -- the subset boundary ----------------------------------------------

    #[test]
    fn a_metric_query_is_sent_to_the_routes_that_answer_it() {
        for query in [
            r#"rate({app="x"}[5m])"#,
            r#"count_over_time({app="x"}[1h])"#,
            r#"sum by (app) (rate({app="x"}[5m]))"#,
        ] {
            let err = parse(query).unwrap_err();
            assert!(matches!(err, Error::BadRequest(_)), "{query}: {err:?}");
            assert!(err.to_string().contains("query_range"), "{err}");
        }
    }

    fn run(query: &str, line: &str) -> Option<Processed> {
        let stream = labels(&[("app", "x"), ("level", "info")]);
        parse(query).unwrap().process(line, &stream, &stream)
    }

    #[test]
    fn parsers_and_formats_transform_the_line_and_its_labels() {
        let out = run(
            r#"{app="x"} | regexp "(?P<method>[A-Z]+) (?P<path>\S+)" | line_format "{{.method}} → {{.path | ToUpper}}""#,
            "GET /checkout 200",
        )
        .unwrap();
        assert_eq!(out.line, "GET → /CHECKOUT");
        assert_eq!(out.labels.get("path"), Some("/checkout"));

        let out = run(
            r#"{app="x"} | pattern "<_> <status> <_>" | label_format code=status, where="{{.app}}""#,
            "GET 503 12ms",
        )
        .unwrap();
        assert_eq!(out.labels.get("code"), Some("503"));
        assert!(out.labels.get("status").is_none(), "renamed away");
        assert_eq!(out.labels.get("where"), Some("x"));

        let out = run(
            r#"{app="x"} | logfmt | drop user, level="info""#,
            "user=42 route=/a",
        )
        .unwrap();
        assert!(out.labels.get("user").is_none());
        assert!(out.labels.get("level").is_none());
        assert_eq!(out.labels.get("route"), Some("/a"));

        let out = run(r#"{app="x"} | logfmt | keep route"#, "user=42 route=/a").unwrap();
        assert_eq!(out.labels.iter().count(), 1);

        // A filter after `line_format` sees the rewritten line.
        assert!(run(r#"{app="x"} | line_format "hidden" |= "GET""#, "GET /").is_none());
        let query = parse(r#"{app="x"} |= "GET" | line_format "x" |= "x""#).unwrap();
        assert_eq!(
            query.raw_line_filters().count(),
            1,
            "only the first reaches storage"
        );
        assert_eq!(
            run(r#"{app="x"} | decolorize"#, "\u{1b}[31mred\u{1b}[0m")
                .unwrap()
                .line,
            "red"
        );
    }

    #[test]
    fn unsupported_pipeline_stages_name_themselves() {
        let cases = [
            (r#"{app="x"} | distinct foo"#, "distinct"),
            (r#"{app="x"} | ip("10.0.0.0/8")"#, "ip"),
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

    fn passes(query: &str, line: &str) -> Option<Labels> {
        let q = parse(query).unwrap();
        let stream: Labels = [("app", "x")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        q.process(line, &stream, &stream).map(|p| p.labels)
    }

    #[test]
    fn numeric_label_filters_compare_as_numbers() {
        let q = r#"{app="x"} | logfmt | status >= 500"#;
        assert!(passes(q, "status=503").is_some());
        assert!(passes(q, "status=500").is_some());
        assert!(passes(q, "status=404").is_none());
        // As a string, "1000" < "500"; as a number it is not.
        assert!(passes(q, "status=1000").is_some());
        assert!(passes(r#"{app="x"} | logfmt | status == 200"#, "status=200").is_some());
        assert!(passes(r#"{app="x"} | logfmt | status = 200"#, "status=200.0").is_some());
        assert!(passes(r#"{app="x"} | logfmt | status != 200"#, "status=200").is_none());
        assert!(passes(r#"{app="x"} | logfmt | delta > -1"#, "delta=0").is_some());
        // `=` against a quoted value is still a string match.
        assert!(passes(r#"{app="x"} | logfmt | status = "200""#, "status=200.0").is_none());
    }

    #[test]
    fn a_missing_label_fails_a_comparison() {
        assert!(passes(r#"{app="x"} | logfmt | status > 1"#, "other=5").is_none());
    }

    #[test]
    fn a_value_that_is_no_number_is_kept_and_marked() {
        let labels = passes(r#"{app="x"} | logfmt | status > 400"#, "status=oops").unwrap();
        assert_eq!(labels.get("__error__"), Some("LabelFilterErr"));
        assert!(
            passes(
                r#"{app="x"} | logfmt | status > 400 | __error__="""#,
                "status=oops"
            )
            .is_none()
        );
        // An earlier error passes every comparison: only `__error__` filters decide.
        let labels = passes(r#"{app="x"} | json | status > 400"#, "not json").unwrap();
        assert_eq!(labels.get("__error__"), Some("JSONParserErr"));
    }

    #[test]
    fn durations_and_sizes_compare_in_their_units() {
        let q = r#"{app="x"} | logfmt | took > 250ms"#;
        assert!(passes(q, "took=1.5s").is_some());
        assert!(passes(q, "took=300ms").is_some());
        assert!(passes(q, "took=250ms").is_none());
        assert!(passes(q, "took=1m").is_some());
        assert!(passes(q, "took=90us").is_none());
        let q = r#"{app="x"} | logfmt | size >= 20MB"#;
        assert!(passes(q, "size=20000000").is_some());
        assert!(passes(q, "size=19.9MB").is_none());
        assert!(passes(q, "size=1GiB").is_some());
        assert!(passes(q, r#"size="25 mb""#).is_some());
    }

    #[test]
    fn comparisons_combine_with_matchers() {
        let q = r#"{app="x"} | logfmt | status >= 500 or level="error" and took > 1s"#;
        assert!(passes(q, "status=502 level=info took=0s").is_some());
        assert!(passes(q, "status=200 level=error took=2s").is_some());
        assert!(passes(q, "status=200 level=error took=0.5s").is_none());
        // Without a parser, a comparison reads the stream labels and attributes.
        let q = parse(r#"{app="x"} | code > 400"#).unwrap();
        let base: Labels = [("app", "x"), ("code", "404")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        assert!(q.evaluate("line", &base, &base));
        let base: Labels = [("app", "x"), ("code", "200")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        assert!(!q.evaluate("line", &base, &base));
    }

    #[test]
    fn a_comparison_needs_a_literal() {
        let err = parse(r#"{app="x"} | status > "400""#).unwrap_err();
        assert!(err.to_string().contains("a number, a duration"), "{err}");
    }

    #[test]
    fn every_unsupported_error_links_the_compatibility_doc() {
        let err = parse(r#"{app="x"} | distinct foo"#).unwrap_err();
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
        assert!(query.evaluate("http.status=200", &labels(&[]), &labels(&[])));
    }

    #[test]
    fn json_flattening_sanitises_and_joins_nested_names() {
        let query = parse(r#"{app="x"} | json | http_status_code="500""#).unwrap();
        assert!(query.evaluate(
            r#"{"http":{"status.code":500}}"#,
            &labels(&[]),
            &labels(&[])
        ));
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

        assert!(query.evaluate(
            "l",
            &labels(&[("status", "500"), ("method", "GET")]),
            &Labels::new()
        ));
        assert!(!query.evaluate(
            "l",
            &labels(&[("status", "500"), ("method", "POST")]),
            &Labels::new()
        ));
        assert!(!query.evaluate(
            "l",
            &labels(&[("status", "200"), ("method", "GET")]),
            &Labels::new()
        ));
    }

    #[test]
    fn label_filters_combine_with_or() {
        let query = parse(r#"{app="x"} | status="500" or status="503""#).unwrap();

        assert!(query.evaluate("l", &labels(&[("status", "500")]), &Labels::new()));
        assert!(query.evaluate("l", &labels(&[("status", "503")]), &Labels::new()));
        assert!(!query.evaluate("l", &labels(&[("status", "200")]), &Labels::new()));
    }

    #[test]
    fn and_binds_tighter_than_or() {
        // `a or b and c` is `a or (b and c)`, as in LogQL.
        let query = parse(r#"{app="x"} | a="1" or b="2" and c="3""#).unwrap();

        assert!(query.evaluate("l", &labels(&[("a", "1")]), &Labels::new()));
        assert!(query.evaluate("l", &labels(&[("b", "2"), ("c", "3")]), &Labels::new()));
        assert!(
            !query.evaluate("l", &labels(&[("b", "2")]), &Labels::new()),
            "b alone must not satisfy `b and c`"
        );
    }

    #[test]
    fn a_long_or_chain_parses() {
        let query = parse(r#"{app="x"} | s="1" or s="2" or s="3" or s="4""#).unwrap();
        for value in ["1", "2", "3", "4"] {
            assert!(
                query.evaluate("l", &labels(&[("s", value)]), &Labels::new()),
                "{value}"
            );
        }
        assert!(!query.evaluate("l", &labels(&[("s", "5")]), &Labels::new()));
    }

    #[test]
    fn mixed_operators_in_a_filter_chain_work() {
        let query = parse(r#"{app="x"} | route=~"/api/.*" and status!="200""#).unwrap();
        assert!(query.evaluate(
            "l",
            &labels(&[("route", "/api/orders"), ("status", "500")]),
            &Labels::new()
        ));
        assert!(!query.evaluate(
            "l",
            &labels(&[("route", "/api/orders"), ("status", "200")]),
            &Labels::new()
        ));
    }
}
