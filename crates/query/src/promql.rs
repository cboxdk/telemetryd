//! The PromQL subset: parsing and lowering.
//!
//! Scoped to what `laravel-telemetry-ui`'s `PromqlCompiler` emits. That list
//! is larger than our first guess, and notably includes the counter-increase form:
//!
//! ```text
//! clamp_min(sel - (sel offset 5m or sel * 0), 0)
//! ```
//!
//! which needs `offset`, vector-to-vector `or`, and `clamp_min` — all three of which
//! the original plan listed as out of scope. The `or sel * 0` idiom exists so the
//! expression yields zero rather than nothing when a series has no older sample, so
//! dropping it does not degrade a chart, it empties it.
//!
//! As elsewhere, the input is parsed in full and then lowered, so a construct outside
//! the subset reports itself by name rather than as a syntax error.

use std::time::Duration;

use telemetryd_core::{Error, LabelMatcher, MatchOp, Result};

use crate::lexer::{Spanned, Token, tokenize};

/// Default lookback when resolving an instant vector, matching Prometheus.
pub const DEFAULT_LOOKBACK: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub enum Expr {
    Number(f64),
    Selector(Selector),
    Call {
        function: Function,
        args: Vec<Expr>,
    },
    Aggregation {
        op: AggregateOp,
        grouping: Grouping,
        inner: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary minus.
    Negate(Box<Expr>),
}

#[derive(Debug, Clone)]
pub struct Selector {
    pub matchers: Vec<LabelMatcher>,
    /// `[5m]` — present makes this a range vector.
    pub range: Option<Duration>,
    /// `offset 5m` — shifts the evaluation time backwards.
    pub offset: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Function {
    Rate,
    Increase,
    HistogramQuantile,
    ClampMin,
    ClampMax,
    Abs,
}

impl Function {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "rate" => Self::Rate,
            "increase" => Self::Increase,
            "histogram_quantile" => Self::HistogramQuantile,
            "clamp_min" => Self::ClampMin,
            "clamp_max" => Self::ClampMax,
            "abs" => Self::Abs,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rate => "rate",
            Self::Increase => "increase",
            Self::HistogramQuantile => "histogram_quantile",
            Self::ClampMin => "clamp_min",
            Self::ClampMax => "clamp_max",
            Self::Abs => "abs",
        }
    }

    /// Whether the function takes a range vector (`sel[5m]`) as its argument.
    pub fn wants_range(self) -> bool {
        matches!(self, Self::Rate | Self::Increase)
    }

    fn arity(self) -> usize {
        match self {
            Self::Rate | Self::Increase | Self::Abs => 1,
            Self::HistogramQuantile | Self::ClampMin | Self::ClampMax => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

impl AggregateOp {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            "count" => Self::Count,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Count => "count",
        }
    }
}

/// `by (…)` / `without (…)`, or neither.
#[derive(Debug, Clone, Default)]
pub enum Grouping {
    #[default]
    All,
    By(Vec<String>),
    Without(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    /// Vector union: the left side wins, the right fills gaps.
    Or,
}

impl BinaryOp {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Pow => "^",
            Self::Or => "or",
        }
    }
}

/// Parse a PromQL expression.
pub fn parse(input: &str) -> Result<Expr> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(Error::BadRequest("empty PromQL query".to_owned()));
    }
    let mut parser = Parser {
        input,
        tokens,
        pos: 0,
    };
    let expr = parser.parse_expr()?;
    if parser.pos < parser.tokens.len() {
        return Err(parser.unexpected("the end of the query"));
    }
    Ok(expr)
}

struct Parser<'a> {
    input: &'a str,
    tokens: Vec<Spanned>,
    pos: usize,
}

impl Parser<'_> {
    /// `or` binds loosest, matching PromQL.
    fn parse_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_additive()?;
        loop {
            match self.peek() {
                Some(Token::Ident(word)) if word == "or" => {
                    self.pos += 1;
                    let right = self.parse_additive()?;
                    left = Expr::Binary {
                        op: BinaryOp::Or,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                }
                // Recognised so they can be named rather than reported as trailing junk.
                Some(Token::Ident(word)) if word == "and" || word == "unless" => {
                    return Err(Error::unsupported_with_hint(
                        format!("PromQL `{word}` between vectors"),
                        "only `or` is supported for vector matching",
                    ));
                }
                _ => return Ok(left),
            }
        }
    }

    fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Some(Token::Plus) => BinaryOp::Add,
                Some(Token::Minus) => BinaryOp::Sub,
                _ => return Ok(left),
            };
            self.pos += 1;
            let right = self.parse_multiplicative()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Some(Token::Star) => BinaryOp::Mul,
                Some(Token::Slash) => BinaryOp::Div,
                Some(Token::Percent) => BinaryOp::Mod,
                Some(Token::Caret) => BinaryOp::Pow,
                _ => return Ok(left),
            };
            self.pos += 1;
            let right = self.parse_unary()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.peek() == Some(&Token::Minus) {
            self.pos += 1;
            return Ok(Expr::Negate(Box::new(self.parse_unary()?)));
        }
        self.parse_atom()
    }

    fn parse_atom(&mut self) -> Result<Expr> {
        match self.peek().cloned() {
            Some(Token::Number(value)) => {
                self.pos += 1;
                Ok(Expr::Number(value))
            }
            // A bare duration in value position is a scalar in some dialects; refuse
            // it clearly rather than silently coercing.
            Some(Token::Duration(_)) => Err(Error::BadRequest(
                "a duration is not a value here; durations belong in `[…]` or after `offset`"
                    .to_owned(),
            )),
            Some(Token::LeftParen) => {
                self.pos += 1;
                let inner = self.parse_expr()?;
                self.expect(&Token::RightParen, "`)`")?;
                Ok(inner)
            }
            Some(Token::LeftBrace) => {
                let matchers = self.parse_matchers()?;
                self.finish_selector(matchers)
            }
            Some(Token::Ident(name)) => self.parse_ident_atom(&name),
            _ => Err(self.unexpected("a metric selector, function call or number")),
        }
    }

    fn parse_ident_atom(&mut self, name: &str) -> Result<Expr> {
        // An identifier followed by `(` is a call or an aggregation; otherwise it is a
        // metric name.
        let is_call = matches!(
            self.tokens.get(self.pos + 1).map(|s| &s.token),
            Some(Token::LeftParen)
        ) || matches!(
            self.tokens.get(self.pos + 1).map(|s| &s.token),
            Some(Token::Ident(word)) if word == "by" || word == "without"
        );

        if !is_call {
            self.pos += 1;
            let mut matchers = vec![LabelMatcher::equal(
                telemetryd_core::METRIC_NAME_LABEL,
                name,
            )];
            if self.peek() == Some(&Token::LeftBrace) {
                matchers.extend(self.parse_matchers()?);
            }
            return self.finish_selector(matchers);
        }

        self.pos += 1;

        if let Some(op) = AggregateOp::from_name(name) {
            return self.parse_aggregation(op);
        }
        if let Some(function) = Function::from_name(name) {
            return self.parse_call(function);
        }

        // Named rather than "unknown token": the caller wrote a real PromQL function
        // that we do not run, and saying which one is the whole point.
        Err(Error::unsupported_with_hint(
            format!("PromQL function `{name}`"),
            "see COMPATIBILITY.md for the supported functions",
        ))
    }

    fn parse_aggregation(&mut self, op: AggregateOp) -> Result<Expr> {
        // Both spellings are legal: `sum by (a) (expr)` and `sum(expr) by (a)`.
        let mut grouping = self.parse_grouping()?;

        self.expect(&Token::LeftParen, "`(` after an aggregation")?;
        let inner = self.parse_expr()?;
        self.expect(&Token::RightParen, "`)`")?;

        if matches!(grouping, Grouping::All) {
            grouping = self.parse_grouping()?;
        }

        Ok(Expr::Aggregation {
            op,
            grouping,
            inner: Box::new(inner),
        })
    }

    fn parse_grouping(&mut self) -> Result<Grouping> {
        let keyword = match self.peek() {
            Some(Token::Ident(word)) if word == "by" || word == "without" => word.clone(),
            _ => return Ok(Grouping::All),
        };
        self.pos += 1;

        self.expect(&Token::LeftParen, "`(` after `by`/`without`")?;
        let mut labels = Vec::new();
        loop {
            match self.peek().cloned() {
                Some(Token::RightParen) => {
                    self.pos += 1;
                    break;
                }
                Some(Token::Comma) => self.pos += 1,
                Some(Token::Ident(label)) => {
                    self.pos += 1;
                    labels.push(label);
                }
                _ => return Err(self.unexpected("a label name or `)`")),
            }
        }

        Ok(if keyword == "by" {
            Grouping::By(labels)
        } else {
            Grouping::Without(labels)
        })
    }

    fn parse_call(&mut self, function: Function) -> Result<Expr> {
        self.expect(&Token::LeftParen, "`(` after a function name")?;

        let mut args = Vec::new();
        if self.peek() == Some(&Token::RightParen) {
            self.pos += 1;
        } else {
            loop {
                args.push(self.parse_expr()?);
                match self.peek() {
                    Some(Token::Comma) => self.pos += 1,
                    Some(Token::RightParen) => {
                        self.pos += 1;
                        break;
                    }
                    _ => return Err(self.unexpected("`,` or `)`")),
                }
            }
        }

        if args.len() != function.arity() {
            return Err(Error::BadRequest(format!(
                "`{}` takes {} argument(s), got {}",
                function.as_str(),
                function.arity(),
                args.len()
            )));
        }

        Ok(Expr::Call { function, args })
    }

    fn parse_matchers(&mut self) -> Result<Vec<LabelMatcher>> {
        self.expect(&Token::LeftBrace, "`{`")?;

        let mut matchers = Vec::new();
        if self.peek() == Some(&Token::RightBrace) {
            self.pos += 1;
            return Ok(matchers);
        }

        loop {
            let name = self.expect_ident("a label name")?;
            let op = match self.peek() {
                Some(Token::Equal) => MatchOp::Equal,
                Some(Token::NotEqual) => MatchOp::NotEqual,
                Some(Token::RegexMatch) => MatchOp::Regex,
                Some(Token::RegexNotMatch) => MatchOp::NotRegex,
                _ => return Err(self.unexpected("a matcher operator")),
            };
            self.pos += 1;
            let value = self.expect_string("a quoted label value")?;
            matchers.push(LabelMatcher::new(name, op, value)?);

            match self.peek() {
                Some(Token::Comma) => {
                    self.pos += 1;
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
        Ok(matchers)
    }

    /// Attach `[range]`, `offset` and reject the modifiers we do not run.
    fn finish_selector(&mut self, matchers: Vec<LabelMatcher>) -> Result<Expr> {
        let mut range = None;
        if self.peek() == Some(&Token::LeftBracket) {
            self.pos += 1;
            let Some(Token::Duration(nanos)) = self.peek().cloned() else {
                return Err(self.unexpected("a duration inside `[…]`"));
            };
            self.pos += 1;

            // `[5m:1m]` is a subquery — real PromQL, and out of the subset.
            if self.peek() == Some(&Token::Colon) {
                return Err(Error::unsupported_with_hint(
                    "PromQL subqueries",
                    "aggregate over a range vector instead, e.g. rate(metric[5m])",
                ));
            }
            self.expect(&Token::RightBracket, "`]`")?;
            range = Some(Duration::from_nanos(nanos));
        }

        let mut offset = None;
        if matches!(self.peek(), Some(Token::Ident(word)) if word == "offset") {
            self.pos += 1;
            let Some(Token::Duration(nanos)) = self.peek().cloned() else {
                return Err(self.unexpected("a duration after `offset`"));
            };
            self.pos += 1;
            offset = Some(Duration::from_nanos(nanos));
        }

        if self.peek() == Some(&Token::At) {
            return Err(Error::unsupported_with_hint(
                "the PromQL `@` modifier",
                "use `offset` to shift the evaluation time",
            ));
        }

        Ok(Expr::Selector(Selector {
            matchers,
            range,
            offset,
        }))
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

impl Expr {
    /// Every selector in the expression, for planning which series to read.
    pub fn selectors(&self) -> Vec<&Selector> {
        let mut out = Vec::new();
        self.collect_selectors(&mut out);
        out
    }

    fn collect_selectors<'a>(&'a self, out: &mut Vec<&'a Selector>) {
        match self {
            Self::Selector(selector) => out.push(selector),
            Self::Call { args, .. } => {
                for arg in args {
                    arg.collect_selectors(out);
                }
            }
            Self::Aggregation { inner, .. } | Self::Negate(inner) => inner.collect_selectors(out),
            Self::Binary { left, right, .. } => {
                left.collect_selectors(out);
                right.collect_selectors(out);
            }
            Self::Number(_) => {}
        }
    }

    /// The widest lookback any part of this expression needs.
    ///
    /// Used to widen the storage read so a `rate(x[5m])` at the start of a range still
    /// has samples behind it — without this the first points of every chart are empty.
    pub fn required_lookback(&self) -> Duration {
        let mut widest = DEFAULT_LOOKBACK;
        self.walk(&mut |expr| {
            if let Self::Selector(selector) = expr {
                let needed = selector.range.unwrap_or(DEFAULT_LOOKBACK)
                    + selector.offset.unwrap_or(Duration::ZERO);
                widest = widest.max(needed);
            }
        });
        widest
    }

    fn walk(&self, visit: &mut impl FnMut(&Self)) {
        visit(self);
        match self {
            Self::Call { args, .. } => {
                for arg in args {
                    arg.walk(visit);
                }
            }
            Self::Aggregation { inner, .. } | Self::Negate(inner) => inner.walk(visit),
            Self::Binary { left, right, .. } => {
                left.walk(visit);
                right.walk(visit);
            }
            Self::Number(_) | Self::Selector(_) => {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn selector_of(expr: &Expr) -> &Selector {
        match expr {
            Expr::Selector(selector) => selector,
            other => panic!("expected a selector, got {other:?}"),
        }
    }

    #[test]
    fn a_bare_metric_name_becomes_a_name_matcher() {
        let expr = parse("http_requests_total").unwrap();
        let selector = selector_of(&expr);
        assert_eq!(selector.matchers.len(), 1);
        assert_eq!(selector.matchers[0].name, "__name__");
        assert_eq!(selector.matchers[0].value, "http_requests_total");
    }

    #[test]
    fn a_selector_carries_its_matchers() {
        let expr = parse(r#"http_requests_total{app="checkout", status=~"5.."}"#).unwrap();
        let selector = selector_of(&expr);
        assert_eq!(selector.matchers.len(), 3, "including __name__");
        assert!(selector.matchers.iter().any(|m| m.name == "app"));
    }

    #[test]
    fn a_matcher_only_selector_parses() {
        let expr = parse(r#"{__name__="up", app="checkout"}"#).unwrap();
        assert_eq!(selector_of(&expr).matchers.len(), 2);
    }

    #[test]
    fn range_and_offset_attach_to_the_selector() {
        let expr = parse("http_requests_total[5m] offset 1h").unwrap();
        let selector = selector_of(&expr);
        assert_eq!(selector.range, Some(Duration::from_secs(300)));
        assert_eq!(selector.offset, Some(Duration::from_secs(3600)));
    }

    #[test]
    fn rate_and_increase_parse() {
        for (query, expected) in [
            ("rate(http_requests_total[5m])", Function::Rate),
            ("increase(http_requests_total[1h])", Function::Increase),
        ] {
            match parse(query).unwrap() {
                Expr::Call { function, args } => {
                    assert_eq!(function, expected);
                    assert_eq!(args.len(), 1);
                }
                other => panic!("{query}: {other:?}"),
            }
        }
    }

    #[test]
    fn aggregations_parse_in_both_spellings() {
        for query in [
            "sum by (app) (rate(http_requests_total[5m]))",
            "sum(rate(http_requests_total[5m])) by (app)",
        ] {
            match parse(query).unwrap() {
                Expr::Aggregation { op, grouping, .. } => {
                    assert_eq!(op, AggregateOp::Sum);
                    assert!(matches!(grouping, Grouping::By(labels) if labels == ["app"]));
                }
                other => panic!("{query}: {other:?}"),
            }
        }
    }

    #[test]
    fn without_grouping_parses() {
        match parse("sum without (instance) (up)").unwrap() {
            Expr::Aggregation { grouping, .. } => {
                assert!(matches!(grouping, Grouping::Without(labels) if labels == ["instance"]));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn every_supported_aggregation_parses() {
        for name in ["sum", "avg", "min", "max", "count"] {
            assert!(parse(&format!("{name}(up)")).is_ok(), "{name}");
        }
    }

    #[test]
    fn the_uis_histogram_quantile_form_parses() {
        // Exactly what PromqlCompiler emits.
        let query = "histogram_quantile(0.95, sum by (le, app) (rate(http_duration_bucket[5m])))";
        match parse(query).unwrap() {
            Expr::Call { function, args } => {
                assert_eq!(function, Function::HistogramQuantile);
                assert_eq!(args.len(), 2);
                assert!(matches!(args[0], Expr::Number(q) if (q - 0.95).abs() < f64::EPSILON));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn the_uis_counter_increase_form_parses() {
        // clamp_min(sel - (sel offset 5m or sel * 0), 0) — needs offset, vector `or`
        // and clamp_min, all three of which were originally listed as out of scope.
        let query = "clamp_min(http_requests_total - (http_requests_total offset 5m or http_requests_total * 0), 0)";
        let expr = parse(query).unwrap();

        assert_eq!(expr.selectors().len(), 3);
        assert!(
            expr.selectors()
                .iter()
                .any(|s| s.offset == Some(Duration::from_secs(300))),
            "the offset must survive parsing"
        );
    }

    #[test]
    fn scalar_arithmetic_parses() {
        match parse("rate(x[5m]) * 60").unwrap() {
            Expr::Binary { op, right, .. } => {
                assert_eq!(op, BinaryOp::Mul);
                assert!(matches!(*right, Expr::Number(n) if (n - 60.0).abs() < f64::EPSILON));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn operator_precedence_follows_promql() {
        // `a + b * c` is `a + (b * c)`.
        match parse("1 + 2 * 3").unwrap() {
            Expr::Binary { op, right, .. } => {
                assert_eq!(op, BinaryOp::Add);
                assert!(matches!(
                    *right,
                    Expr::Binary {
                        op: BinaryOp::Mul,
                        ..
                    }
                ));
            }
            other => panic!("{other:?}"),
        }

        // `or` binds loosest.
        match parse("a * 2 or b").unwrap() {
            Expr::Binary { op, left, .. } => {
                assert_eq!(op, BinaryOp::Or);
                assert!(matches!(
                    *left,
                    Expr::Binary {
                        op: BinaryOp::Mul,
                        ..
                    }
                ));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn parentheses_override_precedence() {
        match parse("(1 + 2) * 3").unwrap() {
            Expr::Binary { op, left, .. } => {
                assert_eq!(op, BinaryOp::Mul);
                assert!(matches!(
                    *left,
                    Expr::Binary {
                        op: BinaryOp::Add,
                        ..
                    }
                ));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn unary_minus_parses() {
        assert!(matches!(parse("-up").unwrap(), Expr::Negate(_)));
    }

    #[test]
    fn required_lookback_widens_for_range_and_offset() {
        assert_eq!(parse("up").unwrap().required_lookback(), DEFAULT_LOOKBACK);
        assert_eq!(
            parse("rate(up[1h])").unwrap().required_lookback(),
            Duration::from_secs(3600)
        );
        // Range plus offset, so the first points of a chart are not empty.
        assert_eq!(
            parse("rate(up[5m] offset 1h)").unwrap().required_lookback(),
            Duration::from_secs(300 + 3600)
        );
    }

    #[test]
    fn unsupported_functions_are_named() {
        for (query, needle) in [
            ("predict_linear(up[1h], 3600)", "predict_linear"),
            ("holt_winters(up[1h], 0.5, 0.5)", "holt_winters"),
            ("topk(5, up)", "topk"),
            ("quantile(0.9, up)", "quantile"),
            (
                "label_replace(up, \"a\", \"b\", \"c\", \"d\")",
                "label_replace",
            ),
        ] {
            let err = parse(query).unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{query}: {err:?}");
            assert!(err.to_string().contains(needle), "{query}: {err}");
        }
    }

    #[test]
    fn subqueries_and_the_at_modifier_are_named() {
        let err = parse("rate(up[5m:1m])").unwrap_err();
        assert!(err.to_string().contains("subquer"), "{err}");

        let err = parse("up @ 1700000000").unwrap_err();
        assert!(err.to_string().contains('@'), "{err}");
    }

    #[test]
    fn and_and_unless_are_named_rather_than_treated_as_junk() {
        for op in ["and", "unless"] {
            let err = parse(&format!("up {op} down")).unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{op}");
            assert!(err.to_string().contains(op), "{op}");
        }
    }

    #[test]
    fn wrong_arity_is_reported_with_the_expected_count() {
        let err = parse("rate(up[5m], 2)").unwrap_err().to_string();
        assert!(err.contains("takes 1 argument"), "{err}");

        let err = parse("histogram_quantile(0.9)").unwrap_err().to_string();
        assert!(err.contains("takes 2 argument"), "{err}");
    }

    #[test]
    fn malformed_queries_are_client_errors() {
        for query in [
            "", "   ", "{", "up{", "up{a=", "sum by", "rate(", "1 +", ")(",
        ] {
            let err = parse(query).unwrap_err();
            assert!(
                matches!(err, Error::BadRequest(_) | Error::Unsupported { .. }),
                "{query} produced {err:?}"
            );
        }
    }

    #[test]
    fn trailing_junk_is_refused() {
        assert!(parse("up up").is_err());
        assert!(parse("up)").is_err());
    }
}
