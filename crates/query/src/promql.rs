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
        /// The scalar `k` of `topk(k, v)`. `None` for the operators that take none.
        param: Option<Box<Expr>>,
        inner: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
        modifier: BinaryModifier,
    },
    /// Unary minus.
    Negate(Box<Expr>),
}

#[derive(Debug, Clone)]
pub struct Selector {
    pub matchers: Vec<LabelMatcher>,
    /// `[5m]` — present makes this a range vector.
    pub range: Option<Duration>,
    /// `offset 5m` looks back five minutes; `offset -5m` looks ahead.
    pub offset: Offset,
}

/// How far a selector shifts its evaluation time.
///
/// Signed, because Prometheus 3 allows `offset -5m`: the value five minutes *after* the
/// evaluation time, which is how a panel lines tomorrow's forecast up against today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Offset {
    pub nanos: u64,
    pub ahead: bool,
}

impl Offset {
    #[must_use]
    pub fn back(duration: Duration) -> Self {
        Self {
            nanos: u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX),
            ahead: false,
        }
    }

    /// The time a selector evaluated at `at_nanos` reads.
    #[must_use]
    pub fn apply(self, at_nanos: u64) -> u64 {
        if self.ahead {
            at_nanos.saturating_add(self.nanos)
        } else {
            at_nanos.saturating_sub(self.nanos)
        }
    }

    /// How far before the evaluation time this reaches.
    #[must_use]
    pub fn behind(self) -> Duration {
        if self.ahead {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.nanos)
        }
    }

    /// How far after the evaluation time this reaches.
    #[must_use]
    pub fn beyond(self) -> Duration {
        if self.ahead {
            Duration::from_nanos(self.nanos)
        } else {
            Duration::ZERO
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Function {
    Rate,
    Increase,
    HistogramQuantile,
    ClampMin,
    ClampMax,
    Abs,
    /// `vector(s)`: a scalar as a one-element vector with no labels. Grafana's Loki
    /// health check is `vector(1)+vector(1)`.
    Vector,
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
            "vector" => Self::Vector,
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
            Self::Vector => "vector",
        }
    }

    /// Whether the function answers a number rather than a vector.
    #[must_use]
    pub fn returns_scalar(self) -> bool {
        false
    }

    /// Whether the function takes a range vector (`sel[5m]`) as its argument.
    pub fn wants_range(self) -> bool {
        matches!(self, Self::Rate | Self::Increase)
    }

    fn arity(self) -> usize {
        match self {
            Self::Rate | Self::Increase | Self::Abs | Self::Vector => 1,
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
    /// `topk(k, v)` — the `k` largest elements, keeping their own labels.
    TopK,
    /// `bottomk(k, v)` — the `k` smallest.
    BottomK,
}

impl AggregateOp {
    /// Whether the operator takes a scalar before the vector, as `topk(5, x)` does.
    #[must_use]
    pub fn takes_parameter(self) -> bool {
        matches!(self, Self::TopK | Self::BottomK)
    }

    /// Whether the result keeps each element's own labels instead of the grouping's.
    ///
    /// `sum by (route)` reduces a group to one value labelled by the grouping.
    /// `topk` selects *elements*: the answer is a subset of the input, labels and all,
    /// and grouping only decides within which set the selection happens. Treating it
    /// like `sum` would collapse the very series the panel is asking to see.
    #[must_use]
    pub fn selects_elements(self) -> bool {
        matches!(self, Self::TopK | Self::BottomK)
    }
}

impl AggregateOp {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "sum" => Self::Sum,
            "avg" => Self::Avg,
            "min" => Self::Min,
            "max" => Self::Max,
            "count" => Self::Count,
            "topk" => Self::TopK,
            "bottomk" => Self::BottomK,
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
            Self::TopK => "topk",
            Self::BottomK => "bottomk",
        }
    }
}

/// `by (…)` / `without (…)`, or neither.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    Atan2,
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    /// The left side's elements that have a match on the right.
    And,
    /// Vector union: the left side wins, the right fills gaps.
    Or,
    /// The left side's elements that have no match on the right.
    Unless,
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
            Self::Atan2 => "atan2",
            Self::Eq => "==",
            Self::Ne => "!=",
            Self::Gt => ">",
            Self::Lt => "<",
            Self::Ge => ">=",
            Self::Le => "<=",
            Self::And => "and",
            Self::Or => "or",
            Self::Unless => "unless",
        }
    }

    /// `==`, `!=`, `>`, `<`, `>=`, `<=`: they filter, or with `bool` answer 0 or 1.
    #[must_use]
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Ne | Self::Gt | Self::Lt | Self::Ge | Self::Le
        )
    }

    /// `and`, `or`, `unless`: they choose elements rather than compute values.
    #[must_use]
    pub fn is_set(self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Unless)
    }

    /// How tightly it binds, loosest first, as in Prometheus. `^` binds tighter than
    /// all of these and is parsed on its own.
    fn precedence(self) -> u8 {
        match self {
            Self::Or => 1,
            Self::And | Self::Unless => 2,
            Self::Eq | Self::Ne | Self::Gt | Self::Lt | Self::Ge | Self::Le => 3,
            Self::Add | Self::Sub => 4,
            Self::Mul | Self::Div | Self::Mod | Self::Atan2 => 5,
            Self::Pow => 6,
        }
    }
}

/// What follows a binary operator: `bool`, `on(…)`/`ignoring(…)`, and
/// `group_left(…)`/`group_right(…)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BinaryModifier {
    /// A comparison answers 0 or 1 instead of filtering.
    pub return_bool: bool,
    /// `on` or `ignoring` and its labels. `None` matches on every label but the name.
    pub matching: Option<Matching>,
    /// Many-to-one or one-to-many, with the labels to copy from the "one" side.
    pub group: Option<Group>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matching {
    /// `on(…)` when true, `ignoring(…)` when false.
    pub on: bool,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Group {
    /// `group_left(…)`: many on the left, one on the right.
    Left(Vec<String>),
    /// `group_right(…)`: one on the left, many on the right.
    Right(Vec<String>),
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
        depth: 0,
    };
    let expr = parser.parse_expr()?;
    if parser.pos < parser.tokens.len() {
        return Err(parser.unexpected("the end of the query"));
    }
    Ok(expr)
}

/// How deep an expression may nest, counting parentheses, unary minus, function and
/// aggregation arguments, and every link in a chain of binary operators.
///
/// Parsing, evaluating, cloning and dropping an expression all recurse on its tree, on a
/// thread with a two-megabyte stack. Two thousand `(` — a two-kilobyte URL — overflowed
/// that stack, and an overflow aborts the process: one request from anyone holding a
/// query token took the server down. Real queries nest a handful of levels; the limit is
/// generous for them and far below where the stack runs out.
const MAX_DEPTH: usize = 128;

struct Parser<'a> {
    input: &'a str,
    tokens: Vec<Spanned>,
    pos: usize,
    /// The nesting depth reached so far. Checked as it grows rather than measured on the
    /// finished tree, because a tree too deep to evaluate is also too deep to drop.
    depth: usize,
}

impl Parser<'_> {
    fn deeper(&mut self) -> Result<()> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(Error::BadRequest(format!(
                "this PromQL expression nests more than {MAX_DEPTH} levels deep, counting \
                 parentheses, function arguments and chained operators; split it up"
            )));
        }
        Ok(())
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_binary(1)
    }

    /// Binary operators by precedence climbing: `or` loosest, then `and`/`unless`,
    /// comparisons, `+`/`-`, and `*`/`/`/`%`/`atan2`, each associating left.
    fn parse_binary(&mut self, min_precedence: u8) -> Result<Expr> {
        let base = self.depth;
        self.deeper()?;
        let mut left = self.parse_unary()?;
        while let Some(op) = self.peek_binary_op() {
            if op.precedence() < min_precedence {
                break;
            }
            self.pos += 1;
            let modifier = self.parse_modifier(op)?;
            self.deeper()?;
            let right = self.parse_binary(op.precedence() + 1)?;
            check_operands(op, &modifier, &left, &right)?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
                modifier,
            };
        }
        self.depth = base;
        Ok(left)
    }

    fn peek_binary_op(&self) -> Option<BinaryOp> {
        Some(match self.peek()? {
            Token::Plus => BinaryOp::Add,
            Token::Minus => BinaryOp::Sub,
            Token::Star => BinaryOp::Mul,
            Token::Slash => BinaryOp::Div,
            Token::Percent => BinaryOp::Mod,
            Token::EqualEqual => BinaryOp::Eq,
            Token::NotEqual => BinaryOp::Ne,
            Token::Greater => BinaryOp::Gt,
            Token::Less => BinaryOp::Lt,
            Token::GreaterEqual => BinaryOp::Ge,
            Token::LessEqual => BinaryOp::Le,
            Token::Ident(word) => match word.as_str() {
                "and" => BinaryOp::And,
                "or" => BinaryOp::Or,
                "unless" => BinaryOp::Unless,
                "atan2" => BinaryOp::Atan2,
                _ => return None,
            },
            _ => return None,
        })
    }

    /// `bool`, then `on(…)` or `ignoring(…)`, then `group_left(…)` or `group_right(…)`.
    fn parse_modifier(&mut self, op: BinaryOp) -> Result<BinaryModifier> {
        let mut modifier = BinaryModifier::default();
        if matches!(self.peek(), Some(Token::Ident(word)) if word == "bool") {
            if !op.is_comparison() {
                return Err(Error::BadRequest(format!(
                    "`bool` only follows a comparison operator, not `{}`",
                    op.as_str()
                )));
            }
            self.pos += 1;
            modifier.return_bool = true;
        }
        if let Some(Token::Ident(word)) = self.peek()
            && (word == "on" || word == "ignoring")
        {
            let on = word == "on";
            self.pos += 1;
            modifier.matching = Some(Matching {
                on,
                labels: self.parse_label_list()?,
            });
            if let Some(Token::Ident(word)) = self.peek()
                && (word == "group_left" || word == "group_right")
            {
                if op.is_set() {
                    return Err(Error::BadRequest(format!(
                        "no grouping is allowed with `{}`: it matches many to many already",
                        op.as_str()
                    )));
                }
                let left = word == "group_left";
                self.pos += 1;
                let labels = if self.peek() == Some(&Token::LeftParen) {
                    self.parse_label_list()?
                } else {
                    Vec::new()
                };
                modifier.group = Some(if left {
                    Group::Left(labels)
                } else {
                    Group::Right(labels)
                });
            }
        } else if matches!(self.peek(), Some(Token::Ident(word)) if word == "group_left" || word == "group_right")
        {
            return Err(Error::BadRequest(
                "`group_left`/`group_right` need `on(…)` or `ignoring(…)` before them".to_owned(),
            ));
        }
        Ok(modifier)
    }

    /// `(a, b, c)` — the labels of `on`, `ignoring`, `group_left` and `group_right`.
    fn parse_label_list(&mut self) -> Result<Vec<String>> {
        self.expect(&Token::LeftParen, "`(`")?;
        let mut labels = Vec::new();
        loop {
            if self.peek() == Some(&Token::RightParen) {
                self.pos += 1;
                return Ok(labels);
            }
            labels.push(self.expect_ident("a label name")?);
            match self.peek() {
                Some(Token::Comma) => self.pos += 1,
                Some(Token::RightParen) => {}
                _ => return Err(self.unexpected("`,` or `)`")),
            }
        }
    }

    /// Unary minus binds looser than `^` and tighter than `*`: `-2^2` is `-(2^2)`, -4,
    /// as in PromQL and in arithmetic.
    fn parse_unary(&mut self) -> Result<Expr> {
        // Unary plus changes nothing and is allowed, as in PromQL: `+Inf`, `2 * +1`.
        if self.peek() == Some(&Token::Plus) {
            self.pos += 1;
            let base = self.depth;
            self.deeper()?;
            let inner = self.parse_unary()?;
            self.depth = base;
            return Ok(inner);
        }
        if self.peek() == Some(&Token::Minus) {
            self.pos += 1;
            let base = self.depth;
            self.deeper()?;
            let inner = self.parse_unary()?;
            self.depth = base;
            return Ok(Expr::Negate(Box::new(inner)));
        }
        self.parse_power()
    }

    /// `^` binds tightest of the binary operators and to the right: `2 * 3^2` is 18 and
    /// `2^3^2` is `2^(3^2)`, 512. It shared a level with `*` and associated left, which
    /// made them 36 and 64. The exponent may carry its own sign, `2^-1`.
    fn parse_power(&mut self) -> Result<Expr> {
        let base = self.parse_atom()?;
        if self.peek() != Some(&Token::Caret) {
            return Ok(base);
        }
        self.pos += 1;
        let depth = self.depth;
        self.deeper()?;
        let exponent = self.parse_unary()?;
        self.depth = depth;
        Ok(Expr::Binary {
            op: BinaryOp::Pow,
            left: Box::new(base),
            right: Box::new(exponent),
            modifier: BinaryModifier::default(),
        })
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
            // `NaN` and `Inf` are numbers in any case, as Prometheus lexes them — never
            // metric names, which is how they used to be read.
            Some(Token::Ident(name))
                if name.eq_ignore_ascii_case("nan") || name.eq_ignore_ascii_case("inf") =>
            {
                self.pos += 1;
                Ok(Expr::Number(if name.eq_ignore_ascii_case("nan") {
                    f64::NAN
                } else {
                    f64::INFINITY
                }))
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
        let param = if op.takes_parameter() {
            let k = self.parse_expr()?;
            self.expect(&Token::Comma, "`,` after the count in `topk`/`bottomk`")?;
            Some(Box::new(k))
        } else {
            None
        };
        let inner = self.parse_expr()?;
        self.expect(&Token::RightParen, "`)`")?;

        if matches!(grouping, Grouping::All) {
            grouping = self.parse_grouping()?;
        }

        Ok(Expr::Aggregation {
            op,
            grouping,
            param,
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

        let mut offset = Offset::default();
        if matches!(self.peek(), Some(Token::Ident(word)) if word == "offset") {
            self.pos += 1;
            let ahead = self.peek() == Some(&Token::Minus);
            if ahead {
                self.pos += 1;
            }
            let Some(Token::Duration(nanos)) = self.peek().cloned() else {
                return Err(self.unexpected("a duration after `offset`"));
            };
            self.pos += 1;
            offset = Offset { nanos, ahead };
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

/// Whether an expression evaluates to a scalar rather than a vector, decided from its
/// shape as Prometheus's parser decides it.
fn is_scalar(expr: &Expr) -> bool {
    match expr {
        Expr::Number(_) => true,
        Expr::Negate(inner) => is_scalar(inner),
        Expr::Binary { left, right, .. } => is_scalar(left) && is_scalar(right),
        Expr::Call { function, .. } => function.returns_scalar(),
        Expr::Selector(_) | Expr::Aggregation { .. } => false,
    }
}

/// The type rules Prometheus's parser applies to a binary operation, so an expression
/// Prometheus refuses is refused here too rather than answered.
fn check_operands(
    op: BinaryOp,
    modifier: &BinaryModifier,
    left: &Expr,
    right: &Expr,
) -> Result<()> {
    let (left_scalar, right_scalar) = (is_scalar(left), is_scalar(right));
    if op.is_set() && (left_scalar || right_scalar) {
        return Err(Error::BadRequest(format!(
            "`{}` works between vectors, and one side of this one is a number",
            op.as_str()
        )));
    }
    if op.is_comparison() && left_scalar && right_scalar && !modifier.return_bool {
        return Err(Error::BadRequest(format!(
            "comparing two numbers needs `bool`, as in `1 {} bool 2`",
            op.as_str()
        )));
    }
    if modifier.matching.is_some() && (left_scalar || right_scalar) {
        return Err(Error::BadRequest(
            "`on`/`ignoring` match two vectors, and one side of this one is a number".to_owned(),
        ));
    }
    Ok(())
}

impl Expr {
    /// Every selector in the expression, for planning which series to read.
    pub fn selectors(&self) -> Vec<&Selector> {
        let mut out = Vec::new();
        self.collect_selectors(&mut out);
        out
    }

    /// Every grouping this expression aggregates by, so they can be resolved once
    /// instead of once per step.
    #[must_use]
    pub fn groupings(&self) -> Vec<Grouping> {
        let mut out = Vec::new();
        self.collect_groupings(&mut out);
        out
    }

    fn collect_groupings(&self, out: &mut Vec<Grouping>) {
        match self {
            Self::Aggregation {
                grouping,
                param,
                inner,
                ..
            } => {
                out.push(grouping.clone());
                if let Some(param) = param {
                    param.collect_groupings(out);
                }
                inner.collect_groupings(out);
            }
            Self::Call { args, .. } => {
                for arg in args {
                    arg.collect_groupings(out);
                }
            }
            Self::Binary { left, right, .. } => {
                left.collect_groupings(out);
                right.collect_groupings(out);
            }
            Self::Negate(inner) => inner.collect_groupings(out),
            Self::Selector(_) | Self::Number(_) => {}
        }
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
                let needed = selector.range.unwrap_or(DEFAULT_LOOKBACK) + selector.offset.behind();
                widest = widest.max(needed);
            }
        });
        widest
    }

    /// How far past the last evaluation time any part of this expression reads — a
    /// negative `offset`'s reach. Zero for everything else.
    pub fn required_lookahead(&self) -> Duration {
        let mut furthest = Duration::ZERO;
        self.walk(&mut |expr| {
            if let Self::Selector(selector) = expr {
                furthest = furthest.max(selector.offset.beyond());
            }
        });
        furthest
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

    /// Each of these used to overflow the stack and abort the process. Now each is a
    /// plain refusal, and a realistically nested query still parses.
    #[test]
    fn deep_nesting_is_refused_instead_of_overflowing_the_stack() {
        let parens = format!("{}1{}", "(".repeat(2_000), ")".repeat(2_000));
        let minus = format!("{}1", "-".repeat(16_000));
        let chain = vec!["1"; 16_000].join("-");
        let calls = format!("{}x{}", "abs(".repeat(2_000), ")".repeat(2_000));
        for query in [&parens, &minus, &chain, &calls] {
            let err = parse(query).expect_err("nested far too deep");
            assert!(err.to_string().contains("nests more than"), "{err}");
        }

        let fine = format!("{}sum(rate(x[5m])){}", "(".repeat(100), ")".repeat(100));
        parse(&fine).expect("a hundred levels is still a query");
        let long = vec!["a"; 100].join(" + ");
        parse(&long).expect("a hundred terms is still a query");
    }

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
        assert_eq!(selector.offset, Offset::back(Duration::from_secs(3600)));
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
                .any(|s| s.offset == Offset::back(Duration::from_secs(300))),
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
            ("quantile(0.9, up)", "quantile"),
            ("count_values(\"v\", up)", "count_values"),
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

    fn binary_parts(expr: &Expr) -> (BinaryOp, &Expr, &Expr, &BinaryModifier) {
        match expr {
            Expr::Binary {
                op,
                left,
                right,
                modifier,
            } => (*op, left, right, modifier),
            other => panic!("expected a binary operation, got {other:?}"),
        }
    }

    /// Prometheus's ladder: `or` loosest, then `and`/`unless`, comparisons, `+`/`-`,
    /// `*`/`/`/`%`/`atan2`.
    #[test]
    fn set_and_comparison_operators_bind_as_in_prometheus() {
        let first = parse("a or b and c").unwrap();
        let (op, _, right, _) = binary_parts(&first);
        assert_eq!(op, BinaryOp::Or);
        assert_eq!(binary_parts(right).0, BinaryOp::And);

        let second = parse("a + 1 > b unless c").unwrap();
        let (op, left, _, _) = binary_parts(&second);
        assert_eq!(op, BinaryOp::Unless);
        let (op, left, _, _) = binary_parts(left);
        assert_eq!(op, BinaryOp::Gt);
        assert_eq!(binary_parts(left).0, BinaryOp::Add);

        let third = parse("a - b atan2 c").unwrap();
        let (op, _, right, _) = binary_parts(&third);
        assert_eq!(op, BinaryOp::Sub);
        assert_eq!(binary_parts(right).0, BinaryOp::Atan2);
    }

    #[test]
    fn modifiers_parse_after_the_operator() {
        let expr = parse("a / on(job, instance) group_left(team) b").unwrap();
        let (op, _, _, modifier) = binary_parts(&expr);
        assert_eq!(op, BinaryOp::Div);
        assert_eq!(
            modifier.matching,
            Some(Matching {
                on: true,
                labels: vec!["job".into(), "instance".into()]
            })
        );
        assert_eq!(modifier.group, Some(Group::Left(vec!["team".into()])));

        let expr = parse("a > bool ignoring(code) b").unwrap();
        let (op, _, _, modifier) = binary_parts(&expr);
        assert_eq!(op, BinaryOp::Gt);
        assert!(modifier.return_bool);
        assert_eq!(
            modifier.matching,
            Some(Matching {
                on: false,
                labels: vec!["code".into()]
            })
        );

        let expr = parse("a * on() group_right b").unwrap();
        assert_eq!(binary_parts(&expr).3.group, Some(Group::Right(Vec::new())));
    }

    /// What Prometheus's parser refuses, refused here too.
    #[test]
    fn misused_operators_are_refused() {
        for (query, says) in [
            ("1 > 2", "needs `bool`"),
            ("a + bool b", "only follows a comparison"),
            ("a and 1", "between vectors"),
            ("1 or a", "between vectors"),
            ("a and on(x) group_left b", "no grouping"),
            ("a + group_left b", "need `on(…)`"),
            ("a + on(x) 1", "match two vectors"),
        ] {
            let err = parse(query).unwrap_err().to_string();
            assert!(err.contains(says), "{query}: {err}");
        }
        assert!(parse("1 > bool 2").is_ok());
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
