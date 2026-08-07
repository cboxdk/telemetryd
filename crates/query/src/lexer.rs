//! Shared tokenizer for LogQL and PromQL.
//!
//! Both languages share a lexical core — `{}`-delimited selectors, the same four
//! matcher operators, quoted and backquoted strings, durations like `5m`. One lexer
//! means the two parsers cannot drift on what a string literal or a duration is.

use std::fmt;

use telemetryd_core::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    LeftBrace,
    RightBrace,
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Comma,
    /// `|` — starts a parser or label-filter stage.
    Pipe,
    Equal,
    NotEqual,
    RegexMatch,
    RegexNotMatch,
    /// `&&` — TraceQL condition conjunction.
    AndAnd,
    /// `||` — TraceQL disjunction. Tokenised so it can be *named* as unsupported.
    OrOr,
    /// `|=` — line contains.
    LineContains,
    /// `|~` — line matches regex.
    LineRegex,
    /// Comparison operators, used by PromQL and by LogQL numeric label filters.
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    /// `and` / `or` / `unless` and friends arrive as `Ident`.
    Ident(String),
    String(String),
    Number(f64),
    /// A duration literal such as `5m` or `1h30m`, in nanoseconds.
    Duration(u64),
    /// `@` — PromQL's `@` modifier. Tokenised so it can be *named* as unsupported.
    At,
    /// `:` — subquery separator, likewise.
    Colon,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeftBrace => f.write_str("{"),
            Self::RightBrace => f.write_str("}"),
            Self::LeftParen => f.write_str("("),
            Self::RightParen => f.write_str(")"),
            Self::LeftBracket => f.write_str("["),
            Self::RightBracket => f.write_str("]"),
            Self::Comma => f.write_str(","),
            Self::Pipe => f.write_str("|"),
            Self::Equal => f.write_str("="),
            Self::NotEqual => f.write_str("!="),
            Self::RegexMatch => f.write_str("=~"),
            Self::RegexNotMatch => f.write_str("!~"),
            Self::AndAnd => f.write_str("&&"),
            Self::OrOr => f.write_str("||"),
            Self::LineContains => f.write_str("|="),
            Self::LineRegex => f.write_str("|~"),
            Self::Less => f.write_str("<"),
            Self::LessEqual => f.write_str("<="),
            Self::Greater => f.write_str(">"),
            Self::GreaterEqual => f.write_str(">="),
            Self::Plus => f.write_str("+"),
            Self::Minus => f.write_str("-"),
            Self::Star => f.write_str("*"),
            Self::Slash => f.write_str("/"),
            Self::Percent => f.write_str("%"),
            Self::Caret => f.write_str("^"),
            Self::At => f.write_str("@"),
            Self::Colon => f.write_str(":"),
            Self::Ident(name) => f.write_str(name),
            Self::String(value) => write!(f, "{value:?}"),
            Self::Number(value) => write!(f, "{value}"),
            Self::Duration(nanos) => write!(f, "{nanos}ns"),
        }
    }
}

/// A token plus where it started, so errors can point at the offending character.
#[derive(Debug, Clone, PartialEq)]
pub struct Spanned {
    pub token: Token,
    pub offset: usize,
}

pub fn tokenize(input: &str) -> Result<Vec<Spanned>> {
    Lexer::new(input).run()
}

struct Lexer<'a> {
    input: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
        }
    }

    fn run(mut self) -> Result<Vec<Spanned>> {
        let mut out = Vec::new();
        loop {
            self.skip_whitespace_and_comments();
            if self.pos >= self.bytes.len() {
                return Ok(out);
            }
            let offset = self.pos;
            let token = self.next_token()?;
            out.push(Spanned { token, offset });
        }
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_whitespace() {
                self.pos += 1;
            }
            // `#` to end of line, as both languages allow.
            if self.pos < self.bytes.len() && self.bytes[self.pos] == b'#' {
                while self.pos < self.bytes.len() && self.bytes[self.pos] != b'\n' {
                    self.pos += 1;
                }
            } else {
                return;
            }
        }
    }

    fn peek(&self, ahead: usize) -> Option<u8> {
        self.bytes.get(self.pos + ahead).copied()
    }

    fn next_token(&mut self) -> Result<Token> {
        let ch = self.bytes[self.pos];

        // Two-character operators first, so `!=` is never read as `!` then `=`.
        let two = (ch, self.peek(1));
        let token = match two {
            (b'!', Some(b'=')) => Some(Token::NotEqual),
            (b'!', Some(b'~')) => Some(Token::RegexNotMatch),
            (b'=', Some(b'~')) => Some(Token::RegexMatch),
            (b'=', Some(b'=')) => Some(Token::Equal),
            (b'&', Some(b'&')) => Some(Token::AndAnd),
            (b'|', Some(b'|')) => Some(Token::OrOr),
            (b'|', Some(b'=')) => Some(Token::LineContains),
            (b'|', Some(b'~')) => Some(Token::LineRegex),
            (b'<', Some(b'=')) => Some(Token::LessEqual),
            (b'>', Some(b'=')) => Some(Token::GreaterEqual),
            _ => None,
        };
        if let Some(token) = token {
            self.pos += 2;
            return Ok(token);
        }

        let single = match ch {
            b'{' => Some(Token::LeftBrace),
            b'}' => Some(Token::RightBrace),
            b'(' => Some(Token::LeftParen),
            b')' => Some(Token::RightParen),
            b'[' => Some(Token::LeftBracket),
            b']' => Some(Token::RightBracket),
            b',' => Some(Token::Comma),
            b'|' => Some(Token::Pipe),
            b'=' => Some(Token::Equal),
            b'<' => Some(Token::Less),
            b'>' => Some(Token::Greater),
            b'+' => Some(Token::Plus),
            b'-' => Some(Token::Minus),
            b'*' => Some(Token::Star),
            b'/' => Some(Token::Slash),
            b'%' => Some(Token::Percent),
            b'^' => Some(Token::Caret),
            b'@' => Some(Token::At),
            b':' => Some(Token::Colon),
            _ => None,
        };
        if let Some(token) = single {
            self.pos += 1;
            return Ok(token);
        }

        match ch {
            b'"' | b'\'' => self.lex_quoted(ch),
            b'`' => self.lex_raw(),
            b'0'..=b'9' => self.lex_number(),
            c if c.is_ascii_alphabetic() || c == b'_' || c == b':' => Ok(self.lex_ident()),
            _ => Err(self.error(format!(
                "unexpected character {:?}",
                self.input[self.pos..].chars().next().unwrap_or('?')
            ))),
        }
    }

    fn lex_quoted(&mut self, quote: u8) -> Result<Token> {
        let start = self.pos;
        self.pos += 1;
        let mut out = String::new();

        while self.pos < self.bytes.len() {
            let ch = self.bytes[self.pos];
            if ch == b'\\' {
                self.pos += 1;
                // Decode the escaped character as a `char`, not a byte. Advancing one
                // byte past a multi-byte lead byte would leave `pos` inside a UTF-8
                // sequence, and the next slice would panic — reachable from any
                // request, since this runs on the raw query string.
                let escaped = self.input[self.pos..]
                    .chars()
                    .next()
                    .ok_or_else(|| self.error("string ends with a trailing backslash"))?;
                self.pos += escaped.len_utf8();

                let decoded = match escaped {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '\\' => '\\',
                    '"' => '"',
                    '\'' => '\'',
                    '`' => '`',
                    // An unknown escape keeps both characters, which is what a user
                    // writing a regex like "\d" almost always means.
                    other => {
                        out.push('\\');
                        other
                    }
                };
                out.push(decoded);
                continue;
            }
            if ch == quote {
                self.pos += 1;
                return Ok(Token::String(out));
            }
            // Multi-byte UTF-8 passes through intact.
            let rest = &self.input[self.pos..];
            let c = rest.chars().next().unwrap_or('\u{fffd}');
            out.push(c);
            self.pos += c.len_utf8();
        }

        self.pos = start;
        Err(self.error("unterminated string literal"))
    }

    /// Backquoted raw string: no escape processing, as in both languages.
    fn lex_raw(&mut self) -> Result<Token> {
        let start = self.pos;
        self.pos += 1;
        let content_start = self.pos;
        while self.pos < self.bytes.len() {
            if self.bytes[self.pos] == b'`' {
                let value = self.input[content_start..self.pos].to_owned();
                self.pos += 1;
                return Ok(Token::String(value));
            }
            self.pos += 1;
        }
        self.pos = start;
        Err(self.error("unterminated raw string literal"))
    }

    fn lex_number(&mut self) -> Result<Token> {
        let start = self.pos;
        while self
            .peek(0)
            .is_some_and(|c| c.is_ascii_digit() || c == b'.' || c == b'e' || c == b'E')
        {
            // `1e5` is a number; `1e` followed by a letter would be a duration unit.
            if (self.bytes[self.pos] == b'e' || self.bytes[self.pos] == b'E')
                && !self
                    .peek(1)
                    .is_some_and(|c| c.is_ascii_digit() || c == b'-' || c == b'+')
            {
                break;
            }
            self.pos += 1;
        }

        // A trailing unit makes this a duration: 5m, 1h30m, 500ms.
        if self.peek(0).is_some_and(|c| c.is_ascii_alphabetic()) {
            self.pos = start;
            return self.lex_duration();
        }

        self.input[start..self.pos]
            .parse::<f64>()
            .map(Token::Number)
            .map_err(|_| {
                let text = self.input[start..self.pos].to_owned();
                self.error(format!("{text:?} is not a valid number"))
            })
    }

    /// Durations are a sequence of value+unit pairs: `1h30m`, `500ms`, `2w`.
    fn lex_duration(&mut self) -> Result<Token> {
        let start = self.pos;
        let mut total: u64 = 0;
        let mut parsed_any = false;

        while self.pos < self.bytes.len() {
            let digits_start = self.pos;
            while self
                .peek(0)
                .is_some_and(|c| c.is_ascii_digit() || c == b'.')
            {
                self.pos += 1;
            }
            if digits_start == self.pos {
                break;
            }
            let value: f64 = self.input[digits_start..self.pos]
                .parse()
                .map_err(|_| self.error("invalid duration value"))?;

            let unit_start = self.pos;
            while self.peek(0).is_some_and(|c| c.is_ascii_alphabetic()) {
                self.pos += 1;
            }
            let unit = &self.input[unit_start..self.pos];
            let nanos_per = match unit {
                "ns" => 1.0,
                "us" | "µs" => 1_000.0,
                "ms" => 1_000_000.0,
                "s" => 1_000_000_000.0,
                "m" => 60.0 * 1e9,
                "h" => 3_600.0 * 1e9,
                "d" => 86_400.0 * 1e9,
                "w" => 7.0 * 86_400.0 * 1e9,
                "y" => 365.0 * 86_400.0 * 1e9,
                other => {
                    return Err(self.error(format!(
                        "{other:?} is not a duration unit (use ns, us, ms, s, m, h, d, w or y)"
                    )));
                }
            };
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let component = (value * nanos_per) as u64;
            total = total.saturating_add(component);
            parsed_any = true;
        }

        if !parsed_any {
            self.pos = start;
            return Err(self.error("expected a duration"));
        }
        Ok(Token::Duration(total))
    }

    /// Identifiers, including dotted paths.
    ///
    /// TraceQL field paths are dotted (`resource.service.name`, `span.http.status_code`),
    /// so `.` continues an identifier — but only when a name character follows it.
    /// Requiring the lookahead keeps `1.5` a number and stops a trailing `.` from
    /// swallowing the next token.
    fn lex_ident(&mut self) -> Token {
        let start = self.pos;
        loop {
            match self.peek(0) {
                Some(c) if c.is_ascii_alphanumeric() || c == b'_' || c == b':' => self.pos += 1,
                Some(b'.')
                    if self
                        .peek(1)
                        .is_some_and(|next| next.is_ascii_alphanumeric() || next == b'_') =>
                {
                    self.pos += 1;
                }
                _ => break,
            }
        }
        Token::Ident(self.input[start..self.pos].to_owned())
    }

    fn error(&self, message: impl Into<String>) -> Error {
        Error::BadRequest(format!(
            "{} at position {} in {:?}",
            message.into(),
            self.pos,
            self.input
        ))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn tokens(input: &str) -> Vec<Token> {
        tokenize(input)
            .unwrap()
            .into_iter()
            .map(|s| s.token)
            .collect()
    }

    #[test]
    fn tokenizes_a_stream_selector() {
        assert_eq!(
            tokens(r#"{app="checkout", level=~"err.*"}"#),
            vec![
                Token::LeftBrace,
                Token::Ident("app".into()),
                Token::Equal,
                Token::String("checkout".into()),
                Token::Comma,
                Token::Ident("level".into()),
                Token::RegexMatch,
                Token::String("err.*".into()),
                Token::RightBrace,
            ]
        );
    }

    #[test]
    fn two_character_operators_are_never_split() {
        assert_eq!(tokens("!="), vec![Token::NotEqual]);
        assert_eq!(tokens("!~"), vec![Token::RegexNotMatch]);
        assert_eq!(tokens("=~"), vec![Token::RegexMatch]);
        assert_eq!(tokens("|="), vec![Token::LineContains]);
        assert_eq!(tokens("|~"), vec![Token::LineRegex]);
        assert_eq!(tokens(">="), vec![Token::GreaterEqual]);
        // A bare pipe is still a pipe.
        assert_eq!(
            tokens("| json"),
            vec![Token::Pipe, Token::Ident("json".into())]
        );
    }

    #[test]
    fn string_escapes_are_processed() {
        assert_eq!(tokens(r#""a\nb""#), vec![Token::String("a\nb".into())]);
        assert_eq!(
            tokens(r#""quote\"inside""#),
            vec![Token::String("quote\"inside".into())]
        );
        assert_eq!(
            tokens(r#""back\\slash""#),
            vec![Token::String("back\\slash".into())]
        );
    }

    #[test]
    fn an_unknown_escape_keeps_the_backslash() {
        // \d in a regex must survive as \d, not become d.
        assert_eq!(tokens(r#""\d+""#), vec![Token::String(r"\d+".into())]);
    }

    #[test]
    fn raw_strings_do_not_process_escapes() {
        assert_eq!(tokens(r"`a\nb`"), vec![Token::String(r"a\nb".into())]);
    }

    #[test]
    fn unterminated_strings_are_a_clean_error() {
        for input in [r#""unterminated"#, "`unterminated", r#""trailing\"#] {
            let err = tokenize(input).unwrap_err();
            assert!(matches!(err, Error::BadRequest(_)), "{input}");
            assert!(err.to_string().contains("terminat") || err.to_string().contains("backslash"));
        }
    }

    #[test]
    fn durations_parse_including_compound_forms() {
        assert_eq!(tokens("5m"), vec![Token::Duration(300_000_000_000)]);
        assert_eq!(tokens("500ms"), vec![Token::Duration(500_000_000)]);
        assert_eq!(tokens("1h30m"), vec![Token::Duration(5_400_000_000_000)]);
        assert_eq!(tokens("1w"), vec![Token::Duration(604_800_000_000_000)]);
    }

    #[test]
    fn numbers_and_durations_are_distinguished() {
        assert_eq!(tokens("42"), vec![Token::Number(42.0)]);
        assert_eq!(tokens("1.5"), vec![Token::Number(1.5)]);
        assert_eq!(tokens("1e3"), vec![Token::Number(1000.0)]);
        assert_eq!(tokens("30s"), vec![Token::Duration(30_000_000_000)]);
    }

    #[test]
    fn an_unknown_duration_unit_names_the_valid_ones() {
        let err = tokenize("5q").unwrap_err().to_string();
        assert!(err.contains("not a duration unit"), "{err}");
        assert!(err.contains("ms"), "{err}");
    }

    #[test]
    fn comments_and_whitespace_are_skipped() {
        assert_eq!(
            tokens("  {app=\"x\"}  # trailing comment\n"),
            tokens(r#"{app="x"}"#)
        );
    }

    #[test]
    fn utf8_survives_inside_strings() {
        assert_eq!(tokens(r#""æøå 🎉""#), vec![Token::String("æøå 🎉".into())]);
    }

    #[test]
    fn an_unexpected_character_reports_its_position() {
        let err = tokenize("{app=$}").unwrap_err().to_string();
        assert!(err.contains("unexpected character"), "{err}");
        assert!(err.contains("position"), "{err}");
    }

    #[test]
    fn dotted_identifiers_lex_as_one_token() {
        // TraceQL field paths.
        assert_eq!(
            tokens("resource.service.name"),
            vec![Token::Ident("resource.service.name".into())]
        );
        assert_eq!(
            tokens("span.http.status_code"),
            vec![Token::Ident("span.http.status_code".into())]
        );
    }

    #[test]
    fn a_dot_does_not_swallow_numbers_or_trailing_tokens() {
        // The lookahead is what keeps these correct.
        assert_eq!(tokens("1.5"), vec![Token::Number(1.5)]);
        assert_eq!(tokens("1.5h"), vec![Token::Duration(5_400_000_000_000)]);
        // A trailing dot is not part of the identifier, and `.` is not a token on its
        // own — so this is a syntax error rather than a silently truncated name.
        assert!(tokenize("foo.").is_err());
    }

    #[test]
    fn traceql_conjunctions_lex_as_single_tokens() {
        assert_eq!(tokens("&&"), vec![Token::AndAnd]);
        // `||` must not be read as two pipes, or a TraceQL disjunction would look
        // like an empty pipeline stage.
        assert_eq!(tokens("||"), vec![Token::OrOr]);
        assert_eq!(tokens("|"), vec![Token::Pipe]);
    }

    #[test]
    fn promql_only_tokens_are_recognised_so_they_can_be_named_later() {
        // Tokenising `@` and `:` is what lets the parser say "the @ modifier is not
        // supported" instead of "syntax error".
        assert!(tokens("foo @ 1").contains(&Token::At));
        assert!(tokens("foo[5m:1m]").contains(&Token::Colon));
    }
}
