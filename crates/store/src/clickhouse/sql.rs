//! Building SQL from what a query asks for.
//!
//! Every value reaches ClickHouse as a quoted literal built here, never spliced in raw.

use telemetryd_core::{APP_LABEL, LabelMatcher, MatchOp};

/// A string literal: backslashes and single quotes escaped, which is all ClickHouse's
/// quoted strings need.
pub(crate) fn quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// `LIKE '%text%'`, with the pattern's own `%`, `_` and `\` escaped.
pub(crate) fn like_contains(text: &str) -> String {
    let mut pattern = String::with_capacity(text.len() + 2);
    pattern.push('%');
    for c in text.chars() {
        if matches!(c, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(c);
    }
    pattern.push('%');
    quote(&pattern)
}

/// A nanosecond bound as a `DateTime64(9)` literal. `u64` nanos past `i64::MAX` are
/// later than any timestamp ClickHouse can hold, so they clamp rather than wrap.
pub(crate) fn nanos(value: u64) -> String {
    let value = i64::try_from(value).unwrap_or(i64::MAX);
    format!("fromUnixTimestamp64Nano(toInt64({value}))")
}

/// One label matcher as a condition on the stream map.
///
/// `app` reads `ServiceName` instead, because that is the table's sort key: an `app`
/// matcher is what lets ClickHouse skip every other app's data without reading it. An
/// absent key reads as `''`, which is the absent-label semantics matchers have
/// everywhere else.
///
/// Regexes are RE2 on this side and Rust's on the other, which agree on the syntax
/// matchers use. Where they might not, the caller's own check after the read is the
/// authority — this may only ever select too much, never too little.
pub(crate) fn matcher(stream_column: &str, service_column: &str, m: &LabelMatcher) -> String {
    let column = if m.name == APP_LABEL {
        service_column.to_owned()
    } else {
        format!("{stream_column}[{}]", quote(&m.name))
    };
    // `(?-s)`: `.` stops at a newline in Rust, and must here too, or `!~ "x.y"` would
    // drop a value with a newline in it that Rust keeps.
    let anchored = || quote(&format!("^(?-s:{})$", m.value));
    match m.op {
        MatchOp::Equal => format!("{column} = {}", quote(&m.value)),
        MatchOp::NotEqual => format!("{column} != {}", quote(&m.value)),
        // A pattern the two engines could read differently selects nothing here: the
        // matchers are checked again on what comes back, so leaving it out can only
        // read more.
        MatchOp::Regex | MatchOp::NotRegex if !same_in_both(&m.value) => "1".to_owned(),
        MatchOp::Regex => format!("match({column}, {})", anchored()),
        MatchOp::NotRegex => format!("NOT match({column}, {})", anchored()),
    }
}

/// Whether ClickHouse's RE2 reads a pattern as Rust's regex does.
///
/// They share their syntax but not their classes: `\w`, `\d`, `\s` and `\b` are
/// Unicode-aware in Rust and ASCII in RE2, so `\w+` matches "José" in one and not the
/// other. A pattern using any of them, or any Unicode at all, is left to Rust.
pub(crate) fn same_in_both(pattern: &str) -> bool {
    if !pattern.is_ascii() {
        return false;
    }
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == '\\'
            && matches!(
                chars.next(),
                Some('w' | 'W' | 'd' | 'D' | 's' | 'S' | 'b' | 'B' | 'p' | 'P' | 'X')
            )
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn quoting_escapes_what_would_end_the_literal() {
        assert_eq!(quote(r"a'b\c"), r"'a\'b\\c'");
    }

    #[test]
    fn like_escapes_its_own_wildcards() {
        assert_eq!(like_contains("50%_x"), r"'%50\\%\\_x%'");
    }

    #[test]
    fn an_app_matcher_reads_the_sort_key() {
        let m = LabelMatcher::new("app", MatchOp::Equal, "checkout").unwrap();
        assert_eq!(
            matcher("Stream", "ServiceName", &m),
            "ServiceName = 'checkout'"
        );
        let m = LabelMatcher::new("level", MatchOp::Regex, "err.*").unwrap();
        assert_eq!(
            matcher("Stream", "ServiceName", &m),
            "match(Stream['level'], '^(?-s:err.*)$')"
        );
    }

    #[test]
    fn a_pattern_the_engines_read_differently_is_left_to_rust() {
        for pattern in [r"\w+", r"user\d", r"a\sb", r"\bword", "José.*", r"\p{L}+"] {
            let m = LabelMatcher::new("user", MatchOp::Regex, pattern).unwrap();
            assert_eq!(matcher("Stream", "ServiceName", &m), "1", "{pattern}");
            let m = LabelMatcher::new("user", MatchOp::NotRegex, pattern).unwrap();
            assert_eq!(matcher("Stream", "ServiceName", &m), "1", "{pattern}");
        }
        assert!(same_in_both(r"api|shop\.v[0-9]+"));
    }
}
