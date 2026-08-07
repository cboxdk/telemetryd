//! Label matchers, shared by the query languages and by storage pruning.
//!
//! One implementation on purpose: if the matcher that decides "can this segment be
//! skipped?" and the matcher that decides "does this row belong in the result?"
//! disagreed even slightly, queries would silently return incomplete data — the worst
//! possible failure for a telemetry store, because it looks like an answer.

use std::fmt;

use regex::Regex;

use crate::error::{Error, Result};
use crate::record::Labels;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
}

impl MatchOp {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::Regex => "=~",
            Self::NotRegex => "!~",
        }
    }

    pub fn is_negative(self) -> bool {
        matches!(self, Self::NotEqual | Self::NotRegex)
    }
}

#[derive(Clone)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
    /// Compiled once at parse time. A query that recompiles a regex per row is a
    /// performance bug waiting for the first busy afternoon.
    regex: Option<Regex>,
}

impl LabelMatcher {
    pub fn new(name: impl Into<String>, op: MatchOp, value: impl Into<String>) -> Result<Self> {
        let name = name.into();
        let value = value.into();

        let regex = match op {
            MatchOp::Regex | MatchOp::NotRegex => Some(compile_anchored(&value)?),
            _ => None,
        };

        Ok(Self {
            name,
            op,
            value,
            regex,
        })
    }

    pub fn equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op: MatchOp::Equal,
            value: value.into(),
            regex: None,
        }
    }

    /// Test this matcher against a label set.
    ///
    /// An absent label is treated as the empty string, which is what PromQL and LogQL
    /// both specify. It is why `{env!="prod"}` matches a stream with no `env` label at
    /// all, and getting it wrong silently changes what a negative matcher returns.
    pub fn matches(&self, labels: &Labels) -> bool {
        self.matches_value(labels.get(&self.name).unwrap_or(""))
    }

    pub fn matches_value(&self, value: &str) -> bool {
        match self.op {
            MatchOp::Equal => value == self.value,
            MatchOp::NotEqual => value != self.value,
            MatchOp::Regex => self.regex.as_ref().is_some_and(|r| r.is_match(value)),
            MatchOp::NotRegex => !self.regex.as_ref().is_some_and(|r| r.is_match(value)),
        }
    }

    /// Whether this matcher requires the label to be present with a non-empty value.
    ///
    /// Used for pruning: only a matcher that *demands* a value can rule a segment out
    /// on the basis that the segment never saw that value.
    pub fn is_selective(&self) -> bool {
        match self.op {
            MatchOp::Equal => !self.value.is_empty(),
            MatchOp::Regex => !self.matches_value(""),
            MatchOp::NotEqual | MatchOp::NotRegex => false,
        }
    }
}

impl fmt::Debug for LabelMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{:?}", self.name, self.op.as_str(), self.value)
    }
}

impl PartialEq for LabelMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.op == other.op && self.value == other.value
    }
}

impl Eq for LabelMatcher {}

/// Compile a matcher regex with PromQL/LogQL semantics: **fully anchored**.
///
/// `{job=~"api"}` must not match `"api-gateway"`. The non-capturing group matters as
/// much as the anchors — `^a|b$` without it parses as `(^a)|(b$)`, which would match
/// far more than the user wrote.
fn compile_anchored(pattern: &str) -> Result<Regex> {
    Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|e| Error::BadRequest(format!("invalid regular expression {pattern:?}: {e}")))
}

/// Test a whole matcher set.
pub fn matches_all(matchers: &[LabelMatcher], labels: &Labels) -> bool {
    matchers.iter().all(|m| m.matches(labels))
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

    #[test]
    fn equality_and_inequality() {
        let l = labels(&[("app", "checkout"), ("level", "error")]);

        assert!(
            LabelMatcher::new("app", MatchOp::Equal, "checkout")
                .unwrap()
                .matches(&l)
        );
        assert!(
            !LabelMatcher::new("app", MatchOp::Equal, "cart")
                .unwrap()
                .matches(&l)
        );
        assert!(
            LabelMatcher::new("app", MatchOp::NotEqual, "cart")
                .unwrap()
                .matches(&l)
        );
    }

    #[test]
    fn regex_matchers_are_fully_anchored() {
        let l = labels(&[("job", "api-gateway")]);

        // The whole point: =~"api" must not match "api-gateway".
        assert!(
            !LabelMatcher::new("job", MatchOp::Regex, "api")
                .unwrap()
                .matches(&l)
        );
        assert!(
            LabelMatcher::new("job", MatchOp::Regex, "api.*")
                .unwrap()
                .matches(&l)
        );
        assert!(
            LabelMatcher::new("job", MatchOp::Regex, "api-gateway")
                .unwrap()
                .matches(&l)
        );
    }

    #[test]
    fn alternation_is_grouped_before_anchoring() {
        let l = labels(&[("level", "warn")]);

        // Without the non-capturing group this compiles as (^error)|(warn$) and the
        // anchors stop meaning what the user wrote.
        assert!(
            LabelMatcher::new("level", MatchOp::Regex, "error|warn")
                .unwrap()
                .matches(&l)
        );
        assert!(
            !LabelMatcher::new("level", MatchOp::Regex, "error|warning")
                .unwrap()
                .matches(&l)
        );
    }

    #[test]
    fn an_absent_label_is_the_empty_string() {
        let l = labels(&[("app", "checkout")]);

        // This is why {env!="prod"} matches a stream that has no env label at all.
        assert!(
            LabelMatcher::new("env", MatchOp::NotEqual, "prod")
                .unwrap()
                .matches(&l)
        );
        assert!(
            LabelMatcher::new("env", MatchOp::Equal, "")
                .unwrap()
                .matches(&l)
        );
        assert!(
            !LabelMatcher::new("env", MatchOp::Equal, "prod")
                .unwrap()
                .matches(&l)
        );
        assert!(
            LabelMatcher::new("env", MatchOp::Regex, ".*")
                .unwrap()
                .matches(&l)
        );
        assert!(
            !LabelMatcher::new("env", MatchOp::Regex, ".+")
                .unwrap()
                .matches(&l)
        );
    }

    #[test]
    fn negative_regex_matches_when_the_pattern_does_not() {
        let l = labels(&[("level", "info")]);
        assert!(
            LabelMatcher::new("level", MatchOp::NotRegex, "err.*")
                .unwrap()
                .matches(&l)
        );
        assert!(
            !LabelMatcher::new("level", MatchOp::NotRegex, "in.*")
                .unwrap()
                .matches(&l)
        );
    }

    #[test]
    fn selectivity_identifies_matchers_usable_for_pruning() {
        // A matcher that demands a value can rule a segment out.
        assert!(
            LabelMatcher::new("app", MatchOp::Equal, "checkout")
                .unwrap()
                .is_selective()
        );
        assert!(
            LabelMatcher::new("app", MatchOp::Regex, "check.+")
                .unwrap()
                .is_selective()
        );

        // These all match streams lacking the label, so they can never prune.
        assert!(
            !LabelMatcher::new("app", MatchOp::Equal, "")
                .unwrap()
                .is_selective()
        );
        assert!(
            !LabelMatcher::new("app", MatchOp::NotEqual, "x")
                .unwrap()
                .is_selective()
        );
        assert!(
            !LabelMatcher::new("app", MatchOp::Regex, ".*")
                .unwrap()
                .is_selective()
        );
        assert!(
            !LabelMatcher::new("app", MatchOp::NotRegex, "x")
                .unwrap()
                .is_selective()
        );
    }

    #[test]
    fn an_invalid_regex_is_a_clean_client_error() {
        let err = LabelMatcher::new("app", MatchOp::Regex, "[unclosed").unwrap_err();
        assert!(matches!(err, Error::BadRequest(_)));
        assert!(err.to_string().contains("[unclosed"), "{err}");
    }

    #[test]
    fn matches_all_requires_every_matcher() {
        let l = labels(&[("app", "checkout"), ("level", "error")]);
        let all = vec![
            LabelMatcher::equal("app", "checkout"),
            LabelMatcher::equal("level", "error"),
        ];
        assert!(matches_all(&all, &l));

        let with_miss = vec![
            LabelMatcher::equal("app", "checkout"),
            LabelMatcher::equal("level", "info"),
        ];
        assert!(!matches_all(&with_miss, &l));
        assert!(matches_all(&[], &l), "no matchers selects everything");
    }
}
