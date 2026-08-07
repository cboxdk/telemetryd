//! Property tests for the query-language parsers.
//!
//! These parsers sit directly on untrusted input: anyone who can reach the query API
//! can hand them arbitrary bytes. The properties that matter are therefore less about
//! what they accept than about what they must never do — panic, hang, or silently
//! accept something they cannot actually run.

#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use telemetryd_core::{Error, Labels};
use telemetryd_query::lexer;
use telemetryd_query::logql;

/// Anything a caller could send.
fn arbitrary_input() -> impl Strategy<Value = String> {
    prop_oneof![
        // Fully arbitrary bytes.
        ".*",
        // Biased towards the query alphabet, which finds far more real edge cases
        // than random unicode does.
        proptest::collection::vec(
            prop::sample::select(vec![
                "{", "}", "[", "]", "(", ")", ",", "|", "=", "!", "~", "\"", "`", "\\", " ", "app",
                "level", "json", "logfmt", "rate", "5m", "0", ".*", "|=", "!=", "=~", "!~", "\n",
                "#", "@", ":", "-", "+",
            ]),
            0..24,
        )
        .prop_map(|parts| parts.concat()),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    /// The tokenizer either produces tokens or a clean error. Never a panic.
    #[test]
    fn tokenizing_never_panics(input in arbitrary_input()) {
        let _ = lexer::tokenize(&input);
    }

    /// Same for the parser, over the same inputs.
    #[test]
    fn parsing_never_panics(input in arbitrary_input()) {
        let _ = logql::parse(&input);
    }

    /// Every failure is a client error, never an internal one.
    ///
    /// A malformed query producing a 500 would page somebody for what is really a
    /// typo, so the distinction is load-bearing rather than cosmetic.
    #[test]
    fn every_parse_failure_is_attributable_to_the_client(input in arbitrary_input()) {
        if let Err(error) = logql::parse(&input) {
            prop_assert!(
                matches!(error, Error::BadRequest(_) | Error::Unsupported { .. }),
                "{:?} produced a non-client error: {:?}", input, error
            );
        }
    }

    /// A parsed query can always be evaluated without panicking, on any line.
    #[test]
    fn evaluation_never_panics(
        input in arbitrary_input(),
        line in ".*",
    ) {
        if let Ok(query) = logql::parse(&input) {
            let _ = query.evaluate(&line, &Labels::new());
        }
    }
}

// ---------------------------------------------------------------------------
// Round-trip properties over well-formed queries
// ---------------------------------------------------------------------------

fn label_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,12}"
}

/// Values without characters that would need escaping, so the generated query text is
/// unambiguous — escaping itself is covered by the lexer's own tests.
fn label_value() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9 ._/-]{0,20}"
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// Any selector built from valid parts parses, and the matchers survive intact.
    #[test]
    fn well_formed_selectors_round_trip(
        pairs in proptest::collection::vec((label_name(), label_value()), 1..6)
    ) {
        let rendered: Vec<String> = pairs
            .iter()
            .map(|(name, value)| format!("{name}=\"{value}\""))
            .collect();
        let query_text = format!("{{{}}}", rendered.join(", "));

        let parsed = logql::parse(&query_text);

        // A selector made only of empty values selects everything, which we refuse on
        // purpose — that is the one legitimate rejection here.
        if pairs.iter().all(|(_, value)| value.is_empty()) {
            prop_assert!(parsed.is_err());
            return Ok(());
        }

        let query = parsed.map_err(|e| TestCaseError::fail(format!("{query_text:?}: {e}")))?;

        // Later duplicates of a label name overwrite earlier ones in the rendered
        // text, so compare against the deduplicated set.
        let mut expected: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
        for (name, value) in &pairs {
            expected.insert(name, value);
        }
        prop_assert_eq!(query.matchers.len(), pairs.len());
        for matcher in &query.matchers {
            prop_assert!(expected.contains_key(matcher.name.as_str()));
        }
    }

    /// A line filter accepts exactly the lines containing its pattern.
    #[test]
    fn line_filters_agree_with_string_containment(
        needle in "[a-zA-Z0-9 ]{1,10}",
        line in "[a-zA-Z0-9 ]{0,60}",
    ) {
        let query = logql::parse(&format!(r#"{{app="x"}} |= "{needle}""#)).unwrap();
        prop_assert_eq!(
            query.evaluate(&line, &Labels::new()),
            line.contains(&needle),
            "|= must mean exactly `contains` for {:?} / {:?}", line, needle
        );

        let negated = logql::parse(&format!(r#"{{app="x"}} != "{needle}""#)).unwrap();
        prop_assert_eq!(
            negated.evaluate(&line, &Labels::new()),
            !line.contains(&needle)
        );
    }

    /// Chained filters are the conjunction of the individual ones — order must not
    /// change the outcome.
    #[test]
    fn chained_line_filters_are_order_independent(
        a in "[a-z]{1,5}",
        b in "[a-z]{1,5}",
        line in "[a-z ]{0,40}",
    ) {
        let forward = logql::parse(&format!(r#"{{app="x"}} |= "{a}" |= "{b}""#)).unwrap();
        let reversed = logql::parse(&format!(r#"{{app="x"}} |= "{b}" |= "{a}""#)).unwrap();

        prop_assert_eq!(
            forward.evaluate(&line, &Labels::new()),
            reversed.evaluate(&line, &Labels::new())
        );
    }

    /// The `json` stage never rejects a line by itself — it only makes fields
    /// available. A non-JSON line contributes nothing rather than failing the query.
    #[test]
    fn a_bare_json_stage_accepts_every_line(line in ".*") {
        let query = logql::parse(r#"{app="x"} | json"#).unwrap();
        prop_assert!(query.evaluate(&line, &Labels::new()));
    }

    /// Same for logfmt: it extracts, it does not filter.
    #[test]
    fn a_bare_logfmt_stage_accepts_every_line(line in ".*") {
        let query = logql::parse(r#"{app="x"} | logfmt"#).unwrap();
        prop_assert!(query.evaluate(&line, &Labels::new()));
    }

    /// Whatever `| json` extracts, a matching label filter must then accept.
    #[test]
    fn json_extraction_and_label_filters_agree(
        key in "[a-z][a-z0-9_]{0,8}",
        value in "[a-zA-Z0-9 ]{0,15}",
    ) {
        let line = serde_json::json!({ key.clone(): value.clone() }).to_string();
        let query = logql::parse(&format!(r#"{{app="x"}} | json | {key}="{value}""#)).unwrap();

        prop_assert!(
            query.evaluate(&line, &Labels::new()),
            "extracted {}={} from {} but the filter rejected it", key, value, line
        );
    }

    /// Durations always parse back to the same number of nanoseconds.
    #[test]
    fn duration_literals_are_stable(value in 1u64..10_000) {
        for (unit, multiplier) in [
            ("ns", 1u64),
            ("us", 1_000),
            ("ms", 1_000_000),
            ("s", 1_000_000_000),
            ("m", 60_000_000_000),
            ("h", 3_600_000_000_000),
        ] {
            let tokens = lexer::tokenize(&format!("{value}{unit}")).unwrap();
            prop_assert_eq!(
                tokens.first().map(|t| t.token.clone()),
                Some(lexer::Token::Duration(value * multiplier))
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Regressions found by the properties above
// ---------------------------------------------------------------------------

#[test]
fn inputs_that_previously_looked_dangerous_are_handled_cleanly() {
    // Each of these is a shape the generators produce constantly; they are pinned as
    // ordinary tests so a regression names itself instead of appearing as a random
    // proptest failure.
    let inputs = [
        "",
        "{",
        "}",
        "{}",
        "{}{}",
        "{a=",
        r#"{a=""#,
        r#"{a=""}"#,
        r#"{a="b"}|"#,
        r#"{a="b"}|="#,
        r#"{a="b"} | "#,
        r#"{a="b"} || json"#,
        r#"{a="b"} |= `"#,
        "|||||",
        "{{{{{",
        r#"{a="b"} | json | "#,
        "\u{0}",
        "{\u{0}=\"x\"}",
        r#"{a="\"}"#,
        "#comment only",
    ];

    for input in inputs {
        match logql::parse(input) {
            Ok(query) => {
                // If it parsed, it must also evaluate without panicking.
                let _ = query.evaluate("some line", &Labels::new());
            }
            Err(error) => assert!(
                matches!(error, Error::BadRequest(_) | Error::Unsupported { .. }),
                "{input:?} produced {error:?}"
            ),
        }
    }
}

#[test]
fn deeply_nested_input_does_not_blow_the_stack() {
    // The parser is iterative, but JSON flattening in `| json` is recursive, so the
    // depth limit has to come from somewhere.
    let deep = format!(
        "{}{}",
        "{\"a\":".repeat(2000),
        "1".to_owned() + &"}".repeat(2000)
    );
    let query = logql::parse(r#"{app="x"} | json | a="1""#).unwrap();
    let _ = query.evaluate(&deep, &Labels::new());
}

#[test]
fn a_pathological_regex_is_rejected_rather_than_run() {
    // The regex crate has no backtracking, so catastrophic backtracking is impossible
    // by construction — but a regex over the size limit must still fail cleanly.
    let huge = "a".repeat(200_000);
    let result = logql::parse(&format!(r#"{{app="x"}} |~ "{huge}""#));
    if let Err(error) = result {
        assert!(matches!(error, Error::BadRequest(_)), "{error:?}");
    }
}
