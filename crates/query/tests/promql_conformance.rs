//! PromQL answers, held to what Prometheus itself answers.
//!
//! `conformance/promql.json` is a promtool unit-test file: input series in promtool's
//! notation, expressions, and the samples each evaluates to. The expectations were
//! written by Prometheus — `scripts/promql-conformance.py --update` asks it — and CI asks
//! it again on every run, so they cannot drift from what Prometheus says.
//!
//! Every expression is answered here three ways, because telemetryd has three roads to
//! an answer and a bug on any one of them is a wrong chart: the in-memory evaluator, the
//! store read for one instant, and the store read for a range of points — the chart
//! path, which folds instead of loading when the expression allows.

#![allow(clippy::unwrap_used)]

use serde_json::Value as Json;
use telemetryd_core::config::Config;
use telemetryd_core::metric::STALE_MARKER_BITS;
use telemetryd_core::{Labels, MetricKind, MetricSample};
use telemetryd_query::promeval::{Snapshot, Value};
use telemetryd_query::promql;
use telemetryd_store::Store;

const SECOND: u64 = 1_000_000_000;
/// Where promtool's time zero lands. Any instant well clear of the epoch will do; the
/// answers are relative to it.
const BASE: u64 = 1_700_000_000 * SECOND;

#[test]
fn every_expression_answers_as_prometheus_does() {
    let doc: Json = serde_json::from_str(include_str!("conformance/promql.json")).unwrap();
    let test = &doc["tests"][0];
    let interval = duration(test["interval"].as_str().unwrap());
    let samples: Vec<MetricSample> = test["input_series"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|input| {
            expand(
                input["series"].as_str().unwrap(),
                input["values"].as_str().unwrap(),
                interval,
            )
        })
        .collect();

    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = Some(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    store.metrics().append(&samples).unwrap();

    let mut failures = Vec::new();
    let cases = test["promql_expr_test"].as_array().unwrap();
    for case in cases {
        let query = case["expr"].as_str().unwrap();
        let at = BASE + duration(case["eval_time"].as_str().unwrap());
        let expected = expected(&case["exp_samples"]);
        let expr = promql::parse(query).unwrap();

        let mut in_memory = Snapshot::from_samples(samples.clone());
        in_memory.prepare(&expr, &[at]);
        let instant = Snapshot::load_at(store.metrics(), &expr, &[at], 0).unwrap();
        let points: Vec<u64> = (0..20).map(|i| at - (19 - i) * interval).collect();
        let ranged = Snapshot::load_at(store.metrics(), &expr, &points, 0).unwrap();

        for (road, snapshot) in [
            ("in memory", &in_memory),
            ("one instant", &instant),
            ("a range", &ranged),
        ] {
            let got = answer(snapshot.eval(&expr, at).unwrap());
            if let Some(difference) = differs(&expected, &got) {
                failures.push(format!(
                    "{query} at {}, read {road}: {difference}",
                    case["eval_time"].as_str().unwrap()
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} answers differ from Prometheus:\n{}",
        failures.len(),
        cases.len() * 3,
        failures.join("\n")
    );
}

/// promtool's value notation, one sample per step: `a+bxn` is `a`, `a+b`, … `a+nb`;
/// `a-bxn` counts down; `axn` repeats `a`; `_` skips a step; `stale` ends the series.
fn expand(series: &str, values: &str, interval: u64) -> Vec<MetricSample> {
    let labels = parse_labels(series);
    let mut out = Vec::new();
    let mut step = 0u64;
    let mut push = |step: u64, value: f64| {
        out.push(MetricSample {
            series: labels.clone(),
            timestamp_nanos: BASE + step * interval,
            value,
            kind: MetricKind::Counter,
        });
    };
    for token in values.split_whitespace() {
        match token {
            "_" => step += 1,
            "stale" => {
                push(step, f64::from_bits(STALE_MARKER_BITS));
                step += 1;
            }
            _ => {
                let (start, rest) = token
                    .split_once('x')
                    .map_or((token, None), |(a, n)| (a, Some(n)));
                let times: u64 = rest.map_or(0, |n| n.parse().unwrap());
                // The sign that separates start from increment is the last one that
                // does not begin the number.
                let split = start
                    .char_indices()
                    .skip(1)
                    .filter(|(_, c)| *c == '+' || *c == '-')
                    .map(|(i, _)| i)
                    .last();
                let (first, by) = match split {
                    Some(i) => (
                        start[..i].parse::<f64>().unwrap(),
                        start[i..].parse::<f64>().unwrap(),
                    ),
                    None => (start.parse::<f64>().unwrap(), 0.0),
                };
                for n in 0..=times {
                    #[allow(clippy::cast_precision_loss)]
                    push(step, first + by * n as f64);
                    step += 1;
                }
            }
        }
    }
    out
}

/// `name{a="1", b="2"}`, `{a="1"}` or a bare name, as promtool writes them.
fn parse_labels(text: &str) -> Labels {
    let mut labels = Labels::new();
    let (name, rest) = text.split_once('{').map_or((text, ""), |(n, r)| (n, r));
    if !name.trim().is_empty() {
        labels.insert("__name__", name.trim());
    }
    let mut chars = rest.chars().peekable();
    loop {
        while chars.next_if(|c| *c == ',' || c.is_whitespace()).is_some() {}
        let key: String =
            std::iter::from_fn(|| chars.next_if(|c| *c != '=' && *c != '}')).collect();
        if chars.next() != Some('=') {
            break;
        }
        assert_eq!(chars.next(), Some('"'), "a label value is quoted in {text}");
        let mut value = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\\' => value.extend(chars.next()),
                '"' => break,
                _ => value.push(c),
            }
        }
        labels.insert(key.trim(), value);
    }
    labels
}

fn expected(samples: &Json) -> Vec<(Labels, f64)> {
    let mut out: Vec<(Labels, f64)> = samples
        .as_array()
        .unwrap()
        .iter()
        .map(|sample| {
            (
                parse_labels(sample["labels"].as_str().unwrap()),
                sample["value"].as_f64().unwrap(),
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn answer(value: Value) -> Vec<(Labels, f64)> {
    let mut out = match value {
        Value::Scalar(value) => vec![(Labels::new(), value)],
        Value::Vector(vector) => vector.samples,
    };
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn differs(expected: &[(Labels, f64)], got: &[(Labels, f64)]) -> Option<String> {
    let same = expected.len() == got.len()
        && expected.iter().zip(got).all(|((a, x), (b, y))| {
            a == b && (x - y).abs() <= 1e-9 * x.abs().max(y.abs()).max(1.0)
        });
    (!same).then(|| format!("expected {expected:?}, got {got:?}"))
}

fn duration(text: &str) -> u64 {
    let mut total = 0u64;
    let mut number = String::new();
    for c in text.chars() {
        if c.is_ascii_digit() {
            number.push(c);
            continue;
        }
        let unit = match c {
            'h' => 3600,
            'm' => 60,
            's' => 1,
            _ => panic!("unsupported duration {text}"),
        };
        total += number.parse::<u64>().unwrap() * unit * SECOND;
        number.clear();
    }
    total
}
