//! Where a PromQL query's time goes, stage by stage, against a real data directory:
//! `cargo run --release -p telemetryd-query --example query_timing -- <data-dir> <query>
//! <start-secs> <end-secs> <step-secs>`. An instant query passes the same time as start
//! and end. The directory is opened, so no server may be running on it.
//!
//! Prints the median of `ROUNDS` runs (default 15) for: parsing; the store handing out
//! the samples alone, to a visitor that does nothing; loading the snapshot, which is that
//! read plus folding; evaluating every step; and encoding the answer as JSON.

#![allow(clippy::unwrap_used, clippy::cast_precision_loss)]

use std::time::{Duration, Instant};

use telemetryd_core::config::Config;
use telemetryd_query::prometheus::{RangeParams, range};
use telemetryd_query::promeval::Snapshot;
use telemetryd_store::Store;

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("a data directory");
    let query = args.next().expect("a query");
    let start: u64 = args
        .next()
        .and_then(|n| n.parse().ok())
        .expect("start seconds");
    let end: u64 = args
        .next()
        .and_then(|n| n.parse().ok())
        .expect("end seconds");
    let step: u64 = args.next().and_then(|n| n.parse().ok()).unwrap_or(60);
    let rounds: usize = std::env::var("ROUNDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(15);

    let mut config = Config::default();
    config.storage.data_dir = Some(dir.into());
    let opened = Instant::now();
    let store = Store::open(&config).unwrap();
    println!("open {:?}", opened.elapsed());
    let metrics = store.metrics();

    let points = telemetryd_query::prometheus::step_grid(
        start * 1_000_000_000,
        end * 1_000_000_000,
        step.max(1) * 1_000_000_000,
    )
    .unwrap();
    let (mut parse, mut scan, mut load, mut eval, mut encode, mut whole) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let mut scanned = 0usize;
    for _ in 0..rounds {
        let at = Instant::now();
        let expr = telemetryd_query::promql::parse(&query).unwrap();
        parse.push(at.elapsed());

        let windows: Vec<(u64, u64)> = vec![(
            points[0].saturating_sub(u64::try_from(expr.required_lookback().as_nanos()).unwrap()),
            *points.last().unwrap(),
        )];
        let matchers: Vec<_> = expr
            .selectors()
            .first()
            .map(|selector| selector.matchers.clone())
            .unwrap_or_default();
        let at = Instant::now();
        scanned = 0;
        metrics
            .scan_series(&windows, &matchers, &mut |run| {
                scanned += run.timestamps.len();
                std::ops::ControlFlow::Continue(())
            })
            .unwrap();
        scan.push(at.elapsed());

        let at = Instant::now();
        let mut snapshot = Snapshot::load_at(metrics, &expr, &points, 0).unwrap();
        snapshot.prepare(&expr, &points);
        load.push(at.elapsed());

        let at = Instant::now();
        for point in &points {
            std::hint::black_box(snapshot.eval(&expr, *point).unwrap());
        }
        eval.push(at.elapsed());

        let params = RangeParams {
            query: Some(query.clone()),
            start: Some(start.to_string()),
            end: Some(end.to_string()),
            step: Some(step.to_string()),
            timeout: None,
        };
        let at = Instant::now();
        let answered = range(metrics, &params, 0, 0).unwrap();
        whole.push(at.elapsed());
        let at = Instant::now();
        std::hint::black_box(serde_json::to_vec(&answered).unwrap());
        encode.push(at.elapsed());
    }
    println!(
        "samples read {scanned}\nparse {:?}\nstore read alone {:?}\nload (read + fold) {:?}\neval {} points {:?}\nencode {:?}\nthe whole range handler, before encoding {:?}",
        median(parse),
        median(scan),
        median(load),
        points.len(),
        median(eval),
        median(encode),
        median(whole)
    );
}
