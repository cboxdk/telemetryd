//! The shipped example configuration claims that every value in it is the default.
//!
//! Documentation that drifts from the code is worse than no documentation, and this
//! particular file is what a new user copies. So the claim is a test.

#![allow(clippy::unwrap_used)]

use std::path::PathBuf;

use telemetryd_core::Config;

fn example_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../telemetryd.toml.example")
}

#[test]
fn the_example_config_parses() {
    let text = std::fs::read_to_string(example_path()).unwrap();
    let config: Config = toml::from_str(&text)
        .unwrap_or_else(|e| panic!("telemetryd.toml.example does not parse: {e}"));
    config
        .validate()
        .unwrap_or_else(|e| panic!("telemetryd.toml.example does not validate: {e}"));
}

#[test]
fn every_uncommented_value_in_the_example_is_the_documented_default() {
    let text = std::fs::read_to_string(example_path()).unwrap();
    let from_example: Config = toml::from_str(&text).unwrap();

    let example = serde_json::to_value(&from_example).unwrap();
    let defaults = serde_json::to_value(Config::default()).unwrap();

    assert_eq!(
        example, defaults,
        "telemetryd.toml.example says these are the defaults, but they no longer are. \
         Update the example (or the default) so the file a new user copies stays true."
    );
}

#[test]
fn the_example_covers_every_configurable_section() {
    let text = std::fs::read_to_string(example_path()).unwrap();
    for section in [
        "[server]",
        "[auth]",
        "[storage]",
        "[retention]",
        "[limits]",
        "[log]",
    ] {
        assert!(
            text.contains(section),
            "{section} is missing from telemetryd.toml.example"
        );
    }
    // And nothing that does not exist. `[[scrape]]` was documented here, with
    // validation behind it, and was read by no code at all — an example file is where
    // people learn what a tool can do, so a section here is a promise.
    assert!(
        !text.contains("[[scrape]]"),
        "the example advertises a scrape feature that telemetryd does not have"
    );
}
