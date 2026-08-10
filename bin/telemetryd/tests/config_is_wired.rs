//! Every configurable value must be read by something.
//!
//! Five settings in this project were declared, defaulted, validated, documented and
//! mapped to an environment variable — and read by no code at all:
//!
//! - `storage.max_segment_bytes` counted the wrong bytes, so a 256 MiB memory budget
//!   held 6.4 GB
//! - `server.max_body_bytes` was overridden by a framework default, so 16 MiB rejected
//!   anything past 2 MiB
//! - `limits.max_series` and `max_series_per_app` stored 400 series against a cap of 50
//! - `server.shutdown_grace` bounded nothing
//! - the whole `[[scrape]]` section described a feature that did not exist
//!
//! Each was found by running the binary and noticing the number was wrong. That is a
//! poor way to find them, and it only works for settings someone thinks to test.
//!
//! This is a coarse check — a grep, not a proof — and it cannot tell whether a value is
//! used *correctly*. What it can do is fail the moment a field is added with no reader
//! at all, which is the state all five were in.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Fields whose only job is to be reported back, with the place that reports them.
///
/// Anything listed here has to name where it is consumed, so the exemption is a claim
/// someone can check rather than a way to silence the test.
const READ_INDIRECTLY: &[(&str, &str)] = &[
    // Serialised wholesale into `telemetryd validate` output and /status.
    ("log", "the LogConfig struct is passed to logging::init"),
];

/// Every source file of the configuration module, concatenated.
///
/// Reads the directory rather than one file: the schema lives in `schema.rs`, the
/// environment table in `env.rs`, and loading in `mod.rs`. Naming them individually
/// would mean a future split silently narrows what this test covers — which is the
/// opposite of what a completeness test is for.
fn config_source() -> String {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/core/src/config")
        .canonicalize()
        .expect("the config module directory should exist");
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .expect("the config module should be readable")
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "rs"))
        .collect();
    assert!(
        entries.len() >= 2,
        "expected the configuration to be split across several files, found {}",
        entries.len()
    );
    entries.sort();
    entries
        .into_iter()
        .map(|path| std::fs::read_to_string(&path).expect("a config source should be readable"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Public fields of every `*Config` struct.
fn declared_fields() -> Vec<(String, String)> {
    let source = config_source();
    let mut fields = Vec::new();
    let mut current: Option<String> = None;

    for line in source.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("pub struct ") {
            let name = rest.split_whitespace().next().unwrap_or_default();
            current = name.strip_suffix('{').or(Some(name)).map(str::to_owned);
            continue;
        }
        if trimmed == "}" {
            current = None;
            continue;
        }
        if let (Some(struct_name), Some(rest)) = (current.as_ref(), trimmed.strip_prefix("pub ")) {
            if !struct_name.ends_with("Config") {
                continue;
            }
            if let Some(field) = rest.split(':').next()
                && !field.is_empty()
                && field.chars().all(|c| c.is_ascii_lowercase() || c == '_')
            {
                fields.push((struct_name.clone(), field.to_owned()));
            }
        }
    }
    assert!(
        fields.len() > 20,
        "the parser found only {} fields, so it has stopped working rather than the \
         configuration having shrunk",
        fields.len()
    );
    fields
}

/// Every `.rs` file under the workspace, except the one that declares the settings.
///
/// Read here rather than shelled out to `grep`: the first version of this test used
/// `grep --include`, which BusyBox does not support, so it failed on Alpine — the
/// distribution the release binary is actually built on. A test that depends on which
/// grep the host happens to ship is not a test.
fn source_files(root: &Path) -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n == "target") {
                    continue;
                }
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs")
                && !path.ends_with("core/src/config.rs")
            {
                out.push(path);
            }
        }
    }

    let mut files = Vec::new();
    for sub in ["crates", "bin"] {
        walk(&root.join(sub), &mut files);
    }
    assert!(
        files.len() > 20,
        "found only {} source files, so the walk is broken",
        files.len()
    );
    files
}

#[test]
fn every_configured_value_is_read_somewhere() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let exempt: BTreeSet<&str> = READ_INDIRECTLY.iter().map(|(field, _)| *field).collect();

    let sources: Vec<String> = source_files(&root)
        .iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .collect();

    let mut unread = Vec::new();
    for (struct_name, field) in declared_fields() {
        if exempt.contains(field.as_str()) {
            continue;
        }
        let needle = format!(".{field}");
        if !sources.iter().any(|text| text.contains(&needle)) {
            unread.push(format!("{struct_name}.{field}"));
        }
    }

    assert!(
        unread.is_empty(),
        "these settings are declared but read by nothing, so they do not do what the \
         documentation says they do:\n  {}\n\nEither wire them up, or remove them. A \
         setting that silently does nothing is worse than an absent one: someone will \
         tune it and believe it worked.",
        unread.join("\n  ")
    );
}
