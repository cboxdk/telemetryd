//! `telemetryd validate`
//!
//! Type-checks the configuration, runs the cross-field rules, and prints every
//! resolved value with the layer it came from. The provenance is the point — "is this
//! setting actually taking effect?" is the question a config check exists to answer,
//! and a plain syntax check does not answer it.

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::Value;
use telemetryd_core::Config;
use telemetryd_core::config::{Overrides, env_var_path};

/// Where a resolved value came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Default,
    File,
    Env,
    Flag,
}

impl Origin {
    fn label(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::File => "file",
            Self::Env => "env",
            Self::Flag => "flag",
        }
    }
}

pub fn run(config_file: Option<&Path>, data_dir: Option<&Path>) -> anyhow::Result<()> {
    let overrides = Overrides {
        data_dir: data_dir.map(Path::to_path_buf),
        ..Overrides::default()
    };
    let loaded = Config::load(config_file, &overrides)?;

    let resolved = flatten(&serde_json::to_value(&loaded.config)?);
    let defaults = flatten(&serde_json::to_value(Config::default())?);

    let env_paths: Vec<&str> = loaded
        .env_overrides
        .iter()
        .filter_map(|v| env_var_path(v))
        .collect();

    println!("Configuration is valid.\n");

    match &loaded.config_file {
        Some(path) => println!("  config file  {}", path.display()),
        None => println!("  config file  (none found — using defaults)"),
    }
    let data_dir = loaded.config.storage.resolve_data_dir();
    println!("  data dir     {}", data_dir.display());
    println!(
        "               {}",
        if data_dir.exists() {
            "exists"
        } else {
            "will be created on first run"
        }
    );
    println!();

    let width = resolved.keys().map(String::len).max().unwrap_or(0);
    for (path, value) in &resolved {
        let origin = if loaded.flag_overrides.contains(&path.as_str()) {
            Origin::Flag
        } else if env_paths.contains(&path.as_str()) {
            Origin::Env
        } else if defaults.get(path) != Some(value) {
            // Not from a flag or the environment, yet not the default — so the file
            // set it. Derived by comparison rather than tracked, which keeps this
            // honest even for values a provider rewrote.
            Origin::File
        } else {
            Origin::Default
        };
        println!("  {path:<width$}  {value:<24}  ({})", origin.label());
    }

    if !loaded.warnings.is_empty() {
        println!("\nWarnings:");
        for warning in &loaded.warnings {
            println!("  ! {warning}");
        }
    }

    print_security_posture(&loaded.config);
    Ok(())
}

/// Say plainly whether this configuration would expose telemetry to the network.
/// Everything here is already enforced at startup by ADR-004; restating it means an
/// operator does not have to infer it from a listen address and two token fields.
fn print_security_posture(config: &Config) {
    let exposed = !config.server.listen.ip().is_loopback();
    let ingest = if config.auth.ingest_token.is_empty() {
        "open"
    } else {
        "token required"
    };
    let query = if config.auth.query_token.is_empty() {
        "open"
    } else {
        "token required"
    };

    println!("\nSecurity:");
    println!(
        "  listen       {} ({})",
        config.server.listen,
        if exposed {
            "reachable from the network"
        } else {
            "loopback only"
        }
    );
    println!("  ingest       {ingest}");
    println!("  query        {query}");

    if exposed && config.server.insecure {
        println!(
            "\n  ! --insecure is set on a network-reachable address. Anyone who can reach\n  \
             ! {} can read and write your telemetry.",
            config.server.listen
        );
    } else if !exposed {
        println!("\n  Loopback only: no token needed. Put a reverse proxy in front to expose it.");
    }
}

/// Flatten nested JSON into dotted paths, so the resolved config and the defaults can
/// be compared key by key.
fn flatten(value: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    walk(value, &mut String::new(), &mut out);
    out
}

fn walk(value: &Value, path: &mut String, out: &mut BTreeMap<String, String>) {
    use std::fmt::Write as _;

    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let len = path.len();
                if !path.is_empty() {
                    path.push('.');
                }
                path.push_str(key);
                walk(child, path, out);
                path.truncate(len);
            }
        }
        Value::Array(items) if items.is_empty() => {
            out.insert(path.clone(), "[]".to_owned());
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let len = path.len();
                let _ = write!(path, "[{index}]");
                walk(child, path, out);
                path.truncate(len);
            }
        }
        Value::String(s) => {
            out.insert(path.clone(), s.clone());
        }
        other => {
            out.insert(path.clone(), other.to_string());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn flatten_produces_dotted_paths_for_nested_values() {
        let value = serde_json::json!({
            "server": { "listen": "127.0.0.1:4319", "insecure": false },
            "scrape": [],
        });
        let flat = flatten(&value);
        assert_eq!(flat.get("server.listen").unwrap(), "127.0.0.1:4319");
        assert_eq!(flat.get("server.insecure").unwrap(), "false");
        assert_eq!(flat.get("scrape").unwrap(), "[]");
    }

    #[test]
    fn the_default_config_flattens_without_leaking_tokens() {
        let flat = flatten(&serde_json::to_value(Config::default()).unwrap());
        assert_eq!(
            flat.get("auth.ingest_token").map(String::as_str),
            Some("[]")
        );
        assert_eq!(
            flat.get("storage.disk_budget").map(String::as_str),
            Some("10.0 GiB")
        );
        assert_eq!(
            flat.get("retention.metrics").map(String::as_str),
            Some("30days")
        );
    }
}
