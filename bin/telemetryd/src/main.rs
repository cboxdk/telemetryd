//! `telemetryd` — a single-binary observability backend for the cboxdk Laravel
//! ecosystem.
//!
//! This crate is deliberately thin: argument parsing, configuration resolution,
//! logging setup and process lifecycle. Everything else lives in the library crates
//! so it can be tested without a socket or a shell.

// The workspace enables `unreachable_pub` for the library crates, where it catches a
// real mistake. A binary has no external consumers, so here it only argues about
// `pub` vs `pub(crate)` on items nothing outside the crate can name either way.
#![allow(unreachable_pub)]

#[macro_use]
mod out;
mod commands;
mod logging;
mod reload;

/// telemetryd does not use the platform allocator.
///
/// This is not a micro-optimisation, it is a correctness-of-shipping decision. The
/// primary target is `*-unknown-linux-musl`, and musl's allocator serialises on a
/// single global lock. Our workload is allocation-heavy by nature — every record is a
/// handful of small `String`s — so under concurrency that lock, not the disk and not
/// our own mutexes, becomes the limit. Measured on musl, over the same 100k-record
/// store:
///
/// | | musl allocator | mimalloc |
/// |---|---|---|
/// | unbounded scan, one thread | 130 ms | 65 ms |
/// | unbounded scan, four threads | 432 ms | 61 ms |
///
/// Four threads being **3.3x slower than one** is the tell: that is not our code
/// scaling badly, it is threads queueing for `malloc`. With mimalloc the same scan
/// gets faster with more threads, as it should.
///
/// It helps on macOS too (72 ms to 57 ms), so this is set for every target rather
/// than only for musl — one allocator everywhere means the benchmarks, the tests and
/// the shipped binary are all measuring the same program.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use telemetryd_core::config::Overrides;

#[derive(Debug, Parser)]
#[command(
    name = "telemetryd",
    version,
    about = "Single-binary observability backend — OTLP in, Loki/Tempo/Prometheus APIs out",
    long_about = "telemetryd stores logs, traces and metrics in one directory \
                  and serves the Loki, Tempo and Prometheus query APIs that \
                  laravel-telemetry-ui speaks.\n\n\
                  Your telemetry never leaves your infrastructure: one binary, one \
                  port, one data directory, no sidecars.",
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the server. Works with no arguments at all.
    Serve(ServeArgs),

    /// Pretty-print the status of a running instance.
    Status(commands::status::StatusArgs),

    /// Run a LogQL query, for debugging over SSH or for exporting.
    Query(commands::query::QueryArgs),

    /// Write a time range out as OTLP, for migration or for a bug report.
    Export(commands::transfer::ExportArgs),

    /// Read a time range in, from another instance or from an export file.
    Import(commands::transfer::ImportArgs),

    /// Check a configuration and show where every value came from.
    Validate(ConfigArgs),

    /// Write a configuration with generated tokens, ready for `service install`.
    Init(commands::init::InitArgs),

    /// Generate and install a service unit for this machine.
    Service {
        #[command(subcommand)]
        action: commands::service::ServiceAction,
    },

    /// Print version and build information.
    Version {
        /// Also ask GitHub whether a newer release exists.
        ///
        /// Never automatic: telemetryd contacts nothing on its own, and this is the
        /// only code path that talks to anything outside your infrastructure.
        #[arg(long)]
        check: bool,
    },

    /// Measure what this machine can ingest and query, for sizing.
    ///
    /// Runs against a temporary directory that is deleted afterwards; it will never
    /// touch a real data directory.
    #[command(hide = true)]
    Bench(commands::bench::BenchArgs),
}

#[derive(Debug, Args)]
struct ServeArgs {
    #[command(flatten)]
    config: ConfigArgs,

    /// Address to listen on. One port serves ingest, query and the UI APIs.
    #[arg(long, value_name = "ADDR")]
    listen: Option<std::net::SocketAddr>,

    /// Allow binding a non-loopback address with no token configured.
    ///
    /// Refused by default: telemetry routinely contains emails, tokens and stack
    /// traces, so an exposed instance without authentication fails closed.
    #[arg(long)]
    insecure: bool,

    /// Log level: trace, debug, info, warn or error.
    #[arg(long, value_name = "LEVEL")]
    log_level: Option<String>,
}

#[derive(Debug, Args, Clone)]
struct ConfigArgs {
    /// Path to telemetryd.toml. Discovered automatically when omitted.
    #[arg(long, short, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Where to store data. Defaults to ./telemetryd-data if it exists, else the
    /// platform data directory.
    #[arg(long, value_name = "DIR")]
    data_dir: Option<PathBuf>,
}

impl ServeArgs {
    fn overrides(&self) -> Overrides {
        Overrides {
            listen: self.listen,
            data_dir: self.config.data_dir.clone(),
            // `--insecure` is a flag, so absence means "not specified" rather than
            // "false" — otherwise it would silently override a config file that set it.
            insecure: self.insecure.then_some(true),
            log_level: self.log_level.clone(),
        }
    }
}

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();

    match run(cli) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            // Startup errors are shown to a human at a terminal, so they are printed
            // plainly rather than as a structured log line. The exposed-bind message
            // in particular is multi-line remediation instructions.
            eprintln!("\nerror: {error}");
            for cause in error.chain().skip(1) {
                eprintln!("  caused by: {cause}");
            }
            std::process::ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Command::Serve(args) => {
            commands::serve::run(args.config.config.as_deref(), &args.overrides())
        }
        Command::Status(args) => commands::status::run(&args),
        Command::Query(args) => commands::query::run(&args),
        Command::Export(args) => commands::transfer::export(&args),
        Command::Import(args) => commands::transfer::import(&args),
        Command::Validate(args) => {
            commands::validate::run(args.config.as_deref(), args.data_dir.as_deref())
        }
        Command::Init(args) => commands::init::run(&args),
        Command::Service { action } => commands::service::run(&action),
        Command::Version { check } => {
            commands::version::run(check);
            Ok(())
        }
        Command::Bench(args) => commands::bench::run(&args),
    }
}
