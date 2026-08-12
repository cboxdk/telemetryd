//! `telemetryd service`
//!
//! Generates and installs a service unit for this machine.
//!
//! `install` writes outside the data directory and invokes `systemctl`/`launchctl`, so
//! it says exactly what it is about to do and refuses rather than guessing when it
//! cannot. It never invokes `sudo` on the operator's behalf: a tool that silently
//! escalates is a tool you cannot reason about, so when a path is not writable it
//! prints the command to run instead.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum ServiceAction {
    /// Print a service unit for this platform to stdout.
    Print {
        #[command(flatten)]
        account: Account,
    },
    /// Install and enable the service unit.
    Install {
        #[command(flatten)]
        account: Account,
    },
    /// Stop and remove the service unit.
    Uninstall,
}

/// Which account the unit runs as.
///
/// A parameter rather than a constant because the unit used to hardcode
/// `User=telemetryd`, which decides on the operator's behalf. On a Forge box `forge`
/// owns everything; somewhere with central user management a local account is the wrong
/// answer entirely; in a container root is fine. The default is unchanged, so nobody who
/// does not care has to choose.
#[derive(Debug, Clone, clap::Args)]
pub struct Account {
    /// The system user the service runs as.
    ///
    /// Created for you only when it is the default and does not exist. A user you name
    /// yourself is never created — an unknown one is a typo, and inventing an account
    /// from a misspelling is worse than saying so.
    #[arg(long, default_value = DEFAULT_USER)]
    pub user: String,
}

/// The account the unit runs as unless told otherwise.
const DEFAULT_USER: &str = "telemetryd";

pub fn run(action: &ServiceAction) -> anyhow::Result<()> {
    match action {
        ServiceAction::Print { account } => {
            print!("{}", unit_for_this_platform(&account.user)?);
            Ok(())
        }
        ServiceAction::Install { account } => install(&account.user),
        ServiceAction::Uninstall => uninstall(),
    }
}

/// Where the unit belongs on this platform, and what manages it.
fn unit_path() -> anyhow::Result<PathBuf> {
    if cfg!(target_os = "macos") {
        let home = std::env::var("HOME").context("HOME is not set")?;
        // A LaunchAgent rather than a LaunchDaemon: it runs as the user, needs no root,
        // and a developer laptop is the case this actually serves.
        Ok(PathBuf::from(home).join("Library/LaunchAgents/dk.cbox.telemetryd.plist"))
    } else if cfg!(target_os = "linux") {
        Ok(PathBuf::from("/etc/systemd/system/telemetryd.service"))
    } else {
        bail!("no service manager is supported on this platform")
    }
}

fn install(user: &str) -> anyhow::Result<()> {
    let unit = unit_for_this_platform(user)?;
    let path = unit_path()?;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "creating {}\n\nIf this needs root, run:\n{}",
                parent.display(),
                manual_steps()
            )
        })?;
    }

    if path.exists() {
        crate::out::outln!("replacing the existing unit at {}", path.display());
    }

    std::fs::write(&path, &unit).with_context(|| {
        format!(
            "writing {}\n\n\
             telemetryd does not invoke sudo on your behalf. If this needs root, run:\n{}",
            path.display(),
            manual_steps()
        )
    })?;
    crate::out::outln!("wrote {}", path.display());

    #[cfg(not(target_os = "macos"))]
    {
        ensure_service_user(user)?;
        grant_token_access(user);
    }

    enable(&path)?;
    confirm_running()?;

    crate::out::outln!();
    crate::out::outln!("telemetryd is running and will start on boot.");
    crate::out::outln!("Check it with: telemetryd status");
    Ok(())
}

/// Create the account the unit runs as, if it is not already there.
///
/// The unit says `User=telemetryd` and nothing created that user, so `systemctl start`
/// failed with `217/USER` on a real server — while this command had already printed
/// "enabled and started". `useradd` was named only in the *manual* instructions, which
/// the automatic path never shows.
#[cfg(not(target_os = "macos"))]
fn ensure_service_user(user: &str) -> anyhow::Result<()> {
    if exec("getent", &["passwd", user]).is_ok() {
        return Ok(());
    }
    // Only the default is created. A name the operator typed and that does not exist is
    // a typo, and conjuring an account from a misspelling is worse than refusing.
    if user != DEFAULT_USER {
        bail!(
            "the user `{user}` does not exist.\n\
             Create it, or pass --user with an account that does. Only the default \
             (`{DEFAULT_USER}`) is created automatically."
        );
    }
    // `--system` so it gets no login, no home and a reserved uid. Failing here is fatal:
    // enabling a unit that cannot start is what produced the confusing report.
    exec(
        "useradd",
        &[
            "--system",
            "--no-create-home",
            "--shell",
            "/usr/sbin/nologin",
            user,
        ],
    )
    .context(
        "could not create the `telemetryd` system user, which the unit runs as.\n\
         Create it and re-run:\n  \
         sudo useradd --system --no-create-home --shell /usr/sbin/nologin telemetryd",
    )?;
    crate::out::outln!("created the {user} system user");
    Ok(())
}

/// Hand the token files to the account that has to read them.
///
/// `telemetryd init` writes them `0600` as whoever ran it — root, in the documented
/// order — and the service runs as someone else. Best effort: a deployment that keeps
/// its tokens somewhere else entirely is not wrong, and this should not fail the install
/// over files it does not own.
#[cfg(not(target_os = "macos"))]
fn grant_token_access(user: &str) {
    let Ok(entries) = std::fs::read_dir("/etc/telemetryd") else {
        return;
    };
    let mut granted = 0;
    for path in entries
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "token"))
    {
        if exec("chown", &[user, &path.display().to_string()]).is_ok() {
            granted += 1;
        }
    }
    if granted > 0 {
        crate::out::outln!("gave {user} access to {granted} token file(s)");
    }
}

/// Say it is running only once it is.
///
/// `systemctl enable --now` returns success when the unit is *enabled*, which it does
/// even when the start behind it failed — so this command reported "enabled and started"
/// against a dead service and sent someone to `journalctl` to find out why.
fn confirm_running() -> anyhow::Result<()> {
    if cfg!(target_os = "macos") {
        return Ok(());
    }
    // A moment for a failing unit to exit, so this reads the settled state rather than
    // catching it mid-activation and calling that success.
    std::thread::sleep(std::time::Duration::from_millis(1500));
    if exec("systemctl", &["is-active", "--quiet", "telemetryd"]).is_ok() {
        return Ok(());
    }
    let detail = std::process::Command::new("systemctl")
        .args(["status", "telemetryd", "--no-pager", "--lines", "20"])
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_default();
    bail!(
        "the unit was installed but is not running.\n\n{detail}\n\n\
         `journalctl -xeu telemetryd` has the rest. Common causes: a token file the \
         `telemetryd` user cannot read, or a configuration error — `telemetryd validate` \
         reports those before systemd does."
    );
}

fn enable(path: &Path) -> anyhow::Result<()> {
    if cfg!(target_os = "macos") {
        // `bootstrap` is the modern spelling; `load` is kept for older systems, and
        // either failing is reported rather than swallowed.
        exec("launchctl", &["unload", &path.display().to_string()]).ok();
        exec("launchctl", &["load", "-w", &path.display().to_string()])
            .context("launchctl load failed")?;
        crate::out::outln!("loaded the LaunchAgent");
    } else {
        exec("systemctl", &["daemon-reload"]).context("systemctl daemon-reload failed")?;
        exec("systemctl", &["enable", "--now", "telemetryd"])
            .context("systemctl enable --now telemetryd failed")?;
        crate::out::outln!("enabled and started the systemd unit");
    }
    Ok(())
}

fn uninstall() -> anyhow::Result<()> {
    let path = unit_path()?;

    if cfg!(target_os = "macos") {
        exec("launchctl", &["unload", &path.display().to_string()]).ok();
    } else {
        exec("systemctl", &["disable", "--now", "telemetryd"]).ok();
    }

    match std::fs::remove_file(&path) {
        Ok(()) => crate::out::outln!("removed {}", path.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            crate::out::outln!("no unit installed at {}", path.display());
        }
        Err(e) => {
            bail!(
                "could not remove {}: {e}\n\n\
                 telemetryd does not invoke sudo on your behalf; run:\n  sudo rm {}",
                path.display(),
                path.display()
            );
        }
    }

    if cfg!(target_os = "linux") {
        exec("systemctl", &["daemon-reload"]).ok();
    }

    // The data directory is never touched. Removing a service should not delete the
    // telemetry it collected, and a flag that did would be one keystroke from a very
    // bad afternoon.
    crate::out::outln!();
    crate::out::outln!(
        "The data directory was left alone. Remove it yourself if you want it gone."
    );
    Ok(())
}

/// Run a service-manager command, surfacing its stderr rather than swallowing it.
fn exec(program: &str, args: &[&str]) -> anyhow::Result<()> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("running {program}"))?;

    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    bail!("{program} {}: {}", args.join(" "), stderr.trim())
}

fn unit_for_this_platform(user: &str) -> anyhow::Result<String> {
    let exe = std::env::current_exe().map_or_else(
        |_| "/usr/local/bin/telemetryd".to_owned(),
        |p| p.display().to_string(),
    );

    Ok(if cfg!(target_os = "macos") {
        launchd_plist(&exe)
    } else if cfg!(target_os = "linux") {
        systemd_unit(&exe, user)
    } else {
        bail!("no service unit template for this platform");
    })
}

/// Where the `.deb` installs the binary, and therefore the `ExecStart` in the unit
/// that ships inside it.
///
/// The packaged unit is a checked-in file rather than something the release job
/// produces by running the binary it just built. That approach shipped a broken
/// package: `service print` embeds `current_exe()`, so the unit generated on a build
/// runner pointed at the runner's `target/` directory — a path that does not exist on
/// the machine installing the `.deb`. It also only worked at all for the architecture
/// that happened to match the runner, leaving the other one to a different code path.
/// Test-only: the packaging path is a fact about the `.deb`, and the only thing that
/// needs it at compile time is the check that the shipped unit still matches.
#[cfg(test)]
const DEB_EXEC_PATH: &str = "/usr/bin/telemetryd";

/// A deliberately locked-down unit. telemetryd needs one directory and one socket, so
/// everything else is denied — a telemetry backend is an attractive target precisely
/// because everything sends it data.
fn systemd_unit(exe: &str, user: &str) -> String {
    format!(
        "[Unit]
Description=telemetryd — observability backend
Documentation=https://github.com/cboxdk/telemetryd
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
ExecStart={exe} serve --config /etc/telemetryd/telemetryd.toml
Restart=on-failure
RestartSec=5s

User={user}
Group={user}
StateDirectory=telemetryd
Environment=TELEMETRYD_STORAGE_DATA_DIR=/var/lib/telemetryd

# SIGTERM triggers a graceful drain, then a final WAL flush. Give it room to
# finish: killing it early costs the unsynced window.
KillSignal=SIGTERM
TimeoutStopSec=30s

NoNewPrivileges=true
PrivateTmp=true
PrivateDevices=true
ProtectSystem=strict
ProtectHome=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_INET AF_INET6
RestrictNamespaces=true
LockPersonality=true
MemoryDenyWriteExecute=true
SystemCallFilter=@system-service
SystemCallArchitectures=native

[Install]
WantedBy=multi-user.target
"
    )
}

fn launchd_plist(exe: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>dk.cbox.telemetryd</string>
  <key>ProgramArguments</key>
  <array>
    <string>{exe}</string>
    <string>serve</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>StandardOutPath</key>
  <string>/usr/local/var/log/telemetryd.log</string>
  <key>StandardErrorPath</key>
  <string>/usr/local/var/log/telemetryd.log</string>
</dict>
</plist>
"#
    )
}

fn manual_steps() -> &'static str {
    if cfg!(target_os = "macos") {
        "  telemetryd service print > ~/Library/LaunchAgents/dk.cbox.telemetryd.plist\n  \
         launchctl load ~/Library/LaunchAgents/dk.cbox.telemetryd.plist"
    } else {
        "  telemetryd service print | sudo tee /etc/systemd/system/telemetryd.service\n  \
         sudo useradd --system --no-create-home telemetryd\n  \
         sudo systemctl daemon-reload\n  \
         sudo systemctl enable --now telemetryd"
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The unit used to hardcode `User=telemetryd`, which decided for the operator.
    /// On a Forge box `forge` owns everything; somewhere with central user management a
    /// local account is the wrong answer entirely.
    #[test]
    fn the_account_is_whatever_was_asked_for() {
        let default = systemd_unit("/usr/local/bin/telemetryd", DEFAULT_USER);
        assert!(default.contains("User=telemetryd"));
        assert!(default.contains("Group=telemetryd"));

        let forge = systemd_unit("/usr/local/bin/telemetryd", "forge");
        assert!(forge.contains("User=forge"), "{forge}");
        assert!(forge.contains("Group=forge"));
        assert!(
            !forge.contains("User=telemetryd"),
            "the default leaked into a unit that asked for another account"
        );
    }

    /// The `.deb` ships `packaging/telemetryd.service` verbatim. Nothing at release
    /// time regenerates it, so without this test the packaged unit could drift from
    /// the hardening the code applies and nobody would find out until a deployed
    /// service was running with fewer restrictions than we document.
    #[test]
    fn the_packaged_unit_matches_what_the_code_generates() {
        let packaged = include_str!("../../../../packaging/telemetryd.service");
        assert_eq!(
            packaged,
            systemd_unit(DEB_EXEC_PATH, DEFAULT_USER),
            "packaging/telemetryd.service is stale — regenerate it with \
             `telemetryd service print` and set ExecStart to {DEB_EXEC_PATH}"
        );
    }

    #[test]
    fn the_generated_unit_is_for_this_platform_and_is_hardened() {
        let unit = unit_for_this_platform(DEFAULT_USER).unwrap();

        if cfg!(target_os = "macos") {
            assert!(unit.contains("dk.cbox.telemetryd"));
            assert!(unit.contains("<key>RunAtLoad</key>"));
        } else {
            assert!(unit.contains("[Unit]"));
            assert!(unit.contains("KillSignal=SIGTERM"));
            // A stop timeout shorter than the drain would cost the unsynced WAL window.
            assert!(unit.contains("TimeoutStopSec="));
            assert!(unit.contains("ProtectSystem=strict"));
            assert!(unit.contains("NoNewPrivileges=true"));
        }
    }

    #[test]
    fn the_unit_path_is_the_platform_convention() {
        // Deliberately does not call `install()`. An earlier version of this test did,
        // and installing a real LaunchAgent on whoever ran `cargo test` is not a test
        // result, it is a side effect.
        let path = unit_path().unwrap();

        if cfg!(target_os = "macos") {
            assert!(path.ends_with("Library/LaunchAgents/dk.cbox.telemetryd.plist"));
            assert!(path.is_absolute());
        } else {
            assert_eq!(
                path,
                std::path::Path::new("/etc/systemd/system/telemetryd.service")
            );
        }
    }

    #[test]
    fn the_manual_steps_match_the_platform() {
        let steps = manual_steps();
        assert!(steps.contains("telemetryd service print"));
        if cfg!(target_os = "macos") {
            assert!(steps.contains("launchctl"), "{steps}");
        } else {
            assert!(steps.contains("systemctl"), "{steps}");
        }
    }
}
