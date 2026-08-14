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

    // Read it before removing it: the unit is the only record of which directory the
    // *service* was using, and that is rarely the one the CLI resolves interactively.
    // Saying "the data directory was left alone" without saying where is an answer that
    // sends someone hunting.
    let leftovers = leftovers(&path);

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

    // Nothing else is touched. Removing a service should not delete the telemetry it
    // collected, and a flag that did would be one keystroke from a very bad afternoon.
    // What it owes the operator instead is the paths, so the choice is theirs to make.
    crate::out::outln!();
    if leftovers.is_empty() {
        crate::out::outln!("Nothing else was left behind.");
        return Ok(());
    }
    crate::out::outln!("Left alone, because removing a service should not delete data:");
    crate::out::outln!();
    for (what, path, size) in &leftovers {
        match size {
            Some(size) => crate::out::outln!("  {what:<14} {}  ({size})", path.display()),
            None => crate::out::outln!("  {what:<14} {}", path.display()),
        }
    }
    crate::out::outln!();
    crate::out::outln!("Remove them yourself if you want them gone:");
    for (_, path, _) in &leftovers {
        crate::out::outln!("  sudo rm -rf {}", path.display());
    }
    Ok(())
}

/// What an uninstall deliberately leaves behind, with sizes where they help decide.
///
/// The data directory comes from the unit rather than from the CLI's own resolution: the
/// service runs with `TELEMETRYD_STORAGE_DATA_DIR` set, so what `telemetryd` would pick
/// interactively is a different directory — on the box this was reported from,
/// `/root/.local/share/telemetryd` against the service's `/var/lib/telemetryd`. Printing
/// the wrong one would be worse than printing none.
fn leftovers(unit: &Path) -> Vec<(&'static str, PathBuf, Option<String>)> {
    let mut found = Vec::new();

    let data_dir = std::fs::read_to_string(unit).ok().and_then(|text| {
        text.lines().find_map(|line| {
            line.trim()
                .strip_prefix("Environment=TELEMETRYD_STORAGE_DATA_DIR=")
                .map(|value| PathBuf::from(value.trim()))
        })
    });
    if let Some(dir) = data_dir.filter(|dir| dir.exists()) {
        let size = directory_size(&dir).map(|bytes| bytesize::ByteSize::b(bytes).to_string());
        found.push(("telemetry", dir, size));
    }

    let config_dir = PathBuf::from("/etc/telemetryd");
    if config_dir.exists() {
        // Named separately: these are credentials, and someone deciding what to keep
        // should be told they are still on disk rather than discovering it later.
        found.push(("config, tokens", config_dir, None));
    }
    found
}

/// Bytes under a directory, or `None` if it cannot be walked.
///
/// Best effort and non-recursive beyond what `read_dir` gives directly per level; a
/// number that is roughly right helps someone decide whether they care, and a missing
/// one is not worth failing an uninstall over.
fn directory_size(root: &Path) -> Option<u64> {
    let mut total = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)
            .ok()?
            .filter_map(std::result::Result::ok)
        {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    Some(total)
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

# Retention, the disk budget and the log level change in place on SIGHUP. Without
# this line `systemctl reload` fails, and the obvious next move is `restart` — which
# drains and replays the WAL to apply a number that never needed a restart.
ExecReload=/bin/kill -HUP $MAINPID

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

/// Directives this version's unit has that the installed one does not.
///
/// # Why this is checked at all
///
/// Upgrading telemetryd replaces the binary and nothing else. The unit file was written
/// once, by whichever version happened to be installed that day, and it stays exactly as
/// it was — so a directive added later never reaches the machine. That is not
/// theoretical: `ExecReload` arrived in 0.34.0, and a box running 0.44.0 answered
/// `systemctl reload` with "Job type reload is not applicable for unit
/// telemetryd.service", because its unit predated the line. Nothing anywhere said why.
///
/// Only directive *names* are compared, and only in one direction. Values cannot be
/// compared: `ExecStart` embeds the path this binary was invoked from, `User=` is an
/// install-time choice, and the `.deb`'s unit legitimately differs from a
/// `service install` one in both. A name present in the template and absent from disk is
/// the narrow thing that actually means "written by an older version", and it is the
/// only thing reported.
pub fn missing_directives() -> Vec<String> {
    let Ok(path) = unit_path() else {
        return Vec::new();
    };
    let Ok(installed) = std::fs::read_to_string(&path) else {
        // No unit is not a stale unit. Plenty of instances run under something else, or
        // in the foreground.
        return Vec::new();
    };
    // The account only reaches `User=`/`Group=`, which are excluded below, so any value
    // renders the same set of names.
    let Ok(current) = unit_for_this_platform("telemetryd") else {
        return Vec::new();
    };

    missing_from(&installed, &current)
}

fn missing_from(installed: &str, current: &str) -> Vec<String> {
    let on_disk: std::collections::HashSet<&str> = directives(installed).collect();
    directives(current)
        .filter(|key| !on_disk.contains(key))
        // Written by the operator's choice rather than by the template, and absent from
        // a launchd plist entirely.
        .filter(|key| !matches!(*key, "User" | "Group"))
        .map(str::to_owned)
        .collect()
}

/// The left-hand sides of a unit file, ignoring comments and section headers.
///
/// A launchd plist is XML rather than `key=value`, so this yields nothing for one — and
/// yielding nothing is the correct answer there, not a bug to work around. macOS runs
/// this as a user agent that is reinstalled by hand, and inventing a plist differ to
/// support it would be more code than the problem is worth.
///
/// The identifier filter is what makes that true. Without it a plist yields two
/// "directives" — `<?xml version` and `<plist version`, both of which contain an `=` —
/// which is harmless while both sides are plists and nonsense the moment anything else
/// is compared. A systemd directive is one word; nothing else counts as one.
fn directives(unit: &str) -> impl Iterator<Item = &str> {
    unit.lines()
        .map(str::trim)
        .filter(|line| !line.starts_with('#') && !line.starts_with('['))
        .filter_map(|line| line.split_once('='))
        .map(|(key, _)| key.trim())
        .filter(|key| {
            !key.is_empty()
                && key
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        })
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

    /// The case this was built for: a unit written before `ExecReload` existed, which is
    /// what a box that has only ever had its *binary* upgraded is running.
    #[test]
    fn a_unit_written_by_an_older_version_is_named_directive_by_directive() {
        let current = systemd_unit("/usr/bin/telemetryd", "telemetryd");
        let older: String = current
            .lines()
            .filter(|line| !line.starts_with("ExecReload="))
            .collect::<Vec<_>>()
            .join("\n");

        assert_eq!(missing_from(&older, &current), vec!["ExecReload"]);
    }

    /// The same unit installed under a different account, or from the `.deb` with its own
    /// `ExecStart` path, is not stale — and saying it is would train people to ignore the
    /// warning that matters.
    #[test]
    fn a_current_unit_is_not_stale_whatever_its_paths_and_account_say() {
        let current = systemd_unit("/usr/bin/telemetryd", "telemetryd");
        let installed = systemd_unit("/usr/local/bin/telemetryd", "forge");

        assert!(missing_from(&installed, &current).is_empty());
        assert!(missing_from(&current, &current).is_empty());
    }

    /// A plist yields no directives at all, so macOS reports nothing rather than
    /// reporting every line of the systemd template as missing.
    #[test]
    fn a_launchd_plist_is_compared_against_nothing() {
        let plist = launchd_plist("/usr/local/bin/telemetryd");
        assert_eq!(directives(&plist).count(), 0);
        assert!(missing_from(&plist, &plist).is_empty());
    }

    /// The report has to name the directory the *service* used, which is not the one
    /// the CLI resolves interactively — `/var/lib/telemetryd` against
    /// `/root/.local/share/telemetryd` on the box this was reported from.
    #[test]
    fn the_data_directory_is_read_from_the_unit_being_removed() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("var-lib-telemetryd");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("wal"), vec![0u8; 4096]).unwrap();

        let unit = dir.path().join("telemetryd.service");
        std::fs::write(
            &unit,
            format!(
                "[Service]\nStateDirectory=telemetryd\n\
                 Environment=TELEMETRYD_STORAGE_DATA_DIR={}\n",
                data.display()
            ),
        )
        .unwrap();

        let found = leftovers(&unit);
        let telemetry = found
            .iter()
            .find(|(what, _, _)| *what == "telemetry")
            .expect("the data directory should be reported");
        assert_eq!(telemetry.1, data);
        assert!(
            telemetry
                .2
                .as_deref()
                .is_some_and(|size| size.contains('4')),
            "expected a size near 4 KiB, got {:?}",
            telemetry.2
        );
    }

    /// A unit that never recorded one, or a directory already gone, must not invent a
    /// path — sending someone to `rm -rf` somewhere that was never ours is the one
    /// genuinely dangerous thing this could do.
    #[test]
    fn a_missing_directory_is_not_guessed() {
        let dir = tempfile::tempdir().unwrap();
        let unit = dir.path().join("telemetryd.service");
        std::fs::write(
            &unit,
            "[Service]\nExecStart=/usr/local/bin/telemetryd serve\n",
        )
        .unwrap();
        assert!(
            !leftovers(&unit)
                .iter()
                .any(|(what, _, _)| *what == "telemetry"),
            "no data directory in the unit means none is reported"
        );

        std::fs::write(
            &unit,
            "[Service]\nEnvironment=TELEMETRYD_STORAGE_DATA_DIR=/nonexistent/telemetryd\n",
        )
        .unwrap();
        assert!(
            !leftovers(&unit)
                .iter()
                .any(|(what, _, _)| *what == "telemetry"),
            "a directory that is not there is not left behind"
        );
    }

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
            // Without this, `systemctl reload` fails and the operator reaches for
            // `restart` — a full drain and WAL replay to change a retention window.
            assert!(unit.contains("ExecReload=/bin/kill -HUP $MAINPID"));
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
