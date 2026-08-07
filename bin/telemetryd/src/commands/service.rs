//! `telemetryd service`
//!
//! Installing a unit means writing outside the data directory and invoking
//! `systemctl`/`launchctl`, which lands with the packaging work in M5. Generating the
//! unit needs neither, so `print` works now — an operator can pipe it wherever they
//! like today, and `install` will just automate the same text.

use anyhow::bail;
use clap::Subcommand;

#[derive(Debug, Subcommand)]
pub enum ServiceAction {
    /// Print a service unit for this platform to stdout.
    Print,
    /// Install and enable the service unit.
    Install,
    /// Stop and remove the service unit.
    Uninstall,
}

pub fn run(action: &ServiceAction) -> anyhow::Result<()> {
    match action {
        ServiceAction::Print => {
            print!("{}", unit_for_this_platform()?);
            Ok(())
        }
        ServiceAction::Install | ServiceAction::Uninstall => bail!(
            "`telemetryd service install` lands with the packaging work in M5.\n\
             \n\
             Until then, `telemetryd service print` emits the same unit:\n\
             \n\
             {}",
            manual_steps()
        ),
    }
}

fn unit_for_this_platform() -> anyhow::Result<String> {
    let exe = std::env::current_exe().map_or_else(
        |_| "/usr/local/bin/telemetryd".to_owned(),
        |p| p.display().to_string(),
    );

    Ok(if cfg!(target_os = "macos") {
        launchd_plist(&exe)
    } else if cfg!(target_os = "linux") {
        systemd_unit(&exe)
    } else {
        bail!("no service unit template for this platform");
    })
}

/// A deliberately locked-down unit. telemetryd needs one directory and one socket, so
/// everything else is denied — a telemetry backend is an attractive target precisely
/// because everything sends it data.
fn systemd_unit(exe: &str) -> String {
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

User=telemetryd
Group=telemetryd
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

    #[test]
    fn the_generated_unit_is_for_this_platform_and_is_hardened() {
        let unit = unit_for_this_platform().unwrap();

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
    fn install_is_honest_about_not_being_built_yet() {
        let err = run(&ServiceAction::Install).unwrap_err().to_string();
        assert!(err.contains("M5"), "{err}");
        // …and still tells the operator how to do it by hand.
        assert!(err.contains("telemetryd service print"), "{err}");
    }
}
