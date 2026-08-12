//! `telemetryd init` — write a configuration a server deployment can actually use.
//!
//! # Why this exists
//!
//! `service install` generated a unit whose `ExecStart` reads
//! `/etc/telemetryd/telemetryd.toml`, and nothing ever created that file. An explicitly
//! requested configuration that is missing is a startup error, so the unit the tool
//! wrote could not start until somebody hand-wrote a config first — and the deployment
//! guide grew a heredoc and an `openssl` loop to paper over it.
//!
//! The container has done the right thing since it shipped: generate tokens on first
//! start, store them beside the data, print them once. This is the same idea for a
//! machine, done once and deliberately rather than on every boot.
//!
//! # Why not in `install.sh`
//!
//! The documented install is `curl … | sh`, which makes the *script* the shell's stdin.
//! A `read` there consumes script text rather than waiting for a person, and the fix —
//! reading `/dev/tty` — fails in CI, in containers and over automation. A tool that
//! wants an answer has to be the thing the operator runs.
//!
//! # Why tokens go in their own files
//!
//! `auth.ingest_token = ["file:/etc/telemetryd/ingest.token"]` rather than the value
//! inline. The config is the file people paste into issues and commit by accident; the
//! token files are `0600` and referenced by path. It is also what the deployment guide
//! already recommends, so the generated configuration and the documentation agree.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// Where a configuration lives when nobody says otherwise.
///
/// The same path `service install` bakes into the unit, so the two commands compose
/// without arguments — which is the whole point of adding this one.
#[cfg(unix)]
const DEFAULT_DIR: &str = "/etc/telemetryd";

#[derive(Debug, clap::Args)]
pub struct InitArgs {
    /// Directory for the configuration and the token files.
    #[arg(long, default_value = DEFAULT_DIR)]
    pub dir: PathBuf,

    /// Overwrite an existing configuration.
    ///
    /// Off by default: re-running `init` on a live server would otherwise rotate every
    /// token and take down every writer that holds one.
    #[arg(long)]
    pub force: bool,
}

/// 24 bytes of urandom, URL-safe base64, no padding.
///
/// `read_exact` into a fixed buffer, and nothing else. The first version of this called
/// `std::fs::read`, which reads to end of file — and a character device has no end. It
/// allocated until the machine ran out of memory, and it did that on a real laptop
/// before it ever reached a test. The correct implementation was present the whole time,
/// as a fallback in an `or_else` that only runs when the first call *fails*; an infinite
/// read never fails, so it never ran.
///
/// Read straight from the device rather than through a crate: every target is Unix, the
/// file is the same source a dependency would wrap, and a token generator is not a place
/// to add a supply-chain edge for convenience. That reasoning stands — the defect was
/// the API, not the decision.
fn token() -> Result<String> {
    use std::io::Read as _;
    let mut file = std::fs::File::open("/dev/urandom").context("opening /dev/urandom")?;
    let mut buffer = [0u8; 24];
    file.read_exact(&mut buffer)
        .context("reading 24 bytes from /dev/urandom")?;
    Ok(base64_url(&buffer))
}

/// Base64 without padding and with the URL alphabet, so a token never needs quoting in
/// a shell, a header or a URL.
fn base64_url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..=chunk.len() {
            out.push(ALPHABET[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
        }
    }
    out
}

#[cfg(unix)]
fn write_private(path: &Path, contents: &str) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("writing {}", path.display()))?;
    file.write_all(contents.as_bytes())
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

pub fn run(args: &InitArgs) -> Result<()> {
    let config_path = args.dir.join("telemetryd.toml");
    if config_path.exists() && !args.force {
        bail!(
            "{} already exists.\n\
             Re-running would rotate every token and lock out everything holding one. \
             Pass --force if that is what you want, or edit the file directly.",
            config_path.display()
        );
    }

    std::fs::create_dir_all(&args.dir)
        .with_context(|| format!("creating {}", args.dir.display()))?;

    let surfaces = [
        ("ingest", "writes telemetry"),
        ("query", "reads telemetry"),
        ("admin", "reads /status and /metrics"),
    ];
    let mut generated = Vec::new();
    for (name, _) in surfaces {
        let value = token()?;
        let path = args.dir.join(format!("{name}.token"));
        write_private(&path, &value)?;
        generated.push((name, value, path));
    }

    let config = format!(
        "# Written by `telemetryd init`. Every setting here is also an environment\n\
         # variable named TELEMETRYD_<SECTION>_<KEY>, and the environment wins.\n\
         \n\
         [server]\n\
         # Loopback: put a TLS-terminating proxy in front, or set server.tls.cert_file\n\
         # and key_file, before making this reachable from anywhere else.\n\
         listen = \"127.0.0.1:4319\"\n\
         \n\
         [auth]\n\
         # Referenced by path rather than inlined, so the values stay out of this file\n\
         # and out of `ps`. One per surface: a leaked write token does not open reads.\n\
         ingest_token = [\"file:{ingest}\"]\n\
         query_token  = [\"file:{query}\"]\n\
         admin_token  = [\"file:{admin}\"]\n\
         \n\
         [retention]\n\
         logs    = \"7d\"\n\
         traces  = \"7d\"\n\
         metrics = \"30d\"\n\
         \n\
         [storage]\n\
         # The ceiling that actually bounds the disk. Retention is the window you want;\n\
         # this is the limit that wins when the two disagree.\n\
         disk_budget = \"10GiB\"\n",
        ingest = generated[0].2.display(),
        query = generated[1].2.display(),
        admin = generated[2].2.display(),
    );
    std::fs::write(&config_path, &config)
        .with_context(|| format!("writing {}", config_path.display()))?;

    crate::out::outln!("wrote {}", config_path.display());
    crate::out::outln!("");
    crate::out::outln!("These are printed once. They are stored beside the config:");
    crate::out::outln!("");
    for ((name, value, path), (_, what)) in generated.iter().zip(surfaces) {
        crate::out::outln!("  {name:<6} ({what})");
        crate::out::outln!("    {value}");
        crate::out::outln!("    {}", path.display());
    }
    crate::out::outln!("");
    crate::out::outln!("Next:");
    crate::out::outln!("  telemetryd validate            # check what it resolved to");
    crate::out::outln!("  sudo telemetryd service install");
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn base64_is_url_safe_and_unpadded() {
        // The point of the alphabet: a token that never needs quoting in a shell, a
        // header or a URL. `+` and `/` would, and `=` invites truncation on copy.
        for length in 1..40 {
            let encoded = base64_url(&vec![0xfb; length]);
            assert!(!encoded.contains('+') && !encoded.contains('/') && !encoded.contains('='));
        }
        assert_eq!(base64_url(b""), "");
        assert_eq!(base64_url(&[0, 0, 0]), "AAAA");
    }

    #[test]
    fn tokens_do_not_repeat() {
        let first = token().unwrap();
        let second = token().unwrap();
        assert_ne!(
            first, second,
            "two reads of urandom produced the same token"
        );
        assert!(
            first.len() >= 32,
            "a 24-byte token encodes to 32 characters"
        );
    }

    #[test]
    fn an_existing_configuration_is_not_overwritten_by_accident() {
        let dir = tempfile::tempdir().unwrap();
        let args = InitArgs {
            dir: dir.path().to_path_buf(),
            force: false,
        };
        run(&args).unwrap();
        let first = std::fs::read_to_string(dir.path().join("ingest.token")).unwrap();

        // The failure this guards is rotating live credentials on a re-run.
        let error = run(&args).unwrap_err();
        assert!(error.to_string().contains("already exists"), "{error}");
        let after = std::fs::read_to_string(dir.path().join("ingest.token")).unwrap();
        assert_eq!(
            first, after,
            "a refused init must not have touched the tokens"
        );

        run(&InitArgs {
            dir: dir.path().to_path_buf(),
            force: true,
        })
        .unwrap();
        let forced = std::fs::read_to_string(dir.path().join("ingest.token")).unwrap();
        assert_ne!(first, forced, "--force should rotate");
    }

    #[cfg(unix)]
    #[test]
    fn token_files_are_readable_only_by_their_owner() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        run(&InitArgs {
            dir: dir.path().to_path_buf(),
            force: false,
        })
        .unwrap();
        for name in ["ingest", "query", "admin"] {
            let mode = std::fs::metadata(dir.path().join(format!("{name}.token")))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "{name}.token is {mode:o}");
        }
    }
}
