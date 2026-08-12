#!/bin/sh
# Install telemetryd.
#
#   curl -fsSL https://raw.githubusercontent.com/cboxdk/telemetryd/main/install.sh | sh
#
# POSIX sh, not bash: this has to run on an Alpine container and a stock Debian image
# as well as on a Mac. No sudo is invoked — if the install directory is not writable
# the script says exactly what to run instead, rather than escalating on its own.

set -eu

REPO="cboxdk/telemetryd"
BIN="telemetryd"

VERSION="${TELEMETRYD_VERSION:-latest}"
INSTALL_DIR="${TELEMETRYD_INSTALL_DIR:-}"

say() { printf '%s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

need() {
    command -v "$1" >/dev/null 2>&1 || die "this installer needs $1"
}

detect_target() {
    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Linux)  os_part="unknown-linux-musl" ;;
        Darwin) os_part="apple-darwin" ;;
        *) die "unsupported operating system: $os. Build from source: https://github.com/$REPO" ;;
    esac

    case "$arch" in
        x86_64|amd64)  arch_part="x86_64" ;;
        aarch64|arm64) arch_part="aarch64" ;;
        *) die "unsupported architecture: $arch. Build from source: https://github.com/$REPO" ;;
    esac

    printf '%s-%s' "$arch_part" "$os_part"
}

# First writable directory already on PATH, so the binary is runnable without the user
# editing their shell profile.
detect_install_dir() {
    if [ -n "$INSTALL_DIR" ]; then
        printf '%s' "$INSTALL_DIR"
        return
    fi
    for candidate in "$HOME/.local/bin" /usr/local/bin "$HOME/bin"; do
        if [ -d "$candidate" ] && [ -w "$candidate" ]; then
            printf '%s' "$candidate"
            return
        fi
    done
    # Nothing writable exists yet; ~/.local/bin is the least surprising thing to create.
    printf '%s' "$HOME/.local/bin"
}

resolve_version() {
    if [ "$VERSION" != "latest" ]; then
        printf '%s' "${VERSION#v}"
        return
    fi
    tag="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
        | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
        | head -n 1)"
    [ -n "$tag" ] || die "could not determine the latest release; set TELEMETRYD_VERSION"
    printf '%s' "${tag#v}"
}

main() {
    need curl
    need tar

    target="$(detect_target)"
    version="$(resolve_version)"
    install_dir="$(detect_install_dir)"

    name="$BIN-$version-$target"
    url="https://github.com/$REPO/releases/download/v$version/$name.tar.gz"

    say "telemetryd $version ($target)"

    tmp="$(mktemp -d)"
    # Clean up even if the download fails, so a retry does not accumulate temp dirs.
    trap 'rm -rf "$tmp"' EXIT INT TERM

    say "downloading $url"
    curl -fsSL "$url" -o "$tmp/$name.tar.gz" \
        || die "download failed. Check that v$version exists: https://github.com/$REPO/releases"

    # Verify against the release's checksum file. A failure here is refused rather than
    # warned about: an installer that continues past a bad checksum provides no
    # protection at all.
    if curl -fsSL "https://github.com/$REPO/releases/download/v$version/SHA256SUMS" -o "$tmp/SHA256SUMS" 2>/dev/null; then
        # The checksum file arrives from the same server as the archive, so on its own
        # it proves the download was not corrupted — not that we published it. The
        # Sigstore signature over it is what establishes that, so verify it when
        # cosign is available and say plainly when it is not.
        if command -v cosign >/dev/null 2>&1; then
            if curl -fsSL "https://github.com/$REPO/releases/download/v$version/SHA256SUMS.cosign.bundle" \
                -o "$tmp/SHA256SUMS.cosign.bundle" 2>/dev/null; then
                # --certificate-identity is not optional: without pinning it, any
                # valid Sigstore signature by anyone at all would satisfy this.
                cosign verify-blob \
                    --bundle "$tmp/SHA256SUMS.cosign.bundle" \
                    --certificate-identity "https://github.com/$REPO/.github/workflows/release.yml@refs/tags/v$version" \
                    --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
                    "$tmp/SHA256SUMS" >/dev/null 2>&1 \
                    || die "signature verification failed for v$version — refusing to install.
  The checksum file is not signed by the telemetryd release workflow."
                say "signature verified"
            else
                say "note: v$version predates release signing, falling back to checksums"
            fi
        else
            say "note: cosign not installed — verifying the checksum only, not who published it"
        fi

        expected="$(grep " $name.tar.gz\$" "$tmp/SHA256SUMS" | awk '{print $1}' | head -n 1)"
        if [ -n "$expected" ]; then
            if command -v sha256sum >/dev/null 2>&1; then
                actual="$(sha256sum "$tmp/$name.tar.gz" | awk '{print $1}')"
            elif command -v shasum >/dev/null 2>&1; then
                actual="$(shasum -a 256 "$tmp/$name.tar.gz" | awk '{print $1}')"
            else
                actual=""
                say "warning: no sha256 tool found, skipping checksum verification"
            fi
            if [ -n "$actual" ]; then
                [ "$actual" = "$expected" ] || die "checksum mismatch — refusing to install
  expected $expected
  actual   $actual"
                say "checksum verified"
            fi
        fi
    else
        say "warning: no SHA256SUMS published for v$version, skipping verification"
    fi

    tar -xzf "$tmp/$name.tar.gz" -C "$tmp"
    [ -f "$tmp/$name/$BIN" ] || die "the archive did not contain $BIN"

    mkdir -p "$install_dir" 2>/dev/null || true
    if [ ! -w "$install_dir" ]; then
        die "$install_dir is not writable. Either:
  sudo install -m 0755 $tmp/$name/$BIN $install_dir/$BIN
or choose somewhere you own:
  TELEMETRYD_INSTALL_DIR=\$HOME/.local/bin sh install.sh"
    fi

    install -m 0755 "$tmp/$name/$BIN" "$install_dir/$BIN"
    say "installed $install_dir/$BIN"

    case ":$PATH:" in
        *":$install_dir:"*) ;;
        *) say ""
           say "note: $install_dir is not on your PATH. Add it with:"
           say "  export PATH=\"$install_dir:\$PATH\"" ;;
    esac

    say ""
    say "Start it with:"
    say "  $BIN serve"
    say ""
    say "It listens on 127.0.0.1:4319 and needs no configuration."
    say ""
    # Named here because the alternative is that someone runs it in a terminal, closes
    # the terminal, and discovers a week later that they have a week of no telemetry.
    say "To keep it running across reboots:"
    say "  $BIN service print      # read the unit first"
    say "  sudo $BIN service install"
}

main "$@"
