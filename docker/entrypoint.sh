#!/bin/sh
# Make `docker run telemetryd` work, without making it insecure.
#
# telemetryd refuses to listen on an address reachable from outside the machine with no
# authentication configured (ADR-004), and a container binds 0.0.0.0 by definition. So
# the obvious "zero-config" image either fails to start or sets `insecure = true` and
# quietly serves everyone's telemetry to anyone who can reach the port.
#
# Neither is acceptable, so this does what a database image does: on the *first* start
# it generates tokens, persists them next to the data, and prints them once. A restart
# reuses them. Supplying your own — with -e, a mounted config, or a secret — skips all
# of this, which is what any real deployment should do.
set -eu

DATA_DIR="${TELEMETRYD_STORAGE_DATA_DIR:-/var/lib/telemetryd}"
GENERATED="${DATA_DIR}/generated-tokens.env"

configured() {
    # A mounted config file counts: it may carry tokens this script cannot see, and
    # guessing wrong would mean generating tokens nobody uses while startup still fails.
    [ -n "${TELEMETRYD_AUTH_INGEST_TOKEN:-}" ] && return 0
    [ -n "${TELEMETRYD_AUTH_QUERY_TOKEN:-}" ] && return 0
    [ -n "${TELEMETRYD_AUTH_ADMIN_TOKEN:-}" ] && return 0
    [ -n "${TELEMETRYD_SERVER_INSECURE:-}" ] && return 0
    [ -f "${DATA_DIR}/telemetryd.toml" ] && return 0
    [ -f /etc/telemetryd/telemetryd.toml ] && return 0
    return 1
}

random_token() {
    # 24 bytes of urandom, base64, made URL-safe. `tr -d` because a token that has to be
    # quoted in a curl command is a token people will paste wrongly.
    head -c 24 /dev/urandom | base64 | tr '+/' '-_' | tr -d '=\n'
}

if ! configured; then
    if [ ! -f "$GENERATED" ]; then
        mkdir -p "$DATA_DIR"
        INGEST="$(random_token)"
        QUERY="$(random_token)"
        ADMIN="$(random_token)"
        # Written before it is printed: if the write fails, the operator must not be
        # told a token that no longer exists after a restart.
        umask 077
        cat > "$GENERATED" <<EOF
TELEMETRYD_AUTH_INGEST_TOKEN=${INGEST}
TELEMETRYD_AUTH_QUERY_TOKEN=${QUERY}
TELEMETRYD_AUTH_ADMIN_TOKEN=${ADMIN}
EOF
        cat >&2 <<EOF

  ────────────────────────────────────────────────────────────────────────
  No authentication was configured, so telemetryd generated its own.
  These are printed once. They are stored in ${GENERATED}
  and reused on restart; delete that file to get new ones.

    ingest (write telemetry)  ${INGEST}
    query  (read telemetry)   ${QUERY}
    admin  (/status, /metrics) ${ADMIN}

  Set TELEMETRYD_AUTH_*_TOKEN yourself for anything that is not a laptop.
  ────────────────────────────────────────────────────────────────────────

EOF
    fi
    # shellcheck disable=SC1090
    . "$GENERATED"
    export TELEMETRYD_AUTH_INGEST_TOKEN TELEMETRYD_AUTH_QUERY_TOKEN TELEMETRYD_AUTH_ADMIN_TOKEN
fi

# `exec` so cbox-init becomes PID 1 rather than a child of this shell — otherwise it
# never receives the signals it exists to forward.
exec "$@"
