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

configured_elsewhere() {
    # A mounted config file counts: it may carry tokens this script cannot see, and
    # guessing wrong would mean generating tokens nobody uses while startup still fails.
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

# Every surface needs a token of its own. This used to generate tokens only when *none*
# was set, so `-e TELEMETRYD_AUTH_INGEST_TOKEN=…` alone skipped generation and started a
# container whose reads and exports answered anyone. telemetryd now refuses that at
# startup, so here the surfaces you did not set get a generated token instead.
if ! configured_elsewhere; then
    FRESH=""
    if [ ! -f "$GENERATED" ]; then
        mkdir -p "$DATA_DIR"
        # Written before it is used or printed: if the write fails, the operator must not
        # be told a token that no longer exists after a restart.
        umask 077
        cat > "$GENERATED" <<EOF
TELEMETRYD_AUTH_INGEST_TOKEN=$(random_token)
TELEMETRYD_AUTH_QUERY_TOKEN=$(random_token)
TELEMETRYD_AUTH_ADMIN_TOKEN=$(random_token)
EOF
        FRESH=1
    fi

    # What you set wins over what was generated, surface by surface.
    GIVEN_INGEST="${TELEMETRYD_AUTH_INGEST_TOKEN:-}"
    GIVEN_QUERY="${TELEMETRYD_AUTH_QUERY_TOKEN:-}"
    GIVEN_ADMIN="${TELEMETRYD_AUTH_ADMIN_TOKEN:-}"
    # shellcheck disable=SC1090
    . "$GENERATED"
    [ -n "$GIVEN_INGEST" ] && TELEMETRYD_AUTH_INGEST_TOKEN="$GIVEN_INGEST"
    [ -n "$GIVEN_QUERY" ] && TELEMETRYD_AUTH_QUERY_TOKEN="$GIVEN_QUERY"
    [ -n "$GIVEN_ADMIN" ] && TELEMETRYD_AUTH_ADMIN_TOKEN="$GIVEN_ADMIN"
    export TELEMETRYD_AUTH_INGEST_TOKEN TELEMETRYD_AUTH_QUERY_TOKEN TELEMETRYD_AUTH_ADMIN_TOKEN

    if [ -n "$FRESH" ] && { [ -z "$GIVEN_INGEST" ] || [ -z "$GIVEN_QUERY" ] || [ -z "$GIVEN_ADMIN" ]; }; then
        {
            echo
            echo "  ────────────────────────────────────────────────────────────────────────"
            echo "  Some surfaces had no token, so telemetryd generated them. They are"
            echo "  printed once, stored in ${GENERATED} and reused on restart;"
            echo "  delete that file to get new ones."
            echo
            [ -z "$GIVEN_INGEST" ] && echo "    ingest (write telemetry)   ${TELEMETRYD_AUTH_INGEST_TOKEN}"
            [ -z "$GIVEN_QUERY" ] && echo "    query  (read telemetry)    ${TELEMETRYD_AUTH_QUERY_TOKEN}"
            [ -z "$GIVEN_ADMIN" ] && echo "    admin  (/status, /metrics) ${TELEMETRYD_AUTH_ADMIN_TOKEN}"
            echo
            echo "  Set TELEMETRYD_AUTH_*_TOKEN yourself for anything that is not a laptop."
            echo "  ────────────────────────────────────────────────────────────────────────"
            echo
        } >&2
    fi
fi

# `exec` so cbox-init becomes PID 1 rather than a child of this shell — otherwise it
# never receives the signals it exists to forward.
exec "$@"
