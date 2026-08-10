# telemetryd as a container.
#
# The binary is *not* rebuilt here. It is the same statically linked musl artifact the
# release workflow builds, soak-tests and signs — copied in, with its checksum checked
# against the published SHA256SUMS. Rebuilding it in this Dockerfile would ship an
# image whose binary nothing had verified, which is the failure this project has already
# made once: a green suite says nothing about the artifact that actually ships.
#
# Expects the release assets unpacked into the build context:
#   context/telemetryd-linux-amd64
#   context/telemetryd-linux-arm64
# `scripts/docker-context.py` assembles that from a tag or from a local build.

ARG ALPINE_VERSION=3.21
ARG CBOX_INIT_VERSION=v2.4.1

# ---------------------------------------------------------------------------
# cbox-init — PID 1, so signals reach telemetryd and no zombie is left behind
# ---------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION} AS init
ARG CBOX_INIT_VERSION
ARG TARGETARCH
RUN apk add --no-cache curl \
    && curl -fsSL -o /tmp/cbox-init \
       "https://github.com/cboxdk/init/releases/download/${CBOX_INIT_VERSION}/cbox-init-linux-${TARGETARCH}" \
    && curl -fsSL -o /tmp/checksums.txt \
       "https://github.com/cboxdk/init/releases/download/${CBOX_INIT_VERSION}/checksums.txt" \
    # Verify before trusting. A download over TLS says the bytes arrived intact from
    # somewhere, not that they are the bytes that were released.
    && EXPECTED="$(grep "cbox-init-linux-${TARGETARCH}\$" /tmp/checksums.txt | cut -d' ' -f1)" \
    && ACTUAL="$(sha256sum /tmp/cbox-init | cut -d' ' -f1)" \
    && if [ -z "$EXPECTED" ] || [ "$EXPECTED" != "$ACTUAL" ]; then \
         echo "cbox-init checksum mismatch: expected '${EXPECTED}', got '${ACTUAL}'" >&2; \
         exit 1; \
       fi \
    && chmod +x /tmp/cbox-init

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}
ARG TARGETARCH

# Nothing is installed here on purpose. telemetryd is statically linked, and it verifies
# outbound TLS against roots compiled into the binary (see crates/core/src/http.rs), so
# the image needs no ca-certificates package and cannot change behaviour depending on
# whether one is present. cbox-init is PID 1, so no tini either.
COPY --from=init /tmp/cbox-init /usr/local/bin/cbox-init
COPY telemetryd-linux-${TARGETARCH} /usr/local/bin/telemetryd
COPY cbox-init.yaml /etc/cbox-init/cbox-init.yaml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/telemetryd /usr/local/bin/entrypoint.sh \
    && addgroup -g 10001 -S telemetryd \
    && adduser -u 10001 -S -G telemetryd -h /var/lib/telemetryd telemetryd \
    && mkdir -p /var/lib/telemetryd \
    && chown -R telemetryd:telemetryd /var/lib/telemetryd \
    && /usr/local/bin/telemetryd version

# Defaults that make `docker run` work with nothing else set. Everything here is an
# ordinary config key, so any of it can be overridden with -e or in compose, and a
# mounted telemetryd.toml still loses to the environment (defaults → file → env → flags).
ENV TELEMETRYD_SERVER_LISTEN=0.0.0.0:4319 \
    TELEMETRYD_STORAGE_DATA_DIR=/var/lib/telemetryd \
    TELEMETRYD_LOG_FORMAT=json

# Named so a `docker run` with no -v still keeps data across a restart, and so the
# write path is never the container's own layer.
VOLUME ["/var/lib/telemetryd"]

EXPOSE 4319

USER telemetryd
WORKDIR /var/lib/telemetryd

# /healthz rather than `telemetryd status`: it is unauthenticated by design because it
# reports liveness and not telemetry, so the check keeps working when admin tokens are
# set. busybox wget is already in the base image.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:4319/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["cbox-init"]
