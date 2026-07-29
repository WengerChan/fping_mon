# syntax=docker/dockerfile:1.6
# Build the runtime image for fping-monitor. The build installs fping
# and grants it the ICMP capability so the container can run as a
# non-root user. The runtime image is slim: only Python and fping.

ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim AS builder
WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install .

FROM python:${PYTHON_VERSION}-slim AS runtime
ARG FPING_VERSION=5.1
ARG UID=10001
ARG GID=10001

# fping + libcap2-bin (setcap) + curl (healthcheck) + tini (PID 1)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        fping \
        libcap2-bin \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Grant fping the ICMP capability so the runtime user does not need root.
RUN setcap cap_net_raw+ep /usr/bin/fping

# Create a non-root user and copy the application code.
RUN groupadd --system --gid ${GID} fping \
    && useradd --system --uid ${UID} --gid fping --home /app --shell /usr/sbin/nologin fping
WORKDIR /app
COPY --from=builder /install /usr/local
COPY src /app/src
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    FPING_MONITOR_LOG_LEVEL=INFO

RUN mkdir -p /var/lib/fping-monitor \
    && chown -R fping:fping /var/lib/fping-monitor /app

USER fping
EXPOSE 9100
VOLUME ["/var/lib/fping-monitor"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:9100/healthz || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "python", "-m", "fping_monitor"]
CMD ["--config", "/etc/fping-monitor/config.yaml"]
