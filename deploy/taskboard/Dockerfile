# Build from the repository root: docker build -f deploy/taskboard/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Taskboard is stdlib-only; it reuses Switchboard's envelope, relay agent and credential broker, and validates
# events against schemas/taskboard (json_schema.py resolves it as /app/schemas/taskboard).
COPY src/taskboard /app/src/taskboard
COPY src/switchboard /app/src/switchboard
COPY schemas/taskboard /app/schemas/taskboard

RUN useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin taskboard \
    && mkdir -p /data /etc/taskboard \
    && chown taskboard:taskboard /data /etc/taskboard

USER taskboard

VOLUME ["/data"]
EXPOSE 8791
STOPSIGNAL SIGTERM

# /healthz answers with or without [http] base_path, so the probe does not depend on the prefix.
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8791/healthz', timeout=3)"]

ENTRYPOINT ["python3", "-m", "taskboard.app", "--config", "/etc/taskboard/taskboard.toml"]
