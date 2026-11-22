# Build from the repository root: docker build -f deploy/switchboard/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Switchboard is stdlib-only and the only package in this image.
COPY src/switchboard /app/src/switchboard

RUN useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin switchboard \
    && mkdir -p /data /etc/switchboard \
    && chown switchboard:switchboard /data /etc/switchboard

USER switchboard

VOLUME ["/data"]
EXPOSE 8790
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python3", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8790/healthz', timeout=3)"]

ENTRYPOINT ["python3", "-m", "switchboard.app", "--config", "/etc/switchboard/switchboard.toml"]
