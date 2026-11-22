from __future__ import annotations

import argparse
import os
import signal
import ssl
import sys
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from typing import IO
from urllib.parse import urlsplit

from switchboard.ports import CredentialBrokerPort, EventBusPort
from taskboard.adapters.audit_adapter import SafeAuditAdapter, StderrAuditSink
from taskboard.adapters.http_server import TaskboardHTTPServer
from taskboard.adapters.mcp_tools import TaskboardMCPServer, serve_stdio
from taskboard.adapters.remote_bus import SwitchboardRemoteBus
from taskboard.adapters.sqlite_store import SQLiteTaskStore
from taskboard.adapters.switchboard_adapter import SwitchboardAdapter
from taskboard.app.config import ConfigError, TaskboardConfig, load_config, service_secrets
from taskboard.engine.ingestion import IngestionService
from taskboard.ports import AuditSinkPort


@dataclass(slots=True)
class TaskboardService:
    config: TaskboardConfig
    store: SQLiteTaskStore
    ingestion: IngestionService
    mcp: TaskboardMCPServer
    adapter: SwitchboardAdapter | None
    http: TaskboardHTTPServer | None
    remote: SwitchboardRemoteBus | None
    subscribe: bool

    def start(self) -> None:
        if self.http is not None:
            self.http.start()
        # Subscribing starts the relay agent, so pushes only arrive once everything above is up.
        if self.adapter is not None and self.subscribe:
            self.adapter.attach(self.config.switchboard.consumer_id)

    def stop(self) -> None:
        # The relay first: in-flight handlers still write to the store.
        if self.remote is not None:
            self.remote.stop()
        if self.http is not None:
            self.http.stop()
        self.store.close()


def remote_bus(
    config: TaskboardConfig, credentials: CredentialBrokerPort, *, relay: bool
) -> SwitchboardRemoteBus | None:
    link = config.switchboard
    if not link.mcp_url:
        return None
    return SwitchboardRemoteBus(
        mcp_url=link.mcp_url,
        relay_url=link.relay_url if relay else None,
        consumer_id=link.consumer_id,
        credentials=credentials,
        ssl_context=ssl.create_default_context(cafile=link.ca_file) if link.ca_file else None,
    )


# bus None: nothing is consumed or published (ready tasks are still claimed, see run_stdio).
def build_service(
    config: TaskboardConfig,
    *,
    mcp_token: str | None,
    bus: EventBusPort | None,
    audit: AuditSinkPort | None = None,
    serve_http: bool = True,
) -> TaskboardService:
    safe_audit = SafeAuditAdapter(audit or StderrAuditSink())
    store = SQLiteTaskStore(config.sqlite.path, busy_timeout_ms=config.sqlite.busy_timeout_ms)
    adapter = (
        SwitchboardAdapter(bus, audit=safe_audit, auto_ready_sources=config.intake.auto_ready_sources)
        if bus is not None
        else None
    )
    ingestion = IngestionService(store, publisher=adapter, audit=safe_audit)
    if adapter is not None:
        adapter.bind_ingestion(ingestion)
    mcp = TaskboardMCPServer(ingestion)
    http = (
        TaskboardHTTPServer(
            mcp,
            host=config.http.host,
            port=config.http.port,
            base_path=config.http.base_path,
            max_body_bytes=config.http.max_body_bytes,
            token=mcp_token,
        )
        if serve_http
        else None
    )
    remote = bus if isinstance(bus, SwitchboardRemoteBus) else None
    return TaskboardService(
        config=config,
        store=store,
        ingestion=ingestion,
        mcp=mcp,
        adapter=adapter,
        http=http,
        remote=remote,
        subscribe=bus is not None and (remote is None or remote.agent is not None),
    )


def _warn_plaintext(config: TaskboardConfig) -> None:
    for url in (config.switchboard.relay_url, config.switchboard.mcp_url):
        if not url:
            continue
        parts = urlsplit(url)
        if parts.scheme in ("ws", "http") and parts.hostname not in ("127.0.0.1", "localhost", "::1"):
            print(f"[taskboard] warning: {url} sends tokens in plaintext", file=sys.stderr)


# Never connects the relay: a second agent with the same consumer_id would take over (close 4009) the
# long-running service's connection.
def run_stdio(config: TaskboardConfig, stdin: IO[str], stdout: IO[str]) -> int:
    secrets = service_secrets(config, os.environ, stdio=True)
    bus = remote_bus(config, secrets.credentials, relay=False) if secrets.credentials.issue_ephemeral("mcp") else None
    if bus is None:
        print(
            "[taskboard] no Switchboard uplink (needs [switchboard] mcp_url and SWITCHBOARD_MCP_TOKEN); "
            "ready tasks are claimed but no work.ready is published",
            file=sys.stderr,
        )
    service = build_service(config, mcp_token=None, bus=bus, serve_http=False)
    try:
        serve_stdio(service.mcp, stdin, stdout)
    finally:
        service.stop()
    return 0


def run_service(config: TaskboardConfig) -> int:
    secrets = service_secrets(config, os.environ, stdio=False)
    if secrets.mcp_token is None:
        print(
            "[taskboard] TASKBOARD_MCP_TOKEN is not set; HTTP /mcp rejects every request",
            file=sys.stderr,
        )
    _warn_plaintext(config)
    service = build_service(
        config, mcp_token=secrets.mcp_token, bus=remote_bus(config, secrets.credentials, relay=True)
    )
    stopped = threading.Event()

    def _stop(signum: int, frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    service.start()
    assert service.http is not None
    base = config.http.base_path
    print(
        f"[taskboard] listening on {service.http.base_url} (mcp: /mcp{f', {base}/mcp' if base else ''}); "
        f"relay {config.switchboard.relay_url} as {config.switchboard.consumer_id}",
        file=sys.stderr,
        flush=True,
    )
    stopped.wait()
    service.stop()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m taskboard.app",
        description="Taskboard service: consumes Switchboard events over the relay and serves tb_* MCP tools. "
        "Tokens come from SWITCHBOARD_RELAY_TOKEN, SWITCHBOARD_MCP_TOKEN and TASKBOARD_MCP_TOKEN.",
    )
    parser.add_argument("--config", required=True, help="path to taskboard.toml")
    parser.add_argument(
        "--mcp-stdio",
        action="store_true",
        help="serve MCP over stdin/stdout instead of HTTP, without connecting the relay",
    )
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.mcp_stdio:
            return run_stdio(config, sys.stdin, sys.stdout)
        return run_service(config)
    except (ConfigError, OSError) as exc:
        print(f"[taskboard] {exc}", file=sys.stderr)
        return 2
