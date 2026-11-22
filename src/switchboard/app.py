from __future__ import annotations

import argparse
import os
import signal
import sys
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from switchboard.config import SwitchboardConfig, load_config
from switchboard.envelope import utc_now
from switchboard.http_client import HttpClient, UrllibHttpClient
from switchboard.http_server import SwitchboardHTTPServer
from switchboard.inbound.feishu import FeishuInboundAdapter
from switchboard.inbound.github import GitHubInboundAdapter
from switchboard.inbound.mcp import MCPInboundAdapter
from switchboard.outbound.feishu_adapter import FeishuOutboundAdapter
from switchboard.outbound.gateway import OutboundGateway
from switchboard.outbound.github_adapter import GitHubOutboundAdapter
from switchboard.pipeline import (
    Clock,
    DeliveryWorker,
    InboundPipeline,
    InProcessEventBus,
    InProcessTransport,
)
from switchboard.ports import (
    AuditSinkPort,
    CredentialBrokerPort,
    InboundAdapter,
    NoopAuditSink,
    OutboundChannelAdapter,
)
from switchboard.router import OUTBOUND_CONSUMER, Router, ScopeResolver
from switchboard.store_sqlite import SwitchboardStore


# Development broker: secrets come from the environment, never from config or code.
class EnvCredentialBroker:
    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        self._environ = os.environ if environ is None else environ

    def get_webhook_secret(self, channel: str) -> str | None:
        return self._environ.get(f"SWITCHBOARD_{channel.upper()}_WEBHOOK_SECRET") or None

    def issue_ephemeral(self, channel: str) -> str | None:
        return self._environ.get(f"SWITCHBOARD_{channel.upper()}_TOKEN") or None


@dataclass(slots=True)
class Switchboard:
    config: SwitchboardConfig
    store: SwitchboardStore
    pipeline: InboundPipeline
    transport: InProcessTransport
    worker: DeliveryWorker
    gateway: OutboundGateway
    bus: InProcessEventBus
    http: SwitchboardHTTPServer

    def start(self) -> None:
        self.gateway.start()
        self.worker.start()
        self.http.start()

    def stop(self) -> None:
        self.http.stop()
        self.worker.stop()
        self.gateway.stop()
        self.store.close()


def build_switchboard(
    config: SwitchboardConfig,
    *,
    credentials: CredentialBrokerPort,
    audit: AuditSinkPort | None = None,
    http_client: HttpClient | None = None,
    outbound_adapters: Mapping[str, OutboundChannelAdapter] | None = None,
    clock: Clock = utc_now,
    router: Router | None = None,
    port: int | None = None,
) -> Switchboard:
    sink: AuditSinkPort = audit or NoopAuditSink()
    store = SwitchboardStore(config.sqlite.path, busy_timeout_ms=config.sqlite.busy_timeout_ms)
    client: HttpClient = http_client or UrllibHttpClient()
    adapters: Mapping[str, OutboundChannelAdapter] = outbound_adapters or {
        "feishu": FeishuOutboundAdapter(client, credentials, api_base=config.feishu.api_base),
        "github": GitHubOutboundAdapter(client, credentials, api_base=config.outbound.github_api_base),
    }
    gateway = OutboundGateway(store, adapters, config=config.outbound, audit=sink, clock=clock)
    pipeline = InboundPipeline(store, router=router, audit=sink, clock=clock)
    transport = InProcessTransport()
    transport.register(OUTBOUND_CONSUMER, gateway.consume_event)
    worker = DeliveryWorker(
        store, transport, config=config.delivery, gateway=gateway, audit=sink, clock=clock
    )
    pipeline.on_accepted(worker.wake)
    scopes = ScopeResolver(config.routing)
    routes: dict[str, InboundAdapter] = {
        "/webhooks/github": GitHubInboundAdapter(credentials, scopes, clock=clock),
        "/webhooks/feishu": FeishuInboundAdapter(
            credentials,
            scopes,
            group_require_mention=config.feishu.group_require_mention,
            bot_open_id=config.feishu.bot_open_id,
            clock=clock,
        ),
        "/mcp": MCPInboundAdapter(credentials, pipeline, scopes, worker.replay_delivery, clock=clock),
    }
    http = SwitchboardHTTPServer(
        pipeline,
        routes,
        host=config.http.host,
        port=config.http.port if port is None else port,
        max_body_bytes=config.http.max_body_bytes,
    )
    return Switchboard(
        config=config,
        store=store,
        pipeline=pipeline,
        transport=transport,
        worker=worker,
        gateway=gateway,
        bus=InProcessEventBus(pipeline, transport, worker),
        http=http,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="switchboard")
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    board = build_switchboard(load_config(args.config), credentials=EnvCredentialBroker())
    stopped = threading.Event()

    def _stop(signum: int, frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    board.start()
    print(f"[switchboard] listening on {board.http.base_url}", file=sys.stderr)
    stopped.wait()
    board.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
