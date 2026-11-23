from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from queue import SimpleQueue
from typing import Final

from switchboard.check import config_problems, config_summary
from switchboard.config import ConfigError, SwitchboardConfig, load_config
from switchboard.credentials import EnvCredentialBroker, feishu_app_credentials, github_app_credentials
from switchboard.envelope import utc_now
from switchboard.feishu_auth import FeishuTenantTokenSource, FeishuTokenSource
from switchboard.github_app import GitHubAppTokenSource, GitHubTokenSource
from switchboard.http_client import HttpClient, UrllibHttpClient
from switchboard.http_server import SwitchboardHTTPServer, UpgradeHandler
from switchboard.inbound.feishu import FeishuInboundAdapter
from switchboard.inbound.github import GitHubInboundAdapter
from switchboard.inbound.mcp import MCPInboundAdapter
from switchboard.outbound.feishu_adapter import FeishuOutboundAdapter
from switchboard.outbound.gateway import OutboundGateway
from switchboard.outbound.github_adapter import GitHubOutboundAdapter
from switchboard.pipeline import (
    Clock,
    CompositeTransport,
    DeliveryWorker,
    InboundPipeline,
    InProcessEventBus,
    InProcessTransport,
)
from switchboard.ports import (
    AuditSinkPort,
    ConsumerTransport,
    CredentialBrokerPort,
    InboundAdapter,
    NoopAuditSink,
    OutboundChannelAdapter,
)
from switchboard.relay_hub import RelayHub
from switchboard.router import OUTBOUND_CONSUMER, Router, ScopeResolver
from switchboard.rs256 import load_private_key
from switchboard.store_sqlite import SwitchboardStore

__all__ = ["EnvCredentialBroker", "Switchboard", "build_switchboard", "main"]

_log = logging.getLogger(__name__)
# After the drain, whatever its outcome: for close frames, and for pushes cut off with them to record "offline".
_CLOSE_SEC: Final = 1.0
_SETTLE_SEC: Final = 0.5


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
    relay: RelayHub | None = None
    # Set only when [relay] listen_port gives the hub its own listener.
    relay_http: SwitchboardHTTPServer | None = None

    @property
    def relay_port(self) -> int:
        return (self.relay_http or self.http).port

    @property
    def relay_path(self) -> str:
        return self.config.relay.path

    @property
    def relay_url(self) -> str:
        server = self.relay_http or self.http
        return server.base_url.replace("http://", "ws://", 1) + self.config.relay.path

    def start(self) -> None:
        self.gateway.start()
        self.worker.start()
        if self.relay is not None:
            self.relay.start()
        self.http.start()
        if self.relay_http is not None:
            self.relay_http.start()

    # Every intake closes before anything is waited for, so the drain only ever gets shorter. Relay connections
    # outlive the delivery drain: the acks of pushes in flight arrive on them. False: cut off at the timeout.
    def stop(self, timeout: float | None = None) -> bool:
        limit = self.config.shutdown_timeout_seconds if timeout is None else timeout
        deadline = time.monotonic() + limit

        def left() -> float:
            return max(0.0, deadline - time.monotonic())

        servers = [server for server in (self.http, self.relay_http) if server is not None]
        for server in servers:
            server.stop_accepting()
        self.worker.request_stop()
        self.gateway.request_stop()
        if self.relay is not None:
            self.relay.drain()

        blocked: list[str] = []
        # A list, not a generator: every server must refuse late requests, also after the first one timed out.
        if not all([server.drain(deadline) for server in servers]):
            blocked.append("HTTP requests")
        if not self.worker.join(left()):
            blocked.append("deliveries")
        if not self.gateway.join(left()):
            blocked.append("outbound sends")

        if self.relay is not None:
            self.relay.stop(_CLOSE_SEC)
        if blocked:
            self.worker.join(_SETTLE_SEC)
        if not self.store.close(max(_SETTLE_SEC, left())):
            blocked.append("SQLite write")
        if blocked:
            _log.warning(
                "[switchboard] shutdown timed out after %gs, forced with unfinished: %s", limit, ", ".join(blocked)
            )
        return not blocked


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
    github_tokens: GitHubTokenSource | None = None,
    feishu_tokens: FeishuTokenSource | None = None,
) -> Switchboard:
    sink: AuditSinkPort = audit or NoopAuditSink()
    store = SwitchboardStore(config.sqlite.path, busy_timeout_ms=config.sqlite.busy_timeout_ms)
    client: HttpClient = http_client or UrllibHttpClient()
    adapters: Mapping[str, OutboundChannelAdapter] = outbound_adapters or {
        "feishu": FeishuOutboundAdapter(
            client, credentials, api_base=config.feishu.api_base, tokens=feishu_tokens
        ),
        "github": GitHubOutboundAdapter(
            client, credentials, api_base=config.outbound.github_api_base, tokens=github_tokens
        ),
    }
    gateway = OutboundGateway(store, adapters, config=config.outbound, audit=sink, clock=clock)
    pipeline = InboundPipeline(
        store, router=router or Router.from_config(config.routing), audit=sink, clock=clock
    )
    transport = InProcessTransport()
    transport.register(OUTBOUND_CONSUMER, gateway.consume_event)
    relay = RelayHub(config.relay, credentials, audit=sink) if config.relay.enabled else None
    mode = config.relay.transport
    delivery_transport: ConsumerTransport = transport
    if relay is not None and mode != "in_process":
        delivery_transport = CompositeTransport(
            transport, relay.transport, mode=mode, local_consumers={OUTBOUND_CONSUMER}
        )
    max_parallel = config.delivery.max_parallel or (
        1 if mode == "in_process" else 2 * config.relay.max_in_flight
    )
    worker = DeliveryWorker(
        store,
        delivery_transport,
        config=config.delivery,
        gateway=gateway,
        audit=sink,
        clock=clock,
        max_parallel=max_parallel,
    )
    pipeline.on_accepted(worker.wake)
    upgrades: dict[str, UpgradeHandler] = {}
    relay_http: SwitchboardHTTPServer | None = None
    if relay is not None:
        relay.on_consumer_connected(worker.reset_offline_pending)
        if config.relay.listen_port is None:
            upgrades[config.relay.path] = relay.serve
        else:
            relay_http = SwitchboardHTTPServer(
                pipeline,
                {},
                host=config.relay.listen_host or config.http.host,
                port=config.relay.listen_port,
                max_body_bytes=config.http.max_body_bytes,
                upgrades={config.relay.path: relay.serve},
                base_path=config.http.base_path,
            )
    scopes = ScopeResolver(config.routing)
    routes: dict[str, InboundAdapter] = {
        "/webhooks/github": GitHubInboundAdapter(
            credentials, scopes, require_label=config.github.require_label, clock=clock
        ),
        "/webhooks/feishu": FeishuInboundAdapter(
            credentials,
            scopes,
            card_report=store.feishu_card_report,
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
        upgrades=upgrades,
        base_path=config.http.base_path,
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
        relay=relay,
        relay_http=relay_http,
    )


def _github_app_tokens(config: SwitchboardConfig, *, announce: bool) -> GitHubTokenSource | None:
    app = github_app_credentials()
    if app is None:
        return None
    owners = config.outbound.github_app_owners
    if not owners:
        raise ConfigError("SWITCHBOARD_GITHUB_APP_ID is set but [outbound] github_app_owners is empty")
    app_id, pem = app
    try:
        key = load_private_key(pem)
    except ValueError as exc:
        raise ConfigError(f"SWITCHBOARD_GITHUB_APP_PRIVATE_KEY: {exc}") from exc
    if announce:
        print(f"[switchboard] GitHub comments as App {app_id} for {', '.join(owners)}", file=sys.stderr)
    return GitHubAppTokenSource(
        UrllibHttpClient(),
        app_id=app_id,
        key=key,
        allowed_owners=owners,
        api_base=config.outbound.github_api_base,
    )


def _feishu_tokens(config: SwitchboardConfig, *, announce: bool) -> FeishuTokenSource | None:
    app = feishu_app_credentials()
    if app is None:
        return None
    app_id, app_secret = app
    if announce:
        print(f"[switchboard] Feishu messages as app {app_id}", file=sys.stderr)
    return FeishuTenantTokenSource(
        UrllibHttpClient(), app_id=app_id, app_secret=app_secret, api_base=config.feishu.api_base
    )


def _config_error(path: str, problems: Sequence[str]) -> int:
    for problem in problems:
        print(f"[switchboard] invalid config {path}: {problem}", file=sys.stderr)
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="switchboard")
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--check", action="store_true", help="validate the config and the credentials in the environment, then exit"
    )
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        github_tokens = _github_app_tokens(config, announce=not args.check)
        feishu_tokens = _feishu_tokens(config, announce=not args.check)
    except (OSError, ValueError) as exc:
        return _config_error(args.config, [str(exc)])
    if args.check:
        problems = config_problems(config)
        if problems:
            return _config_error(args.config, problems)
        print("\n  ".join([f"[switchboard] config ok: {args.config}", *config_summary(config)]))
        return 0

    logging.basicConfig(format="%(levelname)s %(message)s")
    # SimpleQueue.put is reentrant, so a handler may call it whatever the main thread is doing. The drain runs on its
    # own thread and reports through the same queue, which keeps the main thread free for a second signal. That one
    # is handled in Python because PID 1 of a container ignores signals left at their default action.
    wakeups: SimpleQueue[int | None] = SimpleQueue()

    def _on_signal(signum: int, frame: object) -> None:
        wakeups.put(signum)

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)
    board = build_switchboard(
        config, credentials=EnvCredentialBroker(), github_tokens=github_tokens, feishu_tokens=feishu_tokens
    )
    board.start()
    prefix = f" (also under {config.http.base_path})" if config.http.base_path else ""
    print(f"[switchboard] listening on {board.http.base_url}{prefix}", file=sys.stderr)
    if board.relay is not None:
        print(
            f"[switchboard] relay endpoint {board.relay_url} (transport={board.config.relay.transport})",
            file=sys.stderr,
        )
    wakeups.get()
    print(
        f"[switchboard] stopping, waiting up to {config.shutdown_timeout_seconds:g}s for work in flight; "
        "a second signal exits at once",
        file=sys.stderr,
    )

    def _drain() -> None:
        try:
            board.stop()
        finally:
            wakeups.put(None)

    threading.Thread(target=_drain, name="switchboard-shutdown", daemon=True).start()
    again = wakeups.get()
    if again is not None:
        sys.stderr.flush()
        os._exit(128 + again)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
