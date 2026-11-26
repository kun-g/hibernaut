from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from typing import Any

from switchboard.app import build_switchboard
from switchboard.envelope import CanonicalEvent
from switchboard.ports import DeliverStatus, RecordingAuditSink
from switchboard.relay_agent import RelayAgent
from switchboard.relay_protocol import (
    Close,
    Hello,
    Ping,
    Pong,
    RelayMessage,
    WebSocketConnection,
    Welcome,
    connect,
    decode_message,
    encode_message,
)
from tests.support.switchboard_support import RELAY_TOKEN, FakeHttpClient, StaticCredentials, make_config


FAST_BACKOFF = (0.05, 0.1, 0.2)


def wait_until(predicate: Callable[[], bool], timeout: float = 45.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def ready_event(number: int, scope: str) -> CanonicalEvent:
    return CanonicalEvent(
        source="github",
        event_type="work.ready",
        external_id=f"ready:{scope}:{number}:{time.monotonic_ns()}",
        scope=scope,
        correlation_key=f"github:kun-g/hibernaut#{number}",
        subject=f"task {number}",
        metadata={"work_kind": "issue", "state": "open", "task_id": f"T{number}"},
    )


class TokenBroker:
    def __init__(self, token: str | None) -> None:
        self.token = token

    def get_webhook_secret(self, channel: str) -> str | None:
        return None

    def issue_ephemeral(self, channel: str) -> str | None:
        return self.token if channel == "relay" else None


class RelayHarness:
    def __init__(
        self,
        relay: Mapping[str, Any] | None = None,
        delivery: Mapping[str, Any] | None = None,
    ) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        sections: dict[str, Mapping[str, Any]] = {
            "relay": {"enabled": True, "transport": "hybrid", **(relay or {})}
        }
        if delivery is not None:
            sections["delivery"] = delivery
        config = make_config(self.tmp.name, **sections)
        config = replace(config, http=replace(config.http, port=0))
        self.audit = RecordingAuditSink()
        self.board = build_switchboard(
            config,
            credentials=StaticCredentials(),
            audit=self.audit,
            http_client=FakeHttpClient(),
        )
        self.board.start()
        hub = self.board.relay
        assert hub is not None
        self.hub = hub
        self.agents: list[RelayAgent] = []

    @property
    def url(self) -> str:
        return self.board.relay_url

    def agent(
        self,
        handler: Callable[[CanonicalEvent], DeliverStatus | None],
        *,
        url: str | None = None,
        token: str | None = RELAY_TOKEN,
        consumer_id: str = "hibernaut",
        **options: Any,
    ) -> RelayAgent:
        agent = RelayAgent(
            url or self.url,
            consumer_id,
            handler,
            TokenBroker(token),
            backoff_seconds=FAST_BACKOFF,
            **options,
        )
        agent.start()
        self.agents.append(agent)
        return agent

    def delivery(self, event_id: str, consumer: str = "hibernaut") -> tuple[str, int]:
        row = self.board.store.get_delivery(event_id, consumer)
        assert row is not None
        return row.status, row.attempts

    def relay_audits(self, outcome: str) -> list[dict[str, Any]]:
        return [r for r in self.audit.records if r["stage"] == "relay" and r["outcome"] == outcome]

    def close(self) -> None:
        for agent in self.agents:
            agent.stop()
        self.board.stop()
        self.tmp.cleanup()


def recv_message(ws: WebSocketConnection) -> RelayMessage | Close:
    while True:
        received = ws.recv()
        if isinstance(received, Close):
            return received
        message = decode_message(received)
        if not isinstance(message, (Ping, Pong)):
            return message


def handshake(url: str, token: str = RELAY_TOKEN, consumer_id: str = "hibernaut") -> tuple[WebSocketConnection, Welcome]:
    ws = connect(url, timeout=5)
    ws.send_text(encode_message(Hello(consumer_id=consumer_id, token=token)))
    welcome = recv_message(ws)
    assert isinstance(welcome, Welcome), welcome
    return ws, welcome
