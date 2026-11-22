from __future__ import annotations

import ssl
import threading
from typing import Protocol

from switchboard.envelope import CanonicalEvent
from switchboard.http_client import HttpClient
from switchboard.mcp_publisher import SwitchboardMCPPublisher
from switchboard.ports import ConsumerHandler, CredentialBrokerPort, DeliverStatus, IngestResult
from switchboard.relay_agent import RelayAgent


class SwitchboardBus(Protocol):
    def publish(self, event: CanonicalEvent) -> IngestResult: ...

    def subscribe(self, consumer_id: str, handler: ConsumerHandler) -> None: ...


# Stage B: the same publish/subscribe surface as the in-process bus, but downlink events arrive over the
# RelayAgent's outbound WebSocket and execution.reported goes back over HTTP /mcp.
class SwitchboardRelayLink:
    def __init__(
        self,
        *,
        relay_url: str,
        mcp_url: str,
        consumer_id: str,
        credentials: CredentialBrokerPort,
        ssl_context: ssl.SSLContext | None = None,
        http_client: HttpClient | None = None,
    ) -> None:
        self.consumer_id = consumer_id
        self.publisher = SwitchboardMCPPublisher(mcp_url, credentials, http_client=http_client)
        self.agent = RelayAgent(relay_url, consumer_id, self._deliver, credentials, ssl_context=ssl_context)
        self._lock = threading.Lock()
        self._handler: ConsumerHandler | None = None

    @property
    def relay_url(self) -> str:
        return self.agent.url

    @property
    def mcp_url(self) -> str:
        return self.publisher.mcp_url

    def publish(self, event: CanonicalEvent) -> IngestResult:
        return self.publisher.publish(event)

    # The agent connects on the first subscribe, so no push arrives before a handler can store it;
    # a later subscribe (config reload) only swaps the handler and keeps the connection.
    def subscribe(self, consumer_id: str, handler: ConsumerHandler) -> None:
        if consumer_id != self.consumer_id:
            raise ValueError(f"relay link is connected as {self.consumer_id!r}, not {consumer_id!r}")
        with self._lock:
            self._handler = handler
        self.agent.start()

    def stop(self) -> None:
        self.agent.stop()

    def _deliver(self, event: CanonicalEvent) -> DeliverStatus | None:
        with self._lock:
            handler = self._handler
        if handler is None:
            return "offline"
        return handler(event)
