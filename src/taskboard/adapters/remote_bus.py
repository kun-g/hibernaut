from __future__ import annotations

import ssl
import threading

from switchboard.envelope import CanonicalEvent
from switchboard.http_client import HttpClient
from switchboard.mcp_publisher import SwitchboardMCPPublisher
from switchboard.ports import ConsumerHandler, CredentialBrokerPort, DeliverStatus, IngestResult
from switchboard.relay_agent import RelayAgent


class RemotePublishRejectedError(RuntimeError):
    def __init__(self, event: CanonicalEvent, problems: tuple[str, ...]) -> None:
        super().__init__(f"switchboard rejected {event.event_type} {event.external_id}: {'; '.join(problems)}")
        self.problems = problems


# EventBusPort over the network: downlink pushes arrive on the RelayAgent's outbound WebSocket, uplink
# work.ready / execution.reported go to the hub's /mcp. Without relay_url it only publishes (stdio mode).
class SwitchboardRemoteBus:
    def __init__(
        self,
        *,
        mcp_url: str,
        credentials: CredentialBrokerPort,
        relay_url: str | None = None,
        consumer_id: str = "taskboard",
        ssl_context: ssl.SSLContext | None = None,
        http_client: HttpClient | None = None,
    ) -> None:
        self.consumer_id = consumer_id
        self.publisher = SwitchboardMCPPublisher(mcp_url, credentials, http_client=http_client)
        self.agent = (
            RelayAgent(relay_url, consumer_id, self._deliver, credentials, ssl_context=ssl_context)
            if relay_url
            else None
        )
        self._lock = threading.Lock()
        self._handler: ConsumerHandler | None = None

    # An "invalid" result means the hub will never accept this envelope; raising turns it into
    # PublishFailedError, so the inbound event is nacked instead of silently losing the dispatch.
    def publish(self, event: CanonicalEvent) -> IngestResult:
        result = self.publisher.publish(event)
        if result.status == "invalid":
            raise RemotePublishRejectedError(event, result.problems)
        return result

    # The agent connects on the first subscribe, so no push arrives before a handler can store it.
    def subscribe(self, consumer_id: str, handler: ConsumerHandler) -> None:
        if self.agent is None:
            raise RuntimeError("remote bus has no relay_url; it can only publish")
        if consumer_id != self.consumer_id:
            raise ValueError(f"relay link is connected as {self.consumer_id!r}, not {consumer_id!r}")
        with self._lock:
            self._handler = handler
        self.agent.start()

    def stop(self) -> None:
        if self.agent is not None:
            self.agent.stop()

    def _deliver(self, event: CanonicalEvent) -> DeliverStatus | None:
        with self._lock:
            handler = self._handler
        if handler is None:
            return "offline"
        return handler(event)
