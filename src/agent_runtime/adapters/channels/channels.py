"""L3 Channel Adapters: Console, outbound Webhook, and inbound HTTP Webhook server."""

from __future__ import annotations

import json
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from agent_runtime.engine.handler import EventHandler


class ConsoleChannelAdapter:
    """Local stdout channel adapter for CLI / demo runs."""

    def __init__(self) -> None:
        self.delivered: list[tuple[str, str, dict[str, Any]]] = []

    async def send(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None:
        self.delivered.append((inbox_id, content, dict(metadata)))
        kind = metadata.get("kind", "emit")
        print(f"[OUTBOX:{kind}] inbox={inbox_id} | {content}")


class WebhookChannelAdapter:
    """Outbound HTTP POST webhook delivery adapter (stdlib only)."""

    def __init__(self, endpoint_url: str, timeout_seconds: float = 10.0) -> None:
        self._endpoint_url = endpoint_url
        self._timeout_seconds = timeout_seconds

    async def send(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None:
        body = json.dumps(
            {"inbox_id": inbox_id, "content": content, "metadata": metadata},
            ensure_ascii=False,
        ).encode("utf-8")
        req = urllib.request.Request(
            url=self._endpoint_url,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=self._timeout_seconds):  # noqa: S310
            return None


def create_webhook_server(
    host: str,
    port: int,
    handler: EventHandler,
) -> HTTPServer:
    """Create a lightweight HTTP server that ingests POST /events and returns immediate ACK."""

    class _IngestRequestHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/events":
                self.send_response(HTTPStatus.NOT_FOUND)
                self.end_headers()
                return

            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            body = json.loads(raw.decode("utf-8"))
            result = handler.ingest(
                inbox_id=str(body.get("inbox_id", "default")),
                source=str(body.get("source", "webhook")),
                external_id=str(body["external_id"]),
                payload=dict(body.get("payload", {})),
                session_id=body.get("session_id"),
            )
            ack = json.dumps(
                {
                    "event_id": result.event_id,
                    "status": result.status,
                    "duplicate": result.duplicate,
                }
            ).encode("utf-8")
            self.send_response(HTTPStatus.ACCEPTED)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(ack)))
            self.end_headers()
            self.wfile.write(ack)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            _ = (format, args)

    return HTTPServer((host, port), _IngestRequestHandler)
