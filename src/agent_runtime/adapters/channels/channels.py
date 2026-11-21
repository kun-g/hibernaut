from __future__ import annotations

import json
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from agent_runtime.adapters.channels.feishu_webhook import FeishuWebhookAdapter
from agent_runtime.adapters.channels.github_webhook import GitHubWebhookAdapter
from agent_runtime.adapters.channels.mcp_server import MCPServerAdapter
from agent_runtime.engine.handler import EventHandler


class ConsoleChannelAdapter:
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


def webhook_request_handler(
    handler: EventHandler,
    *,
    github_adapter: GitHubWebhookAdapter | None = None,
    feishu_adapter: FeishuWebhookAdapter | None = None,
    mcp_adapter: MCPServerAdapter | None = None,
) -> type[BaseHTTPRequestHandler]:
    class _IngestRequestHandler(BaseHTTPRequestHandler):
        def _write_json(self, status_code: int, payload: dict[str, Any]) -> None:
            encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            header_map = {k: v for k, v in self.headers.items()}

            if self.path == "/events":
                body = json.loads(raw.decode("utf-8"))
                result = handler.ingest(
                    inbox_id=str(body.get("inbox_id", "default")),
                    source=str(body.get("source", "webhook")),
                    external_id=str(body["external_id"]),
                    payload=dict(body.get("payload", {})),
                    session_id=body.get("session_id"),
                    correlation_key=body.get("correlation_key"),
                )
                self._write_json(
                    HTTPStatus.ACCEPTED,
                    {
                        "event_id": result.event_id,
                        "status": result.status,
                        "duplicate": result.duplicate,
                    },
                )
                return

            if self.path == "/webhooks/github" and github_adapter is not None:
                resp = github_adapter.handle(raw, header_map)
                self._write_json(resp.status_code, resp.body)
                return

            if self.path == "/webhooks/feishu" and feishu_adapter is not None:
                resp = feishu_adapter.handle(raw, header_map)
                self._write_json(resp.status_code, resp.body)
                return

            if self.path == "/mcp" and mcp_adapter is not None and mcp_adapter.http_enabled:
                resp = mcp_adapter.handle_http(raw, header_map)
                self._write_json(resp.status_code, resp.body)
                return

            self._write_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            _ = (format, args)

    return _IngestRequestHandler


def create_webhook_server(
    host: str,
    port: int,
    handler: EventHandler,
    *,
    github_adapter: GitHubWebhookAdapter | None = None,
    feishu_adapter: FeishuWebhookAdapter | None = None,
    mcp_adapter: MCPServerAdapter | None = None,
) -> HTTPServer:
    return HTTPServer((host, port), webhook_request_handler(
        handler, github_adapter=github_adapter, feishu_adapter=feishu_adapter,
        mcp_adapter=mcp_adapter,
    ))
