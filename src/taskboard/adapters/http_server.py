from __future__ import annotations

import hmac
import json
import threading
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

from taskboard.adapters.mcp_tools import TaskboardMCPServer


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

    def __init__(
        self,
        address: tuple[str, int],
        mcp: TaskboardMCPServer,
        *,
        base_path: str,
        max_body_bytes: int,
        token: str | None,
    ) -> None:
        self.mcp = mcp
        self.max_body_bytes = max_body_bytes
        self.token = token
        # Bare paths stay routable so a prefix-stripping proxy (Caddy handle_path) and in-container
        # probes work whatever base_path is; a prefix-keeping proxy (Traefik PathPrefix) uses base_path.
        self.health_paths = frozenset({"/healthz", f"{base_path}/healthz"})
        self.mcp_paths = frozenset({"/mcp", f"{base_path}/mcp"})
        super().__init__(address, _Handler)


class _Handler(BaseHTTPRequestHandler):
    server_version = "Taskboard"
    timeout = 10.0

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def do_GET(self) -> None:
        if self._path() in self._srv().health_paths:
            self._reply(200, {"status": "ok"})
            return
        self._reply(404, {"error": "not found"})

    def do_POST(self) -> None:
        srv = self._srv()
        if self._path() not in srv.mcp_paths:
            self._reply(404, {"error": "not found"})
            return
        length_header = self.headers.get("Content-Length")
        if length_header is None:
            self._reply(411, {"error": "Content-Length required"})
            return
        try:
            length = int(length_header)
        except ValueError:
            self._reply(400, {"error": "invalid Content-Length"})
            return
        # Reject oversized bodies before reading them.
        if length < 0 or length > srv.max_body_bytes:
            self.close_connection = True
            self._reply(413, {"error": "payload too large"})
            return
        if not self._authorized():
            self.close_connection = True
            self._reply(401, {"error": "invalid bearer token"})
            return
        try:
            raw_body = self.rfile.read(length)
        except TimeoutError:
            self.close_connection = True
            self._reply(408, {"error": "request body timed out"})
            return
        if len(raw_body) != length:
            self.close_connection = True
            return
        try:
            request = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            request = None
        if not isinstance(request, dict):
            self._reply(400, {"error": "invalid json-rpc payload"})
            return
        response = srv.mcp.handle_rpc(cast(dict[str, Any], request))
        if response is None:
            self._reply(202, None)
            return
        self._reply(200, response)

    # Fail closed: without TASKBOARD_MCP_TOKEN no request is authorized.
    def _authorized(self) -> bool:
        expected = self._srv().token
        supplied = self.headers.get("Authorization", "")
        if not expected or not supplied.startswith("Bearer "):
            return False
        return hmac.compare_digest(
            expected.encode("utf-8"), supplied.removeprefix("Bearer ").strip().encode("utf-8")
        )

    def _srv(self) -> _Server:
        return cast(_Server, self.server)

    def _path(self) -> str:
        return self.path.split("?", 1)[0]

    def _reply(self, status: int, body: Mapping[str, Any] | None) -> None:
        payload = b"" if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        if body is not None:
            self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class TaskboardHTTPServer:
    def __init__(
        self,
        mcp: TaskboardMCPServer,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        base_path: str = "",
        max_body_bytes: int = 1_048_576,
        token: str | None = None,
    ) -> None:
        self._server = _Server(
            (host, port), mcp, base_path=base_path, max_body_bytes=max_body_bytes, token=token
        )
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    @property
    def base_url(self) -> str:
        return f"http://{self._server.server_address[0]}:{self.port}"

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.05},
            name="taskboard-http",
            daemon=True,
        )
        self._thread.start()

    # shutdown() blocks until serve_forever exits, so it is only called on a started server.
    def stop(self) -> None:
        if self._thread is not None:
            self._server.shutdown()
            self._thread.join(5)
            self._thread = None
        self._server.server_close()
