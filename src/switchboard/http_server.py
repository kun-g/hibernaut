from __future__ import annotations

import json
import socket
import sys
import threading
import time
from collections.abc import Callable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Final, cast

from switchboard.pipeline import InboundPipeline, describe_ingest
from switchboard.ports import InboundAdapter, IngestAwareAdapter
from switchboard.relay_protocol import HandshakeError, WebSocketConnection, server_handshake_headers

# Takes over an upgraded connection and returns when it ends; runs on the request thread.
UpgradeHandler = Callable[[WebSocketConnection, str], None]

# During a drain, how long an accepted connection may take to send its request head.
HEAD_GRACE_SEC: Final = 0.5
# How long requests cut at the deadline get to leave their handler.
CUT_SETTLE_SEC: Final = 0.2


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(
        self,
        address: tuple[str, int],
        pipeline: InboundPipeline,
        routes: Mapping[str, InboundAdapter],
        max_body_bytes: int,
        upgrades: Mapping[str, UpgradeHandler],
        base_path: str = "",
    ) -> None:
        self.pipeline = pipeline
        self.routes = dict(routes)
        self.max_body_bytes = max_body_bytes
        self.upgrades = dict(upgrades)
        self.health_paths = frozenset({"/healthz", f"{base_path}/healthz"})
        if base_path:
            # Both forms are served: the bare paths for probes, old hostnames and proxies that strip the prefix,
            # the prefixed ones for a proxy that keeps it (Traefik PathPrefix without StripPrefix).
            self.routes.update({f"{base_path}{path}": adapter for path, adapter in routes.items()})
            self.upgrades.update({f"{base_path}{path}": handler for path, handler in upgrades.items()})
        # Connections still to send a request head, requests being handled, and whether new heads are refused.
        self._state = threading.Condition()
        self._waiting: set[object] = set()
        self._active: set[object] = set()
        self._closed = False
        # Set when a drain ran out of time: the store is about to close, so no request goes any further.
        self.cut = False
        super().__init__(address, _Handler)

    # Counted on the accept thread, so a drain after shutdown() sees every accepted connection.
    def process_request(self, request: Any, client_address: Any) -> None:
        with self._state:
            self._waiting.add(request)
        super().process_request(request, client_address)

    def shutdown_request(self, request: Any) -> None:
        super().shutdown_request(request)
        self.release(request)

    # A connection the drain cut fails in its handler; that is not worth a traceback.
    def handle_error(self, request: Any, client_address: Any) -> None:
        if self.cut:
            print(f"[switchboard] request cut off by the shutdown: {sys.exception()!r}", file=sys.stderr)
        elif not (self._closed and isinstance(sys.exception(), OSError)):
            super().handle_error(request, client_address)

    def begin(self, request: object) -> bool:
        with self._state:
            self._waiting.discard(request)
            if not self._closed:
                self._active.add(request)
            self._state.notify_all()
            return not self._closed

    # Also called for an upgraded connection: from then on it belongs to the upgrade handler.
    def release(self, request: object) -> None:
        with self._state:
            self._waiting.discard(request)
            self._active.discard(request)
            self._state.notify_all()

    def drain(self, deadline: float, on_cut: Callable[[], None] | None) -> bool:
        head_deadline = min(deadline, time.monotonic() + HEAD_GRACE_SEC)
        with self._state:
            while self._active or self._waiting:
                now = time.monotonic()
                if now >= deadline or (not self._active and now >= head_deadline):
                    break
                self._state.wait((deadline if self._active else head_deadline) - now)
            self._closed = True
            drained = not self._active
            self.cut = not drained
            # Before the sockets go, so that no write starts once a client has seen its connection cut.
            if self.cut and on_cut is not None:
                on_cut()
            cut = [*self._waiting, *self._active]
        for request in cut:
            try:
                cast(socket.socket, request).shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if not drained:
            with self._state:
                self._state.wait_for(lambda: not self._active, CUT_SETTLE_SEC)
        return drained


class _Handler(BaseHTTPRequestHandler):
    server_version = "Switchboard"

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def parse_request(self) -> bool:
        if not super().parse_request():
            return False
        if self._srv().begin(self.connection):
            return True
        self._reply(503, {"error": "shutting down"})
        return False

    def do_GET(self) -> None:
        path = self._path()
        if path in self._srv().health_paths:
            self._reply(200, {"status": "ok"})
            return
        upgrade = self._srv().upgrades.get(path)
        if upgrade is not None:
            self._upgrade(upgrade)
            return
        self._reply(404, {"error": "not found"})

    def _upgrade(self, upgrade: UpgradeHandler) -> None:
        headers = {key: value for key, value in self.headers.items()}
        if self.headers.get("Upgrade", "").strip().lower() != "websocket":
            self._reply(426, {"error": "WebSocket upgrade required"})
            return
        try:
            response_headers = server_handshake_headers(headers)
        except HandshakeError as exc:
            self._reply(400, {"error": str(exc)})
            return
        self.send_response(101, "Switching Protocols")
        for name, value in response_headers:
            self.send_header(name, value)
        self.end_headers()
        self.close_connection = True
        self._srv().release(self.connection)
        # rfile may already hold the client's first frame, so frames are read through it.
        ws = WebSocketConnection(
            cast(socket.socket, self.connection),
            self.rfile.read,
            client_side=False,
            max_message_bytes=self._srv().max_body_bytes,
        )
        upgrade(ws, f"{self.client_address[0]}:{self.client_address[1]}")

    def do_POST(self) -> None:
        path = self._path()
        adapter = self._srv().routes.get(path)
        if adapter is None:
            self._reply(404, {"error": "not found"})
            return
        source = path.rstrip("/").rsplit("/", 1)[-1]
        pipeline = self._srv().pipeline

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
        if length < 0 or length > self._srv().max_body_bytes:
            pipeline.record_rejection(
                stage="http", http_status=413, source=source, reason=f"body of {length} bytes"
            )
            self.close_connection = True
            self._reply(413, {"error": "payload too large"})
            return
        raw_body = self.rfile.read(length)
        if len(raw_body) < length:
            # The client left, or the drain cut the connection; a partial body is never handled.
            self.close_connection = True
            return
        headers = {key: value for key, value in self.headers.items()}

        try:
            parsed = adapter.handle(headers, raw_body)
        except Exception as exc:  # noqa: BLE001
            if self._srv().cut:
                self.close_connection = True
                return
            pipeline.record_rejection(stage="inbound", http_status=500, source=source, reason=repr(exc))
            self._reply(500, {"error": "internal error"})
            return

        if self._srv().cut:
            self.close_connection = True
            return
        if parsed.event is None:
            if parsed.http_status in (400, 401):
                reason = str((parsed.response_body or {}).get("error", "rejected"))
                pipeline.record_rejection(
                    stage="inbound", http_status=parsed.http_status, source=source, reason=reason
                )
            self._reply(parsed.http_status, parsed.response_body or {})
            return

        ingest = pipeline.ingest(parsed.event)
        if isinstance(adapter, IngestAwareAdapter):
            final = adapter.finalize(parsed, ingest)
            status = final.http_status
            body: dict[str, Any] = final.response_body or dict(describe_ingest(ingest))
        else:
            status = 400 if ingest.status == "invalid" else parsed.http_status
            body = {**(parsed.response_body or {}), **describe_ingest(ingest)}
        self._reply(status, body)

    def _srv(self) -> _Server:
        return cast(_Server, self.server)

    def _path(self) -> str:
        return self.path.split("?", 1)[0]

    def _reply(self, status: int, body: Mapping[str, Any]) -> None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class SwitchboardHTTPServer:
    def __init__(
        self,
        pipeline: InboundPipeline,
        routes: Mapping[str, InboundAdapter],
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        max_body_bytes: int = 1_048_576,
        upgrades: Mapping[str, UpgradeHandler] | None = None,
        base_path: str = "",
    ) -> None:
        self._server = _Server((host, port), pipeline, routes, max_body_bytes, upgrades or {}, base_path)
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    @property
    def base_url(self) -> str:
        host = str(self._server.server_address[0])
        return f"http://{host}:{self.port}"

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.02},
            name="switchboard-http",
            daemon=True,
        )
        self._thread.start()

    def serve_forever(self) -> None:
        self._server.serve_forever(poll_interval=0.02)

    # shutdown() blocks until serve_forever exits, so it is only called on a started server.
    def stop_accepting(self) -> None:
        if self._thread is not None:
            self._server.shutdown()
            self._thread.join(5)
            self._thread = None
        self._server.server_close()

    # Waits for the requests already accepted; afterwards none is handled any more. False: some are still running,
    # and on_cut was called before their connections were cut.
    def drain(self, deadline: float, on_cut: Callable[[], None] | None = None) -> bool:
        return self._server.drain(deadline, on_cut)

    def stop(self, timeout: float = 5.0) -> None:
        self.stop_accepting()
        self.drain(time.monotonic() + timeout)
