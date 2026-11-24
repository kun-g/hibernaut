from __future__ import annotations

import ipaddress
import json
import socket
import sys
import threading
import urllib.request
from collections.abc import MutableMapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from agent_runtime.adapters.channels.mcp_server import MCPServerAdapter
from agent_runtime.engine.handler import EventHandler, RemoteRefused, ReservedSource


class WebhookHTTPServer(ThreadingHTTPServer):
    request_queue_size = 128
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        bind_and_activate: bool = True,
    ) -> None:
        self._inflight = 0
        self._idle = threading.Condition()
        super().__init__(server_address, handler_class, bind_and_activate)

    def process_request(
        self,
        request: socket.socket | tuple[bytes, socket.socket],
        client_address: Any,
    ) -> None:
        with self._idle:
            self._inflight += 1
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_finished()
            raise

    def process_request_thread(
        self,
        request: socket.socket | tuple[bytes, socket.socket],
        client_address: Any,
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_finished()

    def _request_finished(self) -> None:
        with self._idle:
            self._inflight -= 1
            self._idle.notify_all()

    def wait_idle(self, timeout: float | None = None) -> bool:
        with self._idle:
            return self._idle.wait_for(lambda: self._inflight == 0, timeout)

    def shutdown(self) -> None:
        # Callers rely on shutdown() meaning every accepted request has finished ingesting,
        # as it did with the single-threaded server (drain before stopping workers or reloading).
        super().shutdown()
        self.wait_idle()

    @classmethod
    def adopt(cls, listener: socket.socket, handler_class: type[BaseHTTPRequestHandler]) -> WebhookHTTPServer:
        host, port = listener.getsockname()[:2]
        server = cls((host, port), handler_class, bind_and_activate=False)
        server.socket.close()
        server.socket = listener
        server.server_name = socket.getfqdn(host)
        server.server_port = port
        return server


# Set by scripts/run_updating.py. LISTEN_FD names a listening socket a previous daemon generation served on;
# HANDOVER_FD names a Unix socket on which this daemon sends its listener back when it stops. As long as one of the
# processes holds the listener, connections made between two generations wait in its backlog instead of being refused.
LISTEN_FD_ENV = "HIBERNAUT_LISTEN_FD"
HANDOVER_FD_ENV = "HIBERNAUT_HANDOVER_FD"


def _bound_to(sock: socket.socket, host: str, port: int) -> bool:
    try:
        wanted = socket.getaddrinfo(host or None, port, sock.family, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
    except OSError:
        return False
    return port != 0 and sock.getsockname()[:2] in {info[4][:2] for info in wanted}


def _adopt_fd(value: str | None) -> socket.socket | None:
    if value is None:
        return None
    try:
        sock = socket.socket(fileno=int(value))
    except (ValueError, OSError):
        return None
    # Executor subprocesses must not keep the listener (or the channel) open after this process lets it go.
    sock.set_inheritable(False)
    return sock


class ListenerHandover:
    def __init__(self, inherited: socket.socket | None = None, channel: socket.socket | None = None) -> None:
        self._inherited = inherited
        self._channel = channel

    @classmethod
    def from_environ(cls, environ: MutableMapping[str, str]) -> ListenerHandover:
        return cls(_adopt_fd(environ.pop(LISTEN_FD_ENV, None)), _adopt_fd(environ.pop(HANDOVER_FD_ENV, None)))

    def server(self, address: tuple[str, int], handler_class: type[BaseHTTPRequestHandler]) -> WebhookHTTPServer:
        # The inherited listener is used at most once, and only for the configured address; a listener left over
        # from before a port change is closed so its port refuses connections again.
        inherited, self._inherited = self._inherited, None
        if inherited is not None:
            if inherited.type == socket.SOCK_STREAM and _bound_to(inherited, *address):
                print(f"[hibernaut] Serving HTTP on the listener inherited from the previous daemon "
                      f"({address[0]}:{address[1]}).", file=sys.stderr, flush=True)
                return WebhookHTTPServer.adopt(inherited, handler_class)
            inherited.close()
        return WebhookHTTPServer(address, handler_class)

    def release(self, server: WebhookHTTPServer, ack_timeout: float = 10.0) -> None:
        # Called once serve_forever() has stopped. Our copy stays open until the supervisor acks: on macOS a socket
        # that is only in flight is closed, queued connections and all, when the sender closes its last copy.
        channel, self._channel = self._channel, None
        if channel is None:
            return
        with channel:
            try:
                socket.send_fds(channel, [b"listener"], [server.socket.fileno()])
                channel.settimeout(ack_timeout)
                if not channel.recv(1):
                    raise ConnectionError("the supervisor closed the channel")
            except OSError as exc:
                print(f"[hibernaut] Could not hand the HTTP listener to the supervisor: {exc!r}",
                      file=sys.stderr, flush=True)

    def close(self) -> None:
        for sock in (self._inherited, self._channel):
            if sock is not None:
                sock.close()
        self._inherited = self._channel = None


class ConsoleChannelAdapter:
    # echo=False when a file Outbox prints the one-line summary instead of the full text.
    def __init__(self, *, echo: bool = True) -> None:
        self.echo = echo
        self.delivered: list[tuple[str, str, dict[str, Any]]] = []

    async def send(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None:
        self.delivered.append((inbox_id, content, dict(metadata)))
        if not self.echo:
            return
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

        def _remote(self) -> bool:
            # A request a proxy passed on comes from this host only in name.
            if self.headers.get("Forwarded") or self.headers.get("X-Forwarded-For"):
                return True
            return not ipaddress.ip_address(str(self.client_address[0]).partition("%")[0]).is_loopback

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)

            if self.path == "/events":
                body = json.loads(raw.decode("utf-8"))
                source = str(body.get("source", "webhook"))
                try:
                    result = handler.ingest(
                        inbox_id=str(body.get("inbox_id", "default")),
                        source=source,
                        external_id=str(body["external_id"]),
                        payload=dict(body.get("payload", {})),
                        session_id=body.get("session_id"),
                        correlation_key=body.get("correlation_key"),
                        remote=self._remote(),
                    )
                except RemoteRefused as exc:
                    self._write_json(HTTPStatus.FORBIDDEN, {"error": str(exc)})
                    return
                except ReservedSource as exc:
                    self._write_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                    return
                self._write_json(
                    HTTPStatus.ACCEPTED,
                    {
                        "event_id": result.event_id,
                        "status": result.status,
                        "duplicate": result.duplicate,
                    },
                )
                return

            if self.path == "/mcp" and mcp_adapter is not None and mcp_adapter.http_enabled:
                resp = mcp_adapter.handle_http(raw, dict(self.headers.items()), remote=self._remote())
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
    mcp_adapter: MCPServerAdapter | None = None,
) -> WebhookHTTPServer:
    return WebhookHTTPServer((host, port), webhook_request_handler(handler, mcp_adapter=mcp_adapter))
