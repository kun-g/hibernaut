import io
import socket
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPResponse
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from switchboard import http_server as switch_http
from switchboard.inbound.mcp import MCPInboundAdapter
from taskboard.adapters import http_server as task_http
from tests.support.switchboard_support import MCP_TOKEN, StaticCredentials


@pytest.fixture(params=[task_http, switch_http], ids=["taskboard", "switchboard"])
def endpoint(request):
    server = SimpleNamespace(token=MCP_TOKEN, mcp_paths={"/mcp"}, max_body_bytes=1024,
                             begin=lambda _: True, cut=False, pipeline=Mock())
    server.routes = {"/mcp": MCPInboundAdapter(StaticCredentials(), server.pipeline, Mock(), Mock())}
    return request.param._Handler, server


@pytest.mark.parametrize("auth", [b"", b"Authorization: Bearer wrong\r\n"])
def test_unauthorized_request_does_not_read_body(endpoint, auth):
    handler, server = endpoint
    stream = Mock(wraps=io.BytesIO(b"POST /mcp HTTP/1.0\r\nContent-Length: 10\r\n" + auth + b"\r\n"))
    conn = Mock()
    conn.makefile.return_value = stream
    handler(conn, ("local", 0), server)
    assert any(b" 401 " in call.args[0] for call in conn.sendall.call_args_list)
    stream.read.assert_not_called()


def test_unauthorized_keepalive_gets_401_and_eof(endpoint, monkeypatch):
    handler, server = endpoint
    monkeypatch.setattr(handler, "protocol_version", "HTTP/1.1")
    conn, client = socket.socketpair()

    def serve():
        with conn:
            handler(conn, ("local", 0), server)

    with ThreadPoolExecutor(max_workers=1) as pool, client:
        done = pool.submit(serve)
        client.settimeout(2)
        client.sendall(b"POST /mcp HTTP/1.1\r\nHost: local\r\nContent-Length: 10\r\n\r\n")
        with HTTPResponse(client) as response:
            response.begin()
            assert response.status == 401
            response.read()
            assert client.recv(1) == b""
            assert response.will_close
        done.result(timeout=2)


def test_authorized_body_read_times_out(endpoint, monkeypatch):
    handler, server = endpoint
    assert handler.timeout is not None and handler.timeout > 0
    monkeypatch.setattr(handler, "timeout", 0.05)
    conn, client = socket.socketpair()
    with conn, client:
        client.sendall(f"POST /mcp HTTP/1.0\r\nContent-Length: 10\r\nAuthorization: Bearer {MCP_TOKEN}\r\n\r\n".encode())
        handler(conn, ("local", 0), server)
        assert b" 408 " in client.recv(1024)
