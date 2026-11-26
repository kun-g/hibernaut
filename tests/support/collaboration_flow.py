from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.app.config import HibernautConfig, parse_config
from agent_runtime.app.main import AlwaysOnDaemon, build_daemon_from_config
from agent_runtime.core.models import CLOSEOUT_SOURCE
from taskboard.adapters.http_server import TaskboardHTTPServer
from taskboard.adapters.mcp_tools import TaskboardMCPServer
from taskboard.adapters.sqlite_store import SQLiteTaskStore
from taskboard.engine.ingestion import IngestionService
from tests.support.collaboration_wiring import config_of


BOARD_TOKEN = "board-token-for-tests"
HUB_TOKEN = "hub-token-for-tests"
MCP_TOKEN = "mcp-token-for-tests"


class Board:
    def __init__(self, tmp_path: Path) -> None:
        self.ingestion = IngestionService(SQLiteTaskStore(str(tmp_path / "board.db")))
        self._server: TaskboardHTTPServer | None = None
        self._port = 0
        self.start()

    def start(self) -> None:
        self._server = TaskboardHTTPServer(
            TaskboardMCPServer(self.ingestion), host="127.0.0.1", port=self._port, base_path="", token=BOARD_TOKEN,
            max_body_bytes=1_000_000,
        )
        self._server.start()
        self._port = self._server.port

    def stop(self) -> None:
        if self._server is not None:
            self._server.stop()
            self._server = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._port}/mcp"


@pytest.fixture
def board(tmp_path: Path) -> Iterator[Board]:
    opened = Board(tmp_path)
    yield opened
    opened.stop()


def settings(tmp_path: Path, board: Board | None = None, *, hub: bool = False, **over: Any) -> HibernautConfig:
    collaboration: dict[str, Any] = {"enabled": True, "hub": {"enabled": hub, "listen_port": 0, "base_path": "/hub"}}
    if board is not None:
        collaboration["taskboard"] = {"mcp_url": board.url}
    return parse_config(config_of(
        tmp_path, collaboration=collaboration, channels={"mcp_enabled": True}, **over,
    ))


@pytest.fixture
def tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIBERNAUT_TASKBOARD_MCP_TOKEN", BOARD_TOKEN)
    monkeypatch.setenv("HIBERNAUT_COLLABORATION_TOKEN", HUB_TOKEN)
    monkeypatch.setenv("HIBERNAUT_MCP_TOKEN", MCP_TOKEN)


@pytest.fixture
def daemon(tmp_path: Path, board: Board, tokens: None) -> Iterator[AlwaysOnDaemon]:
    opened = build_daemon_from_config(settings(tmp_path, board, hub=True))
    yield opened
    opened.close()


def rpc(daemon: AlwaysOnDaemon, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    assert daemon.mcp_server is not None
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}).encode()
    reply = daemon.mcp_server.handle_http(body, {"Authorization": f"Bearer {MCP_TOKEN}"})
    assert isinstance(reply.body, dict)
    return reply.body


def call(daemon: AlwaysOnDaemon, tool: str, **arguments: Any) -> dict[str, Any]:
    reply = rpc(daemon, "tools/call", {"name": tool, "arguments": arguments})
    if "error" in reply:
        raise AssertionError(reply["error"]["message"])
    return reply["result"]["structuredContent"]


def refused(daemon: AlwaysOnDaemon, tool: str, **arguments: Any) -> str:
    reply = rpc(daemon, "tools/call", {"name": tool, "arguments": arguments})
    assert "error" in reply, reply
    return str(reply["error"]["message"])


CARD: dict[str, Any] = {
    "session_id": "s-1", "agent_id": "pool:architect", "project_key": "hibernaut", "summary": "Boundaries locked",
    "decisions": [{"key": "D6-A", "title": "Sync", "choice": "Explicit card"}],
}


def hub_call(daemon: AlwaysOnDaemon, path: str, *, post: bool = False, token: str = HUB_TOKEN) -> tuple[int, Any]:
    assert daemon.collaboration is not None and daemon.collaboration.hub_server is not None
    host, port = daemon.collaboration.hub_server.address
    request = urllib.request.Request(
        f"http://{host}:{port}/hub/api/{path}", data=b"{}" if post else None,
        headers={"Authorization": f"Bearer {token}"}, method="POST" if post else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as reply:
            return reply.status, json.loads(reply.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def chief_events(daemon: AlwaysOnDaemon) -> list[Any]:
    return [e for e in daemon.inbox.list_events(inbox_id="user:main", limit=50) if e.source == CLOSEOUT_SOURCE]
