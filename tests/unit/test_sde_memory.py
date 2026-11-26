from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest

from agent_runtime.adapters.executor_cli import memory_access, memory_mcp
from agent_runtime.adapters.executor_cli.claude_code import with_mcp_server
from agent_runtime.adapters.executor_cli.memory_access import MemoryAccess, channel_path
from agent_runtime.adapters.executor_cli.orca import HANDOFF_SERVER, HANDOFF_TOOLS, with_handoff, with_peer_handoff
from agent_runtime.adapters.executor_cli.steward_tools import StewardTools
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import SQLiteMemoryGrantStore
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config, main
from tests.support.sde_memory import PERMITTED, SERVER, TOOLS, World, _toml, servers, world


def test_the_tools_are_those_of_the_steward(world: World) -> None:
    steward = {tool["name"]: tool["inputSchema"]["properties"] for tool in StewardTools.definitions()}
    own = {tool["name"]: tool["inputSchema"]["properties"] for tool in memory_mcp.tool_definitions()}
    assert {name: steward[name] for name in TOOLS} == own


def test_the_default_policy_allows_the_two_tools(world: World) -> None:
    assert {AutoModePermissionAdapter().check(name, {}).action for name in TOOLS} == {"allow"}


def test_a_runtime_that_cannot_open_the_channel_still_runs(
    world: World, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    blocked = tmp_path / "file"
    blocked.write_text("")
    access = MemoryAccess(grants=SQLiteMemoryGrantStore(world.db), sessions=world.sessions, path=blocked / "d" / "s")

    async def scenario() -> None:
        async with access.serving():
            pass

    asyncio.run(scenario())
    assert "memory tools" in capsys.readouterr().err


def test_where_the_channel_is(tmp_path: Path) -> None:
    assert channel_path(":memory:") is None
    short = Path(tempfile.mkdtemp(dir="/tmp")).resolve()
    try:
        assert channel_path(short / "hb.db") == short / ".hibernaut-memory" / "hb.db.sock"
    finally:
        short.rmdir()
    long = tmp_path / ("x" * 90) / "hb.db"
    path = channel_path(long)
    assert path is not None and len(os.fsencode(path)) <= 100 and path == channel_path(long)
    assert path != channel_path(long.with_name("other.db"))


def test_the_memory_server_goes_beside_the_handoffs(tmp_path: Path) -> None:
    server = {"command": "python", "args": []}
    args = with_mcp_server(("--allowedTools", "Read,Bash", "--x"), SERVER, server, TOOLS)
    assert args[:2] == ["--allowedTools", ",".join(("Read", "Bash", *(f"mcp__{SERVER}__{t}" for t in TOOLS)))]
    assert servers(args) == {SERVER: server}
    both = with_handoff(args, tmp_path)
    assert set(servers(both)) == {SERVER, HANDOFF_SERVER}
    assert {*HANDOFF_TOOLS, *PERMITTED, "Read"} <= set(both[1].split(","))
    peer = with_peer_handoff(args)
    assert servers(peer) == {SERVER: server} and set(PERMITTED) <= set(peer[1].split(","))


def test_a_demo_and_a_database_in_memory_have_no_channel(tmp_path: Path) -> None:
    for db_path, demo in ((":memory:", False), (str(tmp_path / "hb.db"), True)):
        daemon = build_daemon_from_config(parse_config({
            "runtime": {"db_path": db_path, "memory_root": str(tmp_path / "memory")},
        }), demo=demo)
        try:
            assert daemon.memory_access is None
        finally:
            daemon.close()


def test_the_stdio_companion_leaves_the_daemons_channel_alone(tmp_path: Path) -> None:
    path = tmp_path / "hibernaut.toml"
    path.write_text(_toml({"runtime": {"db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory")}}))
    channel = channel_path(tmp_path / "hb.db")
    assert channel is not None
    seen: list[bool] = []
    with patch("agent_runtime.adapters.channels.mcp_server.MCPServerAdapter.serve_stdio",
               side_effect=lambda *_: seen.append(channel.exists())), \
            patch("sys.argv", ["hibernaut", "--config", str(path), "--mcp-stdio"]):
        main()
    assert seen == [False]


class TestNothingElseGetsThem(unittest.TestCase):
    def test_the_developers_executor_is_the_only_one_that_is_given_the_channel(self) -> None:
        root = Path(memory_access.__file__).parent
        named = {p.name for p in root.glob("*.py") if "memory_access" in p.read_text()}
        self.assertEqual(named, {"sde.py"})
