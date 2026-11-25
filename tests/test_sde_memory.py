from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import sqlite3
import stat
import tempfile
import unittest
from collections.abc import AsyncGenerator, Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager, memory_access, memory_mcp
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeProfile, with_mcp_server
from agent_runtime.adapters.executor_cli.memory_access import MemoryAccess, channel_path
from agent_runtime.adapters.executor_cli.orca import HANDOFF_SERVER, HANDOFF_TOOLS, with_handoff, with_peer_handoff
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.executor_cli.steward_tools import StewardTools
from agent_runtime.adapters.memory_fs import ExpertMount, GovernedMemoryHub, WorkspacePartitionRegistry, open_role_store
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteMemoryGrantStore, SQLiteSessionStoreAdapter
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon_from_config, main, serve_configured
from agent_runtime.core.models import Emit, Event, PermissionDecision, SessionRef, Suspend
from collaboration import ExpertPoolRegistry
from memory_hub import KnowledgeMountConfig, MemoryHubConfig
from tests.fakes import InMemoryPermission
from tests.test_collaboration_wiring import config_of
from tests.test_resume_runs import SDERestartCase, committed, until, verdict_pass
from tests.test_sde_executor import BUGGY, CHANGES_REQUESTED, FIXED, PASS, dev, event
from tests.test_visible_roles import VisibleCase, arg

ALPHA, BETA, EXPERT = "proj:alpha", "proj:beta", "proj:alpha:architect"
SERVER = "hibernaut_memory"
TOOLS = ("memory_read", "memory_search")
PERMITTED = [f"mcp__{SERVER}__{name}" for name in TOOLS]
UNAVAILABLE = "cannot be reached"


class Session:
    # The stdio MCP server of one agent session, started from its --mcp-config entry the way Claude Code starts it.
    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self.proc = proc
        self._ids = iter(range(1, 1000))

    @classmethod
    async def start(cls, server: dict[str, Any]) -> Session:
        session = cls(await asyncio.create_subprocess_exec(
            server["command"], *server["args"], stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            limit=1 << 20,
        ))
        await session.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {}})
        return session

    async def rpc(self, method: str, params: Any = None) -> dict[str, Any]:
        assert self.proc.stdin is not None and self.proc.stdout is not None
        rid = next(self._ids)
        request = {"jsonrpc": "2.0", "id": rid, "method": method, "params": {} if params is None else params}
        self.proc.stdin.write(json.dumps(request).encode() + b"\n")
        await self.proc.stdin.drain()
        reply = json.loads(await asyncio.wait_for(self.proc.stdout.readline(), 20))
        assert reply["id"] == rid
        return reply

    async def call(self, name: str, **arguments: Any) -> tuple[bool, str]:
        result = (await self.rpc("tools/call", {"name": name, "arguments": arguments}))["result"]
        return bool(result.get("isError")), "".join(part["text"] for part in result["content"])

    async def refused(self, name: str, **arguments: Any) -> str:
        failed, text = await self.call(name, **arguments)
        assert failed, text
        return text

    async def found(self, name: str, **arguments: Any) -> dict[str, Any]:
        failed, text = await self.call(name, **arguments)
        assert not failed, text
        return json.loads(text)

    async def search(self, query: str, **options: Any) -> set[str]:
        return {hit["uri"] for hit in (await self.found("memory_search", query=query, **options))["items"]}

    async def read(self, uri: str, **options: Any) -> str:
        return (await self.found("memory_read", uri=uri, **options))["content"]

    async def end(self) -> int:
        assert self.proc.stdin is not None
        self.proc.stdin.close()
        return await asyncio.wait_for(self.proc.wait(), 20)


class World:
    # Two projects' memory and one process's memory channel over them; the database outlives the process.
    def __init__(self, root: Path) -> None:
        self.root = root
        kb = root / "kb"
        kb.mkdir()
        (kb / "card.md").write_text("# Layering\nlayering card of the knowledge base\n")
        self.hub = GovernedMemoryHub(MemoryHubConfig(
            memory_root=str(root / "memory"), mounts=(KnowledgeMountConfig(name="pkb", root=str(kb)),),
        ))
        for key in ("alpha", "beta"):
            self.hub.lay_out(key)
            notes = self.hub.project_dir(key) / "notes"
            (notes / "tried.md").write_text(f"# Layering\nlayering was tried in {key}\n\n## Outcome\nno gain in {key}\n")
        shared = self.hub.root / "shared" / "notes"
        shared.mkdir(parents=True, exist_ok=True)
        (shared / "common.md").write_text("# Layering\nlayering for everybody\n")
        self.roles = open_role_store(self.hub)
        plan = ExpertPoolRegistry().resolve_for_project(
            role_id="architect", project_key="alpha", extra_readonly_projects=(), budget_chars=6500
        ).mount_plan
        self.registry = WorkspacePartitionRegistry(
            self.hub.root, hub=self.hub,
            mounts=lambda inbox_id: ExpertMount(plan, self.roles) if inbox_id == EXPERT else None,
        )
        self.db = SQLiteDatabase(root / "hb.db")
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        for session_id, inbox_id in (("sess-alpha", ALPHA), ("sess-beta", BETA), ("sess-expert", EXPERT)):
            self.sessions.save(SessionRef(session_id=session_id, inbox_id=inbox_id, executor_id="sde"))
        self.permissions: Any = InMemoryPermission()
        self.access = self.process(self.db)

    def process(self, db: SQLiteDatabase) -> MemoryAccess:
        path = channel_path(self.root / "hb.db")
        assert path is not None
        access = MemoryAccess(grants=SQLiteMemoryGrantStore(db), sessions=SQLiteSessionStoreAdapter(db), path=path)
        access.activate(self.registry.memory_for, self.permissions)
        return access

    @contextlib.asynccontextmanager
    async def session(self, session_id: str) -> AsyncGenerator[Session]:
        session = await Session.start(self.access.mcp_server(session_id))
        try:
            yield session
        finally:
            assert await session.end() == 0

    def files(self) -> dict[str, str]:
        return {str(p.relative_to(self.hub.root)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in self.hub.root.rglob("*") if p.is_file() and ".index" not in p.parts}

    def close(self) -> None:
        self.hub.close()
        self.db.close()


@pytest.fixture
def world(tmp_path: Path) -> Iterator[World]:
    opened = World(tmp_path)
    yield opened
    opened.close()


# --- the tools, through a real MCP process -----------------------------------------------------------------------


def test_a_session_reads_and_searches_its_project_the_shared_memory_and_the_knowledge_base(world: World) -> None:
    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            listed = (await session.rpc("tools/list"))["result"]["tools"]
            assert sorted(tool["name"] for tool in listed) == list(TOOLS)
            assert "tried in alpha" in await session.read("notes/tried.md")
            assert await session.read("proj://alpha/notes/tried.md", heading="Outcome") == "## Outcome\nno gain in alpha"
            assert "everybody" in await session.read("shared://notes/common.md")
            assert "knowledge base" in await session.read("kb://pkb/card.md")
            assert await session.search("layering") == {"proj://alpha/notes/tried.md", "shared://notes/common.md"}
            assert "kb://pkb/card.md" in await session.search("layering", include_kb=True)
            assert await session.search("layering", include_shared=False) == {"proj://alpha/notes/tried.md"}
            assert "query must be a non-empty string" in await session.refused("memory_search")
            assert "not found" in (await session.refused("memory_read", uri="notes/none.md")).lower()

    asyncio.run(scenario())


def test_the_tools_are_those_of_the_steward(world: World) -> None:
    steward = {tool["name"]: tool["inputSchema"]["properties"] for tool in StewardTools.definitions()}
    own = {tool["name"]: tool["inputSchema"]["properties"] for tool in memory_mcp.tool_definitions()}
    assert {name: steward[name] for name in TOOLS} == own


@pytest.mark.parametrize("uri", [
    "proj://beta/notes/tried.md", "proj://beta/index.md", "notes/../../beta/notes/tried.md", "../beta/notes/tried.md",
    "proj://alpha/../beta/notes/tried.md", "shared://../projects/beta/notes/tried.md", "/../../../hb.db",
    "kb://pkb/../hb.db", "role://architect/skills.md",
])
def test_another_project_and_paths_out_of_the_memory_are_refused(world: World, uri: str) -> None:
    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            assert "tried in beta" not in await session.refused("memory_read", uri=uri)
            # The scope is the session's; no argument changes it.
            assert not any("beta" in hit for hit in await session.search("layering", scope="beta", inbox_id=BETA))

    asyncio.run(scenario())


def test_the_channel_writes_nothing(world: World) -> None:
    asked = {"uri": "notes/new.md", "target_uri": "notes/new.md", "path": "notes/new.md", "op": "upsert_file",
             "content": "x", "rationale": "x", "proposal_id": "prop_1"}

    async def direct(name: str) -> dict[str, Any]:
        # Past the MCP process, as a session could with its own shell.
        *_, path, token = world.access.mcp_server("sess-alpha")["args"]
        reader, writer = await asyncio.open_unix_connection(path)
        writer.write(json.dumps({"token": token, "name": name, "arguments": asked}).encode() + b"\n")
        answer = json.loads(await reader.readline())
        writer.close()
        return answer

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            await session.search("layering")
            before = world.files()
            for name in ("memory_propose_write", "write_memory", "memory_approve_proposal", "memory_reject_proposal",
                         "read_memory", ""):
                assert "unknown tool" in await session.refused(name, **asked)
                answer = await direct(name)
                assert answer["error"] and "unknown tool" in answer["text"]
            await session.read("notes/tried.md")
            assert world.files() == before

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["block", "defer"])
def test_every_call_passes_the_permission_gate_first(world: World, action: Any) -> None:
    class Gate(InMemoryPermission):
        asked: list[tuple[str, dict[str, Any]]] = []
        decision = PermissionDecision(action="allow", reason="")

        def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
            self.asked.append((tool_name, args))
            return self.decision

    gate, opened = Gate(), Mock(side_effect=world.registry.memory_for)
    gate.asked = []
    world.access.activate(opened, gate)

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            assert "alpha" in await session.read("notes/tried.md")
            await session.search("layering")
            assert gate.asked == [("memory_read", {"uri": "notes/tried.md"}), ("memory_search", {"query": "layering"})]
            opened.reset_mock()
            gate.decision = PermissionDecision(action=action, reason="closed today")
            for name, args in (("memory_read", {"uri": "notes/tried.md"}), ("memory_search", {"query": "layering"})):
                assert await session.refused(name, **args) == f"{name} is not permitted ({action}): closed today"
            opened.assert_not_called()

    asyncio.run(scenario())


def test_the_default_policy_allows_the_two_tools(world: World) -> None:
    assert {AutoModePermissionAdapter().check(name, {}).action for name in TOOLS} == {"allow"}


def test_an_experts_session_sees_what_is_mounted_for_it(world: World) -> None:
    for role in ("architect", "critic"):
        folder = world.hub.root / "roles" / role
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "skills.md").write_text(f"layering rules of the {role}\n")

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-expert") as session:
            assert "rules of the architect" in await session.read("role://architect/skills.md")
            assert "tried in alpha" in await session.read("notes/tried.md")
            assert "everybody" in await session.read("shared://notes/common.md")
            for uri in ("role://critic/skills.md", "proj://beta/notes/tried.md", "shared://elsewhere.md"):
                assert "outside the memory mounted" in await session.refused("memory_read", uri=uri)
            assert await session.search("layering", include_kb=True) == {
                "proj://alpha/notes/tried.md", "shared://notes/common.md", "kb://pkb/card.md",
            }

    asyncio.run(scenario())


def test_sessions_of_two_projects_at_once_see_only_their_own(world: World) -> None:
    async def look(session: Session, own: str, other: str) -> None:
        for _ in range(5):
            assert f"tried in {own}" in await session.read("notes/tried.md")
            assert await session.search("layering", include_shared=False) == {f"proj://{own}/notes/tried.md"}
            assert "outside the memory" in await session.refused("memory_read", uri=f"proj://{other}/notes/tried.md")

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as a, world.session("sess-beta") as b:
            await asyncio.gather(look(a, "alpha", "beta"), look(b, "beta", "alpha"))

    asyncio.run(scenario())


def test_only_an_open_session_that_was_granted_the_tools_gets_answers(world: World) -> None:
    server = world.access.mcp_server("sess-alpha")
    # A session keeps its grant, whichever process asks.
    assert world.process(world.db).mcp_server("sess-alpha") == server
    *_, path, token = server["args"]

    async def scenario() -> None:
        async with world.access.serving():
            for forged in ("", "sess-beta", token[:-1], "x" * len(token)):
                session = await Session.start({**server, "args": [*server["args"][:-1], forged]})
                assert "no session" in await session.refused("memory_read", uri="notes/tried.md")
                await session.end()
            async with world.session("sess-alpha") as session:
                current = world.sessions.get("sess-alpha")
                assert current is not None
                world.sessions.save(replace(current, status="suspended", pending_request_id="req"))
                assert "alpha" in await session.read("notes/tried.md")
                world.sessions.close_session("sess-alpha")
                assert "task is closed" in await session.refused("memory_read", uri="notes/tried.md")

    asyncio.run(scenario())
    assert stat.S_IMODE(os.stat(Path(path).parent).st_mode) == 0o700


# --- outages -----------------------------------------------------------------------------------------------------


def test_an_unreachable_runtime_is_explained_and_the_session_goes_on_after_a_restart(world: World) -> None:
    async def scenario() -> None:
        async with world.session("sess-alpha") as session:
            # Not started yet, stopped, and started again by another process: the same MCP process all along.
            assert sorted(t["name"] for t in (await session.rpc("tools/list"))["result"]["tools"]) == list(TOOLS)
            assert UNAVAILABLE in await session.refused("memory_read", uri="notes/tried.md")
            async with world.access.serving():
                assert "alpha" in await session.read("notes/tried.md")
            assert UNAVAILABLE in await session.refused("memory_search", query="layering")
            db = SQLiteDatabase(world.root / "hb.db")
            try:
                async with world.process(db).serving():
                    assert "alpha" in await session.read("notes/tried.md")
            finally:
                db.close()

    asyncio.run(scenario())


def test_the_socket_left_by_a_killed_process_is_replaced_and_a_successors_is_not_removed(world: World) -> None:
    path = channel_path(world.root / "hb.db")
    assert path is not None

    async def scenario() -> None:
        async with world.session("sess-alpha") as session:
            async with world.access.serving():
                async with world.process(world.db).serving():
                    assert "alpha" in await session.read("notes/tried.md")
                    first = os.stat(path).st_ino
                # The successor removed its own.
                assert not path.exists()
            outgoing = world.access.serving()
            await outgoing.__aenter__()
            async with world.process(world.db).serving():
                assert os.stat(path).st_ino
                await outgoing.__aexit__(None, None, None)
                assert "alpha" in await session.read("notes/tried.md")
            assert first

    asyncio.run(scenario())


def test_failures_inside_the_runtime_are_answers(world: World) -> None:
    def broken(inbox_id: str) -> Any:
        raise sqlite3.OperationalError("disk I/O error")

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            world.access.activate(broken, world.permissions)
            assert "disk I/O error" in await session.refused("memory_read", uri="notes/tried.md")
            world.access.activate(world.registry.memory_for, world.permissions)
            assert "alpha" in await session.read("notes/tried.md")
            # Whatever the client sends, the process answers or ignores it and goes on.
            assert session.proc.stdin is not None
            session.proc.stdin.write(b"not json\n\xff\xfe\n[]\n" + json.dumps({"method": "notifications/x"}).encode() + b"\n")
            assert (await session.rpc("no/such"))["error"]["code"] == -32601
            for params in ([], {"name": "memory_read", "arguments": []}, {"name": ["x"], "arguments": {}}):
                reply = await session.rpc("tools/call", params)
                assert "error" in reply or reply["result"]["isError"]
            assert "项目" in await session.refused("memory_read", uri="notes/项目.md")
            assert "alpha" in await session.read("notes/tried.md")

    asyncio.run(scenario())


def test_long_answers_are_cut(world: World) -> None:
    (world.hub.project_dir("alpha") / "notes" / "long.md").write_text("layering " * 40_000)

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            failed, text = await session.call("memory_read", uri="notes/long.md")
            assert not failed and len(text) < 101_000 and "more characters cut" in text

    asyncio.run(scenario())


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


# --- what is launched --------------------------------------------------------------------------------------------


def servers(argv: list[str]) -> dict[str, Any]:
    assert argv.count("--mcp-config") <= 1
    return json.loads(argv[argv.index("--mcp-config") + 1])["mcpServers"] if "--mcp-config" in argv else {}


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


class MemoryCase(VisibleCase):
    def setUp(self) -> None:
        super().setUp()
        (self.root / "world").mkdir()
        self.world = World(self.root / "world")
        self.addCleanup(self.world.close)
        self.index("")
        self.caps.memory = self.world.registry.memory_for(ALPHA)
        self.access: Any = self.world.access

    def index(self, text: str) -> None:
        (self.world.hub.project_dir("alpha") / "index.md").write_text(text)

    def sde(self, review: bool = True, terminal: Any = None, **options: Any) -> SDEExecutor:
        profile = ClaudeCodeProfile(name="sde", timeout_seconds=30, launcher=self.launcher())
        return SDEExecutor(
            profile=profile, options=SDEOptions(review=review, reviewer="coder-reviewer"),
            reviewer=self.reviewer(terminal) if review else None,
            worktree_manager=OrcaWorktreeManager(workspace_store=self.workspaces,
                                                 session_store=SQLiteSessionStoreAdapter(self.db), backend="git"),
            inbox_checkout_map={}, tasks=self.tasks, terminal=terminal, memory=self.access, **options,
        )

    def session(self, **extra: Any) -> SessionRef:
        return SessionRef(session_id="sess-alpha", inbox_id=ALPHA, correlation_key="task:1", executor_id="sde", **extra)

    def task(self, **extra: Any) -> Event:
        return event(self.payload(**extra), inbox_id=ALPHA)

    def answers(self) -> list[dict[str, Any]]:
        return [c for c in self.calls("sde") if "memory" in c]


READ = {"memory": [["memory_read", {"uri": "notes/tried.md"}], ["memory_search", {"query": "layering"}],
                   ["memory_read", {"uri": "proj://beta/notes/tried.md"}]]}


class TestSessionsHaveTheTools(MemoryCase):
    def assert_read(self, answer: dict[str, Any]) -> None:
        (read_failed, read), (search_failed, search), (other_failed, _) = answer["memory"]
        self.assertEqual((read_failed, search_failed, other_failed), (False, False, True))
        self.assertIn("tried in alpha", read)
        self.assertIn("proj://alpha/notes/tried.md", search)
        self.assertEqual(answer["memory_tools"], list(TOOLS))
        self.assertEqual(sorted(answer["memory_permitted"]), PERMITTED)

    async def test_a_one_off_developer_has_them_in_every_round_and_the_reviewer_does_not(self) -> None:
        self.set_plan(sde=[{**dev(BUGGY), **READ}, {**dev(FIXED), **READ}],
                      reviewer=[CHANGES_REQUESTED, PASS])
        async with self.world.access.serving():
            outcome = await self.sde().execute([self.task()], self.session(), self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("review=pass after 2 round", outcome.messages[0])
        first, second = self.answers()
        self.assert_read(first)
        self.assert_read(second)
        for call in [c for c in self.calls("reviewer") if c["mode"] == "print"]:
            self.assertNotIn(SERVER, json.dumps(call["argv"]))

    async def test_a_visible_developer_keeps_them_through_a_restart_and_the_reviewer_does_not_get_them(self) -> None:
        self.set_plan(sde=[{**dev(FIXED, status="needs_input", question="which?"), **READ}, {**dev(FIXED), **READ}],
                      reviewer=[PASS])
        executor = self.sde(terminal=self.runner)
        async with self.world.access.serving():
            asked = await executor.execute([self.task()], self.session(), self.caps)
        assert isinstance(asked, Suspend), asked
        # Suspended: the conversation and its MCP process go on while the next process takes over the channel.
        self.access = self.world.process(self.world.db)
        waiting = self.session(status="suspended", pending_request_id=asked.request_id,
                               checkpoint=asked.session_checkpoint, executor_session_id=asked.executor_session_id)
        approval = event({"approval_request_id": asked.request_id, "approved": True, "comment": "that one"}, inbox_id=ALPHA)
        async with self.access.serving():
            outcome = await self.sde(terminal=self.runner).execute([approval], waiting, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual([c["turn"] for c in self.calls("sde", "interactive") if "turn" in c], [1, 2])
        first, second = self.answers()
        self.assert_read(first)
        self.assert_read(second)
        launched = self.calls("sde", "interactive")[0]
        self.assertEqual(set(servers(launched["argv"])), {SERVER, HANDOFF_SERVER})
        [reviewer] = [c for c in self.calls("reviewer", "interactive") if "turn" in c]
        self.assertEqual(set(servers(reviewer["argv"])), {HANDOFF_SERVER})
        self.assertNotIn(SERVER, arg(reviewer, "--allowedTools"))

    async def test_a_resumed_conversation_gets_them_again(self) -> None:
        self.set_plan(sde=[{**dev(FIXED), **READ}])
        async with self.world.access.serving():
            first = await self.sde(review=False, terminal=self.runner).execute([self.task()], self.session(), self.caps)
            await self.orca.close_all()
            session = self.session(executor_session_id=first.executor_session_id)
            await self.sde(review=False, terminal=self.runner).execute([self.task(text="and one")], session, self.caps)
        self.assertEqual(["--resume" in c["argv"] for c in self.answers()], [False, True])
        for answer in self.answers():
            self.assert_read(answer)

    async def test_an_outage_does_not_touch_the_delivery(self) -> None:
        self.set_plan(sde=[{**dev(FIXED), **READ}], reviewer=[PASS])
        outcome = await self.sde().execute([self.task()], self.session(), self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("[sde] delivered (review=pass", outcome.messages[0])
        [answer] = self.answers()
        for failed, text in answer["memory"]:
            self.assertTrue(failed)
            self.assertIn(UNAVAILABLE, text)

    async def test_a_session_that_cannot_be_granted_them_starts_without(self) -> None:
        self.access = Mock(spec=MemoryAccess)
        self.access.mcp_server.side_effect = sqlite3.OperationalError("database is locked")
        self.set_plan(sde=[{**dev(FIXED), **READ}])
        outcome = await self.sde(review=False).execute([self.task()], self.session(), self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("[sde] delivered", outcome.messages[0])
        [answer] = self.answers()
        self.assertIsNone(answer["memory"])
        self.assertEqual(answer["memory_permitted"], [])

    async def test_without_the_channel_nothing_is_added(self) -> None:
        self.access = None
        self.index("- [Tried](notes/tried.md)\n")
        self.set_plan(sde=[dev(FIXED)])
        await self.sde(review=False).execute([self.task()], self.session(), self.caps)
        [call] = self.calls("sde")
        self.assertNotIn("--mcp-config", call["argv"])
        self.assertNotIn("memory_read", call["prompt"])


class TestFirstPrompt(MemoryCase):
    async def prompt(self, access: Any) -> str:
        self.access = access
        self.log.unlink(missing_ok=True)
        self.set_plan(sde=[dev(FIXED)])
        await self.sde(review=False).execute([self.task()], self.session(), self.caps)
        return self.calls("sde")[0]["prompt"]

    async def test_it_says_to_open_the_listed_notes_before_starting(self) -> None:
        self.index("- [Tried](notes/tried.md): what was tried\n")
        plain = await self.prompt(None)
        prompt = await self.prompt(self.world.access)
        added = prompt.replace(plain[:plain.index("## Task requirements")], "").removesuffix(
            plain[plain.index("## Task requirements"):]).strip()
        self.assertTrue(added.startswith("## Project memory tools\n"), added)
        for part in ("index.md", "memory_read", "memory_search", "Before you start", "read-only"):
            self.assertIn(part, added)
        self.assertLess(prompt.index("notes/tried.md"), prompt.index("## Project memory tools"))

    async def test_without_an_index_it_is_unchanged(self) -> None:
        for text in ("", " \n"):
            self.index(text)
            self.assertEqual(await self.prompt(self.world.access), await self.prompt(None))


class TestPeerHandoff(SDERestartCase):
    async def test_the_tools_outlive_the_process_that_started_the_session(self) -> None:
        (self.root / "world").mkdir()
        world = World(self.root / "world")
        self.addCleanup(world.close)
        self.sde_extra = {"memory": world.access}
        self.orca.scripts += [[None], [verdict_pass]]
        evt = self.submit()
        worker, runs = self.process(self.db, "old")
        async with world.access.serving():
            await self.stop_when(worker, runs, "develop")
            session_id = runs.list_open(evt.inbox_id)[0].session_id
            world.sessions.save(SessionRef(session_id=session_id, inbox_id=ALPHA, executor_id="sde"))
            argv = self.orca.specs[0]["exec"]
            self.assertEqual(set(servers(argv)), {SERVER})
            self.assertLessEqual({*PERMITTED, "SendMessage"}, set(argv[argv.index("--allowedTools") + 1].split(",")))
            session = await Session.start(servers(argv)[SERVER])
            self.assertIn("alpha", await session.read("notes/tried.md"))
        self.assertIn(UNAVAILABLE, await session.refused("memory_read", uri="notes/tried.md"))
        developer = self.orca.session(0)
        [task] = self.tasks(developer)
        committed(self.repo, "def divide(a, b):\n    if b == 0:\n        raise ValueError\n    return a / b\n")(developer, task)

        self.sde_extra = {"memory": world.process(world.db)}
        worker, _, db = self.restarted()
        async with self.sde_extra["memory"].serving():
            [report] = await asyncio.wait_for(worker.process_inbox(evt.inbox_id), 20)
            self.assertIsInstance(report.outcome, Emit)
            self.assertIn("alpha", await session.read("notes/tried.md"))
        self.assertEqual(await session.end(), 0)
        self.assertIn("review=pass", "".join(self.outbox(db)))
        reviewer = self.orca.specs[1]["exec"]
        self.assertNotIn(SERVER, json.dumps(reviewer))


# --- the daemon --------------------------------------------------------------------------------------------------


def _ask(daemon: Any, session_id: str, inbox_id: str) -> dict[str, Any]:
    daemon.sessions.save(SessionRef(session_id=session_id, inbox_id=inbox_id, executor_id="sde"))
    return daemon.memory_access.mcp_server(session_id)


def test_the_daemon_serves_its_sessions_until_it_ends_and_the_next_one_goes_on(tmp_path: Path) -> None:
    config = parse_config({
        "runtime": {"db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory"), "default_executor": "sde"},
        "executors": {"sde": {"role": "sde", "review": False}},
        "projects": {"alpha": {"inbox_id": ALPHA, "repo": "o/alpha", "checkout": str(tmp_path)}},
    })

    async def scenario() -> None:
        session: Session | None = None
        for _ in range(2):
            daemon = build_daemon_from_config(config)
            server = _ask(daemon, "sess-1", ALPHA)
            assert daemon.memory_hub is not None and daemon.memory_access is not None
            daemon.memory_hub.lay_out("alpha")
            (daemon.memory_hub.project_dir("alpha") / "notes" / "tried.md").write_text("layering in alpha\n")
            session = session or await Session.start(server)
            stop = asyncio.Event()
            running = asyncio.create_task(daemon.run(stop_event=stop))
            try:
                await until(lambda: running.done() or Path(server["args"][-2]).exists(), what="the channel")
                assert "alpha" in await session.read("notes/tried.md")
            finally:
                stop.set()
                await asyncio.wait_for(running, 20)
                daemon.close()
            assert UNAVAILABLE in await session.refused("memory_read", uri="notes/tried.md")
        assert session is not None and await session.end() == 0

    asyncio.run(scenario())


def test_a_reload_puts_the_new_configurations_scopes_in_force(tmp_path: Path) -> None:
    path = tmp_path / "hibernaut.toml"
    raw = config_of(tmp_path, channels={"port": 0})
    path.write_text(_toml(raw))

    async def scenario() -> None:
        config = load_config(path)
        daemon = build_daemon_from_config(config)
        assert daemon.memory_hub is not None
        server = _ask(daemon, "sess-1", "proj:hibernaut:architect")
        folder = daemon.memory_hub.root / "roles" / "architect"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "skills.md").write_text("rules of the architect\n")
        stop = asyncio.Event()
        session = await Session.start(server)
        running = asyncio.create_task(serve_configured(path, config, daemon, stop, reload_interval=0.02))
        try:
            await until(lambda: running.done() or Path(server["args"][-2]).exists(), what="the channel")
            assert "architect" in await session.read("role://architect/skills.md")
            for project in raw["projects"].values():
                project["experts"] = []
            path.write_text(_toml(raw))

            async def dismissed() -> bool:
                return (await session.call("memory_read", uri="role://architect/skills.md"))[0]

            async with asyncio.timeout(20):
                while not await dismissed():
                    await asyncio.sleep(0.02)
            assert UNAVAILABLE not in await session.refused("memory_read", uri="role://architect/skills.md")
        finally:
            stop.set()
            await asyncio.wait_for(running, 20)
            daemon.close()
            await session.end()

    asyncio.run(scenario())


def _toml_value(value: Any) -> str:
    if isinstance(value, dict):
        return "{" + ", ".join(f"{k} = {_toml_value(v)}" for k, v in value.items()) + "}"
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(v) for v in value) + "]"
    return json.dumps(value)


def _toml(raw: dict[str, Any], prefix: str = "") -> str:
    plain = {k: v for k, v in raw.items() if not isinstance(v, dict)}
    tables = {k: v for k, v in raw.items() if isinstance(v, dict)}
    lines = [f"[{prefix}]"] if prefix else []
    lines += [f"{k} = {_toml_value(v)}" for k, v in plain.items()]
    lines += [_toml(v, f'{prefix}."{k}"' if prefix else k) for k, v in tables.items()]
    return "\n".join(lines) + "\n"


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
