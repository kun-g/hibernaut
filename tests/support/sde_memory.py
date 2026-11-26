from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.memory_access import MemoryAccess, channel_path
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.memory_fs import (
    ExpertMount,
    GovernedMemoryHub,
    WorkspacePartitionRegistry,
    open_role_store,
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteMemoryGrantStore, SQLiteSessionStoreAdapter
from agent_runtime.core.models import Event, SessionRef
from collaboration import ExpertPoolRegistry
from memory_hub import KnowledgeMountConfig, MemoryHubConfig
from tests.support.fakes import InMemoryPermission
from tests.support.sde_executor import event
from tests.support.visible_roles import VisibleCase


ALPHA, BETA, EXPERT = "proj:alpha", "proj:beta", "proj:alpha:architect"
SERVER = "hibernaut_memory"
TOOLS = ("memory_read", "memory_search")
PERMITTED = [f"mcp__{SERVER}__{name}" for name in TOOLS]


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


def servers(argv: list[str]) -> dict[str, Any]:
    assert argv.count("--mcp-config") <= 1
    return json.loads(argv[argv.index("--mcp-config") + 1])["mcpServers"] if "--mcp-config" in argv else {}


@contextlib.contextmanager
def ungranted(db: SQLiteDatabase) -> Iterator[None]:
    # No session gets a token: the write of every grant fails.
    with db.transaction() as conn:
        conn.execute("CREATE TRIGGER no_grants BEFORE INSERT ON memory_grants BEGIN SELECT RAISE(ABORT, 'disk I/O error'); END")
        conn.execute("DELETE FROM memory_grants")
    try:
        yield
    finally:
        with db.transaction() as conn:
            conn.execute("DROP TRIGGER no_grants")


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
