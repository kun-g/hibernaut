from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import stat
import sys
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import Any, cast

from agent_runtime.adapters.executor_cli.memory_mcp import TOOL_NAMES as MEMORY_TOOLS
from agent_runtime.adapters.executor_cli.tool_socket import MAX_SOCKET_PATH, clip, line_server, request_of
from agent_runtime.adapters.memory_fs import call_memory_tool
from agent_runtime.adapters.storage_sqlite import SQLiteMemoryGrantStore
from agent_runtime.core.ports import MemoryPort, PermissionPort, SessionStorePort
from agent_runtime.engine.forwarding import is_open

MEMORY_SCRIPT = Path(__file__).with_name("memory_mcp.py")


def channel_path(db_path: str | Path) -> Path | None:
    # Where every process on this database serves, so a session finds the next one. None: a database in memory has no
    # next one.
    if str(db_path) == ":memory:":
        return None
    db = Path(db_path).expanduser().resolve()
    path = db.parent / ".hibernaut-memory" / f"{db.name}.sock"
    if len(os.fsencode(path)) <= MAX_SOCKET_PATH:
        return path
    digest = hashlib.sha256(os.fsencode(db)).hexdigest()[:16]
    return Path(f"/tmp/hibernaut-memory-{os.getuid()}") / f"{digest}.sock"


def _private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, exist_ok=True)
    found = os.lstat(path)
    if not stat.S_ISDIR(found.st_mode) or found.st_uid != os.getuid():
        raise PermissionError(f"{path} is not a directory of this user")
    path.chmod(0o700)


def _identity(path: Path) -> tuple[int, int] | None:
    try:
        found = os.lstat(path)
    except OSError:
        return None
    return found.st_dev, found.st_ino


class MemoryAccess:
    # What SDE sessions read of the project memory, and how: a session outlives the Run and the process that started
    # it, so its token is stored and the socket stays where it is. One per process; a reload points it at the new
    # configuration's scopes once that is in force. Nothing here writes memory, whatever a session sends.
    def __init__(self, *, grants: SQLiteMemoryGrantStore, sessions: SessionStorePort, path: Path) -> None:
        self._grants = grants
        self._sessions = sessions
        self._path = path
        self._scopes: tuple[Callable[[str], MemoryPort], PermissionPort] | None = None

    def activate(self, memory_for: Callable[[str], MemoryPort], permissions: PermissionPort) -> None:
        self._scopes = memory_for, permissions

    def mcp_server(self, session_id: str) -> dict[str, Any]:
        return {"type": "stdio", "command": sys.executable,
                "args": ["-I", str(MEMORY_SCRIPT), str(self._path), self._grants.issue(session_id)]}

    @contextlib.asynccontextmanager
    async def serving(self) -> AsyncGenerator[None]:
        async with contextlib.AsyncExitStack() as stack:
            try:
                _private_dir(self._path.parent)
                await stack.enter_async_context(line_server(str(self._path), self._answer))
            except OSError as exc:
                print(f"[hibernaut] The memory tools of SDE sessions are unavailable: {exc}", file=sys.stderr, flush=True)
            else:
                stack.callback(self._remove, _identity(self._path))
            yield

    def _remove(self, bound: tuple[int, int] | None) -> None:
        # A process that took over the path already serves there.
        if bound is not None and _identity(self._path) == bound:
            self._path.unlink(missing_ok=True)

    async def _answer(self, line: bytes) -> dict[str, Any]:
        text, error = await asyncio.to_thread(self._call, line)
        return {"text": text, "error": error}

    def _call(self, line: bytes) -> tuple[str, bool]:
        request = request_of(line)
        if request is None:
            return "malformed request", True
        name, arguments, token = request.get("name"), request.get("arguments"), request.get("token")
        if not isinstance(name, str) or name not in MEMORY_TOOLS:
            return f"unknown tool {name}; there are {' and '.join(MEMORY_TOOLS)}", True
        if not isinstance(arguments, dict):
            return f"{name} needs an object of arguments", True
        try:
            return self._read(name, cast(dict[str, Any], arguments), token if isinstance(token, str) else "")
        except (OSError, ValueError) as exc:
            return str(exc), True
        except Exception as exc:
            # A failure of the store or the hub is told to the session too, which goes on.
            return f"{name} failed ({type(exc).__name__}): {exc}", True

    def _read(self, name: str, arguments: dict[str, Any], token: str) -> tuple[str, bool]:
        session_id = self._grants.session_for(token)
        session = self._sessions.get(session_id) if session_id is not None else None
        if session is None:
            return "Hibernaut has no session with this token", True
        if not is_open(session):
            return "this session's task is closed, so its project memory is closed to it too", True
        if self._scopes is None:
            return "project memory is not open yet; try again later", True
        memory_for, permissions = self._scopes
        decision = permissions.check(name, arguments)
        if decision.action != "allow":
            return f"{name} is not permitted ({decision.action}): {decision.reason}", True
        memory = memory_for(session.inbox_id)
        return clip(call_memory_tool(memory, name, arguments, actor=session.inbox_id)), False
