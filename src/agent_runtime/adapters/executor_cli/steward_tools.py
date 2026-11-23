from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import subprocess
import tempfile
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast

from agent_runtime.core.models import Event, ForwardRefused, SessionStatus
from agent_runtime.core.ports import RunCapabilities, SessionStorePort

_STATUSES: tuple[SessionStatus, ...] = ("active", "suspended", "archived", "closed")
_MAX_LIST = 100
_MAX_TEXT = 100_000
_SNIPPET = 500
_GIT_TIMEOUT = 30.0
# Longest Unix socket path every supported system accepts (macOS allows 104 bytes).
_MAX_SOCKET_PATH = 100
_STREAM_LIMIT = 4 * 1024 * 1024

GIT_COMMANDS = ("status", "diff", "log", "show", "blame", "rev-parse", "merge-base")
# Options that write a file, read one outside the checkout, or run a configured program. Git accepts any unambiguous
# prefix of a long option, so each prefix is refused too.
_GIT_REFUSED = ("--output", "--ext-diff", "--textconv", "--no-index", "--contents", "--ignore-revs-file",
                "--show-signature")
# The same for short options, alone or bundled (-pO<file>): (refused letter, letters whose value ends the bundle).
_GIT_REFUSED_SHORT = {"blame": ("S", "LMC")}
_GIT_REFUSED_SHORT_DEFAULT = ("O", "SGULlnMBCX")
# A signature format (%G?) would run the configured signature checker.
_GIT_CONFIG = ("core.fsmonitor=false", "log.showSignature=false", "gpg.program=false", "gpg.openpgp.program=false",
               "gpg.x509.program=false", "gpg.ssh.program=false")


class ToolError(Exception):
    pass


class GitRefused(ToolError):
    pass


class EventLister(Protocol):
    def list_events(
        self, *, inbox_id: str | None = None, status: str | None = None, limit: int = 50
    ) -> list[Event]: ...


def check_git_args(args: Sequence[str]) -> None:
    if not args or args[0] not in GIT_COMMANDS:
        raise GitRefused(f"git: only {', '.join(GIT_COMMANDS)} are available")
    refused, valued = _GIT_REFUSED_SHORT.get(args[0], _GIT_REFUSED_SHORT_DEFAULT)
    for arg in args[1:]:
        name = arg.split("=", 1)[0]
        if arg.startswith("--"):
            # --ignore-rev is an option of its own, so Git never reads it as short for the refused one.
            bad = len(name) > 2 and name != "--ignore-rev" and any(option.startswith(name) for option in _GIT_REFUSED)
        else:
            bad = re.match(rf"-[^{valued}]*?{refused}", arg) is not None
        if bad:
            raise GitRefused(f"git: option {name} is not available")
        # Two paths of which one is outside the checkout turn `git diff` into a reader of arbitrary files.
        if arg.startswith(("/", "~")) or ".." in arg.split("/"):
            raise GitRefused(f"git: {arg} is outside the checkout")


def _git(checkout: Path, args: Sequence[str]) -> str:
    check_git_args(args)
    plain = ("--no-ext-diff", "--no-textconv") if args[0] in ("diff", "log", "show", "blame") else ()
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    # Without optional locks `git status` does not even refresh the index.
    env.update(GIT_OPTIONAL_LOCKS="0", GIT_TERMINAL_PROMPT="0")
    try:
        done = subprocess.run(
            ["git", "--no-pager", *(part for item in _GIT_CONFIG for part in ("-c", item)), args[0], *plain, *args[1:]],
            cwd=checkout, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, errors="replace",
            timeout=_GIT_TIMEOUT,
        )
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        raise ToolError(f"git: {exc}") from exc
    if done.returncode != 0:
        raise ToolError(f"git {args[0]} exited {done.returncode}: {done.stderr.strip()[-2000:]}")
    return done.stdout


def _clip(text: str) -> str:
    return text if len(text) <= _MAX_TEXT else f"{text[:_MAX_TEXT]}\n[{len(text) - _MAX_TEXT} more characters cut]"


def _required(arguments: dict[str, Any], key: str) -> str:
    value = arguments.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ToolError(f"{key} must be a non-empty string")
    return value


def _optional(arguments: dict[str, Any], key: str) -> str | None:
    value = arguments.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ToolError(f"{key} must be a string")
    return value.strip() or None


def _strings(arguments: dict[str, Any], key: str) -> list[str]:
    value = arguments.get(key)
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in cast(list[object], value)):
        raise ToolError(f"{key} must be an array of strings")
    return cast(list[str], value)


_TEXT = {"type": "string"}
_TEXTS = {"type": "array", "items": _TEXT}


def _schema(required: Sequence[str] = (), **properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": list(required)}


@dataclass(slots=True)
class StewardTools:
    # The tools of one Steward run. Every call passes PermissionPort.check under its own tool name first.
    caps: RunCapabilities
    checkout: Path
    inbox_id: str
    sessions: SessionStorePort
    events: EventLister
    in_project: Callable[[str], bool]
    calls: int = 0
    # forward_to_inbox arguments a person has to approve before they go out.
    deferred: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])

    @staticmethod
    def definitions() -> list[dict[str, Any]]:
        inbox = {**_TEXT, "description": "An inbox of this project; leave out for the default."}
        return [
            {"name": "forward_to_inbox",
             "description": "Hand work to another agent through its inbox. Nothing waits for it: end your turn after "
                            "forwarding. Without return_to that agent answers the person directly; with return_to "
                            "(normally your own inbox) its result comes back to you as an agent:return event.",
             "inputSchema": _schema(
                 ("target_inbox", "text"),
                 target_inbox={**_TEXT, "description": "An inbox id from the routing manifest."},
                 text={**_TEXT, "description": "The complete task: the receiver sees this, not your conversation."},
                 title=_TEXT, tags=_TEXTS,
                 target_agent={**_TEXT, "description": "Leave out to let the inbox's rules pick the agent."},
                 return_to=_TEXT,
             )},
            {"name": "read_memory", "description": "Read a file of the project memory, e.g. notes/release.md.",
             "inputSchema": _schema(("path",), path=_TEXT)},
            {"name": "write_memory", "description": "Replace a file of the project memory.",
             "inputSchema": _schema(("path", "content"), path=_TEXT, content=_TEXT)},
            {"name": "list_sessions", "description": "List this project's sessions, newest first.",
             "inputSchema": _schema(inbox_id=inbox, status={"type": "string", "enum": list(_STATUSES)})},
            {"name": "list_events", "description": "List this project's inbox events, newest first.",
             "inputSchema": _schema(inbox_id=inbox, status=_TEXT, limit={"type": "integer"})},
            {"name": "git", "description": f"Read the checkout's history: {', '.join(GIT_COMMANDS)}.",
             "inputSchema": _schema(("args",), args={**_TEXTS, "description": 'e.g. ["log", "--oneline", "-10"]'})},
        ]

    async def call(self, name: object, arguments: object) -> tuple[str, bool]:
        # (answer, is_error)
        handlers: dict[str, Callable[[dict[str, Any]], Awaitable[str]]] = {
            "forward_to_inbox": self._forward,
            "read_memory": self._read_memory,
            "write_memory": self._write_memory,
            "list_sessions": self._list_sessions,
            "list_events": self._list_events,
            "git": self._run_git,
        }
        handler = handlers.get(name) if isinstance(name, str) else None
        if handler is None:
            return f"unknown tool {name}", True
        if not isinstance(arguments, dict):
            return "arguments must be an object", True
        self.calls += 1
        try:
            return _clip(await handler(cast(dict[str, Any], arguments))), False
        except (ToolError, OSError, ValueError) as exc:
            return str(exc), True

    def _allow(self, tool: str, args: dict[str, Any]) -> None:
        decision = self.caps.permissions.check(tool, args)
        if decision.action != "allow":
            raise ToolError(f"{tool} is not permitted ({decision.action}): {decision.reason}")

    async def _forward(self, arguments: dict[str, Any]) -> str:
        wanted: dict[str, Any] = {
            "target_inbox": _required(arguments, "target_inbox"),
            "text": _required(arguments, "text"),
            "title": _optional(arguments, "title") or "",
            "tags": _strings(arguments, "tags"),
            "target_agent": _optional(arguments, "target_agent"),
            "return_to": _optional(arguments, "return_to"),
        }
        try:
            event = self.forward(wanted)
        except ForwardRefused as exc:
            if exc.action != "defer":
                raise ToolError(f"not forwarded: {exc}") from exc
            self.deferred.append(wanted)
            return (f"Held for a person's approval: {exc}. Hibernaut asks for it when this turn ends and forwards the "
                    "work once it is approved; do not forward it again.")
        return f"Forwarded to {event.inbox_id}; it is queued when this turn ends."

    def forward(self, wanted: dict[str, Any], approved_request_id: str | None = None) -> Event:
        options = {k: v for k, v in wanted.items() if k not in ("target_inbox", "text")}
        return self.caps.forward_to_inbox(
            wanted["target_inbox"], wanted["text"], **options, approved_request_id=approved_request_id
        )

    async def _read_memory(self, arguments: dict[str, Any]) -> str:
        path = _required(arguments, "path")
        self._allow("read_memory", {"path": path})
        return self.caps.memory.read_on_demand(path)

    async def _write_memory(self, arguments: dict[str, Any]) -> str:
        path = _required(arguments, "path")
        content = arguments.get("content")
        if not isinstance(content, str):
            raise ToolError("content must be a string")
        self._allow("write_memory", {"path": path, "content": content})
        self.caps.memory.write(path, content)
        return f"Wrote {path}."

    def _inbox(self, arguments: dict[str, Any]) -> str | None:
        inbox_id = _optional(arguments, "inbox_id")
        if inbox_id is not None and not self.in_project(inbox_id):
            raise ToolError(f"'{inbox_id}' is not an inbox of this project")
        return inbox_id

    async def _list_sessions(self, arguments: dict[str, Any]) -> str:
        inbox_id, raw = self._inbox(arguments), _optional(arguments, "status")
        status = next((s for s in _STATUSES if s == raw), None)
        if raw is not None and status is None:
            raise ToolError(f"status must be one of {', '.join(_STATUSES)}")
        self._allow("list_sessions", {"inbox_id": inbox_id, "status": status})
        found = [s for s in self.sessions.list_sessions(inbox_id=inbox_id, status=status) if self.in_project(s.inbox_id)]
        return json.dumps([
            {"session_id": s.session_id, "inbox_id": s.inbox_id, "agent_id": s.agent_id, "status": s.status,
             "correlation_key": s.correlation_key, "work_key": s.work_key, "parent_session_id": s.parent_session_id,
             "waiting_for_approval": s.pending_request_id is not None, "updated_at": s.updated_at}
            for s in found[:_MAX_LIST]
        ], ensure_ascii=False)

    async def _list_events(self, arguments: dict[str, Any]) -> str:
        inbox_id, status = self._inbox(arguments), _optional(arguments, "status")
        limit = arguments.get("limit", 20)
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ToolError("limit must be an integer")
        self._allow("list_events", {"inbox_id": inbox_id, "status": status, "limit": limit})
        # Other projects share the table, so without an inbox the newest rows are filtered after reading.
        rows = self.events.list_events(inbox_id=inbox_id, status=status, limit=500 if inbox_id is None else limit)
        found = [e for e in rows if self.in_project(e.inbox_id)][: max(1, min(limit, _MAX_LIST))]
        return json.dumps([self._event_row(e) for e in found], ensure_ascii=False)

    @staticmethod
    def _event_row(event: Event) -> dict[str, Any]:
        row: dict[str, Any] = {
            "event_id": event.event_id, "inbox_id": event.inbox_id, "source": event.source, "status": event.status,
            "occurred_at": event.occurred_at, "session_id": event.session_id, "correlation_key": event.correlation_key,
        }
        for key in ("title", "text", "agent_id", "executor_id", "forwarded_from", "forwarded_by", "hop_count",
                    "return_to"):
            value = event.payload.get(key)
            if isinstance(value, str) and value:
                row[key] = value[:_SNIPPET]
            elif isinstance(value, int):
                row[key] = value
        return row

    async def _run_git(self, arguments: dict[str, Any]) -> str:
        args = _strings(arguments, "args")
        check_git_args(args)
        self._allow("git_read", {"args": args})
        return await asyncio.to_thread(_git, self.checkout, args)

    @contextlib.asynccontextmanager
    async def serve(self) -> AsyncGenerator[str]:
        # A socket in a fresh 0700 directory, gone with the run: only processes of this user can reach it.
        base = tempfile.gettempdir()
        if len(os.fsencode(base)) > _MAX_SOCKET_PATH - 24:
            base = "/tmp"
        with tempfile.TemporaryDirectory(prefix="hb-steward-", dir=base) as tmp:
            path = os.path.join(tmp, "s")
            connections: set[asyncio.Task[None]] = set()

            async def connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                task = asyncio.current_task()
                assert task is not None
                connections.add(cast(asyncio.Task[None], task))
                try:
                    while line := await reader.readline():
                        writer.write((json.dumps(await self._answer(line), ensure_ascii=False) + "\n").encode("utf-8"))
                        await writer.drain()
                except (ConnectionError, ValueError):
                    pass
                finally:
                    connections.discard(cast(asyncio.Task[None], task))
                    writer.close()

            server = await asyncio.start_unix_server(connected, path, limit=_STREAM_LIMIT)
            try:
                yield path
            finally:
                # No tool call outlives the run: a late forward would miss the batch the worker queues.
                server.close()
                for task in list(connections):
                    task.cancel()
                await asyncio.gather(*connections, return_exceptions=True)

    async def _answer(self, line: bytes) -> dict[str, Any]:
        try:
            request: object = json.loads(line)
        except ValueError:
            request = None
        if not isinstance(request, dict):
            return {"text": "malformed request", "error": True}
        item = cast(dict[str, Any], request)
        if item.get("op") == "tools":
            return {"tools": self.definitions()}
        text, error = await self.call(item.get("name"), item.get("arguments"))
        return {"text": text, "error": error}
