from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import tempfile
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast

from agent_runtime.adapters.executor_cli import memory_mcp
from agent_runtime.adapters.executor_cli.steward_git import (
    GIT_COMMANDS,
    ToolError,
    check_git_args,
    exec_git,
    file_name,
    git_names,
    outside,
    plan_git,
    run_git,
)
from agent_runtime.adapters.executor_cli.tool_socket import MAX_SOCKET_PATH, clip, line_server, request_of
from agent_runtime.adapters.memory_fs import call_memory_tool, write_memory
from agent_runtime.core.models import Event, ForwardRefused, SessionStatus
from agent_runtime.core.ports import RunCapabilities, SessionStorePort

_STATUSES: tuple[SessionStatus, ...] = ("active", "suspended", "archived", "closed")
_MAX_LIST = 100
_SNIPPET = 500
_MAX_LINES = 2000
_MAX_MATCHES = 200
_MAX_FILE = 10 * 1024 * 1024


class EventLister(Protocol):
    def list_events(
        self, *, inbox_id: str | None = None, status: str | None = None, limit: int = 50
    ) -> list[Event]: ...


class SystemTools(Protocol):
    # What a Steward with system tools may do to the deployment; each call blocks, so it runs in a thread. A failure is
    # raised as ToolError with the reason and what was left behind.
    def check(self) -> str: ...

    def onboard(self, *, repo: str, name: str | None, description: str | None) -> str: ...

    def complete(self, *, name: str, description: str | None) -> str: ...

    # Read only: a summary of failures, suspensions, deliveries, undelivered messages and timer firings.
    def status(self, *, hours: int, timer_runs: int) -> str: ...

    # Whether a path must not be shown to it: the deployment file and what is written beside it.
    def hides(self, path: Path) -> bool: ...


_STATUS_HOURS, _MAX_STATUS_HOURS = 24, 24 * 30
_TIMER_RUNS, _MAX_TIMER_RUNS = 5, 20
_MATCH = re.compile(rb"([^\0]+)\0(\d+)[\0:]([^\n]*)\n")


def _real_path(checkout: Path, path: str) -> str:
    # The path the permission gate judges and that is read: no other spelling or link leads round a rule for it.
    target = (checkout / path).resolve()
    if outside(path) or not target.is_relative_to(checkout):
        raise ToolError(f"{path} is outside the checkout")
    return target.relative_to(checkout).as_posix()


def _list_files(checkout: Path, pathspec: str) -> list[str]:
    # What git tracks or would track: neither .git nor ignored files (.env and the like).
    return git_names(checkout, ["ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", pathspec],
                     skip=True)


def _read_file(checkout: Path, path: str, offset: int, limit: int) -> str:
    target = checkout / path
    if path not in _list_files(checkout, f":(literal){path}") or not target.is_file():
        raise ToolError(f"{path} is not a file of the checkout")
    if target.stat().st_size > _MAX_FILE:
        raise ToolError(f"{path} is larger than {_MAX_FILE} bytes; use search_files")
    data = target.read_bytes()
    if b"\0" in data:
        raise ToolError(f"{path} is a binary file")
    lines = data.decode("utf-8", errors="replace").split("\n")
    if not lines[-1]:
        lines.pop()
    end = min(len(lines), offset - 1 + limit)
    shown = "".join(f"{n}\t{lines[n - 1]}\n" for n in range(offset, end + 1))
    return shown + (f"[{len(lines) - end} more lines: go on with offset {end + 1}]\n" if end < len(lines) else "")


def _search(checkout: Path, pattern: str, pathspec: str) -> list[tuple[str, str, str]]:
    # (path, line number, line); links are not followed, ignored files are left out.
    out = run_git(checkout, [
        "grep", "-n", "-I", "-E", "-z", "--no-color", "--no-column", "--no-textconv", "--no-recurse-submodules",
        "--untracked", "-e", pattern, "--", pathspec,
    ], ok=(0, 1))
    # A file name may hold a newline, so each record is read from where the last one ended: the name up to the first
    # NUL, the number, the line. One that does not fit leaves unknown whose content follows, so nothing is shown.
    found: list[tuple[str, str, str]] = []
    at = 0
    while at < len(out):
        record = _MATCH.match(out, at)
        if record is None:
            raise ToolError("search_files: the result cannot be told apart by file; narrow the glob")
        path = file_name(record[1])
        if path is not None:
            found.append((path, record[2].decode(), record[3].decode("utf-8", errors="replace")))
        at = record.end()
    return found


def _shown_path(path: str) -> str:
    return path if path.isprintable() else json.dumps(path, ensure_ascii=False)


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


def _pathspec(arguments: dict[str, Any]) -> tuple[str | None, str]:
    # (the glob as given, the git pathspec for it)
    glob = _optional(arguments, "glob")
    if glob is not None and outside(glob):
        raise ToolError(f"{glob} is outside the checkout")
    return glob, f":(glob){glob or '**'}"


def _count(arguments: dict[str, Any], key: str, default: int) -> int:
    value = arguments.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ToolError(f"{key} must be a positive integer")
    return value


def _bounded(arguments: dict[str, Any], key: str, default: int, most: int) -> int:
    value = _count(arguments, key, default)
    if value > most:
        raise ToolError(f"{key} must be at most {most}")
    return value


def _strings(arguments: dict[str, Any], key: str) -> list[str]:
    value = arguments.get(key)
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in cast(list[object], value)):
        raise ToolError(f"{key} must be an array of strings")
    return cast(list[str], value)


_TEXT = {"type": "string"}
_TEXTS = {"type": "array", "items": _TEXT}
_FLAG = {"type": "boolean"}


def _schema(required: Sequence[str] = (), **properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": list(required)}


def _system_definitions() -> list[dict[str, Any]]:
    description = {**_TEXT, "description": "One sentence saying what the project is, in the person's words."}
    found = [
        {"name": "check_projects",
         "description": "List the projects Hibernaut is set up for, and what each lacks: a Steward for its main inbox, a "
                        "description, a place in the general Steward's list.",
         "inputSchema": _schema()},
        {"name": "onboard_project",
         "description": "Set a GitHub repository up as a project: clone it, register it in Orca, create the trigger label "
                        "where that is allowed, and add the project with its Steward to the configuration. What is there "
                        "already stays as it is.",
         "inputSchema": _schema(
             ("repo",), repo={**_TEXT, "description": "owner/name on GitHub."},
             name={**_TEXT, "description": "The project's name. Leave it out for the repository's own name in lower case."},
             description=description,
         )},
        {"name": "complete_project",
         "description": "Add what a project lacks according to check_projects, and nothing else: values that are set stay.",
         "inputSchema": _schema(
             ("name",), name={**_TEXT, "description": "The project's name, from check_projects."}, description=description,
         )},
        {"name": "runtime_status",
         "description": "How Hibernaut has been running, read only: failed tasks, suspended tasks and what each waits "
                        "for (approval, an answer, the user to leave the computer, a retry), delivery results, Outbox "
                        "messages that did not go out, and the last firings of each [[timers]] entry with their "
                        "results. Summaries only; it also says which time it covers and what it cannot see.",
         "inputSchema": _schema(
             hours={"type": "integer", "minimum": 1, "maximum": _MAX_STATUS_HOURS,
                    "description": f"How far back to look; default {_STATUS_HOURS}."},
             timer_runs={"type": "integer", "minimum": 1, "maximum": _MAX_TIMER_RUNS,
                         "description": f"Firings to show per timer; default {_TIMER_RUNS}."},
         )},
    ]
    for tool in found:
        tool["inputSchema"] = {**cast(dict[str, Any], tool["inputSchema"]), "additionalProperties": False}
    return found


@dataclass(slots=True)
class StewardTools:
    # The tools of one Steward run. Every call passes PermissionPort.check under its own tool name first.
    caps: RunCapabilities
    checkout: Path
    inbox_id: str
    sessions: SessionStorePort
    events: EventLister
    # The inboxes whose sessions and events it may list.
    in_project: Callable[[str], bool]
    # Set for a Steward of no project.
    projectless: bool = False
    # Set only for a Steward whose inbox has system tools; without it they are neither offered nor served.
    system: SystemTools | None = None
    calls: int = 0
    # Forwards this Run took; they go out once it ends in anything but a failure.
    forwarded: int = 0
    # forward_to_inbox arguments a person has to approve before they go out.
    deferred: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])

    @staticmethod
    def definitions(*, projectless: bool = False, system: bool = False) -> list[dict[str, Any]]:
        where = "your own inbox or a project in the routing manifest" if projectless else "this project"
        memory = "your inbox's memory" if projectless else "the project memory"
        whose = "your inbox's" if projectless else "the project's"
        inbox = {**_TEXT, "description": f"An inbox of {where}; leave out for the default."}
        glob = {**_TEXT, "description": "Limit to these paths, e.g. src/**/*.py or docs/**."}
        tools: list[dict[str, Any]] = [
            {"name": "forward_to_inbox",
             "description": "Hand work to another agent through its inbox. Nothing waits for it: end your turn after "
                            "forwarding. Without return_to that agent answers the person directly; with return_to "
                            "(normally your own inbox) its result comes back to you as an agent:return event. Work "
                            "another agent waits for goes on whole: name its return_to, then end your turn with silent.",
             "inputSchema": _schema(
                 ("target_inbox", "text"),
                 target_inbox={**_TEXT, "description": "An inbox id from the routing manifest."},
                 text={**_TEXT, "description": "The complete task: the receiver sees this, not your conversation."},
                 title=_TEXT, tags=_TEXTS,
                 target_agent={**_TEXT, "description": "Only for your own inbox: a project's rules pick its agent."
                               if projectless else "Leave out to let the inbox's rules pick the agent."},
                 return_to=_TEXT,
                 **({"continues": {**_TEXT, "description": "The key of a task you sent there before, from this "
                                   "conversation: text adds to that task, in its session and worktree. Leave "
                                   "return_to out."}} if projectless else {}),
             )},
            {"name": "read_file",
             "description": f"Read a file of the checkout, with line numbers, at most {_MAX_LINES} lines at a time. "
                            "Ignored files cannot be read.",
             "inputSchema": _schema(
                 ("path",), path={**_TEXT, "description": "Relative to the checkout, e.g. src/app.py."},
                 offset={"type": "integer", "description": "First line, from 1."}, limit={"type": "integer"},
             )},
            {"name": "search_files",
             "description": "Search the checkout's files for a POSIX extended regular expression. Files you may not "
                            "read are left out.",
             "inputSchema": _schema(("pattern",), pattern=_TEXT, glob=glob)},
            {"name": "list_files", "description": "List the checkout's files.", "inputSchema": _schema(glob=glob)},
            {"name": "read_memory", "description": f"Read a file of {memory}, e.g. notes/release.md.",
             "inputSchema": _schema(("path",), path=_TEXT)},
            {"name": "write_memory",
             "description": f"Replace a file of {memory}. reported.md is appended to instead; instructions.md "
                            "waits for a person's approval.",
             "inputSchema": _schema(("path", "content"), path=_TEXT, content=_TEXT)},
            *memory_mcp.tool_definitions(memory=memory, whose=whose, uris="proj://, shared://, global:// or kb://"),
            {"name": "memory_propose_write",
             "description": f"Governed write to {memory if projectless else 'project memory'}: notes/ and progress/ apply at once (near-duplicates are "
                            "refused unless supersedes or force_new is given), instructions.md and shared:// wait for "
                            "a person's approval. base_sha256 from memory_read makes it a compare-and-set.",
             "inputSchema": _schema(
                 ("target_uri", "op", "content", "rationale"), target_uri=_TEXT,
                 op={"type": "string", "enum": ["append", "replace_section", "upsert_file", "archive",
                                                "append_reported"]},
                 content=_TEXT, rationale=_TEXT, section_heading=_TEXT, base_sha256=_TEXT, evidence_ref=_TEXT,
                 supersedes=_TEXTS, force_new=_FLAG,
             )},
            {"name": "list_sessions", "description": f"List the sessions of {where}, newest first."
             if projectless else "List this project's sessions, newest first.",
             "inputSchema": _schema(inbox_id=inbox, status={"type": "string", "enum": list(_STATUSES)})},
            {"name": "list_events", "description": f"List the inbox events of {where}, newest first."
             if projectless else "List this project's inbox events, newest first.",
             "inputSchema": _schema(inbox_id=inbox, status=_TEXT, limit={"type": "integer"})},
            {"name": "git",
             "description": f"Read the checkout's history: {', '.join(GIT_COMMANDS)}. Spell options out, put paths after "
                            "--. log gives no patches: use show <commit>, diff or blame, which need every file they show "
                            "to be readable to you; --stat and --name-only need none.",
             "inputSchema": _schema(("args",), args={**_TEXTS, "description": 'e.g. ["log", "--oneline", "-10"]'})},
        ]
        if not system:
            return tools
        # It passes no work on.
        return [*_system_definitions(), *(tool for tool in tools if tool["name"] != "forward_to_inbox")]

    async def call(self, name: object, arguments: object) -> tuple[str, bool]:
        # (answer, is_error)
        handlers: dict[str, Callable[[dict[str, Any]], Awaitable[str]]] = {
            "forward_to_inbox": self._forward,
            "read_file": self._read_file,
            "search_files": self._search_files,
            "list_files": self._list_files,
            "read_memory": self._read_memory,
            "write_memory": self._write_memory,
            # No memory_approve_proposal: a Run cannot ask a person in the middle of a turn, and must not decide itself.
            "memory_search": self._memory_tool("memory_search"),
            "memory_read": self._memory_tool("memory_read"),
            "memory_propose_write": self._memory_tool("memory_propose_write"),
            "list_sessions": self._list_sessions,
            "list_events": self._list_events,
            "git": self._run_git,
        }
        if self.system is not None:
            del handlers["forward_to_inbox"]
            handlers |= {"check_projects": self._check_projects, "onboard_project": self._onboard_project,
                         "complete_project": self._complete_project, "runtime_status": self._runtime_status}
        handler = handlers.get(name) if isinstance(name, str) else None
        if handler is None:
            return f"unknown tool {name}", True
        if not isinstance(arguments, dict):
            return "arguments must be an object", True
        self.calls += 1
        try:
            return clip(await handler(cast(dict[str, Any], arguments))), False
        except (ToolError, OSError, ValueError) as exc:
            return str(exc), True

    def _allow(self, tool: str, args: dict[str, Any]) -> None:
        decision = self.caps.permissions.check(tool, args)
        if decision.action != "allow":
            raise ToolError(f"{tool} is not permitted ({decision.action}): {decision.reason}")

    def _system(self, tool: str, arguments: dict[str, Any], *keys: str) -> SystemTools:
        assert self.system is not None
        unknown = sorted(set(arguments) - set(keys))
        if unknown:
            raise ToolError(f"{tool} takes {', '.join(keys) or 'no arguments'}, not {', '.join(unknown)}")
        self._allow(tool, arguments)
        return self.system

    async def _check_projects(self, arguments: dict[str, Any]) -> str:
        system = self._system("check_projects", arguments)
        return await asyncio.to_thread(system.check)

    async def _onboard_project(self, arguments: dict[str, Any]) -> str:
        repo, name = _required(arguments, "repo"), _optional(arguments, "name")
        description = _optional(arguments, "description")
        system = self._system("onboard_project", arguments, "repo", "name", "description")
        return await asyncio.to_thread(system.onboard, repo=repo, name=name, description=description)

    async def _complete_project(self, arguments: dict[str, Any]) -> str:
        name, description = _required(arguments, "name"), _optional(arguments, "description")
        system = self._system("complete_project", arguments, "name", "description")
        return await asyncio.to_thread(system.complete, name=name, description=description)

    async def _runtime_status(self, arguments: dict[str, Any]) -> str:
        hours = _bounded(arguments, "hours", _STATUS_HOURS, _MAX_STATUS_HOURS)
        timer_runs = _bounded(arguments, "timer_runs", _TIMER_RUNS, _MAX_TIMER_RUNS)
        system = self._system("runtime_status", arguments, "hours", "timer_runs")
        return await asyncio.to_thread(system.status, hours=hours, timer_runs=timer_runs)

    def _readable(self, path: str) -> bool:
        # Every way a file's content is shown passes the gate as a read of that file.
        if self.system is not None and self.system.hides(self.checkout / path):
            return False
        return self.caps.permissions.check("read_file", {"path": path}).action == "allow"

    async def _forward(self, arguments: dict[str, Any]) -> str:
        wanted: dict[str, Any] = {
            "target_inbox": _required(arguments, "target_inbox"),
            "text": _required(arguments, "text"),
            "title": _optional(arguments, "title") or "",
            "tags": _strings(arguments, "tags"),
            "target_agent": _optional(arguments, "target_agent"),
            "return_to": _optional(arguments, "return_to"),
        }
        added = _optional(arguments, "continues")
        if added is not None:
            wanted["continues"] = added
        try:
            event = self.forward(wanted)
        except ForwardRefused as exc:
            if exc.action != "defer":
                raise ToolError(f"not forwarded: {exc}") from exc
            self.deferred.append(wanted)
            return (f"Held for a person's approval: {exc}. Hibernaut asks for it when this turn ends and forwards the "
                    "work once it is approved; do not forward it again.")
        if self.projectless and event.inbox_id != self.inbox_id:
            did = "Added to task" if added is not None else "Forwarded as task"
            return (f"{did} {event.correlation_key} in {event.inbox_id}; it is queued when this turn ends. To add to it "
                    "later, forward there again with continues set to that key.")
        return f"Forwarded to {event.inbox_id}; it is queued when this turn ends."

    def forward(self, wanted: dict[str, Any], approved_request_id: str | None = None) -> Event:
        options = {k: v for k, v in wanted.items() if k not in ("target_inbox", "text")}
        event = self.caps.forward_to_inbox(
            wanted["target_inbox"], wanted["text"], **options, approved_request_id=approved_request_id
        )
        self.forwarded += 1
        return event

    async def _read_file(self, arguments: dict[str, Any]) -> str:
        path = _real_path(self.checkout, _required(arguments, "path"))
        offset, limit = _count(arguments, "offset", 1), min(_count(arguments, "limit", _MAX_LINES), _MAX_LINES)
        self._allow("read_file", {"path": path})
        if self.system is not None and self.system.hides(self.checkout / path):
            raise ToolError(f"{path} is not a file of the checkout")
        return await asyncio.to_thread(_read_file, self.checkout, path, offset, limit)

    async def _search_files(self, arguments: dict[str, Any]) -> str:
        pattern, (glob, pathspec) = _required(arguments, "pattern"), _pathspec(arguments)
        self._allow("search_files", {"pattern": pattern, "glob": glob})
        readable: dict[str, bool] = {}
        shown: list[str] = []
        for path, number, line in await asyncio.to_thread(_search, self.checkout, pattern, pathspec):
            # Saying how many were left out would tell whether the pattern matches in them.
            if path not in readable:
                readable[path] = self._readable(path)
            if not readable[path]:
                continue
            if len(shown) == _MAX_MATCHES:
                shown.append("[more matches: narrow the pattern or the glob]\n")
                break
            shown.append(f"{_shown_path(path)}:{number}:{line[:_SNIPPET]}\n")
        return "".join(shown)

    async def _list_files(self, arguments: dict[str, Any]) -> str:
        glob, pathspec = _pathspec(arguments)
        self._allow("list_files", {"glob": glob})
        found = await asyncio.to_thread(_list_files, self.checkout, pathspec)
        more = [f"[{len(found) - _MAX_LINES} more files: narrow the glob]"] if len(found) > _MAX_LINES else []
        return "".join(f"{name}\n" for name in [*found[:_MAX_LINES], *more])

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
        return write_memory(self.caps.memory, path, content)

    def _memory_tool(self, name: str) -> Callable[[dict[str, Any]], Awaitable[str]]:
        async def run(arguments: dict[str, Any]) -> str:
            self._allow(name, arguments)
            return await asyncio.to_thread(call_memory_tool, self.caps.memory, name, arguments, actor=self.inbox_id)

        return run

    def _inbox(self, arguments: dict[str, Any]) -> str | None:
        inbox_id = _optional(arguments, "inbox_id")
        if inbox_id is not None and not self.in_project(inbox_id):
            where = "your inbox or of a project you may forward to" if self.projectless else "this project"
            raise ToolError(f"'{inbox_id}' is not an inbox of {where}")
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
                    "return_to", "from_task"):
            value = event.payload.get(key)
            if isinstance(value, str) and value:
                row[key] = value[:_SNIPPET]
            elif isinstance(value, int):
                row[key] = value
        return row

    async def _run_git(self, arguments: dict[str, Any]) -> str:
        args = _strings(arguments, "args")
        call = check_git_args(args)
        self._allow("git_read", {"args": args})
        shown, argv = await asyncio.to_thread(plan_git, self.checkout, call)
        refused = [path for path in shown if not self._readable(path)]
        if refused:
            raise ToolError(
                f"git: this shows the content of files you may not read ({', '.join(refused[:20])}); name other files "
                "after --, or ask for --stat or --name-only"
            )
        return await asyncio.to_thread(exec_git, self.checkout, argv) if argv else ""

    @contextlib.asynccontextmanager
    async def serve(self) -> AsyncGenerator[str]:
        # A socket in a fresh 0700 directory, gone with the run: only processes of this user can reach it.
        base = tempfile.gettempdir()
        if len(os.fsencode(base)) > MAX_SOCKET_PATH - 24:
            base = "/tmp"
        with tempfile.TemporaryDirectory(prefix="hb-steward-", dir=base) as tmp:
            path = os.path.join(tmp, "s")
            # No tool call outlives the run: a late forward would miss the batch the worker queues.
            async with line_server(path, self._answer):
                yield path

    async def _answer(self, line: bytes) -> dict[str, Any]:
        item = request_of(line)
        if item is None:
            return {"text": "malformed request", "error": True}
        if item.get("op") == "tools":
            return {"tools": self.definitions(projectless=self.projectless, system=self.system is not None)}
        text, error = await self.call(item.get("name"), item.get("arguments"))
        return {"text": text, "error": error}
