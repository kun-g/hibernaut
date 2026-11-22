from __future__ import annotations

import json
import os
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from agent_runtime.adapters.storage_sqlite.stores import SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore

# Claude Code's native cross-session messaging. A visible role session is addressed by its --name; the
# registry entry Claude Code keeps per live process maps that name to the native session ID it runs.
PEER_TOOLS = ("SendMessage", "ListAgents")
# Subagent tools: a reviewer must be its own task, never a child the developer starts.
SUBAGENT_TOOLS = ("Agent", "Task")


@dataclass(frozen=True, slots=True)
class PeerSession:
    name: str
    native_session_id: str
    hibernaut_session_id: str
    executor_id: str
    # The session's messaging socket; Claude Code puts it in the `from` of every message the session sends.
    address: str


@dataclass(frozen=True, slots=True)
class PeerLookup:
    peer: PeerSession | None
    # Why there is no channel, when peer is None.
    reason: str = ""


def registry_dirs(config_roots: Sequence[Path]) -> list[Path]:
    return [root / "sessions" for root in config_roots]


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def live_sessions(dirs: Sequence[Path]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for directory in dirs:
        for path in directory.glob("*.json"):
            try:
                raw: object = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if not isinstance(raw, dict):
                continue
            entry = cast(dict[str, Any], raw)
            pid = entry.get("pid")
            if isinstance(pid, int) and entry.get("messagingSocketPath") and _alive(pid):
                entries.append(entry)
    return entries


class DeveloperPeerResolver:
    # Finds the developer session a review_of key names, through bindings the runtime itself recorded:
    # the task session of that key in the same inbox, run by a developer executor, whose terminal binding ran
    # in the reviewed workdir and whose native session is live under exactly one name. Anything else: no channel.
    def __init__(
        self,
        *,
        sessions: SQLiteSessionStoreAdapter,
        bindings: SQLiteTerminalBindingStore,
        developer_executors: Collection[str],
        registry: Sequence[Path],
    ) -> None:
        self._sessions = sessions
        self._bindings = bindings
        self._developers = frozenset(developer_executors)
        self._registry = list(registry)

    def resolve(self, *, inbox_id: str, review_of: str, workdir: Path) -> PeerLookup:
        session = self._sessions.find_by_correlation_key(inbox_id, review_of)
        if session is None:
            return PeerLookup(None, f"no open task session for '{review_of}' in {inbox_id}")
        if session.executor_id not in self._developers:
            return PeerLookup(None, f"task '{review_of}' is run by '{session.executor_id}', not a developer executor")
        binding = self._bindings.get(session.session_id)
        if binding is None or binding.executor_id != session.executor_id or not binding.native_session_id:
            return PeerLookup(None, f"task '{review_of}' has no visible developer conversation")
        here = os.path.realpath(workdir)
        if os.path.realpath(binding.workdir) != here:
            return PeerLookup(None, f"the developer of '{review_of}' worked in {binding.workdir}, not {here}")
        native = binding.native_session_id
        if session.executor_session_id and session.executor_session_id != native:
            return PeerLookup(None, f"task '{review_of}' records two developer sessions")
        live = live_sessions(self._registry)
        mine = [e for e in live if e.get("sessionId") == native]
        if len(mine) != 1:
            return PeerLookup(None, f"the developer session {native} is not running")
        entry = mine[0]
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            return PeerLookup(None, f"the developer session {native} has no name to message")
        cwd = entry.get("cwd")
        if not isinstance(cwd, str) or os.path.realpath(cwd) != here:
            return PeerLookup(None, f"the developer session {native} runs in {cwd}, not {here}")
        if sum(1 for e in live if e.get("name") == name) != 1:
            return PeerLookup(None, f"the name '{name}' is held by more than one live session")
        return PeerLookup(PeerSession(name=name, native_session_id=native, hibernaut_session_id=session.session_id,
                                      executor_id=session.executor_id,
                                      address=f"uds:{entry['messagingSocketPath']}"))
