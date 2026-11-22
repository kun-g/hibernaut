from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import utc_now_iso


@dataclass(frozen=True, slots=True)
class TerminalBinding:
    # Which visible agent conversation a Hibernaut session continues: the live terminal (handle plus the
    # process incarnation that received earlier prompts) and the agent's own session ID for --resume.
    session_id: str
    executor_id: str
    agent: str
    workdir: str
    handle: str | None = None
    incarnation_id: str | None = None
    native_session_id: str | None = None
    # Peer handoff: the Claude Code registration the terminal's session answers on, and the attempt it was last sent.
    peer_name: str | None = None
    peer_pid: int | None = None
    peer_socket: str | None = None
    peer_attempt: str | None = None


_COLUMNS = ("session_id, executor_id, agent, workdir, handle, incarnation_id, native_session_id, "
            "peer_name, peer_pid, peer_socket, peer_attempt")


class SQLiteTerminalBindingStore:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    @staticmethod
    def _row(row: object) -> TerminalBinding:
        r = cast(dict[str, object], row)

        def opt(key: str) -> str | None:
            val = r[key]
            return str(val) if val is not None else None

        return TerminalBinding(
            session_id=str(r["session_id"]),
            executor_id=str(r["executor_id"]),
            agent=str(r["agent"]),
            workdir=str(r["workdir"]),
            handle=opt("handle"),
            incarnation_id=opt("incarnation_id"),
            native_session_id=opt("native_session_id"),
            peer_name=opt("peer_name"),
            peer_pid=int(cast(int, r["peer_pid"])) if r["peer_pid"] is not None else None,
            peer_socket=opt("peer_socket"),
            peer_attempt=opt("peer_attempt"),
        )

    def get(self, session_id: str) -> TerminalBinding | None:
        row = self._db.fetchone(f"SELECT {_COLUMNS} FROM terminal_bindings WHERE session_id = ?", (session_id,))
        return self._row(row) if row is not None else None

    def find_by_native(self, executor_id: str, native_session_id: str) -> TerminalBinding | None:
        row = self._db.fetchone(
            f"SELECT {_COLUMNS} FROM terminal_bindings WHERE executor_id = ? AND native_session_id = ? "
            "ORDER BY updated_at DESC LIMIT 1",
            (executor_id, native_session_id),
        )
        return self._row(row) if row is not None else None

    def save(self, binding: TerminalBinding) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                f"""
                INSERT INTO terminal_bindings ({_COLUMNS}, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    executor_id = excluded.executor_id,
                    agent = excluded.agent,
                    workdir = excluded.workdir,
                    handle = excluded.handle,
                    incarnation_id = excluded.incarnation_id,
                    native_session_id = excluded.native_session_id,
                    peer_name = excluded.peer_name,
                    peer_pid = excluded.peer_pid,
                    peer_socket = excluded.peer_socket,
                    peer_attempt = excluded.peer_attempt,
                    updated_at = excluded.updated_at
                """,
                (
                    binding.session_id,
                    binding.executor_id,
                    binding.agent,
                    binding.workdir,
                    binding.handle,
                    binding.incarnation_id,
                    binding.native_session_id,
                    binding.peer_name,
                    binding.peer_pid,
                    binding.peer_socket,
                    binding.peer_attempt,
                    utc_now_iso(),
                ),
            )
