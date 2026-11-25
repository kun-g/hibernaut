from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import Event, utc_now_iso

_COLUMNS = ("attempt_id", "inbox_id", "goal", "external_id", "title", "session_id", "work_key", "summary", "result",
            "detail", "pr_url", "written")
_SELECT = f"SELECT {', '.join(_COLUMNS)} FROM attempt_notes"


@dataclass(frozen=True, slots=True)
class AttemptRow:
    attempt_id: str
    inbox_id: str
    goal: str
    external_id: str
    title: str
    session_id: str
    work_key: str = ""
    summary: str = ""
    # Empty until the task ended once.
    result: str = ""
    detail: str = ""
    pr_url: str = ""
    # Whether the project memory says what this row says.
    written: bool = False


def attempt_id(event: Event) -> str:
    # The inbox's deduplication key: a replayed or redelivered event is the same task.
    key = "\0".join((event.inbox_id, event.source, event.external_id))
    return hashlib.sha256(key.encode()).hexdigest()[:16]


class SQLiteAttemptStore:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def begin(self, event: Event, *, goal: str, title: str, session_id: str) -> None:
        now = utc_now_iso()
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT INTO attempt_notes (attempt_id, inbox_id, goal, external_id, title, session_id, begun_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (attempt_id) DO UPDATE SET session_id = excluded.session_id, begun_at = excluded.begun_at",
                (attempt_id(event), event.inbox_id, goal, event.external_id, title, session_id, now),
            )

    def latest(self, session_id: str) -> AttemptRow | None:
        row = self._db.fetchone(f"{_SELECT} WHERE session_id = ? ORDER BY begun_at DESC, rowid DESC LIMIT 1", (session_id,))
        return _row(row) if row is not None else None

    def by_work_key(self, work_key: str) -> list[AttemptRow]:
        # Newest first.
        if not work_key:
            return []
        return [_row(r) for r in self._db.fetchall(
            f"{_SELECT} WHERE work_key = ? ORDER BY begun_at DESC, rowid DESC", (work_key,))]

    def locate(self, attempt: str, work_key: str) -> None:
        with self._db.transaction() as conn:
            conn.execute("UPDATE attempt_notes SET work_key = ? WHERE attempt_id = ?", (work_key, attempt))

    def save(self, row: AttemptRow) -> None:
        # What the note is to say; written again once it does.
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE attempt_notes SET summary = ?, result = ?, detail = ?, pr_url = ?, written = 0 WHERE attempt_id = ?",
                (row.summary, row.result, row.detail, row.pr_url, row.attempt_id),
            )

    def mark_written(self, row: AttemptRow) -> None:
        # Unless it changed meanwhile.
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE attempt_notes SET written = 1 "
                "WHERE attempt_id = ? AND summary = ? AND result = ? AND detail = ? AND pr_url = ?",
                (row.attempt_id, row.summary, row.result, row.detail, row.pr_url),
            )


def _row(row: Any) -> AttemptRow:
    return AttemptRow(**{name: row[name] for name in _COLUMNS[:-1]}, written=bool(row["written"]))
