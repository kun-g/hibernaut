from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import Event, utc_now_iso

_COLUMNS = ("attempt_id", "inbox_id", "goal", "external_id", "title", "session_id", "work_key", "summary", "result",
            "detail", "pr_url", "workdir", "seen_head", "dev_head", "written")
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
    workdir: str = ""
    # The worktree's HEAD when last looked at: at the begin, and after each developer turn.
    seen_head: str = ""
    # The HEAD the attempt's own last commits made; empty while it has committed nothing.
    dev_head: str = ""
    # Whether the project memory says what this row says.
    written: bool = False

    def said(self) -> tuple[str, ...]:
        return self.summary, self.result, self.detail, self.pr_url


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

    def get(self, attempt_id: str) -> AttemptRow:
        row = self._db.fetchone(f"{_SELECT} WHERE attempt_id = ?", (attempt_id,))
        if row is None:
            raise LookupError(f"no attempt {attempt_id}")
        return _row(row)

    def update(self, attempt_id: str, change: Callable[[AttemptRow], AttemptRow]) -> AttemptRow:
        # change gets the row as it is now, whatever the caller read before: the transaction has the write lock (BEGIN
        # IMMEDIATE) before it reads, so no other connection writes in between. They wait meanwhile: no git in it.
        # What is written stays so as long as the row says the same.
        with self._db.transaction() as conn:
            row = self.get(attempt_id)
            now = change(row)
            now = replace(now, written=row.written and now.said() == row.said())
            if now != row:
                conn.execute(
                    "UPDATE attempt_notes SET work_key = ?, workdir = ?, seen_head = ?, dev_head = ?, summary = ?, "
                    "result = ?, detail = ?, pr_url = ?, written = ? WHERE attempt_id = ?",
                    (now.work_key, now.workdir, now.seen_head, now.dev_head, *now.said(), now.written, attempt_id),
                )
        return now

    def mark_written(self, row: AttemptRow) -> None:
        # row: as the file says it. Compared in the same statement: one that changed meanwhile is still to be written.
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE attempt_notes SET written = 1 "
                "WHERE attempt_id = ? AND summary = ? AND result = ? AND detail = ? AND pr_url = ?",
                (row.attempt_id, *row.said()),
            )


def _row(row: Any) -> AttemptRow:
    return AttemptRow(**{name: row[name] for name in _COLUMNS[:-1]}, written=bool(row["written"]))
