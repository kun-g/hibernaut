from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import utc_now_iso


@dataclass(frozen=True, slots=True)
class QuotaMark:
    executor_id: str
    until: float
    reason: str


@dataclass(frozen=True, slots=True)
class ReviewerChoice:
    # "" while the task has none: before its first review, or after the one it had ran out of quota.
    reviewer: str = ""
    # The list the task chooses from; empty for a single configured reviewer, and while nobody was available.
    candidates: tuple[str, ...] = ()
    # Why each reviewer of the task was chosen, in order.
    notes: tuple[str, ...] = ()
    # Why the last one was left, until the next one is chosen; left_name is who that was.
    left: str = ""
    left_name: str = ""


def _strings(raw: object) -> tuple[str, ...]:
    data = json.loads(str(raw))
    return tuple(str(v) for v in cast(list[object], data)) if isinstance(data, list) else ()


class SQLiteReviewerStore:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def mark(self, executor_id: str, *, until: float, reason: str) -> None:
        # Two tasks may meet the error at once; the later end stands.
        with self._db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO reviewer_quota_marks (executor_id, until, reason, updated_at) VALUES (?, ?, ?, ?)
                ON CONFLICT(executor_id) DO UPDATE SET
                    until = excluded.until, reason = excluded.reason, updated_at = excluded.updated_at
                WHERE excluded.until >= until
                """,
                (executor_id, until, reason, utc_now_iso()),
            )

    def marks(self, now: float) -> dict[str, QuotaMark]:
        rows = self._db.fetchall("SELECT executor_id, until, reason FROM reviewer_quota_marks WHERE until > ?", (now,))
        return {str(r["executor_id"]): QuotaMark(str(r["executor_id"]), float(r["until"]), str(r["reason"]))
                for r in rows}

    def choice(self, session_id: str, owner: str) -> ReviewerChoice:
        row = self._db.fetchone(
            "SELECT reviewer, candidates_json, notes_json, left_reason, left_name FROM reviewer_choices "
            "WHERE session_id = ? AND executor_id = ?",
            (session_id, owner),
        )
        if row is None:
            return ReviewerChoice()
        return ReviewerChoice(
            reviewer=str(row["reviewer"]), candidates=_strings(row["candidates_json"]), notes=_strings(row["notes_json"]),
            left=str(row["left_reason"]), left_name=str(row["left_name"]),
        )

    def save(
        self, session_id: str, owner: str, choice: ReviewerChoice, also: Callable[[], object] | None = None
    ) -> None:
        # also runs in the same transaction: the notice of a switch is queued exactly when the switch is recorded.
        with self._db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO reviewer_choices
                    (session_id, executor_id, reviewer, candidates_json, notes_json, left_reason, left_name, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, executor_id) DO UPDATE SET
                    reviewer = excluded.reviewer, candidates_json = excluded.candidates_json,
                    notes_json = excluded.notes_json, left_reason = excluded.left_reason,
                    left_name = excluded.left_name, updated_at = excluded.updated_at
                """,
                (session_id, owner, choice.reviewer, json.dumps(list(choice.candidates), ensure_ascii=False),
                 json.dumps(list(choice.notes), ensure_ascii=False), choice.left, choice.left_name, utc_now_iso()),
            )
            if also is not None:
                also()
