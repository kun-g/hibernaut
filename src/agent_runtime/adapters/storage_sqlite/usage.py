from __future__ import annotations

import json
from typing import Any, cast

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase


def _review_key(work_key: str) -> str:
    return f"review:{work_key}"


class SQLiteWorkUsage:
    # Who still needs a task's worktree, read from the shared database at the moment of asking. Sessions that carry
    # the work_key itself are counted by the session store; this adds independently dispatched reviewers (their own
    # correlation_key, review_of in the payload), queued or running events, open Runs of its sessions, and a
    # delivery left unfinished.
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def blockers(self, work_key: str) -> list[str]:
        out = self.peers(work_key)
        rows = self._db.fetchall(
            """
            SELECT event_id, status FROM event_inbox
            WHERE status IN ('pending', 'processing') AND correlation_key = ?
              AND COALESCE(json_extract(payload_json, '$.action'), '') != 'close_session'
            ORDER BY seq
            """,
            (work_key,),
        )
        out += [f"event {r['event_id']} ({r['status']})" for r in rows]
        # A Run left open (its executor may still write in the worktree) holds it whatever its session's status.
        runs = self._db.fetchall(
            """
            SELECT o.session_id FROM open_runs o JOIN sessions s ON s.session_id = o.session_id
            WHERE s.work_key = ? ORDER BY o.started_at, o.session_id
            """,
            (work_key,),
        )
        out += [f"run of session {r['session_id']} is open" for r in runs]
        row = self._db.fetchone("SELECT record_json FROM deliveries WHERE work_key = ?", (work_key,))
        if row is not None:
            raw: object = json.loads(str(row["record_json"]))
            problem = cast(dict[str, Any], raw).get("problem") if isinstance(raw, dict) else None
            if problem:
                out.append(f"delivery is unfinished: {problem}")
        return out

    def peers(self, work_key: str) -> list[str]:
        # Live exchanges with the task's developer: reviewer sessions and review requests still queued or running.
        review = _review_key(work_key)
        sessions = self._db.fetchall(
            """
            SELECT session_id, status FROM sessions
            WHERE status IN ('active', 'suspended') AND (work_key = ? OR correlation_key = ?)
            ORDER BY updated_at
            """,
            (review, review),
        )
        out = [f"session {r['session_id']} ({r['status']})" for r in sessions]
        events = self._db.fetchall(
            """
            SELECT event_id, status FROM event_inbox
            WHERE status IN ('pending', 'processing')
              AND (correlation_key = ? OR json_extract(payload_json, '$.review_of') = ?)
            ORDER BY seq
            """,
            (review, work_key),
        )
        out += [f"review event {r['event_id']} ({r['status']})" for r in events]
        return out
