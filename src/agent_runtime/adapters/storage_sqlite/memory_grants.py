from __future__ import annotations

import secrets

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import utc_now_iso


class SQLiteMemoryGrantStore:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def issue(self, session_id: str) -> str:
        # A session keeps its token: agents it started before a restart still hold it.
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT INTO memory_grants (session_id, token, created_at) VALUES (?, ?, ?) "
                "ON CONFLICT (session_id) DO NOTHING",
                (session_id, secrets.token_urlsafe(32), utc_now_iso()),
            )
            row = conn.execute("SELECT token FROM memory_grants WHERE session_id = ?", (session_id,)).fetchone()
        return str(row["token"])

    def session_for(self, token: str) -> str | None:
        row = self._db.fetchone("SELECT session_id FROM memory_grants WHERE token = ?", (token,))
        return str(row["session_id"]) if row is not None else None
