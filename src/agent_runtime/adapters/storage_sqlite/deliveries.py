from __future__ import annotations

import json
from typing import Any, cast

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import utc_now_iso


class SQLiteDeliveryStore:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def get(self, work_key: str) -> dict[str, Any] | None:
        row = self._db.fetchone("SELECT record_json FROM deliveries WHERE work_key = ?", (work_key,))
        if row is None:
            return None
        data: object = json.loads(str(row["record_json"]))
        return cast(dict[str, Any], data) if isinstance(data, dict) else None

    def save(self, work_key: str, record: dict[str, Any]) -> None:
        now = utc_now_iso()
        with self._db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO deliveries (work_key, state, pr_url, version, merge_sha, record_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(work_key) DO UPDATE SET
                    state = excluded.state,
                    pr_url = excluded.pr_url,
                    version = excluded.version,
                    merge_sha = excluded.merge_sha,
                    record_json = excluded.record_json,
                    updated_at = excluded.updated_at
                """,
                (
                    work_key,
                    str(record.get("state", "local")),
                    str(record.get("pr_url", "")),
                    str(record.get("version", "")),
                    str(record.get("merge_sha", "")),
                    json.dumps(record, ensure_ascii=False),
                    now,
                    now,
                ),
            )
