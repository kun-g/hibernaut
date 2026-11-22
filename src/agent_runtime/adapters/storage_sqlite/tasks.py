from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import utc_now_iso


@dataclass(frozen=True, slots=True)
class TaskMetadata:
    work_key: str
    workdir: str
    # Commit whose guidance files the task's development, rework, resumes and independent reviews read.
    guidance_commit: str
    activities: tuple[str, ...]
    tags: tuple[str, ...]
    created_at: str
    updated_at: str


def _strings(raw: object) -> tuple[str, ...]:
    data = json.loads(str(raw))
    return tuple(str(v) for v in cast(list[object], data)) if isinstance(data, list) else ()


class SQLiteTaskMetadataStore:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def get(self, work_key: str) -> TaskMetadata | None:
        row = self._db.fetchone(
            "SELECT work_key, workdir, guidance_commit, activities_json, tags_json, created_at, updated_at "
            "FROM task_metadata WHERE work_key = ?",
            (work_key,),
        )
        if row is None:
            return None
        return TaskMetadata(
            work_key=str(row["work_key"]),
            workdir=str(row["workdir"]),
            guidance_commit=str(row["guidance_commit"]),
            activities=_strings(row["activities_json"]),
            tags=_strings(row["tags_json"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def save(
        self,
        work_key: str,
        *,
        workdir: str,
        guidance_commit: str,
        activities: Sequence[str],
        tags: Sequence[str],
    ) -> TaskMetadata:
        now = utc_now_iso()
        with self._db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO task_metadata
                    (work_key, workdir, guidance_commit, activities_json, tags_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(work_key) DO UPDATE SET
                    workdir = excluded.workdir,
                    guidance_commit = excluded.guidance_commit,
                    activities_json = excluded.activities_json,
                    tags_json = excluded.tags_json,
                    updated_at = excluded.updated_at
                """,
                (work_key, workdir, guidance_commit, json.dumps(list(activities)), json.dumps(list(tags)), now, now),
            )
        record = self.get(work_key)
        assert record is not None
        return record
