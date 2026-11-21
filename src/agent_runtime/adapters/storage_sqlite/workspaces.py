from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias, cast

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import utc_now_iso

WorkspaceStatus: TypeAlias = Literal["creating", "ready", "reclaiming"]


@dataclass(frozen=True, slots=True)
class WorkspaceRecord:
    work_key: str
    repo_checkout: str
    workdir: str
    branch: str
    status: WorkspaceStatus
    created_at: str
    updated_at: str
    seq: int = 0


class SQLiteWorkspaceStore:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def _row_to_record(self, row: object) -> WorkspaceRecord:
        r = cast(dict[str, object], row)
        return WorkspaceRecord(
            seq=int(str(r["seq"])),
            work_key=str(r["work_key"]),
            repo_checkout=str(r["repo_checkout"]),
            workdir=str(r["workdir"]),
            branch=str(r["branch"]),
            status=cast(WorkspaceStatus, str(r["status"])),
            created_at=str(r["created_at"]),
            updated_at=str(r["updated_at"]),
        )

    def get(self, work_key: str) -> WorkspaceRecord | None:
        row = self._db.fetchone(
            """
            SELECT seq, work_key, repo_checkout, workdir, branch, status, created_at, updated_at
            FROM workspaces
            WHERE work_key = ?
            """,
            (work_key,),
        )
        return self._row_to_record(row) if row is not None else None

    def list_by_repo(self, repo_checkout: str) -> list[WorkspaceRecord]:
        rows = self._db.fetchall(
            """
            SELECT seq, work_key, repo_checkout, workdir, branch, status, created_at, updated_at
            FROM workspaces
            WHERE repo_checkout = ?
            ORDER BY seq ASC
            """,
            (repo_checkout,),
        )
        return [self._row_to_record(row) for row in rows]

    def list_all(self) -> list[WorkspaceRecord]:
        rows = self._db.fetchall(
            """
            SELECT seq, work_key, repo_checkout, workdir, branch, status, created_at, updated_at
            FROM workspaces
            ORDER BY seq ASC
            """
        )
        return [self._row_to_record(row) for row in rows]

    def next_task_seq(self) -> int:
        row = self._db.fetchone("SELECT COALESCE(MAX(seq), 0) AS max_seq FROM workspaces")
        return int(row["max_seq"]) + 1 if row is not None else 1

    def reserve_creating(
        self,
        *,
        work_key: str,
        repo_checkout: str,
        workdir: str,
        branch: str,
    ) -> WorkspaceRecord:
        now = utc_now_iso()
        with self._db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO workspaces (
                    work_key, repo_checkout, workdir, branch, status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, 'creating', ?, ?)
                ON CONFLICT(work_key) DO UPDATE SET
                    repo_checkout = excluded.repo_checkout,
                    workdir = excluded.workdir,
                    branch = excluded.branch,
                    status = 'creating',
                    updated_at = excluded.updated_at
                """,
                (work_key, repo_checkout, workdir, branch, now, now),
            )
        rec = self.get(work_key)
        assert rec is not None
        return rec

    def mark_ready(self, work_key: str, *, workdir: str, branch: str) -> WorkspaceRecord:
        now = utc_now_iso()
        with self._db.transaction() as conn:
            conn.execute(
                """
                UPDATE workspaces
                SET workdir = ?, branch = ?, status = 'ready', updated_at = ?
                WHERE work_key = ?
                """,
                (workdir, branch, now, work_key),
            )
        rec = self.get(work_key)
        assert rec is not None
        return rec

    def claim_for_reclaim(self, work_key: str, *, seen_updated_at: str) -> bool:
        # Compare-and-set on updated_at: of several callers that read the same record, only
        # one moves it to 'reclaiming'; a crashed claim can still be retried from a fresh read.
        now = utc_now_iso()
        while now == seen_updated_at:
            now = utc_now_iso()
        with self._db.transaction() as conn:
            cur = conn.execute(
                """
                UPDATE workspaces SET status = 'reclaiming', updated_at = ?
                WHERE work_key = ? AND updated_at = ?
                """,
                (now, work_key, seen_updated_at),
            )
            return cur.rowcount == 1

    def delete(self, work_key: str) -> None:
        with self._db.transaction() as conn:
            conn.execute("DELETE FROM workspaces WHERE work_key = ?", (work_key,))
