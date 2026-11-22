from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from taskboard.core.dag import assert_no_cycle
from taskboard.core.errors import (
    OptimisticLockError,
    TaskNotFoundError,
    UpstreamTaskNotFoundError,
)
from taskboard.core.models import (
    DependencyKind,
    TaskDependency,
    TaskItem,
    TaskSourceLink,
    TaskStatus,
    TaskView,
    utc_now_iso,
)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    source_ref TEXT DEFAULT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'backlog',
    assignee TEXT NOT NULL DEFAULT 'agent:default',
    task_type TEXT NOT NULL DEFAULT 'general',
    stage TEXT NOT NULL DEFAULT '',
    priority INTEGER NOT NULL DEFAULT 2,
    preferred_executor TEXT NOT NULL DEFAULT '',
    scope TEXT NOT NULL DEFAULT '',
    parent_task_id TEXT DEFAULT NULL,
    manual_override_fields TEXT NOT NULL DEFAULT '[]',
    blocked_reason TEXT NOT NULL DEFAULT '',
    dispatch_seq INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1,
    last_source_event_at INTEGER NOT NULL DEFAULT 0,
    ready_at TEXT DEFAULT NULL,
    artifact_ref TEXT DEFAULT NULL,
    reply_target TEXT DEFAULT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (parent_task_id) REFERENCES tasks(task_id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_source_ref
    ON tasks(source_ref)
    WHERE source_ref IS NOT NULL AND source_ref != '';

CREATE INDEX IF NOT EXISTS idx_tasks_status_priority
    ON tasks(status, priority ASC, ready_at ASC);

CREATE INDEX IF NOT EXISTS idx_tasks_assignee
    ON tasks(assignee)
    WHERE assignee != '';

CREATE INDEX IF NOT EXISTS idx_tasks_parent
    ON tasks(parent_task_id)
    WHERE parent_task_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS task_source_links (
    source_ref TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    source TEXT NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0,
    last_synced_at INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_source_links_task_id
    ON task_source_links(task_id);

CREATE TABLE IF NOT EXISTS task_dependencies (
    upstream_task_id TEXT NOT NULL,
    downstream_task_id TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'blocks',
    created_at TEXT NOT NULL,
    PRIMARY KEY (upstream_task_id, downstream_task_id, kind),
    FOREIGN KEY (upstream_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE,
    FOREIGN KEY (downstream_task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_deps_downstream
    ON task_dependencies(downstream_task_id, kind);

CREATE INDEX IF NOT EXISTS idx_deps_upstream
    ON task_dependencies(upstream_task_id, kind);
"""


def _row_to_task(row: sqlite3.Row) -> TaskItem:
    raw_overrides = json.loads(str(row["manual_override_fields"] or "[]"))
    overrides = frozenset(str(item) for item in raw_overrides)
    return TaskItem(
        task_id=str(row["task_id"]),
        source_ref=cast(str | None, row["source_ref"]),
        title=str(row["title"]),
        description=str(row["description"]),
        status=cast(TaskStatus, str(row["status"])),
        assignee=str(row["assignee"]),
        task_type=str(row["task_type"]),
        stage=str(row["stage"]),
        priority=int(row["priority"]),
        preferred_executor=str(row["preferred_executor"]),
        scope=str(row["scope"]),
        parent_task_id=cast(str | None, row["parent_task_id"]),
        manual_override_fields=overrides,
        blocked_reason=str(row["blocked_reason"]),
        dispatch_seq=int(row["dispatch_seq"]),
        version=int(row["version"]),
        last_source_event_at=int(row["last_source_event_at"]),
        ready_at=cast(str | None, row["ready_at"]),
        artifact_ref=cast(str | None, row["artifact_ref"]),
        reply_target=cast(str | None, row["reply_target"]),
        created_at=str(row["created_at"]),
        updated_at=str(row["updated_at"]),
    )


def _row_to_source_link(row: sqlite3.Row) -> TaskSourceLink:
    return TaskSourceLink(
        source_ref=str(row["source_ref"]),
        task_id=str(row["task_id"]),
        source=str(row["source"]),
        is_primary=bool(row["is_primary"]),
        last_synced_at=int(row["last_synced_at"]),
        created_at=str(row["created_at"]),
    )


class SQLiteTaskStore:
    def __init__(self, db_path: str | Path) -> None:
        self._db_path = str(db_path)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA busy_timeout=5000;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.executescript(_SCHEMA_SQL)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _insert_task_row(self, cur: sqlite3.Cursor, task: TaskItem) -> None:
        cur.execute(
            """
            INSERT INTO tasks (
                task_id, source_ref, title, description, status, assignee,
                task_type, stage, priority, preferred_executor, scope,
                parent_task_id, manual_override_fields, blocked_reason,
                dispatch_seq, version, last_source_event_at, ready_at,
                artifact_ref, reply_target, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task.task_id,
                task.source_ref,
                task.title,
                task.description,
                task.status,
                task.assignee,
                task.task_type,
                task.stage,
                task.priority,
                task.preferred_executor,
                task.scope,
                task.parent_task_id,
                json.dumps(sorted(task.manual_override_fields)),
                task.blocked_reason,
                task.dispatch_seq,
                task.version,
                task.last_source_event_at,
                task.ready_at,
                task.artifact_ref,
                task.reply_target,
                task.created_at,
                task.updated_at,
            ),
        )

    def get_task(self, task_id: str) -> TaskItem | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            return _row_to_task(row) if row is not None else None

    def find_by_source_ref(self, source_ref: str) -> TaskItem | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT t.* FROM tasks t
                JOIN task_source_links l ON t.task_id = l.task_id
                WHERE l.source_ref = ?
                """,
                (source_ref,),
            ).fetchone()
            if row is not None:
                return _row_to_task(row)
            direct = self._conn.execute(
                "SELECT * FROM tasks WHERE source_ref = ?",
                (source_ref,),
            ).fetchone()
            return _row_to_task(direct) if direct is not None else None

    def link_source(
        self,
        task_id: str,
        source_ref: str,
        source: str,
        *,
        is_primary: bool = False,
        last_synced_at: int = 0,
    ) -> TaskSourceLink:
        now = utc_now_iso()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                exists = cur.execute(
                    "SELECT 1 FROM tasks WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
                if exists is None:
                    raise TaskNotFoundError(task_id)

                if is_primary:
                    cur.execute(
                        "UPDATE task_source_links SET is_primary = 0 WHERE task_id = ?",
                        (task_id,),
                    )
                    cur.execute(
                        "UPDATE tasks SET source_ref = ?, updated_at = ? WHERE task_id = ?",
                        (source_ref, now, task_id),
                    )

                cur.execute(
                    """
                    INSERT INTO task_source_links (
                        source_ref, task_id, source, is_primary, last_synced_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_ref) DO UPDATE SET
                        task_id = excluded.task_id,
                        source = excluded.source,
                        is_primary = excluded.is_primary,
                        last_synced_at = excluded.last_synced_at
                    """,
                    (source_ref, task_id, source, 1 if is_primary else 0, last_synced_at, now),
                )
                row = cur.execute(
                    "SELECT * FROM task_source_links WHERE source_ref = ?",
                    (source_ref,),
                ).fetchone()
                cur.execute("COMMIT")
                return _row_to_source_link(row)
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def list_source_links(self, task_id: str) -> list[TaskSourceLink]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM task_source_links WHERE task_id = ? ORDER BY is_primary DESC, created_at ASC",
                (task_id,),
            ).fetchall()
            return [_row_to_source_link(r) for r in rows]

    def create_task(self, task: TaskItem, *, source: str | None = None) -> TaskItem:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                self._insert_task_row(cur, task)
                if task.source_ref:
                    cur.execute(
                        """
                        INSERT INTO task_source_links (
                            source_ref, task_id, source, is_primary, last_synced_at, created_at
                        ) VALUES (?, ?, ?, 1, ?, ?)
                        """,
                        (
                            task.source_ref,
                            task.task_id,
                            source or "external",
                            task.last_source_event_at,
                            task.created_at,
                        ),
                    )
                cur.execute("COMMIT")
                return task
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def upsert_task(self, task: TaskItem, *, expected_version: int) -> TaskItem:
        next_version = expected_version + 1
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute(
                    """
                    UPDATE tasks
                    SET source_ref = ?,
                        title = ?,
                        description = ?,
                        status = ?,
                        assignee = ?,
                        task_type = ?,
                        stage = ?,
                        priority = ?,
                        preferred_executor = ?,
                        scope = ?,
                        parent_task_id = ?,
                        manual_override_fields = ?,
                        blocked_reason = ?,
                        dispatch_seq = ?,
                        version = ?,
                        last_source_event_at = ?,
                        ready_at = ?,
                        artifact_ref = ?,
                        reply_target = ?,
                        updated_at = ?
                    WHERE task_id = ? AND version = ?
                    """,
                    (
                        task.source_ref,
                        task.title,
                        task.description,
                        task.status,
                        task.assignee,
                        task.task_type,
                        task.stage,
                        task.priority,
                        task.preferred_executor,
                        task.scope,
                        task.parent_task_id,
                        json.dumps(sorted(task.manual_override_fields)),
                        task.blocked_reason,
                        task.dispatch_seq,
                        next_version,
                        task.last_source_event_at,
                        task.ready_at,
                        task.artifact_ref,
                        task.reply_target,
                        task.updated_at,
                        task.task_id,
                        expected_version,
                    ),
                )
                if cur.rowcount == 0:
                    exists = cur.execute(
                        "SELECT 1 FROM tasks WHERE task_id = ?",
                        (task.task_id,),
                    ).fetchone()
                    cur.execute("ROLLBACK")
                    if exists is None:
                        raise TaskNotFoundError(task.task_id)
                    raise OptimisticLockError(task.task_id, expected_version)
                cur.execute("COMMIT")
                return replace(task, version=next_version)
            except Exception:
                if self._conn.in_transaction:
                    cur.execute("ROLLBACK")
                raise

    def add_dependency(
        self,
        upstream_task_id: str,
        downstream_task_id: str,
        kind: DependencyKind = "blocks",
    ) -> TaskDependency:
        now = utc_now_iso()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                if cur.execute(
                    "SELECT 1 FROM tasks WHERE task_id = ?",
                    (upstream_task_id,),
                ).fetchone() is None:
                    raise UpstreamTaskNotFoundError(upstream_task_id)

                if cur.execute(
                    "SELECT 1 FROM tasks WHERE task_id = ?",
                    (downstream_task_id,),
                ).fetchone() is None:
                    raise TaskNotFoundError(downstream_task_id)

                if kind == "blocks":
                    def _get_downstream(node_id: str) -> list[str]:
                        rows = cur.execute(
                            """
                            SELECT downstream_task_id
                            FROM task_dependencies
                            WHERE upstream_task_id = ? AND kind = 'blocks'
                            """,
                            (node_id,),
                        ).fetchall()
                        return [str(r["downstream_task_id"]) for r in rows]

                    assert_no_cycle(upstream_task_id, downstream_task_id, _get_downstream)

                cur.execute(
                    """
                    INSERT OR IGNORE INTO task_dependencies (
                        upstream_task_id, downstream_task_id, kind, created_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (upstream_task_id, downstream_task_id, kind, now),
                )
                cur.execute("COMMIT")
                return TaskDependency(
                    upstream_task_id=upstream_task_id,
                    downstream_task_id=downstream_task_id,
                    kind=kind,
                    created_at=now,
                )
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def remove_dependency(
        self,
        upstream_task_id: str,
        downstream_task_id: str,
        kind: DependencyKind = "blocks",
    ) -> bool:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute(
                    """
                    DELETE FROM task_dependencies
                    WHERE upstream_task_id = ? AND downstream_task_id = ? AND kind = ?
                    """,
                    (upstream_task_id, downstream_task_id, kind),
                )
                removed = cur.rowcount > 0
                cur.execute("COMMIT")
                return removed
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def count_incomplete_blockers(self, task_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM task_dependencies d
                JOIN tasks t ON d.upstream_task_id = t.task_id
                WHERE d.downstream_task_id = ?
                  AND d.kind = 'blocks'
                  AND t.status != 'done'
                """,
                (task_id,),
            ).fetchone()
            return int(row["cnt"]) if row is not None else 0

    def list_incomplete_blocker_ids(self, task_id: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT d.upstream_task_id
                FROM task_dependencies d
                JOIN tasks t ON d.upstream_task_id = t.task_id
                WHERE d.downstream_task_id = ?
                  AND d.kind = 'blocks'
                  AND t.status != 'done'
                ORDER BY d.upstream_task_id ASC
                """,
                (task_id,),
            ).fetchall()
            return [str(r["upstream_task_id"]) for r in rows]

    def list_unblocked_downstream(self, completed_task_id: str) -> list[TaskItem]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT DISTINCT t_down.*
                FROM task_dependencies d
                JOIN tasks t_down ON d.downstream_task_id = t_down.task_id
                WHERE d.upstream_task_id = ?
                  AND d.kind = 'blocks'
                  AND t_down.status = 'backlog'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM task_dependencies d_sub
                      JOIN tasks t_sub ON d_sub.upstream_task_id = t_sub.task_id
                      WHERE d_sub.downstream_task_id = d.downstream_task_id
                        AND d_sub.kind = 'blocks'
                        AND t_sub.status != 'done'
                  )
                ORDER BY t_down.priority ASC, t_down.created_at ASC
                """,
                (completed_task_id,),
            ).fetchall()
            return [_row_to_task(r) for r in rows]

    def list_downstream_tasks(
        self,
        upstream_task_id: str,
        *,
        kind: DependencyKind = "blocks",
    ) -> list[TaskItem]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT t.*
                FROM task_dependencies d
                JOIN tasks t ON d.downstream_task_id = t.task_id
                WHERE d.upstream_task_id = ? AND d.kind = ?
                ORDER BY t.priority ASC, t.created_at ASC
                """,
                (upstream_task_id, kind),
            ).fetchall()
            return [_row_to_task(r) for r in rows]

    def try_promote_to_ready(
        self,
        task_id: str,
        *,
        expected_version: int,
        ready_at: str,
    ) -> TaskItem | None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute(
                    """
                    UPDATE tasks
                    SET status = 'ready',
                        ready_at = ?,
                        blocked_reason = '',
                        version = version + 1,
                        updated_at = ?
                    WHERE task_id = ?
                      AND status = 'backlog'
                      AND version = ?
                    """,
                    (ready_at, ready_at, task_id, expected_version),
                )
                if cur.rowcount == 0:
                    cur.execute("COMMIT")
                    return None
                row = cur.execute(
                    "SELECT * FROM tasks WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
                cur.execute("COMMIT")
                return _row_to_task(row) if row is not None else None
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def try_claim_dispatch(
        self,
        task_id: str,
        *,
        expected_version: int,
    ) -> TaskItem | None:
        now = utc_now_iso()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute(
                    """
                    UPDATE tasks
                    SET dispatch_seq = dispatch_seq + 1,
                        version = version + 1,
                        updated_at = ?
                    WHERE task_id = ?
                      AND status = 'ready'
                      AND version = ?
                    """,
                    (now, task_id, expected_version),
                )
                if cur.rowcount == 0:
                    cur.execute("COMMIT")
                    return None
                row = cur.execute(
                    "SELECT * FROM tasks WHERE task_id = ?",
                    (task_id,),
                ).fetchone()
                cur.execute("COMMIT")
                return _row_to_task(row) if row is not None else None
            except Exception:
                cur.execute("ROLLBACK")
                raise

    def list_tasks(
        self,
        *,
        scope: str | None = None,
        status: TaskStatus | None = None,
        assignee_prefix: str | None = None,
        dispatchable_only: bool = False,
        parent_task_id: str | None = None,
    ) -> list[TaskView]:
        clauses: list[str] = []
        params: list[Any] = []

        if scope is not None:
            clauses.append("t.scope = ?")
            params.append(scope)
        if status is not None:
            clauses.append("t.status = ?")
            params.append(status)
        if assignee_prefix is not None:
            clauses.append("t.assignee LIKE ?")
            params.append(f"{assignee_prefix}%")
        if parent_task_id is not None:
            clauses.append("t.parent_task_id = ?")
            params.append(parent_task_id)
        if dispatchable_only:
            clauses.append("t.status = 'ready'")
            clauses.append(
                """
                NOT EXISTS (
                    SELECT 1 FROM task_dependencies d
                    JOIN tasks u ON d.upstream_task_id = u.task_id
                    WHERE d.downstream_task_id = t.task_id
                      AND d.kind = 'blocks'
                      AND u.status != 'done'
                )
                """
            )

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT t.*,
                   (
                       SELECT COUNT(*)
                       FROM task_dependencies d
                       JOIN tasks u ON d.upstream_task_id = u.task_id
                       WHERE d.downstream_task_id = t.task_id
                         AND d.kind = 'blocks'
                         AND u.status != 'done'
                   ) AS incomplete_blocker_count
            FROM tasks t
            {where_sql}
            ORDER BY t.priority ASC,
                     CASE WHEN t.ready_at IS NULL THEN 1 ELSE 0 END ASC,
                     t.ready_at ASC,
                     t.created_at ASC
        """
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
            return [
                TaskView(
                    task=_row_to_task(r),
                    incomplete_blocker_count=int(r["incomplete_blocker_count"]),
                )
                for r in rows
            ]

    def save_expanded_flow(
        self,
        parent_task: TaskItem,
        tasks: Sequence[TaskItem],
        edges: Sequence[TaskDependency],
    ) -> None:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                self._insert_task_row(cur, parent_task)
                for task in tasks:
                    self._insert_task_row(cur, task)
                for edge in edges:
                    cur.execute(
                        """
                        INSERT INTO task_dependencies (
                            upstream_task_id, downstream_task_id, kind, created_at
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (
                            edge.upstream_task_id,
                            edge.downstream_task_id,
                            edge.kind,
                            edge.created_at,
                        ),
                    )
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
