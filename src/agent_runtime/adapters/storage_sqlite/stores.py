from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, cast

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import (
    Event,
    EventStatus,
    OutboxMessage,
    OutboxStatus,
    SessionRef,
    SessionStatus,
    TimerKind,
    TimerSpec,
    TimerStatus,
    new_id,
    to_utc_iso,
    utc_now_iso,
)


@dataclass(frozen=True, slots=True)
class EventDiagnostics:
    # How often the event was claimed (survives restarts) and the last executor failure.
    attempts: int
    last_error: str | None


_EVENT_COLUMNS = """event_id, inbox_id, source, external_id, occurred_at,
                   payload_json, session_id, correlation_key, status"""


class SQLiteInboxAdapter:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    @staticmethod
    def _row_to_event(row: Any) -> Event:
        return Event(
            event_id=str(row["event_id"]),
            inbox_id=str(row["inbox_id"]),
            source=str(row["source"]),
            external_id=str(row["external_id"]),
            occurred_at=str(row["occurred_at"]),
            payload=json.loads(str(row["payload_json"])),
            session_id=row["session_id"],
            correlation_key=row["correlation_key"],
            status=cast(EventStatus, str(row["status"])),
        )

    def append(self, event: Event) -> tuple[Event, bool]:
        with self._db.transaction() as conn:
            existing = conn.execute(
                """
                SELECT 1 FROM event_inbox
                WHERE inbox_id = ? AND source = ? AND external_id = ? AND status != 'dropped'
                LIMIT 1
                """,
                (event.inbox_id, event.source, event.external_id),
            ).fetchone()

            is_duplicate = existing is not None
            status: EventStatus = "dropped" if is_duplicate else "pending"
            stored = replace(event, status=status)
            conn.execute(
                """
                INSERT INTO event_inbox (
                    event_id, inbox_id, source, external_id, occurred_at,
                    payload_json, session_id, correlation_key, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    stored.event_id,
                    stored.inbox_id,
                    stored.source,
                    stored.external_id,
                    stored.occurred_at,
                    json.dumps(stored.payload, ensure_ascii=False),
                    stored.session_id,
                    stored.correlation_key,
                    stored.status,
                ),
            )
        return stored, is_duplicate

    def fetch_pending(self, inbox_id: str) -> list[Event]:
        rows = self._db.fetchall(
            f"""
            SELECT {_EVENT_COLUMNS}
            FROM event_inbox
            WHERE inbox_id = ? AND status = 'pending'
            ORDER BY seq ASC
            """,
            (inbox_id,),
        )
        return [self._row_to_event(row) for row in rows]

    def pending_inbox_ids(self) -> list[str]:
        rows = self._db.fetchall(
            "SELECT inbox_id FROM event_inbox WHERE status = 'pending' GROUP BY inbox_id ORDER BY MIN(seq)"
        )
        return [str(row["inbox_id"]) for row in rows]

    def fetch_processing(self, inbox_id: str) -> list[Event]:
        rows = self._db.fetchall(
            f"""
            SELECT {_EVENT_COLUMNS}
            FROM event_inbox
            WHERE inbox_id = ? AND status = 'processing'
            ORDER BY seq ASC
            """,
            (inbox_id,),
        )
        return [self._row_to_event(row) for row in rows]

    def claim_pending(self, inbox_id: str, event_ids: Collection[str] | None = None) -> list[Event]:
        wanted = set(event_ids) if event_ids is not None else None
        with self._db.transaction() as conn:
            rows = conn.execute(
                f"""
                SELECT {_EVENT_COLUMNS}
                FROM event_inbox
                WHERE inbox_id = ? AND status = 'pending'
                ORDER BY seq ASC
                """,
                (inbox_id,),
            ).fetchall()
            if wanted is not None:
                rows = [row for row in rows if str(row["event_id"]) in wanted]
            if not rows:
                return []
            ids = [str(row["event_id"]) for row in rows]
            chunk = max(1, conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
            for start in range(0, len(ids), chunk):
                part = ids[start : start + chunk]
                placeholders = ",".join("?" for _ in part)
                conn.execute(
                    f"""
                    UPDATE event_inbox
                    SET status = 'processing', attempts = attempts + 1
                    WHERE event_id IN ({placeholders})
                    """,
                    tuple(part),
                )
            return [replace(self._row_to_event(row), status="processing") for row in rows]

    def bind_session(self, event_ids: Sequence[str], session_id: str) -> None:
        if not event_ids:
            return
        ids = list(event_ids)
        with self._db.transaction() as conn:
            chunk = max(1, conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) - 1)
            for start in range(0, len(ids), chunk):
                part = ids[start : start + chunk]
                placeholders = ",".join("?" for _ in part)
                conn.execute(
                    f"UPDATE event_inbox SET session_id = ? WHERE event_id IN ({placeholders})",
                    (session_id, *part),
                )

    def requeue_processing(self, event_ids: Sequence[str]) -> None:
        if not event_ids:
            return
        ids = list(event_ids)
        with self._db.transaction() as conn:
            chunk = max(1, conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
            for start in range(0, len(ids), chunk):
                part = ids[start : start + chunk]
                placeholders = ",".join("?" for _ in part)
                conn.execute(
                    f"""
                    UPDATE event_inbox
                    SET status = 'pending'
                    WHERE event_id IN ({placeholders}) AND status = 'processing'
                    """,
                    tuple(part),
                )

    def list_events(
        self,
        *,
        inbox_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[Event]:
        clauses: list[str] = []
        params: list[Any] = []
        if inbox_id:
            clauses.append("inbox_id = ?")
            params.append(inbox_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(max(1, min(limit, 500)))
        rows = self._db.fetchall(
            f"""
            SELECT {_EVENT_COLUMNS}
            FROM event_inbox
            {where}
            ORDER BY seq DESC
            LIMIT ?
            """,
            tuple(params),
        )
        return [self._row_to_event(row) for row in rows]

    def diagnostics(self, event_ids: Sequence[str]) -> dict[str, EventDiagnostics]:
        result: dict[str, EventDiagnostics] = {}
        ids = list(event_ids)
        if not ids:
            return result
        # 500 stays under SQLite's smallest default bound-parameter limit (999).
        for start in range(0, len(ids), 500):
            part = ids[start : start + 500]
            placeholders = ",".join("?" for _ in part)
            rows = self._db.fetchall(
                f"SELECT event_id, attempts, last_error FROM event_inbox WHERE event_id IN ({placeholders})",
                tuple(part),
            )
            for row in rows:
                result[str(row["event_id"])] = EventDiagnostics(
                    attempts=int(row["attempts"]),
                    last_error=row["last_error"],
                )
        return result

    def mark_done(self, event_ids: Sequence[str]) -> None:
        if not event_ids:
            return
        ids = list(event_ids)
        with self._db.transaction() as conn:
            # Chunk under SQLite's bound-parameter limit; one transaction keeps the ack atomic.
            chunk = max(1, conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
            for start in range(0, len(ids), chunk):
                part = ids[start : start + chunk]
                placeholders = ",".join("?" for _ in part)
                conn.execute(
                    f"UPDATE event_inbox SET status = 'done' WHERE event_id IN ({placeholders})",
                    tuple(part),
                )

    def mark_failed(self, event_ids: Sequence[str], error: str) -> None:
        if not event_ids:
            return
        ids = list(event_ids)
        with self._db.transaction() as conn:
            chunk = max(1, conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) - 1)
            for start in range(0, len(ids), chunk):
                part = ids[start : start + chunk]
                placeholders = ",".join("?" for _ in part)
                conn.execute(
                    f"""
                    UPDATE event_inbox
                    SET status = 'failed', last_error = ?
                    WHERE event_id IN ({placeholders})
                    """,
                    (error, *part),
                )


class SQLiteTimerAdapter:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def schedule(
        self,
        kind: TimerKind,
        due_at: str,
        payload: dict[str, Any],
        recurrence_seconds: int | None = None,
        *,
        inbox_id: str = "default",
        session_id: str | None = None,
    ) -> str:
        timer_id = new_id("tmr")
        with self._db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO timers (
                    timer_id, inbox_id, kind, due_at,
                    payload_json, recurrence_seconds, session_id, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    timer_id,
                    inbox_id,
                    kind,
                    due_at,
                    json.dumps(payload, ensure_ascii=False),
                    recurrence_seconds,
                    session_id,
                ),
            )
        return timer_id

    def fetch_due(self, now_iso: str) -> list[TimerSpec]:
        rows = self._db.fetchall(
            """
            SELECT timer_id, inbox_id, kind, due_at,
                   payload_json, recurrence_seconds, session_id, status
            FROM timers
            WHERE status = 'pending' AND due_at <= ?
            ORDER BY due_at ASC
            """,
            (now_iso,),
        )
        return [
            TimerSpec(
                timer_id=str(row["timer_id"]),
                inbox_id=str(row["inbox_id"]),
                kind=cast(TimerKind, str(row["kind"])),
                due_at=str(row["due_at"]),
                payload=json.loads(str(row["payload_json"])),
                recurrence_seconds=row["recurrence_seconds"],
                session_id=row["session_id"],
                status=cast(TimerStatus, str(row["status"])),
            )
            for row in rows
        ]

    def mark_triggered(self, timer_id: str, next_due_at: str | None = None) -> None:
        with self._db.transaction() as conn:
            row = conn.execute(
                "SELECT kind FROM timers WHERE timer_id = ?",
                (timer_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Timer not found: {timer_id}")
            kind = str(row["kind"])
            if kind == "schedule" and next_due_at is not None:
                conn.execute(
                    "UPDATE timers SET due_at = ?, status = 'pending' WHERE timer_id = ?",
                    (next_due_at, timer_id),
                )
            else:
                conn.execute(
                    "UPDATE timers SET status = 'done' WHERE timer_id = ?",
                    (timer_id,),
                )


class SQLiteOutboxAdapter:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def enqueue(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> OutboxMessage:
        msg = OutboxMessage(
            message_id=new_id("msg"),
            inbox_id=inbox_id,
            created_at=utc_now_iso(),
            content=content,
            status="pending",
            attempts=0,
            metadata=dict(metadata or {}),
        )
        with self._db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO outbox (
                    message_id, inbox_id, created_at, content,
                    status, attempts, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    msg.message_id,
                    msg.inbox_id,
                    msg.created_at,
                    msg.content,
                    msg.status,
                    msg.attempts,
                    json.dumps(msg.metadata, ensure_ascii=False),
                ),
            )
        return msg

    def fetch_pending(self, limit: int = 20, *, now_iso: str | None = None) -> list[OutboxMessage]:
        rows = self._db.fetchall(
            """
            SELECT message_id, inbox_id, created_at, content, status, attempts, metadata_json, next_retry_at
            FROM outbox
            WHERE status = 'pending' AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY seq ASC
            LIMIT ?
            """,
            (now_iso or to_utc_iso(datetime.now(timezone.utc)), limit),
        )
        return [
            OutboxMessage(
                message_id=str(row["message_id"]),
                inbox_id=str(row["inbox_id"]),
                created_at=str(row["created_at"]),
                content=str(row["content"]),
                status=cast(OutboxStatus, str(row["status"])),
                attempts=int(row["attempts"]),
                metadata=json.loads(str(row["metadata_json"])),
                next_retry_at=None if row["next_retry_at"] is None else str(row["next_retry_at"]),
            )
            for row in rows
        ]

    def mark_sent(self, message_id: str) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE outbox SET status = 'sent', next_retry_at = NULL WHERE message_id = ?",
                (message_id,),
            )

    def schedule_retry(self, message_id: str, error: str, next_retry_at: str) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                """
                UPDATE outbox SET attempts = attempts + 1, last_error = ?, next_retry_at = ?
                WHERE message_id = ? AND status = 'pending'
                """,
                (error, next_retry_at, message_id),
            )

    def mark_failed(self, message_id: str, error: str) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                """
                UPDATE outbox SET status = 'failed', attempts = attempts + 1, last_error = ?, next_retry_at = NULL
                WHERE message_id = ? AND status = 'pending'
                """,
                (error, message_id),
            )


class SQLiteLeaseLockAdapter:
    # Lease key is inbox_id; expired leases allow automatic crash takeover.
    def __init__(
        self,
        db: SQLiteDatabase,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._db = db
        self._clock = clock

    def acquire(self, inbox_id: str, worker_id: str, ttl_seconds: float) -> bool:
        with self._db.transaction() as conn:
            now = float(self._clock())
            expires_at = now + ttl_seconds
            row = conn.execute(
                "SELECT worker_id, expires_at FROM worker_leases WHERE inbox_id = ?",
                (inbox_id,),
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO worker_leases (inbox_id, worker_id, expires_at) VALUES (?, ?, ?)",
                    (inbox_id, worker_id, expires_at),
                )
                return True
            if str(row["worker_id"]) == worker_id or float(row["expires_at"]) <= now:
                conn.execute(
                    "UPDATE worker_leases SET worker_id = ?, expires_at = ? WHERE inbox_id = ?",
                    (worker_id, expires_at, inbox_id),
                )
                return True
        return False

    def renew(self, inbox_id: str, worker_id: str, ttl_seconds: float) -> bool:
        with self._db.transaction() as conn:
            now = float(self._clock())
            expires_at = now + ttl_seconds
            cur = conn.execute(
                """
                UPDATE worker_leases
                SET expires_at = ?
                WHERE inbox_id = ? AND worker_id = ? AND expires_at > ?
                """,
                (expires_at, inbox_id, worker_id, now),
            )
            return cur.rowcount > 0

    def release(self, inbox_id: str, worker_id: str) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                "DELETE FROM worker_leases WHERE inbox_id = ? AND worker_id = ?",
                (inbox_id, worker_id),
            )


class SQLiteSessionStoreAdapter:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

    def _row_to_session(self, row: Any) -> SessionRef:
        return SessionRef(
            session_id=str(row["session_id"]),
            inbox_id=str(row["inbox_id"]),
            correlation_key=row["correlation_key"],
            executor_id=row["executor_id"],
            executor_session_id=row["executor_session_id"],
            status=cast(SessionStatus, str(row["status"])),
            work_key=row["work_key"],
            checkpoint=row["checkpoint"],
            pending_request_id=row["pending_request_id"],
            updated_at=str(row["updated_at"]),
            parent_session_id=row["parent_session_id"],
        )

    def get(self, session_id: str) -> SessionRef | None:
        row = self._db.fetchone(
            """
            SELECT session_id, inbox_id, correlation_key, executor_id, executor_session_id,
                   status, work_key, checkpoint, pending_request_id, updated_at, parent_session_id
            FROM sessions
            WHERE session_id = ?
            """,
            (session_id,),
        )
        return self._row_to_session(row) if row is not None else None

    def find_by_request_id(self, request_id: str) -> SessionRef | None:
        row = self._db.fetchone(
            """
            SELECT session_id, inbox_id, correlation_key, executor_id, executor_session_id,
                   status, work_key, checkpoint, pending_request_id, updated_at, parent_session_id
            FROM sessions
            WHERE pending_request_id = ?
            LIMIT 1
            """,
            (request_id,),
        )
        return self._row_to_session(row) if row is not None else None

    def find_by_correlation_key(self, inbox_id: str, correlation_key: str) -> SessionRef | None:
        row = self._db.fetchone(
            """
            SELECT session_id, inbox_id, correlation_key, executor_id, executor_session_id,
                   status, work_key, checkpoint, pending_request_id, updated_at, parent_session_id
            FROM sessions
            WHERE inbox_id = ? AND correlation_key = ? AND status IN ('active', 'suspended')
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (inbox_id, correlation_key),
        )
        return self._row_to_session(row) if row is not None else None

    def count_active_by_work_key(self, work_key: str, inbox_id: str | None = None) -> int:
        row = self._db.fetchone(
            """
            SELECT COUNT(*) AS cnt
            FROM sessions
            WHERE work_key = ? AND status IN ('active', 'suspended') AND (? IS NULL OR inbox_id = ?)
            """,
            (work_key, inbox_id, inbox_id),
        )
        return int(row["cnt"]) if row is not None else 0

    def list_active_by_work_key(self, work_key: str, inbox_id: str | None = None) -> list[str]:
        rows = self._db.fetchall(
            """
            SELECT session_id
            FROM sessions
            WHERE work_key = ? AND status IN ('active', 'suspended') AND (? IS NULL OR inbox_id = ?)
            ORDER BY updated_at DESC
            """,
            (work_key, inbox_id, inbox_id),
        )
        return [str(row["session_id"]) for row in rows]

    def archive_session(self, session_id: str) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                """
                UPDATE sessions
                SET status = 'archived',
                    correlation_key = NULL,
                    pending_request_id = NULL,
                    updated_at = ?
                WHERE session_id = ?
                """,
                (utc_now_iso(), session_id),
            )

    def close_session(self, session_id: str) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                """
                UPDATE sessions
                SET status = 'closed',
                    correlation_key = NULL,
                    pending_request_id = NULL,
                    checkpoint = NULL,
                    updated_at = ?
                WHERE session_id = ?
                """,
                (utc_now_iso(), session_id),
            )

    def list_sessions(
        self,
        *,
        inbox_id: str | None = None,
        status: str | None = None,
        parent_session_id: str | None = None,
    ) -> list[SessionRef]:
        clauses: list[str] = []
        params: list[Any] = []
        if inbox_id:
            clauses.append("inbox_id = ?")
            params.append(inbox_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if parent_session_id:
            clauses.append("parent_session_id = ?")
            params.append(parent_session_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._db.fetchall(
            f"""
            SELECT session_id, inbox_id, correlation_key, executor_id, executor_session_id,
                   status, work_key, checkpoint, pending_request_id, updated_at, parent_session_id
            FROM sessions
            {where}
            ORDER BY updated_at DESC
            """,
            tuple(params),
        )
        return [self._row_to_session(row) for row in rows]

    def save(self, session: SessionRef) -> None:
        with self._db.transaction() as conn:
            if session.correlation_key is not None and session.status in ("active", "suspended"):
                # Rebind correlation_key exclusively to the latest active session_id (rotation archives old).
                conn.execute(
                    """
                    UPDATE sessions
                    SET status = 'archived',
                        correlation_key = NULL,
                        pending_request_id = NULL,
                        updated_at = ?
                    WHERE inbox_id = ? AND correlation_key = ?
                      AND session_id != ? AND status IN ('active', 'suspended')
                    """,
                    (utc_now_iso(), session.inbox_id, session.correlation_key, session.session_id),
                )
            conn.execute(
                """
                INSERT INTO sessions (
                    session_id, inbox_id, correlation_key, executor_id, executor_session_id,
                    status, work_key, checkpoint, pending_request_id, updated_at, parent_session_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    inbox_id = excluded.inbox_id,
                    correlation_key = excluded.correlation_key,
                    executor_id = excluded.executor_id,
                    executor_session_id = excluded.executor_session_id,
                    status = excluded.status,
                    work_key = excluded.work_key,
                    checkpoint = excluded.checkpoint,
                    pending_request_id = excluded.pending_request_id,
                    updated_at = excluded.updated_at,
                    parent_session_id = excluded.parent_session_id
                """,
                (
                    session.session_id,
                    session.inbox_id,
                    session.correlation_key,
                    session.executor_id,
                    session.executor_session_id,
                    session.status,
                    session.work_key,
                    session.checkpoint,
                    session.pending_request_id,
                    session.updated_at,
                    session.parent_session_id,
                ),
            )
