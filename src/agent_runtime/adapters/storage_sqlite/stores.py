from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable, Sequence
from dataclasses import replace
from typing import Any, cast

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import (
    Event,
    EventStatus,
    OutboxMessage,
    OutboxStatus,
    SessionRef,
    TimerKind,
    TimerSpec,
    TimerStatus,
    new_id,
    utc_now_iso,
)


class SQLiteInboxAdapter:
    def __init__(self, db: SQLiteDatabase) -> None:
        self._db = db

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
                    event_id, inbox_id, source, external_id,
                    occurred_at, payload_json, session_id, correlation_key, status
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
            """
            SELECT event_id, inbox_id, source, external_id,
                   occurred_at, payload_json, session_id, correlation_key, status
            FROM event_inbox
            WHERE inbox_id = ? AND status = 'pending'
            ORDER BY seq ASC
            """,
            (inbox_id,),
        )
        return [
            Event(
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
            for row in rows
        ]

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
            SELECT event_id, inbox_id, source, external_id,
                   occurred_at, payload_json, session_id, correlation_key, status
            FROM event_inbox
            {where}
            ORDER BY seq DESC
            LIMIT ?
            """,
            tuple(params),
        )
        return [
            Event(
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
            for row in rows
        ]

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

    def fetch_pending(self, limit: int = 20) -> list[OutboxMessage]:
        rows = self._db.fetchall(
            """
            SELECT message_id, inbox_id, created_at, content, status, attempts, metadata_json
            FROM outbox
            WHERE status = 'pending'
            ORDER BY seq ASC
            LIMIT ?
            """,
            (limit,),
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
            )
            for row in rows
        ]

    def mark_sent(self, message_id: str) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE outbox SET status = 'sent' WHERE message_id = ?",
                (message_id,),
            )

    def mark_failed(self, message_id: str, error: str) -> None:
        with self._db.transaction() as conn:
            conn.execute(
                "UPDATE outbox SET attempts = attempts + 1, last_error = ? WHERE message_id = ?",
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
            # Read the clock after the write lock is held; time spent waiting must not
            # be charged to the new lease.
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
            checkpoint=row["checkpoint"],
            pending_request_id=row["pending_request_id"],
            updated_at=str(row["updated_at"]),
        )

    def get(self, session_id: str) -> SessionRef | None:
        row = self._db.fetchone(
            """
            SELECT session_id, inbox_id, correlation_key, executor_id,
                   executor_session_id, checkpoint, pending_request_id, updated_at
            FROM sessions
            WHERE session_id = ?
            """,
            (session_id,),
        )
        return self._row_to_session(row) if row is not None else None

    def find_by_request_id(self, request_id: str) -> SessionRef | None:
        row = self._db.fetchone(
            """
            SELECT session_id, inbox_id, correlation_key, executor_id,
                   executor_session_id, checkpoint, pending_request_id, updated_at
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
            SELECT session_id, inbox_id, correlation_key, executor_id,
                   executor_session_id, checkpoint, pending_request_id, updated_at
            FROM sessions
            WHERE inbox_id = ? AND correlation_key = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (inbox_id, correlation_key),
        )
        return self._row_to_session(row) if row is not None else None

    def list_sessions(
        self,
        *,
        inbox_id: str | None = None,
        status: str | None = None,
    ) -> list[SessionRef]:
        clauses: list[str] = []
        params: list[Any] = []
        if inbox_id:
            clauses.append("inbox_id = ?")
            params.append(inbox_id)
        if status == "suspended":
            clauses.append("pending_request_id IS NOT NULL")
        elif status == "active":
            clauses.append("pending_request_id IS NULL")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._db.fetchall(
            f"""
            SELECT session_id, inbox_id, correlation_key, executor_id,
                   executor_session_id, checkpoint, pending_request_id, updated_at
            FROM sessions
            {where}
            ORDER BY updated_at DESC
            """,
            tuple(params),
        )
        return [self._row_to_session(row) for row in rows]

    def save(self, session: SessionRef) -> None:
        with self._db.transaction() as conn:
            if session.correlation_key is not None:
                # Rebind correlation_key exclusively to the latest active session_id (rotation).
                conn.execute(
                    """
                    UPDATE sessions
                    SET correlation_key = NULL
                    WHERE inbox_id = ? AND correlation_key = ? AND session_id != ?
                    """,
                    (session.inbox_id, session.correlation_key, session.session_id),
                )
            conn.execute(
                """
                INSERT INTO sessions (
                    session_id, inbox_id, correlation_key, executor_id,
                    executor_session_id, checkpoint, pending_request_id, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    inbox_id = excluded.inbox_id,
                    correlation_key = excluded.correlation_key,
                    executor_id = excluded.executor_id,
                    executor_session_id = excluded.executor_session_id,
                    checkpoint = excluded.checkpoint,
                    pending_request_id = excluded.pending_request_id,
                    updated_at = excluded.updated_at
                """,
                (
                    session.session_id,
                    session.inbox_id,
                    session.correlation_key,
                    session.executor_id,
                    session.executor_session_id,
                    session.checkpoint,
                    session.pending_request_id,
                    session.updated_at,
                ),
            )
