from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast

from switchboard.envelope import CanonicalEvent, format_utc
from switchboard.ports import OutboundKind, OutboundMessage

DeliveryStatus = Literal["pending", "acked", "retry", "dead_letter"]
OutboundStatus = Literal["pending", "sent", "failed"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS inbound_events (
    event_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    scope TEXT NOT NULL,
    correlation_key TEXT,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE (source, external_id)
);
CREATE INDEX IF NOT EXISTS idx_inbound_corr ON inbound_events (correlation_key);

CREATE TABLE IF NOT EXISTS delivery_log (
    event_id TEXT NOT NULL REFERENCES inbound_events (event_id),
    consumer_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'acked', 'retry', 'dead_letter')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT NOT NULL,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (event_id, consumer_id)
);
CREATE INDEX IF NOT EXISTS idx_delivery_sched ON delivery_log (consumer_id, status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_delivery_order ON delivery_log (consumer_id, scope, created_at);

CREATE TABLE IF NOT EXISTS outbound_messages (
    msg_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    channel TEXT NOT NULL,
    kind TEXT NOT NULL,
    correlation_key TEXT,
    target_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT NOT NULL,
    platform_msg_id TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbound_sched ON outbound_messages (status, next_retry_at);
"""

# Head of each (consumer_id, scope) queue: the oldest row not yet acked. A head in
# retry or dead_letter blocks everything behind it in that queue only.
_DUE_HEADS_SQL = """
SELECT d.event_id, d.consumer_id, d.scope, d.status, d.attempts, d.next_retry_at,
       d.last_error, e.payload_json
FROM delivery_log AS d
JOIN inbound_events AS e ON e.event_id = d.event_id
WHERE d.status IN ('pending', 'retry')
  AND d.next_retry_at <= ?
  AND NOT EXISTS (
      SELECT 1 FROM delivery_log AS older
      WHERE older.consumer_id = d.consumer_id
        AND older.scope = d.scope
        AND older.status != 'acked'
        AND (older.created_at < d.created_at
             OR (older.created_at = d.created_at AND older.rowid < d.rowid))
  )
ORDER BY d.created_at, d.rowid
LIMIT ?
"""


@dataclass(frozen=True, slots=True)
class DeliveryRow:
    event_id: str
    consumer_id: str
    scope: str
    status: DeliveryStatus
    attempts: int
    next_retry_at: str
    last_error: str | None


@dataclass(frozen=True, slots=True)
class DueDelivery:
    row: DeliveryRow
    event: CanonicalEvent


@dataclass(frozen=True, slots=True)
class OutboundRow:
    message: OutboundMessage
    status: OutboundStatus
    attempts: int
    next_retry_at: str
    platform_msg_id: str | None
    last_error: str | None


class ReplayError(ValueError):
    pass


class SwitchboardStore:
    def __init__(self, path: str, *, busy_timeout_ms: int = 5000) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        # One connection is shared by HTTP threads and background workers, so every
        # statement (reads included) runs under this lock.
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def pragma(self, name: str) -> Any:
        with self._lock:
            return self._conn.execute(f"PRAGMA {name}").fetchone()[0]

    def insert_inbound(
        self,
        event: CanonicalEvent,
        consumers: Iterable[str],
        now: datetime,
    ) -> bool:
        stamp = format_utc(now)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._conn.execute(
                    """
                    INSERT OR IGNORE INTO inbound_events (
                        event_id, schema_version, source, external_id, event_type, scope,
                        correlation_key, occurred_at, received_at, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.event_id,
                        event.schema_version,
                        event.source,
                        event.external_id,
                        event.event_type,
                        event.scope,
                        event.correlation_key,
                        event.occurred_at,
                        stamp,
                        event.to_json(),
                    ),
                )
                inserted = cursor.rowcount == 1
                if inserted:
                    self._conn.executemany(
                        """
                        INSERT INTO delivery_log (
                            event_id, consumer_id, scope, status, attempts, next_retry_at,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, 'pending', 0, ?, ?, ?)
                        """,
                        [
                            (event.event_id, consumer, event.scope, stamp, stamp, stamp)
                            for consumer in consumers
                        ],
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        return inserted

    def find_inbound(self, source: str, external_id: str) -> CanonicalEvent | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT payload_json FROM inbound_events WHERE source = ? AND external_id = ?",
                (source, external_id),
            ).fetchone()
        return None if row is None else CanonicalEvent.from_json(str(row["payload_json"]))

    def get_inbound(self, event_id: str) -> CanonicalEvent | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT payload_json FROM inbound_events WHERE event_id = ?", (event_id,)
            ).fetchone()
        return None if row is None else CanonicalEvent.from_json(str(row["payload_json"]))

    def count_inbound(self) -> int:
        with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) FROM inbound_events").fetchone()[0])

    def due_deliveries(self, now: datetime, limit: int = 100) -> list[DueDelivery]:
        with self._lock:
            rows = self._conn.execute(_DUE_HEADS_SQL, (format_utc(now), limit)).fetchall()
        return [
            DueDelivery(row=_delivery_row(row), event=CanonicalEvent.from_json(str(row["payload_json"])))
            for row in rows
        ]

    def next_delivery_at(self) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(next_retry_at) FROM delivery_log WHERE status IN ('pending', 'retry')"
            ).fetchone()
        return None if row[0] is None else str(row[0])

    def get_delivery(self, event_id: str, consumer_id: str) -> DeliveryRow | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM delivery_log WHERE event_id = ? AND consumer_id = ?",
                (event_id, consumer_id),
            ).fetchone()
        return None if row is None else _delivery_row(row)

    def list_deliveries(self, event_id: str | None = None) -> list[DeliveryRow]:
        with self._lock:
            if event_id is None:
                rows = self._conn.execute(
                    "SELECT * FROM delivery_log ORDER BY created_at, rowid"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM delivery_log WHERE event_id = ? ORDER BY consumer_id",
                    (event_id,),
                ).fetchall()
        return [_delivery_row(row) for row in rows]

    def update_delivery(
        self,
        event_id: str,
        consumer_id: str,
        *,
        status: DeliveryStatus,
        attempts: int,
        next_retry_at: datetime,
        last_error: str | None,
        now: datetime,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE delivery_log
                SET status = ?, attempts = ?, next_retry_at = ?, last_error = ?, updated_at = ?
                WHERE event_id = ? AND consumer_id = ?
                """,
                (
                    status,
                    attempts,
                    format_utc(next_retry_at),
                    last_error,
                    format_utc(now),
                    event_id,
                    consumer_id,
                ),
            )

    # Only 'pending' rows: offline deferrals. Online failures ('retry') keep their backoff.
    def reset_offline_pending(self, consumer_id: str, now: datetime) -> int:
        stamp = format_utc(now)
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE delivery_log SET next_retry_at = ?, updated_at = ?
                WHERE consumer_id = ? AND status = 'pending' AND next_retry_at > ?
                """,
                (stamp, stamp, consumer_id, stamp),
            )
            return cursor.rowcount

    def replay_delivery(self, event_id: str, consumer_id: str, now: datetime) -> DeliveryRow:
        stamp = format_utc(now)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT status FROM delivery_log WHERE event_id = ? AND consumer_id = ?",
                    (event_id, consumer_id),
                ).fetchone()
                if row is None:
                    raise ReplayError(f"no delivery for {event_id} -> {consumer_id}")
                if row["status"] not in ("dead_letter", "retry"):
                    raise ReplayError(
                        f"only dead_letter/retry deliveries can be replayed (status={row['status']})"
                    )
                self._conn.execute(
                    """
                    UPDATE delivery_log
                    SET status = 'pending', attempts = 0, next_retry_at = ?, updated_at = ?
                    WHERE event_id = ? AND consumer_id = ?
                    """,
                    (stamp, stamp, event_id, consumer_id),
                )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        replayed = self.get_delivery(event_id, consumer_id)
        assert replayed is not None
        return replayed

    def insert_outbound(self, msg: OutboundMessage, now: datetime) -> tuple[str, bool]:
        stamp = format_utc(now)
        payload = {
            "title": msg.title,
            "body_md": msg.body_md,
            "card_payload": None if msg.card_payload is None else dict(msg.card_payload),
        }
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._conn.execute(
                    """
                    INSERT OR IGNORE INTO outbound_messages (
                        msg_id, idempotency_key, channel, kind, correlation_key, target_json,
                        payload_json, status, attempts, next_retry_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
                    """,
                    (
                        msg.msg_id,
                        msg.idempotency_key,
                        msg.channel,
                        msg.kind,
                        msg.correlation_key,
                        json.dumps(dict(msg.target), ensure_ascii=False, sort_keys=True),
                        json.dumps(payload, ensure_ascii=False, sort_keys=True),
                        stamp,
                        stamp,
                        stamp,
                    ),
                )
                inserted = cursor.rowcount == 1
                existing = self._conn.execute(
                    "SELECT msg_id FROM outbound_messages WHERE idempotency_key = ?",
                    (msg.idempotency_key,),
                ).fetchone()
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
        return str(existing["msg_id"]), inserted

    def due_outbound(self, now: datetime, limit: int = 100) -> list[OutboundRow]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM outbound_messages
                WHERE status = 'pending' AND next_retry_at <= ?
                ORDER BY created_at, rowid
                LIMIT ?
                """,
                (format_utc(now), limit),
            ).fetchall()
        return [_outbound_row(row) for row in rows]

    def next_outbound_at(self) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MIN(next_retry_at) FROM outbound_messages WHERE status = 'pending'"
            ).fetchone()
        return None if row[0] is None else str(row[0])

    def get_outbound(self, msg_id: str) -> OutboundRow | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM outbound_messages WHERE msg_id = ?", (msg_id,)
            ).fetchone()
        return None if row is None else _outbound_row(row)

    def list_outbound(self) -> list[OutboundRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM outbound_messages ORDER BY created_at, rowid"
            ).fetchall()
        return [_outbound_row(row) for row in rows]

    def update_outbound(
        self,
        msg_id: str,
        *,
        status: OutboundStatus,
        attempts: int,
        next_retry_at: datetime,
        platform_msg_id: str | None,
        last_error: str | None,
        now: datetime,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE outbound_messages
                SET status = ?, attempts = ?, next_retry_at = ?, platform_msg_id = ?,
                    last_error = ?, updated_at = ?
                WHERE msg_id = ?
                """,
                (
                    status,
                    attempts,
                    format_utc(next_retry_at),
                    platform_msg_id,
                    last_error,
                    format_utc(now),
                    msg_id,
                ),
            )

    def table_names(self) -> set[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')"
            ).fetchall()
        return {str(row["name"]) for row in rows}


def _delivery_row(row: sqlite3.Row) -> DeliveryRow:
    return DeliveryRow(
        event_id=str(row["event_id"]),
        consumer_id=str(row["consumer_id"]),
        scope=str(row["scope"]),
        status=cast(DeliveryStatus, str(row["status"])),
        attempts=int(row["attempts"]),
        next_retry_at=str(row["next_retry_at"]),
        last_error=None if row["last_error"] is None else str(row["last_error"]),
    )


def _outbound_row(row: sqlite3.Row) -> OutboundRow:
    payload = cast(Mapping[str, Any], json.loads(str(row["payload_json"])))
    card = payload.get("card_payload")
    return OutboundRow(
        message=OutboundMessage(
            msg_id=str(row["msg_id"]),
            idempotency_key=str(row["idempotency_key"]),
            channel=str(row["channel"]),
            kind=cast(OutboundKind, str(row["kind"])),
            target=cast(Mapping[str, Any], json.loads(str(row["target_json"]))),
            correlation_key=None if row["correlation_key"] is None else str(row["correlation_key"]),
            title=str(payload.get("title") or ""),
            body_md=str(payload.get("body_md") or ""),
            card_payload=None if card is None else cast(Mapping[str, Any], card),
        ),
        status=cast(OutboundStatus, str(row["status"])),
        attempts=int(row["attempts"]),
        next_retry_at=str(row["next_retry_at"]),
        platform_msg_id=None if row["platform_msg_id"] is None else str(row["platform_msg_id"]),
        last_error=None if row["last_error"] is None else str(row["last_error"]),
    )
