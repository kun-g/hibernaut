"""SQLite connection and schema management (WAL mode, Inbox/Timers/Outbox/Leases/Sessions)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS event_inbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    inbox_id TEXT NOT NULL,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    session_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'done', 'dropped'))
);

CREATE INDEX IF NOT EXISTS idx_inbox_pending
    ON event_inbox (inbox_id, status, seq);

CREATE INDEX IF NOT EXISTS idx_inbox_dedup
    ON event_inbox (inbox_id, source, external_id, status);

CREATE TABLE IF NOT EXISTS timers (
    timer_id TEXT PRIMARY KEY,
    inbox_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('follow_up', 'schedule')),
    due_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    recurrence_seconds INTEGER,
    session_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'done', 'cancelled'))
);

CREATE INDEX IF NOT EXISTS idx_timers_due
    ON timers (status, due_at);

CREATE TABLE IF NOT EXISTS outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL UNIQUE,
    inbox_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    content TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    metadata_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (status, seq);

CREATE TABLE IF NOT EXISTS worker_leases (
    inbox_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    expires_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    inbox_id TEXT NOT NULL,
    checkpoint TEXT,
    pending_request_id TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_request
    ON sessions (pending_request_id);
"""


class SQLiteDatabase:
    """Thin SQLite wrapper supporting both file-backed and ':memory:' databases."""

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self._db_path = str(db_path)
        if self._db_path != ":memory:":
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        if self._db_path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA_SQL)
        self._conn.commit()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()
