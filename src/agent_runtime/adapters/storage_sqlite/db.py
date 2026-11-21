from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

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
    correlation_key TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'done', 'failed', 'dropped')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
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
    correlation_key TEXT,
    executor_id TEXT,
    executor_session_id TEXT,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'archived', 'closed')),
    work_key TEXT,
    checkpoint TEXT,
    pending_request_id TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_request
    ON sessions (pending_request_id);

CREATE TABLE IF NOT EXISTS workspaces (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    work_key TEXT NOT NULL UNIQUE,
    repo_checkout TEXT NOT NULL,
    workdir TEXT NOT NULL,
    branch TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('creating', 'ready', 'reclaiming')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workspaces_repo
    ON workspaces (repo_checkout, status);

CREATE TABLE IF NOT EXISTS terminal_bindings (
    session_id TEXT PRIMARY KEY,
    executor_id TEXT NOT NULL,
    agent TEXT NOT NULL,
    workdir TEXT NOT NULL,
    handle TEXT,
    incarnation_id TEXT,
    native_session_id TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_terminal_bindings_native
    ON terminal_bindings (executor_id, native_session_id);
"""


class SQLiteDatabase:
    # One connection shared by the event loop and HTTP handler threads. Every statement runs
    # under one re-entrant lock, and writes use BEGIN IMMEDIATE so check-then-write sequences
    # (dedup, leases) stay atomic across threads and across processes sharing the file.
    def __init__(self, db_path: str | Path = ":memory:", *, busy_timeout_ms: int = 5000) -> None:
        self._db_path = str(db_path)
        if self._db_path != ":memory:":
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._tx_depth = 0
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,
            timeout=busy_timeout_ms / 1000,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)};")
        if self._db_path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA_SQL)
        with self.transaction() as conn:
            self._ensure_migrations(conn)

    @staticmethod
    def _ensure_migrations(conn: sqlite3.Connection) -> None:
        inbox_cols = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(event_inbox)").fetchall()
        }
        if "correlation_key" not in inbox_cols:
            conn.execute("ALTER TABLE event_inbox ADD COLUMN correlation_key TEXT")
        if "attempts" not in inbox_cols:
            conn.execute("ALTER TABLE event_inbox ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if "last_error" not in inbox_cols:
            conn.execute("ALTER TABLE event_inbox ADD COLUMN last_error TEXT")

        # Legacy event_inbox tables had CHECK (status IN ('pending', 'done', 'dropped'));
        # rebuild once in-transaction when 'processing' is missing from the table DDL.
        ddl_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'event_inbox'"
        ).fetchone()
        if ddl_row is not None and "'processing'" not in str(ddl_row["sql"]):
            conn.execute(
                """
                CREATE TABLE event_inbox_v9 (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    inbox_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    session_id TEXT,
                    correlation_key TEXT,
                    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'done', 'failed', 'dropped')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO event_inbox_v9 (
                    seq, event_id, inbox_id, source, external_id, occurred_at,
                    payload_json, session_id, correlation_key, status, attempts, last_error
                )
                SELECT
                    seq, event_id, inbox_id, source, external_id, occurred_at,
                    payload_json, session_id, correlation_key, status, attempts, last_error
                FROM event_inbox
                """
            )
            conn.execute("DROP TABLE event_inbox")
            conn.execute("ALTER TABLE event_inbox_v9 RENAME TO event_inbox")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_inbox_pending
                    ON event_inbox (inbox_id, status, seq)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_inbox_dedup
                    ON event_inbox (inbox_id, source, external_id, status)
                """
            )

        session_cols = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
        }
        if "correlation_key" not in session_cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN correlation_key TEXT")
        for col in ("executor_id", "executor_session_id", "work_key"):
            if col not in session_cols:
                conn.execute(f"ALTER TABLE sessions ADD COLUMN {col} TEXT")
        if "status" not in session_cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
            conn.execute(
                """
                UPDATE sessions
                SET status = 'suspended'
                WHERE pending_request_id IS NOT NULL AND status = 'active'
                """
            )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sessions_correlation
                ON sessions (inbox_id, correlation_key, updated_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sessions_work_key
                ON sessions (work_key, status)
            """
        )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            if self._tx_depth:
                self._tx_depth += 1
                try:
                    yield self._conn
                finally:
                    self._tx_depth -= 1
                return
            self._conn.execute("BEGIN IMMEDIATE")
            self._tx_depth = 1
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except BaseException:
                if self._conn.in_transaction:
                    self._conn.execute("ROLLBACK")
                raise
            finally:
                self._tx_depth = 0

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def fetchone(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
