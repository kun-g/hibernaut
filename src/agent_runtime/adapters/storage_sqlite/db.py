from __future__ import annotations

import sqlite3
import threading
from collections.abc import Generator, Sequence
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
    -- Storage-internal diagnostics (claim count, failure reason); not part of the L0 Event.
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
    metadata_json TEXT NOT NULL,
    -- NULL means due now; idx_outbox_sched is created in _ensure_migrations, after legacy tables gain the column.
    next_retry_at TEXT
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
    updated_at TEXT NOT NULL,
    parent_session_id TEXT,
    task_link TEXT,
    forward TEXT
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
    updated_at TEXT NOT NULL,
    inbox_id TEXT
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
    updated_at TEXT NOT NULL,
    peer_name TEXT,
    peer_pid INTEGER,
    peer_socket TEXT,
    peer_attempt TEXT
);

CREATE INDEX IF NOT EXISTS idx_terminal_bindings_native
    ON terminal_bindings (executor_id, native_session_id);

-- How far Hibernaut has read each ccpeer receiver log (inbox.jsonl, control.jsonl), as a byte offset.
CREATE TABLE IF NOT EXISTS peer_cursors (
    receiver TEXT NOT NULL,
    log TEXT NOT NULL,
    offset INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (receiver, log)
);

-- Records read from a receiver's logs; msg_id dedupes, consumed_at marks the ones a conversation has handled.
CREATE TABLE IF NOT EXISTS peer_messages (
    msg_id TEXT PRIMARY KEY,
    receiver TEXT NOT NULL,
    kind TEXT NOT NULL,
    from_addr TEXT,
    body TEXT NOT NULL,
    record_json TEXT NOT NULL,
    ts REAL NOT NULL,
    consumed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_peer_messages_open
    ON peer_messages (receiver, kind, consumed_at, ts);

-- Per-task facts that outlive one session and are read by other executors (keyed like workspaces).
CREATE TABLE IF NOT EXISTS task_metadata (
    work_key TEXT PRIMARY KEY,
    workdir TEXT NOT NULL,
    guidance_commit TEXT NOT NULL,
    activities_json TEXT NOT NULL DEFAULT '[]',
    tags_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Where a task's delivery stands (local, pr_open, merged), so a retry resumes from the last confirmed step.
CREATE TABLE IF NOT EXISTS deliveries (
    work_key TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    pr_url TEXT NOT NULL DEFAULT '',
    version TEXT NOT NULL DEFAULT '',
    merge_sha TEXT NOT NULL DEFAULT '',
    record_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Unfinished Runs of resumable executors, at most one per session; the executor's own state is in run_dir.
CREATE TABLE IF NOT EXISTS open_runs (
    session_id TEXT PRIMARY KEY,
    inbox_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    run_dir TEXT NOT NULL,
    event_ids_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    code_version TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_open_runs_inbox
    ON open_runs (inbox_id, started_at);

-- Reviewers that ran out of quota; no task chooses one before until (epoch seconds).
CREATE TABLE IF NOT EXISTS reviewer_quota_marks (
    executor_id TEXT PRIMARY KEY,
    until REAL NOT NULL,
    reason TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- The reviewer each task settled on: a session is one task, through every round, suspension and restart. executor_id
-- is the sde whose candidates they are.
CREATE TABLE IF NOT EXISTS reviewer_choices (
    session_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    reviewer TEXT NOT NULL,
    candidates_json TEXT NOT NULL,
    notes_json TEXT NOT NULL,
    left_reason TEXT NOT NULL,
    left_name TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (session_id, executor_id)
);

-- Sessions that may read project memory over the runtime's socket, by the token their agents hold.
CREATE TABLE IF NOT EXISTS memory_grants (
    session_id TEXT PRIMARY KEY,
    token TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

-- SDE tasks whose end is recorded in the project memory (payload attempt_notes); attempt_id is of the task's event.
CREATE TABLE IF NOT EXISTS attempt_notes (
    attempt_id TEXT PRIMARY KEY,
    inbox_id TEXT NOT NULL,
    goal TEXT NOT NULL,
    external_id TEXT NOT NULL,
    title TEXT NOT NULL,
    session_id TEXT NOT NULL,
    work_key TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    result TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    pr_url TEXT NOT NULL DEFAULT '',
    workdir TEXT NOT NULL DEFAULT '',
    seen_head TEXT NOT NULL DEFAULT '',
    dev_head TEXT NOT NULL DEFAULT '',
    written INTEGER NOT NULL DEFAULT 0,
    begun_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempt_notes_session ON attempt_notes (session_id);
CREATE INDEX IF NOT EXISTS idx_attempt_notes_work_key ON attempt_notes (work_key);
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
        bindings_existed = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'terminal_bindings'"
        ).fetchone() is not None
        self._conn.executescript(SCHEMA_SQL)
        with self.transaction() as conn:
            self._ensure_migrations(conn)
            if not bindings_existed:
                # Sessions that already ran before bindings were recorded get a marker with no terminal,
                # so the visible runner reports them instead of mistaking them for new conversations.
                conn.execute(
                    "INSERT OR IGNORE INTO terminal_bindings "
                    "(session_id, executor_id, agent, workdir, handle, incarnation_id, native_session_id, updated_at) "
                    "SELECT session_id, executor_id, '', '', NULL, NULL, NULL, updated_at FROM sessions "
                    "WHERE executor_id IS NOT NULL"
                )

    @staticmethod
    def _ensure_migrations(conn: sqlite3.Connection) -> None:
        binding_cols = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(terminal_bindings)").fetchall()
        }
        for col, kind in (("peer_name", "TEXT"), ("peer_pid", "INTEGER"), ("peer_socket", "TEXT"),
                          ("peer_attempt", "TEXT")):
            if col not in binding_cols:
                conn.execute(f"ALTER TABLE terminal_bindings ADD COLUMN {col} {kind}")
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

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_inbox_outstanding ON event_inbox (correlation_key, source) "
            "WHERE status IN ('pending', 'processing')"
        )
        outbox_cols = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(outbox)").fetchall()
        }
        if "last_error" not in outbox_cols:
            conn.execute("ALTER TABLE outbox ADD COLUMN last_error TEXT")
        if "next_retry_at" not in outbox_cols:
            # Existing rows get NULL, so whatever was pending before the upgrade is due at once.
            conn.execute("ALTER TABLE outbox ADD COLUMN next_retry_at TEXT")
            # The old dispatcher gave up at 3 attempts but left those rows pending; the new default cap is
            # higher, so without this they would be resent long after they were abandoned.
            conn.execute("UPDATE outbox SET status = 'failed' WHERE status = 'pending' AND attempts >= 3")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_outbox_sched
                ON outbox (status, next_retry_at, seq)
            """
        )

        workspace_cols = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(workspaces)").fetchall()
        }
        if "inbox_id" not in workspace_cols:
            # The inbox that owns the worktree; NULL for rows created before ownership was recorded.
            conn.execute("ALTER TABLE workspaces ADD COLUMN inbox_id TEXT")

        session_cols = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
        }
        if "correlation_key" not in session_cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN correlation_key TEXT")
        for col in ("executor_id", "executor_session_id", "work_key", "parent_session_id", "task_link", "forward"):
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
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sessions_parent
                ON sessions (parent_session_id, updated_at)
            """
        )

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
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
