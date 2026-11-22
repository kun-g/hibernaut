import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteOutboxAdapter


class TestSQLiteMigrations(unittest.TestCase):
    def test_reopen_legacy_database_preserves_events_and_checkpoints(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy.db"
            with closing(sqlite3.connect(path)) as conn:
                conn.executescript("""
                    CREATE TABLE event_inbox (
                        seq INTEGER PRIMARY KEY, event_id TEXT, inbox_id TEXT, source TEXT,
                        external_id TEXT, occurred_at TEXT, payload_json TEXT, session_id TEXT,
                        status TEXT
                    );
                    CREATE TABLE sessions (
                        session_id TEXT PRIMARY KEY, inbox_id TEXT, checkpoint TEXT,
                        pending_request_id TEXT, updated_at TEXT
                    );
                    INSERT INTO event_inbox VALUES
                        (1, 'e1', 'i1', 'test', 'ext1', 'now', '{}', 's1', 'pending');
                    INSERT INTO sessions VALUES ('s1', 'i1', 'saved', 'r1', 'now');
                """)
            for _ in range(2):
                with closing(SQLiteDatabase(path)) as db:
                    event = db.fetchone("SELECT status, correlation_key FROM event_inbox WHERE event_id='e1'")
                    self.assertEqual(tuple(event), ('pending', None))
                    session = db.fetchone("SELECT checkpoint, pending_request_id, correlation_key FROM sessions WHERE session_id='s1'")
                    self.assertEqual(tuple(session), ('saved', 'r1', None))
                    index = db.fetchall("PRAGMA index_info(idx_sessions_correlation)")
                    self.assertEqual([col['name'] for col in index], ['inbox_id', 'correlation_key', 'updated_at'])


class TestOutboxBackoffMigration(unittest.TestCase):
    def test_legacy_outbox_gains_next_retry_at_and_old_rows_stay_due(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy_outbox.db"
            with closing(sqlite3.connect(path)) as conn:
                conn.executescript("""
                    CREATE TABLE outbox (
                        seq INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT NOT NULL UNIQUE,
                        inbox_id TEXT NOT NULL, created_at TEXT NOT NULL, content TEXT NOT NULL,
                        status TEXT NOT NULL CHECK (status IN ('pending', 'sent', 'failed')),
                        attempts INTEGER NOT NULL DEFAULT 0, metadata_json TEXT NOT NULL
                    );
                    INSERT INTO outbox (message_id, inbox_id, created_at, content, status, attempts, metadata_json)
                    VALUES ('m1', 'i1', '2026-11-01T00:00:00+00:00', 'old reply', 'pending', 1, '{}');
                    -- The old dispatcher gave up after 3 attempts but left the row pending.
                    INSERT INTO outbox (message_id, inbox_id, created_at, content, status, attempts, metadata_json)
                    VALUES ('m_given_up', 'i1', '2026-10-01T00:00:00+00:00', 'stale reply', 'pending', 3, '{}');
                """)
            for _ in range(2):
                with closing(SQLiteDatabase(path)) as db:
                    cols = {row["name"] for row in db.fetchall("PRAGMA table_info(outbox)")}
                    self.assertTrue({"next_retry_at", "last_error"} <= cols, cols)
                    index = db.fetchall("PRAGMA index_info(idx_outbox_sched)")
                    self.assertEqual([col["name"] for col in index], ["status", "next_retry_at", "seq"])
                    (msg,) = SQLiteOutboxAdapter(db).fetch_pending()
                    self.assertEqual((msg.message_id, msg.attempts, msg.next_retry_at), ("m1", 1, None))
                    given_up = db.fetchone("SELECT status, attempts FROM outbox WHERE message_id = 'm_given_up'")
                    self.assertEqual(tuple(given_up), ("failed", 3))
