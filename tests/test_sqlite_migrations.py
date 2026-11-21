import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from agent_runtime.adapters.storage_sqlite import SQLiteDatabase


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
