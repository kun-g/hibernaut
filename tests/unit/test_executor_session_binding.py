from __future__ import annotations
import asyncio
import io
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import SessionRef, resolve_executor_id


class TestExecutorSessionBindingAndIngestOnly(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="exec_bind_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_resolve_executor_id_four_level_priority(self) -> None:
        sess_grok = SessionRef(
            session_id="s1",
            inbox_id="proj:hibernaut",
            executor_id="grok",
            executor_session_id="grok-1",
        )
        sess_unbound = SessionRef(session_id="s2", inbox_id="proj:hibernaut")

        # 1. Event explicit overrides session, project, global
        self.assertEqual(
            resolve_executor_id("claude", sess_grok, "cursor-agent", "dsh"),
            "claude",
        )
        # 2. Existing session executor_id is sticky when event omits executor_id
        self.assertEqual(
            resolve_executor_id(None, sess_grok, "cursor-agent", "claude"),
            "grok",
        )
        # 3. Project default wins when event and session have no executor_id
        self.assertEqual(
            resolve_executor_id(None, sess_unbound, "cursor-agent", "claude"),
            "cursor-agent",
        )
        # 4. Global default fallback
        self.assertEqual(
            resolve_executor_id(None, None, None, "claude"),
            "claude",
        )

    async def test_config_without_executor_starts_ingest_only_and_keeps_events_pending(
        self,
    ) -> None:
        cfg = parse_config(
            {
                "runtime": {
                    "db_path": str(self.root / "ingest_only.db"),
                    "memory_root": str(self.root / "mem_ingest"),
                    "default_executor": "claude",
                },
                "channels": {"mcp_enabled": True},
            }
        )
        err_buf = io.StringIO()
        with redirect_stderr(err_buf):
            daemon = build_daemon_from_config(cfg)

        self.assertTrue(daemon.ingest_only)
        self.assertIn("ingest-only", err_buf.getvalue())

        stop = asyncio.Event()
        daemon_task = asyncio.create_task(
            daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
        )
        try:
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="pending-evt-1",
                correlation_key="github:kun-g/hibernaut#77",
                payload={"text": "do not consume with demo"},
            )
            await asyncio.sleep(0.15)

            pending = daemon.inbox.fetch_pending("proj:hibernaut")
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0].external_id, "pending-evt-1")
            self.assertEqual(daemon.pool.reports, [])

            assert daemon.mcp_server is not None
            reply = daemon.mcp_server.handle_rpc(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "hibernaut_list_events",
                        "arguments": {"status": "pending"},
                    },
                }
            )
            assert reply is not None
            events = reply["result"]["structuredContent"]["events"]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["external_id"], "pending-evt-1")
            self.assertEqual(events[0]["status"], "pending")
        finally:
            stop.set()
            await daemon_task
            daemon.close()

    def test_config_with_explicit_demo_flag_allows_demo_executor(self) -> None:
        cfg = parse_config(
            {
                "runtime": {
                    "db_path": str(self.root / "demo_mode.db"),
                    "memory_root": str(self.root / "mem_demo"),
                }
            }
        )
        daemon = build_daemon_from_config(cfg, demo=True)
        try:
            self.assertFalse(daemon.ingest_only)
        finally:
            daemon.close()

    def test_sqlite_legacy_migration_adds_executor_columns(self) -> None:
        db_path = self.root / "legacy_exec.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript(
            """
            CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                inbox_id TEXT NOT NULL,
                correlation_key TEXT,
                checkpoint TEXT,
                pending_request_id TEXT,
                updated_at TEXT NOT NULL
            );
            INSERT INTO sessions (session_id, inbox_id, correlation_key, checkpoint, pending_request_id, updated_at)
            VALUES ('s_old', 'proj:a', 'github:a/b#1', NULL, NULL, '2026-01-01T00:00:00Z');
            """
        )
        conn.commit()
        conn.close()

        db = SQLiteDatabase(db_path)
        try:
            cols = {
                str(r["name"])
                for r in db.fetchall("PRAGMA table_info(sessions)")
            }
            self.assertIn("executor_id", cols)
            self.assertIn("executor_session_id", cols)
        finally:
            db.close()
