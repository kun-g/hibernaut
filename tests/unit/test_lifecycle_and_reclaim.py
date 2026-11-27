from __future__ import annotations

import sqlite3
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.adapters.executor_cli import CommandResult, OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Suspend, new_id
from agent_runtime.core.ports import RunCapabilities
from tests.support.fakes import RecordingChannel


class _LifecycleTestExecutor:
    executor_id = "lifecycle-mock"

    def __init__(self, *, rotate_on_text: str | None = None, suspend_on_text: str | None = None) -> None:
        self._rotate_on_text = rotate_on_text
        self._suspend_on_text = suspend_on_text
        self.seen_sessions: list[SessionRef | None] = []
        self.observed_event_session_ids: list[str | None] = []
        self._inbox_ref: object | None = None

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = caps
        self.seen_sessions.append(session)
        if self._inbox_ref is not None and hasattr(self._inbox_ref, "fetch_processing"):
            proc = self._inbox_ref.fetch_processing(events[0].inbox_id)  # type: ignore[attr-defined]
            self.observed_event_session_ids.append(proc[0].session_id if proc else None)
        text = str(events[-1].payload.get("text", ""))
        if self._suspend_on_text and text == self._suspend_on_text:
            return Suspend(
                request_id="req-life-1",
                prompt="Approve deployment?",
                session_checkpoint='{"step": "wait"}',
                session_id=session.session_id if session else None,
            )
        if self._rotate_on_text and text == self._rotate_on_text:
            return Emit(messages=("rotated",), session_id=new_id("sess_rot"))
        return Emit(messages=(f"ok:{text}",), session_id=session.session_id if session else None)


class TestEventSessionLifecycleAndReclaim(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="lifecycle_v9_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def test_pre_execute_session_binding_and_orphan_processing_recovery(self) -> None:
        executor = _LifecycleTestExecutor()
        daemon = build_daemon(
            db_path=self.root / "orphan.db",
            memory_root=self.root / "mem_orphan",
            max_concurrency=1,
            executor_factory=lambda _iid: executor,
            channel=RecordingChannel(),
        )
        executor._inbox_ref = daemon.inbox
        try:
            # Simulate a crashed worker leaving an orphaned 'processing' event in SQLite.
            evt, _ = daemon.inbox.append(
                Event(
                    inbox_id="proj:hibernaut",
                    source="github_webhook",
                    external_id="orphan-1",
                    correlation_key="github:kun-g/hibernaut#301",
                    payload={"text": "recover-me"},
                )
            )
            claimed = daemon.inbox.claim_pending("proj:hibernaut")
            self.assertEqual(len(claimed), 1)
            self.assertEqual(claimed[0].status, "processing")
            self.assertEqual(daemon.inbox.diagnostics([evt.event_id])[evt.event_id].attempts, 1)

            # New worker acquires lease -> recovers orphan processing -> claims & runs -> done.
            worker = daemon.pool._worker_factory("proj:hibernaut", "worker-recovery")
            reports = await worker.process_inbox("proj:hibernaut")
            self.assertEqual(len(reports), 1)
            self.assertIsNotNone(executor.seen_sessions[0])
            assert executor.seen_sessions[0] is not None
            # Event was already bound to the pre-created active session during processing.
            self.assertEqual(executor.observed_event_session_ids[0], executor.seen_sessions[0].session_id)

            done_events = daemon.inbox.list_events(inbox_id="proj:hibernaut", status="done")
            self.assertEqual(len(done_events), 1)
            self.assertEqual(done_events[0].event_id, evt.event_id)
            self.assertEqual(daemon.inbox.diagnostics([evt.event_id])[evt.event_id].attempts, 2)
            self.assertEqual(done_events[0].session_id, executor.seen_sessions[0].session_id)
        finally:
            daemon.close()

    async def test_suspend_marks_event_done_and_different_worker_resumes(self) -> None:
        executor = _LifecycleTestExecutor(suspend_on_text="deploy")
        daemon = build_daemon(
            db_path=self.root / "susp.db",
            memory_root=self.root / "mem_susp",
            max_concurrency=1,
            executor_factory=lambda _iid: executor,
            channel=RecordingChannel(),
        )
        try:
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="im",
                external_id="s-1",
                correlation_key="github:kun-g/hibernaut#302",
                payload={"text": "deploy"},
            )
            w1 = daemon.pool._worker_factory("proj:hibernaut", "worker-A")
            r1 = await w1.process_inbox("proj:hibernaut")
            self.assertEqual(len(r1), 1)
            self.assertIsInstance(r1[0].outcome, Suspend)
            sid = r1[0].session_id
            assert sid is not None

            # Event is immediately 'done' after Suspend side effects are persisted.
            done1 = daemon.inbox.list_events(inbox_id="proj:hibernaut", status="done")
            self.assertEqual(len(done1), 1)
            sess = daemon.sessions.get(sid)
            assert sess is not None
            self.assertEqual(sess.status, "suspended")
            self.assertEqual(sess.pending_request_id, "req-life-1")

            # A completely different worker resumes the suspended session via approval event.
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="feishu:approval",
                external_id="s-2",
                payload={"approval_request_id": "req-life-1", "approved": True, "text": "approved"},
            )
            w2 = daemon.pool._worker_factory("proj:hibernaut", "worker-B")
            r2 = await w2.process_inbox("proj:hibernaut")
            self.assertEqual(len(r2), 1)
            self.assertIsInstance(r2[0].outcome, Emit)
            resumed = daemon.sessions.get(sid)
            assert resumed is not None
            self.assertEqual(resumed.status, "active")
            self.assertIsNone(resumed.pending_request_id)
        finally:
            daemon.close()

    async def test_rotation_keeps_ref_count_one_and_mcp_close_session_triggers_reclaim(
        self,
    ) -> None:
        wt_dir = self.root / "hb-issue-303"
        wt_dir.mkdir()
        work_key = "github:kun-g/hibernaut#303"
        executor = _LifecycleTestExecutor(rotate_on_text="rotate")

        daemon = build_daemon(
            db_path=self.root / "reclaim.db",
            memory_root=self.root / "mem_reclaim",
            max_concurrency=1,
            executor_factory=lambda _iid: executor,
            channel=RecordingChannel(),
        )
        # Attach deterministic mock runner to worktree manager with session_store ref check.
        wt_mgr = OrcaWorktreeManager(
            workspace_store=daemon.workspaces,
            session_store=daemon.sessions,
            backend="git",
            runner=lambda cmd, _cwd: CommandResult(0, "[]" if "pr" in cmd else "", ""),
        )
        assert daemon.mcp_server is not None
        daemon.mcp_server._worktree_manager = wt_mgr
        daemon.workspaces.reserve_creating(
            work_key=work_key,
            repo_checkout=str(self.root / "repo"),
            workdir=str(wt_dir),
            branch="hb/issue-303",
        )
        daemon.workspaces.mark_ready(work_key, workdir=str(wt_dir), branch="hb/issue-303")

        try:
            # 1. Start session s1 on work_key -> active ref count == 1; reclaim MUST be refused.
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="rot-1",
                correlation_key=work_key,
                payload={"text": "start"},
            )
            w = daemon.pool._worker_factory("proj:hibernaut", "worker-1")
            r1 = await w.process_inbox("proj:hibernaut")
            s1_id = r1[0].session_id
            assert s1_id is not None
            self.assertEqual(daemon.sessions.count_active_by_work_key(work_key), 1)

            refused = wt_mgr.reclaim(work_key)
            self.assertFalse(refused.reclaimed)
            self.assertIn(s1_id, refused.reason)

            # 2. Rotate session -> old s1 archived, new s2 active -> ref count still 1, no reclaim.
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="rot-2",
                correlation_key=work_key,
                payload={"text": "rotate"},
            )
            r2 = await w.process_inbox("proj:hibernaut")
            s2_id = r2[0].session_id
            assert s2_id is not None
            self.assertNotEqual(s2_id, s1_id)

            s1 = daemon.sessions.get(s1_id)
            s2 = daemon.sessions.get(s2_id)
            assert s1 is not None and s2 is not None
            self.assertEqual(s1.status, "archived")
            self.assertEqual(s2.status, "active")
            self.assertEqual(s2.work_key, work_key)
            self.assertEqual(daemon.sessions.count_active_by_work_key(work_key), 1)
            self.assertFalse(wt_mgr.reclaim(work_key).reclaimed)

            # 3. MCP hibernaut_close_session(s2_id) -> ref count drops to 0 -> triggers reclaim!
            close_reply = daemon.mcp_server.handle_rpc(
                {
                    "jsonrpc": "2.0",
                    "id": 99,
                    "method": "tools/call",
                    "params": {
                        "name": "hibernaut_close_session",
                        "arguments": {"session_id": s2_id},
                    },
                }
            )
            assert close_reply is not None
            result = close_reply["result"]["structuredContent"]
            self.assertEqual(result["status"], "closed")
            self.assertEqual(result["active_refs"], 0)
            self.assertTrue(result["reclaim"]["reclaimed"])
            self.assertIsNone(daemon.workspaces.get(work_key))
            # Closed session is excluded from find_by_correlation_key.
            self.assertIsNone(daemon.sessions.find_by_correlation_key("proj:hibernaut", work_key))
        finally:
            daemon.close()

    def test_sqlite_legacy_migration_adds_four_lifecycle_columns_and_upgrades_check(self) -> None:
        db_path = self.root / "legacy_v8.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript(
            """
            CREATE TABLE event_inbox (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                inbox_id TEXT NOT NULL,
                source TEXT NOT NULL,
                external_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                session_id TEXT,
                correlation_key TEXT,
                status TEXT NOT NULL CHECK (status IN ('pending', 'done', 'dropped'))
            );
            INSERT INTO event_inbox (
                event_id, inbox_id, source, external_id, occurred_at, payload_json, status
            ) VALUES ('e_old', 'u1', 'im', 'ext1', '2026-01-01T00:00:00Z', '{}', 'pending');

            CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY,
                inbox_id TEXT NOT NULL,
                correlation_key TEXT,
                executor_id TEXT,
                executor_session_id TEXT,
                checkpoint TEXT,
                pending_request_id TEXT,
                updated_at TEXT NOT NULL
            );
            INSERT INTO sessions (
                session_id, inbox_id, pending_request_id, updated_at
            ) VALUES ('s_old_susp', 'u1', 'req_old', '2026-01-01T00:00:00Z');
            """
        )
        conn.commit()
        conn.close()

        db = SQLiteDatabase(db_path)
        try:
            inbox_cols = {
                str(r["name"]) for r in db.fetchall("PRAGMA table_info(event_inbox)")
            }
            self.assertIn("attempts", inbox_cols)
            self.assertIn("last_error", inbox_cols)

            sess_cols = {
                str(r["name"]) for r in db.fetchall("PRAGMA table_info(sessions)")
            }
            self.assertIn("status", sess_cols)
            self.assertIn("work_key", sess_cols)

            # Legacy suspended session automatically migrated to status='suspended'.
            row = db.fetchone("SELECT status FROM sessions WHERE session_id = 's_old_susp'")
            assert row is not None
            self.assertEqual(row["status"], "suspended")

            # Upgraded CHECK constraint accepts 'processing' and 'failed'.
            with db.transaction() as tx:
                tx.execute("UPDATE event_inbox SET status = 'processing' WHERE event_id = 'e_old'")
                tx.execute(
                    "UPDATE event_inbox SET status = 'failed', last_error = 'err' WHERE event_id = 'e_old'"
                )
        finally:
            db.close()
