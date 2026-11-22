from __future__ import annotations

import sqlite3
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.adapters.executor_cli import CommandResult, OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Event, RunOutcome, SessionRef, Silent
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine.worker import AgentWorker
from tests.fakes import (
    InMemoryInbox,
    InMemoryLock,
    InMemoryMemoryStore,
    InMemoryOutbox,
    InMemoryPermission,
    InMemoryTimerStore,
    RecordingChannel,
)

INBOX = "proj:hibernaut"
WORK_KEY = "github:kun-g/hibernaut#13"


class _NamedExecutor:
    def __init__(self, executor_id: str) -> None:
        self.executor_id = executor_id
        self.calls: list[tuple[list[Event], SessionRef | None]] = []

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = caps
        self.calls.append((list(events), session))
        return Silent(executor_session_id=f"{self.executor_id}-native")


class TestParentSessionDelegation(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="parent_session_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def test_readonly_review_child_holds_worktree_until_it_closes(self) -> None:
        wt_dir = self.root / "hb-issue-13"
        wt_dir.mkdir()
        claude = _NamedExecutor("claude")
        grok = _NamedExecutor("grok")
        daemon = build_daemon(
            db_path=self.root / "parent.db",
            memory_root=self.root / "mem",
            max_concurrency=1,
            channel=RecordingChannel(),
        )
        wt_mgr = OrcaWorktreeManager(
            workspace_store=daemon.workspaces,
            session_store=daemon.sessions,
            backend="git",
            runner=lambda cmd, _cwd: CommandResult(0, "[]" if "pr" in cmd else "", ""),
        )
        reclaims: list[tuple[str, bool]] = []

        def _reclaim(work_key: str) -> None:
            reclaims.append((work_key, wt_mgr.reclaim(work_key).reclaimed))

        worker = AgentWorker(
            worker_id="w1",
            inbox=daemon.inbox,
            timers=daemon.timers,
            memory=daemon.partitions.memory_for(INBOX),
            permissions=InMemoryPermission(),
            outbox=daemon.outbox,
            lock=daemon.lock,
            sessions=daemon.sessions,
            executors={"claude": claude, "grok": grok},
            global_default_executor="claude",
            on_worktree_reclaim=_reclaim,
        )
        daemon.workspaces.reserve_creating(
            work_key=WORK_KEY,
            repo_checkout=str(self.root / "repo"),
            workdir=str(wt_dir),
            branch="hb/issue-13",
        )
        daemon.workspaces.mark_ready(WORK_KEY, workdir=str(wt_dir), branch="hb/issue-13")

        try:
            daemon.handler.ingest(
                inbox_id=INBOX,
                source="github_webhook",
                external_id="main-1",
                correlation_key=WORK_KEY,
                payload={"text": "implement", "executor_id": "claude"},
            )
            main_report = (await worker.process_inbox(INBOX))[0]
            main_sid = main_report.session_id
            assert main_sid is not None

            # Main session delegates a read-only review; no work_key given, so it is inherited.
            daemon.handler.ingest(
                inbox_id=INBOX,
                source="delegate",
                external_id="review-1",
                correlation_key=f"{WORK_KEY}:review",
                payload={
                    "text": "review the diff",
                    "parent_session_id": main_sid,
                    "executor_id": "grok",
                    "readonly": True,
                },
            )
            child_report = (await worker.process_inbox(INBOX))[0]
            child_sid = child_report.session_id
            assert child_sid is not None
            self.assertNotEqual(child_sid, main_sid)

            events, seen_session = grok.calls[0]
            self.assertTrue(events[0].payload["readonly"])
            assert seen_session is not None
            self.assertEqual(seen_session.parent_session_id, main_sid)

            main = daemon.sessions.get(main_sid)
            child = daemon.sessions.get(child_sid)
            assert main is not None and child is not None
            self.assertEqual(main.status, "active")
            self.assertEqual(child.status, "active")
            self.assertEqual(child.parent_session_id, main_sid)
            self.assertEqual(child.work_key, WORK_KEY)
            self.assertEqual(child.executor_id, "grok")
            self.assertEqual(child.executor_session_id, "grok-native")
            self.assertEqual(daemon.sessions.count_active_by_work_key(WORK_KEY), 2)

            assert daemon.mcp_server is not None
            listed = _list_sessions(daemon.mcp_server, {"parent_session_id": main_sid})
            self.assertEqual([s["session_id"] for s in listed], [child_sid])
            self.assertEqual(listed[0]["parent_session_id"], main_sid)
            everything = {s["session_id"]: s for s in _list_sessions(daemon.mcp_server, {"inbox_id": INBOX})}
            self.assertIsNone(everything[main_sid]["parent_session_id"])

            # Main closes first: the child still references the worktree, so it must survive.
            daemon.handler.ingest(
                inbox_id=INBOX,
                source="session:close",
                external_id="close-main",
                payload={"action": "close_session", "session_id": main_sid},
            )
            await worker.process_inbox(INBOX)
            closed_main = daemon.sessions.get(main_sid)
            assert closed_main is not None
            self.assertEqual(closed_main.status, "closed")
            self.assertEqual(daemon.sessions.count_active_by_work_key(WORK_KEY), 1)
            self.assertEqual(reclaims, [])
            self.assertIsNotNone(daemon.workspaces.get(WORK_KEY))
            self.assertTrue(wt_dir.exists())

            # Child closes: ref count reaches 0 and the worktree is reclaimed.
            daemon.handler.ingest(
                inbox_id=INBOX,
                source="session:close",
                external_id="close-child",
                payload={"action": "close_session", "session_id": child_sid},
            )
            await worker.process_inbox(INBOX)
            self.assertEqual(daemon.sessions.count_active_by_work_key(WORK_KEY), 0)
            self.assertEqual(reclaims, [(WORK_KEY, True)])
            self.assertIsNone(daemon.workspaces.get(WORK_KEY))
            closed_child = daemon.sessions.get(child_sid)
            assert closed_child is not None
            self.assertEqual(closed_child.parent_session_id, main_sid)
        finally:
            daemon.close()

    async def test_explicit_work_key_wins_and_parent_survives_outcome_saves(self) -> None:
        db = SQLiteDatabase(":memory:")
        try:
            sessions = SQLiteSessionStoreAdapter(db)
            sessions.save(SessionRef(session_id="parent", inbox_id=INBOX, work_key="wk-parent"))
            inbox = InMemoryInbox()
            grok = _NamedExecutor("grok")
            worker = AgentWorker(
                worker_id="w1",
                inbox=inbox,
                timers=InMemoryTimerStore(),
                memory=InMemoryMemoryStore(),
                permissions=InMemoryPermission(),
                outbox=InMemoryOutbox(),
                lock=InMemoryLock(),
                sessions=sessions,
                executor=grok,
            )
            inbox.append(
                Event(
                    inbox_id=INBOX,
                    source="delegate",
                    external_id="d1",
                    correlation_key="review-key",
                    payload={"parent_session_id": "parent", "work_key": "wk-explicit"},
                )
            )
            child_sid = (await worker.process_inbox(INBOX))[0].session_id
            assert child_sid is not None
            child = sessions.get(child_sid)
            assert child is not None
            self.assertEqual(child.work_key, "wk-explicit")
            self.assertEqual(child.parent_session_id, "parent")

            # A follow-up on the child (no parent key in payload) keeps the recorded parent.
            inbox.append(
                Event(inbox_id=INBOX, source="delegate", external_id="d2", correlation_key="review-key", payload={})
            )
            follow_sid = (await worker.process_inbox(INBOX))[0].session_id
            self.assertEqual(follow_sid, child_sid)
            again = sessions.get(child_sid)
            assert again is not None
            self.assertEqual(again.parent_session_id, "parent")
            self.assertEqual(again.work_key, "wk-explicit")
        finally:
            db.close()


class TestParentSessionMigration(unittest.TestCase):
    def test_legacy_sessions_table_gains_parent_column_and_index(self) -> None:
        with tempfile.TemporaryDirectory(prefix="parent_mig_") as tmp:
            db_path = Path(tmp) / "legacy.db"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE sessions (
                    session_id TEXT PRIMARY KEY,
                    inbox_id TEXT NOT NULL,
                    correlation_key TEXT,
                    executor_id TEXT,
                    executor_session_id TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    work_key TEXT,
                    checkpoint TEXT,
                    pending_request_id TEXT,
                    updated_at TEXT NOT NULL
                );
                INSERT INTO sessions (session_id, inbox_id, work_key, updated_at)
                VALUES ('legacy', 'u1', 'wk', '2026-01-01T00:00:00Z');
                """
            )
            conn.commit()
            conn.close()

            db = SQLiteDatabase(db_path)
            try:
                cols = {str(r["name"]) for r in db.fetchall("PRAGMA table_info(sessions)")}
                self.assertIn("parent_session_id", cols)
                indexes = {str(r["name"]) for r in db.fetchall("PRAGMA index_list(sessions)")}
                self.assertIn("idx_sessions_parent", indexes)

                store = SQLiteSessionStoreAdapter(db)
                legacy = store.get("legacy")
                assert legacy is not None
                self.assertIsNone(legacy.parent_session_id)
                store.save(SessionRef(session_id="kid", inbox_id="u1", work_key="wk", parent_session_id="legacy"))
                self.assertEqual([s.session_id for s in store.list_sessions(parent_session_id="legacy")], ["kid"])
                self.assertEqual(store.count_active_by_work_key("wk"), 2)
            finally:
                db.close()


def _list_sessions(server: object, arguments: dict[str, str]) -> list[dict[str, object]]:
    reply = server.handle_rpc(  # type: ignore[attr-defined]
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "hibernaut_list_sessions", "arguments": arguments},
        }
    )
    assert reply is not None
    return reply["result"]["structuredContent"]["sessions"]
