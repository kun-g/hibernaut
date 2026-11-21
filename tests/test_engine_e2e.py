"""End-to-end tests covering Sleep-Wake-Run, Suspend/Resume approval, Timers, Lock, and Outbox."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.app.bootstrap import AgentRuntimeApp, build_runtime
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Silent, Suspend
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine import OutboxDispatcher
from agent_runtime.engine.worker import ExecutorFailed
from tests.fakes import RecordingChannel


class _OtherExecutor:
    executor_id = "other"

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = (session, caps)
        self.calls.append([evt.external_id for evt in events])
        return Silent()


class TestAlwaysOnAgentEndToEnd(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="e2e_agent_")
        root = Path(self._tmp.name)
        self.channel = RecordingChannel()
        self.app = build_runtime(
            db_path=root / "runtime.db",
            memory_dir=root / "memory",
            channel=self.channel,
        )

    def tearDown(self) -> None:
        self.app.close()
        self._tmp.cleanup()

    async def test_silent_emit_and_idempotent_dedup(self) -> None:
        r1 = self.app.handler.ingest(
            inbox_id="u1",
            source="cron",
            external_id="hb-1",
            payload={"action": "heartbeat_check"},
        )
        r1_dup = self.app.handler.ingest(
            inbox_id="u1",
            source="cron",
            external_id="hb-1",
            payload={"action": "heartbeat_check"},
        )
        self.assertFalse(r1.duplicate)
        self.assertTrue(r1_dup.duplicate)
        self.assertEqual(r1_dup.status, "dropped")

        reports = await self.app.worker.process_inbox("u1")
        self.assertEqual(len(reports), 1)
        self.assertIsInstance(reports[0].outcome, Silent)
        self.assertEqual(await self.app.dispatcher.dispatch_once(), 0)
        self.assertEqual(self.channel.sent, [])

    async def test_suspend_and_async_human_approval_resume(self) -> None:
        # 1. Trigger high-impact tool -> Suspend without blocking Worker
        self.app.handler.ingest(
            inbox_id="u1",
            source="im",
            external_id="req-email-1",
            payload={"action": "send_email", "to": "alice@example.com", "subject": "Report"},
        )
        reports1 = await self.app.worker.process_inbox("u1")
        self.assertEqual(len(reports1), 1)
        self.assertIsInstance(reports1[0].outcome, Suspend)
        suspend_outcome = reports1[0].outcome
        assert isinstance(suspend_outcome, Suspend)

        # Approval prompt delivered via Outbox
        sent_count = await self.app.dispatcher.dispatch_once()
        self.assertEqual(sent_count, 1)
        self.assertEqual(self.channel.sent[0][2]["kind"], "approval_request")
        self.assertEqual(self.channel.sent[0][2]["request_id"], suspend_outcome.request_id)

        # 2. Human approves -> arrives as new Event in Inbox -> resumes suspended Session
        self.app.handler.ingest(
            inbox_id="u1",
            source="approval_ui",
            external_id="appr-email-1",
            payload={
                "approval_request_id": suspend_outcome.request_id,
                "approved": True,
            },
        )
        reports2 = await self.app.worker.process_inbox("u1")
        self.assertEqual(len(reports2), 1)
        self.assertIsInstance(reports2[0].outcome, Emit)
        assert isinstance(reports2[0].outcome, Emit)
        self.assertIn("alice@example.com", reports2[0].outcome.messages[0])

        await self.app.dispatcher.dispatch_once()
        self.assertEqual(len(self.channel.sent), 2)

    async def test_approval_still_applies_when_same_session_note_is_queued_first(self) -> None:
        self.app.handler.ingest(
            inbox_id="u1",
            source="im",
            external_id="req-email-2",
            payload={"action": "send_email", "to": "bob@example.com", "subject": "Report"},
        )
        reports1 = await self.app.worker.process_inbox("u1")
        suspend_outcome = reports1[0].outcome
        assert isinstance(suspend_outcome, Suspend)
        session_id = reports1[0].session_id

        self.app.handler.ingest(
            inbox_id="u1",
            source="im",
            external_id="note-2",
            payload={"text": "any update?"},
            session_id=session_id,
        )
        self.app.handler.ingest(
            inbox_id="u1",
            source="approval_ui",
            external_id="appr-email-2",
            payload={"approval_request_id": suspend_outcome.request_id, "approved": True},
        )
        reports2 = await self.app.worker.process_inbox("u1")

        self.assertEqual(len(reports2), 1)
        outcome = reports2[0].outcome
        assert isinstance(outcome, Emit)
        self.assertIn("[APPROVED]", outcome.messages[0])
        self.assertIn("bob@example.com", outcome.messages[0])

    async def test_approval_applies_when_incompatible_notes_are_queued_first(self) -> None:
        self.app.handler.ingest(
            inbox_id="u1",
            source="im",
            external_id="req-email-3",
            payload={"action": "send_email", "to": "carol@example.com", "subject": "Report"},
        )
        reports1 = await self.app.worker.process_inbox("u1")
        suspend_outcome = reports1[0].outcome
        assert isinstance(suspend_outcome, Suspend)
        session_id = reports1[0].session_id

        for ext, workdir in (("note-a", "/a"), ("note-b", "/b")):
            self.app.handler.ingest(
                inbox_id="u1",
                source="im",
                external_id=ext,
                payload={"text": ext, "explicit_workdir": workdir},
                session_id=session_id,
            )
        self.app.handler.ingest(
            inbox_id="u1",
            source="approval_ui",
            external_id="appr-email-3",
            payload={"approval_request_id": suspend_outcome.request_id, "approved": True},
        )
        reports2 = await self.app.worker.process_inbox("u1")

        first = reports2[0].outcome
        assert isinstance(first, Emit)
        self.assertIn("[APPROVED]", first.messages[0])
        self.assertEqual(len(reports2), 2)

    async def _suspend_on_whitebox(self, external_id: str) -> tuple[str, str, _OtherExecutor]:
        other = _OtherExecutor()
        self.app.worker._executors["other"] = other  # pyright: ignore[reportPrivateUsage]
        self.app.handler.ingest(
            inbox_id="u1",
            source="im",
            external_id=external_id,
            payload={"action": "send_email", "to": "dave@example.com", "subject": "Report"},
        )
        reports = await self.app.worker.process_inbox("u1")
        suspend_outcome = reports[0].outcome
        assert isinstance(suspend_outcome, Suspend)
        session_id = reports[0].session_id
        assert session_id is not None
        return session_id, suspend_outcome.request_id, other

    async def test_approval_resumes_on_the_executor_holding_the_checkpoint(self) -> None:
        session_id, request_id, other = await self._suspend_on_whitebox("req-email-4")
        self.app.handler.ingest(
            inbox_id="u1",
            source="im",
            external_id="switch-note",
            payload={"text": "move on", "executor_id": "other"},
            session_id=session_id,
        )
        self.app.handler.ingest(
            inbox_id="u1",
            source="approval_ui",
            external_id="appr-email-4",
            payload={"approval_request_id": request_id, "approved": True},
        )
        reports = await self.app.worker.process_inbox("u1")

        first = reports[0].outcome
        assert isinstance(first, Emit)
        self.assertIn("[APPROVED]", first.messages[0])
        self.assertEqual(other.calls, [["switch-note"]])

    async def test_approval_routed_elsewhere_still_resumes_the_holder(self) -> None:
        _, request_id, other = await self._suspend_on_whitebox("req-email-5")
        self.app.handler.ingest(
            inbox_id="u1",
            source="approval_ui",
            external_id="appr-email-5",
            payload={"approval_request_id": request_id, "approved": True, "executor_id": "other"},
        )
        reports = await self.app.worker.process_inbox("u1")

        outcome = reports[0].outcome
        assert isinstance(outcome, Emit)
        self.assertIn("[APPROVED]", outcome.messages[0])
        self.assertEqual(other.calls, [])

    async def _legacy_suspended_app(self) -> tuple[AgentRuntimeApp, str, str, _OtherExecutor]:
        # Suspend on whitebox, rewrite sessions to the pre-binding schema and reopen so the
        # real migration backfills executor_id as NULL; then make "other" the new default.
        root = Path(self._tmp.name) / "legacy"
        app = build_runtime(db_path=root / "runtime.db", memory_dir=root / "memory", channel=RecordingChannel())
        app.handler.ingest(
            inbox_id="u1",
            source="im",
            external_id="legacy-req",
            payload={"action": "send_email", "to": "erin@example.com", "subject": "Report"},
        )
        reports = await app.worker.process_inbox("u1")
        suspend_outcome = reports[0].outcome
        assert isinstance(suspend_outcome, Suspend)
        session_id = reports[0].session_id
        assert session_id is not None
        app.close()
        with sqlite3.connect(root / "runtime.db") as raw:
            raw.execute("ALTER TABLE sessions DROP COLUMN executor_id")
            raw.execute("ALTER TABLE sessions DROP COLUMN executor_session_id")

        app = build_runtime(db_path=root / "runtime.db", memory_dir=root / "memory", channel=RecordingChannel())
        self.addCleanup(app.close)
        migrated = app.db.fetchone(
            "SELECT executor_id, pending_request_id FROM sessions WHERE session_id = ?", (session_id,)
        )
        assert migrated is not None
        self.assertEqual(tuple(migrated), (None, suspend_outcome.request_id))
        other = _OtherExecutor()
        app.worker._executors["other"] = other  # pyright: ignore[reportPrivateUsage]
        app.worker._global_default_executor = "other"  # pyright: ignore[reportPrivateUsage]
        return app, session_id, suspend_outcome.request_id, other

    async def test_legacy_suspended_session_resumes_on_the_executor_the_approval_names(self) -> None:
        app, session_id, request_id, other = await self._legacy_suspended_app()
        app.handler.ingest(
            inbox_id="u1",
            source="im",
            external_id="legacy-switch",
            payload={"text": "move on", "executor_id": "other"},
            session_id=session_id,
        )
        app.handler.ingest(
            inbox_id="u1",
            source="approval_ui",
            external_id="legacy-approve",
            payload={"approval_request_id": request_id, "approved": True, "executor_id": "whitebox"},
        )
        reports = await app.worker.process_inbox("u1")

        first = reports[0].outcome
        assert isinstance(first, Emit)
        self.assertIn("[APPROVED]", first.messages[0])
        self.assertEqual(other.calls, [["legacy-switch"]])

    async def test_legacy_suspended_session_without_known_executor_waits_for_binding(self) -> None:
        app, session_id, request_id, other = await self._legacy_suspended_app()
        app.handler.ingest(
            inbox_id="u1",
            source="approval_ui",
            external_id="legacy-approve",
            payload={"approval_request_id": request_id, "approved": True},
        )
        with self.assertRaises(ExecutorFailed):
            await app.worker.process_inbox("u1")

        self.assertEqual(other.calls, [])
        session = app.db.fetchone(
            "SELECT checkpoint, pending_request_id FROM sessions WHERE session_id = ?", (session_id,)
        )
        assert session is not None
        self.assertIsNotNone(session["checkpoint"])
        self.assertEqual(session["pending_request_id"], request_id)

        # Recovery: once the binding is filled in, the original approval completes.
        with app.db.transaction() as conn:
            conn.execute("UPDATE sessions SET executor_id = 'whitebox' WHERE session_id = ?", (session_id,))
        reports = await app.worker.process_inbox("u1")
        outcome = reports[0].outcome
        assert isinstance(outcome, Emit)
        self.assertIn("[APPROVED]", outcome.messages[0])
        self.assertEqual(other.calls, [])

    async def test_suspended_session_whose_holder_is_unregistered_stays_pending(self) -> None:
        session_id, request_id, other = await self._suspend_on_whitebox("req-email-6")
        with self.app.db.transaction() as conn:
            conn.execute("UPDATE sessions SET executor_id = 'retired' WHERE session_id = ?", (session_id,))
        self.app.handler.ingest(
            inbox_id="u1",
            source="approval_ui",
            external_id="appr-email-6",
            payload={"approval_request_id": request_id, "approved": True, "executor_id": "other"},
        )
        with self.assertRaises(ExecutorFailed):
            await self.app.worker.process_inbox("u1")

        session = self.app.db.fetchone(
            "SELECT checkpoint, pending_request_id FROM sessions WHERE session_id = ?", (session_id,)
        )
        assert session is not None
        self.assertIsNotNone(session["checkpoint"])
        self.assertEqual(session["pending_request_id"], request_id)
        self.assertEqual(other.calls, [])
        pending = self.app.db.fetchall("SELECT external_id FROM event_inbox WHERE status = 'pending'")
        self.assertEqual([r["external_id"] for r in pending], ["appr-email-6"])

    async def test_auto_mode_blocks_dangerous_tool_call(self) -> None:
        self.app.handler.ingest(
            inbox_id="u1",
            source="im",
            external_id="danger-1",
            payload={"action": "dangerous_cmd"},
        )
        reports = await self.app.worker.process_inbox("u1")
        self.assertEqual(len(reports), 1)
        self.assertIsInstance(reports[0].outcome, Emit)
        assert isinstance(reports[0].outcome, Emit)
        self.assertIn("安全策略拦截", reports[0].outcome.messages[0])

    async def test_timer_scheduler_and_memory_persistence(self) -> None:
        self.app.timers.schedule(
            kind="follow_up",
            due_at="2026-11-21T09:00:00+00:00",
            payload={"action": "remember", "path": "progress/job1.md", "content": "step 2 ok"},
            inbox_id="u1",
        )
        triggered = self.app.scheduler.tick(now_iso="2026-11-21T09:01:00+00:00")
        self.assertEqual(triggered, 1)

        reports = await self.app.worker.process_inbox("u1")
        self.assertEqual(len(reports), 1)
        self.assertEqual(self.app.memory.read_on_demand("progress/job1.md"), "step 2 ok")

    async def test_outbox_dispatcher_pacing_and_retry(self) -> None:
        flaky_channel = RecordingChannel(fail_times=1)
        now_box = [100.0]
        dispatcher = OutboxDispatcher(
            outbox=self.app.outbox,
            channel=flaky_channel,
            min_interval_seconds=10.0,
            max_attempts=3,
            clock=lambda: now_box[0],
        )
        self.app.outbox.enqueue("u1", "msg-1")
        self.app.outbox.enqueue("u1", "msg-2")

        # First attempt fails on msg-1, succeeds on msg-2
        sent1 = await dispatcher.dispatch_once()
        self.assertEqual(sent1, 1)
        self.assertEqual(len(flaky_channel.sent), 1)
        self.assertEqual(flaky_channel.sent[0][1], "msg-2")

        # Immediately retrying msg-1 is throttled by min_interval_seconds=10
        sent_throttled = await dispatcher.dispatch_once()
        self.assertEqual(sent_throttled, 0)

        # Advance clock past pacing window -> msg-1 retries and succeeds
        now_box[0] += 11.0
        sent2 = await dispatcher.dispatch_once()
        self.assertEqual(sent2, 1)
        self.assertEqual(flaky_channel.sent[1][1], "msg-1")


if __name__ == "__main__":
    unittest.main()
