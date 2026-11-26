import os
import unittest

from agent_runtime.adapters.channels.gdrive_tasks import DriveResultWriter
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
    SQLiteWorkUsage,
)
from agent_runtime.core.models import Emit, Event
from agent_runtime.engine.worker import AgentWorker
from tests.support.fakes import InMemoryMemoryStore, InMemoryPermission
from tests.support.gdrive_tasks import _FakeDriveClient
from tests.support.merge_cleanup import _FinishingExecutor


class TestFinishedTaskClosesItsSession(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.inbox = SQLiteInboxAdapter(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.usage = SQLiteWorkUsage(self.db)
        self.reclaims: list[tuple[str, int, list[str]]] = []

    def worker(self, executor, reclaim=None):
        def record(work_key):
            # What the reclaim gates would see at this moment.
            self.reclaims.append((work_key, self.sessions.count_active_by_work_key(work_key),
                                  self.usage.blockers(work_key)))
            return reclaim(work_key) if reclaim is not None else None

        return AgentWorker(worker_id="w", inbox=self.inbox, timers=SQLiteTimerAdapter(self.db),
                           memory=InMemoryMemoryStore(), permissions=InMemoryPermission(), outbox=self.outbox,
                           lock=SQLiteLeaseLockAdapter(self.db), sessions=self.sessions, executor=executor,
                           on_worktree_reclaim=record)

    def ingest(self, key="task:1"):
        self.inbox.append(Event(inbox_id="proj:x", source="test", external_id=f"e-{os.urandom(4).hex()}",
                                payload={"text": "go"}, correlation_key=key))

    async def test_a_finished_task_closes_its_session_and_asks_for_reclaim_after_the_result_is_written(self):
        self.ingest()
        reports = await self.worker(_FinishingExecutor(close=True)).process_inbox("proj:x")
        self.assertEqual(len(reports), 1)
        session = self.sessions.get(reports[0].session_id or "")
        assert session is not None
        self.assertEqual(session.status, "closed")
        self.assertEqual(self.reclaims, [("task:1", 0, [])])
        self.assertEqual([m.content for m in self.outbox.fetch_pending()], ["[sde] delivered"])
        self.assertEqual(reports[0].correlation_key, "task:1")

    async def test_a_reclaim_failure_does_not_fail_the_run(self):
        self.ingest()

        def boom(_work_key):
            raise RuntimeError("orca is down")

        with self.assertLogs("agent_runtime.engine.worker", level="ERROR"):
            reports = await self.worker(_FinishingExecutor(close=True), reclaim=boom).process_inbox("proj:x")
        self.assertEqual(len(reports), 1)
        self.assertIsInstance(reports[0].outcome, Emit)
        self.assertEqual([e.status for e in self.inbox.list_events(inbox_id="proj:x")], ["done"])
        self.assertEqual([m.content for m in self.outbox.fetch_pending()], ["[sde] delivered"])

    async def test_an_unfinished_task_keeps_its_session(self):
        self.ingest()
        reports = await self.worker(_FinishingExecutor(close=False)).process_inbox("proj:x")
        session = self.sessions.get(reports[0].session_id or "")
        assert session is not None
        self.assertEqual(session.status, "active")
        self.assertEqual(self.reclaims, [])

    async def test_a_drive_task_result_is_written_after_its_session_closed(self):
        drive = _FakeDriveClient()
        drive.add_file("file-1", "task.md", "Implement X.", folder_id="dir-progress")
        writer = DriveResultWriter(client=drive, sessions=self.sessions, root_folder_id="root-1")
        self.ingest(key="gdrive:file-1")
        reports = await self.worker(_FinishingExecutor(close=True)).process_inbox("proj:x")
        self.assertTrue(writer.handle_run_report(reports[0]))
        self.assertIn("[sde] delivered", drive.contents["file-1"])
        self.assertEqual(drive.files["file-1"].parents, ("dir-done",))


if __name__ == "__main__":
    unittest.main()
