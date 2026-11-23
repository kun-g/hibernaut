from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import DriveResultWriter, DriveTaskPoller, parse_task_markdown
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.core.models import FORWARD_SOURCE, Emit, Event, SessionRef, utc_now_iso
from agent_runtime.engine import EventHandler, TimerScheduler
from agent_runtime.engine.forwarding import chain_external_id
from agent_runtime.engine.worker import RunReport
from tests.fakes import InMemoryInbox, InMemoryTimerStore
from tests.test_gdrive_tasks import _FakeDriveClient
from tests.test_inbox_forwarding import SDE, ForwardCase


class DriveTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.drive = _FakeDriveClient()
        self.drive.add_file("f1", "task.md", "body", folder_id="dir-progress")
        db = SQLiteDatabase(Path(self._tmp.name) / "x.db")
        self.addCleanup(db.close)
        sessions = SQLiteSessionStoreAdapter(db)
        sessions.save(SessionRef(session_id="s1", inbox_id="proj:x", correlation_key="gdrive:f1", executor_id="steward"))
        self.writer = DriveResultWriter(client=self.drive, sessions=sessions, root_folder_id="root-1")

    def report(self, text: str, **kwargs: Any) -> RunReport:
        return RunReport(inbox_id="proj:x", session_id=kwargs.pop("session_id", "s1"), event_ids=("e1",),
                         outcome=Emit(messages=(text,)), correlation_key="gdrive:f1", **kwargs)

    def parent(self) -> str:
        return self.drive.files["f1"].parents[0]

    def test_a_task_handed_on_is_not_finished(self) -> None:
        self.assertFalse(self.writer.handle_run_report(self.report("forwarded", pending_elsewhere=True)))
        self.assertEqual((self.parent(), self.drive.contents["f1"]), ("dir-progress", "body"))
        self.assertTrue(self.writer.handle_run_report(self.report("done")))
        self.assertEqual(self.parent(), "dir-done")

    def test_a_refused_agent_fails_the_task_with_the_reason(self) -> None:
        report = self.report("", session_id=None, rejected="agent 'sde' is not allowed")
        self.assertTrue(self.writer.handle_run_report(report))
        self.assertEqual(self.parent(), "dir-failed")
        self.assertIn("agent 'sde' is not allowed", self.drive.contents["f1"])

    def test_failures_of_forwarded_work(self) -> None:
        def failed(**payload: Any) -> bool:
            return self.writer.handle_failed_event(
                Event(inbox_id="proj:x:sde", source=FORWARD_SOURCE, external_id="1",
                      correlation_key="forward:code-reviewer:gdrive:f1",
                      payload={"origin_key": "gdrive:f1", **payload}), error="boom")

        # Owed to the agent that asked for it: the task goes on there.
        self.assertFalse(failed(return_to="proj:x"))
        self.assertEqual(self.parent(), "dir-progress")
        self.assertTrue(failed())
        self.assertEqual(self.parent(), "dir-failed")

    def test_frontmatter_names_an_agent(self) -> None:
        self.assertEqual(parse_task_markdown("---\nagent_id: ops\n---\nx")[0], {"agent_id": "ops"})
        self.drive.add_file("f2", "t2.md", "---\nscope: proj:x\nagent_id: ops\n---\nx")
        inbox = InMemoryInbox()
        DriveTaskPoller(EventHandler(inbox=inbox), client=self.drive, default_inbox_id="default",
                        root_folder_id="root-1").poll_once()
        self.assertEqual([e.payload["agent_id"] for e in inbox.fetch_pending("proj:x")], ["ops"])


class TimerTests(unittest.TestCase):
    def test_a_named_timer_is_its_own_source(self) -> None:
        timers, inbox = InMemoryTimerStore(), InMemoryInbox()
        timers.schedule("schedule", utc_now_iso(), {"timer_name": "ops"}, 60, inbox_id="proj:x")
        timers.schedule("follow_up", utc_now_iso(), {"timer_name": " "}, inbox_id="proj:x")
        TimerScheduler(timers=timers, inbox=inbox).tick("2999-01-01T00:00:00+00:00")
        self.assertEqual(sorted(e.source for e in inbox.fetch_pending("proj:x")), ["timer:follow_up", "timer:ops"])


class ChildKeyTests(ForwardCase):
    async def test_a_relays_session_key_is_not_read_as_the_tasks_key(self) -> None:
        # Channels read the task id from keys such as gdrive:<id>; a suffix would name a task that does not exist.
        self.forward("sde", SDE, "review", target_agent="code-reviewer")
        self.ingest(SDE, key="gdrive:42")
        await self.run_inbox(SDE)
        reviewer = self.agents["code-reviewer"].runs[0][1]
        assert reviewer is not None
        self.assertEqual(reviewer.correlation_key, "forward:code-reviewer:gdrive:42")

    def test_external_ids_tell_replays_apart(self) -> None:
        event = Event(inbox_id="i", source="s", external_id="e", payload={}, event_id="evt")
        replayed = Event(inbox_id="i", source="s", external_id="e", payload={"_replay_id": "r"}, event_id="evt")
        self.assertEqual(chain_external_id("forward", [event], 1), "forward:evt:1")
        self.assertEqual(chain_external_id("forward", [replayed], 1), "forward:evt:r:1")


if __name__ == "__main__":
    unittest.main()
