from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import DriveResultWriter, DriveTaskPoller, parse_task_markdown
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter
from agent_runtime.core.models import (
    FORWARD_SOURCE,
    RETURN_SOURCE,
    Emit,
    Event,
    ForwardSpec,
    PermissionAction,
    PermissionDecision,
    SessionRef,
    Silent,
    utc_now_iso,
)
from agent_runtime.engine import EventHandler, TimerScheduler
from agent_runtime.engine.forwarding import chain_external_id
from agent_runtime.engine.worker import ExecutorFailed, RunReport
from tests.fakes import InMemoryInbox, InMemoryPermission, InMemoryTimerStore
from tests.test_gdrive_tasks import _FakeDriveClient
from tests.test_inbox_forwarding import DOCS, MAIN, SDE, ForwardCase, Ledger


class DriveTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.drive = _FakeDriveClient()
        self.drive.add_file("f1", "task.md", "body", folder_id="dir-progress")
        db = SQLiteDatabase(Path(self._tmp.name) / "x.db")
        self.addCleanup(db.close)
        self.sessions = SQLiteSessionStoreAdapter(db)
        self.sessions.save(
            SessionRef(session_id="s1", inbox_id="proj:x", correlation_key="gdrive:f1", executor_id="steward"))
        self.writer = DriveResultWriter(client=self.drive, sessions=self.sessions, root_folder_id="root-1")

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

    def test_a_refusal_owed_to_another_agent_leaves_the_task_open(self) -> None:
        report = self.report("", session_id=None, rejected="agent 'sde' is not allowed", pending_elsewhere=True)
        self.assertFalse(self.writer.handle_run_report(report))
        self.assertEqual((self.parent(), self.drive.contents["f1"]), ("dir-progress", "body"))

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

    def test_a_failure_the_session_gave_back_is_not_written(self) -> None:
        event = Event(inbox_id="proj:x", source=RETURN_SOURCE, external_id="1", event_id="evt1", session_id="child",
                      payload={"origin_key": "gdrive:f1"})
        self.sessions.save(SessionRef(session_id="child", inbox_id="proj:x", status="closed", forward=ForwardSpec(
            target_inbox="proj:x", owed_failures=("evt1",))))
        self.assertFalse(self.writer.handle_failed_event(event, error="boom"))
        self.assertTrue(self.writer.handle_failed_event(replace(event, event_id="evt2"), error="boom"))

    def test_frontmatter_names_an_agent(self) -> None:
        self.assertEqual(parse_task_markdown("---\nagent_id: ops\n---\nx")[0], {"agent_id": "ops"})
        self.drive.add_file("f2", "t2.md", "---\nscope: proj:x\nagent_id: ops\n---\nx")
        inbox = InMemoryInbox()
        DriveTaskPoller(EventHandler(inbox=inbox), client=self.drive, default_inbox_id="default",
                        root_folder_id="root-1").poll_once()
        self.assertEqual([e.payload["agent_id"] for e in inbox.fetch_pending("proj:x")], ["ops"])


class DriveChainTests(ForwardCase):
    # A Drive task whose work is done by another agent: what reaches the task file and the conversation it came from.
    def setUp(self) -> None:
        super().setUp()
        self.ledger = Ledger()
        self.drive = _FakeDriveClient()
        self.drive.add_file("f1", "task.md", "body", folder_id="dir-progress")
        self.writer = DriveResultWriter(client=self.drive, sessions=self.sessions, root_folder_id="root-1")
        self.published: list[Any] = []
        self.switchboard = SwitchboardChannelAdapter(self.published.append)
        self.delivered: set[str] = set()

    async def step(self, inbox_id: str) -> None:
        try:
            reports = await self.run_inbox(inbox_id)
        except ExecutorFailed:
            reports = []
            for event in self.inbox.all_events():
                if event.inbox_id == inbox_id and event.status == "failed":
                    self.writer.handle_failed_event(event, error="MARK-55 from the inbox", attempts=1)
        for report in reports:
            self.writer.handle_run_report(report)
        for message in self.outbox.fetch_pending(limit=1000):
            if message.message_id not in self.delivered:
                self.delivered.add(message.message_id)
                await self.writer.send(message.inbox_id, message.content, message.metadata)
                await self.switchboard.send(message.inbox_id, message.content, message.metadata)

    def outside(self) -> str:
        return self.drive.contents["f1"] + "\n".join(str(event) for event in self.published)

    def folder(self) -> str:
        return self.drive.files["f1"].parents[0]

    async def child(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest(key="gdrive:f1", reply_channel="feishu", reply_target="oc_1")
        await self.step(MAIN)

    def gate(self, action: PermissionAction) -> None:
        self.permissions = InMemoryPermission({"inbox.forward": PermissionDecision(action, "policy")})

    async def decide(self, approved: bool, inbox_id: str = SDE) -> None:
        card = self.sent("approval_request")[-1]
        self.ingest(inbox_id, key=None, approval_request_id=card.metadata["approval_request_id"], approved=approved,
                    event_type="approval.decided")
        await self.step(inbox_id)

    async def test_a_held_result_stays_out_of_the_task_file_and_the_conversation(self) -> None:
        for action in ("defer", "block"):
            for approved in (False, True):
                with self.subTest(action=action, approved=approved):
                    self.setUp()
                    await self.child()
                    self.gate(action)
                    self.answer("sde", "MARK-55 result")
                    await self.step(SDE)
                    self.assertNotIn("MARK-55", self.outside())
                    request = self.sent("approval_request")[-1].metadata["approval_request_id"]
                    self.assertIn(request, self.drive.contents["f1"])
                    self.assertEqual(self.folder(), "dir-progress")
                    await self.decide(approved)
                    self.assertNotIn("MARK-55", self.outside())

    async def test_a_held_failure_stays_out_of_them_too(self) -> None:
        for action in ("allow", "defer", "block"):
            with self.subTest(action):
                self.setUp()
                await self.child()
                self.gate(action)
                self.crash("sde", "MARK-55 no disk")
                await self.step(SDE)
                self.assertNotIn("MARK-55", self.outside())
                self.assertEqual(self.folder(), "dir-progress")

    async def test_a_failure_while_handling_what_came_back_is_owed_too(self) -> None:
        # The events that failed are not the forward itself: a result the child waited for, then a person's message.
        for source in ("agent:return", "message"):
            with self.subTest(source):
                self.setUp()
                await self.child()
                self.forward("sde", DOCS, "document it", return_to=SDE)
                await self.step(SDE)
                if source == "message":
                    self.ingest(SDE, key="gdrive:f1", text="hurry")
                else:
                    self.answer("writer", "documented")
                    await self.step(DOCS)
                self.crash("sde", "MARK-55 no disk")
                await self.step(SDE)
                self.assertNotIn("MARK-55", self.outside())
                self.assertEqual(self.folder(), "dir-progress")
                # Back to the steward: the result the child got back used no hop.
                self.assertEqual((len(self.returned()), len(self.sent("approval_request"))), (1, 0))
                # A later failure the child answers for itself is written.
                self.ingest(SDE, key="gdrive:f1", text="again")
                await self.step(SDE)
                self.assertEqual(self.folder(), "dir-failed")

    async def test_a_withheld_result_ends_the_drive_task(self) -> None:
        await self.child()
        self.gate("block")
        self.answer("sde", "MARK-55 result")
        await self.step(SDE)
        await self.decide(False)
        self.assertEqual(self.folder(), "dir-progress")
        await self.step(MAIN)
        self.assertEqual(self.folder(), "dir-failed")
        self.assertIn("withheld", self.drive.contents["f1"])
        self.assertNotIn("MARK-55", self.outside())

    async def test_the_rest_of_a_split_task_still_finishes_it(self) -> None:
        def split(_e: Any, _s: Any, caps: Any) -> Any:
            caps.forward_to_inbox(SDE, "code", return_to=MAIN)
            caps.forward_to_inbox(DOCS, "docs", return_to=MAIN)
            return Silent()

        self.agents["steward"].script = split
        self.ingest(key="gdrive:f1")
        await self.step(MAIN)
        self.gate("block")
        self.answer("sde", "MARK-55 result")
        await self.step(SDE)
        await self.decide(False)
        await self.step(MAIN)
        self.assertEqual(self.folder(), "dir-progress")
        self.permissions = InMemoryPermission()
        self.answer("writer", "documented")
        await self.step(DOCS)
        self.answer("steward", "only the docs")
        await self.step(MAIN)
        self.assertEqual(self.folder(), "dir-done")
        self.assertIn("only the docs", self.drive.contents["f1"])
        self.assertNotIn("MARK-55", self.outside())


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
