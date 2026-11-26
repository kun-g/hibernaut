from __future__ import annotations

import asyncio
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.adapters.channels import DriveFileMeta
from agent_runtime.app.main import AlwaysOnDaemon, build_daemon
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine.worker import AgentWorker
from tests.support.fakes import (
    InMemoryInbox,
    InMemoryLock,
    InMemoryMemoryStore,
    InMemoryOutbox,
    InMemoryPermission,
    InMemorySessionStore,
    InMemoryTimerStore,
    RecordingChannel,
)
from tests.support.gdrive_tasks import _DriveTestExecutor, _FakeDriveClient
from tests.support.gdrive_move_on_start import DONE, FAILED, PENDING, PROGRESS


WORKER_LOGGER = "agent_runtime.engine.worker"
# The Drive listener logs its own failed start move, so the Run still gets the task's attachments.
DRIVE_LOGGER = "agent_runtime.adapters.channels.gdrive_tasks"


class _FlakyDriveClient(_FakeDriveClient):
    def __init__(self) -> None:
        super().__init__()
        # folder -> how many more moves into it are refused
        self.refuse_moves_into: dict[str, int] = {}

    def move_file(self, file_id: str, *, add_parent: str, remove_parent: str) -> None:
        if self.refuse_moves_into.get(add_parent, 0) > 0:
            self.refuse_moves_into[add_parent] -= 1
            raise RuntimeError(f"Drive queue proxy refused 'move' into {add_parent}")
        super().move_file(file_id, add_parent=add_parent, remove_parent=remove_parent)


class _ParentsAtStartExecutor(_DriveTestExecutor):
    # Records where each batch's file sits when the executor starts, and the whole batch.
    def __init__(self, drive: _FakeDriveClient) -> None:
        super().__init__()
        self.drive = drive
        self.parents_at_start: list[tuple[str, ...]] = []
        self.batches: list[list[Event]] = []
        self.release = asyncio.Event()
        self.release.set()

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        # An approval resuming a suspended task carries no Drive payload; its session holds gdrive:<file_id>.
        raw_id = events[-1].payload.get("drive_file_id")
        if raw_id is None and session is not None and session.correlation_key:
            raw_id = session.correlation_key.removeprefix("gdrive:")
        file_id = str(raw_id)
        self.parents_at_start.append(self.drive.files[file_id].parents)
        self.batches.append(list(events))
        await self.release.wait()
        return await super().execute(events, session, caps)


def _event_rows(daemon: AlwaysOnDaemon) -> list[tuple[str, str]]:
    rows = daemon.db.fetchall("SELECT external_id, status FROM event_inbox ORDER BY seq")
    return [(str(r["external_id"]), str(r["status"])) for r in rows]


class TestDriveMovesOnRunStart(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="gdrive_start_")))
        self.drive = _FlakyDriveClient()
        self.executor = _ParentsAtStartExecutor(self.drive)
        self.daemon = build_daemon(
            db_path=self.root / "gd.db",
            memory_root=self.root / "mem",
            max_concurrency=1,
            executor_factory=lambda _iid: self.executor,
            channel=RecordingChannel(),
            gdrive_client=self.drive,
            gdrive_root_folder_id="root-1",
            gdrive_default_inbox_id="proj:drive",
        )
        self.addCleanup(self.daemon.close)
        assert self.daemon.gdrive_poller is not None
        self.poller = self.daemon.gdrive_poller

    async def run_pool_until_idle(self) -> None:
        stop = asyncio.Event()
        pool = asyncio.create_task(self.daemon.pool.run_forever(stop))
        try:
            await asyncio.sleep(0)
            await self.daemon.pool.wait_until_idle(timeout_seconds=5.0)
        finally:
            stop.set()
            await pool
        await self.daemon.dispatcher.dispatch_once()

    async def test_ingest_keeps_the_file_queued_and_the_run_start_moves_it(self) -> None:
        self.drive.add_file("f1", "task.md", "Build it.")
        (result,) = self.poller.poll_once()
        self.assertFalse(result.duplicate)
        self.assertEqual(self.drive.files["f1"].parents, (PENDING,))
        self.assertEqual(self.drive.move_history, [])

        await self.run_pool_until_idle()
        self.assertEqual(self.executor.parents_at_start, [(PROGRESS,)])
        self.assertEqual(self.drive.move_history, [("f1", PENDING, PROGRESS), ("f1", PROGRESS, DONE)])

    async def test_repeated_polls_skip_an_open_event_without_dropped_rows(self) -> None:
        self.drive.add_file("f1", "task.md", "Build it.")
        self.assertEqual(len(self.poller.poll_once()), 1)
        # Pending: no worker has picked it up yet.
        self.assertEqual(self.poller.poll_once(), [])
        self.assertEqual(self.poller.poll_once(), [])
        self.assertEqual(_event_rows(self.daemon), [("gdrive:f1:2026-11-22T03:00:00Z", "pending")])

        # Processing: claimed, but the start move has not happened (or failed); still skipped.
        self.daemon.inbox.claim_pending("proj:drive")
        self.assertEqual(self.poller.poll_once(), [])
        self.assertEqual(_event_rows(self.daemon), [("gdrive:f1:2026-11-22T03:00:00Z", "processing")])
        self.assertEqual(self.drive.files["f1"].parents, (PENDING,))
        self.assertEqual(self.drive.move_history, [])

    async def test_a_finished_revision_moved_back_is_dropped_once_not_every_poll(self) -> None:
        self.drive.add_file("f1", "task.md", "Build it.")
        self.poller.poll_once()
        await self.run_pool_until_idle()
        meta = self.drive.files["f1"]
        self.drive.files["f1"] = DriveFileMeta(meta.file_id, meta.name, meta.mime_type, meta.modified_time, (PENDING,))

        with self.assertLogs("agent_runtime.adapters.channels.gdrive_tasks", level="WARNING"):
            (dup,) = self.poller.poll_once()
        self.assertTrue(dup.duplicate)
        self.assertEqual(self.poller.poll_once(), [])
        self.assertEqual(self.poller.poll_once(), [])
        statuses = [status for _, status in _event_rows(self.daemon)]
        self.assertEqual(statuses, ["done", "dropped"])

    async def test_a_draining_daemon_leaves_the_file_queued(self) -> None:
        stop = asyncio.Event()
        pool = asyncio.create_task(self.daemon.pool.run_forever(stop))
        await asyncio.sleep(0)
        self.daemon.pool.drain()
        try:
            self.drive.add_file("f1", "task.md", "Build it.")
            self.poller.poll_once()
            await asyncio.sleep(0.2)
            self.assertEqual(self.executor.batches, [])
            self.assertEqual(self.drive.files["f1"].parents, (PENDING,))
            self.assertEqual(self.drive.move_history, [])
            self.assertEqual([s for _, s in _event_rows(self.daemon)], ["pending"])
        finally:
            stop.set()
            await pool

    async def test_an_edit_while_queued_runs_both_revisions_in_one_batch(self) -> None:
        self.drive.add_file("f1", "task.md", "First draft.", modified_time="2026-11-22T03:00:00Z")
        self.poller.poll_once()
        self.drive.add_file("f1", "task.md", "Second draft.", modified_time="2026-11-22T03:05:00Z")
        (edited,) = self.poller.poll_once()
        self.assertFalse(edited.duplicate)
        self.assertEqual(self.poller.poll_once(), [])

        await self.run_pool_until_idle()
        (batch,) = self.executor.batches
        self.assertEqual([evt.payload["body"] for evt in batch], ["First draft.", "Second draft."])
        self.assertEqual(self.drive.move_history, [("f1", PENDING, PROGRESS), ("f1", PROGRESS, DONE)])
        self.assertEqual([s for _, s in _event_rows(self.daemon)], ["done", "done"])

    async def test_a_failed_start_move_does_not_fail_the_run_and_results_still_land(self) -> None:
        self.drive.refuse_moves_into[PROGRESS] = 2
        # Separate passes: a failing batch discards the RunReports of earlier batches in the same pass.
        self.drive.add_file("ok", "ok.md", "Build it.", modified_time="2026-11-22T03:00:00Z")
        self.poller.poll_once()
        with self.assertLogs(DRIVE_LOGGER, level="WARNING") as logs:
            await self.run_pool_until_idle()
        self.drive.add_file("bad", "bad.md", "FORCE_FAIL here", modified_time="2026-11-22T03:01:00Z")
        self.poller.poll_once()
        with self.assertLogs(DRIVE_LOGGER, level="WARNING") as more_logs:
            with self.assertLogs("agent_runtime.app.main", level="ERROR"):
                await self.run_pool_until_idle()
        self.assertTrue(any("refused 'move'" in line for line in more_logs.output), more_logs.output)
        self.assertTrue(any("refused 'move'" in line for line in logs.output), logs.output)
        self.assertEqual(self.executor.parents_at_start[0], (PENDING,))
        # Emit and failure both move the file straight out of 待执行/.
        self.assertEqual(self.drive.files["ok"].parents, (DONE,))
        self.assertIn("Completed task", self.drive.contents["ok"])
        self.assertEqual(self.drive.files["bad"].parents, (FAILED,))
        self.assertIn("simulated build failure", self.drive.contents["bad"])

    async def test_a_suspend_whose_start_move_failed_still_parks_the_file_in_progress(self) -> None:
        self.drive.add_file("s1", "approve.md", "NEED_APPROVAL first")
        self.poller.poll_once()
        self.drive.refuse_moves_into[PROGRESS] = 1
        with self.assertLogs(DRIVE_LOGGER, level="WARNING"):
            await self.run_pool_until_idle()
        self.assertEqual(self.executor.parents_at_start, [(PENDING,)])
        self.assertEqual(self.drive.files["s1"].parents, (PROGRESS,))
        self.assertIn("等待审批", self.drive.contents["s1"])
        # Parked in 执行中/, so the next poll does not pick the suspended task up again.
        self.assertEqual(self.poller.poll_once(), [])

    async def test_resuming_after_approval_does_not_move_the_file_again(self) -> None:
        self.drive.add_file("s1", "approve.md", "NEED_APPROVAL first")
        self.poller.poll_once()
        await self.run_pool_until_idle()
        self.assertEqual(self.drive.files["s1"].parents, (PROGRESS,))
        self.assertEqual(self.drive.move_history, [("s1", PENDING, PROGRESS)])

        # The approval carries no correlation_key; the resumed batch takes gdrive:s1 from the suspended session.
        self.daemon.handler.ingest(
            inbox_id="proj:drive",
            source="events:approval",
            external_id="approval:req-gd-1:approve",
            payload={"approval_request_id": "req-gd-1", "approved": True},
        )
        with self.assertNoLogs(WORKER_LOGGER, level="WARNING"):
            await self.run_pool_until_idle()
        self.assertEqual(len(self.executor.batches), 2)
        self.assertEqual(self.executor.parents_at_start[1], (PROGRESS,))
        self.assertEqual(self.drive.move_history, [("s1", PENDING, PROGRESS), ("s1", PROGRESS, DONE)])

    async def test_a_repeated_run_start_for_the_same_file_moves_it_once(self) -> None:
        # An interrupted batch is requeued and started again; the file is already in 执行中/ by then.
        writer = self.daemon.gdrive_result_writer
        assert writer is not None
        self.drive.add_file("f1", "task.md", "x")
        evt = Event(inbox_id="proj:drive", source="gdrive:task", external_id="gdrive:f1:t", payload={},
                    correlation_key="gdrive:f1")
        writer.on_run_started("proj:drive", [evt], "gdrive:f1")
        writer.on_run_started("proj:drive", [evt], "gdrive:f1")
        self.assertEqual(self.drive.move_history, [("f1", PENDING, PROGRESS)])
        self.assertEqual(self.drive.files["f1"].parents, (PROGRESS,))

    async def test_the_start_move_ignores_a_file_no_longer_queued(self) -> None:
        writer = self.daemon.gdrive_result_writer
        assert writer is not None
        self.drive.add_file("moved", "m.md", "x", folder_id=PROGRESS)
        self.drive.add_file("done", "d.md", "x", folder_id=DONE)
        for file_id in ("moved", "done"):
            evt = Event(inbox_id="proj:drive", source="gdrive:task", external_id=file_id, payload={},
                        correlation_key=f"gdrive:{file_id}")
            writer.on_run_started("proj:drive", [evt], f"gdrive:{file_id}")
        writer.on_run_started("proj:drive", [], "github:kun-g/hibernaut#1")
        writer.on_run_started("proj:drive", [], None)
        self.assertEqual(self.drive.move_history, [])


class _Listener:
    def __init__(self, *, fail: bool = False, bodies: dict[str, str] | None = None) -> None:
        self.calls: list[tuple[str, list[str], str | None]] = []
        self.fail = fail
        # external_id -> body the listener hands the executor instead of the stored one
        self.bodies = bodies or {}

    def on_run_started(
        self, inbox_id: str, events: Sequence[Event], correlation_key: str | None
    ) -> dict[str, dict[str, object]] | None:
        self.calls.append((inbox_id, [e.external_id for e in events], correlation_key))
        if self.fail:
            raise RuntimeError("listener down")
        return {e.event_id: {**e.payload, "body": self.bodies[e.external_id]}
                for e in events if e.external_id in self.bodies} or None


class _SilentExecutor:
    executor_id = "silent"

    def __init__(self, listener: _Listener) -> None:
        self.listener = listener
        self.calls_seen_at_start: list[int] = []
        self.payloads: list[list[dict[str, object]]] = []

    async def execute(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
        self.calls_seen_at_start.append(len(self.listener.calls))
        self.payloads.append([dict(e.payload) for e in events])
        return Emit(messages=("ok",))


class TestWorkerRunStartListener(unittest.IsolatedAsyncioTestCase):
    def build(self, listener: _Listener, hold: bool = False) -> tuple[AgentWorker, InMemoryInbox, _SilentExecutor]:
        inbox = InMemoryInbox()
        executor = _SilentExecutor(listener)
        worker = AgentWorker(
            worker_id="w1",
            inbox=inbox,
            timers=InMemoryTimerStore(),
            memory=InMemoryMemoryStore(),
            permissions=InMemoryPermission(),
            outbox=InMemoryOutbox(),
            executor=executor,
            lock=InMemoryLock(),
            sessions=InMemorySessionStore(),
            run_start_listener=listener,
            hold=(lambda _evt: "no project") if hold else None,
        )
        return worker, inbox, executor

    async def test_the_listener_hears_each_batch_before_the_executor_starts(self) -> None:
        listener = _Listener()
        worker, inbox, executor = self.build(listener)
        inbox.append(Event(inbox_id="i", source="t", external_id="a1", payload={}, correlation_key="k:a"))
        inbox.append(Event(inbox_id="i", source="t", external_id="a2", payload={}, correlation_key="k:a"))
        inbox.append(Event(inbox_id="i", source="t", external_id="b1", payload={}, correlation_key="k:b"))
        inbox.append(Event(inbox_id="i", source="t", external_id="close", payload={"action": "close_session"}))
        await worker.process_inbox("i")
        self.assertEqual(listener.calls, [("i", ["a1", "a2"], "k:a"), ("i", ["b1"], "k:b")])
        self.assertEqual(executor.calls_seen_at_start, [1, 2])

    async def test_replacement_payloads_reach_the_executor_for_this_run_only(self) -> None:
        listener = _Listener(bodies={"a2": "task\n\n## 附件\n- `b.zip`"})
        worker, inbox, executor = self.build(listener)
        inbox.append(Event(inbox_id="i", source="t", external_id="a1", payload={"body": "old"}, correlation_key="k:a"))
        inbox.append(Event(inbox_id="i", source="t", external_id="a2", payload={"body": "task", "n": 2},
                           correlation_key="k:a"))
        await worker.process_inbox("i")
        self.assertEqual(executor.payloads, [[{"body": "old"}, {"body": "task\n\n## 附件\n- `b.zip`", "n": 2}]])
        stored = {e.external_id: e.payload for e in inbox._events}  # pyright: ignore[reportPrivateUsage]
        self.assertEqual(stored["a2"], {"body": "task", "n": 2})

    async def test_a_failing_listener_is_logged_and_the_run_completes(self) -> None:
        listener = _Listener(fail=True)
        worker, inbox, executor = self.build(listener)
        inbox.append(Event(inbox_id="i", source="t", external_id="a1", payload={}, correlation_key="k:a"))
        with self.assertLogs(WORKER_LOGGER, level="WARNING") as logs:
            reports = await worker.process_inbox("i")
        self.assertIn("listener down", "\n".join(logs.output))
        self.assertEqual(executor.calls_seen_at_start, [1])
        self.assertIsInstance(reports[0].outcome, Emit)
        self.assertEqual(inbox.fetch_pending("i"), [])
        self.assertFalse(inbox.has_open_event("i", "t", "a1"))

    async def test_held_events_never_reach_the_listener(self) -> None:
        listener = _Listener()
        worker, inbox, executor = self.build(listener, hold=True)
        inbox.append(Event(inbox_id="i", source="t", external_id="a1", payload={}, correlation_key="k:a"))
        await worker.process_inbox("i")
        self.assertEqual(listener.calls, [])
        self.assertEqual(executor.calls_seen_at_start, [])
        self.assertTrue(inbox.has_open_event("i", "t", "a1"))
