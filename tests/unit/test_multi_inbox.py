from __future__ import annotations

import asyncio
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.adapters.memory_fs import WorkspacePartitionRegistry
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Suspend
from agent_runtime.core.ports import RunCapabilities
from tests.support.fakes import RecordingChannel


class _ConcurrencyProbeExecutor:
    executor_id = "mock-probe"

    def __init__(self, delay_seconds: float = 0.08) -> None:
        self._delay = delay_seconds
        self.active_inboxes: set[str] = set()
        self.max_concurrent_inboxes = 0
        self.per_inbox_overlap_detected = False
        self.seen_baselines: dict[str, str] = {}

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = session
        inbox_id = events[0].inbox_id
        if inbox_id in self.active_inboxes:
            self.per_inbox_overlap_detected = True
        self.active_inboxes.add(inbox_id)
        self.max_concurrent_inboxes = max(self.max_concurrent_inboxes, len(self.active_inboxes))

        baseline = caps.memory.load_baseline()
        self.seen_baselines[inbox_id] = baseline["instructions.md"]
        caps.memory.write("progress/last_event.md", f"{inbox_id}:{events[-1].external_id}")

        await asyncio.sleep(self._delay)
        self.active_inboxes.discard(inbox_id)
        return Emit(messages=(f"done:{inbox_id}:{len(events)}",))


class TestPartitionedMemoryAndWorkspace(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="partition_test_")
        self.root = Path(self._tmp.name)
        self.registry = WorkspacePartitionRegistry(self.root / "memory")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_project_isolation_and_readonly_global_instructions(self) -> None:
        self.registry.set_global_instructions("GLOBAL_RULE: quiet by default")

        wt_a = self.root / "wt_a"
        wt_b = self.root / "wt_b"
        wt_a.mkdir()
        wt_b.mkdir()
        self.registry.register("proj:hibernaut", workspace_dir=wt_a)
        self.registry.register("proj:web-app", workspace_dir=wt_b)

        mem_a = self.registry.memory_for("proj:hibernaut")
        mem_b = self.registry.memory_for("proj:web-app")

        mem_a.write("instructions.md", "LOCAL_A: python strict")
        self.assertEqual(mem_a.load_baseline()["instructions.md"], "GLOBAL_RULE: quiet by default")
        (pending,) = self.registry.hub.list_proposals()
        self.registry.hub.approve_proposal(pending["proposal_id"], actor="human")
        mem_a.write("progress/task.md", "A progress")
        mem_b.write("progress/task.md", "B progress")

        self.assertIn("GLOBAL_RULE", mem_a.load_baseline()["instructions.md"])
        self.assertIn("LOCAL_A", mem_a.load_baseline()["instructions.md"])
        self.assertEqual(mem_b.load_baseline()["instructions.md"], "GLOBAL_RULE: quiet by default")
        self.assertEqual(mem_a.read_on_demand("progress/task.md"), "A progress")
        self.assertEqual(mem_b.read_on_demand("progress/task.md"), "B progress")
        self.assertEqual(self.registry.workspace_for("proj:hibernaut"), wt_a.resolve())

        with self.assertRaises(PermissionError):
            mem_a.write("global/instructions.md", "tampered")

    def test_forbids_sharing_a_directory_under_another_spelling_or_created_later(self) -> None:
        self.registry.register("sys:a", workspace_dir=self.root / "Later")
        for other in ("Later", "later"):
            with self.subTest(other), self.assertRaises(ValueError):
                self.registry.register("sys:b", workspace_dir=self.root / other)
        (self.root / "Later").mkdir()
        (self.root / "link").symlink_to(self.root / "Later")
        for other in ("Later", "link"):
            with self.subTest(other, created=True), self.assertRaises(ValueError):
                self.registry.register("sys:b", workspace_dir=self.root / other)
        # An inbox may change its own directory, which frees the one it had.
        self.registry.register("sys:a", workspace_dir=self.root / "mine")
        self.registry.register("sys:b", workspace_dir=self.root / "later2")
        self.registry.register("sys:b", workspace_dir=self.root / "Later")

    def test_forbids_two_inbox_ids_mounting_same_workspace_directory(self) -> None:
        shared_repo = self.root / "shared_repo"
        shared_repo.mkdir()
        self.registry.register("hibernaut:pr-12", workspace_dir=shared_repo)

        with self.assertRaises(ValueError):
            self.registry.register("hibernaut:issue-18", workspace_dir=shared_repo)


class TestMultiInboxConcurrentDaemon(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="multi_inbox_daemon_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def test_executor_failure_isolated_and_pending_batch_can_resume(self) -> None:
        class Probe(_ConcurrencyProbeExecutor):
            broken = True

            async def execute(self, events, session, caps):
                if self.broken and events[0].inbox_id == "broken":
                    raise FileNotFoundError("worktree")
                return Emit(messages=("ok",))

        probe = Probe()
        daemon = build_daemon(db_path=":memory:", memory_root=self.root, max_concurrency=1,
                              executor_factory=lambda _: probe)
        stop = asyncio.Event()
        task = asyncio.create_task(daemon.pool.run_forever(stop))
        try:
            with self.assertLogs("agent_runtime.app.main", level="ERROR") as log:
                for inbox in ("broken", "healthy"):
                    daemon.handler.ingest(inbox_id=inbox, source="test", external_id="1", payload={})
                await daemon.pool.wait_until_idle()
            self.assertIn("broken", log.output[0])
            self.assertEqual(daemon.inbox.fetch_pending("broken"), [])
            failed = daemon.inbox.list_events(inbox_id="broken", status="failed")
            self.assertEqual(len(failed), 1)
            diag = daemon.inbox.diagnostics([failed[0].event_id])[failed[0].event_id]
            self.assertIn("worktree", diag.last_error or "")
            self.assertEqual([r.inbox_id for r in daemon.pool.reports], ["healthy"])
        finally:
            stop.set()
            await task
            daemon.close()

    async def test_cross_inbox_parallel_and_same_inbox_serial(self) -> None:
        probe = _ConcurrencyProbeExecutor(delay_seconds=0.08)
        channel = RecordingChannel()
        daemon = build_daemon(
            db_path=self.root / "runtime.db",
            memory_root=self.root / "memory",
            max_concurrency=3,
            executor_factory=lambda _inbox_id: probe,
            channel=channel,
        )
        try:
            daemon.partitions.set_global_instructions("GLOBAL_SHARED")
            wt1 = self.root / "wt1"
            wt2 = self.root / "wt2"
            wt1.mkdir()
            wt2.mkdir()
            daemon.register_partition("proj:alpha", workspace_dir=wt1)
            daemon.register_partition("proj:beta", workspace_dir=wt2)

            stop = asyncio.Event()
            daemon_task = asyncio.create_task(
                daemon.run(
                    stop_event=stop,
                    timer_interval_seconds=0.05,
                    outbox_interval_seconds=0.02,
                )
            )

            daemon.handler.ingest(
                inbox_id="proj:alpha",
                source="webhook",
                external_id="a-1",
                payload={"text": "alpha-1"},
            )
            daemon.handler.ingest(
                inbox_id="proj:beta",
                source="webhook",
                external_id="b-1",
                payload={"text": "beta-1"},
            )
            await asyncio.sleep(0.02)
            # Second event on proj:alpha while alpha is active must not overlap.
            daemon.handler.ingest(
                inbox_id="proj:alpha",
                source="webhook",
                external_id="a-2",
                payload={"text": "alpha-2"},
            )

            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            await daemon.dispatcher.dispatch_once()
            stop.set()
            await daemon_task

            self.assertGreaterEqual(probe.max_concurrent_inboxes, 2)
            self.assertFalse(probe.per_inbox_overlap_detected)
            self.assertEqual(probe.seen_baselines["proj:alpha"], "GLOBAL_SHARED")
            self.assertEqual(probe.seen_baselines["proj:beta"], "GLOBAL_SHARED")
            self.assertEqual(
                daemon.partitions.memory_for("proj:alpha").read_on_demand("progress/last_event.md"),
                "proj:alpha:a-2",
            )
            self.assertEqual(
                daemon.partitions.memory_for("proj:beta").read_on_demand("progress/last_event.md"),
                "proj:beta:b-1",
            )
        finally:
            daemon.close()

    async def test_suspend_releases_lock_non_blocking_across_and_within_inbox(self) -> None:
        channel = RecordingChannel()
        daemon = build_daemon(
            db_path=self.root / "runtime_suspend.db",
            memory_root=self.root / "memory_suspend",
            max_concurrency=2,
            channel=channel,
        )
        try:
            stop = asyncio.Event()
            daemon_task = asyncio.create_task(
                daemon.run(
                    stop_event=stop,
                    timer_interval_seconds=0.05,
                    outbox_interval_seconds=0.02,
                )
            )

            daemon.handler.ingest(
                inbox_id="proj:alpha",
                source="im",
                external_id="email-1",
                payload={"action": "send_email", "to": "ops@example.com", "subject": "Deploy"},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            suspend_reports = [r for r in daemon.pool.reports if isinstance(r.outcome, Suspend)]
            self.assertEqual(len(suspend_reports), 1)
            assert isinstance(suspend_reports[0].outcome, Suspend)
            req_id = suspend_reports[0].outcome.request_id

            # Lease must already be released immediately after Suspend.
            self.assertTrue(daemon.lock.acquire("proj:alpha", "probe-worker", ttl_seconds=5.0))
            daemon.lock.release("proj:alpha", "probe-worker")

            # Human approval arrives as a new Event and resumes the suspended session.
            daemon.handler.ingest(
                inbox_id="proj:alpha",
                source="approval_ui",
                external_id="appr-1",
                payload={"approval_request_id": req_id, "approved": True},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            await daemon.dispatcher.dispatch_once()
            stop.set()
            await daemon_task

            emit_reports = [r for r in daemon.pool.reports if isinstance(r.outcome, Emit)]
            self.assertEqual(len(emit_reports), 1)
            assert isinstance(emit_reports[0].outcome, Emit)
            self.assertIn("ops@example.com", emit_reports[0].outcome.messages[0])
        finally:
            daemon.close()


if __name__ == "__main__":
    unittest.main()
