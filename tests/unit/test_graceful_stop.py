import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit
from tests.support.graceful_stop import BLOCKED, OTHER, free_port, post


class _GatedExecutor:
    executor_id = "gated"

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.started: list[str] = []

    async def execute(self, events, session, caps):
        text = events[-1].payload["text"]
        self.started.append(text)
        if text.startswith("block"):
            await self.release.wait()
        return Emit(messages=(f"{text} done",))


class TestDaemonRunDrains(unittest.IsolatedAsyncioTestCase):
    # AlwaysOnDaemon.run, used without --config, drains the same way.
    policy = "defer"

    async def asyncSetUp(self):
        self.executor = _GatedExecutor()
        self.daemon = build_daemon(db_path=":memory:", executor_factory=lambda _inbox: self.executor,
                                   new_event_policy=self.policy)
        self.addCleanup(self.daemon.close)
        self.port = free_port()
        self.workdir = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.stop = asyncio.Event()
        self.force = asyncio.Event()
        self.drained = asyncio.Event()
        drain = self.daemon.pool.drain

        def observe_drain():
            drain()
            self.drained.set()

        self.daemon.pool.drain = observe_drain
        self.serving = asyncio.create_task(self.daemon.run(
            stop_event=self.stop, force_stop_event=self.force, webhook_host="127.0.0.1", webhook_port=self.port,
            timer_interval_seconds=0.05, outbox_interval_seconds=0.02,
        ))

    async def asyncTearDown(self):
        self.executor.release.set()
        self.stop.set()
        await asyncio.wait_for(self.serving, timeout=10)

    async def post(self, inbox_id: str, text: str):
        return await asyncio.to_thread(post, self.port, inbox_id, text, self.workdir)

    async def wait_for(self, check, timeout: float = 10.0):
        deadline = time.monotonic() + timeout
        while not check():
            self.assertLess(time.monotonic(), deadline)
            await asyncio.sleep(0.02)

    def status(self, event_id: str) -> str:
        return self.daemon.db.fetchall("SELECT status FROM event_inbox WHERE event_id = ?", (event_id,))[0]["status"]

    def outbox(self, inbox_id: str) -> list[tuple[str, str]]:
        rows = self.daemon.db.fetchall("SELECT content, status FROM outbox WHERE inbox_id = ? ORDER BY seq", (inbox_id,))
        return [(row["content"], row["status"]) for row in rows]

    async def start_blocked(self) -> str:
        while (await self.post(BLOCKED, "block-1"))[0] != 202:
            await asyncio.sleep(0.02)
        await self.wait_for(lambda: "block-1" in self.executor.started)
        return self.daemon.inbox.list_events(inbox_id=BLOCKED)[0].event_id

    async def test_stop_waits_for_the_run_and_keeps_ingress_and_delivery(self):
        blocked_id = await self.start_blocked()
        self.stop.set()
        await asyncio.wait_for(self.drained.wait(), 10)
        status, other = await self.post(OTHER, "other-1")
        self.assertEqual(status, 202)
        self.daemon.outbox.enqueue(OTHER, "notice")
        await self.wait_for(lambda: self.outbox(OTHER) == [("notice", "sent")])
        self.assertFalse(self.serving.done())
        self.assertEqual(self.status(other["event_id"]), "pending")
        self.assertNotIn("other-1", self.executor.started)

        self.executor.release.set()
        await asyncio.wait_for(self.serving, timeout=10)
        self.assertEqual(self.status(blocked_id), "done")
        self.assertEqual(self.status(other["event_id"]), "pending")
        self.assertEqual((await self.post(OTHER, "after"))[0], None)

    async def test_force_stop_cancels_the_run_and_leaves_it_pending(self):
        blocked_id = await self.start_blocked()
        self.stop.set()
        await asyncio.wait_for(self.drained.wait(), 10)
        self.assertFalse(self.serving.done())
        self.force.set()
        await asyncio.wait_for(self.serving, timeout=5)
        self.assertEqual(self.status(blocked_id), "pending")
        self.assertEqual(self.outbox(BLOCKED), [])


class TestDrainUnderInterruptPolicy(TestDaemonRunDrains):
    policy = "interrupt"

    async def test_an_event_for_the_draining_inbox_does_not_interrupt_its_run(self):
        blocked_id = await self.start_blocked()
        self.stop.set()
        await asyncio.wait_for(self.drained.wait(), 10)
        status, follow_up = await self.post(BLOCKED, "follow-up")
        self.assertEqual(status, 202)
        await asyncio.sleep(0.3)
        self.assertEqual(self.executor.started, ["block-1"])
        self.assertEqual(self.status(blocked_id), "processing")

        self.executor.release.set()
        await asyncio.wait_for(self.serving, timeout=10)
        self.assertEqual(self.status(blocked_id), "done")
        self.assertEqual(self.outbox(BLOCKED)[-1][0], "block-1 done")
        self.assertEqual(self.status(follow_up["event_id"]), "pending")
        self.assertEqual(self.executor.started, ["block-1"])


if __name__ == "__main__":
    unittest.main()
