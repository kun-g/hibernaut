import asyncio
import unittest

from tests.support.unmatched_project_hold import ORPHAN


class TestHeldEventsInARunningInbox(unittest.IsolatedAsyncioTestCase):
    async def test_a_held_event_is_neither_claimed_folded_nor_an_interrupt(self):
        from tests.support.fakes import (
            InMemoryInbox,
            InMemoryLock,
            InMemoryMemoryStore,
            InMemoryOutbox,
            InMemoryPermission,
            InMemorySessionStore,
            InMemoryTimerStore,
        )
        from tests.support.batch_grouping import _RecordingExecutor
        from agent_runtime.core.models import Event
        from agent_runtime.engine.worker import AgentWorker

        for policy in ("defer", "fold", "interrupt"):
            with self.subTest(policy=policy):
                inbox = InMemoryInbox()
                executor = _RecordingExecutor()
                checked = asyncio.Event()

                def hold(event: Event) -> str | None:
                    if event.external_id == "held-2":
                        checked.set()
                    return "needs a project" if event.payload.get("held") else None

                worker = AgentWorker(
                    worker_id="w1", inbox=inbox, timers=InMemoryTimerStore(), memory=InMemoryMemoryStore(),
                    permissions=InMemoryPermission(), outbox=InMemoryOutbox(), executor=executor,
                    lock=InMemoryLock(), sessions=InMemorySessionStore(), new_event_policy=policy,
                    hold=hold,
                )

                def add(external_id: str, **payload: object) -> Event:
                    stored, _ = inbox.append(Event(inbox_id=ORPHAN, source="test", external_id=external_id,
                                                   payload={"text": external_id, "explicit_workdir": "/tmp", **payload},
                                                   correlation_key="task:k"))
                    return stored

                held_before = add("held-0", held=True)
                add("run-1")

                async def arrive(_batch: list[str]) -> None:
                    if not executor.calls:
                        add("held-2", held=True)
                        worker.notify_new_event(ORPHAN)
                        if policy == "interrupt":
                            await asyncio.wait_for(checked.wait(), timeout=5)

                executor.during_run = arrive
                await worker.process_inbox(ORPHAN)

                self.assertEqual(executor.calls, [["run-1"]])
                self.assertEqual(executor.started, [["run-1"]])
                pending = inbox.fetch_pending(ORPHAN)
                self.assertEqual([e.external_id for e in pending], ["held-0", "held-2"])
                attempts = inbox.diagnostics([e.event_id for e in pending])
                self.assertEqual({d.attempts for d in attempts.values()}, {0})
                self.assertIsNone(pending[0].session_id)
                self.assertEqual(pending[0].event_id, held_before.event_id)
