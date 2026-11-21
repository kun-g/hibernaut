from __future__ import annotations

import unittest
from collections.abc import Sequence

from agent_runtime.core.models import Event, NewEventPolicy, RunOutcome, SessionRef, Silent
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine.worker import AgentWorker
from tests.fakes import (
    InMemoryInbox,
    InMemoryLock,
    InMemoryMemoryStore,
    InMemoryOutbox,
    InMemoryPermission,
    InMemorySessionStore,
    InMemoryTimerStore,
)

INBOX = "proj:batching"
POLICIES: tuple[NewEventPolicy, ...] = ("defer", "fold", "interrupt")


class _RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = session
        batch = [evt.external_id for evt in events]
        batch += [evt.external_id for evt in caps.poll_folded_events()]
        self.calls.append(batch)
        return Silent()


def _build(policy: NewEventPolicy) -> tuple[AgentWorker, InMemoryInbox, _RecordingExecutor]:
    inbox = InMemoryInbox()
    executor = _RecordingExecutor()
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
        new_event_policy=policy,
    )
    return worker, inbox, executor


def _event(
    external_id: str,
    *,
    session_id: str | None = None,
    correlation_key: str | None = None,
) -> Event:
    return Event(
        inbox_id=INBOX,
        source="test",
        external_id=external_id,
        payload={},
        session_id=session_id,
        correlation_key=correlation_key,
    )


class TestBatchGrouping(unittest.IsolatedAsyncioTestCase):
    async def test_events_without_session_run_one_at_a_time_under_defer_and_fold(self) -> None:
        for policy in ("defer", "fold"):
            with self.subTest(policy=policy):
                worker, inbox, executor = _build(policy)
                for idx in range(3):
                    inbox.append(_event(f"anon-{idx}"))

                reports = await worker.process_inbox(INBOX)

                self.assertEqual(executor.calls, [["anon-0"], ["anon-1"], ["anon-2"]])
                self.assertEqual(len(reports), 3)
                self.assertEqual(inbox.fetch_pending(INBOX), [])

    async def test_events_without_session_merge_on_interrupt(self) -> None:
        worker, inbox, executor = _build("interrupt")
        for idx in range(3):
            inbox.append(_event(f"anon-{idx}"))

        await worker.process_inbox(INBOX)

        self.assertEqual(executor.calls, [["anon-0", "anon-1", "anon-2"]])

    async def test_interleaved_sessions_group_stably_in_first_seen_order(self) -> None:
        for policy in POLICIES:
            with self.subTest(policy=policy):
                worker, inbox, executor = _build(policy)
                inbox.append(_event("A1", session_id="chat_1"))
                inbox.append(_event("B1", session_id="chat_2"))
                inbox.append(_event("A2", session_id="chat_1"))

                await worker.process_inbox(INBOX)

                self.assertEqual(executor.calls, [["A1", "A2"], ["B1"]])

    async def test_mixed_session_correlation_and_anonymous_keys(self) -> None:
        worker, inbox, executor = _build("defer")
        inbox.append(_event("n1"))
        inbox.append(_event("c1", correlation_key="gh#1"))
        inbox.append(_event("s1", session_id="chat_1"))
        inbox.append(_event("n2"))
        inbox.append(_event("c2", correlation_key="gh#1"))
        inbox.append(_event("s2", session_id="chat_1"))
        inbox.append(_event("c3", correlation_key="gh#2"))

        await worker.process_inbox(INBOX)

        self.assertEqual(
            executor.calls,
            [["n1"], ["c1", "c2"], ["s1", "s2"], ["n2"], ["c3"]],
        )

    async def test_approval_decision_runs_alone_and_later_events_wait(self) -> None:
        worker, inbox, executor = _build("defer")
        inbox.append(_event("A1", session_id="chat_1"))
        inbox.append(_approval("A-approve", session_id="chat_1"))
        inbox.append(_event("B1", session_id="chat_2"))
        inbox.append(_event("A2", session_id="chat_1"))

        await worker.process_inbox(INBOX)

        self.assertEqual(executor.calls, [["A1"], ["B1"], ["A-approve"], ["A2"]])
        self.assertEqual(inbox.fetch_pending(INBOX), [])


def _approval(external_id: str, *, session_id: str) -> Event:
    return Event(
        inbox_id=INBOX,
        source="test",
        external_id=external_id,
        payload={"approval_request_id": "req-1", "approved": True},
        session_id=session_id,
    )


if __name__ == "__main__":
    unittest.main()
