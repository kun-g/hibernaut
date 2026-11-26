from __future__ import annotations

import asyncio
import unittest

from agent_runtime.core.models import Event, NewEventPolicy
from agent_runtime.engine.worker import AgentWorker
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


INBOX = "proj:batching"
POLICIES: tuple[NewEventPolicy, ...] = ("defer", "fold", "interrupt")


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
    workdir: str | None = None,
) -> Event:
    return Event(
        inbox_id=INBOX,
        source="test",
        external_id=external_id,
        payload={"explicit_workdir": workdir} if workdir else {},
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

    async def test_approval_decision_closes_batch_and_later_events_wait(self) -> None:
        worker, inbox, executor = _build("defer")
        inbox.append(_event("A1", session_id="chat_1"))
        inbox.append(_approval("A-approve", session_id="chat_1"))
        inbox.append(_event("B1", session_id="chat_2"))
        inbox.append(_event("A2", session_id="chat_1"))

        await worker.process_inbox(INBOX)

        self.assertEqual(executor.calls, [["A1", "A-approve"], ["B1"], ["A2"]])
        self.assertEqual(inbox.fetch_pending(INBOX), [])

    async def test_events_with_different_execution_targets_do_not_share_a_run(self) -> None:
        worker, inbox, executor = _build("defer")
        inbox.append(_event("A1", session_id="chat_1", workdir="/repo/a"))
        inbox.append(_event("B1", session_id="chat_2"))
        inbox.append(_event("A2", session_id="chat_1", workdir="/repo/b"))
        inbox.append(_event("A3", session_id="chat_1", workdir="/repo/b"))

        await worker.process_inbox(INBOX)

        self.assertEqual(executor.calls, [["A1"], ["B1"], ["A2", "A3"]])

    async def test_pending_approval_joins_first_batch_past_incompatible_events(self) -> None:
        worker, inbox, executor = _build("defer")
        inbox.append(_event("A1", session_id="chat_1", workdir="/repo/a"))
        inbox.append(_event("A2", session_id="chat_1", workdir="/repo/b"))
        inbox.append(_approval("A-approve", session_id="chat_1"))
        inbox.append(_event("A3", session_id="chat_1", workdir="/repo/a"))

        await worker.process_inbox(INBOX)

        self.assertEqual(executor.calls, [["A1", "A-approve"], ["A2"], ["A3"]])

    async def test_fold_does_not_cross_approval_boundary(self) -> None:
        worker, inbox, executor = _build("fold")
        inbox.append(_approval("A-approve", session_id="chat_1"))
        inbox.append(_event("A2", session_id="chat_1"))

        await worker.process_inbox(INBOX)

        self.assertEqual(executor.calls, [["A-approve"], ["A2"]])

    async def test_fold_stops_before_an_approval_arriving_mid_run(self) -> None:
        worker, inbox, executor = _build("fold")
        inbox.append(_event("A1", session_id="chat_1"))

        async def arrive(batch: list[str]) -> None:
            if batch == ["A1"]:
                inbox.append(_approval("A-approve", session_id="chat_1"))
                inbox.append(_event("A2", session_id="chat_1"))

        executor.during_run = arrive
        await worker.process_inbox(INBOX)

        self.assertEqual(executor.calls, [["A1"], ["A-approve"], ["A2"]])

    async def test_interrupt_ignores_events_already_in_the_snapshot(self) -> None:
        worker, inbox, executor = _build("interrupt")
        inbox.append(_event("A1", session_id="chat_1"))
        inbox.append(_event("B1", session_id="chat_2"))

        async def slow(batch: list[str]) -> None:
            _ = batch
            await asyncio.sleep(0.1)

        executor.during_run = slow
        await worker.process_inbox(INBOX)

        self.assertEqual(executor.started, [["A1"], ["B1"]])

    async def test_interrupt_does_not_restart_an_approval_run(self) -> None:
        worker, inbox, executor = _build("interrupt")
        inbox.append(_approval("A-approve", session_id="chat_1"))

        async def arrive(batch: list[str]) -> None:
            if batch == ["A-approve"]:
                inbox.append(_event("B1", session_id="chat_2"))
                await asyncio.sleep(0.1)

        executor.during_run = arrive
        await worker.process_inbox(INBOX)

        self.assertEqual(executor.started, [["A-approve"], ["B1"]])

    async def test_executor_routing_follows_ordinary_events_not_the_approval(self) -> None:
        primary, other = _RecordingExecutor(), _RecordingExecutor()
        other.executor_id = "other"
        worker = AgentWorker(
            worker_id="w1",
            inbox=(inbox := InMemoryInbox()),
            timers=InMemoryTimerStore(),
            memory=InMemoryMemoryStore(),
            permissions=InMemoryPermission(),
            outbox=InMemoryOutbox(),
            executors={"recording": primary, "other": other},
            global_default_executor="recording",
            lock=InMemoryLock(),
            sessions=InMemorySessionStore(),
        )
        inbox.append(_routed("A1", executor_id="other"))
        inbox.append(_routed("A-approve", executor_id="recording", approval=True))
        inbox.append(_routed("A2", executor_id="recording"))

        await worker.process_inbox(INBOX)

        self.assertEqual(other.calls, [["A1", "A-approve"]])
        self.assertEqual(primary.calls, [["A2"]])


def _routed(external_id: str, *, executor_id: str, approval: bool = False) -> Event:
    payload: dict[str, object] = {"executor_id": executor_id}
    if approval:
        payload |= {"approval_request_id": "req-1", "approved": True}
    return Event(
        inbox_id=INBOX,
        source="test",
        external_id=external_id,
        payload=payload,
        session_id="chat_1",
    )


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
