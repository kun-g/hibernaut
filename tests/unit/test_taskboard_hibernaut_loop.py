from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels.file_outbox import FileOutboxChannel
from agent_runtime.adapters.channels.switchboard_bridge import (
    SwitchboardChannelAdapter,
    SwitchboardInboundBridge,
    canonical_to_payload,
)
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.core.models import (
    DeliveryOutcome,
    Emit,
    Event,
    NewEventPolicy,
    OutboxMessage,
    RunOutcome,
    SessionRef,
    Silent,
    Suspend,
    TaskLink,
)
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine import AgentWorker, EventHandler, OutboxDispatcher
from switchboard.envelope import CanonicalEvent
from taskboard.adapters.json_schema import validate_against_schema
from taskboard.adapters.sqlite_store import SQLiteTaskStore
from taskboard.adapters.switchboard_adapter import SwitchboardAdapter
from taskboard.engine.ingestion import IngestionService
from tests.support.fakes import InMemoryMemoryStore, RecordingChannel
from tests.support.switchboard_support import Harness, make_harness, post
from tests.support.resume_runs import ParkingExecutor, RuntimeCase
from tests.support.switchboard_approval_flow import card_click

SCOPE = "proj:hibernaut"
KEY = "github:kun-g/hibernaut#42"
TASK_KEYS = ("task_action", "stage", "task_id", "trace_id", "hop_count")


def work_ready(task_id: str = "tb_tsk_1", **metadata: Any) -> CanonicalEvent:
    fields: dict[str, Any] = {
        "work_kind": "task",
        "state": "ready",
        "task_id": task_id,
        "dispatch_seq": 1,
        "assignee": "agent:default",
        "task_type": "general",
        "priority": 2,
        "trace_id": "trace-1",
        "hop_count": 1,
    }
    fields.update(metadata)
    return CanonicalEvent(
        source="taskboard",
        event_type="work.ready",
        external_id=f"tb:{task_id}:d{fields['dispatch_seq']}",
        scope=SCOPE,
        correlation_key=KEY,
        actor="taskboard",
        subject="Ready task",
        body="Do the thing",
        metadata=fields,
    )


class ScriptedExecutor:
    executor_id = "scripted"

    def __init__(self) -> None:
        self.outcomes: list[RunOutcome | Exception] = []
        self.batches: list[list[Event]] = []

    async def execute(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
        _ = (session, caps)
        self.batches.append(list(events))
        outcome = self.outcomes.pop(0) if self.outcomes else Silent()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def assert_report_schema(metadata: dict[str, Any]) -> None:
    validate_against_schema("execution_reported.metadata.schema.json", metadata)


class CanonicalToPayloadTests(unittest.TestCase):
    def test_work_ready_task_fields_are_lifted_and_kept_in_metadata(self) -> None:
        payload = canonical_to_payload(work_ready(preferred_executor="claude", dispatch_seq=3))
        for key, value in {
            "task_id": "tb_tsk_1", "trace_id": "trace-1", "hop_count": 1, "dispatch_seq": 3,
            "assignee": "agent:default", "task_type": "general", "priority": 2,
        }.items():
            with self.subTest(key):
                self.assertEqual(payload[key], value)
                self.assertEqual(payload["metadata"][key], value)
        self.assertEqual(payload["executor_id"], "claude")

    def test_priority_zero_is_lifted_and_empty_values_are_not(self) -> None:
        payload = canonical_to_payload(work_ready(priority=0, preferred_executor="", task_type=""))
        self.assertEqual(payload["priority"], 0)
        self.assertNotIn("executor_id", payload)
        self.assertNotIn("task_type", payload)

    def test_events_without_task_fields_are_unchanged(self) -> None:
        event = CanonicalEvent(
            source="github", event_type="conversation.message", external_id="c:1", scope=SCOPE,
            correlation_key=KEY, actor="kun", subject="s", body="b", metadata={"channel": "github"},
        )
        payload = canonical_to_payload(event)
        for key in ("task_id", "trace_id", "hop_count", "executor_id", "priority"):
            self.assertNotIn(key, payload)


class BuildReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.channel = SwitchboardChannelAdapter(lambda e: None)

    def report(self, **metadata: Any) -> CanonicalEvent:
        base: dict[str, Any] = {"correlation_key": KEY, "session_id": "s1", "run_id": "run_1"}
        built = self.channel.build_report(SCOPE, "body", {**base, **metadata})
        assert built is not None
        return built

    def test_task_sync_is_a_done_report_with_task_fields(self) -> None:
        report = self.report(kind="task_sync", task_id="t1", task_action="claim", stage="running",
                             trace_id="trace-1", hop_count=2)
        self.assertEqual(report.metadata["outcome_kind"], "done")
        for key, value in {"task_action": "claim", "stage": "running", "task_id": "t1", "trace_id": "trace-1",
                           "hop_count": 2}.items():
            self.assertEqual(report.metadata[key], value)
        assert_report_schema(dict(report.metadata))

    def test_explicit_done_outcome_kind_is_kept(self) -> None:
        report = self.report(kind="emit", outcome_kind="done", task_id="t1", task_action="complete")
        self.assertEqual(report.metadata["outcome_kind"], "done")

    def test_missing_trace_fields_default_when_the_report_names_a_task(self) -> None:
        report = self.report(kind="emit", outcome_kind="emit", task_id="t1", task_action="complete")
        self.assertEqual((report.metadata["trace_id"], report.metadata["hop_count"]), ("run_1", 1))
        assert_report_schema(dict(report.metadata))

    def test_hop_count_zero_is_kept(self) -> None:
        report = self.report(kind="task_sync", task_id="t1", task_action="claim", trace_id="tr", hop_count=0)
        self.assertEqual(report.metadata["hop_count"], 0)

    def test_reports_without_a_task_get_no_trace_defaults(self) -> None:
        report = self.report(kind="emit")
        for key in ("trace_id", "hop_count", "task_action", "task_id"):
            self.assertNotIn(key, report.metadata)


class _WorkerCase(unittest.IsolatedAsyncioTestCase):
    policy: NewEventPolicy = "defer"

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = SQLiteDatabase(Path(self.tmp.name) / "hibernaut.db")
        self.inbox = SQLiteInboxAdapter(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.handler = EventHandler(inbox=self.inbox)
        self.bridge = SwitchboardInboundBridge(self.handler)
        self.executor = ScriptedExecutor()
        self.worker = AgentWorker(
            worker_id="w1",
            inbox=self.inbox,
            timers=SQLiteTimerAdapter(self.db),
            memory=InMemoryMemoryStore(),
            permissions=AutoModePermissionAdapter(),
            outbox=self.outbox,
            lock=SQLiteLeaseLockAdapter(self.db),
            sessions=self.sessions,
            executor=self.executor,
            max_retries=0,
            new_event_policy=self.policy,
        )

    def tearDown(self) -> None:
        self.db.close()
        self.tmp.cleanup()

    def deliver(self, event: CanonicalEvent) -> None:
        self.assertEqual(self.bridge.handle_canonical_event(event), "acked")

    def queued(self) -> list[OutboxMessage]:
        return self.outbox.fetch_pending(limit=50)

    def task_fields(self, message: OutboxMessage) -> dict[str, Any]:
        return {key: message.metadata[key] for key in TASK_KEYS if key in message.metadata}

    def assert_claim(self, message: OutboxMessage, *, task_id: str = "tb_tsk_1") -> None:
        meta = message.metadata
        self.assertEqual((meta["kind"], meta["outcome_kind"]), ("task_sync", "done"))
        self.assertEqual(
            self.task_fields(message),
            {"task_action": "claim", "stage": "running", "task_id": task_id, "trace_id": "trace-1", "hop_count": 1},
        )
        for key in ("run_id", "session_id", "correlation_key"):
            self.assertTrue(meta.get(key), key)


class WorkerTaskLifecycleTests(_WorkerCase):
    async def test_emit_claims_then_completes_on_the_last_message(self) -> None:
        self.deliver(work_ready())
        self.executor.outcomes.append(Emit(messages=("first", "second")))
        await self.worker.process_inbox(SCOPE)

        claim, first, second = self.queued()
        self.assert_claim(claim)
        self.assertEqual(self.task_fields(first), {})
        self.assertEqual(second.content, "second")
        self.assertEqual((second.metadata["kind"], second.metadata["outcome_kind"]), ("emit", "emit"))
        self.assertEqual(
            self.task_fields(second),
            {"task_action": "complete", "task_id": "tb_tsk_1", "trace_id": "trace-1", "hop_count": 1},
        )
        # One Run, one run_id across the claim and its result.
        self.assertEqual(claim.metadata["run_id"], second.metadata["run_id"])
        self.assertEqual(claim.metadata["session_id"], second.metadata["session_id"])

    async def test_the_executor_sees_the_lifted_task_fields(self) -> None:
        self.deliver(work_ready())
        await self.worker.process_inbox(SCOPE)
        [[event]] = self.executor.batches
        self.assertEqual((event.payload["task_id"], event.payload["trace_id"]), ("tb_tsk_1", "trace-1"))

    async def test_silent_and_empty_emit_complete_with_a_task_sync(self) -> None:
        for index, outcome in enumerate((Silent(), Emit(messages=()))):
            with self.subTest(type(outcome).__name__):
                task_id = f"tb_tsk_{index}"
                self.deliver(work_ready(task_id))
                self.executor.outcomes.append(outcome)
                await self.worker.process_inbox(SCOPE)
                claim, done = self.queued()[-2:]
                self.assert_claim(claim, task_id=task_id)
                self.assertEqual((done.metadata["kind"], done.metadata["outcome_kind"]), ("task_sync", "done"))
                self.assertEqual(done.metadata["task_action"], "complete")
                self.assertEqual(done.metadata["task_id"], task_id)

    async def test_open_pr_moves_the_task_to_waiting_pr_merge(self) -> None:
        self.deliver(work_ready())
        self.executor.outcomes.append(
            Emit(messages=("PR opened",), delivery=DeliveryOutcome(state="pr_open", pr_url="https://x/pull/1"))
        )
        await self.worker.process_inbox(SCOPE)
        _, result = self.queued()
        self.assertEqual(result.metadata["outcome_kind"], "emit")
        self.assertEqual(
            (result.metadata["task_action"], result.metadata["stage"]), ("update_stage", "waiting_pr_merge")
        )
        # The task stays in progress, so a later Run in the session still reports on it.
        session = self.sessions.find_by_correlation_key(SCOPE, KEY)
        assert session is not None
        self.assertEqual(session.task_link, TaskLink(task_id="tb_tsk_1", trace_id="trace-1", hop_count=1))

    async def test_merged_delivery_completes_the_task(self) -> None:
        self.deliver(work_ready())
        self.executor.outcomes.append(Emit(messages=("merged",), delivery=DeliveryOutcome(state="merged")))
        await self.worker.process_inbox(SCOPE)
        self.assertEqual(self.queued()[-1].metadata["task_action"], "complete")

    async def test_failed_emit_releases_the_task_and_keeps_the_message(self) -> None:
        self.deliver(work_ready())
        self.executor.outcomes.append(Emit(messages=("could not do it",), status="failed"))
        await self.worker.process_inbox(SCOPE)
        _, result = self.queued()
        self.assertEqual(result.content, "could not do it")
        self.assertEqual(result.metadata["outcome_kind"], "emit")
        self.assertEqual((result.metadata["task_action"], result.metadata["stage"]), ("release", "failed"))

    async def test_executor_failure_releases_the_task(self) -> None:
        self.deliver(work_ready())
        self.executor.outcomes.append(RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            await self.worker.process_inbox(SCOPE)
        claim, release = self.queued()
        self.assert_claim(claim)
        self.assertEqual((release.metadata["kind"], release.metadata["outcome_kind"]), ("task_sync", "error"))
        self.assertEqual(
            self.task_fields(release),
            {"task_action": "release", "stage": "failed", "task_id": "tb_tsk_1", "trace_id": "trace-1",
             "hop_count": 1},
        )
        self.assertEqual(release.metadata["run_id"], claim.metadata["run_id"])

    async def test_suspend_then_approval_still_completes_the_task(self) -> None:
        self.deliver(work_ready())
        self.executor.outcomes.append(Suspend(request_id="apr_1", prompt="Deploy?", session_checkpoint="cp"))
        await self.worker.process_inbox(SCOPE)
        claim, approval = self.queued()
        self.assert_claim(claim)
        self.assertEqual(approval.metadata["kind"], "approval_request")
        self.assertEqual(approval.metadata["outcome_kind"], "suspend")
        self.assertEqual(
            self.task_fields(approval),
            {"task_action": "update_stage", "stage": "awaiting_approval", "task_id": "tb_tsk_1",
             "trace_id": "trace-1", "hop_count": 1},
        )
        suspended = self.sessions.find_by_request_id("apr_1")
        assert suspended is not None
        for message in (claim, approval):
            self.outbox.mark_sent(message.message_id)

        # The decision carries no task_id; the session's task link supplies it.
        self.deliver(CanonicalEvent(
            source="feishu", event_type="approval.decided", external_id="card:apr_1", scope=SCOPE,
            correlation_key=KEY, actor="kun", subject="approve",
            metadata={"approval_request_id": "apr_1", "decision": "approve", "session_id": suspended.session_id},
        ))
        self.executor.outcomes.append(Emit(messages=("Deployed",)))
        await self.worker.process_inbox(SCOPE)
        self.assertNotIn("task_id", self.executor.batches[-1][0].payload)
        reclaim, result = self.queued()
        self.assert_claim(reclaim)
        self.assertEqual(
            self.task_fields(result),
            {"task_action": "complete", "task_id": "tb_tsk_1", "trace_id": "trace-1", "hop_count": 1},
        )
        resumed = self.sessions.get(suspended.session_id)
        assert resumed is not None
        self.assertIsNone(resumed.task_link)

    async def test_a_later_message_after_completion_reports_nothing_to_taskboard(self) -> None:
        self.deliver(work_ready())
        self.executor.outcomes.append(Emit(messages=("done",)))
        await self.worker.process_inbox(SCOPE)
        for message in self.queued():
            self.outbox.mark_sent(message.message_id)

        self.deliver(CanonicalEvent(
            source="github", event_type="conversation.message", external_id="comment:9", scope=SCOPE,
            correlation_key=KEY, actor="kun", subject="s", body="thanks", metadata={"channel": "github"},
        ))
        self.executor.outcomes.append(Emit(messages=("you're welcome",)))
        await self.worker.process_inbox(SCOPE)
        [reply] = self.queued()
        self.assertEqual(self.task_fields(reply), {})

    async def test_a_new_dispatch_replaces_the_session_task_link(self) -> None:
        self.deliver(work_ready())
        self.executor.outcomes.append(Emit(messages=("PR",), delivery=DeliveryOutcome(state="pr_open")))
        await self.worker.process_inbox(SCOPE)
        for message in self.queued():
            self.outbox.mark_sent(message.message_id)
        self.deliver(work_ready("tb_tsk_9", dispatch_seq=2, trace_id="trace-9", hop_count=3))
        await self.worker.process_inbox(SCOPE)
        release, claim, done = self.queued()
        # The dropped task keeps its own trace fields.
        self.assertEqual(
            (release.metadata["task_action"], release.metadata["task_id"], release.metadata["trace_id"]),
            ("release", "tb_tsk_1", "trace-1"),
        )
        self.assertEqual((claim.metadata["task_id"], claim.metadata["trace_id"], claim.metadata["hop_count"]),
                         ("tb_tsk_9", "trace-9", 3))
        self.assertEqual(done.metadata["task_id"], "tb_tsk_9")


class WorkerWithoutTaskTests(_WorkerCase):
    async def test_runs_without_a_task_write_no_task_fields(self) -> None:
        self.deliver(CanonicalEvent(
            source="github", event_type="conversation.message", external_id="comment:1", scope=SCOPE,
            correlation_key=KEY, actor="kun", subject="s", body="hi", metadata={"channel": "github"},
        ))
        self.executor.outcomes.append(Emit(messages=("hello",)))
        await self.worker.process_inbox(SCOPE)
        [message] = self.queued()
        self.assertEqual(message.metadata["kind"], "emit")
        self.assertEqual(self.task_fields(message), {})

        for outcome in (Silent(), RuntimeError("boom")):
            self.outbox.mark_sent(message.message_id)
            self.deliver(CanonicalEvent(
                source="github", event_type="conversation.message", external_id=f"comment:{outcome!r}",
                scope=SCOPE, correlation_key=KEY, actor="kun", subject="s", body="hi",
                metadata={"channel": "github"},
            ))
            self.executor.outcomes.append(outcome)
            try:
                await self.worker.process_inbox(SCOPE)
            except Exception:  # noqa: BLE001
                pass
            self.assertEqual(self.queued(), [])

    async def test_suspend_without_a_task_keeps_the_old_metadata(self) -> None:
        self.deliver(CanonicalEvent(
            source="github", event_type="conversation.message", external_id="comment:1", scope=SCOPE,
            correlation_key=KEY, actor="kun", subject="s", body="deploy", metadata={"channel": "github"},
        ))
        self.executor.outcomes.append(Suspend(request_id="apr_2", prompt="Deploy?", session_checkpoint="cp"))
        await self.worker.process_inbox(SCOPE)
        [approval] = self.queued()
        self.assertEqual(self.task_fields(approval), {})


class _SupersedeMixin(_WorkerCase):
    async def suspend_task_a(self) -> SessionRef:
        self.deliver(work_ready("tb_tsk_a"))
        self.executor.outcomes.append(Suspend(request_id="apr_a", prompt="Deploy?", session_checkpoint="cp"))
        await self.worker.process_inbox(SCOPE)
        for message in self.queued():
            self.outbox.mark_sent(message.message_id)
        session = self.sessions.find_by_request_id("apr_a")
        assert session is not None and session.task_link is not None
        return session

    def assert_superseded(self, messages: list[OutboxMessage], *, old: str, new: str) -> None:
        release, claim, result = messages
        self.assertEqual((release.metadata["kind"], release.metadata["task_id"]), ("task_sync", old))
        self.assertEqual((release.metadata["task_action"], release.metadata["stage"]), ("release", "superseded"))
        self.assertEqual((claim.metadata["task_action"], claim.metadata["task_id"]), ("claim", new))
        self.assertEqual((result.metadata["task_action"], result.metadata["task_id"]), ("complete", new))
        self.assertEqual(len({m.metadata["run_id"] for m in messages}), 1)


class SupersededTaskTests(_SupersedeMixin):
    async def test_a_new_task_in_a_suspended_session_releases_the_old_one(self) -> None:
        session = await self.suspend_task_a()
        self.deliver(work_ready("tb_tsk_b", dispatch_seq=1))
        self.executor.outcomes.append(Emit(messages=("B done",)))
        await self.worker.process_inbox(SCOPE)
        self.assert_superseded(self.queued(), old="tb_tsk_a", new="tb_tsk_b")
        after = self.sessions.get(session.session_id)
        assert after is not None
        self.assertIsNone(after.task_link)

    async def test_a_new_task_in_a_waiting_pr_session_releases_the_old_one_on_silent(self) -> None:
        self.deliver(work_ready("tb_tsk_a"))
        self.executor.outcomes.append(Emit(messages=("PR",), delivery=DeliveryOutcome(state="pr_open")))
        await self.worker.process_inbox(SCOPE)
        for message in self.queued():
            self.outbox.mark_sent(message.message_id)
        self.deliver(work_ready("tb_tsk_b"))
        self.executor.outcomes.append(Silent())
        await self.worker.process_inbox(SCOPE)
        release, claim, done = self.queued()
        self.assertEqual((release.metadata["task_action"], release.metadata["task_id"]), ("release", "tb_tsk_a"))
        self.assertEqual((claim.metadata["task_action"], claim.metadata["task_id"]), ("claim", "tb_tsk_b"))
        self.assertEqual((done.metadata["task_action"], done.metadata["task_id"]), ("complete", "tb_tsk_b"))

    async def test_an_approval_batch_that_also_carries_a_new_task_releases_the_old_one(self) -> None:
        session = await self.suspend_task_a()
        # The decision closes its batch, so the new dispatch comes first to share the Run.
        self.deliver(work_ready("tb_tsk_b"))
        self.deliver(CanonicalEvent(
            source="feishu", event_type="approval.decided", external_id="card:apr_a", scope=SCOPE,
            correlation_key=KEY, actor="kun", subject="approve",
            metadata={"approval_request_id": "apr_a", "decision": "approve", "session_id": session.session_id},
        ))
        self.executor.outcomes.append(Emit(messages=("B done",)))
        await self.worker.process_inbox(SCOPE)
        self.assertEqual(len(self.executor.batches[-1]), 2)
        self.assert_superseded(self.queued(), old="tb_tsk_a", new="tb_tsk_b")

    async def test_the_same_task_redispatched_is_not_released(self) -> None:
        await self.suspend_task_a()
        self.deliver(work_ready("tb_tsk_a", dispatch_seq=2))
        self.executor.outcomes.append(Emit(messages=("A done",)))
        await self.worker.process_inbox(SCOPE)
        claim, result = self.queued()
        self.assertEqual((claim.metadata["task_action"], result.metadata["task_action"]), ("claim", "complete"))

    async def test_a_failed_run_of_the_new_task_still_released_the_old_one(self) -> None:
        session = await self.suspend_task_a()
        self.deliver(work_ready("tb_tsk_b"))
        self.executor.outcomes.append(RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            await self.worker.process_inbox(SCOPE)
        actions = [(m.metadata["task_action"], m.metadata["task_id"]) for m in self.queued()]
        self.assertEqual(actions, [("release", "tb_tsk_a"), ("claim", "tb_tsk_b"), ("release", "tb_tsk_b")])
        after = self.sessions.get(session.session_id)
        assert after is not None
        self.assertIsNone(after.task_link)


class InterruptibleSupersedeTests(_SupersedeMixin):
    policy: NewEventPolicy = "interrupt"

    async def test_release_and_claim_are_written_with_the_result(self) -> None:
        await self.suspend_task_a()
        self.deliver(work_ready("tb_tsk_b"))
        self.executor.outcomes.append(Emit(messages=("B done",)))
        await self.worker.process_inbox(SCOPE)
        self.assert_superseded(self.queued(), old="tb_tsk_a", new="tb_tsk_b")


class FoldedTaskTests(_WorkerCase):
    policy: NewEventPolicy = "fold"

    async def test_a_task_folded_into_the_run_supersedes_the_claimed_one(self) -> None:
        started = asyncio.Event()
        resume = asyncio.Event()
        folded: list[Event] = []

        async def execute(events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            _ = session
            started.set()
            await resume.wait()
            folded.extend(caps.poll_folded_events())
            return Emit(messages=("both",))

        self.executor.execute = execute  # type: ignore[method-assign]
        self.deliver(work_ready("tb_tsk_a"))
        running = asyncio.create_task(self.worker.process_inbox(SCOPE))
        await asyncio.wait_for(started.wait(), 5)
        self.deliver(work_ready("tb_tsk_b"))
        resume.set()
        await asyncio.wait_for(running, 5)

        self.assertEqual([e.payload["task_id"] for e in folded], ["tb_tsk_b"])
        claim_a, release_a, claim_b, result = self.queued()
        self.assertEqual((claim_a.metadata["task_action"], claim_a.metadata["task_id"]), ("claim", "tb_tsk_a"))
        self.assertEqual((release_a.metadata["task_action"], release_a.metadata["task_id"]), ("release", "tb_tsk_a"))
        self.assertEqual((claim_b.metadata["task_action"], claim_b.metadata["task_id"]), ("claim", "tb_tsk_b"))
        self.assertEqual((result.metadata["task_action"], result.metadata["task_id"]), ("complete", "tb_tsk_b"))


class InterruptTests(_WorkerCase):
    policy: NewEventPolicy = "interrupt"

    async def test_an_interrupted_run_writes_nothing_and_the_restart_claims_and_completes(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        executor = self.executor
        runs: list[list[str]] = []

        async def execute(events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            _ = (session, caps)
            runs.append([str(e.payload.get("body")) for e in events])
            if len(runs) == 1:
                started.set()
                await release.wait()
            return Emit(messages=("merged",))

        executor.execute = execute  # type: ignore[method-assign]
        self.deliver(work_ready())
        running = asyncio.create_task(self.worker.process_inbox(SCOPE))
        await asyncio.wait_for(started.wait(), 5)
        # Nothing reaches the Outbox while an interruptible Run may still be thrown away.
        self.assertEqual(self.queued(), [])
        self.deliver(CanonicalEvent(
            source="github", event_type="conversation.message", external_id="comment:urgent", scope=SCOPE,
            correlation_key=KEY, actor="kun", subject="s", body="urgent", metadata={"channel": "github"},
        ))
        self.worker.notify_new_event(SCOPE)
        await asyncio.wait_for(running, 5)

        self.assertEqual(runs, [["Do the thing"], ["Do the thing", "urgent"]])
        claim, result = self.queued()
        self.assert_claim(claim)
        self.assertEqual(result.content, "merged")
        self.assertEqual(result.metadata["task_action"], "complete")


class ResumedRunTests(RuntimeCase):
    async def test_a_resumed_run_does_not_claim_again(self) -> None:
        parker = ParkingExecutor()
        self.inbox.append(Event(
            inbox_id="proj:x", source="sb:taskboard", external_id="tb:t1:d1",
            payload={"text": "task", "executor_id": "parker", "task_id": "t1", "trace_id": "trace-1", "hop_count": 1},
            correlation_key="task:1",
        ))
        first = self.worker("old", {"parker": parker})
        running = asyncio.create_task(first.process_inbox("proj:x"))
        await asyncio.wait_for(parker.parked.wait(), 5)
        first.detach()
        await asyncio.wait_for(running, 5)
        [claim] = self.outbox.fetch_pending(limit=50)
        self.assertEqual(claim.metadata["task_action"], "claim")

        second = self.worker("new", {"parker": ParkingExecutor()})
        await second.process_inbox("proj:x")
        claim_again, result = self.outbox.fetch_pending(limit=50)
        self.assertEqual(claim_again.message_id, claim.message_id)
        self.assertEqual(result.metadata["task_action"], "complete")
        self.assertEqual(result.metadata["task_id"], "t1")


class FileOutboxTaskSyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_task_sync_writes_no_file_but_still_reaches_the_inner_channel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            inner = RecordingChannel()
            announced: list[str] = []
            channel = FileOutboxChannel(Path(tmp) / "outbox", inner, events_url="http://x/events",
                                        announce=announced.append)
            meta = {"kind": "task_sync", "outcome_kind": "done", "task_action": "claim", "session_id": "s1"}
            await channel.send(SCOPE, "claim", dict(meta))
            self.assertFalse((Path(tmp) / "outbox").exists() and any((Path(tmp) / "outbox").iterdir()))
            self.assertEqual(announced, [])
            self.assertEqual([sent[1] for sent in inner.sent], ["claim"])

            await channel.send(SCOPE, "result", {"kind": "emit", "session_id": "s1"})
            self.assertEqual(len(list((Path(tmp) / "outbox").glob("*.md"))), 1)


class TaskboardClosedLoopTests(unittest.IsolatedAsyncioTestCase):
    # Taskboard, Switchboard and Hibernaut in one process over the in-process bus.
    def setUp(self) -> None:
        self.h: Harness = make_harness()
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.db = SQLiteDatabase(root / "hibernaut.db")
        self.inbox = SQLiteInboxAdapter(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.executor = ScriptedExecutor()
        self.worker = AgentWorker(
            worker_id="w1", inbox=self.inbox, timers=SQLiteTimerAdapter(self.db), memory=InMemoryMemoryStore(),
            permissions=AutoModePermissionAdapter(), outbox=self.outbox, lock=SQLiteLeaseLockAdapter(self.db),
            sessions=self.sessions, executor=self.executor, max_retries=0,
        )
        self.h.board.bus.subscribe("hibernaut", SwitchboardInboundBridge(EventHandler(inbox=self.inbox)).handle_canonical_event)
        self.tb_store = SQLiteTaskStore(root / "taskboard.db")
        adapter = SwitchboardAdapter(self.h.board.bus)
        self.taskboard = IngestionService(self.tb_store, publisher=adapter)
        adapter.bind_ingestion(self.taskboard)
        adapter.attach("taskboard")
        channel = SwitchboardChannelAdapter(self.h.board.bus.publish, fallback=RecordingChannel())
        self.dispatcher = OutboxDispatcher(outbox=self.outbox, channel=channel)

    def tearDown(self) -> None:
        self.tb_store.close()
        self.db.close()
        self.h.close()
        self.tmp.cleanup()

    def pump(self) -> None:
        for _ in range(3):
            self.h.board.worker.run_once()
            self.h.board.gateway.run_once()

    async def flush(self) -> None:
        while await self.dispatcher.dispatch_once():
            pass
        self.pump()

    def status(self, task_id: str) -> tuple[str, str | None]:
        task = self.tb_store.get_task(task_id)
        assert task is not None
        return task.status, task.stage

    def create(self) -> str:
        task = self.taskboard.tb_create_task(SCOPE, "Fix it", initial_status="ready", source_ref=KEY)
        self.pump()
        return task.task_id

    def taskboard_deliveries_failed(self) -> list[str]:
        return [row.status for row in self.h.board.store.list_deliveries()
                if row.consumer_id == "taskboard" and row.status != "acked"]

    async def test_work_ready_to_emit_marks_the_task_done_without_extra_comments(self) -> None:
        task_id = self.create()
        self.executor.outcomes.append(Emit(messages=("Fixed",)))
        await self.worker.process_inbox(SCOPE)
        await self.flush()
        self.assertEqual(self.status(task_id)[0], "done")
        self.assertEqual(self.taskboard_deliveries_failed(), [])
        # The claim stays internal: only the result reaches the GitHub origin.
        self.assertEqual([call["json"]["body"] for call in self.h.http.calls if "github" in call["url"]],
                         [self.h.http.calls[-1]["json"]["body"]])
        self.assertIn("Fixed", self.h.http.calls[-1]["json"]["body"])

    async def test_suspend_waits_in_progress_and_the_approval_completes_it(self) -> None:
        task_id = self.create()
        self.executor.outcomes.append(Suspend(request_id="apr_5", prompt="Deploy?", session_checkpoint="cp"))
        await self.worker.process_inbox(SCOPE)
        await self.flush()
        self.assertEqual(self.status(task_id), ("in_progress", "awaiting_approval"))

        card = next(c for c in self.h.http.calls if "receive_id" in (c["json"] or {}))
        content = json.loads(card["json"]["content"])
        approve = next(a for el in content["elements"] if el["tag"] == "action" for a in el["actions"])["value"]
        status, _ = post(f"{self.h.url}/webhooks/feishu", card_click(approve))
        self.assertEqual(status, 200)
        self.pump()
        self.executor.outcomes.append(Emit(messages=("Deployed",)))
        await self.worker.process_inbox(SCOPE)
        await self.flush()
        self.assertEqual(self.status(task_id)[0], "done")
        self.assertEqual(self.taskboard_deliveries_failed(), [])

    async def test_failure_releases_the_task_back_to_ready(self) -> None:
        task_id = self.create()
        self.executor.outcomes.append(RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            await self.worker.process_inbox(SCOPE)
        await self.flush()
        # The claim set stage "running"; release returns the task to ready and leaves the stage as it was.
        self.assertEqual(self.status(task_id), ("ready", "running"))
        self.assertEqual(self.taskboard_deliveries_failed(), [])

    async def test_open_pr_leaves_the_task_in_progress(self) -> None:
        task_id = self.create()
        self.executor.outcomes.append(Emit(messages=("PR",), delivery=DeliveryOutcome(state="pr_open")))
        await self.worker.process_inbox(SCOPE)
        await self.flush()
        self.assertEqual(self.status(task_id), ("in_progress", "waiting_pr_merge"))


if __name__ == "__main__":
    unittest.main()
