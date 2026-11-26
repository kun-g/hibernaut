from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.adapters.executor_whitebox import LLMMessage, LLMReply, ToolCallRequest
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit, Event
from tests.support.fakes import RecordingChannel


class _FoldDemoLLM:
    def __init__(self, on_first_turn: asyncio.Event, allow_continue: asyncio.Event) -> None:
        self.calls = 0
        self.seen_user_payloads: list[dict[str, object]] = []
        self._on_first_turn = on_first_turn
        self._allow_continue = allow_continue

    async def complete(
        self,
        *,
        system_prefix: str,
        messages: Sequence[LLMMessage],
        available_tools: Sequence[str],
    ) -> LLMReply:
        _ = (system_prefix, available_tools)
        self.calls += 1
        user_payloads = [
            json.loads(m.content)["payload"]
            for m in messages
            if m.role == "user"
        ]
        self.seen_user_payloads = user_payloads

        if self.calls == 1:
            self._on_first_turn.set()
            await self._allow_continue.wait()
            return LLMReply(
                tool_calls=(ToolCallRequest(tool_name="fetch_status", args={"step": 1}),)
            )

        texts = [str(p.get("text", "")) for p in user_payloads]
        return LLMReply(emit_messages=(f"folded:{'+'.join(texts)}",))


class _InterruptDemoLLM:
    def __init__(self, first_run_started: asyncio.Event) -> None:
        self.started_batches: list[list[str]] = []
        self.cancelled_count = 0
        self._first_run_started = first_run_started

    async def complete(
        self,
        *,
        system_prefix: str,
        messages: Sequence[LLMMessage],
        available_tools: Sequence[str],
    ) -> LLMReply:
        _ = (system_prefix, available_tools)
        texts = [
            str(json.loads(m.content)["payload"].get("text", ""))
            for m in messages
            if m.role == "user"
        ]
        self.started_batches.append(texts)

        if len(self.started_batches) == 1:
            self._first_run_started.set()
            try:
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                self.cancelled_count += 1
                raise

        return LLMReply(emit_messages=(f"merged:{'+'.join(texts)}",))


class TestNewEventPolicy(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="policy_e2e_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def test_fold_injects_same_session_event_and_marks_all_done(self) -> None:
        first_turn = asyncio.Event()
        allow_continue = asyncio.Event()
        llm = _FoldDemoLLM(first_turn, allow_continue)
        channel = RecordingChannel()
        daemon = build_daemon(
            db_path=self.root / "fold.db",
            memory_root=self.root / "mem_fold",
            max_concurrency=2,
            llm=llm,
            channel=channel,
            new_event_policy="fold",
        )
        try:
            stop = asyncio.Event()
            daemon_task = asyncio.create_task(
                daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
            )

            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="im",
                external_id="f-1",
                session_id="sess-A",
                payload={"text": "first"},
            )
            await asyncio.wait_for(first_turn.wait(), timeout=2.0)

            # New event on same inbox_id + same session_id arrives mid-Run.
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="im",
                external_id="f-2",
                session_id="sess-A",
                payload={"text": "second"},
            )
            # Event on different session_id must NOT be folded into sess-A's Run.
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="im",
                external_id="f-other",
                session_id="sess-B",
                payload={"text": "other"},
            )
            allow_continue.set()

            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            await daemon.dispatcher.dispatch_once()
            stop.set()
            await daemon_task

            self.assertEqual(daemon.inbox.fetch_pending("proj:hibernaut"), [])
            # First Run report must include both f-1 and f-2, and emit combined output.
            first_report = daemon.pool.reports[0]
            self.assertEqual(len(first_report.event_ids), 2)
            self.assertIsInstance(first_report.outcome, Emit)
            assert isinstance(first_report.outcome, Emit)
            self.assertEqual(first_report.outcome.messages, ("folded:first+second",))

            # Second Run report handles sess-B separately.
            self.assertEqual(len(daemon.pool.reports), 2)
            self.assertEqual(len(daemon.pool.reports[1].event_ids), 1)
        finally:
            daemon.close()

    async def test_interrupt_cancels_cleanly_without_outbox_and_restarts_merged(self) -> None:
        first_started = asyncio.Event()
        llm = _InterruptDemoLLM(first_started)
        channel = RecordingChannel()
        daemon = build_daemon(
            db_path=self.root / "interrupt.db",
            memory_root=self.root / "mem_interrupt",
            max_concurrency=2,
            llm=llm,
            channel=channel,
            new_event_policy="interrupt",
        )
        try:
            stop = asyncio.Event()
            daemon_task = asyncio.create_task(
                daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
            )

            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="im",
                external_id="i-1",
                session_id="sess-1",
                payload={"text": "initial_plan"},
            )
            await asyncio.wait_for(first_started.wait(), timeout=2.0)

            # High-priority user correction arrives while Run 1 is mid-flight.
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="im",
                external_id="i-2",
                session_id="sess-1",
                payload={"text": "urgent_correction"},
            )

            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            await daemon.dispatcher.dispatch_once()
            stop.set()
            await daemon_task

            self.assertEqual(llm.cancelled_count, 1)
            self.assertEqual(llm.started_batches[0], ["initial_plan"])
            self.assertEqual(llm.started_batches[1], ["initial_plan", "urgent_correction"])
            self.assertEqual(daemon.inbox.fetch_pending("proj:hibernaut"), [])

            # Cancelled Run must emit ZERO half-baked Outbox messages; only merged Run emits.
            self.assertEqual(len(daemon.pool.reports), 1)
            self.assertEqual(len(channel.sent), 1)
            self.assertEqual(channel.sent[0][1], "merged:initial_plan+urgent_correction")
        finally:
            daemon.close()


if __name__ == "__main__":
    unittest.main()
