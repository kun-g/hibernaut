from __future__ import annotations

import copy
import json
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteInboxAdapter
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import (
    TASK_CONTEXT_KEY,
    Emit,
    Event,
    OutboxMessage,
    RunOutcome,
    SessionRef,
    Suspend,
    explicit_trace_id,
    resolve_trace_id,
    resolve_trigger_event,
)
from agent_runtime.core.ports import InboxPort, RunCapabilities
from agent_runtime.engine.worker import AgentWorker, RunReport
from tests.fakes import (
    InMemoryInbox,
    InMemoryLock,
    InMemoryMemoryStore,
    InMemoryOutbox,
    InMemoryPermission,
    InMemorySessionStore,
    InMemoryTimerStore,
    RecordingChannel,
)

INBOX = "proj:hibernaut"


def _event(event_id: str, payload: dict[str, Any] | None = None, **kwargs: Any) -> Event:
    return Event(
        inbox_id=INBOX, source="webhook", external_id=event_id, payload=payload or {}, event_id=event_id, **kwargs
    )


class _ScriptedExecutor:
    executor_id = "claude"

    def __init__(self, *outcomes: RunOutcome) -> None:
        self._outcomes = list(outcomes)

    async def execute(
        self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities
    ) -> RunOutcome:
        return self._outcomes.pop(0)


class TestTraceResolution(unittest.TestCase):
    def test_priority_is_trace_id_then_underscore_then_event_id(self) -> None:
        self.assertEqual(resolve_trace_id(_event("e", {"trace_id": "a", "_trace_id": "b"})), "a")
        self.assertEqual(resolve_trace_id(_event("e", {"_trace_id": "b"})), "b")
        self.assertEqual(resolve_trace_id(_event("e")), "e")

    def test_unusable_values_are_skipped(self) -> None:
        for bad in ("", "  ", None, 7, ["x"]):
            with self.subTest(bad=bad):
                self.assertEqual(resolve_trace_id(_event("e", {"trace_id": bad, "_trace_id": "b"})), "b")
                self.assertEqual(resolve_trace_id(_event("e", {"trace_id": bad})), "e")
                self.assertIsNone(explicit_trace_id(_event("e", {"trace_id": bad})))

    def test_resolution_leaves_the_event_unchanged(self) -> None:
        event = _event("e", {"_trace_id": "b", "nested": {"k": [1]}})
        before = copy.deepcopy(event)
        resolve_trace_id(event)
        self.assertEqual(event, before)

    def test_trigger_is_the_last_traced_event_else_the_first(self) -> None:
        plain, t1, t2 = _event("p"), _event("a", {"trace_id": "t1"}), _event("b", {"_trace_id": "t2"})
        self.assertIs(resolve_trigger_event([t1, t2, plain]), t2)
        self.assertIs(resolve_trigger_event([plain, _event("q")]), plain)
        with self.assertRaises(ValueError):
            resolve_trigger_event([])


class TestRunTrace(unittest.IsolatedAsyncioTestCase):
    def _worker(self, inbox: InboxPort, outbox: InMemoryOutbox, *outcomes: RunOutcome) -> AgentWorker:
        return AgentWorker(
            worker_id="w1",
            inbox=inbox,
            timers=InMemoryTimerStore(),
            memory=InMemoryMemoryStore(),
            permissions=InMemoryPermission(),
            outbox=outbox,
            lock=InMemoryLock(),
            sessions=InMemorySessionStore(),
            executor=_ScriptedExecutor(*outcomes),
        )

    async def _run(
        self, *outcomes: RunOutcome, events: Sequence[Event], inbox: InboxPort | None = None
    ) -> tuple[list[RunReport], list[OutboxMessage]]:
        inbox = inbox if inbox is not None else InMemoryInbox()
        outbox = InMemoryOutbox()
        worker = self._worker(inbox, outbox, *outcomes)
        for event in events:
            inbox.append(event)
        return await worker.process_inbox(INBOX), outbox.fetch_pending()

    def _assert_trace(self, message: OutboxMessage, trace_id: str) -> None:
        self.assertEqual(message.metadata["_trace_id"], trace_id)
        self.assertEqual(message.metadata[TASK_CONTEXT_KEY]["trace_id"], trace_id)

    async def test_untraced_root_event_is_its_own_trace(self) -> None:
        reports, messages = await self._run(Emit(messages=("hi",)), events=[_event("evt_root")])
        self.assertEqual([r.trace_id for r in reports], ["evt_root"])
        self._assert_trace(messages[0], "evt_root")

    async def test_external_trace_reaches_report_and_every_message(self) -> None:
        reports, messages = await self._run(
            Emit(messages=("one", "two")), events=[_event("evt_in", {"_trace_id": "trc_ext_01"})]
        )
        self.assertEqual(reports[0].trace_id, "trc_ext_01")
        self.assertEqual(len(messages), 2)
        for message in messages:
            self._assert_trace(message, "trc_ext_01")

    async def test_batch_uses_the_last_traced_event(self) -> None:
        reports, _ = await self._run(
            Emit(messages=("hi",)),
            events=[
                _event("e1", {"trace_id": "t1"}, correlation_key="k"),
                _event("e2", {"trace_id": "t2"}, correlation_key="k"),
                _event("e3", correlation_key="k"),
            ],
        )
        self.assertEqual([(r.event_ids, r.trace_id) for r in reports], [(("e1", "e2", "e3"), "t2")])

    async def test_suspend_card_carries_the_trace(self) -> None:
        reports, messages = await self._run(
            Suspend(request_id="r1", prompt="ok?", session_checkpoint="cp"),
            events=[_event("evt_in", {"_trace_id": "trc_ext_01"})],
        )
        self.assertEqual(reports[0].trace_id, "trc_ext_01")
        self._assert_trace(messages[0], "trc_ext_01")

    async def test_approval_resume_keeps_the_trace_of_the_sessions_task(self) -> None:
        # The approval event carries no trace of its own; the session's Taskboard task does.
        inbox, outbox = InMemoryInbox(), InMemoryOutbox()
        worker = self._worker(
            inbox, outbox, Suspend(request_id="r1", prompt="ok?", session_checkpoint="cp"), Emit(messages=("done",))
        )
        inbox.append(_event("evt_work", {"task_id": "T1", "trace_id": "trc_task", "hop_count": 1}))
        first = await worker.process_inbox(INBOX)
        inbox.append(_event("evt_ok", {"approval_request_id": "r1", "approved": True}))
        second = await worker.process_inbox(INBOX)
        self.assertEqual([r.trace_id for r in first + second], ["trc_task", "trc_task"])
        for message in outbox.fetch_pending():
            self._assert_trace(message, "trc_task")

    async def test_close_session_report_carries_the_trace(self) -> None:
        reports, _ = await self._run(events=[_event("e", {"action": "close_session", "_trace_id": "t"})])
        self.assertEqual(reports[0].trace_id, "t")

    async def test_stored_event_payload_is_byte_identical_after_the_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = SQLiteDatabase(Path(tmp) / "t.db")
            try:
                inbox = SQLiteInboxAdapter(db)
                event = _event("evt_root", {"title": "标题", "n": {"b": 1, "a": [2]}})

                def stored() -> str:
                    with db.transaction() as conn:
                        return conn.execute(
                            "SELECT payload_json FROM event_inbox WHERE event_id = 'evt_root'"
                        ).fetchone()[0]

                inbox.append(event)
                before = stored()
                await self._run(Emit(messages=("hi",)), events=[], inbox=inbox)
                self.assertEqual(inbox.list_events(inbox_id=INBOX)[0].status, "done")
                self.assertEqual(stored(), before)
            finally:
                db.close()


class TestListEventsLineage(unittest.TestCase):
    def test_list_events_exposes_trace_id_and_parent_event_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            daemon = build_daemon(
                db_path=Path(tmp) / "t.db", memory_root=Path(tmp) / "mem", channel=RecordingChannel(),
                mcp_bearer_token="mcp-secret",
            )
            try:
                assert daemon.mcp_server is not None
                for ext, payload in (
                    ("root", {"text": "x"}),
                    ("ext", {"_trace_id": "trc_ext_01"}),
                    ("hop", {"trace_id": "evt_root", "parent_event_id": "evt_root"}),
                    ("bad", {"trace_id": 7, "parent_event_id": ["x"]}),
                ):
                    daemon.handler.ingest(inbox_id=INBOX, source="webhook", external_id=ext, payload=payload)
                reply = daemon.mcp_server.handle_rpc({
                    "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": "hibernaut_list_events", "arguments": {"inbox_id": INBOX}},
                })
                assert reply is not None
                rows = json.loads(reply["result"]["content"][0]["text"])["events"]
                self.assertEqual(
                    {r["external_id"]: (r["trace_id"], r["parent_event_id"]) for r in rows},
                    {
                        "root": (None, None),
                        "ext": ("trc_ext_01", None),
                        "hop": ("evt_root", "evt_root"),
                        "bad": (None, None),
                    },
                )
            finally:
                daemon.close()
