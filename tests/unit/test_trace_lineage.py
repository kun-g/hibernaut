from __future__ import annotations

import copy
import json
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from agent_runtime.adapters.ledger_bridge import RunLedger
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteInboxAdapter, SQLiteSessionStoreAdapter
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import (
    FORWARD_SOURCE,
    RETURN_CANCELLED_SOURCE,
    RETURN_SOURCE,
    TASK_CONTEXT_KEY,
    DeliveryOutcome,
    Emit,
    Event,
    ForwardSpec,
    OutboxMessage,
    RunOutcome,
    SessionRef,
    Silent,
    Suspend,
    explicit_trace_id,
    resolve_trace_id,
    resolve_trigger_event,
)
from agent_runtime.core.ports import InboxPort, RunCapabilities, RunLedgerPort
from agent_runtime.engine.closing import close_session
from agent_runtime.engine.worker import AgentWorker, ExecutorFailed, RunReport
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
from tests.support.inbox_forwarding import DOCS, MAIN, SDE, ForwardCase
from tests.support.inbox_forwarding import Ledger as RecordingLedger

INBOX = "proj:hibernaut"


def _event(event_id: str, payload: dict[str, Any] | None = None, **kwargs: Any) -> Event:
    return Event(
        inbox_id=INBOX, source="webhook", external_id=event_id, payload=payload or {}, event_id=event_id, **kwargs
    )


class _ScriptedExecutor:
    executor_id = "claude"

    def __init__(self, *outcomes: RunOutcome | Exception) -> None:
        self._outcomes = list(outcomes)

    async def execute(
        self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities
    ) -> RunOutcome:
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


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
    def _worker(
        self,
        inbox: InboxPort,
        outbox: InMemoryOutbox,
        *outcomes: RunOutcome | Exception,
        run_ledger: RunLedgerPort | None = None,
    ) -> AgentWorker:
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
            run_ledger=run_ledger,
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

    async def test_ledger_run_and_outbox_carry_the_same_trace(self) -> None:
        for event, trace in ((_event("evt_root"), "evt_root"), (_event("evt_in", {"_trace_id": "trc_ext_01"}), "trc_ext_01")):
            with self.subTest(trace=trace), tempfile.TemporaryDirectory() as tmp:
                ledger = RunLedger(Path(tmp) / "ledger.db")
                try:
                    inbox, outbox = InMemoryInbox(), InMemoryOutbox()
                    inbox.append(event)
                    worker = self._worker(inbox, outbox, Emit(messages=("hi",)), run_ledger=ledger.recorder)
                    await worker.process_inbox(INBOX)
                    (message,) = outbox.fetch_pending()
                    self._assert_trace(message, trace)
                    self.assertEqual(
                        [r.run_id for r in ledger.store.list_runs(trace_id=trace)], [message.metadata["run_id"]]
                    )
                finally:
                    ledger.close()

    async def test_failed_run_alert_and_record_carry_the_trace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = RunLedger(Path(tmp) / "ledger.db", retry_delays=())
            try:
                inbox, outbox = InMemoryInbox(), InMemoryOutbox()
                inbox.append(_event("evt_in", {"_trace_id": "trc_ext_01"}))
                worker = self._worker(inbox, outbox, ValueError("boom"), run_ledger=ledger.recorder)
                with self.assertRaises(ExecutorFailed):
                    await worker.process_inbox(INBOX)
                (alert,) = outbox.fetch_pending()
                self.assertEqual(alert.metadata["kind"], "run_failed")
                self._assert_trace(alert, "trc_ext_01")
                self.assertEqual(len(ledger.store.list_runs(trace_id="trc_ext_01")), 1)
            finally:
                ledger.close()

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


class TestForwardLineage(ForwardCase):
    def setUp(self) -> None:
        super().setUp()
        self.ledger = RecordingLedger()

    def lineage(self, *sources: str) -> list[tuple[object, object]]:
        return [(e.payload.get("trace_id"), e.payload.get("parent_event_id"))
                for e in self.inbox.all_events() if e.source in (sources or (FORWARD_SOURCE, RETURN_SOURCE))]

    def event(self, source: str) -> Event:
        (found,) = [e for e in self.inbox.all_events() if e.source == source]
        return found

    def traces(self) -> set[object]:
        # Every message outside and every Run in the ledger.
        assert self.ledger is not None
        found: set[object] = {entry.trace_id for entry in self.ledger.entries}
        for message in self.outbox.fetch_pending(limit=1000):
            found |= {message.metadata["_trace_id"], message.metadata[TASK_CONTEXT_KEY]["trace_id"]}
        return found

    def suspend(self, agent: str) -> None:
        self.agents[agent].script = lambda _e, _s, _c: Suspend(request_id="r1", prompt="ok?", session_checkpoint="cp")

    async def test_root_forward_and_return_form_one_chain(self) -> None:
        self.forward("steward", SDE, "part", return_to=MAIN)
        root = self.ingest()
        await self.run_inbox()
        hop = self.event(FORWARD_SOURCE)
        self.assertEqual(
            (hop.payload["trace_id"], hop.payload["parent_event_id"]), (root.event_id, root.event_id)
        )
        self.answer("sde", "part done")
        child = await self.run_inbox(SDE)
        back = self.event(RETURN_SOURCE)
        self.assertEqual(
            (back.payload["trace_id"], back.payload["parent_event_id"]), (root.event_id, hop.event_id)
        )
        self.answer("steward", "all done")
        last = await self.run_inbox()
        self.assertEqual([r.trace_id for r in child + last], [root.event_id] * 2)
        self.assertEqual(self.traces(), {root.event_id})

    async def test_external_trace_is_kept_on_every_hop(self) -> None:
        # sde hands its piece on whole, so the reviewer returns it: three hops, the limit.
        self.forward("steward", SDE, "part", return_to=MAIN)
        root = self.ingest(_trace_id="trc_ext_01")
        await self.run_inbox()
        self.forward("sde", SDE, "check it", target_agent="code-reviewer")
        self.answer("code-reviewer", "fine")
        while self.queued(SDE):
            await self.run_inbox(SDE)
        self.answer("steward", "all done")
        await self.run_inbox()
        chain = [e for e in self.inbox.all_events() if e.status != "dropped"]
        self.assertEqual([e.source for e in chain[1:]], [FORWARD_SOURCE, FORWARD_SOURCE, RETURN_SOURCE])
        self.assertEqual({e.payload["trace_id"] for e in chain[1:]}, {"trc_ext_01"})
        self.assertEqual([e.payload["parent_event_id"] for e in chain[1:]], [e.event_id for e in chain[:-1]])
        self.assertEqual(chain[0].payload, root.payload)
        self.assertEqual(len(self.sent()), 1)
        self.assertEqual(self.traces(), {"trc_ext_01"})

    async def test_direct_answer_of_the_forwarded_agent_carries_the_trace(self) -> None:
        self.forward("steward", SDE, "all of it")
        self.ingest(_trace_id="trc_ext_01")
        await self.run_inbox()
        self.answer("sde", "done")
        await self.run_inbox(SDE)
        self.assertEqual([m.content for m in self.sent()], ["done"])
        self.assertEqual(self.traces(), {"trc_ext_01"})

    async def test_pieces_of_one_run_share_the_trace_and_return_from_their_own_forward(self) -> None:
        def split(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(SDE, "code", return_to=MAIN)
            caps.forward_to_inbox(DOCS, "docs", return_to=MAIN)
            return Silent()

        self.agents["steward"].script = split
        root = self.ingest()
        await self.run_inbox()
        self.assertEqual(self.lineage(), [(root.event_id, root.event_id)] * 2)
        hops = {e.inbox_id: e.event_id for e in self.inbox.all_events() if e.source == FORWARD_SOURCE}
        for agent, inbox_id in (("sde", SDE), ("writer", DOCS)):
            self.answer(agent, "done")
            await self.run_inbox(inbox_id)
        self.assertEqual(self.lineage(RETURN_SOURCE), [(root.event_id, hops[SDE]), (root.event_id, hops[DOCS])])

    async def test_a_batch_forwards_from_its_last_traced_event(self) -> None:
        self.forward("steward", SDE, "x")
        events = [self.ingest(trace_id="t1"), self.ingest(_trace_id="t2"), self.ingest()]
        await self.run_inbox()
        self.assertEqual(self.lineage(), [("t2", events[1].event_id)])

    async def test_a_batch_without_a_trace_forwards_from_its_first_event(self) -> None:
        self.forward("steward", SDE, "x")
        first, _ = self.ingest(), self.ingest()
        await self.run_inbox()
        self.assertEqual(self.lineage(), [(first.event_id, first.event_id)])

    async def test_the_child_returns_into_its_chain_after_an_approval(self) -> None:
        self.forward("steward", SDE, "part", return_to=MAIN)
        root = self.ingest()
        await self.run_inbox()
        self.suspend("sde")
        await self.run_inbox(SDE)
        self.answer("sde", "done")
        self.ingest(SDE, key="feishu:1", approval_request_id="r1", approved=True)
        await self.run_inbox(SDE)
        self.assertEqual(self.lineage(RETURN_SOURCE), [(root.event_id, self.event(FORWARD_SOURCE).event_id)])
        self.assertEqual(self.traces(), {root.event_id})

    async def test_a_message_with_another_trace_does_not_move_an_owed_return(self) -> None:
        self.forward("steward", SDE, "part", return_to=MAIN)
        root = self.ingest()
        await self.run_inbox()
        self.suspend("sde")
        await self.run_inbox(SDE)
        self.answer("sde", "done")
        self.ingest(SDE, approval_request_id="r1", approved=True, trace_id="trc_approval")
        await self.run_inbox(SDE)
        self.assertEqual(self.lineage(RETURN_SOURCE), [(root.event_id, self.event(FORWARD_SOURCE).event_id)])

    async def test_a_forward_approved_later_still_descends_from_the_first_event(self) -> None:
        for trace in (None, "trc_ext_01"):
            with self.subTest(trace=trace), tempfile.TemporaryDirectory() as tmp:
                self.setUp()
                db = SQLiteDatabase(Path(tmp) / "t.db")
                try:
                    # The stored session is all that is left of the first Run when the approval comes.
                    self.sessions = SQLiteSessionStoreAdapter(db)  # type: ignore[assignment]
                    self.suspend("steward")
                    root = self.ingest(**({"_trace_id": trace} if trace else {}))
                    await self.run_inbox()
                    self.forward("steward", SDE, "x", return_to=MAIN)
                    self.ingest(approval_request_id="r1", approved=True)
                    await self.run_inbox()
                    self.assertEqual(self.lineage(), [(trace or root.event_id, root.event_id)])
                    self.assertEqual(self.traces(), {trace or root.event_id})
                finally:
                    db.close()

    async def test_a_new_message_after_the_chain_starts_its_own_trace(self) -> None:
        self.suspend("steward")
        self.ingest(_trace_id="trc_ext_01")
        await self.run_inbox()
        self.answer("steward", "done")
        self.ingest(approval_request_id="r1", approved=True)
        await self.run_inbox()
        self.forward("steward", SDE, "x")
        later = self.ingest()
        await self.run_inbox()
        self.assertEqual(self.lineage(), [(later.event_id, later.event_id)])

    async def test_a_held_return_keeps_its_lineage(self) -> None:
        self.forward("steward", SDE, "part", return_to=MAIN)
        root = self.ingest()
        await self.run_inbox()
        self.answer("sde", "done")
        self.gate("defer")
        (report,) = await self.run_inbox(SDE)
        assert isinstance(report.outcome, Suspend)
        self.ingest(SDE, key=None, approval_request_id=report.outcome.request_id, approved=True)
        decided = await self.run_inbox(SDE)
        self.assertEqual(self.lineage(RETURN_SOURCE), [(root.event_id, self.event(FORWARD_SOURCE).event_id)])
        self.assertEqual([r.trace_id for r in decided], [root.event_id])
        self.assertEqual(self.traces(), {root.event_id})

    async def test_failures_go_back_with_the_lineage(self) -> None:
        self.forward("steward", SDE, "part", return_to=MAIN)
        root = self.ingest()
        await self.run_inbox()
        self.crash("sde", "boom")
        with self.assertRaises(ExecutorFailed):
            await self.run_inbox(SDE)
        self.assertEqual(self.lineage(RETURN_SOURCE), [(root.event_id, self.event(FORWARD_SOURCE).event_id)])

    async def test_a_cancelled_return_names_the_chain_it_ends(self) -> None:
        self.forward("steward", SDE, "part", return_to=MAIN)
        root = self.ingest()
        await self.run_inbox()
        self.suspend("sde")
        (report,) = await self.run_inbox(SDE)
        session = self.sessions.get(report.session_id or "")
        assert session is not None
        close_session(session, self.sessions, self.inbox.append, cause="test")
        self.assertEqual(
            self.lineage(RETURN_CANCELLED_SOURCE), [(root.event_id, self.event(FORWARD_SOURCE).event_id)]
        )
        ended = await self.run_inbox()
        self.assertEqual([r.trace_id for r in ended], [root.event_id])
        self.assertEqual(self.traces(), {root.event_id})

    async def test_a_handed_over_task_reports_its_own_trace(self) -> None:
        # The message asking for the forward is in another trace than the task its session works on.
        self.answer("steward", "opened", delivery=DeliveryOutcome("pr_open"))
        self.ingest(event_type="work.ready", task_id="T1", trace_id="trc_task", hop_count=1)
        await self.run_inbox()
        self.forward("steward", SDE, "x")
        self.ingest(_trace_id="trc_ext_01")
        await self.run_inbox()
        hop = self.event(FORWARD_SOURCE)
        self.answer("sde", "done")
        await self.run_inbox(SDE)
        last = self.outbox.fetch_pending(limit=1000)[-1].metadata
        self.assertEqual((last["task_id"], last["trace_id"], last["hop_count"]), ("T1", "trc_task", 1))
        self.assertEqual((hop.payload["trace_id"], last["_trace_id"]), ("trc_ext_01", "trc_ext_01"))

    async def test_a_task_without_a_trace_gets_none_from_the_forward(self) -> None:
        self.forward("steward", SDE, "x")
        root = self.ingest(event_type="work.ready", task_id="T1")
        await self.run_inbox()
        self.answer("sde", "done")
        await self.run_inbox(SDE)
        last = self.outbox.fetch_pending(limit=1000)[-1].metadata
        self.assertEqual((last["task_id"], last.get("trace_id"), last["_trace_id"]), ("T1", None, root.event_id))

    async def test_a_task_forwarded_before_the_upgrade_keeps_its_trace(self) -> None:
        self.answer("sde", "done")
        self.inbox.append(Event(
            inbox_id=SDE, source=FORWARD_SOURCE, external_id="forward:old:0",
            payload={"text": "x", "hop_count": 1, "forwarded_from": MAIN, "task_id": "T1", "trace_id": "trc_task"},
        ))
        await self.run_inbox(SDE)
        self.assertEqual(self.outbox.fetch_pending(limit=1000)[-1].metadata["trace_id"], "trc_task")

    async def test_forwarded_events_stay_byte_identical_in_the_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = SQLiteDatabase(Path(tmp) / "t.db")
            try:
                self.inbox = SQLiteInboxAdapter(db)  # type: ignore[assignment]

                def stored() -> dict[str, bytes]:
                    with db.transaction() as conn:
                        return dict(conn.execute("SELECT event_id, CAST(payload_json AS BLOB) FROM event_inbox"))

                root = _event("evt_root", {"title": "标题", "n": {"b": 1, "a": [2]}}, correlation_key="feishu:1")
                self.inbox.append(root)
                before = stored()
                self.forward("steward", SDE, "part", return_to=MAIN)
                await self.run_inbox()
                after_forward = stored()
                self.assertEqual(len(after_forward), 2)
                self.assertEqual(after_forward["evt_root"], before["evt_root"])
                self.answer("sde", "done")
                await self.run_inbox(SDE)
                self.answer("steward", "all done")
                await self.run_inbox()
                final = stored()
                self.assertEqual(len(final), 3)
                self.assertEqual({k: final[k] for k in after_forward}, after_forward)
            finally:
                db.close()

    def test_the_spec_keeps_its_lineage_in_the_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = SQLiteDatabase(Path(tmp) / "t.db")
            try:
                store = SQLiteSessionStoreAdapter(db)
                spec = ForwardSpec(target_inbox=SDE, trace_id="t", parent_event_id="e")
                store.save(SessionRef(session_id="s", inbox_id=SDE, forward=spec))
                saved = store.get("s")
                assert saved is not None
                self.assertEqual(saved.forward, spec)
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
