from __future__ import annotations

import asyncio
import tempfile
import unittest
from collections.abc import Callable, Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.core.models import (
    CLOSE_SOURCE,
    FORWARD_SOURCE,
    INTERNAL_SOURCES,
    MAX_FORWARD_HOPS,
    RETURN_CANCELLED_SOURCE,
    RETURN_SOURCE,
    TASK_SYNC_KIND,
    Emit,
    Event,
    FailureRoute,
    ForwardRefused,
    ForwardSpec,
    HeldReturn,
    InboxBinding,
    InboxMatchRule,
    InboxRoute,
    PermissionDecision,
    RunLedgerEntry,
    RunOutcome,
    SessionRef,
    Silent,
    Suspend,
)
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine import EventHandler
from agent_runtime.engine.closing import close_session
from agent_runtime.engine.worker import AgentWorker, ExecutorFailed, RunReport
from tests.fakes import (
    InMemoryInbox,
    InMemoryLock,
    InMemoryMemoryStore,
    InMemoryOutbox,
    InMemoryPermission,
    InMemorySessionStore,
    InMemoryTimerStore,
)

MAIN = "proj:hibernaut"
SDE = "proj:hibernaut:sde"
DOCS = "proj:hibernaut:docs"
OTHER = "proj:other"

BINDINGS = {
    MAIN: InboxBinding(
        inbox=MAIN, default_agent="steward", agents=("steward", "code-reviewer", "ops"),
        rules=(
            InboxMatchRule(agent="ops", source="timer:ops"),
            InboxMatchRule(agent="code-reviewer", tags=("review",)),
        ),
    ),
    SDE: InboxBinding(inbox=f"{SDE}(:.*)?", default_agent="sde", agents=("sde", "code-reviewer")),
    DOCS: InboxBinding(inbox=DOCS, default_agent="writer"),
}


def routes(inbox_id: str) -> InboxRoute | None:
    if inbox_id == OTHER:
        return InboxRoute(project="other")
    if not inbox_id.startswith(MAIN):
        return None
    return InboxRoute(project="hibernaut", binding=BINDINGS.get(inbox_id), has_bindings=True)


Script = Callable[[Sequence[Event], SessionRef | None, RunCapabilities], RunOutcome]


class Agent:
    def __init__(self, agent_id: str, log: list[str]) -> None:
        self.executor_id = agent_id
        self.runs: list[tuple[list[Event], SessionRef | None]] = []
        self.script: Script = lambda _events, _session, _caps: Silent()
        self.delay = 0.0
        self._log = log

    async def execute(
        self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities
    ) -> RunOutcome:
        self.runs.append((list(events), session))
        self._log.append(f"start:{self.executor_id}:{events[0].inbox_id}")
        if self.delay:
            await asyncio.sleep(self.delay)
        outcome = self.script(events, session, caps)
        self._log.append(f"end:{self.executor_id}:{events[0].inbox_id}")
        return outcome


class Ledger:
    # Classifies every failure, so the worker alerts on it.
    def __init__(self) -> None:
        self.entries: list[RunLedgerEntry] = []

    def route_failure(self, executor_id: str, *, error: Any = None, telemetry: Any = None) -> FailureRoute:
        _ = (executor_id, error, telemetry)
        return FailureRoute("unknown", "execution")

    def record_run(self, entry: RunLedgerEntry) -> None:
        self.entries.append(entry)


class ForwardCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.log: list[str] = []
        self.agents = {name: Agent(name, self.log) for name in ("steward", "sde", "code-reviewer", "ops", "writer")}
        self.inbox = InMemoryInbox()
        self.outbox = InMemoryOutbox()
        self.sessions = InMemorySessionStore()
        self.lock = InMemoryLock()
        self.permissions = InMemoryPermission()
        self.woken: list[str] = []
        self.policy: Any = "defer"
        self.ledger: Ledger | None = None

    def worker(self, name: str = "w1", hold: Any = None) -> AgentWorker:
        return AgentWorker(
            worker_id=name, inbox=self.inbox, timers=InMemoryTimerStore(), memory=InMemoryMemoryStore(),
            permissions=self.permissions, outbox=self.outbox, lock=self.lock, sessions=self.sessions,
            executors=self.agents, project_default_executor="steward", global_default_executor="steward",
            routes=routes, on_forward=self.woken.append, new_event_policy=self.policy, max_retries=1,
            run_ledger=self.ledger, hold=hold,
        )

    def ingest(self, inbox_id: str = MAIN, *, source: str = "switchboard:feishu", key: str | None = "feishu:1",
               session_id: str | None = None, **payload: Any) -> Event:
        event, _ = self.inbox.append(Event(
            inbox_id=inbox_id, source=source, external_id=f"e{len(self.inbox.all_events())}", payload=payload,
            correlation_key=key, session_id=session_id,
        ))
        return event

    def gate(self, action: Any, reason: str = "ask first") -> None:
        self.permissions = InMemoryPermission({"inbox.forward": PermissionDecision(action, reason)})

    async def run_inbox(self, inbox_id: str = MAIN, hold: Any = None) -> list[RunReport]:
        return await self.worker(hold=hold).process_inbox(inbox_id)

    def sent(self, kind: str = "emit") -> list[Any]:
        return [m for m in self.outbox.fetch_pending(limit=1000) if m.metadata.get("kind") == kind]

    def outbound(self) -> str:
        return "\n".join(f"{m.content} {m.metadata}" for m in self.outbox.fetch_pending(limit=1000))

    def queued(self, inbox_id: str) -> list[Event]:
        return self.inbox.fetch_pending(inbox_id)

    def returned(self, inbox_id: str = MAIN) -> list[Event]:
        return [e for e in self.inbox.all_events()
                if e.inbox_id == inbox_id and e.source == RETURN_SOURCE and e.status != "dropped"]

    def crash(self, agent: str, text: str) -> None:
        def boom(_e: Sequence[Event], _s: SessionRef | None, _c: RunCapabilities) -> RunOutcome:
            raise RuntimeError(text)

        self.agents[agent].script = boom

    def forward(self, agent: str, *args: Any, outcome: RunOutcome | None = None, **kwargs: Any) -> None:
        def script(_events: Sequence[Event], _session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(*args, **kwargs)
            return outcome or Silent()

        self.agents[agent].script = script

    def answer(self, agent: str, text: str, **kwargs: Any) -> None:
        self.agents[agent].script = lambda _e, _s, _c: Emit(messages=(text,), **kwargs)

    async def refusal(self, *args: Any, agent: str = "steward", **kwargs: Any) -> ForwardRefused:
        found: list[ForwardRefused] = []

        def script(_events: Sequence[Event], _session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            try:
                caps.forward_to_inbox(*args, **kwargs)
            except ForwardRefused as exc:
                found.append(exc)
            return Silent()

        self.agents[agent].script = script
        await self.run_inbox()
        self.assertEqual(len(found), 1)
        return found[0]


class RoutingTests(ForwardCase):
    async def test_rule_bypasses_the_steward(self) -> None:
        self.ingest(source="timer:ops", key=None)
        self.ingest(key="feishu:2", tags=["review"])
        self.ingest(key="feishu:3")
        await self.run_inbox()
        self.assertEqual([len(self.agents[a].runs) for a in ("ops", "code-reviewer", "steward")], [1, 1, 1])

    async def test_github_labels_count_as_tags(self) -> None:
        self.ingest(key="github:a/b#1", issue={"labels": [{"name": "review"}]})
        await self.run_inbox()
        self.assertEqual(len(self.agents["code-reviewer"].runs), 1)

    async def test_session_stays_with_its_agent_over_a_rule(self) -> None:
        self.ingest()
        await self.run_inbox()
        self.ingest(source="timer:ops")
        await self.run_inbox()
        self.assertEqual((len(self.agents["steward"].runs), len(self.agents["ops"].runs)), (2, 0))

    async def test_payload_agent_id_wins_over_the_legacy_executor_id(self) -> None:
        self.ingest(agent_id="ops", executor_id="code-reviewer")
        self.ingest(key="feishu:2", executor_id="code-reviewer")
        await self.run_inbox()
        self.assertEqual((len(self.agents["ops"].runs), len(self.agents["code-reviewer"].runs)), (1, 1))
        self.assertEqual(self.sessions.list_sessions()[0].agent_id, "code-reviewer")

    async def test_agent_outside_the_whitelist_fails_the_event(self) -> None:
        for field in ("agent_id", "executor_id"):
            event = self.ingest(key=f"feishu:{field}", reply_channel="feishu", reply_target="oc_1", **{field: "sde"})
            reports = await self.run_inbox()
            self.assertEqual([e.status for e in self.inbox.all_events() if e.event_id == event.event_id], ["failed"])
            self.assertIn("not allowed", reports[0].rejected or "")
        self.assertEqual([a.runs for a in self.agents.values()], [[]] * 5)
        notice = self.sent("agent_refused")[0]
        self.assertIn("'sde' is not allowed", notice.content)
        self.assertEqual(
            (notice.metadata["outcome_kind"], notice.metadata["reply_channel"], notice.metadata["correlation_key"]),
            ("error", "feishu", "feishu:agent_id"),
        )
        self.assertEqual(notice.metadata["task_context"]["status"], "refused")
        self.assertEqual(self.sessions.list_sessions(), [])

    async def test_refusal_leaves_the_rest_of_the_claim_running(self) -> None:
        self.ingest(key="feishu:1", agent_id="sde")
        self.ingest(key="feishu:2")
        await self.run_inbox()
        self.assertEqual(len(self.agents["steward"].runs), 1)
        self.assertEqual(self.queued(MAIN), [])

    async def test_interrupt_policy_does_not_merge_keyless_events_of_different_agents(self) -> None:
        self.policy = "interrupt"
        self.ingest(source="timer:ops", key=None)
        self.ingest(key=None)
        await self.run_inbox()
        self.assertEqual((len(self.agents["ops"].runs), len(self.agents["steward"].runs)), (1, 1))


class ForwardTests(ForwardCase):
    async def test_cross_inbox_forward_answers_the_original_channel_directly(self) -> None:
        self.forward("steward", SDE, "fix the bug", title="Bug", tags=("bugfix",))
        self.ingest(reply_channel="feishu", reply_target="oc_1", text="please")
        reports = await self.run_inbox()
        self.assertEqual(self.woken, [SDE])
        self.assertTrue(reports[0].pending_elsewhere)
        [event] = self.queued(SDE)
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        self.assertEqual((event.source, event.correlation_key), (FORWARD_SOURCE, "feishu:1"))
        self.assertEqual(
            {k: event.payload[k] for k in ("text", "title", "tags", "hop_count", "forwarded_from", "forwarded_by",
                                          "parent_session_id", "reply_channel", "reply_target")},
            {"text": "fix the bug", "title": "Bug", "tags": ["bugfix"], "hop_count": 1, "forwarded_from": MAIN,
             "forwarded_by": "steward", "parent_session_id": steward.session_id, "reply_channel": "feishu",
             "reply_target": "oc_1"},
        )
        self.assertNotIn("return_to", event.payload)

        self.answer("sde", "done")
        child_report = (await self.run_inbox(SDE))[0]
        [message] = self.sent()
        self.assertEqual((message.inbox_id, message.content), (SDE, "done"))
        self.assertEqual(
            {k: message.metadata[k] for k in ("reply_channel", "reply_target", "correlation_key")},
            {"reply_channel": "feishu", "reply_target": "oc_1", "correlation_key": "feishu:1"},
        )
        self.assertFalse(child_report.pending_elsewhere)
        child = self.agents["sde"].runs[0][1]
        assert child is not None
        self.assertEqual(child.parent_session_id, steward.session_id)

    async def test_return_to_brings_the_result_back_to_the_steward(self) -> None:
        self.forward("steward", SDE, "part one", return_to=MAIN)
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        self.answer("sde", "part one done", status="done")
        child_report = (await self.run_inbox(SDE))[0]
        self.assertEqual(self.sent(), [])
        self.assertTrue(child_report.pending_elsewhere)
        self.assertEqual(self.woken, [SDE, MAIN])
        [event] = self.queued(MAIN)
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        self.assertEqual((event.source, event.session_id), (RETURN_SOURCE, steward.session_id))
        self.assertEqual(
            {k: event.payload[k] for k in ("text", "status", "hop_count", "forwarded_from", "forwarded_by",
                                          "parent_session_id")},
            {"text": "part one done", "status": "done", "hop_count": 2, "forwarded_from": SDE, "forwarded_by": "sde",
             "parent_session_id": steward.session_id},
        )
        self.answer("steward", "all done")
        await self.run_inbox()
        _events, session = self.agents["steward"].runs[1]
        assert session is not None
        self.assertEqual(session.session_id, steward.session_id)
        [message] = self.sent()
        self.assertEqual((message.content, message.metadata["reply_target"]), ("all done", "oc_1"))

    async def test_cross_inbox_agents_run_concurrently(self) -> None:
        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(SDE, "code", return_to=MAIN)
            caps.forward_to_inbox(DOCS, "docs", return_to=MAIN)
            return Silent()

        self.agents["steward"].script = script
        self.ingest()
        await self.run_inbox()
        for name in ("sde", "writer"):
            self.answer(name, f"{name} result")
            self.agents[name].delay = 0.05
        self.log.clear()
        await asyncio.gather(self.worker("w1").process_inbox(SDE), self.worker("w2").process_inbox(DOCS))
        self.assertEqual([entry.split(":")[0] for entry in self.log], ["start", "start", "end", "end"])
        self.assertEqual({e.payload["text"] for e in self.queued(MAIN)}, {"sde result", "writer result"})

    async def test_same_inbox_relay_shares_the_lock_and_the_work_key(self) -> None:
        self.forward("sde", SDE, "review my change", target_agent="code-reviewer")
        self.ingest(SDE, key="gdrive:42")
        self.agents["sde"].delay = self.agents["code-reviewer"].delay = 0.02
        # A second worker on the same inbox finds the lease taken, so the relay stays serial.
        results = await asyncio.gather(
            self.worker("w1").process_inbox(SDE), self.worker("w2").process_inbox(SDE)
        )
        self.assertEqual(sorted(len(r) for r in results), [0, 2])
        self.assertEqual([entry for entry in self.log if entry.startswith("start")],
                         [f"start:sde:{SDE}", f"start:code-reviewer:{SDE}"])
        self.assertEqual([entry.split(":")[0] for entry in self.log], ["start", "end", "start", "end"])
        sde = self.agents["sde"].runs[0][1]
        events, reviewer = self.agents["code-reviewer"].runs[0]
        assert sde is not None and reviewer is not None
        self.assertNotEqual(sde.session_id, reviewer.session_id)
        self.assertEqual((sde.work_key, reviewer.work_key), ("gdrive:42", "gdrive:42"))
        # The reviewer role finds the developer's worktree through review_of.
        self.assertEqual((events[0].payload["review_of"], events[0].payload["agent_id"]), ("gdrive:42", "code-reviewer"))

    async def test_relay_hands_back_to_the_first_agents_session(self) -> None:
        self.forward("sde", SDE, "review", target_agent="code-reviewer", return_to=SDE)
        self.ingest(SDE, key="gdrive:42")
        self.answer("code-reviewer", "LGTM")
        first = self.agents["sde"]
        original = first.script

        def once(events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            if events[0].source == RETURN_SOURCE:
                return Emit(messages=(f"reviewed: {events[0].payload['text']}",))
            return original(events, session, caps)

        first.script = once
        reports = await self.run_inbox(SDE)
        self.assertEqual(len(reports), 3)
        self.assertEqual([m.content for m in self.sent()], ["reviewed: LGTM"])
        self.assertEqual(first.runs[0][1].session_id, first.runs[1][1].session_id)  # type: ignore[union-attr]

    async def test_relayed_answer_reports_the_original_conversation(self) -> None:
        self.forward("sde", SDE, "review", target_agent="code-reviewer")
        self.ingest(SDE, key="gdrive:42", reply_channel="drive", reply_target="t")
        self.answer("code-reviewer", "LGTM")
        reports = await self.run_inbox(SDE)
        [message] = self.sent()
        self.assertEqual((message.metadata["correlation_key"], message.metadata["reply_target"]), ("gdrive:42", "t"))
        self.assertEqual([r.correlation_key for r in reports], ["gdrive:42", "gdrive:42"])

    async def test_several_forwards_to_one_inbox_get_their_own_sessions(self) -> None:
        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(SDE, "one", return_to=MAIN)
            caps.forward_to_inbox(SDE, "two", return_to=MAIN)
            return Silent()

        self.agents["steward"].script = script
        self.ingest()
        await self.run_inbox()
        await self.run_inbox(SDE)
        self.assertEqual([e[0].payload["text"] for e, _ in self.agents["sde"].runs], ["one", "two"])
        work_keys = {s.work_key for _, s in self.agents["sde"].runs if s is not None}
        self.assertEqual(len(work_keys), 2)

    async def test_a_later_forward_continues_the_childs_session(self) -> None:
        self.forward("steward", SDE, "work")
        self.ingest()
        await self.run_inbox()
        await self.run_inbox(SDE)
        self.ingest()
        await self.run_inbox()
        await self.run_inbox(SDE)
        first, second = (s for _, s in self.agents["sde"].runs)
        assert first is not None and second is not None
        self.assertEqual(first.session_id, second.session_id)

    async def test_agents_cannot_smuggle_routing_into_the_event(self) -> None:
        # Only the checked arguments shape the payload, so a forward cannot name a workdir, an approval or a hop.
        self.forward("steward", SDE, "x")
        self.ingest(work_key="wk", explicit_workdir="/tmp", approval_note="n")
        await self.run_inbox()
        [event] = self.queued(SDE)
        self.assertEqual(
            sorted(event.payload),
            ["forwarded_by", "forwarded_from", "hop_count", "origin_key", "parent_session_id", "text"],
        )

    async def test_closing_the_parent_closes_children_in_other_inboxes(self) -> None:
        self.forward("steward", SDE, "x")
        self.ingest()
        await self.run_inbox()
        await self.run_inbox(SDE)
        self.ingest(source="switchboard:github", action="close_session")
        await self.run_inbox()
        await self.run_inbox(SDE)
        self.assertEqual({s.status for s in self.sessions.list_sessions()}, {"closed"})


class CommitTests(ForwardCase):
    async def test_failed_attempts_forward_nothing_and_a_retry_forwards_once(self) -> None:
        attempts: list[int] = []

        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(SDE, "x")
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("boom")
            return Silent()

        self.agents["steward"].script = script
        self.ingest()
        await self.run_inbox()
        self.assertEqual((len(attempts), len(self.queued(SDE))), (2, 1))

    async def test_a_run_that_fails_forwards_nothing(self) -> None:
        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(SDE, "x")
            raise RuntimeError("boom")

        self.agents["steward"].script = script
        self.ingest()
        with self.assertRaises(ExecutorFailed):
            await self.run_inbox()
        self.assertEqual((self.queued(SDE), self.woken), ([], []))

    async def test_a_failed_or_refused_emit_forwards_nothing(self) -> None:
        for status in ("failed", "refused", "stopped"):
            self.forward("steward", SDE, "x", outcome=Emit(messages=("no",), status=status))  # type: ignore[arg-type]
            self.ingest(key=f"k:{status}")
            await self.run_inbox()
        self.assertEqual(self.queued(SDE), [])

    async def test_an_interrupted_run_forwards_nothing(self) -> None:
        self.policy = "interrupt"
        steward = self.agents["steward"]
        started = asyncio.Event()
        outer = self

        class Slow:
            executor_id = "steward"
            runs = 0

            async def execute(
                self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities
            ) -> RunOutcome:
                self.runs += 1
                if self.runs == 1:
                    caps.forward_to_inbox(SDE, "half")
                    started.set()
                    await asyncio.sleep(5)
                caps.forward_to_inbox(SDE, f"whole {len(events)}")
                return Silent()

        _ = steward
        self.agents["steward"] = Slow()  # type: ignore[assignment]
        self.ingest()

        async def more() -> None:
            await started.wait()
            outer.ingest()

        await asyncio.gather(self.run_inbox(), more())
        self.assertEqual([e.payload["text"] for e in self.queued(SDE)], ["whole 2"])

    async def test_a_rerun_after_a_crash_does_not_forward_twice(self) -> None:
        self.forward("steward", SDE, "x")
        event = self.ingest()
        await self.run_inbox()
        # The process died after appending the forward and before marking the batch done.
        self.inbox.requeue_processing([event.event_id])
        self.inbox._events = [  # pyright: ignore[reportPrivateUsage]
            e if e.event_id != event.event_id else Event(
                inbox_id=e.inbox_id, source=e.source, external_id=e.external_id, payload=e.payload,
                event_id=e.event_id, session_id=e.session_id, correlation_key=e.correlation_key,
            )
            for e in self.inbox.all_events()
        ]
        await self.run_inbox()
        self.assertEqual(len(self.agents["steward"].runs), 2)
        self.assertEqual(len(self.queued(SDE)), 1)

    async def test_an_explicit_replay_forwards_again(self) -> None:
        self.forward("steward", SDE, "x")
        event = self.ingest(agent_id="code-reviewer")
        self.inbox.mark_failed([event.event_id], "x")
        replayed = self.inbox.replay(event.event_id, executor_id_override="steward")
        self.assertNotEqual(replayed.payload.get("agent_id"), "code-reviewer")
        await self.run_inbox()
        self.inbox.replay(event.event_id, force=True)
        await self.run_inbox()
        self.assertEqual(len(self.queued(SDE)), 2)


class AntiLoopTests(ForwardCase):
    async def test_the_hop_limit_is_enforced_on_every_path(self) -> None:
        for hops in (MAX_FORWARD_HOPS, MAX_FORWARD_HOPS + 1, "3", None, True):
            self.ingest(source=FORWARD_SOURCE, key=f"k:{hops}", forwarded_from=SDE, hop_count=hops)
            refused = await self.refusal(SDE, "again")
            self.assertEqual(refused.action, "block")
            self.assertIn("hop", str(refused))
        self.assertEqual(self.queued(SDE), [])

    async def test_a_negative_hop_count_gains_nothing(self) -> None:
        self.ingest(source=FORWARD_SOURCE, forwarded_from=SDE, hop_count=-50)
        await self.refusal(SDE, "again")

    async def test_agents_forwarding_to_each_other_stop_at_the_limit(self) -> None:
        def to(inbox: str) -> Script:
            def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
                try:
                    caps.forward_to_inbox(inbox, "yours")
                except ForwardRefused as exc:
                    return Emit(messages=(str(exc),))
                return Silent()
            return script

        self.agents["sde"].script = to(DOCS)
        self.agents["writer"].script = to(SDE)
        self.ingest(SDE)
        for _ in range(MAX_FORWARD_HOPS + 3):
            for inbox in (SDE, DOCS):
                await self.run_inbox(inbox)
        self.assertEqual(len(self.agents["sde"].runs) + len(self.agents["writer"].runs), MAX_FORWARD_HOPS + 1)
        self.assertEqual(len(self.sent()), 1)

    async def test_hops_survive_a_suspend(self) -> None:
        self.ingest(source=FORWARD_SOURCE, forwarded_from=SDE, hop_count=MAX_FORWARD_HOPS)
        self.agents["steward"].script = lambda _e, _s, _c: Suspend("req1", "ok?", "{}")
        await self.run_inbox()
        self.ingest(key=None, approval_request_id="req1", approved=True, event_type="approval.decided")
        refused = await self.refusal(SDE, "again")
        self.assertIn("hop", str(refused))

    async def test_a_new_external_event_starts_a_new_chain(self) -> None:
        self.ingest(source=FORWARD_SOURCE, forwarded_from=SDE, hop_count=MAX_FORWARD_HOPS)
        await self.run_inbox()
        self.forward("steward", SDE, "fresh")
        self.ingest()
        await self.run_inbox()
        self.assertEqual([e.payload["hop_count"] for e in self.queued(SDE)], [1])

    async def test_self_loop_is_refused(self) -> None:
        self.ingest()
        self.assertIn("itself", str(await self.refusal(MAIN, "x", target_agent="steward")))
        self.ingest(key="feishu:2")
        # Without a target the inbox rules pick the agent, which is this one again.
        self.assertIn("itself", str(await self.refusal(MAIN, "x")))

    async def test_targets_outside_the_whitelist_or_project_are_refused(self) -> None:
        cases: list[tuple[tuple[str, str], dict[str, Any], str]] = [
            ((SDE, "x"), {"target_agent": "ops"}, "not allowed"),
            ((SDE, "x"), {"target_agent": "ghost"}, "not registered"),
            ((OTHER, "x"), {}, "project"),
            (("proj:hibernaut:ghost", "x"), {}, "no inbox binding"),
            (("user:main", "x"), {}, "project"),
            (("", "x"), {}, "target_inbox"),
            ((SDE, "  "), {}, "text"),
            ((SDE, "x"), {"return_to": OTHER}, "return_to"),
            ((SDE, "x"), {"return_to": SDE}, "return_to"),
        ]
        for index, (args, kwargs, message) in enumerate(cases):
            with self.subTest(message=message, args=args):
                self.ingest(key=f"k:{index}")
                refused = await self.refusal(*args, **kwargs)
                self.assertIn(message, str(refused))
                self.assertEqual(refused.action, "block")
        self.assertEqual(self.woken, [])


class PermissionTests(ForwardCase):
    async def test_every_forward_passes_the_permission_gate(self) -> None:
        seen: list[tuple[str, dict[str, Any]]] = []

        class Recording(InMemoryPermission):
            def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
                seen.append((tool_name, args))
                return super().check(tool_name, args)

        self.permissions = Recording()
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.answer("sde", "done")
        await self.run_inbox(SDE)
        self.assertEqual([name for name, _ in seen], ["inbox.forward", "inbox.forward"])
        self.assertEqual(
            {k: seen[0][1][k] for k in ("inbox_id", "agent_id", "target_inbox", "return_to", "hop_count", "text")},
            {"inbox_id": MAIN, "agent_id": "steward", "target_inbox": SDE, "return_to": MAIN, "hop_count": 1,
             "text": "x"},
        )

    async def test_block_refuses(self) -> None:
        self.permissions = InMemoryPermission({"inbox.forward": PermissionDecision("block", "not today")})
        self.ingest()
        refused = await self.refusal(SDE, "x")
        self.assertEqual((str(refused), refused.action), ("not today", "block"))

    async def test_defer_needs_the_approval_of_that_request(self) -> None:
        self.permissions = InMemoryPermission({"inbox.forward": PermissionDecision("defer", "ask")})
        self.ingest()
        self.assertEqual((await self.refusal(SDE, "x")).action, "defer")

        self.agents["steward"].script = lambda _e, _s, _c: Suspend("req1", "ok?", "{}")
        self.ingest()
        await self.run_inbox()
        self.ingest(key=None, approval_request_id="req1", approved=False, event_type="approval.decided")
        self.assertEqual((await self.refusal(SDE, "x", approved_request_id="req1")).action, "defer")

        self.agents["steward"].script = lambda _e, _s, _c: Suspend("req2", "ok?", "{}")
        self.ingest()
        await self.run_inbox()
        self.ingest(key=None, approval_request_id="req2", approved=True, event_type="approval.decided")
        self.assertEqual((await self.refusal(SDE, "x", approved_request_id="req1")).action, "defer")

        self.agents["steward"].script = lambda _e, _s, _c: Suspend("req3", "ok?", "{}")
        self.ingest()
        await self.run_inbox()
        self.forward("steward", SDE, "approved", approved_request_id="req3")
        self.ingest(key=None, approval_request_id="req3", approved=True, event_type="approval.decided")
        await self.run_inbox()
        self.assertEqual([e.payload["text"] for e in self.queued(SDE)], ["approved"])


class ReturnTests(ForwardCase):
    async def test_the_owed_return_survives_a_suspend(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.agents["sde"].script = lambda _e, _s, _c: Suspend("req1", "merge?", "{}")
        await self.run_inbox(SDE)
        # The approval card still goes outside: only a person can answer it.
        self.assertEqual([m.content for m in self.sent("approval_request")], ["merge?"])
        self.answer("sde", "merged")
        self.ingest(SDE, key=None, approval_request_id="req1", approved=True, event_type="approval.decided")
        await self.run_inbox(SDE)
        self.assertEqual([e.payload["text"] for e in self.queued(MAIN)], ["merged"])

    async def test_a_result_returns_once(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.answer("sde", "first")
        await self.run_inbox(SDE)
        self.ingest(SDE, key="feishu:1", text="thanks, one more thing")
        self.answer("sde", "second")
        await self.run_inbox(SDE)
        self.assertEqual([e.payload["text"] for e in self.queued(MAIN)], ["first"])
        self.assertEqual([m.content for m in self.sent()], ["second"])

    async def test_a_crashed_child_returns_its_failure(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()

        def boom(_e: Sequence[Event], _s: SessionRef | None, _c: RunCapabilities) -> RunOutcome:
            raise RuntimeError("no disk")

        self.agents["sde"].script = boom
        with self.assertRaises(ExecutorFailed):
            await self.run_inbox(SDE)
        [event] = self.queued(MAIN)
        self.assertEqual((event.source, event.payload["status"]), (RETURN_SOURCE, "failed"))
        self.assertIn("no disk", event.payload["text"])

    async def test_a_failure_that_is_owed_back_is_not_told_outside(self) -> None:
        self.ledger = Ledger()
        for action in ("allow", "defer", "block"):
            with self.subTest(action):
                self.permissions = InMemoryPermission()
                key = f"feishu:{action}"
                self.forward("steward", SDE, "x", return_to=MAIN)
                self.ingest(key=key, reply_channel="feishu", reply_target="oc_1")
                await self.run_inbox()
                self.permissions = InMemoryPermission({"inbox.forward": PermissionDecision(action, "policy")})
                self.crash("sde", "MARK-404 no disk")
                with self.assertRaises(ExecutorFailed):
                    await self.run_inbox(SDE)
                self.assertNotIn("MARK-404", self.outbound())
                self.assertEqual(self.sent("run_failed"), [])
                # The Run's own records keep it.
                self.assertIn("MARK-404", self.ledger.entries[-1].error_summary or "")
                if action == "allow":
                    self.assertIn("MARK-404", self.returned()[-1].payload["text"])
                    self.agents["steward"].script = lambda _e, _s, _c: Silent()
                    await self.run_inbox()

    async def test_a_failure_owed_to_nobody_is_still_told(self) -> None:
        self.ledger = Ledger()
        self.forward("steward", SDE, "x")
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        self.crash("sde", "no disk")
        with self.assertRaises(ExecutorFailed):
            await self.run_inbox(SDE)
        [alert] = self.sent("run_failed")
        self.assertEqual(alert.metadata["reply_target"], "oc_1")

    async def test_a_refusal_that_is_owed_back_is_not_told_outside(self) -> None:
        self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=MAIN, hop_count=1, agent_id="ops",
                    reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox(SDE)
        self.assertEqual([e.payload["status"] for e in self.returned()], ["refused"])
        self.assertEqual(self.outbound(), "")

    async def test_a_return_finds_the_session_up_the_chain(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.forward("sde", DOCS, "document it", return_to=MAIN)
        await self.run_inbox(SDE)
        self.answer("writer", "documented")
        await self.run_inbox(DOCS)
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        self.assertEqual([e.session_id for e in self.queued(MAIN)], [steward.session_id])


class InboxOwnershipTests(ForwardCase):
    # A worker holds the lease of one inbox, so it never touches a session of another.
    async def foreign(self) -> SessionRef:
        self.agents["sde"].script = lambda _e, _s, _c: Suspend("req1", "merge?", "{}")
        self.ingest(SDE, key="task:1")
        await self.run_inbox(SDE)
        session = self.sessions.find_by_request_id("req1")
        assert session is not None
        self.agents["sde"].runs.clear()
        return session

    async def test_a_close_in_one_inbox_does_not_reach_a_session_of_another(self) -> None:
        session = await self.foreign()
        for how, event in (
            ("session_id", {"session_id": session.session_id}),
            ("source", {"session_id": session.session_id, "source": "session:close"}),
            ("request", {"approval_request_id": "req1"}),
        ):
            with self.subTest(how):
                event = self.ingest(MAIN, key=None, action="close_session", **event)  # type: ignore[arg-type]
                await self.run_inbox()
                self.assertEqual(self.sessions.get(session.session_id), session)
                self.assertEqual([e.status for e in self.inbox.all_events() if e.event_id == event.event_id], ["failed"])

    async def test_a_close_naming_its_target_in_the_payload_does_not_either(self) -> None:
        session = await self.foreign()
        event, _ = self.inbox.append(Event(
            inbox_id=MAIN, source="test", external_id="c", payload={"action": "close_session",
                                                                    "session_id": session.session_id}))
        await self.run_inbox()
        self.assertEqual(self.sessions.get(session.session_id), session)
        self.assertEqual([e.status for e in self.inbox.all_events() if e.event_id == event.event_id], ["failed"])
        own, _ = self.inbox.append(Event(
            inbox_id=SDE, source="test", external_id="c", payload={"action": "close_session",
                                                                   "session_id": session.session_id}))
        await self.run_inbox(SDE)
        self.assertEqual(self.sessions.get(session.session_id).status, "closed")  # type: ignore[union-attr]

    async def test_hibernauts_own_close_request_goes_on_to_the_inbox_it_belongs_to(self) -> None:
        session = await self.foreign()
        for payload, session_id in (({}, session.session_id), ({"session_id": session.session_id}, None)):
            self.inbox.append(Event(inbox_id=MAIN, source=CLOSE_SOURCE, external_id=f"c{session_id}", payload=payload,
                                    session_id=session_id))
        self.assertTrue(self.lock.acquire(SDE, "busy", 60))
        await self.run_inbox()
        self.assertEqual(self.sessions.get(session.session_id), session)
        self.assertIn(SDE, self.woken)
        self.lock.release(SDE, "busy")
        await self.run_inbox(SDE)
        self.assertEqual(self.sessions.get(session.session_id).status, "closed")  # type: ignore[union-attr]

    async def test_a_message_in_one_inbox_does_not_run_a_session_of_another(self) -> None:
        session = await self.foreign()
        for fields in ({"session_id": session.session_id, "text": "hi"}, {"approval_request_id": "req1", "approved": True}):
            event = self.ingest(MAIN, key=None, **fields)  # type: ignore[arg-type]
            self.assertIsNone(AgentWorker.expected_executor_id(
                event, self.sessions, project_default="steward", global_default=None))
            await self.run_inbox()
            self.assertEqual(self.sessions.get(session.session_id), session)
            self.assertEqual([e.status for e in self.inbox.all_events() if e.event_id == event.event_id], ["failed"])
        self.assertEqual([a.runs for a in self.agents.values()], [[]] * 5)


class ClosedWorkTests(ForwardCase):
    # Work forwarded for a session ends when that session is closed, started or not.
    def close(self, session: SessionRef | None, inbox_id: str = MAIN) -> Event:
        assert session is not None
        return self.ingest(inbox_id, key=None, session_id=session.session_id, action="close_session")

    async def sent_out(self, *, run: bool = False, **kwargs: Any) -> SessionRef:
        self.forward("steward", SDE, "x", **kwargs)
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        self.answer("sde", "secret result")
        self.agents["steward"].script = lambda _e, _s, _c: Emit(messages=("secret summary",))
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        if run:
            await self.run_inbox(SDE)
        return steward

    def assert_nothing_said(self, runs: int = 0) -> None:
        self.assertEqual((self.sent(), self.queued(MAIN), self.queued(SDE)), ([], [], []))
        self.assertNotIn("secret", self.outbound())
        self.assertEqual((len(self.agents["sde"].runs), len(self.agents["steward"].runs)), (runs, 1))

    async def test_work_that_has_not_started_never_does(self) -> None:
        for back in (MAIN, None):
            with self.subTest(back):
                self.setUp()
                steward = await self.sent_out(return_to=back)
                [piece] = self.queued(SDE)
                self.close(steward)
                await self.run_inbox()
                self.assertIsNone(AgentWorker.expected_executor_id(
                    piece, self.sessions, project_default="steward", global_default=None))
                await self.run_inbox(SDE)
                await self.run_inbox()
                self.assert_nothing_said()
                self.assertEqual([s for s in self.sessions.list_sessions() if s.inbox_id == SDE], [])
                status = {e.event_id: e.status for e in self.inbox.all_events()}
                self.assertEqual(status[piece.event_id], "done")

    async def test_work_for_an_agent_of_the_same_inbox_neither(self) -> None:
        self.forward("steward", MAIN, "review", target_agent="code-reviewer", return_to=MAIN)
        self.ingest()

        def waits(event: Event) -> str | None:
            return "busy" if event.source == FORWARD_SOURCE else None

        await self.run_inbox(hold=waits)
        self.close(self.agents["steward"].runs[0][1])
        await self.run_inbox(hold=waits)
        await self.run_inbox()
        self.assertEqual((self.agents["code-reviewer"].runs, self.queued(MAIN), self.sent()), ([], [], []))

    async def test_a_delegation_running_in_the_same_inbox_closes_too(self) -> None:
        self.forward("steward", MAIN, "review", target_agent="code-reviewer", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.close(self.agents["steward"].runs[0][1])
        await self.run_inbox()
        reviewer = self.agents["code-reviewer"].runs[0][1]
        assert reviewer is not None
        self.assertEqual(self.sessions.get(reviewer.session_id).status, "closed")  # type: ignore[union-attr]

    async def test_work_handed_on_ends_with_the_one_who_asked_for_it(self) -> None:
        for closing in ("asker", "middle"):
            with self.subTest(closing):
                self.setUp()
                steward = await self.sent_out(return_to=MAIN)
                self.forward("sde", DOCS, "document it", return_to=MAIN)
                await self.run_inbox(SDE)
                self.answer("writer", "secret documents")
                if closing == "asker":
                    self.close(steward)
                    await self.run_inbox()
                else:
                    self.close(self.agents["sde"].runs[0][1], SDE)
                    await self.run_inbox(SDE)
                await self.run_inbox(DOCS)
                self.assertEqual(len(self.agents["writer"].runs), 0 if closing == "asker" else 1)
                self.assertEqual(len(self.returned()), 0 if closing == "asker" else 1)

    async def test_what_a_child_says_after_its_parent_closed_goes_nowhere(self) -> None:
        steward = await self.sent_out(return_to=MAIN)

        def late(_e: Sequence[Event], _s: SessionRef | None, _c: RunCapabilities) -> RunOutcome:
            current = self.sessions.get(steward.session_id)
            assert current is not None
            close_session(current, self.sessions, self.inbox.append, cause="c")
            return Emit(messages=("secret result",))

        self.agents["sde"].script = late
        await self.run_inbox(SDE)
        await self.run_inbox()
        self.assert_nothing_said(runs=1)
        self.assertEqual(self.returned(), [])

    async def test_a_result_already_queued_is_not_worked_on(self) -> None:
        for how in ("same claim", "closed first"):
            with self.subTest(how):
                self.setUp()
                steward = await self.sent_out(return_to=MAIN, run=how == "same claim")
                if how == "same claim":
                    self.close(steward)
                else:
                    await self.run_inbox(SDE)
                    await self.run_inbox(hold=lambda e: "busy" if e.source == RETURN_SOURCE else None)
                    self.close(steward)
                    await self.run_inbox(hold=lambda e: "busy" if e.source == RETURN_SOURCE else None)
                await self.run_inbox()
                await self.run_inbox(SDE)
                self.assert_nothing_said(runs=1)

    async def test_a_held_result_ends_with_the_session_that_asked(self) -> None:
        steward = await self.sent_out(return_to=MAIN)
        self.gate("defer")
        await self.run_inbox(SDE)
        [card] = self.sent("approval_request")
        self.close(steward)
        await self.run_inbox()
        decision = self.ingest(SDE, key=None, approval_request_id=card.metadata["approval_request_id"], approved=True,
                               event_type="approval.decided")
        await self.run_inbox(SDE)
        self.assertEqual(self.sessions.get(card.metadata["session_id"]).status, "closed")  # type: ignore[union-attr]
        self.assertEqual(self.queued(MAIN), [])
        await self.run_inbox(SDE)
        self.assertIn([e.status for e in self.inbox.all_events() if e.event_id == decision.event_id], (["failed"], ["done"]))
        self.assertNotIn("secret", "\n".join(m.content for m in self.sent()))

    async def test_a_session_that_went_on_under_another_id_is_the_same_one(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN, outcome=Emit(messages=("started",), session_id="second"))
        self.ingest(key=None, session_id="first")
        await self.run_inbox()
        self.answer("sde", "result")
        second = self.sessions.get("second")
        assert second is not None
        for closed in (False, True):
            with self.subTest(closed=closed):
                if closed:
                    self.close(second)
                    await self.run_inbox()
                    await self.run_inbox(SDE)
                    self.assertEqual(self.agents["sde"].runs, [])
                else:
                    # Nothing else of it is closed by what becomes of the first.
                    self.assertEqual(self.sessions.get("first").status, "archived")  # type: ignore[union-attr]

    async def test_a_session_at_work_again_is_not_closed_any_more(self) -> None:
        steward = await self.sent_out(return_to=MAIN)
        self.close(steward)
        await self.run_inbox()
        self.forward("steward", SDE, "again", return_to=MAIN)
        self.ingest(key=None, session_id=steward.session_id, text="reopen")
        await self.run_inbox()
        self.agents["steward"].script = lambda _e, _s, _c: Silent()
        self.agents["sde"].runs.clear()
        await self.run_inbox(SDE)
        # Only the new piece: the first one was ended.
        self.assertEqual([[e.payload["text"] for e in events] for events, _ in self.agents["sde"].runs], [["again"]])
        self.assertEqual(len(self.returned()), 1)

    async def test_a_replayed_piece_runs_although_it_was_ended(self) -> None:
        steward = await self.sent_out(return_to=MAIN)
        [piece] = self.queued(SDE)
        self.close(steward)
        await self.run_inbox()
        await self.run_inbox(SDE)
        self.inbox.replay(piece.event_id, force=True)
        await self.run_inbox(SDE)
        self.assertEqual((len(self.agents["sde"].runs), len(self.returned())), (1, 1))

    async def test_work_sent_by_a_session_that_is_finished_still_runs(self) -> None:
        # Finished by its own Run, not closed by anyone.
        self.forward("steward", SDE, "x", outcome=Emit(messages=("handed over",), close_session=True))
        self.ingest()
        await self.run_inbox()
        self.answer("sde", "did it")
        await self.run_inbox(SDE)
        self.assertEqual([m.content for m in self.sent()], ["handed over", "did it"])


    async def test_a_finish_after_it_was_opened_again_is_no_close(self) -> None:
        steward = await self.sent_out(return_to=MAIN)
        self.close(steward)
        await self.run_inbox()
        self.forward("steward", SDE, "again", outcome=Emit(messages=("handed over",), close_session=True))
        self.ingest(key=None, session_id=steward.session_id, text="reopen")
        await self.run_inbox()
        await self.run_inbox(SDE)
        self.assertEqual([[e.payload["text"] for e in events] for events, _ in self.agents["sde"].runs], [["again"]])

    async def test_a_relay_that_asks_for_no_result_goes_on_by_itself(self) -> None:
        self.forward("steward", MAIN, "review", target_agent="code-reviewer")
        self.ingest()

        def waits(event: Event) -> str | None:
            return "busy" if event.source == FORWARD_SOURCE else None

        await self.run_inbox(hold=waits)
        self.close(self.agents["steward"].runs[0][1])
        await self.run_inbox(hold=waits)
        self.answer("code-reviewer", "looks good")
        await self.run_inbox()
        self.assertEqual([m.content for m in self.sent()], ["looks good"])

    async def test_a_result_nobody_recorded_does_not_open_a_closed_session(self) -> None:
        # Sent out by a Run that crashed before its session was saved.
        steward = await self.sent_out(return_to=MAIN)
        self.close(steward)
        await self.run_inbox()
        self.ingest(source=RETURN_SOURCE, text="late", status="done", hop_count=2, delegation_id="unknown",
                    parent_session_id=steward.session_id, forwarded_from=SDE)
        await self.run_inbox()
        await self.run_inbox(SDE)
        self.assert_nothing_said()

    async def test_work_relayed_back_still_closes_with_the_session_it_came_from(self) -> None:
        steward = await self.sent_out()
        self.forward("sde", SDE, "review it", target_agent="code-reviewer")
        self.forward("code-reviewer", SDE, "fix it", target_agent="sde")
        await self.run_inbox(SDE, hold=lambda e: "busy" if e.payload.get("text") == "fix it" else None)
        self.agents["sde"].script = lambda _e, _s, _c: Silent()
        await self.run_inbox(SDE)
        first, second = (s for _, s in self.agents["sde"].runs)
        assert first is not None and second is not None
        self.assertEqual(first.session_id, second.session_id)
        self.close(steward)
        await self.run_inbox()
        await self.run_inbox(SDE)
        self.assertEqual(self.sessions.get(first.session_id).status, "closed")  # type: ignore[union-attr]

    async def test_a_held_result_is_not_handed_back_to_a_closed_session(self) -> None:
        steward = await self.sent_out(return_to=MAIN)
        self.gate("defer")
        await self.run_inbox(SDE)
        [card] = self.sent("approval_request")
        self.close(steward)
        await self.run_inbox()
        self.permissions = InMemoryPermission()
        self.ingest(SDE, key=None, approval_request_id=card.metadata["approval_request_id"], approved=True,
                    event_type="approval.decided")
        # Decided before its close reaches it.
        await self.run_inbox(SDE, hold=lambda e: "busy" if e.source == CLOSE_SOURCE else None)
        self.assertEqual(self.returned(), [])
        self.assertEqual(self.sessions.get(card.metadata["session_id"]).status, "closed")  # type: ignore[union-attr]

    async def test_work_that_is_folded_in_is_found_and_ended_work_is_not_folded(self) -> None:
        for closed in ("before", "after"):
            with self.subTest(closed):
                self.setUp()
                self.policy = "fold"
                self.sessions.save(SessionRef(session_id="B", inbox_id=DOCS, executor_id="writer"))
                folded: list[Event] = []

                def run(
                    _e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities, closed: str = closed,
                    folded: list[Event] = folded,
                ) -> RunOutcome:
                    asker = self.sessions.get("B")
                    assert asker is not None
                    if closed == "before":
                        close_session(asker, self.sessions, self.inbox.append, cause="c")
                    self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=DOCS, return_to=DOCS, hop_count=1, text="B",
                                delegation_id="piece-B", parent_session_id="B")
                    folded.extend(caps.poll_folded_events())
                    if closed == "after":
                        close_session(asker, self.sessions, self.inbox.append, cause="c")
                    return Emit(messages=("the answer",))

                self.agents["sde"].script = run
                self.ingest(SDE, text="hi")
                await self.run_inbox(SDE)
                self.assertEqual((len(folded), len(self.agents["sde"].runs)), (0 if closed == "before" else 1, 1))
                self.assertEqual(self.returned(DOCS), [])
                self.assertEqual([m.content for m in self.sent()], ["the answer"] if closed == "before" else [])
                session = self.agents["sde"].runs[0][1]
                assert session is not None
                found = self.sessions.get(session.session_id)
                assert found is not None
                self.assertEqual(found.status, "active" if closed == "before" else "closed")


class ReusedChildTests(ForwardCase):
    # A session that works for B now was first used by A.
    def setUp(self) -> None:
        super().setUp()
        for name, inbox_id in (("A", MAIN), ("B", DOCS)):
            self.sessions.save(SessionRef(session_id=name, inbox_id=inbox_id, executor_id="steward", forward=ForwardSpec(
                target_inbox=inbox_id, pending_returns=(f"piece-{name}",))))

    def piece(self, name: str) -> Event:
        inbox_id = {"A": MAIN, "B": DOCS}[name]
        return self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=inbox_id, return_to=inbox_id, hop_count=1,
                           text=name, delegation_id=f"piece-{name}", parent_session_id=name)

    async def close(self, name: str) -> None:
        inbox_id = {"A": MAIN, "B": DOCS}[name]
        self.ingest(inbox_id, key=None, session_id=name, action="close_session")
        await self.run_inbox(inbox_id)

    async def reused(self) -> str:
        self.answer("sde", "did A")
        self.piece("A")
        await self.run_inbox(SDE)
        self.agents["sde"].script = lambda _e, _s, _c: Silent()
        self.piece("B")
        await self.run_inbox(SDE)
        first, second = (s for _, s in self.agents["sde"].runs)
        assert first is not None and second is not None
        self.assertEqual(first.session_id, second.session_id)
        return first.session_id

    async def test_it_closes_with_the_one_it_works_for_now(self) -> None:
        child = await self.reused()
        await self.close("A")
        await self.run_inbox(SDE)
        self.assertEqual(self.sessions.get(child).status, "active")  # type: ignore[union-attr]
        await self.close("B")
        await self.run_inbox(SDE)
        self.assertEqual(self.sessions.get(child).status, "closed")  # type: ignore[union-attr]
        # Where it first came from is still on record.
        self.assertEqual(self.sessions.get(child).parent_session_id, "A")  # type: ignore[union-attr]

    async def test_a_close_claimed_with_the_next_piece_spares_it(self) -> None:
        for close_first in (True, False):
            with self.subTest(close_first=close_first):
                self.setUp()
                self.answer("sde", "did A")
                self.piece("A")
                await self.run_inbox(SDE)
                if close_first:
                    await self.close("A")
                self.piece("B")
                if not close_first:
                    await self.close("A")
                self.answer("sde", "did B")
                await self.run_inbox(SDE)
                self.assertEqual([e.payload["text"] for e in self.returned(DOCS)], ["did B"])

    async def test_a_close_that_arrives_after_the_next_piece_started_spares_it(self) -> None:
        self.answer("sde", "did A")
        self.piece("A")
        await self.run_inbox(SDE)
        await self.close("A")
        self.agents["sde"].script = lambda _e, _s, _c: Silent()
        self.piece("B")
        await self.run_inbox(SDE, hold=lambda e: "busy" if e.source == CLOSE_SOURCE else None)
        await self.run_inbox(SDE)
        self.assertEqual(self.queued(SDE), [])
        child = self.agents["sde"].runs[1][1]
        assert child is not None
        self.assertEqual(self.sessions.get(child.session_id).status, "active")  # type: ignore[union-attr]

    async def test_a_close_by_a_person_spares_a_piece_that_has_not_started(self) -> None:
        self.answer("sde", "did A")
        self.piece("A")
        await self.run_inbox(SDE)
        child = self.agents["sde"].runs[0][1]
        assert child is not None
        self.piece("B")
        self.ingest(SDE, key=None, session_id=child.session_id, action="close_session")
        self.answer("sde", "did B")
        await self.run_inbox(SDE)
        self.assertEqual([e.payload["text"] for e in self.returned(DOCS)], ["did B"])
        self.assertEqual(self.sessions.get(child.session_id).status, "closed")  # type: ignore[union-attr]

    async def test_it_is_found_while_it_runs(self) -> None:
        self.answer("sde", "did A")
        self.piece("A")
        await self.run_inbox(SDE)

        def run(_e: Sequence[Event], _s: SessionRef | None, _c: RunCapabilities) -> RunOutcome:
            current = self.sessions.get("B")
            assert current is not None
            close_session(current, self.sessions, self.inbox.append, cause="c")
            return Emit(messages=("secret B",))

        self.agents["sde"].script = run
        self.piece("B")
        await self.run_inbox(SDE)
        self.assertEqual(self.returned(DOCS), [])
        self.assertNotIn("secret", self.outbound())
        child = self.agents["sde"].runs[0][1]
        assert child is not None
        self.assertEqual(self.sessions.get(child.session_id).status, "closed")  # type: ignore[union-attr]

    async def test_a_held_result_of_the_earlier_piece_does_not_close_with_the_later_one(self) -> None:
        self.gate("defer")
        self.answer("sde", "did A")
        self.piece("A")
        await self.run_inbox(SDE)
        [card] = self.sent("approval_request")
        self.permissions = InMemoryPermission()
        self.agents["sde"].script = lambda _e, _s, _c: Silent()
        self.piece("B")
        await self.run_inbox(SDE)
        await self.close("B")
        await self.run_inbox(SDE)
        self.ingest(SDE, key=None, approval_request_id=card.metadata["approval_request_id"], approved=True,
                    event_type="approval.decided")
        await self.run_inbox(SDE)
        self.assertEqual([e.payload["text"] for e in self.returned()], ["did A"])

    async def test_a_piece_whose_parent_closes_between_the_claim_and_its_session_does_not_run(self) -> None:
        real = self.sessions.save

        def save(session: SessionRef) -> None:
            real(session)
            if session.inbox_id == SDE and session.status == "active":
                current = self.sessions.get("B")
                if current is not None and current.status == "active":
                    close_session(current, self.sessions, lambda _event: None, cause="c")

        self.piece("B")
        with patch.object(self.sessions, "save", save):
            await self.run_inbox(SDE)
        self.assertEqual(self.agents["sde"].runs, [])


class DelegationTests(ForwardCase):
    # Every piece of work sent out for a result is told apart from every other one, from the forward to its return.
    async def owed_twice(self, between: Script | None = None) -> None:
        for text in ("one", "two"):
            self.forward("steward", SDE, text, return_to=MAIN)
            self.ingest()
            await self.run_inbox()
            if between is not None and text == "one":
                self.agents["sde"].script = between
                await self.run_inbox(SDE)

    def echo(self, agent: str = "sde") -> None:
        self.agents[agent].script = lambda events, _s, _c: Emit(messages=(f"did {events[-1].payload['text']}",))

    async def test_changing_the_receipt_does_not_change_what_is_queued(self) -> None:
        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            tags = ["review"]
            receipt = caps.forward_to_inbox(SDE, "approved text", tags=tags, target_agent="sde", return_to=MAIN)
            receipt.payload.update(text="other text", agent_id="code-reviewer", hop_count=-5, return_to=OTHER)
            receipt.payload["tags"].append("bugfix")
            receipt.payload["activities"].append("release")
            return Silent()

        self.agents["steward"].script = script
        event = self.ingest(activities=["versioning"])
        await self.run_inbox()
        [queued] = self.queued(SDE)
        self.assertEqual(
            {k: queued.payload[k] for k in ("text", "agent_id", "hop_count", "return_to", "tags", "activities")},
            {"text": "approved text", "agent_id": "sde", "hop_count": 1, "return_to": MAIN, "tags": ["review"],
             "activities": ["versioning"]},
        )
        self.assertEqual(event.payload["activities"], ["versioning"])

    async def test_two_runs_that_each_send_a_piece_get_a_result_for_each(self) -> None:
        for policy in ("defer", "fold", "interrupt"):
            with self.subTest(policy):
                self.setUp()
                self.policy = policy
                await self.owed_twice()
                self.echo()
                self.agents["steward"].script = lambda _e, _s, _c: Silent()
                # The child claims only now, so both pieces are in one claim.
                await self.run_inbox(SDE)
                self.assertEqual([e.payload["text"] for e in self.returned()], ["did one", "did two"])
                waiting: list[bool] = []
                for event in self.returned():
                    others = [e.event_id for e in self.queued(MAIN) if e.event_id != event.event_id]
                    with patch.object(self.inbox, "claim_pending", self._without(others)):
                        waiting += [r.pending_elsewhere for r in await self.run_inbox()]
                self.assertEqual(waiting, [True, False])

    def _without(self, hidden: list[str]) -> Any:
        claim = self.inbox.claim_pending
        return lambda inbox_id, event_ids=None: claim(
            inbox_id, [e.event_id for e in self.queued(inbox_id) if e.event_id not in hidden])

    async def test_a_piece_sent_while_the_child_still_owes_another_is_its_own_task(self) -> None:
        for between in (Suspend("req1", "merge?", "{}"), Silent()):
            with self.subTest(between=between):
                self.setUp()
                await self.owed_twice(lambda _e, _s, _c, outcome=between: outcome)
                self.echo()
                # A crash between the Run and its events being marked done runs it again.
                with patch.object(self.inbox, "mark_done", side_effect=RuntimeError("crash")):
                    with self.assertRaises(RuntimeError):
                        await self.run_inbox(SDE)
                await self.run_inbox(SDE)
                self.assertEqual([e.payload["text"] for e in self.returned()], ["did two"])
                first, second = self.agents["sde"].runs[0][1], self.agents["sde"].runs[1][1]
                assert first is not None and second is not None
                self.assertNotEqual((first.session_id, first.work_key), (second.session_id, second.work_key))
                self.assertEqual([e.correlation_key for e in self.agents["sde"].runs[1][0]], [second.correlation_key])
                if isinstance(between, Suspend):
                    self.answer("sde", "did one")
                    self.ingest(SDE, key=None, approval_request_id="req1", approved=True, event_type="approval.decided")
                else:
                    self.answer("sde", "did one")
                    self.ingest(SDE, key="feishu:1", text="go on")
                await self.run_inbox(SDE)
                self.assertEqual([e.payload["text"] for e in self.returned()], ["did two", "did one"])
                self.agents["steward"].script = lambda _e, _s, _c: Silent()
                reports = await self.run_inbox()
                self.assertFalse(reports[-1].pending_elsewhere)

    async def test_work_that_owes_nothing_does_not_settle_a_piece_that_does(self) -> None:
        self.forward("steward", SDE, "owed", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.forward("steward", SDE, "not owed")
        self.ingest()
        await self.run_inbox()
        self.echo()
        await self.run_inbox(SDE)
        self.assertEqual([e.payload["text"] for e in self.returned()], ["did owed"])
        self.assertEqual([m.content for m in self.sent()], ["did not owed"])

    async def test_a_settled_piece_is_continued_in_the_childs_session(self) -> None:
        self.echo()
        for text in ("build it", "fix it"):
            self.forward("steward", SDE, text, return_to=MAIN)
            self.ingest(text=text)
            await self.run_inbox()
            await self.run_inbox(SDE)
            self.agents["steward"].script = lambda _e, _s, _c: Silent()
            await self.run_inbox()
        first, second = (s for _, s in self.agents["sde"].runs)
        assert first is not None and second is not None
        self.assertEqual(first.session_id, second.session_id)
        self.assertEqual([e.payload["text"] for e in self.returned()], ["did build it", "did fix it"])

    async def test_work_handed_back_inside_an_inbox_still_owes_its_result(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.forward("sde", SDE, "review it", target_agent="code-reviewer")
        hand_back = self.agents["sde"].script

        def developer(events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            if events[0].payload.get("forwarded_by") == "code-reviewer":
                return Emit(messages=("reworked",))
            return hand_back(events, session, caps)

        self.agents["sde"].script = developer
        self.forward("code-reviewer", SDE, "rework it", target_agent="sde")
        await self.run_inbox(SDE)
        first, second = (s for _, s in self.agents["sde"].runs)
        assert first is not None and second is not None
        self.assertEqual(first.session_id, second.session_id)
        self.assertEqual(self.sent(), [])
        # Out of hops by now: the result waits for a person instead of ending up outside.
        [card] = self.sent("approval_request")
        holder = self.sessions.get(card.metadata["session_id"])
        assert holder is not None and holder.forward is not None and holder.forward.held is not None
        self.assertEqual((holder.forward.return_to, holder.forward.held.payload["text"]), (MAIN, "reworked"))

    async def test_replaying_a_settled_piece_does_not_settle_the_one_the_session_owes_now(self) -> None:
        self.forward("steward", SDE, "one", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        [first] = self.queued(SDE)
        self.crash("sde", "no disk")
        with self.assertRaises(ExecutorFailed):
            await self.run_inbox(SDE)
        self.agents["steward"].script = lambda _e, _s, _c: Silent()
        await self.run_inbox()
        self.forward("steward", SDE, "two", return_to=MAIN)
        self.ingest(text="try the second one")
        await self.run_inbox()
        self.agents["sde"].script = lambda _e, _s, _c: Suspend("req1", "merge?", "{}")
        await self.run_inbox(SDE)
        self.inbox.replay(first.event_id, force=True)
        self.echo()
        await self.run_inbox(SDE)
        self.answer("sde", "did two")
        self.ingest(SDE, key=None, approval_request_id="req1", approved=True, event_type="approval.decided")
        await self.run_inbox(SDE)
        self.assertEqual([e.payload["text"] for e in self.returned()][1:], ["did one", "did two"])

    async def test_a_debt_passed_on_is_settled_by_whoever_answers_it(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.forward("sde", DOCS, "document it", return_to=MAIN)
        await self.run_inbox(SDE)
        self.answer("writer", "documented")
        await self.run_inbox(DOCS)
        self.answer("steward", "all done")
        reports = await self.run_inbox()
        self.assertFalse(reports[-1].pending_elsewhere)

    async def handed_on(self, outcome: RunOutcome | None = None) -> SessionRef:
        # The steward's piece, handed on by the developer to the writer.
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        self.forward("sde", DOCS, "document it", return_to=MAIN, outcome=outcome)
        await self.run_inbox(SDE)
        self.agents["steward"].script = lambda _e, _s, _c: Silent()
        self.answer("writer", "documented")
        middle = self.agents["sde"].runs[0][1]
        assert middle is not None
        return middle

    def cancels(self) -> list[Event]:
        return [e for e in self.inbox.all_events() if e.source == RETURN_CANCELLED_SOURCE and e.status != "dropped"]

    async def test_whoever_takes_a_piece_over_is_the_only_one_who_owes_it(self) -> None:
        middle = await self.handed_on()
        saved = self.sessions.get(middle.session_id)
        assert saved is not None and saved.forward is not None
        self.assertEqual((saved.forward.return_to, saved.forward.delegation_id), (None, None))
        # Closed before the writer answers: neither the piece nor the writer's session ends with it.
        self.ingest(SDE, key=None, session_id=middle.session_id, action="close_session")
        await self.run_inbox(SDE)
        await self.run_inbox(DOCS)
        self.assertEqual(self.cancels(), [])
        self.assertEqual([e.payload["text"] for e in self.returned()], ["documented"])
        self.answer("steward", "all done")
        [report] = await self.run_inbox()
        self.assertFalse(report.pending_elsewhere)
        self.assertEqual([m.content for m in self.sent()], ["all done"])

    async def test_closing_the_one_who_asked_ends_the_piece_with_whoever_has_it(self) -> None:
        await self.handed_on()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        self.agents["writer"].script = lambda _e, _s, _c: Silent()
        await self.run_inbox(DOCS)
        self.ingest(key=None, session_id=steward.session_id, action="close_session")
        await self.run_inbox()
        await self.run_inbox(DOCS)
        writer = self.agents["writer"].runs[0][1]
        assert writer is not None
        self.assertEqual(self.sessions.get(writer.session_id).status, "closed")  # type: ignore[union-attr]

    async def test_what_is_said_after_handing_a_piece_on_stays_inside(self) -> None:
        middle = await self.handed_on(Emit(messages=("secret progress",)))
        self.assertEqual(self.returned(), [])
        for outcome in (Silent(), Emit(messages=("secret timer",))):
            self.agents["sde"].script = lambda _e, _s, _c, outcome=outcome: outcome
            self.ingest(SDE, key=None, session_id=middle.session_id, source="timer:poll")
            [report] = await self.run_inbox(SDE)
            self.assertTrue(report.pending_elsewhere)
        await self.run_inbox(DOCS)
        self.assertEqual([e.payload["text"] for e in self.returned()], ["documented"])
        self.assertNotIn("secret", self.outbound())
        # A new message from outside is answered outside.
        self.answer("sde", "hello")
        self.ingest(SDE, key=None, session_id=middle.session_id, text="hi")
        await self.run_inbox(SDE)
        self.assertEqual([m.content for m in self.sent()], ["hello"])

    async def test_nothing_that_answers_outside_is_sent_out_after_handing_a_piece_on(self) -> None:
        for wanted in (
            {"target_inbox": f"{SDE}:b"}, {"target_inbox": MAIN}, {"target_inbox": SDE, "target_agent": "code-reviewer"},
            {"target_inbox": DOCS, "return_to": MAIN},
        ):
            with self.subTest(wanted):
                self.setUp()
                middle = await self.handed_on()
                refused: list[str] = []

                def run(
                    _e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities,
                    wanted: dict[str, Any] = wanted, refused: list[str] = refused,
                ) -> RunOutcome:
                    try:
                        caps.forward_to_inbox(text="secret more", **wanted)
                    except ForwardRefused as exc:
                        refused.append(str(exc))
                    return Silent()

                self.agents["sde"].script = run
                self.ingest(SDE, key=None, session_id=middle.session_id, source="timer:poll")
                before = len(self.inbox.all_events())
                with patch.dict(BINDINGS, {f"{SDE}:b": BINDINGS[SDE]}):
                    await self.run_inbox(SDE)
                self.assertIn("already handed on", "".join(refused))
                self.assertEqual(len(self.inbox.all_events()), before)
                # A new message from outside is new work.
                self.ingest(SDE, key=None, session_id=middle.session_id, text="hi")
                refused.clear()
                with patch.dict(BINDINGS, {f"{SDE}:b": BINDINGS[SDE]}):
                    await self.run_inbox(SDE)
                self.assertEqual(refused, [])

    async def test_work_another_session_waits_for_is_only_handed_on_whole(self) -> None:
        # Without return_to the receiver would answer outside, while this session still owes the result.
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        self.forward("sde", DOCS, "document it", outcome=Emit(messages=("secret progress",)))
        self.answer("writer", "secret documents")
        await self.run_inbox(SDE)
        await self.run_inbox(DOCS)
        [back] = self.returned()
        self.assertEqual((back.payload["text"], back.session_id), ("secret documents", steward.session_id))
        self.assertNotIn("secret", self.outbound())

    async def test_work_owed_goes_neither_to_the_inbox_that_waits_nor_back_to_a_third_one(self) -> None:
        for wanted, reason in (({"target_inbox": MAIN}, "end your turn with it"),
                               ({"target_inbox": DOCS, "return_to": f"{SDE}:b"}, "return_to")):
            with self.subTest(wanted):
                self.setUp()
                self.forward("steward", SDE, "x", return_to=MAIN)
                self.ingest()
                await self.run_inbox()
                refused: list[str] = []

                def run(
                    _e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities,
                    wanted: dict[str, Any] = wanted, refused: list[str] = refused,
                ) -> RunOutcome:
                    try:
                        caps.forward_to_inbox(text="more", **wanted)
                    except ForwardRefused as exc:
                        refused.append(str(exc))
                    return Silent()

                self.agents["sde"].script = run
                with patch.dict(BINDINGS, {f"{SDE}:b": BINDINGS[SDE]}):
                    await self.run_inbox(SDE)
                self.assertIn(reason, "".join(refused))

    async def test_a_failure_after_handing_a_piece_on_stays_inside(self) -> None:
        middle = await self.handed_on()
        self.crash("sde", "secret")
        timer = self.ingest(SDE, key=None, session_id=middle.session_id, source="timer:poll")
        with self.assertRaises(ExecutorFailed):
            await self.run_inbox(SDE)
        await self.run_inbox(DOCS)
        self.assertEqual([(e.payload["text"], e.payload["status"]) for e in self.returned()], [("documented", "done")])
        self.assertNotIn("secret", self.outbound())
        saved = self.sessions.get(middle.session_id)
        assert saved is not None and saved.forward is not None
        # So no channel ends the task for it either.
        self.assertIn(timer.event_id, saved.forward.owed_failures)

    async def test_a_suspended_session_that_handed_its_piece_on_answers_nobody_with_it(self) -> None:
        middle = await self.handed_on(Suspend("req1", "ok?", "{}"))
        self.answer("sde", "secret")
        self.ingest(SDE, key=None, session_id=middle.session_id, approval_request_id="req1", approved=True,
                    event_type="approval.decided")
        await self.run_inbox(SDE)
        await self.run_inbox(DOCS)
        self.assertEqual([e.payload["text"] for e in self.returned()], ["documented"])
        self.assertNotIn("secret", self.outbound())

    async def test_a_failed_run_hands_nothing_on(self) -> None:
        await self.handed_on(Emit(messages=("broken",), status="failed"))
        self.assertEqual(self.queued(DOCS), [])
        self.assertEqual([(e.payload["text"], e.payload["status"]) for e in self.returned()], [("broken", "failed")])

    async def test_a_piece_is_handed_on_once(self) -> None:
        for second in ({"target_inbox": f"{SDE}:b", "return_to": MAIN},
                       {"target_inbox": SDE, "target_agent": "code-reviewer"}):
            with self.subTest(second):
                self.setUp()
                self.forward("steward", SDE, "x", return_to=MAIN)
                self.ingest()
                await self.run_inbox()
                refused: list[str] = []

                def script(
                    _e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities,
                    second: dict[str, str] = second, refused: list[str] = refused,
                ) -> RunOutcome:
                    caps.forward_to_inbox(DOCS, "document it", return_to=MAIN)
                    try:
                        caps.forward_to_inbox(text="more", **second)  # type: ignore[arg-type]
                    except ForwardRefused as exc:
                        refused.append(str(exc))
                    return Silent()

                self.agents["sde"].script = script
                with patch.dict(BINDINGS, {f"{SDE}:b": BINDINGS[SDE]}):
                    await self.run_inbox(SDE)
                self.assertEqual(len(refused), 1)
                self.assertIn("already handed on", refused[0])
                self.assertEqual(len(self.queued(DOCS)), 1)

    async def test_a_result_of_a_piece_that_is_settled_is_not_worked_on_again(self) -> None:
        for how in ("returned", "cancelled"):
            with self.subTest(how):
                self.setUp()
                self.forward("steward", SDE, "x", return_to=MAIN)
                self.ingest()
                await self.run_inbox()
                self.agents["steward"].script = lambda _e, _s, _c: Silent()
                steward = self.agents["steward"].runs[0][1]
                assert steward is not None
                [piece] = self.queued(SDE)
                late = {"text": "late", "status": "done", "hop_count": 2, "delegation_id": piece.external_id,
                        "parent_session_id": steward.session_id, "forwarded_from": DOCS, "forwarded_by": "writer"}
                if how == "returned":
                    self.answer("sde", "first")
                    await self.run_inbox(SDE)
                else:
                    self.ingest(source=RETURN_CANCELLED_SOURCE, key=None, session_id=steward.session_id,
                                delegation_id=piece.external_id, reason="gone", forwarded_from=SDE, forwarded_by="sde")
                await self.run_inbox()
                runs = len(self.agents["steward"].runs)
                for session_id, key in ((steward.session_id, None), (None, "feishu:1")):
                    event = self.ingest(source=RETURN_SOURCE, key=key, session_id=session_id, **late)
                    self.assertIsNone(AgentWorker.expected_executor_id(
                        event, self.sessions, project_default="steward", global_default=None))
                    await self.run_inbox()
                    self.assertEqual([e.status for e in self.inbox.all_events() if e.event_id == event.event_id], ["done"])
                self.assertEqual(len(self.agents["steward"].runs), runs)
                # An operator who replays one means it.
                self.ingest(source=RETURN_SOURCE, key=None, session_id=steward.session_id, _replay_id="r1", **late)
                await self.run_inbox()
                self.assertEqual(len(self.agents["steward"].runs), runs + 1)

    async def test_the_order_of_arrival_decides_between_a_result_and_its_cancel(self) -> None:
        # Not the time they were made: it can be the same, or the other way round.
        for times in (("t1", "t1"), ("t2", "t1")):
            for first in (RETURN_SOURCE, RETURN_CANCELLED_SOURCE):
                for held in (False, True):
                    with self.subTest(times=times, first=first, held=held):
                        self.setUp()
                        self.forward("steward", SDE, "x", return_to=MAIN)
                        self.ingest(reply_channel="feishu", reply_target="oc_1")
                        await self.run_inbox()
                        steward = self.agents["steward"].runs[0][1]
                        assert steward is not None
                        [piece] = self.queued(SDE)
                        self.answer("steward", "all done")
                        payloads: dict[str, dict[str, Any]] = {
                            RETURN_SOURCE: {"text": "result", "status": "done", "hop_count": 2},
                            RETURN_CANCELLED_SOURCE: {"reason": "gone", "forwarded_by": "sde"},
                        }
                        second = next(source for source in payloads if source != first)
                        for source, at in zip((first, second), times, strict=True):
                            self.inbox.append(Event(
                                inbox_id=MAIN, source=source, external_id="x", session_id=steward.session_id,
                                occurred_at=at, payload={**payloads[source], "delegation_id": piece.external_id,
                                                         "forwarded_from": SDE},
                            ))
                        if held:
                            await self.run_inbox(hold=lambda e: "busy" if e.source == RETURN_SOURCE else None)
                        await self.run_inbox()
                        result = first == RETURN_SOURCE
                        self.assertEqual([m.content for m in self.sent()], ["all done"] if result else [])
                        self.assertEqual(len(self.sent("return_withheld")), 0 if result else 1)

    async def test_a_result_queued_before_its_cancel_counts(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        [piece] = self.queued(SDE)
        self.answer("steward", "all done")
        self.answer("sde", "result")
        await self.run_inbox(SDE)
        self.ingest(source=RETURN_CANCELLED_SOURCE, key=None, session_id=steward.session_id,
                    delegation_id=piece.external_id, reason="gone", forwarded_from=SDE, forwarded_by="sde")
        await self.run_inbox()
        self.assertEqual([m.content for m in self.sent()], ["all done"])
        self.assertEqual(self.sent("return_withheld"), [])

    async def test_a_relay_takes_over_what_its_session_owes(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        [piece] = self.queued(SDE)
        self.forward("sde", SDE, "review it", target_agent="code-reviewer")
        self.answer("code-reviewer", "review: secret finding")
        reports = await self.run_inbox(SDE)
        self.assertEqual([r.pending_elsewhere for r in reports], [True, True])
        [back] = self.returned()
        self.assertEqual(
            (back.payload["text"], back.payload["delegation_id"], back.session_id),
            ("review: secret finding", piece.external_id, steward.session_id),
        )
        self.assertNotIn("secret", self.outbound())
        self.answer("steward", "all done")
        [report] = await self.run_inbox()
        self.assertFalse(report.pending_elsewhere)
        for session in self.sessions.list_sessions():
            if session.inbox_id == SDE:
                self.ingest(SDE, key=None, session_id=session.session_id, action="close_session")
        await self.run_inbox(SDE)
        self.assertEqual(self.cancels(), [])

    async def test_a_relay_inside_the_inbox_that_waits_answers_the_one_who_asked(self) -> None:
        # steward -> code-reviewer (result wanted in the same inbox) -> ops, which takes the piece over.
        self.forward("steward", MAIN, "review", target_agent="code-reviewer", return_to=MAIN)
        self.forward("code-reviewer", MAIN, "check it", target_agent="ops")
        self.answer("ops", "checked")
        first = self.agents["steward"].script

        def steward(events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            if events[0].source == RETURN_SOURCE:
                return Emit(messages=(f"all done: {events[0].payload['text']}",))
            return first(events, session, caps)

        self.agents["steward"].script = steward
        self.ingest()
        reports = await self.run_inbox()
        self.assertEqual([m.content for m in self.sent()], ["all done: checked"])
        self.assertEqual([r.pending_elsewhere for r in reports], [True, True, True, False])
        runs = self.agents["steward"].runs
        self.assertEqual(runs[0][1].session_id, runs[1][1].session_id)  # type: ignore[union-attr]
        reviewer = self.sessions.get(self.agents["code-reviewer"].runs[0][1].session_id)  # type: ignore[union-attr]
        self.assertEqual(reviewer.forward.pending_returns, ())  # type: ignore[union-attr]

    async def test_a_piece_is_not_handed_to_the_session_that_waits_for_it(self) -> None:
        self.forward("steward", MAIN, "review", target_agent="code-reviewer", return_to=MAIN)
        refused: list[str] = []

        def reviewer(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            with self.assertRaises(ForwardRefused) as caught:
                caps.forward_to_inbox(MAIN, "done", target_agent="steward")
            refused.append(str(caught.exception))
            self.answer("steward", "all done")
            return Emit(messages=("looks good",))

        self.agents["code-reviewer"].script = reviewer
        self.ingest()
        await self.run_inbox()
        [reason] = refused
        self.assertIn("end your turn with it", reason)
        self.assertEqual([e.payload["text"] for e in self.returned()], ["looks good"])

    async def test_the_result_of_a_replayed_piece_goes_back_again(self) -> None:
        for handed_on in (False, True):
            with self.subTest(handed_on=handed_on):
                self.setUp()
                self.forward("steward", SDE, "x", return_to=MAIN)
                self.ingest()
                await self.run_inbox()
                [piece] = self.queued(SDE)

                def fail(_e: Sequence[Event], _s: SessionRef | None, _c: RunCapabilities) -> RunOutcome:
                    raise RuntimeError("boom")

                self.agents["sde"].script = fail
                with self.assertRaises(ExecutorFailed):
                    await self.run_inbox(SDE)
                heard: list[object] = []

                def steward(events: Sequence[Event], _s: SessionRef | None, _c: RunCapabilities) -> RunOutcome:
                    heard.extend(e.payload["status"] for e in events)
                    return Silent()

                self.agents["steward"].script = steward
                await self.run_inbox()
                self.assertEqual(heard, ["failed"])
                self.inbox.replay(piece.event_id)
                # Not even a decision in between makes it an ordinary late result.
                self.agents["sde"].script = lambda _e, _s, _c: Suspend("req1", "ok?", "{}")
                await self.run_inbox(SDE)
                if handed_on:
                    self.forward("sde", SDE, "review it", target_agent="code-reviewer")
                    self.answer("code-reviewer", "fixed")
                else:
                    self.answer("sde", "fixed")
                self.ingest(SDE, key=None, approval_request_id="req1", approved=True, event_type="approval.decided")
                await self.run_inbox(SDE)
                await self.run_inbox()
                self.assertEqual(heard, ["failed", "done"])

    async def test_a_result_held_back_from_the_claim_still_beats_its_cancel(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        [piece] = self.queued(SDE)
        self.answer("steward", "all done")
        self.answer("sde", "result")
        await self.run_inbox(SDE)
        self.ingest(source=RETURN_CANCELLED_SOURCE, key=None, session_id=steward.session_id,
                    delegation_id=piece.external_id, reason="gone", forwarded_from=SDE, forwarded_by="sde")
        await self.run_inbox(hold=lambda evt: "busy" if evt.source == RETURN_SOURCE else None)
        self.assertEqual(self.sent("return_withheld"), [])
        await self.run_inbox()
        self.assertEqual([m.content for m in self.sent()], ["all done"])

    async def test_an_ended_wait_does_not_end_work_that_was_handed_on(self) -> None:
        # It waits for a part of its own while the writer has the piece: nobody is told that the task ended.
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()

        def both(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(DOCS, "part", return_to=SDE)
            caps.forward_to_inbox(DOCS, "the rest", return_to=MAIN)
            return Silent()

        self.agents["sde"].script = both
        await self.run_inbox(SDE)
        part = next(e for e in self.queued(DOCS) if e.payload["text"] == "part")
        middle = self.agents["sde"].runs[0][1]
        assert middle is not None
        self.ingest(SDE, source=RETURN_CANCELLED_SOURCE, key=None, session_id=middle.session_id,
                    delegation_id=part.external_id, reason="gone", forwarded_from=DOCS, forwarded_by="writer")
        await self.run_inbox(SDE)
        self.assertEqual((self.sent("return_withheld"), self.returned()), ([], []))

    async def test_a_result_settled_meanwhile_is_not_folded(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        [piece] = self.queued(SDE)
        late = {"text": "late", "status": "done", "hop_count": 2, "delegation_id": piece.external_id,
                "parent_session_id": steward.session_id, "forwarded_from": DOCS, "forwarded_by": "writer"}
        folded: list[Event] = []
        self.policy = "fold"

        def run(events: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            if events[0].payload.get("text") != "late":
                self.ingest(source=RETURN_SOURCE, key=None, session_id=steward.session_id, **late)
                self.ingest(key=None, session_id=steward.session_id, text="late")
                folded.extend(caps.poll_folded_events())
            return Silent()

        self.agents["steward"].script = run
        self.answer("sde", "first")
        await self.run_inbox(SDE)
        # Folded into the Run that settles the piece, then into one that comes after it.
        await self.run_inbox()
        self.ingest(key=None, session_id=steward.session_id, text="next")
        await self.run_inbox()
        self.assertEqual([e.source for e in folded], ["switchboard:feishu"] * 2)
        self.assertEqual(len(self.agents["steward"].runs), 3)

    async def test_two_results_of_one_piece_claimed_together_are_one(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        [piece] = self.queued(SDE)
        back = {"status": "done", "hop_count": 2, "delegation_id": piece.external_id, "forwarded_from": DOCS}
        for text in ("first", "second"):
            self.ingest(source=RETURN_SOURCE, key=None, session_id=steward.session_id, text=text, **back)
        heard: list[object] = []

        def run(events: Sequence[Event], _s: SessionRef | None, _c: RunCapabilities) -> RunOutcome:
            heard.extend(e.payload["text"] for e in events)
            return Silent()

        self.agents["steward"].script = run
        await self.run_inbox()
        self.assertEqual(heard, ["first"])

    async def test_a_cancel_queued_before_a_result_ends_the_piece(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        [piece] = self.queued(SDE)
        self.ingest(source=RETURN_CANCELLED_SOURCE, key=None, session_id=steward.session_id,
                    delegation_id=piece.external_id, reason="gone", forwarded_from=SDE, forwarded_by="sde")
        self.ingest(source=RETURN_SOURCE, key=None, session_id=steward.session_id, text="late", status="done",
                    hop_count=2, delegation_id=piece.external_id, forwarded_from=SDE)
        await self.run_inbox()
        self.assertEqual((len(self.sent("return_withheld")), len(self.agents["steward"].runs)), (1, 1))

    async def test_a_result_is_worked_on_again_after_a_crash_before_it_was_marked(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.answer("sde", "result")
        await self.run_inbox(SDE)
        self.agents["steward"].script = lambda _e, _s, _c: Silent()
        with patch.object(self.inbox, "mark_done", side_effect=OSError("disk")), self.assertRaises(OSError):
            await self.run_inbox()
        await self.run_inbox()
        self.assertEqual([events[0].source for events, _ in self.agents["steward"].runs[1:]], [RETURN_SOURCE] * 2)

    async def test_a_task_goes_with_the_piece_that_is_handed_on(self) -> None:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        self.ingest(SDE, key="feishu:1", event_type="work.ready", task_id="t9", hop_count=1)
        self.forward("sde", SDE, "review it", target_agent="code-reviewer")
        self.answer("code-reviewer", "fine")
        await self.run_inbox(SDE)
        done = [(m.metadata["task_action"], m.metadata.get("stage"), m.metadata["session_id"])
                for m in self.outbox.fetch_pending(limit=1000) if m.metadata.get("task_id") == "t9"]
        sde, reviewer = (self.agents[a].runs[0][1].session_id for a in ("sde", "code-reviewer"))  # type: ignore[union-attr]
        self.assertEqual([d for d in done if d[0] != "claim"],
                         [("update_stage", "forwarded", sde), ("complete", None, reviewer)])

    async def test_a_settled_chain_does_not_shorten_the_next_one(self) -> None:
        self.gate("block")
        self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=MAIN, hop_count=MAX_FORWARD_HOPS,
                    text="one", delegation_id="one")
        self.answer("sde", "did one")
        await self.run_inbox(SDE)
        [card] = self.sent("approval_request")
        self.ingest(SDE, key=None, approval_request_id=card.metadata["approval_request_id"], approved=True,
                    event_type="approval.decided")
        await self.run_inbox(SDE)
        self.permissions = InMemoryPermission()
        self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=MAIN, hop_count=1, text="two",
                    delegation_id="two")
        self.answer("sde", "did two")
        await self.run_inbox(SDE)
        first, second = (s for _, s in self.agents["sde"].runs)
        assert first is not None and second is not None
        self.assertEqual(first.session_id, second.session_id)
        [back] = self.returned()
        self.assertEqual((back.payload["text"], back.payload["hop_count"]), ("did two", 2))

    async def test_a_session_that_waits_for_one_chain_does_not_take_a_piece_of_another(self) -> None:
        # It has answered the first piece, but a part it sent out for it is still away, with that chain's hops.
        def first(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(DOCS, "document it", return_to=SDE)
            return Emit(messages=("did one",))

        self.agents["sde"].script = first
        self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=MAIN, hop_count=1, text="one",
                    delegation_id="one")
        await self.run_inbox(SDE)
        self.agents["sde"].script = lambda _e, _s, _c: Suspend("req1", "ok?", "{}")
        self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=MAIN, hop_count=1, text="two",
                    delegation_id="two")
        await self.run_inbox(SDE)
        self.answer("writer", "documented")
        await self.run_inbox(DOCS)
        self.agents["sde"].script = lambda _e, _s, _c: Silent()
        await self.run_inbox(SDE)
        self.answer("sde", "did two")
        self.ingest(SDE, key=None, approval_request_id="req1", approved=True, event_type="approval.decided")
        await self.run_inbox(SDE)
        self.assertEqual([(e.payload["text"], e.payload["hop_count"]) for e in self.returned()],
                         [("did one", 2), ("did two", 2)])

    async def test_a_refused_follow_up_leaves_the_piece_owed(self) -> None:
        def split(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(SDE, "code", return_to=MAIN)
            caps.forward_to_inbox(DOCS, "docs", return_to=MAIN)
            return Silent()

        for ending in ("emit", "close"):
            with self.subTest(ending):
                self.setUp()
                self.agents["steward"].script = split
                self.ingest(reply_channel="feishu", reply_target="oc_1")
                await self.run_inbox()
                self.agents["sde"].script = lambda _e, _s, _c: Silent()
                await self.run_inbox(SDE)
                self.ingest(SDE, key="feishu:1", text="do it as ops", agent_id="ops", reply_channel="feishu",
                            reply_target="oc_2")
                [refused] = await self.run_inbox(SDE)
                # Only that message is refused, where it came from: the piece itself goes on.
                self.assertTrue(refused.rejected and refused.pending_elsewhere)
                self.assertEqual(self.returned(), [])
                [notice] = self.sent("agent_refused")
                self.assertEqual(notice.metadata["reply_target"], "oc_2")
                self.answer("sde", "code done")
                self.ingest(SDE, key="feishu:1", text="go on",
                            **({"action": "close_session"} if ending == "close" else {}))
                await self.run_inbox(SDE)
                self.assertEqual([e.payload["status"] for e in self.returned()], ["done"] if ending == "emit" else [])
                self.agents["steward"].script = lambda _e, _s, _c: Silent()
                reports = await self.run_inbox()
                # The docs are still awaited.
                self.assertEqual([r.pending_elsewhere for r in reports], [True])

    async def test_a_refused_piece_is_settled_once(self) -> None:
        self.echo()
        self.forward("steward", SDE, "one", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        await self.run_inbox(SDE)
        self.agents["steward"].script = lambda _e, _s, _c: Silent()
        await self.run_inbox()
        # The whitelist changes after the second piece was sent to the same conversation.
        self.forward("steward", SDE, "two", return_to=MAIN, target_agent="code-reviewer")
        self.ingest()
        await self.run_inbox()
        self.assertEqual([e.correlation_key for e in self.queued(SDE)], ["feishu:1"])
        narrowed = replace(BINDINGS[SDE], agents=("sde",))
        with patch.dict(BINDINGS, {SDE: narrowed}):
            await self.run_inbox(SDE)
        self.ingest(SDE, key="feishu:1", text="and now?")
        await self.run_inbox(SDE)
        self.ingest(SDE, key="feishu:1", action="close_session")
        await self.run_inbox(SDE)
        self.assertEqual([e.payload["status"] for e in self.returned()], ["done", "refused"])
        self.assertEqual([e for e in self.queued(MAIN) if e.source == RETURN_CANCELLED_SOURCE], [])
        self.assertEqual([m.content for m in self.sent()], ["did and now?"])


    async def test_a_started_piece_that_is_then_refused_is_settled_once(self) -> None:
        self.forward("steward", SDE, "one", return_to=MAIN, target_agent="code-reviewer")
        self.ingest()
        await self.run_inbox()
        # The Run did not get to its result, and by the next one its agent is no longer allowed.
        with patch.object(self.inbox, "mark_done", side_effect=RuntimeError("crash")), self.assertRaises(RuntimeError):
            await self.run_inbox(SDE)
        with patch.dict(BINDINGS, {SDE: replace(BINDINGS[SDE], agents=("sde",))}):
            # Nor does a crash while refusing it refuse it twice.
            with patch.object(self.inbox, "mark_failed", side_effect=RuntimeError("crash")):
                with self.assertRaises(RuntimeError):
                    await self.run_inbox(SDE)
            await self.run_inbox(SDE)
        self.answer("code-reviewer", "late answer")
        self.ingest(SDE, key="feishu:1", text="and now?")
        await self.run_inbox(SDE)
        self.ingest(SDE, key="feishu:1", action="close_session")
        await self.run_inbox(SDE)
        self.assertEqual([e.payload["status"] for e in self.returned()], ["refused"])
        self.assertEqual([e for e in self.queued(MAIN) if e.source == RETURN_CANCELLED_SOURCE], [])
        self.assertEqual([m.content for m in self.sent()], ["late answer"])


class InternalEventTests(ForwardCase):
    # What the agents tell each other is believed because Hibernaut queued it, not because an event says so.
    async def waiting(self) -> SessionRef:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest(event_type="work.ready", task_id="t1", hop_count=1)
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        self.agents["steward"].script = lambda _e, _s, _c: Silent()
        return steward

    async def test_the_public_entrance_refuses_internal_sources(self) -> None:
        handler = EventHandler(inbox=self.inbox)
        for source in (*INTERNAL_SOURCES, f" {RETURN_CANCELLED_SOURCE} "):
            with self.subTest(source), self.assertRaisesRegex(ValueError, "reserved"):
                handler.ingest(inbox_id=MAIN, source=source, external_id="x", payload={}, correlation_key="feishu:1")
        self.assertEqual(self.inbox.all_events(), [])

    async def test_a_cancel_that_names_no_piece_of_this_session_changes_nothing(self) -> None:
        steward = await self.waiting()
        forwarded = self.queued(SDE)[0]
        other = self.sessions.list_sessions()
        self.assertEqual(len(other), 1)
        before = self.sessions.get(steward.session_id)
        payload = {"reason": "gone", "forwarded_from": SDE, "forwarded_by": "sde"}
        for inbox_id, extra in (
            (MAIN, {}), (MAIN, {"delegation_id": "forward:evt_other:0"}), (MAIN, {"delegation_id": 1}),
            # The right piece, in an inbox the waiting session does not belong to.
            (DOCS, {"delegation_id": forwarded.external_id}),
        ):
            self.ingest(inbox_id, source=RETURN_CANCELLED_SOURCE, key=None, session_id=steward.session_id,
                        **payload, **extra)
            self.assertEqual(await self.run_inbox(inbox_id), [])
        self.assertEqual(self.sessions.get(steward.session_id), before)
        self.assertEqual(self.sent("return_withheld"), [])
        self.assertNotIn("release", [m.metadata.get("task_action") for m in self.outbox.fetch_pending(limit=1000)])

    async def test_a_return_settles_only_the_piece_it_names(self) -> None:
        steward = await self.waiting()
        self.ingest(source=RETURN_SOURCE, key=None, session_id=steward.session_id, text="done, trust me",
                    status="done", forwarded_from=SDE, forwarded_by="sde", hop_count=1)
        [report] = await self.run_inbox()
        self.assertTrue(report.pending_elsewhere)
        self.answer("sde", "really done")
        await self.run_inbox(SDE)
        [report] = await self.run_inbox()
        self.assertFalse(report.pending_elsewhere)


class HeldReturnTests(ForwardCase):
    # A return that is refused waits for a person; it never turns into an answer to the outside.
    async def test_a_close_and_an_approval_claimed_together_are_decided_in_order(self) -> None:
        for order in (("close", "approve"), ("approve", "close")):
            for target in ("child", "holder"):
                with self.subTest(order=order, target=target):
                    self.setUp()
                    await self.owed()
                    self.gate("defer")
                    _report, card = await self.held()
                    child = self.agents["sde"].runs[0][1]
                    assert child is not None
                    closed = child.session_id if target == "child" else card.metadata["session_id"]
                    events = {}
                    for step in order:
                        if step == "close":
                            events[step] = self.ingest(SDE, source=CLOSE_SOURCE, key=None, session_id=closed)
                        else:
                            events[step] = self.ingest(
                                SDE, key=None, approval_request_id=card.metadata["approval_request_id"], approved=True,
                                event_type="approval.decided")
                    await self.run_inbox(SDE)
                    won = [RETURN_CANCELLED_SOURCE] if order[0] == "close" else [RETURN_SOURCE]
                    self.assertEqual([e.source for e in self.queued(MAIN)], won)
                    status = {e.event_id: e.status for e in self.inbox.all_events()}
                    self.assertEqual(status[events["approve"].event_id], "failed" if order[0] == "close" else "done")
                    self.assertEqual(len(self.agents["sde"].runs), 1)

    async def test_a_session_closed_earlier_in_the_claim_is_not_resumed(self) -> None:
        self.agents["sde"].script = lambda _e, _s, _c: Suspend("req1", "merge?", "{}")
        self.ingest(SDE, key="task:1")
        await self.run_inbox(SDE)
        session = self.agents["sde"].runs[0][1]
        assert session is not None
        self.ingest(SDE, source=CLOSE_SOURCE, key=None, session_id=session.session_id)
        approval = self.ingest(SDE, key=None, approval_request_id="req1", approved=True)
        # Grouped with the close only under its session; a decision is a batch of its own.
        with patch.object(AgentWorker, "_group_into_batches", lambda worker, events: [
            (worker._resolve_session(e), [e]) for e in events  # pyright: ignore[reportPrivateUsage]
        ]):
            await self.run_inbox(SDE)
        self.assertEqual(self.sessions.get(session.session_id).status, "closed")  # type: ignore[union-attr]
        self.assertEqual(len(self.agents["sde"].runs), 1)
        self.assertEqual([e.status for e in self.inbox.all_events() if e.event_id == approval.event_id], ["failed"])

    async def owed(self) -> str:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        return steward.session_id

    async def held(self, text: str = "result", *, sent: int = 0, **kwargs: Any) -> tuple[RunReport, Any]:
        self.answer("sde", text, **kwargs)
        [report] = await self.run_inbox(SDE)
        self.assertEqual(len(self.sent()), sent)
        self.assertEqual(self.queued(MAIN), [])
        return report, self.sent("approval_request")[-1]

    async def decide(self, card: Any, approved: bool, inbox_id: str = SDE) -> list[RunReport]:
        self.ingest(inbox_id, key=None, approval_request_id=card.metadata["approval_request_id"], approved=approved,
                    event_type="approval.decided")
        return await self.run_inbox(inbox_id)

    async def test_a_deferred_return_is_approved_on_its_own_and_then_queued(self) -> None:
        steward = await self.owed()
        self.gate("defer")
        report, card = await self.held()
        self.assertIsInstance(report.outcome, Suspend)
        self.assertTrue(report.pending_elsewhere)
        for expected in ("ask first", "done", MAIN, card.metadata["approval_request_id"]):
            self.assertIn(expected, card.content)
        self.assertEqual((card.inbox_id, card.metadata["outcome_kind"], card.metadata["reply_target"]),
                         (SDE, "suspend", "oc_1"))
        waiting = self.sessions.find_by_request_id(card.metadata["approval_request_id"])
        assert waiting is not None
        child = self.agents["sde"].runs[0][1]
        assert child is not None
        self.assertEqual((waiting.inbox_id, waiting.status, waiting.parent_session_id, waiting.agent_id),
                         (SDE, "suspended", child.session_id, "sde"))
        # The parent still waits, so its task stays open.
        parent = self.sessions.get(steward)
        assert parent is not None and parent.forward is not None
        self.assertEqual(len(parent.forward.pending_returns), 1)

        [decided] = await self.decide(card, True)
        self.assertTrue(decided.pending_elsewhere)
        [event] = self.queued(MAIN)
        self.assertEqual((event.source, event.session_id, event.payload["text"], event.payload["status"],
                          event.payload["hop_count"]), (RETURN_SOURCE, steward, "result", "done", 2))
        self.assertEqual((len(self.agents["sde"].runs), self.sent()), (1, []))
        closed = self.sessions.get(waiting.session_id)
        assert closed is not None
        self.assertEqual(closed.status, "closed")
        self.answer("steward", "all done")
        await self.run_inbox()
        self.assertEqual([m.content for m in self.sent()], ["all done"])

    async def test_nothing_sent_before_the_decision_carries_the_result(self) -> None:
        for outcome in ("done", "failed"):
            with self.subTest(outcome):
                self.setUp()
                self.ledger = Ledger()
                await self.owed()
                self.gate("block", "not today")
                report, card = await self.held("MARK-7391", status=outcome)
                assert isinstance(report.outcome, Suspend)
                await self.decide(card, True)
                self.assertEqual(len(self.sent("approval_request")), 2)
                self.assertNotIn("MARK-7391", self.outbound() + report.outcome.prompt)
                # It stays where the person deciding can read it.
                holder = self.sessions.get(card.metadata["session_id"])
                assert holder is not None and holder.forward is not None and holder.forward.held is not None
                self.assertEqual(holder.forward.held.payload["text"], "MARK-7391")
                await self.decide(self.sent("approval_request")[-1], False)
                self.assertNotIn("MARK-7391", self.outbound())

    async def test_a_declined_return_tells_the_waiting_agent_only_that_it_was_withheld(self) -> None:
        await self.owed()
        self.gate("defer")
        _report, card = await self.held("secret result")
        await self.decide(card, False)
        [event] = self.queued(MAIN)
        self.assertEqual((event.source, event.payload["status"]), (RETURN_SOURCE, "refused"))
        self.assertNotIn("secret result", str(event.payload))
        self.assertEqual(self.sent(), [])
        self.answer("steward", "could not get it")
        reports = await self.run_inbox()
        self.assertFalse(reports[-1].pending_elsewhere)

    async def test_a_decision_counts_once(self) -> None:
        await self.owed()
        self.gate("defer")
        _report, card = await self.held()
        await self.decide(card, True)
        await self.decide(card, False)
        self.assertEqual([e.payload["status"] for e in self.queued(MAIN)], ["done"])

    async def test_approval_does_not_override_a_block(self) -> None:
        await self.owed()
        self.gate("block", "not today")
        _report, card = await self.held()
        await self.decide(card, True)
        self.assertEqual((self.queued(MAIN), self.sent()), ([], []))
        again = self.sent("approval_request")[-1]
        self.assertNotEqual(again.metadata["approval_request_id"], card.metadata["approval_request_id"])
        self.assertIn("not today", again.content)
        # Once the policy allows it, the same held result goes out.
        self.permissions = InMemoryPermission()
        await self.decide(again, True)
        self.assertEqual([e.payload["text"] for e in self.queued(MAIN)], ["result"])

    def pending(self, session_id: str) -> int:
        session = self.sessions.get(session_id)
        assert session is not None and session.forward is not None
        return len(session.forward.pending_returns)

    async def test_a_blocked_return_that_is_declined_ends_the_wait(self) -> None:
        steward = await self.owed()
        self.gate("block", "not today")
        _report, card = await self.held("secret result")
        await self.decide(card, False)
        self.assertEqual((self.returned(), self.sent()), ([], []))
        [cancelled] = self.queued(MAIN)
        self.assertEqual((cancelled.source, cancelled.session_id), (RETURN_CANCELLED_SOURCE, steward))
        self.assertNotIn("secret result", str(cancelled.payload))
        self.assertEqual(self.pending(steward), 1)

        [report] = await self.run_inbox()
        # No agent runs for it: the parent's task ends, and the person who asked is told.
        self.assertEqual((self.pending(steward), len(self.agents["steward"].runs)), (0, 1))
        self.assertFalse(report.pending_elsewhere)
        self.assertEqual((report.session_id, report.correlation_key), (steward, "feishu:1"))
        self.assertIn("withheld", report.rejected or "")
        [notice] = self.sent("return_withheld")
        self.assertEqual((notice.inbox_id, notice.metadata["reply_target"], notice.metadata["outcome_kind"]),
                         (MAIN, "oc_1", "error"))
        self.assertNotIn("secret result", self.outbound())
        self.assertEqual(self.queued(MAIN), [])

    async def test_the_parents_task_ends_with_the_last_wait(self) -> None:
        for order in ("cancelled first", "cancelled last"):
            with self.subTest(order):
                self.setUp()

                def split(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
                    caps.forward_to_inbox(SDE, "code", return_to=MAIN)
                    caps.forward_to_inbox(DOCS, "docs", return_to=MAIN)
                    return Emit(messages=("split in two",))

                self.agents["steward"].script = split
                self.ingest(event_type="work.ready", task_id="t1", trace_id="tr1", hop_count=1)
                await self.run_inbox()
                self.answer("writer", "docs done")
                if order == "cancelled last":
                    await self.run_inbox(DOCS)
                    self.agents["steward"].script = lambda _e, _s, _c: Silent()
                    await self.run_inbox()
                self.gate("block", "not today")
                _report, card = await self.held("secret result", sent=1)
                await self.decide(card, False)
                self.permissions = InMemoryPermission()
                if order == "cancelled first":
                    [report] = await self.run_inbox()
                    self.assertTrue(report.pending_elsewhere)
                    self.assertIsNone(report.rejected)
                    await self.run_inbox(DOCS)
                    self.answer("steward", "docs only")
                reports = await self.run_inbox()
                self.assertFalse(reports[-1].pending_elsewhere)
                last = [m.metadata["task_action"] for m in self.outbox.fetch_pending(limit=1000)
                        if "task_action" in m.metadata][-1]
                self.assertEqual(last, "complete" if order == "cancelled first" else "release")
                self.assertNotIn("secret result", str([r[0] for r in self.agents["steward"].runs]))
                self.assertNotIn("secret result", self.outbound())

    async def test_one_decision_ends_one_wait_once(self) -> None:
        def twice(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(SDE, "code", return_to=MAIN)
            caps.forward_to_inbox(DOCS, "docs", return_to=MAIN)
            return Silent()

        self.agents["steward"].script = twice
        self.ingest()
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        self.gate("block", "not today")
        _report, card = await self.held()
        # A crash before either step is marked done repeats it.
        with patch.object(self.inbox, "mark_done", side_effect=RuntimeError("crash")), self.assertRaises(RuntimeError):
            await self.decide(card, False)
        await self.run_inbox(SDE)
        with patch.object(self.inbox, "mark_done", side_effect=RuntimeError("crash")), self.assertRaises(RuntimeError):
            await self.run_inbox()
        await self.run_inbox()
        self.assertEqual(self.pending(steward.session_id), 1)

    async def test_a_cancelled_wait_neither_joins_nor_interrupts_a_run(self) -> None:
        for policy in ("fold", "interrupt"):
            with self.subTest(policy):
                self.setUp()
                self.policy = policy
                steward = await self.owed()
                self.gate("block", "not today")
                _report, card = await self.held()
                worker = self.worker("w2")
                folded: list[Event] = []

                def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
                    folded.extend(caps.poll_folded_events())
                    return Silent()

                async def busy(events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
                    self.agents["steward"].runs.append((list(events), session))
                    await self.decide(card, False)
                    worker.notify_new_event(MAIN)
                    await asyncio.sleep(0.2)
                    return script(events, session, caps)

                self.ingest(text="any news?")
                with patch.object(self.agents["steward"], "execute", busy):
                    await worker.process_inbox(MAIN)
                self.assertEqual((len(self.agents["steward"].runs), folded), (2, []))
                self.assertEqual(self.pending(steward), 0)

    async def test_a_parent_waiting_for_a_person_keeps_its_request(self) -> None:
        steward = await self.owed()
        self.agents["steward"].script = lambda _e, _s, _c: Suspend(request_id="req_s", prompt="may I?", session_checkpoint="")
        self.ingest(text="and one more")
        await self.run_inbox()
        self.gate("block", "not today")
        _report, card = await self.held()
        await self.decide(card, False)
        [report] = await self.run_inbox()
        session = self.sessions.get(steward)
        assert session is not None
        self.assertEqual((session.status, session.pending_request_id, self.pending(steward)), ("suspended", "req_s", 0))
        self.assertIsNone(report.rejected)
        self.answer("steward", "done")
        self.ingest(key=None, approval_request_id="req_s", approved=True, event_type="approval.decided")
        reports = await self.run_inbox()
        self.assertFalse(reports[-1].pending_elsewhere)

    async def test_a_waiting_agent_that_owes_a_result_itself_gives_back_that_it_ended(self) -> None:
        await self.owed()
        self.forward("sde", DOCS, "document it", return_to=SDE)
        await self.run_inbox(SDE)
        self.gate("block", "not today")
        self.answer("writer", "secret result")
        await self.run_inbox(DOCS)
        await self.decide(self.sent("approval_request")[-1], False, DOCS)
        self.permissions = InMemoryPermission()
        [report] = await self.run_inbox(SDE)
        self.assertTrue(report.pending_elsewhere)
        [event] = self.returned()
        self.assertEqual(event.payload["status"], "refused")
        self.assertNotIn("secret result", str(event.payload))
        self.assertEqual(self.sent("return_withheld"), [])

    async def test_closing_a_child_ends_its_parents_wait(self) -> None:
        for held in (False, True):
            with self.subTest(held=held):
                self.setUp()
                steward = await self.owed()
                child_key = self.queued(SDE)[0].correlation_key
                if held:
                    self.gate("block", "not today")
                    await self.held()
                else:
                    self.agents["sde"].script = lambda _e, _s, _c: Silent()
                    await self.run_inbox(SDE)
                self.ingest(SDE, key=child_key, action="close_session")
                await self.run_inbox(SDE)
                await self.run_inbox()
                self.assertEqual((self.pending(steward), self.returned()), (0, []))

    async def test_a_held_result_is_not_closed_past_its_decision(self) -> None:
        await self.owed()
        self.gate("defer")
        _report, card = await self.held()
        self.ingest(SDE, key=None, session_id=card.metadata["session_id"], action="close_session")
        await self.run_inbox(SDE)
        await self.decide(card, True)
        self.assertEqual([e.payload["text"] for e in self.returned()], ["result"])

    async def test_a_result_out_of_hops_is_settled_here_and_never_queued(self) -> None:
        for approved in (True, False):
            for action in ("allow", "defer", "block"):
                with self.subTest(approved=approved, action=action):
                    self.setUp()
                    steward = await self.owed()
                    [forwarded] = self.queued(SDE)
                    self.inbox.mark_done([forwarded.event_id])
                    self.ingest(SDE, key="late", source=FORWARD_SOURCE,
                                **{**forwarded.payload, "hop_count": MAX_FORWARD_HOPS})
                    self.gate(action)
                    _report, card = await self.held("MARK late")
                    self.assertIn("limit", card.content)
                    await self.decide(card, approved)
                    self.assertEqual((self.returned(), len(self.sent("approval_request"))), ([], 1))
                    [report] = await self.run_inbox()
                    self.assertEqual((self.pending(steward), len(self.agents["steward"].runs)), (0, 1))
                    self.assertFalse(report.pending_elsewhere)
                    self.assertEqual((self.returned(), self.sent()), ([], []))
                    self.assertNotIn("MARK", self.outbound())
                    holder = self.sessions.get(card.metadata["session_id"])
                    assert holder is not None and holder.forward is not None and holder.forward.held is not None
                    self.assertEqual((holder.status, holder.forward.held.payload["text"]), ("closed", "MARK late"))

    async def test_a_task_out_of_hops_is_finished_by_the_person(self) -> None:
        for approved, action in ((True, "complete"), (False, "release")):
            with self.subTest(approved=approved):
                self.setUp()
                self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=MAIN, hop_count=MAX_FORWARD_HOPS)
                self.ingest(SDE, event_type="work.ready", task_id="t1", hop_count=1)
                _report, card = await self.held()
                await self.decide(card, approved)
                last = self.outbox.fetch_pending(limit=1000)[-1].metadata
                self.assertEqual((last["task_id"], last["task_action"]), ("t1", action))

    async def test_a_return_to_another_project_never_leaves_this_one(self) -> None:
        self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=OTHER, hop_count=1)
        _report, card = await self.held()
        await self.decide(card, True)
        await self.decide(self.sent("approval_request")[-1], False)
        self.assertEqual((self.queued(OTHER), self.sent()), ([], []))

    async def test_failures_and_refusals_are_held_too(self) -> None:
        await self.owed()
        self.gate("defer")

        def boom(_e: Sequence[Event], _s: SessionRef | None, _c: RunCapabilities) -> RunOutcome:
            raise RuntimeError("no disk")

        self.agents["sde"].script = boom
        with self.assertRaises(ExecutorFailed):
            await self.run_inbox(SDE)
        self.ingest(SDE, key="other", source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=MAIN, hop_count=1,
                    agent_id="ops")
        [refused] = await self.run_inbox(SDE)
        self.assertTrue(refused.rejected and refused.pending_elsewhere)
        self.assertEqual(self.queued(MAIN), [])
        cards = self.sent("approval_request")
        self.assertEqual(len(cards), 2)
        for card in cards:
            await self.decide(card, True)
        self.assertEqual([e.payload["status"] for e in self.queued(MAIN)], ["failed", "refused"])

    async def test_the_child_goes_on_while_its_result_is_held(self) -> None:
        await self.owed()
        self.gate("defer")
        _report, card = await self.held()
        self.ingest(SDE, key="feishu:1", text="thanks, one more thing")
        self.answer("sde", "second")
        await self.run_inbox(SDE)
        child = self.agents["sde"].runs[1][1]
        assert child is not None
        self.assertEqual((child.status, child.pending_request_id), ("active", None))
        self.assertEqual([m.content for m in self.sent()], ["second"])
        await self.decide(card, True)
        self.assertEqual([e.payload["text"] for e in self.queued(MAIN)], ["result"])

    async def test_only_a_decision_reaches_a_held_result(self) -> None:
        await self.owed()
        self.gate("defer")
        report, card = await self.held()
        event = self.ingest(SDE, key=None, session_id=report.session_id, text="hello")
        await self.run_inbox(SDE)
        self.assertEqual(len(self.agents["sde"].runs), 1)
        self.assertEqual([e.status for e in self.inbox.all_events() if e.event_id == event.event_id], ["failed"])
        await self.decide(card, True)
        self.assertEqual([e.payload["text"] for e in self.queued(MAIN)], ["result"])

    async def test_a_repeated_run_keeps_one_request(self) -> None:
        await self.owed()
        self.gate("defer")
        self.answer("sde", "result")
        # A crash before the events are marked done runs them again.
        with patch.object(self.inbox, "mark_done", side_effect=RuntimeError("crash")), self.assertRaises(RuntimeError):
            await self.run_inbox(SDE)
        await self.run_inbox(SDE)
        self.assertEqual(len(self.agents["sde"].runs), 2)
        [card] = self.sent("approval_request")
        await self.decide(card, True)
        self.assertEqual([e.payload["text"] for e in self.queued(MAIN)], ["result"])
        self.assertEqual(self.sent(), [])

    async def test_closing_the_child_drops_its_held_result(self) -> None:
        steward = await self.owed()
        self.gate("defer")
        _report, card = await self.held()
        self.ingest(key=None, session_id=steward, action="close_session")
        await self.run_inbox()
        await self.run_inbox(SDE)
        await self.decide(card, True)
        self.assertEqual([e for e in self.queued(MAIN) if e.source == RETURN_SOURCE], [])

    async def test_a_task_is_finished_by_the_decision(self) -> None:
        for approved, action in ((True, "complete"), (False, "release")):
            with self.subTest(approved=approved):
                self.permissions = InMemoryPermission({"inbox.forward": PermissionDecision("defer", "ask")})
                key = f"k:{approved}"
                self.ingest(SDE, key=key, source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=MAIN, hop_count=1)
                self.ingest(SDE, key=key, event_type="work.ready", task_id=key, hop_count=1)
                self.answer("sde", "result")
                await self.run_inbox(SDE)

                def actions(key: str = key) -> list[str]:
                    return [m.metadata["task_action"] for m in self.outbox.fetch_pending(limit=1000)
                            if m.metadata.get("task_id") == key]

                self.assertNotIn("complete", actions())
                self.assertEqual(actions()[-1], "update_stage")
                await self.decide(self.sent("approval_request")[-1], approved)
                self.assertEqual(actions()[-1], action)


class TaskLinkTests(ForwardCase):
    def task(self, **payload: Any) -> None:
        self.ingest(event_type="work.ready", task_id="t1", trace_id="tr1", hop_count=1, **payload)

    def actions(self) -> list[tuple[str, str | None]]:
        return [(m.metadata["task_action"], m.metadata.get("stage"))
                for m in self.outbox.fetch_pending(limit=1000) if "task_action" in m.metadata]

    async def test_a_forwarded_task_is_completed_by_the_agent_that_got_it(self) -> None:
        self.forward("steward", SDE, "x")
        self.task()
        await self.run_inbox()
        self.assertEqual(self.actions(), [("claim", "running"), ("update_stage", "forwarded")])
        self.answer("sde", "done")
        await self.run_inbox(SDE)
        self.assertEqual(self.actions()[2:], [("claim", "running"), ("complete", None)])
        last = self.outbox.fetch_pending(limit=1000)[-1].metadata
        # Taskboard's own hop count is not the forward hop.
        self.assertEqual((last["task_id"], last["trace_id"], last["hop_count"]), ("t1", "tr1", 1))

    async def test_a_split_task_waits_for_every_return(self) -> None:
        def split(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(SDE, "code", return_to=MAIN)
            caps.forward_to_inbox(DOCS, "docs", return_to=MAIN)
            return Emit(messages=("split in two",))

        self.agents["steward"].script = split
        self.task()
        await self.run_inbox()
        self.assertEqual(self.actions()[-1], ("update_stage", "awaiting_return"))
        self.answer("sde", "code done")
        self.answer("writer", "docs done")
        await self.run_inbox(SDE)
        self.agents["steward"].script = lambda _e, _s, _c: Silent()
        await self.run_inbox()
        self.assertNotIn(("complete", None), self.actions())
        await self.run_inbox(DOCS)
        self.answer("steward", "both done")
        reports = await self.run_inbox()
        self.assertEqual(self.actions()[-1], ("complete", None))
        self.assertFalse(reports[-1].pending_elsewhere)

    async def test_sync_messages_stay_task_sync(self) -> None:
        self.forward("steward", SDE, "x")
        self.task()
        await self.run_inbox()
        self.assertEqual({m.metadata["kind"] for m in self.outbox.fetch_pending(limit=1000)}, {TASK_SYNC_KIND})


class StorageTests(unittest.TestCase):
    def test_forward_spec_round_trips_through_sqlite(self) -> None:
        spec = ForwardSpec(target_inbox=SDE, target_agent="sde", return_to=MAIN, hop_count=2, forwarded_from=MAIN,
                           parent_session_id="p", reply_channel="feishu", reply_target="oc", origin_key="k",
                           delegation_id="forward:e:0", work_key="wk", pending_returns=("forward:a:0", "forward:a:1"),
                           cancelled_returns=("forward:b:0",), owed_failures=("evt",),
                           settled_returns=(("forward:c:0", "evt_1"), ("forward:d:0", "evt_2")), handed_on=True,
                           closed=True, continued_in="s9", replayed=True)
        with tempfile.TemporaryDirectory() as tmp:
            db = SQLiteDatabase(Path(tmp) / "x.db")
            store = SQLiteSessionStoreAdapter(db)
            store.save(SessionRef(session_id="s1", inbox_id=SDE, executor_id="sde", forward=spec))
            store.save(SessionRef(session_id="s2", inbox_id=SDE))
            loaded = store.get("s1")
            assert loaded is not None
            self.assertEqual((loaded.forward, loaded.agent_id), (spec, "sde"))
            waiting = replace(spec, held=HeldReturn(agent_id="sde", external_id="return:e:0", reason="ask",
                                                    payload={"text": "t", "hop_count": 2}, task_fields={"stage": "x"}))
            store.save(SessionRef(session_id="s3", inbox_id=SDE, forward=waiting))
            self.assertEqual(store.get("s3").forward, waiting)  # type: ignore[union-attr]
            self.assertIsNone(store.get("s2").forward)  # type: ignore[union-attr]
            db._conn.execute("UPDATE sessions SET forward = 'not json' WHERE session_id = 's1'")  # pyright: ignore
            # An unreadable record must not reset the hop count.
            broken = store.get("s1")
            assert broken is not None and broken.forward is not None
            self.assertEqual(broken.forward.hop_count, MAX_FORWARD_HOPS)
            db.close()


if __name__ == "__main__":
    unittest.main()
