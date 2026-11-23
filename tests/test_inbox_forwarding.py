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
    FORWARD_SOURCE,
    MAX_FORWARD_HOPS,
    RETURN_SOURCE,
    TASK_SYNC_KIND,
    Emit,
    Event,
    ForwardRefused,
    ForwardSpec,
    HeldReturn,
    InboxBinding,
    InboxMatchRule,
    InboxRoute,
    PermissionDecision,
    RunOutcome,
    SessionRef,
    Silent,
    Suspend,
)
from agent_runtime.core.ports import RunCapabilities
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

    def worker(self, name: str = "w1") -> AgentWorker:
        return AgentWorker(
            worker_id=name, inbox=self.inbox, timers=InMemoryTimerStore(), memory=InMemoryMemoryStore(),
            permissions=self.permissions, outbox=self.outbox, lock=self.lock, sessions=self.sessions,
            executors=self.agents, project_default_executor="steward", global_default_executor="steward",
            routes=routes, on_forward=self.woken.append, new_event_policy=self.policy, max_retries=1,
        )

    def ingest(self, inbox_id: str = MAIN, *, source: str = "switchboard:feishu", key: str | None = "feishu:1",
               session_id: str | None = None, **payload: Any) -> Event:
        event, _ = self.inbox.append(Event(
            inbox_id=inbox_id, source=source, external_id=f"e{len(self.inbox.all_events())}", payload=payload,
            correlation_key=key, session_id=session_id,
        ))
        return event

    async def run_inbox(self, inbox_id: str = MAIN) -> list[RunReport]:
        return await self.worker().process_inbox(inbox_id)

    def sent(self, kind: str = "emit") -> list[Any]:
        return [m for m in self.outbox.fetch_pending(limit=1000) if m.metadata.get("kind") == kind]

    def queued(self, inbox_id: str) -> list[Event]:
        return self.inbox.fetch_pending(inbox_id)

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


class HeldReturnTests(ForwardCase):
    # A return that is refused waits for a person; it never turns into an answer to the outside.
    def gate(self, action: Any, reason: str = "ask first") -> None:
        self.permissions = InMemoryPermission({"inbox.forward": PermissionDecision(action, reason)})

    async def owed(self) -> str:
        self.forward("steward", SDE, "x", return_to=MAIN)
        self.ingest(reply_channel="feishu", reply_target="oc_1")
        await self.run_inbox()
        steward = self.agents["steward"].runs[0][1]
        assert steward is not None
        return steward.session_id

    async def held(self, text: str = "result") -> tuple[RunReport, Any]:
        self.answer("sde", text)
        [report] = await self.run_inbox(SDE)
        self.assertEqual(self.sent(), [])
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
        for expected in ("ask first", "result", MAIN):
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
        self.assertEqual(parent.forward.pending_returns, 1)

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

    async def test_a_blocked_return_that_is_declined_goes_nowhere(self) -> None:
        steward = await self.owed()
        self.gate("block", "not today")
        _report, card = await self.held("secret result")
        await self.decide(card, False)
        self.assertEqual((self.queued(MAIN), self.sent()), ([], []))
        [notice] = self.sent("return_withheld")
        self.assertIn("not today", notice.content)
        self.assertNotIn("secret result", notice.content)
        parent = self.sessions.get(steward)
        assert parent is not None and parent.forward is not None
        self.assertEqual(parent.forward.pending_returns, 1)

    async def test_a_return_at_the_hop_limit_needs_a_person_and_buys_no_hop(self) -> None:
        self.ingest(SDE, source=FORWARD_SOURCE, forwarded_from=MAIN, return_to=MAIN, hop_count=MAX_FORWARD_HOPS,
                    reply_channel="feishu", reply_target="oc_1")
        _report, card = await self.held("late")
        self.assertIn("limit", card.content)
        await self.decide(card, True)
        [event] = self.queued(MAIN)
        self.assertEqual((event.payload["text"], event.payload["hop_count"]), ("late", MAX_FORWARD_HOPS))
        self.assertIn("limit", str(await self.refusal(SDE, "again")))

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
                           pending_returns=2)
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
