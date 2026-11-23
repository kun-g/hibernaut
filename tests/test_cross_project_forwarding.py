from __future__ import annotations

import tempfile
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.core.models import (
    CLOSE_SOURCE,
    FORWARD_SOURCE,
    MAX_FORWARD_HOPS,
    RETURN_SOURCE,
    Emit,
    Event,
    ForwardRefused,
    ForwardSpec,
    InboxBinding,
    InboxRoute,
    PermissionDecision,
    RunOutcome,
    SessionRef,
    Silent,
    Suspend,
)
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine.worker import AgentWorker
from tests.fakes import InMemoryMemoryStore, InMemoryTimerStore
from tests.test_inbox_forwarding import MAIN, OTHER, SDE, Agent, ForwardCase, routes

ASH = "user:main"
# Another inbox of the same binding.
ASH_TOO = "user:main:too"
ELSEWHERE = "user:two"
CHAT = "feishu:p2p:oc_kun"
SCOPE = "runtime.inbox_bindings:user:main"


class Gate:
    def __init__(self) -> None:
        self.decisions: dict[str, PermissionDecision] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
        self.calls.append((tool_name, args))
        return self.decisions.get(tool_name, PermissionDecision("allow", "ok"))


class CrossCase(ForwardCase):
    permissions: Any

    def setUp(self) -> None:
        super().setUp()
        self.permissions = Gate()
        self.agents["ash"] = Agent("ash", self.log)
        self.allowed: tuple[str, ...] = (MAIN,)

    def gate(self, action: Any, reason: str = "ask first") -> None:
        self.permissions.decisions["inbox.forward"] = PermissionDecision(action, reason)

    def routes(self, inbox_id: str) -> InboxRoute | None:
        if inbox_id in (ASH, ASH_TOO):
            return InboxRoute(project=SCOPE, has_bindings=True, forward_to=self.allowed, binding=InboxBinding(
                inbox=ASH, default_agent="ash", agents=("ash", "writer")))
        if inbox_id == ELSEWHERE:
            return InboxRoute(project="runtime.inbox_bindings:user:two", has_bindings=True, binding=InboxBinding(
                inbox=ELSEWHERE, default_agent="ops", agents=("ops",)))
        return routes(inbox_id)

    def worker(self, name: str = "w1", hold: Any = None) -> AgentWorker:
        return AgentWorker(
            worker_id=name, inbox=self.inbox, timers=InMemoryTimerStore(), memory=InMemoryMemoryStore(),
            permissions=self.permissions, outbox=self.outbox, lock=self.lock, sessions=self.sessions,
            executors=self.agents, project_default_executor=None, global_default_executor="steward",
            routes=self.routes, on_forward=self.woken.append, new_event_policy=self.policy, max_retries=1, hold=hold,
        )

    def say(self, text: str = "do it", **payload: Any) -> Event:
        return self.ingest(ASH, key=CHAT, text=text, reply_channel="feishu", reply_target="oc_kun", **payload)

    async def dispatch(self, *calls: dict[str, Any]) -> list[Event]:
        # One message of the chat, one Run of ash, the forwards it made.
        receipts: list[Event] = []

        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            receipts.extend(caps.forward_to_inbox(**{"target_inbox": MAIN, **call}) for call in calls)
            return Silent()

        self.agents["ash"].script = script
        self.say()
        await self.run_inbox(ASH)
        return receipts

    async def refused(self, inbox_id: str = ASH, agent: str = "ash", **call: Any) -> ForwardRefused:
        found: list[ForwardRefused] = []

        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            try:
                caps.forward_to_inbox(**call)
            except ForwardRefused as exc:
                found.append(exc)
            return Silent()

        self.agents[agent].script = script
        if inbox_id == ASH:
            self.say()
        await self.run_inbox(inbox_id)
        self.assertEqual(len(found), 1)
        return found[0]

    def session(self, inbox_id: str, agent: str) -> SessionRef:
        [found] = [s for s in self.sessions.list_sessions(inbox_id=inbox_id) if s.agent_id == agent]
        return found


class WhitelistTests(CrossCase):
    async def test_a_listed_main_inbox_takes_the_work_and_answers_the_chat(self) -> None:
        [receipt] = await self.dispatch({"text": "fix the bug", "title": "Bug"})
        [event] = self.queued(MAIN)
        self.assertEqual((event.source, event.payload["forwarded_from"], event.payload["hop_count"]),
                         (FORWARD_SOURCE, ASH, 1))
        # The project's own rules pick the agent.
        self.assertNotIn("agent_id", event.payload)
        self.assertEqual(event.correlation_key, receipt.correlation_key)
        self.answer("steward", "done")
        await self.run_inbox(MAIN)
        [message] = self.sent()
        self.assertEqual((message.inbox_id, message.content), (MAIN, "done"))
        self.assertEqual(
            {k: message.metadata[k] for k in ("correlation_key", "reply_channel", "reply_target")},
            {"correlation_key": CHAT, "reply_channel": "feishu", "reply_target": "oc_kun"},
        )

    async def test_the_project_rules_pick_the_agent_by_tags(self) -> None:
        await self.dispatch({"text": "look at this", "tags": ("review",)})
        await self.run_inbox(MAIN)
        self.assertEqual((len(self.agents["code-reviewer"].runs), len(self.agents["steward"].runs)), (1, 0))

    async def test_anything_not_listed_is_refused(self) -> None:
        # Another project, an inbox inside a listed project, another binding, an inbox nothing matches.
        for target in (OTHER, SDE, ELSEWHERE, "sys:ops", "proj:hibernaut:new"):
            with self.subTest(target=target):
                refusal = await self.refused(target_inbox=target, text="x")
                self.assertIn(f"target_inbox '{target}' is not an inbox of this project", str(refusal))
                self.assertEqual(refusal.action, "block")
        self.assertEqual([e for e in self.inbox.all_events() if e.source == FORWARD_SOURCE], [])

    async def test_an_empty_list_keeps_work_inside_the_binding(self) -> None:
        self.allowed = ()
        self.assertIn("not an inbox of this project", str(await self.refused(target_inbox=MAIN, text="x")))
        self.forward("ash", ASH, "write it up", target_agent="writer")
        self.say()
        await self.run_inbox(ASH)
        self.assertEqual(len(self.agents["writer"].runs), 1)

    async def test_the_agent_of_another_project_is_not_named(self) -> None:
        for agent in ("steward", "sde", "ghost"):
            refusal = await self.refused(target_inbox=MAIN, text="x", target_agent=agent)
            self.assertIn("target_agent", str(refusal))
        self.assertEqual(self.queued(MAIN), [])

    async def test_the_result_comes_back_to_the_sending_inbox_only(self) -> None:
        for back in (ELSEWHERE, MAIN, SDE, OTHER):
            with self.subTest(back=back):
                self.assertIn("return_to", str(await self.refused(target_inbox=MAIN, text="x", return_to=back)))
        self.assertEqual(self.queued(MAIN), [])

    async def test_work_owed_inside_the_binding_is_handed_to_a_project(self) -> None:
        self.forward("ash", ASH, "find out", target_agent="writer", return_to=ASH)
        self.forward("writer", MAIN, "find out")
        self.say()
        await self.run_inbox(ASH)
        [event] = self.queued(MAIN)
        self.assertEqual(event.payload["return_to"], ASH)
        self.answer("steward", "found")
        await self.run_inbox(MAIN)
        [back] = self.queued(ASH)
        [asked] = [s for s in self.sessions.list_sessions(inbox_id=ASH) if s.agent_id == "ash"]
        self.assertEqual(back.session_id, asked.session_id)

    async def test_work_owed_to_another_inbox_of_the_binding_is_not(self) -> None:
        self.forward("ash", ASH_TOO, "find out", return_to=ASH)
        self.say()
        await self.run_inbox(ASH)
        found: list[str] = []

        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            for back in (None, ASH):
                try:
                    caps.forward_to_inbox(MAIN, "x", return_to=back)
                except ForwardRefused as exc:
                    found.append(str(exc))
            return Silent()

        self.agents["ash"].script = script
        await self.run_inbox(ASH_TOO)
        self.assertEqual(found, [f"return_to must be '{ASH_TOO}' for work sent to another project"] * 2)
        self.assertEqual(self.queued(MAIN), [])

    async def test_a_result_returns_to_the_session_that_asked(self) -> None:
        [receipt] = await self.dispatch({"text": "part one", "return_to": ASH})
        self.answer("steward", "part one done")
        await self.run_inbox(MAIN)
        self.assertEqual(self.sent(), [])
        [event] = self.queued(ASH)
        ash = self.session(ASH, "ash")
        self.assertEqual((event.source, event.session_id), (RETURN_SOURCE, ash.session_id))
        self.assertEqual(event.payload["from_task"], receipt.correlation_key)
        self.answer("ash", "all done")
        await self.run_inbox(ASH)
        [message] = self.sent()
        self.assertEqual((message.inbox_id, message.content, message.metadata["correlation_key"],
                          message.metadata["reply_target"]), (ASH, "all done", CHAT, "oc_kun"))

    async def test_a_result_still_answers_the_chat_when_the_asking_session_is_gone(self) -> None:
        await self.dispatch({"text": "part one", "return_to": ASH})
        ash = self.session(ASH, "ash")
        self.sessions.save(replace(ash, status="closed", correlation_key=None))
        self.answer("steward", "part one done")
        await self.run_inbox(MAIN)
        [event] = self.queued(ASH)
        self.assertEqual((event.session_id, event.correlation_key), (None, CHAT))

    async def test_a_project_starts_nothing_outside_itself(self) -> None:
        for target in (ASH, ELSEWHERE, "sys:ops", OTHER):
            with self.subTest(target=target):
                self.ingest(MAIN, key="feishu:2", text="x")
                refusal = await self.refused(MAIN, "steward", target_inbox=target, text="x")
                self.assertIn("not an inbox of this project", str(refusal))
            self.ingest(MAIN, key="feishu:2", text="x")
            refusal = await self.refused(MAIN, "steward", target_inbox=SDE, text="x", return_to=target)
            self.assertIn("return_to", str(refusal))
        self.assertEqual([e for e in self.inbox.all_events() if e.payload.get("forwarded_from") == MAIN], [])

    async def test_the_receiver_cannot_turn_the_work_round_to_the_asking_inbox(self) -> None:
        await self.dispatch({"text": "part one", "return_to": ASH})
        refusal = await self.refused(MAIN, "steward", target_inbox=ASH, text="x")
        self.assertIn("not an inbox of this project", str(refusal))

    async def test_an_owed_result_is_handed_on_inside_the_project(self) -> None:
        await self.dispatch({"text": "fix it", "return_to": ASH})
        self.forward("steward", SDE, "fix it, please")
        await self.run_inbox(MAIN)
        [event] = self.queued(SDE)
        self.assertEqual(event.payload["return_to"], ASH)
        self.answer("sde", "fixed")
        await self.run_inbox(SDE)
        [back] = self.queued(ASH)
        self.assertEqual((back.source, back.payload["text"], back.session_id),
                         (RETURN_SOURCE, "fixed", self.session(ASH, "ash").session_id))

    async def test_the_hop_limit_counts_across_projects(self) -> None:
        await self.dispatch({"text": "go"})
        self.forward("steward", SDE, "go on")
        await self.run_inbox(MAIN)
        self.forward("sde", SDE, "review", target_agent="code-reviewer")
        found: list[str] = []

        def last(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            try:
                caps.forward_to_inbox(SDE, "again", target_agent="sde")
            except ForwardRefused as exc:
                found.append(str(exc))
            return Silent()

        self.agents["code-reviewer"].script = last
        await self.run_inbox(SDE)
        self.assertEqual(len(found), 1)
        self.assertIn(f"limit {MAX_FORWARD_HOPS}", found[0])

    async def test_work_at_the_limit_is_not_sent_across(self) -> None:
        self.sessions.save(SessionRef(
            session_id="s", inbox_id=ASH, executor_id="ash", correlation_key=CHAT,
            forward=ForwardSpec(target_inbox=ASH, return_to=ASH, delegation_id="d", hop_count=MAX_FORWARD_HOPS)))
        refusal = await self.refused(target_inbox=MAIN, text="x")
        self.assertIn("limit", str(refusal))

    async def test_every_crossing_passes_the_gate(self) -> None:
        self.gate("block", "not today")
        refusal = await self.refused(target_inbox=MAIN, text="x")
        self.assertEqual(str(refusal), "not today")
        tool, args = self.permissions.calls[-1]
        self.assertEqual((tool, args["inbox_id"], args["agent_id"], args["target_inbox"], args["hop_count"]),
                         ("inbox.forward", ASH, "ash", MAIN, 1))

    async def test_an_approved_forward_is_judged_by_the_list_at_release(self) -> None:
        self.gate("defer")
        found: list[str] = []

        def script(events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            request = session.pending_request_id if session is not None else None
            try:
                caps.forward_to_inbox(MAIN, "x", approved_request_id=request)
            except ForwardRefused as exc:
                found.append(str(exc))
                if exc.action == "defer":
                    return Suspend(request_id="req", prompt="may I?", session_checkpoint="")
            return Silent()

        self.agents["ash"].script = script
        self.say()
        await self.run_inbox(ASH)
        self.allowed = ()
        self.inbox.append(Event(inbox_id=ASH, source="switchboard:feishu", external_id="ok",
                                payload={"event_type": "approval.decided", "approval_request_id": "req",
                                         "approved": True}))
        await self.run_inbox(ASH)
        self.assertEqual(len(found), 2)
        self.assertIn("not an inbox of this project", found[1])
        self.assertEqual(self.queued(MAIN), [])

    async def test_an_approved_forward_still_listed_goes_out(self) -> None:
        self.gate("defer")

        def script(events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            request = session.pending_request_id if session is not None else None
            try:
                caps.forward_to_inbox(MAIN, "x", approved_request_id=request)
            except ForwardRefused:
                return Suspend(request_id="req", prompt="may I?", session_checkpoint="")
            return Silent()

        self.agents["ash"].script = script
        self.say()
        await self.run_inbox(ASH)
        self.assertEqual(self.queued(MAIN), [])
        self.inbox.append(Event(inbox_id=ASH, source="switchboard:feishu", external_id="ok",
                                payload={"event_type": "approval.decided", "approval_request_id": "req",
                                         "approved": True}))
        await self.run_inbox(ASH)
        self.assertEqual(len(self.queued(MAIN)), 1)

    async def test_a_forward_is_dropped_when_the_list_loses_it_before_the_run_ends(self) -> None:
        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            caps.forward_to_inbox(MAIN, "secret plan", return_to=ASH, title="Plan")
            self.allowed = ()
            return Emit(messages=("sent it",))

        self.agents["ash"].script = script
        self.say()
        await self.run_inbox(ASH)
        self.assertEqual(self.queued(MAIN), [])
        [notice] = self.sent("forward_refused")
        self.assertIn(MAIN, notice.content)
        self.assertEqual((notice.metadata["correlation_key"], notice.metadata["outcome_kind"]), (CHAT, "error"))
        # Nothing is awaited from work that did not go out.
        forward = self.session(ASH, "ash").forward
        self.assertEqual(forward.pending_returns if forward is not None else (), ())

    async def test_a_result_is_held_when_the_list_lost_its_project(self) -> None:
        await self.dispatch({"text": "part one", "return_to": ASH})
        self.allowed = ()
        self.answer("steward", "the secret result")
        await self.run_inbox(MAIN)
        self.assertEqual(self.queued(ASH), [])
        [card] = self.sent("approval_request")
        self.assertIn("not an inbox of this project", card.content)
        self.assertNotIn("the secret result", self.outbound())


    async def test_a_held_result_is_judged_again_when_it_is_released(self) -> None:
        for listed_again in (False, True):
            with self.subTest(listed_again=listed_again):
                self.setUp()
                await self.dispatch({"text": "part one", "return_to": ASH})
                self.allowed = ()
                self.answer("steward", "the result")
                await self.run_inbox(MAIN)
                [card] = self.sent("approval_request")
                if listed_again:
                    self.allowed = (MAIN,)
                self.inbox.append(Event(
                    inbox_id=MAIN, source="switchboard:feishu", external_id="ok",
                    payload={"event_type": "approval.decided", "approved": True,
                             "approval_request_id": card.metadata["approval_request_id"]}))
                await self.run_inbox(MAIN)
                self.assertEqual([e.payload["text"] for e in self.queued(ASH)], ["the result"] if listed_again else [])


class SeparateTaskTests(CrossCase):
    async def test_two_tasks_of_one_chat_stay_two(self) -> None:
        [one] = await self.dispatch({"text": "first"})
        [two] = await self.dispatch({"text": "second"})
        self.assertNotEqual(one.correlation_key, two.correlation_key)
        self.answer("steward", "ok")
        await self.run_inbox(MAIN)
        runs = self.agents["steward"].runs
        self.assertEqual([[e.payload["text"] for e in events] for events, _ in runs], [["first"], ["second"]])
        sessions = [s for _, s in runs if s is not None]
        self.assertEqual(len({s.session_id for s in sessions}), 2)
        self.assertEqual(len({s.work_key for s in sessions}), 2)
        self.assertEqual({s.correlation_key for s in sessions}, {one.correlation_key, two.correlation_key})
        self.assertEqual([m.metadata["correlation_key"] for m in self.sent()], [CHAT, CHAT])

    async def test_two_tasks_of_one_run_stay_two_whatever_they_ask_for(self) -> None:
        for back in (None, ASH):
            with self.subTest(back=back):
                self.setUp()
                await self.dispatch({"text": "first", "return_to": back}, {"text": "second", "return_to": back})
                await self.run_inbox(MAIN)
                runs = self.agents["steward"].runs
                self.assertEqual([[e.payload["text"] for e in events] for events, _ in runs], [["first"], ["second"]])
                self.assertEqual(len({s.session_id for _, s in runs if s is not None}), 2)

    async def test_the_tasks_stay_two_deeper_in_the_project(self) -> None:
        for back in (None, MAIN):
            with self.subTest(back=back):
                self.setUp()
                [one] = await self.dispatch({"text": "first"})
                [two] = await self.dispatch({"text": "second"})

                def relay(events: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
                    caps.forward_to_inbox(SDE, events[-1].payload["text"], return_to=back)
                    return Silent()

                self.agents["steward"].script = relay
                await self.run_inbox(MAIN)
                self.agents["sde"].script = lambda e, _s, _c: Emit(messages=(f"{e[0].payload['text']} done",))
                await self.run_inbox(SDE)
                runs = self.agents["sde"].runs
                self.assertEqual([[e.payload["text"] for e in events] for events, _ in runs], [["first"], ["second"]])
                sessions = [s for _, s in runs if s is not None]
                self.assertEqual({s.work_key for s in sessions}, {one.correlation_key, two.correlation_key})
                if back is None:
                    self.assertEqual([m.metadata["correlation_key"] for m in self.sent()], [CHAT, CHAT])
                    continue
                # Each result goes back to the session of its own task.
                stewards = {s.correlation_key: s.session_id for s in self.sessions.list_sessions(inbox_id=MAIN)}
                self.assertEqual([e.session_id for e in self.queued(MAIN)],
                                 [stewards[one.correlation_key], stewards[two.correlation_key]])

    async def test_a_result_whose_steward_session_is_gone_stays_with_its_task(self) -> None:
        [one] = await self.dispatch({"text": "first"})
        self.forward("steward", SDE, "first", return_to=MAIN)
        await self.run_inbox(MAIN)
        self.sessions.save(replace(self.session(MAIN, "steward"), status="closed", correlation_key=None))
        self.answer("sde", "done")
        await self.run_inbox(SDE)
        [event] = self.queued(MAIN)
        self.assertEqual((event.session_id, event.correlation_key), (None, one.correlation_key))
        self.answer("steward", "answered")
        await self.run_inbox(MAIN)
        self.assertEqual(self.sent()[-1].metadata["correlation_key"], CHAT)

    async def test_a_result_is_not_read_by_the_other_task(self) -> None:
        [one] = await self.dispatch({"text": "first", "return_to": ASH})
        [two] = await self.dispatch({"text": "second", "return_to": ASH})
        self.agents["steward"].script = lambda e, _s, _c: Emit(messages=(f"{e[0].payload['text']} done",))
        await self.run_inbox(MAIN)
        back = self.queued(ASH)
        self.assertEqual({e.payload["from_task"]: e.payload["text"] for e in back},
                         {one.correlation_key: "first done", two.correlation_key: "second done"})

    async def test_a_project_s_own_chat_is_not_touched(self) -> None:
        # The same Feishu key as a conversation of the project itself.
        self.ingest(MAIN, key=CHAT, text="own")
        await self.run_inbox(MAIN)
        await self.dispatch({"text": "sent"})
        await self.run_inbox(MAIN)
        first, second = (s for _, s in self.agents["steward"].runs)
        assert first is not None and second is not None
        self.assertNotEqual(first.session_id, second.session_id)


class SupplementTests(CrossCase):
    async def supplement(self, key: str | None, text: str = "also this") -> None:
        await self.dispatch({"text": text, "continues": key})

    async def test_it_joins_the_session_of_its_task(self) -> None:
        [one] = await self.dispatch({"text": "first"})
        [two] = await self.dispatch({"text": "second"})
        self.answer("steward", "ok")
        await self.run_inbox(MAIN)
        await self.supplement(one.correlation_key)
        await self.run_inbox(MAIN)
        first, second, again = self.agents["steward"].runs
        assert first[1] is not None and second[1] is not None and again[1] is not None
        self.assertEqual(again[1].session_id, first[1].session_id)
        self.assertEqual(again[1].work_key, first[1].work_key)
        self.assertEqual(self.sent()[-1].metadata["correlation_key"], CHAT)
        self.assertNotEqual(two.correlation_key, one.correlation_key)

    async def test_one_result_answers_a_task_and_its_supplement(self) -> None:
        [one] = await self.dispatch({"text": "first", "return_to": ASH})
        self.agents["steward"].script = lambda _e, _s, _c: Suspend(request_id="r", prompt="?", session_checkpoint="")
        await self.run_inbox(MAIN)
        await self.supplement(one.correlation_key)
        self.answer("steward", "both done")
        await self.run_inbox(MAIN)
        self.assertEqual(len({s.session_id for _, s in self.agents["steward"].runs if s is not None}), 1)
        [back] = self.queued(ASH)
        self.assertEqual((back.payload["text"], back.payload["from_task"]), ("both done", one.correlation_key))
        self.assertEqual(self.sent(), [])

    async def test_one_sent_before_the_task_started_is_read_with_it(self) -> None:
        for back in (None, ASH):
            with self.subTest(back=back):
                self.setUp()
                [one] = await self.dispatch({"text": "first", "return_to": back})
                await self.supplement(one.correlation_key)
                self.answer("steward", "done")
                await self.run_inbox(MAIN)
                [(events, _)] = self.agents["steward"].runs
                self.assertEqual([e.payload["text"] for e in events], ["first", "also this"])
                self.assertEqual((len(self.queued(ASH)), len(self.sent())), (1, 0) if back else (0, 1))

    async def test_a_run_in_progress_takes_it_in(self) -> None:
        self.policy = "fold"
        [one] = await self.dispatch({"text": "first", "return_to": ASH})
        await self.dispatch({"text": "second"})
        seen: list[str] = []

        def script(events: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            texts = [e.payload["text"] for e in events]
            if texts == ["first"]:
                self.inbox.append(Event(
                    inbox_id=MAIN, source=FORWARD_SOURCE, external_id="late", correlation_key=one.correlation_key,
                    payload={"text": "also this", "supplement": True, "task_key": one.correlation_key, "hop_count": 1,
                             "origin_key": CHAT, "forwarded_from": ASH}))
            seen.extend([*texts, *(e.payload["text"] for e in caps.poll_folded_events())])
            return Emit(messages=("done",))

        self.agents["steward"].script = script
        await self.run_inbox(MAIN)
        self.assertEqual(seen, ["first", "also this", "second"])
        self.assertEqual((len(self.queued(ASH)), len(self.sent())), (1, 1))

    async def test_it_reaches_the_developer_s_session_and_worktree(self) -> None:
        # Directly answering, handed on with the result owed, and as a part the steward waits for.
        for owed, part in ((None, None), (ASH, None), (ASH, MAIN), (None, MAIN)):
            with self.subTest(owed=owed, part=part):
                self.setUp()
                [one] = await self.dispatch({"text": "first", "return_to": owed})

                def relay(events: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
                    caps.forward_to_inbox(SDE, events[-1].payload["text"], return_to=part)
                    return Silent()

                self.agents["steward"].script = relay
                await self.run_inbox(MAIN)
                self.agents["sde"].script = lambda _e, _s, _c: Suspend(request_id="r", prompt="?", session_checkpoint="")
                await self.run_inbox(SDE)
                await self.supplement(one.correlation_key)
                await self.run_inbox(MAIN)
                self.answer("sde", "both done")
                await self.run_inbox(SDE)
                runs = self.agents["sde"].runs
                self.assertEqual([e.payload["text"] for events, _ in runs for e in events], ["first", "also this"])
                self.assertEqual(len({s.session_id for _, s in runs if s is not None}), 1)
                self.assertEqual({s.work_key for _, s in runs if s is not None}, {one.correlation_key})
                results = [e for e in self.inbox.all_events() if e.source == RETURN_SOURCE]
                # One result for both, never one each.
                self.assertEqual([e.inbox_id for e in results], [x for x in (part or owed,) if x])
                self.assertEqual(len(self.sent()), 0 if results else 1)

    async def test_only_an_open_task_of_this_conversation_takes_one(self) -> None:
        [one] = await self.dispatch({"text": "first"})
        self.answer("steward", "ok")
        await self.run_inbox(MAIN)
        for key in ("feishu:p2p:oc_kun", "feishu:1", "forward:user:main:forward:evt_none:0:feishu:p2p:oc_kun"):
            with self.subTest(key=key):
                refusal = await self.refused(target_inbox=MAIN, text="x", continues=key)
                self.assertIn(f"'{key}' is not an open task", str(refusal))
        # Another conversation of the same agent.
        self.ingest(ASH, key="feishu:p2p:oc_other", text="x")
        found: list[str] = []

        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            try:
                caps.forward_to_inbox(MAIN, "x", continues=one.correlation_key)
            except ForwardRefused as exc:
                found.append(str(exc))
            return Silent()

        self.agents["ash"].script = script
        await self.run_inbox(ASH)
        self.assertEqual(len(found), 1)
        # A task that is over.
        done = self.sessions.find_by_correlation_key(MAIN, one.correlation_key or "")
        assert done is not None
        self.sessions.save(replace(done, status="closed", correlation_key=None))
        refusal = await self.refused(target_inbox=MAIN, text="x", continues=one.correlation_key)
        self.assertIn("send it as new work", str(refusal))

    async def test_it_asks_for_no_result_of_its_own_and_names_no_agent(self) -> None:
        [one] = await self.dispatch({"text": "first"})
        refusal = await self.refused(target_inbox=MAIN, text="x", continues=one.correlation_key, return_to=ASH)
        self.assertIn("return_to", str(refusal))
        refusal = await self.refused(target_inbox=ASH, text="x", continues=one.correlation_key, target_agent="writer")
        self.assertIn("continues", str(refusal))

    async def test_a_project_agent_has_no_use_of_it(self) -> None:
        [one] = await self.dispatch({"text": "first"})
        refusal = await self.refused(MAIN, "steward", target_inbox=SDE, text="x", continues=one.correlation_key)
        self.assertIn("continues", str(refusal))

    async def test_one_run_may_start_a_task_and_add_to_it(self) -> None:
        def script(_e: Sequence[Event], _s: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
            started = caps.forward_to_inbox(MAIN, "first")
            caps.forward_to_inbox(MAIN, "also this", continues=started.correlation_key)
            return Silent()

        self.agents["ash"].script = script
        self.say()
        await self.run_inbox(ASH)
        await self.run_inbox(MAIN)
        [(events, _)] = self.agents["steward"].runs
        self.assertEqual([e.payload["text"] for e in events], ["first", "also this"])

    async def test_closing_the_sender_withdraws_what_has_not_started(self) -> None:
        [one] = await self.dispatch({"text": "first"})
        await self.supplement(one.correlation_key)
        ash = self.session(ASH, "ash")
        self.inbox.append(Event(inbox_id=ASH, source=CLOSE_SOURCE, external_id="c", session_id=ash.session_id,
                                payload={"action": "close_session"}))
        await self.run_inbox(ASH)
        await self.run_inbox(MAIN)
        self.assertEqual(self.agents["steward"].runs, [])


class UnchangedTests(CrossCase):
    async def test_forwards_inside_a_project_carry_nothing_new(self) -> None:
        self.forward("steward", SDE, "fix", return_to=MAIN)
        self.ingest()
        await self.run_inbox()
        [event] = self.queued(SDE)
        self.assertEqual(event.correlation_key, "feishu:1")
        for key in ("task_key", "return_key", "supplement", "from_task"):
            self.assertNotIn(key, event.payload)
        self.answer("sde", "done")
        await self.run_inbox(SDE)
        [back] = self.queued(MAIN)
        for key in ("task_key", "return_key", "supplement", "from_task"):
            self.assertNotIn(key, back.payload)

    async def test_the_gate_sees_what_it_saw_before(self) -> None:
        self.forward("steward", SDE, "fix")
        self.ingest()
        await self.run_inbox()
        _, args = self.permissions.calls[-1]
        self.assertNotIn("continues", args)


class StorageTests(CrossCase):
    async def test_the_task_of_a_session_survives_a_restart(self) -> None:
        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "x.db"
        spec = ForwardSpec(target_inbox=MAIN, task_key="forward:user:main:x", return_key="forward:user:main:y")
        db = SQLiteDatabase(path)
        SQLiteSessionStoreAdapter(db).save(SessionRef(session_id="s", inbox_id=MAIN, forward=spec))
        db.close()
        db = SQLiteDatabase(path)
        self.addCleanup(db.close)
        stored = SQLiteSessionStoreAdapter(db).get("s")
        assert stored is not None
        self.assertEqual(stored.forward, spec)

    async def test_a_gate_decision_sees_what_is_added_to(self) -> None:
        [one] = await self.dispatch({"text": "first"})
        self.permissions.decisions["inbox.forward"] = PermissionDecision("block", "no")
        await self.refused(target_inbox=MAIN, text="x", continues=one.correlation_key)
        _, args = self.permissions.calls[-1]
        self.assertEqual(args["continues"], one.correlation_key)
