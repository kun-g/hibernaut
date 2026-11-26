from __future__ import annotations

import asyncio
import unittest
from collections.abc import Callable, Sequence
from typing import Any

from agent_runtime.core.models import (
    RETURN_SOURCE,
    Emit,
    Event,
    FailureRoute,
    ForwardRefused,
    InboxBinding,
    InboxMatchRule,
    InboxRoute,
    PermissionDecision,
    RunLedgerEntry,
    RunOutcome,
    SessionRef,
    Silent,
)
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine.worker import AgentWorker, RunReport
from tests.support.fakes import (
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
