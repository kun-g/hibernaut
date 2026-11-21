from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Never, assert_never

from agent_runtime.core.models import (
    Emit,
    Event,
    NewEventPolicy,
    RunOutcome,
    SessionRef,
    Silent,
    Suspend,
    new_id,
    utc_now_iso,
)
from agent_runtime.core.ports import (
    ExecutorPort,
    InboxPort,
    LockPort,
    MemoryPort,
    OutboxPort,
    PermissionPort,
    RunCapabilities,
    SessionStorePort,
    TimerPort,
)


@dataclass(frozen=True, slots=True)
class _ScopedCapabilities:
    memory: MemoryPort
    timers: TimerPort
    permissions: PermissionPort


@dataclass(frozen=True, slots=True)
class RunReport:
    inbox_id: str
    session_id: str | None
    event_ids: tuple[str, ...]
    outcome: RunOutcome


class AgentWorker:
    def __init__(
        self,
        *,
        worker_id: str,
        inbox: InboxPort,
        timers: TimerPort,
        memory: MemoryPort,
        permissions: PermissionPort,
        outbox: OutboxPort,
        executor: ExecutorPort,
        lock: LockPort,
        sessions: SessionStorePort,
        lease_ttl_seconds: float = 60.0,
        max_retries: int = 2,
        new_event_policy: NewEventPolicy = "defer",
    ) -> None:
        self._worker_id = worker_id
        self._inbox = inbox
        self._timers = timers
        self._memory = memory
        self._permissions = permissions
        self._outbox = outbox
        self._executor = executor
        self._lock = lock
        self._sessions = sessions
        self._lease_ttl_seconds = lease_ttl_seconds
        self._max_retries = max_retries
        self._new_event_policy: NewEventPolicy = new_event_policy
        self._caps: RunCapabilities = _ScopedCapabilities(
            memory=memory,
            timers=timers,
            permissions=permissions,
        )

    @property
    def new_event_policy(self) -> NewEventPolicy:
        return self._new_event_policy

    async def process_inbox(self, inbox_id: str) -> list[RunReport]:
        if not self._lock.acquire(inbox_id, self._worker_id, self._lease_ttl_seconds):
            return []

        reports: list[RunReport] = []
        try:
            while True:
                pending = self._inbox.fetch_pending(inbox_id)
                if not pending:
                    break

                batches = self._group_into_batches(pending)
                for session_ref, batch in batches:
                    self._lock.renew(inbox_id, self._worker_id, self._lease_ttl_seconds)
                    outcome = await self._execute_with_retry(batch, session_ref)
                    self._apply_outcome(inbox_id, session_ref, outcome)
                    self._inbox.mark_done([evt.event_id for evt in batch])
                    reports.append(
                        RunReport(
                            inbox_id=inbox_id,
                            session_id=session_ref.session_id if session_ref else None,
                            event_ids=tuple(evt.event_id for evt in batch),
                            outcome=outcome,
                        )
                    )

                # With default 'defer' policy, re-check if new events arrived during the Run.
                if self._new_event_policy != "defer":
                    break
        finally:
            self._lock.release(inbox_id, self._worker_id)

        return reports

    def _group_into_batches(
        self,
        events: list[Event],
    ) -> list[tuple[SessionRef | None, list[Event]]]:
        batches: list[tuple[SessionRef | None, list[Event]]] = []
        for event in events:
            resolved_session = self._resolve_session(event)
            resolved_key = resolved_session.session_id if resolved_session else event.session_id

            if batches:
                prev_session, prev_events = batches[-1]
                prev_key = prev_session.session_id if prev_session else prev_events[-1].session_id
                if resolved_key is not None and resolved_key == prev_key:
                    prev_events.append(event)
                    continue

            batches.append((resolved_session, [event]))
        return batches

    def _resolve_session(self, event: Event) -> SessionRef | None:
        request_id = event.payload.get("approval_request_id")
        if isinstance(request_id, str) and request_id:
            suspended = self._sessions.find_by_request_id(request_id)
            if suspended is not None:
                return suspended
        if event.session_id:
            return self._sessions.get(event.session_id)
        return None

    async def _execute_with_retry(
        self,
        batch: list[Event],
        session_ref: SessionRef | None,
    ) -> RunOutcome:
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                return await self._executor.execute(batch, session_ref, self._caps)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < self._max_retries:
                    await asyncio.sleep(0.01 * (attempt + 1))
        assert last_exc is not None
        raise last_exc

    def _apply_outcome(
        self,
        inbox_id: str,
        session_ref: SessionRef | None,
        outcome: RunOutcome,
    ) -> None:
        match outcome:
            case Silent():
                if session_ref is not None and session_ref.pending_request_id is not None:
                    self._sessions.save(
                        SessionRef(
                            session_id=session_ref.session_id,
                            inbox_id=inbox_id,
                            checkpoint=None,
                            pending_request_id=None,
                            updated_at=utc_now_iso(),
                        )
                    )
            case Emit(messages=messages, session_id=emit_session_id):
                for text in messages:
                    self._outbox.enqueue(
                        inbox_id=inbox_id,
                        content=text,
                        metadata={"kind": "emit"},
                    )
                target_sid = emit_session_id or (session_ref.session_id if session_ref else None)
                if target_sid is not None:
                    self._sessions.save(
                        SessionRef(
                            session_id=target_sid,
                            inbox_id=inbox_id,
                            checkpoint=None,
                            pending_request_id=None,
                            updated_at=utc_now_iso(),
                        )
                    )
            case Suspend(
                request_id=request_id,
                prompt=prompt,
                session_checkpoint=checkpoint,
                session_id=suspend_session_id,
            ):
                target_sid = suspend_session_id or (
                    session_ref.session_id if session_ref else new_id("sess")
                )
                self._sessions.save(
                    SessionRef(
                        session_id=target_sid,
                        inbox_id=inbox_id,
                        checkpoint=checkpoint,
                        pending_request_id=request_id,
                        updated_at=utc_now_iso(),
                    )
                )
                self._outbox.enqueue(
                    inbox_id=inbox_id,
                    content=prompt,
                    metadata={
                        "kind": "approval_request",
                        "request_id": request_id,
                        "session_id": target_sid,
                    },
                )
            case _ as unreachable:
                _exhaustive(unreachable)


def _exhaustive(value: Never) -> Never:
    assert_never(value)
