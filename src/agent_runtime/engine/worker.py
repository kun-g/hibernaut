from __future__ import annotations

import asyncio
from collections.abc import Callable
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
    _poll_folded: Callable[[], list[Event]]
    _is_interrupted: Callable[[], bool]

    def poll_folded_events(self) -> list[Event]:
        return self._poll_folded()

    def is_interrupted(self) -> bool:
        return self._is_interrupted()


@dataclass(frozen=True, slots=True)
class RunReport:
    inbox_id: str
    session_id: str | None
    event_ids: tuple[str, ...]
    outcome: RunOutcome


class _RunInterrupted(Exception):
    pass


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
        interrupt_poll_interval_seconds: float = 0.02,
        max_interrupt_restarts: int = 10,
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
        self._interrupt_poll_interval_seconds = interrupt_poll_interval_seconds
        self._max_interrupt_restarts = max_interrupt_restarts
        self._wake_signals: dict[str, asyncio.Event] = {}

    @property
    def new_event_policy(self) -> NewEventPolicy:
        return self._new_event_policy

    def notify_new_event(self, inbox_id: str) -> None:
        signal = self._wake_signals.get(inbox_id)
        if signal is not None:
            signal.set()

    async def process_inbox(self, inbox_id: str) -> list[RunReport]:
        if not self._lock.acquire(inbox_id, self._worker_id, self._lease_ttl_seconds):
            return []

        reports: list[RunReport] = []
        interrupt_restarts = 0
        wake_signal = asyncio.Event()
        self._wake_signals[inbox_id] = wake_signal

        try:
            while True:
                pending = self._inbox.fetch_pending(inbox_id)
                if not pending:
                    break

                restarted = False
                batches = self._group_into_batches(pending)
                for session_ref, batch in batches:
                    self._lock.renew(inbox_id, self._worker_id, self._lease_ttl_seconds)
                    wake_signal.clear()

                    consumed_ids: set[str] = {evt.event_id for evt in batch}
                    folded_events: list[Event] = []
                    target_session_key = (
                        session_ref.session_id if session_ref is not None else batch[0].session_id
                    )
                    interrupted_flag = asyncio.Event()

                    def _poll_folded(
                        _inbox_id: str = inbox_id,
                        _consumed: set[str] = consumed_ids,
                        _folded: list[Event] = folded_events,
                        _target_key: str | None = target_session_key,
                    ) -> list[Event]:
                        if self._new_event_policy != "fold":
                            return []
                        newly_folded: list[Event] = []
                        for candidate in self._inbox.fetch_pending(_inbox_id):
                            if candidate.event_id in _consumed:
                                continue
                            cand_session = self._resolve_session(candidate)
                            cand_key = (
                                cand_session.session_id
                                if cand_session is not None
                                else candidate.session_id
                            )
                            if cand_key == _target_key:
                                _consumed.add(candidate.event_id)
                                _folded.append(candidate)
                                newly_folded.append(candidate)
                        return newly_folded

                    caps: RunCapabilities = _ScopedCapabilities(
                        memory=self._memory,
                        timers=self._timers,
                        permissions=self._permissions,
                        _poll_folded=_poll_folded,
                        _is_interrupted=interrupted_flag.is_set,
                    )

                    allow_interrupt = (
                        self._new_event_policy == "interrupt"
                        and interrupt_restarts < self._max_interrupt_restarts
                    )
                    try:
                        outcome = await self._execute_batch(
                            inbox_id=inbox_id,
                            batch=batch,
                            session_ref=session_ref,
                            caps=caps,
                            known_ids=consumed_ids,
                            interrupted_flag=interrupted_flag,
                            wake_signal=wake_signal,
                            allow_interrupt=allow_interrupt,
                        )
                    except _RunInterrupted:
                        # Do NOT apply outcome or mark_done; keep original batch pending
                        # and merge with newly arrived events under the same lease.
                        interrupt_restarts += 1
                        restarted = True
                        break

                    self._apply_outcome(inbox_id, session_ref, outcome)
                    all_done_ids = [evt.event_id for evt in batch] + [
                        evt.event_id for evt in folded_events
                    ]
                    self._inbox.mark_done(all_done_ids)
                    reports.append(
                        RunReport(
                            inbox_id=inbox_id,
                            session_id=session_ref.session_id if session_ref else None,
                            event_ids=tuple(all_done_ids),
                            outcome=outcome,
                        )
                    )

                if restarted:
                    continue
        finally:
            self._wake_signals.pop(inbox_id, None)
            self._lock.release(inbox_id, self._worker_id)

        return reports

    async def _execute_batch(
        self,
        *,
        inbox_id: str,
        batch: list[Event],
        session_ref: SessionRef | None,
        caps: RunCapabilities,
        known_ids: set[str],
        interrupted_flag: asyncio.Event,
        wake_signal: asyncio.Event,
        allow_interrupt: bool,
    ) -> RunOutcome:
        if not allow_interrupt:
            return await self._execute_with_retry(batch, session_ref, caps)

        exec_task = asyncio.create_task(self._execute_with_retry(batch, session_ref, caps))
        watcher_task = asyncio.create_task(
            self._watch_for_interrupt(
                inbox_id=inbox_id,
                known_ids=known_ids,
                interrupted_flag=interrupted_flag,
                wake_signal=wake_signal,
                exec_task=exec_task,
            )
        )
        try:
            outcome = await exec_task
            if interrupted_flag.is_set():
                raise _RunInterrupted()
            return outcome
        except asyncio.CancelledError:
            if interrupted_flag.is_set():
                raise _RunInterrupted() from None
            raise
        finally:
            watcher_task.cancel()
            try:
                await watcher_task
            except asyncio.CancelledError:
                pass

    async def _watch_for_interrupt(
        self,
        *,
        inbox_id: str,
        known_ids: set[str],
        interrupted_flag: asyncio.Event,
        wake_signal: asyncio.Event,
        exec_task: asyncio.Task[RunOutcome],
    ) -> None:
        while not exec_task.done():
            has_new = any(
                evt.event_id not in known_ids for evt in self._inbox.fetch_pending(inbox_id)
            )
            if has_new:
                interrupted_flag.set()
                if not exec_task.done():
                    exec_task.cancel()
                return
            try:
                await asyncio.wait_for(
                    wake_signal.wait(),
                    timeout=self._interrupt_poll_interval_seconds,
                )
                wake_signal.clear()
            except asyncio.TimeoutError:
                continue

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
                if resolved_key is None and prev_key is None and self._new_event_policy == "interrupt":
                    # Merge un-sessioned pending events after interrupt restart.
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
        caps: RunCapabilities,
    ) -> RunOutcome:
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                return await self._executor.execute(batch, session_ref, caps)
            except asyncio.CancelledError:
                raise
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
