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


class ExecutorFailed(RuntimeError):
    pass


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
                    target_batch_key = self._batch_key(batch[0], session_ref)
                    interrupted_flag = asyncio.Event()

                    def _poll_folded(
                        _inbox_id: str = inbox_id,
                        _consumed: set[str] = consumed_ids,
                        _folded: list[Event] = folded_events,
                        _target_key: tuple[str, str] | None = target_batch_key,
                    ) -> list[Event]:
                        if self._new_event_policy != "fold" or _target_key is None:
                            return []
                        newly_folded: list[Event] = []
                        for candidate in self._inbox.fetch_pending(_inbox_id):
                            if candidate.event_id in _consumed:
                                continue
                            cand_session = self._resolve_session(candidate)
                            cand_key = self._batch_key(candidate, cand_session)
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

                    batch_correlation_key = self._extract_correlation_key(
                        batch + folded_events,
                        session_ref,
                    )
                    final_session_id = self._apply_outcome(
                        inbox_id,
                        session_ref,
                        outcome,
                        batch_correlation_key,
                    )
                    all_done_ids = [evt.event_id for evt in batch] + [
                        evt.event_id for evt in folded_events
                    ]
                    self._inbox.mark_done(all_done_ids)
                    reports.append(
                        RunReport(
                            inbox_id=inbox_id,
                            session_id=final_session_id,
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

    def _batch_key(
        self,
        event: Event,
        resolved_session: SessionRef | None,
    ) -> tuple[str, str] | None:
        if resolved_session is not None:
            return ("session", resolved_session.session_id)
        if event.session_id is not None:
            return ("session", event.session_id)
        if event.correlation_key is not None:
            return ("corr", event.correlation_key)
        return None

    def _extract_correlation_key(
        self,
        events: list[Event],
        session_ref: SessionRef | None,
    ) -> str | None:
        for evt in events:
            if evt.correlation_key:
                return evt.correlation_key
        return session_ref.correlation_key if session_ref is not None else None

    def _group_into_batches(
        self,
        events: list[Event],
    ) -> list[tuple[SessionRef | None, list[Event]]]:
        # Same-key events form one batch in seq order; batches run in order of first seq.
        # Events without session_id/correlation_key are unrelated, so each runs alone
        # (interrupt restarts still merge them). An approval decision runs alone and closes
        # its key for this pass; later events of that key wait for the next fetch so they
        # resolve against the post-decision session.
        batches: list[tuple[SessionRef | None, list[Event]]] = []
        open_batches: dict[tuple[str, str] | None, list[Event]] = {}
        closed_keys: set[tuple[str, str] | None] = set()
        for event in events:
            resolved_session = self._resolve_session(event)
            resolved_key = self._batch_key(event, resolved_session)
            if resolved_key in closed_keys:
                continue
            if self._is_approval_decision(event):
                if resolved_key is not None:
                    closed_keys.add(resolved_key)
                if resolved_key is None or resolved_key not in open_batches:
                    batches.append((resolved_session, [event]))
                continue
            if resolved_key is None and self._new_event_policy != "interrupt":
                batches.append((resolved_session, [event]))
                continue
            open_batch = open_batches.get(resolved_key)
            if open_batch is not None:
                open_batch.append(event)
                continue
            new_batch = [event]
            open_batches[resolved_key] = new_batch
            batches.append((resolved_session, new_batch))
        return batches

    @staticmethod
    def _is_approval_decision(event: Event) -> bool:
        request_id = event.payload.get("approval_request_id")
        return isinstance(request_id, str) and bool(request_id)

    def _resolve_session(self, event: Event) -> SessionRef | None:
        # 4-level priority: approval_request_id -> explicit session_id -> (inbox_id, correlation_key) -> new.
        request_id = event.payload.get("approval_request_id")
        if isinstance(request_id, str) and request_id:
            suspended = self._sessions.find_by_request_id(request_id)
            if suspended is not None:
                return suspended
        if event.session_id:
            return self._sessions.get(event.session_id)
        if event.correlation_key:
            return self._sessions.find_by_correlation_key(event.inbox_id, event.correlation_key)
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
        raise ExecutorFailed(f"Executor failed after {self._max_retries + 1} attempts") from last_exc

    def _apply_outcome(
        self,
        inbox_id: str,
        session_ref: SessionRef | None,
        outcome: RunOutcome,
        correlation_key: str | None,
    ) -> str | None:
        match outcome:
            case Silent():
                if session_ref is not None:
                    if (
                        session_ref.pending_request_id is not None
                        or (correlation_key is not None and session_ref.correlation_key != correlation_key)
                    ):
                        self._sessions.save(
                            SessionRef(
                                session_id=session_ref.session_id,
                                inbox_id=inbox_id,
                                correlation_key=correlation_key,
                                checkpoint=None,
                                pending_request_id=None,
                                updated_at=utc_now_iso(),
                            )
                        )
                    return session_ref.session_id
                if correlation_key is not None:
                    new_sid = new_id("sess")
                    self._sessions.save(
                        SessionRef(
                            session_id=new_sid,
                            inbox_id=inbox_id,
                            correlation_key=correlation_key,
                            checkpoint=None,
                            pending_request_id=None,
                            updated_at=utc_now_iso(),
                        )
                    )
                    return new_sid
                return None
            case Emit(messages=messages, session_id=emit_session_id):
                for text in messages:
                    self._outbox.enqueue(
                        inbox_id=inbox_id,
                        content=text,
                        metadata={"kind": "emit"},
                    )
                target_sid = emit_session_id or (
                    session_ref.session_id
                    if session_ref is not None
                    else (new_id("sess") if correlation_key is not None else None)
                )
                if target_sid is not None:
                    self._sessions.save(
                        SessionRef(
                            session_id=target_sid,
                            inbox_id=inbox_id,
                            correlation_key=correlation_key,
                            checkpoint=None,
                            pending_request_id=None,
                            updated_at=utc_now_iso(),
                        )
                    )
                return target_sid
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
                        correlation_key=correlation_key,
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
                return target_sid
            case _ as unreachable:
                _exhaustive(unreachable)


def _exhaustive(value: Never) -> Never:
    assert_never(value)
