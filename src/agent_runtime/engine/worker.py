from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
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
    resolve_executor_id,
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


# Payload keys that pick where/how an executor runs (e.g. LocalCLIExecutor's workdir and CLI
# template). Events that disagree on them must not share a Run.
_TARGET_PAYLOAD_KEYS = ("executor_id", "explicit_workdir", "executor")


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
        lock: LockPort,
        sessions: SessionStorePort,
        executor: ExecutorPort | None = None,
        executors: Mapping[str, ExecutorPort] | None = None,
        project_default_executor: str | None = None,
        global_default_executor: str | None = None,
        on_worktree_reclaim: Callable[[str], object] | None = None,
        lease_ttl_seconds: float = 60.0,
        max_retries: int = 2,
        new_event_policy: NewEventPolicy = "defer",
        interrupt_poll_interval_seconds: float = 0.02,
        max_interrupt_restarts: int = 10,
    ) -> None:
        registry: dict[str, ExecutorPort] = dict(executors) if executors is not None else {}
        if executor is not None:
            registry.setdefault(executor.executor_id, executor)
        if not registry:
            raise ValueError("AgentWorker requires at least one executor")

        resolved_global = (
            global_default_executor
            or (executor.executor_id if executor is not None else next(iter(registry.keys())))
        )

        self._worker_id = worker_id
        self._inbox = inbox
        self._timers = timers
        self._memory = memory
        self._permissions = permissions
        self._outbox = outbox
        self._executors = registry
        self._project_default_executor = project_default_executor
        self._global_default_executor = resolved_global
        self._on_worktree_reclaim = on_worktree_reclaim
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
            # Recover any orphaned 'processing' events left by a crashed prior lease holder.
            orphans = self._inbox.fetch_processing(inbox_id)
            if orphans:
                self._inbox.requeue_processing([evt.event_id for evt in orphans])

            while True:
                claimed = self._inbox.claim_pending(inbox_id)
                if not claimed:
                    break

                restarted = False
                snapshot_ids = {evt.event_id for evt in claimed}
                batches = self._group_into_batches(claimed)
                scheduled_ids = {evt.event_id for _, b in batches for evt in b}
                unscheduled_ids = [evt.event_id for evt in claimed if evt.event_id not in scheduled_ids]
                completed_or_failed_ids: set[str] = set()

                try:
                    for batch_idx, (session_ref, batch) in enumerate(batches):
                        self._lock.renew(inbox_id, self._worker_id, self._lease_ttl_seconds)
                        wake_signal.clear()
                        batch_ids = [evt.event_id for evt in batch]

                        if self._is_close_session_batch(batch):
                            closed_sid = self._handle_close_session(batch, session_ref)
                            if closed_sid is not None:
                                self._inbox.bind_session(batch_ids, closed_sid)
                            self._inbox.mark_done(batch_ids)
                            completed_or_failed_ids.update(batch_ids)
                            reports.append(
                                RunReport(
                                    inbox_id=inbox_id,
                                    session_id=closed_sid,
                                    event_ids=tuple(batch_ids),
                                    outcome=Silent(),
                                )
                            )
                            continue

                        try:
                            selected_executor = self._select_executor(batch, session_ref)
                        except ExecutorFailed:
                            # Missing/unregistered executor for a suspended checkpoint: requeue
                            # pending so operator recovery can resume it cleanly.
                            remaining_ids = [
                                evt.event_id
                                for _, rem in batches[batch_idx:]
                                for evt in rem
                            ] + unscheduled_ids
                            self._inbox.requeue_processing(remaining_ids)
                            completed_or_failed_ids.update(remaining_ids)
                            raise

                        batch_correlation_key = self._extract_correlation_key(batch, session_ref)
                        batch_parent_sid = self._extract_parent_session_id(batch, session_ref)
                        batch_work_key = self._extract_work_key(
                            batch,
                            session_ref,
                            batch_correlation_key,
                            batch_parent_sid,
                        )

                        # Create & persist active SessionRef BEFORE execute(), then bind events immediately.
                        explicit_sid = next((e.session_id for e in batch if e.session_id), None)
                        active_session = self._ensure_active_session(
                            inbox_id=inbox_id,
                            session_ref=session_ref,
                            explicit_session_id=explicit_sid,
                            correlation_key=batch_correlation_key,
                            work_key=batch_work_key,
                            executor_id=selected_executor.executor_id,
                            parent_session_id=batch_parent_sid,
                        )
                        self._inbox.bind_session(batch_ids, active_session.session_id)

                        consumed_ids: set[str] = set(batch_ids)
                        folded_events: list[Event] = []
                        target_batch_key = self._batch_key(batch[0], active_session)
                        has_decision = any(self._is_approval_decision(evt) for evt in batch)
                        batch_target = self._batch_target(batch)
                        interrupted_flag = asyncio.Event()

                        def _poll_folded(
                            _inbox_id: str = inbox_id,
                            _consumed: set[str] = consumed_ids,
                            _folded: list[Event] = folded_events,
                            _target_key: tuple[str, str] | None = target_batch_key,
                            _has_decision: bool = has_decision,
                            _batch_target: tuple[object, ...] | None = batch_target,
                            _active_sid: str = active_session.session_id,
                        ) -> list[Event]:
                            if self._new_event_policy != "fold" or _target_key is None or _has_decision:
                                return []
                            newly_folded: list[Event] = []
                            for candidate in self._inbox.fetch_pending(_inbox_id):
                                if candidate.event_id in _consumed:
                                    continue
                                cand_session = self._resolve_session(candidate)
                                if self._batch_key(candidate, cand_session) != _target_key:
                                    continue
                                if self._is_approval_decision(candidate) or (
                                    _batch_target is not None
                                    and self._execution_target(candidate) != _batch_target
                                ):
                                    break
                                _consumed.add(candidate.event_id)
                                _folded.append(candidate)
                                newly_folded.append(candidate)
                            if newly_folded:
                                self._inbox.bind_session(
                                    [e.event_id for e in newly_folded],
                                    _active_sid,
                                )
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
                            and not has_decision
                        )
                        try:
                            outcome = await self._execute_batch(
                                executor=selected_executor,
                                inbox_id=inbox_id,
                                batch=batch,
                                session_ref=active_session,
                                caps=caps,
                                known_ids=snapshot_ids,
                                interrupted_flag=interrupted_flag,
                                wake_signal=wake_signal,
                                allow_interrupt=allow_interrupt,
                            )
                        except _RunInterrupted:
                            # Never write Outbox on interrupt; requeue current & remaining batches.
                            requeue_ids = (
                                batch_ids
                                + [e.event_id for e in folded_events]
                                + [evt.event_id for _, rem in batches[batch_idx + 1 :] for evt in rem]
                                + unscheduled_ids
                            )
                            self._inbox.requeue_processing(requeue_ids)
                            completed_or_failed_ids.update(requeue_ids)
                            interrupt_restarts += 1
                            restarted = True
                            break
                        except ExecutorFailed as exc:
                            all_failed_ids = batch_ids + [e.event_id for e in folded_events]
                            self._inbox.mark_failed(all_failed_ids, str(exc.__cause__ or exc))
                            completed_or_failed_ids.update(all_failed_ids)
                            # Requeue remaining unexecuted batches so they are not stranded in processing.
                            remaining_ids = [
                                evt.event_id
                                for _, rem in batches[batch_idx + 1 :]
                                for evt in rem
                            ] + unscheduled_ids
                            if remaining_ids:
                                self._inbox.requeue_processing(remaining_ids)
                                completed_or_failed_ids.update(remaining_ids)
                            raise

                        final_corr = self._extract_correlation_key(
                            batch + folded_events,
                            active_session,
                        )
                        final_work_key = self._extract_work_key(
                            batch + folded_events,
                            active_session,
                            final_corr,
                            active_session.parent_session_id,
                        )
                        final_session_id = self._apply_outcome(
                            inbox_id,
                            active_session,
                            outcome,
                            final_corr,
                            final_work_key,
                            selected_executor,
                        )
                        all_done_ids = batch_ids + [evt.event_id for evt in folded_events]
                        if final_session_id is not None and final_session_id != active_session.session_id:
                            self._inbox.bind_session(all_done_ids, final_session_id)
                        self._inbox.mark_done(all_done_ids)
                        completed_or_failed_ids.update(all_done_ids)
                        reports.append(
                            RunReport(
                                inbox_id=inbox_id,
                                session_id=final_session_id,
                                event_ids=tuple(all_done_ids),
                                outcome=outcome,
                            )
                        )
                finally:
                    # Any held-back or unfinished claimed events return to pending for the next pass.
                    straggler_ids = [
                        evt.event_id
                        for evt in claimed
                        if evt.event_id not in completed_or_failed_ids
                    ]
                    if straggler_ids:
                        self._inbox.requeue_processing(straggler_ids)

                if restarted:
                    continue
        finally:
            self._wake_signals.pop(inbox_id, None)
            self._lock.release(inbox_id, self._worker_id)

        return reports

    def _ensure_active_session(
        self,
        *,
        inbox_id: str,
        session_ref: SessionRef | None,
        explicit_session_id: str | None,
        correlation_key: str | None,
        work_key: str | None,
        executor_id: str,
        parent_session_id: str | None,
    ) -> SessionRef:
        if session_ref is not None:
            return session_ref
        # Persisting the child before execute() makes count_active_by_work_key include it at once.
        new_session = SessionRef(
            session_id=explicit_session_id or new_id("sess"),
            inbox_id=inbox_id,
            correlation_key=correlation_key,
            executor_id=executor_id,
            status="active",
            work_key=work_key,
            updated_at=utc_now_iso(),
            parent_session_id=parent_session_id,
        )
        self._sessions.save(new_session)
        return new_session

    @staticmethod
    def _is_close_session_batch(batch: list[Event]) -> bool:
        return any(
            evt.payload.get("action") == "close_session"
            or evt.payload.get("close_session") is True
            or evt.source == "session:close"
            for evt in batch
        )

    def _handle_close_session(
        self,
        batch: list[Event],
        session_ref: SessionRef | None,
    ) -> str | None:
        target = session_ref
        if target is None:
            for evt in batch:
                sid = evt.payload.get("session_id") or evt.session_id
                if isinstance(sid, str) and sid:
                    target = self._sessions.get(sid)
                    if target is not None:
                        break
        if target is None:
            return None
        self._sessions.close_session(target.session_id)
        if target.work_key and self._on_worktree_reclaim is not None:
            # Counted within the inbox: another inbox with the same work_key has its own worktree, and the
            # reclaim callback re-checks whatever its workspace is shared with.
            if self._sessions.count_active_by_work_key(target.work_key, target.inbox_id) == 0:
                self._on_worktree_reclaim(target.work_key)
        return target.session_id

    def _select_executor(
        self,
        batch: list[Event],
        session_ref: SessionRef | None,
    ) -> ExecutorPort:
        if self._resumes_suspended(batch, session_ref):
            assert session_ref is not None
            holder = self._suspended_holder(batch, session_ref)
            if holder is None:
                raise ExecutorFailed(
                    f"Suspended session '{session_ref.session_id}' predates executor binding and "
                    "its executor cannot be determined; set sessions.executor_id to resume it"
                )
            held = self._executors.get(holder)
            if held is None:
                raise ExecutorFailed(
                    f"Executor '{holder}' holding the suspended session is not registered"
                )
            return held

        event_executor_id: str | None = None
        target_events = [e for e in batch if not self._is_approval_decision(e)] or batch
        for evt in reversed(target_events):
            raw = evt.payload.get("executor_id")
            if isinstance(raw, str) and raw.strip():
                event_executor_id = raw.strip()
                break

        target_id = resolve_executor_id(
            event_executor_id,
            session_ref,
            self._project_default_executor,
            self._global_default_executor,
        )
        selected = self._executors.get(target_id)
        if selected is None:
            raise ExecutorFailed(f"Executor '{target_id}' is not registered")
        return selected

    async def _execute_batch(
        self,
        *,
        executor: ExecutorPort,
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
            return await self._execute_with_retry(executor, batch, session_ref, caps)

        exec_task = asyncio.create_task(
            self._execute_with_retry(executor, batch, session_ref, caps)
        )
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

    @staticmethod
    def _extract_parent_session_id(
        events: list[Event],
        session_ref: SessionRef | None,
    ) -> str | None:
        if session_ref is not None:
            return session_ref.parent_session_id
        for evt in events:
            raw = evt.payload.get("parent_session_id")
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
        return None

    def _extract_work_key(
        self,
        events: list[Event],
        session_ref: SessionRef | None,
        correlation_key: str | None,
        parent_session_id: str | None,
    ) -> str | None:
        for evt in reversed(events):
            raw = evt.payload.get("work_key")
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
        if session_ref is not None and session_ref.work_key:
            return session_ref.work_key
        if parent_session_id is not None:
            # A delegated child reuses the parent's worktree instead of opening its own.
            parent = self._sessions.get(parent_session_id)
            if parent is not None and parent.work_key:
                return parent.work_key
        return correlation_key

    def _group_into_batches(
        self,
        events: list[Event],
    ) -> list[tuple[SessionRef | None, list[Event]]]:
        resolved: list[tuple[Event, SessionRef | None, tuple[str, str] | None]] = []
        keys_with_decision: set[tuple[str, str]] = set()
        decision_holders: dict[tuple[str, str], str] = {}
        for event in events:
            resolved_session = self._resolve_session(event)
            resolved_key = self._batch_key(event, resolved_session)
            resolved.append((event, resolved_session, resolved_key))
            if resolved_key is not None and self._is_approval_decision(event):
                keys_with_decision.add(resolved_key)
                holder = self._suspended_holder([event], resolved_session)
                if holder is not None:
                    decision_holders.setdefault(resolved_key, holder)

        batches: list[tuple[SessionRef | None, list[Event]]] = []
        open_batches: dict[tuple[str, str] | None, list[Event]] = {}
        closed_keys: set[tuple[str, str] | None] = set()
        for event, resolved_session, resolved_key in resolved:
            if resolved_key in closed_keys:
                continue
            if resolved_key is not None and self._is_approval_decision(event):
                closed_keys.add(resolved_key)
                open_batch = open_batches.get(resolved_key)
                if open_batch is not None:
                    open_batch.append(event)
                else:
                    batches.append((resolved_session, [event]))
                continue
            if resolved_key is None and self._new_event_policy != "interrupt":
                batches.append((resolved_session, [event]))
                continue
            holder = decision_holders.get(resolved_key) if resolved_key is not None else None
            routed_to = self._routed_executor_id(event)
            if holder is not None and routed_to is not None and routed_to != holder:
                continue
            open_batch = open_batches.get(resolved_key)
            if open_batch is not None:
                if self._execution_target(event) != self._batch_target(open_batch):
                    if resolved_key not in keys_with_decision:
                        closed_keys.add(resolved_key)
                    continue
                open_batch.append(event)
                continue
            new_batch = [event]
            open_batches[resolved_key] = new_batch
            batches.append((resolved_session, new_batch))
        return batches

    @staticmethod
    def _resumes_suspended(batch: list[Event], session_ref: SessionRef | None) -> bool:
        if session_ref is None or not session_ref.pending_request_id:
            return False
        return any(
            evt.payload.get("approval_request_id") == session_ref.pending_request_id for evt in batch
        )

    def _suspended_holder(
        self,
        batch: list[Event],
        session_ref: SessionRef | None,
    ) -> str | None:
        if not self._resumes_suspended(batch, session_ref):
            return None
        assert session_ref is not None
        if session_ref.executor_id:
            return session_ref.executor_id
        for evt in batch:
            if evt.payload.get("approval_request_id") == session_ref.pending_request_id:
                named = self._routed_executor_id(evt)
                if named is not None:
                    return named
        return None

    @staticmethod
    def _routed_executor_id(event: Event) -> str | None:
        raw = event.payload.get("executor_id")
        return raw.strip() if isinstance(raw, str) and raw.strip() else None

    @staticmethod
    def _execution_target(event: Event) -> tuple[object, ...]:
        return tuple(event.payload.get(key) for key in _TARGET_PAYLOAD_KEYS)

    def _batch_target(self, batch: list[Event]) -> tuple[object, ...] | None:
        for evt in batch:
            if not self._is_approval_decision(evt):
                return self._execution_target(evt)
        return None

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
        executor: ExecutorPort,
        batch: list[Event],
        session_ref: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                return await executor.execute(batch, session_ref, caps)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < self._max_retries:
                    await asyncio.sleep(0.01 * (attempt + 1))
        assert last_exc is not None
        raise ExecutorFailed(f"Executor failed after {self._max_retries + 1} attempts") from last_exc

    def _resolve_executor_binding(
        self,
        selected_executor: ExecutorPort,
        session_ref: SessionRef | None,
        outcome_exec_sid: str | None,
    ) -> tuple[str, str | None]:
        exec_id = selected_executor.executor_id
        if outcome_exec_sid is not None:
            return exec_id, outcome_exec_sid
        if session_ref is not None and session_ref.executor_id == exec_id:
            return exec_id, session_ref.executor_session_id
        return exec_id, None

    def _apply_outcome(
        self,
        inbox_id: str,
        session_ref: SessionRef,
        outcome: RunOutcome,
        correlation_key: str | None,
        work_key: str | None,
        selected_executor: ExecutorPort,
    ) -> str | None:
        match outcome:
            case Silent(executor_session_id=exec_sid):
                exec_id, bound_exec_sid = self._resolve_executor_binding(
                    selected_executor,
                    session_ref,
                    exec_sid,
                )
                self._sessions.save(
                    SessionRef(
                        session_id=session_ref.session_id,
                        inbox_id=inbox_id,
                        correlation_key=correlation_key,
                        executor_id=exec_id,
                        executor_session_id=bound_exec_sid,
                        status="active",
                        work_key=work_key,
                        checkpoint=None,
                        pending_request_id=None,
                        updated_at=utc_now_iso(),
                        parent_session_id=session_ref.parent_session_id,
                    )
                )
                return session_ref.session_id
            case Emit(
                messages=messages,
                session_id=emit_session_id,
                executor_session_id=exec_sid,
            ):
                exec_id, bound_exec_sid = self._resolve_executor_binding(
                    selected_executor,
                    session_ref,
                    exec_sid,
                )
                for text in messages:
                    self._outbox.enqueue(
                        inbox_id=inbox_id,
                        content=text,
                        metadata={"kind": "emit"},
                    )
                target_sid = emit_session_id or session_ref.session_id
                if target_sid != session_ref.session_id:
                    # Session rotation: archive old session while new session inherits correlation_key & work_key.
                    self._sessions.archive_session(session_ref.session_id)
                self._sessions.save(
                    SessionRef(
                        session_id=target_sid,
                        inbox_id=inbox_id,
                        correlation_key=correlation_key,
                        executor_id=exec_id,
                        executor_session_id=bound_exec_sid,
                        status="active",
                        work_key=work_key,
                        checkpoint=None,
                        pending_request_id=None,
                        updated_at=utc_now_iso(),
                        parent_session_id=session_ref.parent_session_id,
                    )
                )
                return target_sid
            case Suspend(
                request_id=request_id,
                prompt=prompt,
                session_checkpoint=checkpoint,
                session_id=suspend_session_id,
                executor_session_id=exec_sid,
            ):
                exec_id, bound_exec_sid = self._resolve_executor_binding(
                    selected_executor,
                    session_ref,
                    exec_sid,
                )
                target_sid = suspend_session_id or session_ref.session_id
                # Re-suspending under the open request only updates the checkpoint; the card is already out.
                repeated = request_id == session_ref.pending_request_id and target_sid == session_ref.session_id
                if target_sid != session_ref.session_id:
                    self._sessions.archive_session(session_ref.session_id)
                self._sessions.save(
                    SessionRef(
                        session_id=target_sid,
                        inbox_id=inbox_id,
                        correlation_key=correlation_key,
                        executor_id=exec_id,
                        executor_session_id=bound_exec_sid,
                        status="suspended",
                        work_key=work_key,
                        checkpoint=checkpoint,
                        pending_request_id=request_id,
                        updated_at=utc_now_iso(),
                        parent_session_id=session_ref.parent_session_id,
                    )
                )
                if repeated:
                    return target_sid
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
                never_val: Never = unreachable
                assert_never(never_val)
