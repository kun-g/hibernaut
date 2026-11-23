from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncGenerator, Callable, Collection, Generator, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Never, assert_never, cast

from agent_runtime.core.models import (
    DeliveryOutcome,
    Emit,
    Event,
    FailureRoute,
    NewEventPolicy,
    OpenRun,
    RunDetached,
    RunEndKind,
    RunLedgerEntry,
    RunOutcome,
    RunTelemetry,
    TASK_CONTEXT_KEY,
    TASK_SYNC_KIND,
    SessionRef,
    Silent,
    Suspend,
    TaskLink,
    new_id,
    resolve_executor_id,
    utc_now_iso,
)
from agent_runtime.core.ports import (
    ExecutorPort,
    InboxPort,
    LockPort,
    MemoryPort,
    OpenRunPort,
    OutboxPort,
    PermissionPort,
    ResumableExecutorPort,
    RunCapabilities,
    RunLedgerPort,
    RunStartListener,
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
    # Kept on the report: a session closed after its Run no longer carries it.
    correlation_key: str | None = None


class ExecutorFailed(RuntimeError):
    def __init__(self, *args: object, route: FailureRoute | None = None) -> None:
        super().__init__(*args)
        # The run ledger's classification of the last failed attempt; None without a ledger.
        self.route = route


class _RunInterrupted(Exception):
    pass


class _RunDetached(Exception):
    pass


class _RunHandle:
    # RunContext of one resumable Run. A stop detaches the Run only where it is parked (waiting on work outside this
    # process) or where it polls should_detach(); anywhere else the stop waits for it as for any other Run.
    def __init__(self, run_dir: str, detaching: Callable[[], bool]) -> None:
        self._run_dir = run_dir
        self._detaching = detaching
        self._task: asyncio.Task[RunOutcome] | None = None
        self._parked = 0
        self._detached = False

    @property
    def run_dir(self) -> str:
        return self._run_dir

    def bind(self, task: asyncio.Task[RunOutcome]) -> None:
        self._task = task

    @contextlib.contextmanager
    def detachable(self) -> Generator[None]:
        self._parked += 1
        try:
            if self._detaching():
                self.detach()
            yield
        finally:
            self._parked -= 1

    def should_detach(self) -> bool:
        if self._detaching():
            self._detached = True
            return True
        return False

    def detached(self) -> bool:
        return self._detached

    def mark_detached(self) -> None:
        self._detached = True

    def detach(self) -> None:
        # Cancels the Run's task only while it is parked; the cancellation surfaces at its next await.
        if self._parked and self._task is not None and not self._task.done():
            self._detached = True
            self._task.cancel()


@dataclass(slots=True)
class _BatchEnd:
    ids: list[str]
    report: RunReport | None = None
    interrupted: bool = False
    detached: bool = False


# Payload keys that pick where/how an executor runs (e.g. LocalCLIExecutor's workdir and CLI
# template). Events that disagree on them must not share a Run.
_TARGET_PAYLOAD_KEYS = ("executor_id", "explicit_workdir", "executor")
_REPLY_PAYLOAD_KEYS = ("reply_channel", "reply_target")


def _task_link(events: list[Event], session_ref: SessionRef | None) -> TaskLink | None:
    # A work.ready in the batch names the task (a new dispatch replaces the old link); otherwise the session keeps
    # the task it was dispatched for, e.g. across an approval.decided resume, which carries no task_id.
    for evt in reversed(events):
        task_id = evt.payload.get("task_id")
        if isinstance(task_id, str) and task_id:
            trace_id = evt.payload.get("trace_id")
            hop_count = evt.payload.get("hop_count")
            return TaskLink(
                task_id=task_id,
                trace_id=trace_id if isinstance(trace_id, str) and trace_id else None,
                hop_count=hop_count if isinstance(hop_count, int) and not isinstance(hop_count, bool) else None,
            )
    return session_ref.task_link if session_ref is not None else None


def _task_fields(link: TaskLink, action: str, stage: str | None = None) -> dict[str, Any]:
    fields: dict[str, Any] = {"task_id": link.task_id, "task_action": action}
    if stage:
        fields["stage"] = stage
    if link.trace_id:
        fields["trace_id"] = link.trace_id
    if link.hop_count is not None:
        fields["hop_count"] = link.hop_count
    return fields


def _superseded(
    candidates: Sequence[TaskLink | None], kept: TaskLink | None, released: Collection[str]
) -> list[TaskLink]:
    # Linked tasks the session drops for `kept`, each once and not one already released.
    dropped: list[TaskLink] = []
    seen = set(released)
    for link in candidates:
        if link is None or (kept is not None and link.task_id == kept.task_id) or link.task_id in seen:
            continue
        seen.add(link.task_id)
        dropped.append(link)
    return dropped


def _emit_task_fields(link: TaskLink, emit: Emit) -> dict[str, Any]:
    if emit.status == "failed":
        # release returns the task to ready without a new dispatch, so a failing task does not loop.
        return _task_fields(link, "release", "failed")
    if emit.delivery is not None and emit.delivery.state == "pr_open":
        # Still in progress until the PR is merged; the issue or PR closing (work.closed) marks it done.
        return _task_fields(link, "update_stage", "waiting_pr_merge")
    return _task_fields(link, "complete")


def _end_kind(outcome: RunOutcome) -> RunEndKind:
    match outcome:
        case Silent():
            return "silent"
        case Emit():
            return "emit"
        case Suspend():
            return "suspend"
        case _ as unreachable:
            never_val: Never = unreachable
            assert_never(never_val)


def _replay_of_run_id(events: Sequence[Event]) -> str | None:
    for evt in events:
        raw = evt.payload.get("_replay_of_run_id")
        if isinstance(raw, str) and raw:
            return raw
    return None


def _task_title(events: list[Event]) -> str:
    # payload title first, then the Drive file name, then a GitHub issue or pull request title.
    candidates: list[list[object]] = [[], [], []]
    for evt in events:
        candidates[0].append(evt.payload.get("title"))
        candidates[1].append(evt.payload.get("drive_file_name"))
        for key in ("issue", "pull_request"):
            nested: object = evt.payload.get(key)
            if isinstance(nested, dict):
                candidates[2].append(cast(dict[str, object], nested).get("title"))
    for group in candidates:
        for value in group:
            if isinstance(value, str) and value.strip():
                return " ".join(value.split())
    return ""


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
        hold: Callable[[Event], str | None] | None = None,
        run_start_listener: RunStartListener | None = None,
        open_runs: OpenRunPort | None = None,
        run_ledger: RunLedgerPort | None = None,
        code_version: str = "",
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
        self._hold = hold
        self._run_start_listener = run_start_listener
        # Without a registry every executor runs as a plain ExecutorPort and a stop waits for its Runs.
        self._open_runs = open_runs
        # Without a run ledger nothing is recorded and a failing executor is retried max_retries times.
        self._run_ledger = run_ledger
        self._code_version = code_version
        self._lock = lock
        self._sessions = sessions
        self._lease_ttl_seconds = lease_ttl_seconds
        self._max_retries = max_retries
        self._new_event_policy: NewEventPolicy = new_event_policy
        self._interrupt_poll_interval_seconds = interrupt_poll_interval_seconds
        self._max_interrupt_restarts = max_interrupt_restarts
        self._wake_signals: dict[str, asyncio.Event] = {}
        self._retired = False
        self._interruptible = True
        self._detaching = False
        self._handles: set[_RunHandle] = set()

    @property
    def new_event_policy(self) -> NewEventPolicy:
        return self._new_event_policy

    def notify_new_event(self, inbox_id: str) -> None:
        signal = self._wake_signals.get(inbox_id)
        if signal is not None:
            signal.set()

    def retire(self, *, interruptible: bool = True) -> None:
        # A config reload retires this worker: the Run in progress finishes and writes its result, but nothing
        # more is claimed or folded in, so the next generation's workers take the inbox once the lease is released.
        # A stop also makes the Run uninterruptible: nothing in this process would restart an interrupted batch.
        self._retired = True
        if not interruptible:
            self._interruptible = False

    def detach(self) -> None:
        # A process stop: resumable Runs parked on work outside this process are cancelled without ending that work,
        # keep their registration and run directory, and leave their events 'processing' for the next process.
        # Runs that are not parked go on until they park or finish; the second stop signal still cancels them.
        self.retire(interruptible=False)
        self._detaching = True
        for handle in list(self._handles):
            handle.detach()

    def _is_detaching(self) -> bool:
        return self._detaching

    def _resumable(self, executor: ExecutorPort) -> ResumableExecutorPort | None:
        if self._open_runs is None or not isinstance(executor, ResumableExecutorPort):
            return None
        return executor

    async def process_inbox(self, inbox_id: str) -> list[RunReport]:
        if not self._lock.acquire(inbox_id, self._worker_id, self._lease_ttl_seconds):
            return []

        reports: list[RunReport] = []
        interrupt_restarts = 0
        wake_signal = asyncio.Event()
        self._wake_signals[inbox_id] = wake_signal

        try:
            # Runs a stopped process detached are resumed first; their events stay 'processing' meanwhile, so nothing
            # claims them as a new batch. Any other 'processing' event was orphaned by a crashed lease holder.
            open_runs = self._open_runs.list_open(inbox_id) if self._open_runs is not None else []
            held = {eid for run in open_runs for eid in run.event_ids}
            orphans = [evt.event_id for evt in self._inbox.fetch_processing(inbox_id) if evt.event_id not in held]
            if orphans:
                self._inbox.requeue_processing(orphans)
            for open_run in open_runs:
                if self._retired:
                    break
                end = await self._resume_open_run(inbox_id, open_run, wake_signal, interrupt_restarts)
                if end is None:
                    continue
                if end.report is not None:
                    reports.append(end.report)
                if end.interrupted:
                    interrupt_restarts += 1
                if end.detached:
                    break

            while not self._retired:
                claimed = self._claim(inbox_id)
                if not claimed:
                    break

                invalid = [e.event_id for e in claimed if not self._matches_pending_approval(e)]
                if invalid:
                    self._inbox.mark_failed(invalid, "Approval does not match a pending request in this session")
                    rejected = set(invalid)
                    claimed = [e for e in claimed if e.event_id not in rejected]
                    if not claimed:
                        continue

                restarted = False
                snapshot_ids = {evt.event_id for evt in claimed}
                batches = self._group_into_batches(claimed)
                scheduled_ids = {evt.event_id for _, b in batches for evt in b}
                unscheduled_ids = [evt.event_id for evt in claimed if evt.event_id not in scheduled_ids]
                completed_or_failed_ids: set[str] = set()

                try:
                    for batch_idx, (session_ref, batch) in enumerate(batches):
                        if self._retired:
                            break
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

                        folded_events: list[Event] = []
                        allow_interrupt = (
                            self._new_event_policy == "interrupt"
                            and interrupt_restarts < self._max_interrupt_restarts
                            and not any(self._is_approval_decision(evt) for evt in batch)
                        )
                        try:
                            end = await self._run_batch(
                                inbox_id=inbox_id,
                                batch=batch,
                                active_session=active_session,
                                executor=selected_executor,
                                known_ids=snapshot_ids,
                                wake_signal=wake_signal,
                                allow_interrupt=allow_interrupt,
                                folded_events=folded_events,
                                open_run=None,
                            )
                        except ExecutorFailed:
                            completed_or_failed_ids.update(batch_ids + [e.event_id for e in folded_events])
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
                        completed_or_failed_ids.update(end.ids)
                        if end.report is not None:
                            reports.append(end.report)
                        if end.interrupted:
                            # Never write Outbox on interrupt; requeue current & remaining batches.
                            requeue_ids = [
                                evt.event_id for _, rem in batches[batch_idx + 1 :] for evt in rem
                            ] + unscheduled_ids
                            self._inbox.requeue_processing(requeue_ids)
                            completed_or_failed_ids.update(requeue_ids)
                            interrupt_restarts += 1
                            restarted = True
                            break
                        if end.detached:
                            # Its events stay 'processing' under the registration; later batches return to pending below.
                            break
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

    @contextlib.asynccontextmanager
    async def _lease_heartbeat(self, inbox_id: str) -> AsyncGenerator[None]:
        # A Run can outlast the lease TTL (SDE runs take hours). Without renewal another worker pool, such as the
        # next config generation's, would take the expired lease and run the same inbox concurrently.
        async def beat() -> None:
            while True:
                await asyncio.sleep(self._lease_ttl_seconds / 3)
                if not self._lock.renew(inbox_id, self._worker_id, self._lease_ttl_seconds):
                    logging.getLogger(__name__).warning(
                        "Lease on inbox %s lost by %s during a run", inbox_id, self._worker_id
                    )

        task = asyncio.create_task(beat())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _notify_run_started(
        self, inbox_id: str, batch: list[Event], correlation_key: str | None
    ) -> list[Event]:
        # Returns the batch the executor runs: the listener's replacement payloads applied, ids and order unchanged.
        listener = self._run_start_listener
        if listener is None:
            return batch
        try:
            # In a thread: listeners such as the Drive queue make blocking network calls.
            replaced = await asyncio.to_thread(listener.on_run_started, inbox_id, tuple(batch), correlation_key)
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).warning(
                "Run-start listener failed for inbox %s; the Run continues", inbox_id, exc_info=True
            )
            return batch
        if not replaced:
            return batch
        return [
            replace(evt, payload=dict(replaced[evt.event_id])) if evt.event_id in replaced else evt
            for evt in batch
        ]

    def _claim(self, inbox_id: str) -> list[Event]:
        if self._hold is None:
            return self._inbox.claim_pending(inbox_id)
        # Held events are never claimed: they stay pending with their attempts, session and order untouched,
        # and the worker returns instead of waiting so the pool slot serves other inboxes.
        runnable = [evt.event_id for evt in self._runnable(self._inbox.fetch_pending(inbox_id))]
        return self._inbox.claim_pending(inbox_id, runnable) if runnable else []

    def _runnable(self, events: list[Event]) -> list[Event]:
        if self._hold is None:
            return events
        hold = self._hold
        return [evt for evt in events if hold(evt) is None]

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
        self._close_and_reclaim(target)
        return target.session_id

    def _close_and_reclaim(self, target: SessionRef) -> None:
        self._sessions.close_session(target.session_id)
        if target.work_key and self._on_worktree_reclaim is not None:
            # Counted within the inbox: another inbox with the same work_key has its own worktree, and the
            # reclaim callback re-checks whatever its workspace is shared with.
            if self._sessions.count_active_by_work_key(target.work_key, target.inbox_id) == 0:
                self._on_worktree_reclaim(target.work_key)

    async def _close_finished(self, session_id: str) -> None:
        # The Run already wrote its result; a failing close or reclaim is logged and never fails or repeats it.
        target = self._sessions.get(session_id)
        if target is None:
            return
        try:
            await asyncio.to_thread(self._close_and_reclaim, target)
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).exception("Closing finished session %s failed", session_id)

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

    async def _resume_open_run(
        self, inbox_id: str, open_run: OpenRun, wake_signal: asyncio.Event, interrupt_restarts: int
    ) -> _BatchEnd | None:
        registry = self._open_runs
        assert registry is not None
        wanted = set(open_run.event_ids)
        events = [evt for evt in self._inbox.fetch_processing(inbox_id) if evt.event_id in wanted]
        if not events:
            registry.discard(open_run.session_id)
            return None
        ids = [evt.event_id for evt in events]
        executor = self._executors.get(open_run.executor_id)
        session = self._sessions.get(open_run.session_id)
        if executor is None or self._resumable(executor) is None or session is None:
            # Nothing here can resume it: its events run again as a new batch under the existing routing rules,
            # which hold them for a person when the executor is gone.
            self._inbox.requeue_processing(ids)
            registry.discard(open_run.session_id)
            if executor is None:
                self._outbox.enqueue(
                    inbox_id=inbox_id,
                    content=(
                        f"[hibernaut] A stopped process left a Run of session {open_run.session_id} in progress, but "
                        f"executor '{open_run.executor_id}' is no longer registered, so it cannot be resumed. Its "
                        f"{len(ids)} event(s) are pending again; register the executor or recover the session."
                    ),
                    metadata={"kind": "resume_failed", "session_id": open_run.session_id},
                )
            return None
        self._lock.renew(inbox_id, self._worker_id, self._lease_ttl_seconds)
        wake_signal.clear()
        folded_events: list[Event] = []
        allow_interrupt = (
            self._new_event_policy == "interrupt"
            and interrupt_restarts < self._max_interrupt_restarts
            and not any(self._is_approval_decision(evt) for evt in events)
        )
        try:
            return await self._run_batch(
                inbox_id=inbox_id,
                batch=events,
                active_session=session,
                executor=executor,
                known_ids=wanted,
                wake_signal=wake_signal,
                allow_interrupt=allow_interrupt,
                folded_events=folded_events,
                open_run=open_run,
            )
        except asyncio.CancelledError:
            # A forced stop while resuming: the events return to pending like any cancelled Run's.
            self._inbox.requeue_processing(ids + [e.event_id for e in folded_events])
            raise

    async def _run_batch(
        self,
        *,
        inbox_id: str,
        batch: list[Event],
        active_session: SessionRef,
        executor: ExecutorPort,
        known_ids: set[str],
        wake_signal: asyncio.Event,
        allow_interrupt: bool,
        folded_events: list[Event],
        open_run: OpenRun | None,
    ) -> _BatchEnd:
        # Runs one batch to its result. open_run set means resuming a Run a stopped process detached.
        batch_ids = [evt.event_id for evt in batch]
        consumed_ids: set[str] = set(batch_ids)
        target_batch_key = self._batch_key(batch[0], active_session)
        has_decision = any(self._is_approval_decision(evt) for evt in batch)
        batch_target = self._batch_target(batch)
        interrupted_flag = asyncio.Event()
        resuming = open_run is not None
        registry = self._open_runs
        if registry is not None and open_run is None and self._resumable(executor) is not None:
            open_run = registry.open(
                session_id=active_session.session_id,
                inbox_id=inbox_id,
                executor_id=executor.executor_id,
                event_ids=batch_ids,
                code_version=self._code_version,
            )
        registered = open_run

        def _poll_folded() -> list[Event]:
            if (
                self._new_event_policy != "fold"
                or target_batch_key is None
                or has_decision
                or self._retired
            ):
                return []
            newly_folded: list[Event] = []
            for candidate in self._runnable(self._inbox.fetch_pending(inbox_id)):
                if candidate.event_id in consumed_ids:
                    continue
                cand_session = self._resolve_session(candidate)
                if self._batch_key(candidate, cand_session) != target_batch_key:
                    continue
                if self._is_approval_decision(candidate) or (
                    batch_target is not None and self._execution_target(candidate) != batch_target
                ):
                    break
                consumed_ids.add(candidate.event_id)
                folded_events.append(candidate)
                newly_folded.append(candidate)
            if newly_folded:
                new_ids = [e.event_id for e in newly_folded]
                self._inbox.bind_session(new_ids, active_session.session_id)
                if registered is not None and registry is not None:
                    registry.add_events(registered.session_id, new_ids)
            return newly_folded

        caps: RunCapabilities = _ScopedCapabilities(
            memory=self._memory,
            timers=self._timers,
            permissions=self._permissions,
            _poll_folded=_poll_folded,
            _is_interrupted=interrupted_flag.is_set,
        )
        handle = _RunHandle(registered.run_dir, self._is_detaching) if registered is not None else None
        batch_correlation_key = self._extract_correlation_key(batch, active_session)
        run_id = new_id("run")
        previous_link = active_session.task_link
        start_link = _task_link(batch, active_session)
        # An interruptible Run may be thrown away, and an interrupted Run writes nothing to the Outbox, so it releases
        # and claims together with its result. A resumed Run was claimed by the process that started it.
        claimed_link = start_link if not allow_interrupt else None
        released: set[str] = set()
        if claimed_link is not None:
            superseded = _superseded([previous_link], claimed_link, released)
            released.update(link.task_id for link in superseded)
            if not resuming:
                extra = self._report_metadata(batch, batch_correlation_key, executor.executor_id, run_id)
                self._sync_task_switch(inbox_id, extra, active_session.session_id, superseded, claimed_link)

        def _drop_registration() -> None:
            if registered is not None and registry is not None:
                registry.discard(registered.session_id)

        started_at = utc_now_iso()
        started = time.monotonic()
        # None: the Run did not end here (detached); the next process records it when it resumes the Run.
        end_kind: RunEndKind | None = "error"
        ledger_outcome: RunOutcome | None = None
        ledger_error: BaseException | None = None
        if handle is not None:
            self._handles.add(handle)
        try:
            async with self._lease_heartbeat(inbox_id):
                run_batch = await self._notify_run_started(inbox_id, batch, batch_correlation_key)
                outcome = await self._execute_batch(
                    executor=executor,
                    inbox_id=inbox_id,
                    batch=run_batch,
                    session_ref=active_session,
                    caps=caps,
                    known_ids=known_ids,
                    interrupted_flag=interrupted_flag,
                    wake_signal=wake_signal,
                    allow_interrupt=allow_interrupt,
                    handle=handle,
                    resume=resuming,
                )
            end_kind, ledger_outcome = _end_kind(outcome), outcome
        except _RunInterrupted:
            end_kind = "interrupted"
            requeue_ids = batch_ids + [e.event_id for e in folded_events]
            self._inbox.requeue_processing(requeue_ids)
            _drop_registration()
            return _BatchEnd(ids=requeue_ids, interrupted=True)
        except _RunDetached:
            end_kind = None
            return _BatchEnd(ids=batch_ids + [e.event_id for e in folded_events], detached=True)
        except ExecutorFailed as exc:
            ledger_error = exc
            all_failed_ids = batch_ids + [e.event_id for e in folded_events]
            self._inbox.mark_failed(all_failed_ids, str(exc.__cause__ or exc))
            _drop_registration()
            if exc.route is not None:
                self._alert_failed_run(
                    inbox_id,
                    active_session.session_id,
                    executor.executor_id,
                    exc,
                    len(all_failed_ids),
                    self._report_metadata(batch + folded_events, batch_correlation_key, executor.executor_id, run_id),
                )
            if claimed_link is not None:
                # Otherwise the task would stay in_progress on Taskboard with nothing working on it.
                self._release_failed_task(
                    inbox_id,
                    active_session.session_id,
                    claimed_link,
                    self._report_metadata(batch + folded_events, batch_correlation_key, executor.executor_id, run_id),
                )
            raise
        except BaseException as exc:
            # A cancelled Run (forced stop) ends like an interrupted one: no result, events back to pending.
            if isinstance(exc, asyncio.CancelledError):
                end_kind = "interrupted"
            else:
                ledger_error = exc
            _drop_registration()
            raise
        finally:
            if handle is not None:
                self._handles.discard(handle)
            if end_kind is not None:
                self._record_run(
                    run_id=run_id,
                    inbox_id=inbox_id,
                    session=active_session,
                    executor_id=executor.executor_id,
                    events=batch + folded_events,
                    started_at=started_at,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    end_kind=end_kind,
                    outcome=ledger_outcome,
                    error=ledger_error,
                )

        # Resolved once from what the Run actually consumed (a folded work.ready can name a new task); this link is
        # both reported and saved on the session.
        final_link = _task_link(batch + folded_events, active_session)
        final_corr = self._extract_correlation_key(batch + folded_events, active_session)
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
            executor,
            report_metadata=self._report_metadata(batch + folded_events, final_corr, executor.executor_id, run_id),
            task_link=final_link,
            superseded=_superseded([previous_link, claimed_link], final_link, released),
            claim_with_result=final_link is not None
            and (claimed_link is None or claimed_link.task_id != final_link.task_id),
        )
        all_done_ids = batch_ids + [evt.event_id for evt in folded_events]
        if final_session_id is not None and final_session_id != active_session.session_id:
            self._inbox.bind_session(all_done_ids, final_session_id)
        if registered is not None and registry is not None:
            # The events become 'done' in the same transaction that drops the registration.
            registry.finish(registered.session_id, all_done_ids)
        else:
            self._inbox.mark_done(all_done_ids)
        report = RunReport(
            inbox_id=inbox_id,
            session_id=final_session_id,
            event_ids=tuple(all_done_ids),
            outcome=outcome,
            correlation_key=final_corr,
        )
        if isinstance(outcome, Emit) and outcome.close_session and final_session_id is not None:
            # After the result and its events are written, so the reclaim gates no longer see them.
            async with self._lease_heartbeat(inbox_id):
                await self._close_finished(final_session_id)
        return _BatchEnd(ids=all_done_ids, report=report)

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
        handle: _RunHandle | None = None,
        resume: bool = False,
    ) -> RunOutcome:
        if not allow_interrupt and handle is None:
            return await self._execute_with_retry(executor, batch, session_ref, caps)

        # A resumable Run gets its own task, so a stop can cancel it without cancelling this worker.
        exec_task = asyncio.create_task(
            self._execute_with_retry(executor, batch, session_ref, caps, handle=handle, resume=resume)
        )
        if handle is not None:
            handle.bind(exec_task)
            if self._detaching:
                handle.detach()
        watcher_task = (
            asyncio.create_task(
                self._watch_for_interrupt(
                    inbox_id=inbox_id,
                    known_ids=known_ids,
                    interrupted_flag=interrupted_flag,
                    wake_signal=wake_signal,
                    exec_task=exec_task,
                )
            )
            if allow_interrupt
            else None
        )
        try:
            outcome = await exec_task
            if interrupted_flag.is_set():
                raise _RunInterrupted()
            return outcome
        except RunDetached:
            if handle is None:
                raise
            handle.mark_detached()
            raise _RunDetached() from None
        except asyncio.CancelledError:
            if interrupted_flag.is_set():
                raise _RunInterrupted() from None
            current = asyncio.current_task()
            if handle is not None and handle.detached() and not (current is not None and current.cancelling()):
                raise _RunDetached() from None
            raise
        finally:
            if watcher_task is not None:
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
            has_new = self._interruptible and any(
                evt.event_id not in known_ids
                for evt in self._runnable(self._inbox.fetch_pending(inbox_id))
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

    @classmethod
    def _report_metadata(
        cls, events: list[Event], correlation_key: str | None, executor_id: str, run_id: str
    ) -> dict[str, Any]:
        # Routing hints a channel needs to answer where the work came from; extra keys are ignored by
        # channels that do not read them.
        meta: dict[str, Any] = {"run_id": run_id}
        if correlation_key:
            meta["correlation_key"] = correlation_key
        for key in _REPLY_PAYLOAD_KEYS:
            for evt in reversed(events):
                value = evt.payload.get(key)
                if isinstance(value, str) and value:
                    meta[key] = value
                    break
        task_events = [e for e in events if not cls._is_approval_decision(e)] or events
        context: dict[str, Any] = {"source": task_events[0].source, "executor_id": executor_id}
        title = _task_title(events)
        if title:
            context["title"] = title
        meta[TASK_CONTEXT_KEY] = context
        return meta

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

    def _matches_pending_approval(self, event: Event) -> bool:
        if event.payload.get("event_type") != "approval.decided":
            return True
        request_id = event.payload.get("approval_request_id")
        if not isinstance(request_id, str) or not request_id:
            return False
        pending = self._sessions.find_by_request_id(request_id)
        return (
            pending is not None and pending.status == "suspended" and pending.inbox_id == event.inbox_id
            and (event.session_id is None or event.session_id == pending.session_id)
        )

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
        *,
        handle: _RunHandle | None = None,
        resume: bool = False,
    ) -> RunOutcome:
        resumable = self._resumable(executor) if handle is not None else None
        attempt = 0
        while True:
            try:
                if resumable is not None and handle is not None:
                    if resume:
                        return await resumable.resume(batch, session_ref, caps, handle)
                    return await resumable.execute_resumable(batch, session_ref, caps, handle)
                return await executor.execute(batch, session_ref, caps)
            except (asyncio.CancelledError, RunDetached):
                raise
            except Exception as exc:  # noqa: BLE001
                # Each failed attempt is routed on its own: a transient error followed by a deterministic one stops.
                route = self._route_failure(executor.executor_id, error=exc)
                delays = (
                    route.retry_delays
                    if route is not None
                    else tuple(0.01 * (n + 1) for n in range(self._max_retries))
                )
                if attempt < len(delays):
                    await asyncio.sleep(delays[attempt])
                    attempt += 1
                    continue
                raise ExecutorFailed(f"Executor failed after {attempt + 1} attempts", route=route) from exc

    def _route_failure(
        self,
        executor_id: str,
        *,
        error: BaseException | None = None,
        telemetry: RunTelemetry | None = None,
    ) -> FailureRoute | None:
        ledger = self._run_ledger
        if ledger is None:
            return None
        try:
            return ledger.route_failure(executor_id, error=error, telemetry=telemetry)
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).warning("Run ledger could not classify a failure", exc_info=True)
            return None

    def _record_run(
        self,
        *,
        run_id: str,
        inbox_id: str,
        session: SessionRef,
        executor_id: str,
        events: list[Event],
        started_at: str,
        duration_ms: int,
        end_kind: RunEndKind,
        outcome: RunOutcome | None,
        error: BaseException | None,
    ) -> None:
        ledger = self._run_ledger
        if ledger is None:
            return
        try:
            error_kind: str | None = None
            error_summary: str | None = None
            telemetry = outcome.telemetry if outcome is not None else None
            emit_status = outcome.status if isinstance(outcome, Emit) else None
            if error is not None:
                cause = error.__cause__ if isinstance(error, ExecutorFailed) and error.__cause__ else error
                error_summary = str(cause) or type(cause).__name__
                route = error.route if isinstance(error, ExecutorFailed) else None
                route = route or self._route_failure(executor_id, error=cause)
                error_kind = route.error_kind if route is not None else None
            elif isinstance(outcome, Emit) and outcome.status in ("failed", "refused"):
                route = self._route_failure(executor_id, telemetry=telemetry)
                error_kind = route.error_kind if route is not None else None
                error_summary = outcome.messages[0] if outcome.messages else None
            link = _task_link(events, session)
            ledger.record_run(
                RunLedgerEntry(
                    run_id=run_id,
                    inbox_id=inbox_id,
                    session_id=session.session_id,
                    event_ids=tuple(evt.event_id for evt in events),
                    executor_id=executor_id,
                    started_at=started_at,
                    ended_at=utc_now_iso(),
                    duration_ms=max(0, duration_ms),
                    outcome_kind=end_kind,
                    emit_status=emit_status,
                    correlation_key=self._extract_correlation_key(events, session),
                    trace_id=link.trace_id if link is not None else None,
                    work_key=session.work_key,
                    parent_session_id=session.parent_session_id,
                    replay_of_run_id=_replay_of_run_id(events),
                    executor_session_id=outcome.executor_session_id if outcome is not None else None,
                    error_kind=error_kind,
                    error_summary=error_summary,
                    telemetry=telemetry,
                )
            )
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).warning("Run ledger could not record run %s", run_id, exc_info=True)

    def _alert_failed_run(
        self,
        inbox_id: str,
        session_id: str,
        executor_id: str,
        failure: ExecutorFailed,
        event_count: int,
        report_metadata: Mapping[str, Any],
    ) -> None:
        route = failure.route
        assert route is not None
        cause = failure.__cause__ or failure
        summary = " ".join((str(cause) or type(cause).__name__).split())[:500]
        run_id = report_metadata.get("run_id")
        self._outbox.enqueue(
            inbox_id=inbox_id,
            content=(
                f"[hibernaut] Run {run_id} on executor '{executor_id}' failed: {route.error_kind} "
                f"({route.category}). {summary}\nIts {event_count} event(s) are marked failed; the worktree and run "
                "log are kept. Replay with hibernaut_replay_event once the cause is fixed."
            ),
            metadata={
                **report_metadata,
                "kind": "run_failed",
                "outcome_kind": "error",
                "session_id": session_id,
                "error_kind": route.error_kind,
                "error_category": route.category,
            },
        )

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

    @staticmethod
    def _with_status(extra: dict[str, Any], status: str, delivery: DeliveryOutcome | None = None) -> dict[str, Any]:
        context = extra.get(TASK_CONTEXT_KEY)
        if not isinstance(context, dict):
            return extra
        updated: dict[str, Any] = {**cast(dict[str, Any], context), "status": status}
        if delivery is not None:
            updated["delivery"] = {
                "state": delivery.state, "pr_url": delivery.pr_url, "auto_merge": delivery.auto_merge,
                "hold": delivery.hold, "hold_detail": delivery.hold_detail,
            }
        return {**extra, TASK_CONTEXT_KEY: updated}

    def _enqueue_task_sync(
        self,
        inbox_id: str,
        extra: Mapping[str, Any],
        session_id: str,
        fields: Mapping[str, Any],
        outcome_kind: str = "done",
    ) -> None:
        stage = f" ({fields['stage']})" if fields.get("stage") else ""
        self._outbox.enqueue(
            inbox_id=inbox_id,
            content=f"Taskboard task {fields['task_id']}: {fields['task_action']}{stage}",
            metadata={
                **extra,
                **fields,
                "kind": TASK_SYNC_KIND,
                "outcome_kind": outcome_kind,
                "session_id": session_id,
            },
        )

    def _sync_task_switch(
        self,
        inbox_id: str,
        extra: Mapping[str, Any],
        session_id: str,
        superseded: Sequence[TaskLink],
        claim: TaskLink | None,
    ) -> None:
        # A session tracks one task: the one it drops is released before the new one is claimed, so it does not
        # stay in_progress on Taskboard with nothing working on it.
        for link in superseded:
            self._enqueue_task_sync(inbox_id, extra, session_id, _task_fields(link, "release", "superseded"))
        if claim is not None:
            self._enqueue_task_sync(inbox_id, extra, session_id, _task_fields(claim, "claim", "running"))

    def _release_failed_task(
        self, inbox_id: str, session_id: str, link: TaskLink, extra: Mapping[str, Any]
    ) -> None:
        self._enqueue_task_sync(inbox_id, extra, session_id, _task_fields(link, "release", "failed"), "error")
        current = self._sessions.get(session_id)
        if current is not None and current.task_link is not None:
            self._sessions.save(replace(current, task_link=None, updated_at=utc_now_iso()))

    def _apply_outcome(
        self,
        inbox_id: str,
        session_ref: SessionRef,
        outcome: RunOutcome,
        correlation_key: str | None,
        work_key: str | None,
        selected_executor: ExecutorPort,
        *,
        report_metadata: Mapping[str, Any] | None = None,
        task_link: TaskLink | None = None,
        superseded: Sequence[TaskLink] = (),
        claim_with_result: bool = False,
    ) -> str | None:
        extra: dict[str, Any] = dict(report_metadata or {})
        self._sync_task_switch(
            inbox_id, extra, session_ref.session_id, superseded, task_link if claim_with_result else None
        )
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
                if task_link is not None:
                    self._enqueue_task_sync(
                        inbox_id, extra, session_ref.session_id, _task_fields(task_link, "complete")
                    )
                return session_ref.session_id
            case Emit(
                messages=messages,
                session_id=emit_session_id,
                executor_session_id=exec_sid,
                status=emit_status,
                delivery=delivery,
            ):
                exec_id, bound_exec_sid = self._resolve_executor_binding(
                    selected_executor,
                    session_ref,
                    exec_sid,
                )
                target_sid = emit_session_id or session_ref.session_id
                task_fields = _emit_task_fields(task_link, outcome) if task_link is not None else {}
                for index, text in enumerate(messages):
                    # The task action rides on the last message, so Taskboard sees it once.
                    last = index == len(messages) - 1
                    self._outbox.enqueue(
                        inbox_id=inbox_id,
                        content=text,
                        metadata={
                            **self._with_status(extra, emit_status, delivery),
                            **(task_fields if last else {}),
                            "kind": "emit",
                            "outcome_kind": "emit",
                            "session_id": target_sid,
                        },
                    )
                if task_fields and not messages:
                    failed = task_fields["task_action"] == "release"
                    self._enqueue_task_sync(inbox_id, extra, target_sid, task_fields, "error" if failed else "done")
                emit_link = task_link if task_fields.get("task_action") == "update_stage" else None
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
                        task_link=emit_link,
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
                        task_link=task_link,
                    )
                )
                if repeated:
                    return target_sid
                self._outbox.enqueue(
                    inbox_id=inbox_id,
                    content=prompt,
                    metadata={
                        **self._with_status(extra, "awaiting_approval"),
                        "kind": "approval_request",
                        "request_id": request_id,
                        "session_id": target_sid,
                        "outcome_kind": "suspend",
                        "approval_request_id": request_id,
                        # Waiting for a person keeps the task in_progress (no release).
                        **(_task_fields(task_link, "update_stage", "awaiting_approval") if task_link else {}),
                    },
                )
                return target_sid
            case _ as unreachable:
                never_val: Never = unreachable
                assert_never(never_val)
