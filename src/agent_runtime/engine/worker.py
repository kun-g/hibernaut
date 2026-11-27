from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncGenerator, Callable, Collection, Generator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Any, Never, assert_never, cast

from agent_runtime.core.models import (
    CLOSE_SOURCE,
    FORWARD_SOURCE,
    INTERNAL_SOURCES,
    MCP_APPROVAL_SOURCE,
    RETURN_CANCELLED_SOURCE,
    AgentNotAllowed,
    DeliveryOutcome,
    Emit,
    Event,
    FailureRoute,
    ForwardSpec,
    HeldReturn,
    InboxBinding,
    InboxRoute,
    NewEventPolicy,
    OpenRun,
    RunDetached,
    RunEndKind,
    RunLedgerEntry,
    RunCancelled,
    RunOutcome,
    RunTelemetry,
    TASK_CONTEXT_KEY,
    TASK_SYNC_KIND,
    TRACE_METADATA_KEY,
    SessionRef,
    Silent,
    Suspend,
    TaskLink,
    is_timed_resume,
    new_id,
    resolve_agent_id,
    to_utc_iso,
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
from agent_runtime.engine.closing import close_session
from agent_runtime.engine.forwarding import (
    EventOrigin,
    ForwardStage,
    cancel_event,
    carries_forwards,
    chain_external_id,
    event_agent_id,
    event_tags,
    event_type,
    forwarded_piece,
    is_open,
    is_replay,
    out_of_hops,
    own_task,
    owner_session_id,
    return_event,
    return_payload,
    return_refusal,
    returned_piece,
    returned_pieces,
    run_lineage,
    run_spec,
    same_piece,
    settled_return,
    task_hop_count,
    task_trace_id,
    withdrawn,
    withdrawn_forward,
    with_lineage,
)


@dataclass(frozen=True, slots=True)
class _ScopedCapabilities:
    memory: MemoryPort
    timers: TimerPort
    permissions: PermissionPort
    _poll_folded: Callable[[], list[Event]]
    _is_interrupted: Callable[[], bool]
    _forwards: ForwardStage

    def poll_folded_events(self) -> list[Event]:
        return self._poll_folded()

    def is_interrupted(self) -> bool:
        return self._is_interrupted()

    def forward_to_inbox(
        self,
        target_inbox: str,
        text: str,
        *,
        title: str = "",
        tags: Sequence[str] = (),
        target_agent: str | None = None,
        return_to: str | None = None,
        continues: str | None = None,
        approved_request_id: str | None = None,
    ) -> Event:
        return self._forwards.forward(
            target_inbox, text, title=title, tags=tags, target_agent=target_agent, return_to=return_to,
            continues=continues, approved_request_id=approved_request_id,
        )


@dataclass(frozen=True, slots=True)
class RunReport:
    inbox_id: str
    session_id: str | None
    event_ids: tuple[str, ...]
    outcome: RunOutcome
    # Kept on the report: a session closed after its Run no longer carries it.
    correlation_key: str | None = None
    trace_id: str | None = None
    # The batch never ran: routing refused the agent its events named.
    rejected: str | None = None
    # The Run handed the work to another agent or sent its result back to one: the external task is not finished.
    pending_elsewhere: bool = False


class ExecutorFailed(RuntimeError):
    def __init__(self, *args: object, route: FailureRoute | None = None) -> None:
        super().__init__(*args)
        # The run ledger's classification of the last failed attempt; None without a ledger.
        self.route = route
        # True once the failed batch is marked failed; otherwise it is still pending and would fail again at once.
        self.batch_settled = False


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


# Payload keys that, with the agent an event names, pick where/how an executor runs (e.g. LocalCLIExecutor's workdir
# and CLI template). Events that disagree on them must not share a Run.
_TARGET_PAYLOAD_KEYS = ("explicit_workdir", "executor")
_UNWELCOME = "A system inbox takes work only from a person, through an inbox that lists it or on this host"
_STALE_APPROVAL = "Approval does not match a pending request in this session"
_REPLY_PAYLOAD_KEYS = ("reply_channel", "reply_target")


def _task_link(events: list[Event], session_ref: SessionRef | None) -> TaskLink | None:
    # A work.ready in the batch names the task (a new dispatch replaces the old link); otherwise the session keeps
    # the task it was dispatched for, e.g. across an approval.decided resume, which carries no task_id.
    for evt in reversed(events):
        task_id = evt.payload.get("task_id")
        if isinstance(task_id, str) and task_id:
            trace_id = task_trace_id(evt)
            hop_count = task_hop_count(evt)
            return TaskLink(
                task_id=task_id,
                trace_id=trace_id if isinstance(trace_id, str) and trace_id else None,
                hop_count=hop_count if isinstance(hop_count, int) and not isinstance(hop_count, bool) else None,
            )
    return session_ref.task_link if session_ref is not None else None


def _run_trace_id(events: Sequence[Event], session: SessionRef | None) -> str:
    return run_lineage(events, session)[0]


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


def _routed_agent(
    event: Event, named: str | None, session: SessionRef | None, binding: InboxBinding | None,
    project_default: str | None, global_default: str,
) -> str:
    return resolve_agent_id(
        named, session, binding, project_default, global_default,
        source=event.source, event_type=event_type(event), tags=event_tags(event),
    )


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
        # inbox_id -> its project and binding; None runs without inbox bindings.
        routes: Callable[[str], InboxRoute | None] | None = None,
        # Who put an event here, when it was a person; a system inbox takes nothing else.
        origin: Callable[[Event], EventOrigin | None] | None = None,
        # Called with each inbox this worker queued an event into.
        on_forward: Callable[[str], object] | None = None,
        on_worktree_reclaim: Callable[[str], object] | None = None,
        hold: Callable[[Event], str | None] | None = None,
        # Called on the event loop with the inbox and the agent of every batch before it runs.
        on_batch_start: Callable[[str, str], object] | None = None,
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
        self._routes = routes
        self._origin: Callable[[Event], EventOrigin | None] = origin or (lambda _event: None)
        self._on_forward = on_forward
        self._on_worktree_reclaim = on_worktree_reclaim
        self._hold = hold
        self._on_batch_start = on_batch_start
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

                claimed = [e for e in claimed if not self._belongs_elsewhere(inbox_id, e)]
                unwelcome = [e.event_id for e in claimed if not self._welcome(inbox_id, e)]
                if unwelcome:
                    self._inbox.mark_failed(unwelcome, _UNWELCOME)
                    claimed = [e for e in claimed if e.event_id not in unwelcome]
                # Before any batch, so no message to the same session is settled with it and no decision on it counts.
                with_parent = [e for e in claimed if e.source == CLOSE_SOURCE and e.payload.get("closed_parent")]
                if with_parent:
                    reports += [r for event in with_parent if (r := self._close_with_parent(inbox_id, event)) is not None]
                    claimed = [e for e in claimed if e not in with_parent]
                invalid = [e.event_id for e in claimed if not self._matches_pending_approval(e)]
                if invalid:
                    self._inbox.mark_failed(invalid, _STALE_APPROVAL)
                    rejected = set(invalid)
                    claimed = [e for e in claimed if e.event_id not in rejected]
                # Before any batch: the last return may be in this claim too, and its Run then ends the task.
                cancelled = [e for e in claimed if e.source == RETURN_CANCELLED_SOURCE]
                if cancelled:
                    # One that is queued already, perhaps held back from this claim, came before its cancel.
                    results = [e for e in (*claimed, *self._inbox.fetch_pending(inbox_id)) if returned_piece(e)]
                    reports += [r for event in cancelled if (r := self._end_wait(inbox_id, event, results)) is not None]
                    claimed = [e for e in claimed if e.source != RETURN_CANCELLED_SOURCE]
                claimed = self._without_settled(claimed)
                if not claimed:
                    continue

                claimed = [own_task(evt, self._sessions) for evt in claimed]
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
                        # An earlier batch of this claim may have closed the session or decided its request.
                        session_ref, batch, stale = self._as_of_now(inbox_id, session_ref, batch)
                        if stale:
                            self._inbox.mark_failed(stale, _STALE_APPROVAL)
                            completed_or_failed_ids.update(stale)
                        if not batch:
                            continue
                        batch_ids = [evt.event_id for evt in batch]

                        holder = session_ref is not None and session_ref.forward is not None and session_ref.forward.held
                        if session_ref is not None and holder and not any(e.source == CLOSE_SOURCE for e in batch):
                            # No agent runs for it: a person decides, and the worker hands the result on.
                            completed_or_failed_ids.update(batch_ids)
                            decided = self._decide_return(inbox_id, batch, session_ref)
                            if decided is not None:
                                reports.append(decided)
                            continue

                        if self._is_close_session_batch(batch):
                            # Work that has not started in this session is not part of it; it gets another one.
                            batch = [e for e in batch if e.source != FORWARD_SOURCE or e.session_id is not None]
                            batch_ids = [evt.event_id for evt in batch]
                            closed_sid = self._handle_close_session(inbox_id, batch, session_ref)
                            if closed_sid is not None:
                                self._inbox.bind_session(batch_ids, closed_sid)
                            # The request a decision in it answers ended with the session.
                            void = [evt.event_id for evt in batch if self._is_approval_decision(evt)]
                            if void:
                                self._inbox.mark_failed(void, _STALE_APPROVAL)
                            self._inbox.mark_done([event_id for event_id in batch_ids if event_id not in void])
                            completed_or_failed_ids.update(batch_ids)
                            reports.append(
                                RunReport(
                                    inbox_id=inbox_id,
                                    session_id=closed_sid,
                                    event_ids=tuple(batch_ids),
                                    outcome=Silent(),
                                    trace_id=_run_trace_id(batch, session_ref),
                                )
                            )
                            continue

                        try:
                            selected_executor = self._select_executor(inbox_id, batch, session_ref)
                        except AgentNotAllowed as exc:
                            completed_or_failed_ids.update(batch_ids)
                            reports.append(self._refuse_batch(inbox_id, batch, session_ref, exc))
                            continue
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
                            forward=run_spec(batch, session_ref),
                        )
                        # The session says whom it works for before this looks whether that one was closed, and a
                        # close says so before it looks for sessions: one of them sees the other.
                        active_session = self._bound_to_work(active_session, batch)
                        ended = [e.event_id for e in batch if withdrawn_forward(e, self._sessions)]
                        if ended:
                            close_session(active_session, self._sessions, self._queue_event, cause=ended[0],
                                          closed_parent=owner_session_id(active_session, self._sessions))
                            self._reclaim(active_session)
                            self._inbox.mark_done(ended)
                            completed_or_failed_ids.update(ended)
                            continue
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

    def _welcome(self, inbox_id: str, event: Event) -> bool:
        route = self._route(inbox_id)
        if route is None or not route.system or event.source in INTERNAL_SOURCES:
            # A forward was judged where it was sent, and no public entrance takes an internal source.
            return True
        if self._is_approval_decision(event):
            pending = self._sessions.find_by_request_id(str(event.payload["approval_request_id"]))
            return pending is not None and pending.inbox_id == inbox_id
        # A one-to-one chat reaches a system inbox only through the general Steward.
        return self._origin(event) in ("operator", "group")

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
        forward: ForwardSpec | None,
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
            forward=forward,
        )
        self._sessions.save(new_session)
        return new_session

    def _bound_to_work(self, session: SessionRef, batch: list[Event]) -> SessionRef:
        # A session that takes over new work names its sender from the start, so a close finds it while it runs.
        if not any(e.source == FORWARD_SOURCE for e in batch):
            return session
        spec = run_spec(batch, session)
        if spec == session.forward:
            return session
        session = replace(session, forward=spec, updated_at=utc_now_iso())
        self._sessions.save(session)
        return session

    @staticmethod
    def _is_close_session_batch(batch: list[Event]) -> bool:
        return any(
            evt.payload.get("action") == "close_session"
            or evt.payload.get("close_session") is True
            or evt.source in ("session:close", CLOSE_SOURCE)
            for evt in batch
        )

    def _handle_close_session(
        self,
        inbox_id: str,
        batch: list[Event],
        session_ref: SessionRef | None,
    ) -> str | None:
        target = session_ref or next((found for evt in batch if (found := self._named_session(evt))), None)
        # Whoever holds the lease of one inbox closes no session of another.
        if target is None or target.inbox_id != inbox_id:
            return None
        close_session(target, self._sessions, self._queue_event, cause=batch[0].event_id)
        self._reclaim(target)
        return target.session_id

    def _queue_event(self, event: Event) -> None:
        _, duplicate = self._inbox.append(event)
        if not duplicate and self._on_forward is not None:
            self._on_forward(event.inbox_id)

    def _close_with_parent(self, inbox_id: str, event: Event) -> RunReport | None:
        session = self._sessions.get(event.session_id) if event.session_id else None
        parent = event.payload["closed_parent"]
        report: RunReport | None = None
        # It may be closed already, or work for another session by now.
        if session is not None and is_open(session) and owner_session_id(session, self._sessions) == parent:
            close_session(session, self._sessions, self._queue_event, cause=event.event_id, closed_parent=parent)
            self._reclaim(session)
            report = RunReport(inbox_id=inbox_id, session_id=session.session_id, event_ids=(event.event_id,),
                               outcome=Silent(), trace_id=_run_trace_id([event], session))
        self._inbox.mark_done([event.event_id])
        return report

    def _named_session(self, event: Event) -> SessionRef | None:
        sid = event.payload.get("session_id") or event.session_id
        return self._sessions.get(sid) if isinstance(sid, str) and sid else None

    def _belongs_elsewhere(self, inbox_id: str, event: Event) -> bool:
        # An event naming a session of another inbox, whose lease this worker does not hold. Settled here: only
        # Hibernaut's own close request goes on to that inbox.
        target = self._resolve_session(event)
        closing = self._is_close_session_batch([event])
        if target is None and closing:
            target = self._named_session(event)
        if target is None or target.inbox_id == inbox_id:
            return False
        if closing and event.source == CLOSE_SOURCE:
            self._queue_event(Event(
                inbox_id=target.inbox_id,
                source=CLOSE_SOURCE,
                external_id=f"close:{event.event_id}",
                payload={"action": "close_session"},
                session_id=target.session_id,
            ))
            self._inbox.mark_done([event.event_id])
        else:
            self._inbox.mark_failed([event.event_id], "It names a session of another inbox")
        return True

    def _without_settled(self, events: list[Event]) -> list[Event]:
        kept: list[Event] = []
        for event in events:
            piece = returned_piece(event)
            repeated = piece is not None and not is_replay(event) and piece in returned_pieces(kept)
            if repeated or settled_return(event, self._sessions) or withdrawn_forward(event, self._sessions):
                self._inbox.mark_done([event.event_id])
            else:
                kept.append(event)
        return kept

    def _as_of_now(
        self, inbox_id: str, session_ref: SessionRef | None, batch: list[Event]
    ) -> tuple[SessionRef | None, list[Event], list[str]]:
        # The batch's session as it is now, and the ids of decisions on a request it no longer has.
        if session_ref is None:
            return None, batch, []
        current = self._sessions.get(session_ref.session_id)
        request = session_ref.pending_request_id
        stale: list[str] = []
        if request and (
            current is None or current.status != "suspended" or current.inbox_id != inbox_id
            or current.pending_request_id != request
        ):
            stale = [evt.event_id for evt in batch if evt.payload.get("approval_request_id") == request]
        batch = [evt for evt in batch if evt.event_id not in stale]
        if batch and (current is None or not is_open(current)):
            current = self._resolve_session(batch[0])
        return current, batch, stale

    def _end_wait(self, inbox_id: str, event: Event, results: Sequence[Event]) -> RunReport | None:
        # Under the waiting inbox's own lease, so it never races the Runs that settle the same pieces.
        session = self._resolve_session(event)
        spec = session.forward if session is not None and session.inbox_id == inbox_id else None
        piece = event.payload.get("delegation_id")
        report: RunReport | None = None
        if any(returned_piece(e) == piece and (e.seq or 0) < (event.seq or 0) for e in results):
            # The result is here, so there is nothing to call off.
            self._inbox.mark_done([event.event_id])
            return None
        # Only a piece this session itself sent out and still waits for, or this very event a second time.
        if (
            session is not None and spec is not None and is_open(session) and isinstance(piece, str)
            and piece in (*spec.pending_returns, *spec.cancelled_returns)
        ):
            repeated = piece not in spec.pending_returns
            if not repeated:
                spec = replace(spec, pending_returns=tuple(p for p in spec.pending_returns if p != piece),
                               cancelled_returns=(*spec.cancelled_returns, piece))
            report = RunReport(inbox_id=inbox_id, session_id=session.session_id, event_ids=(event.event_id,),
                               outcome=Silent(), trace_id=_run_trace_id([event], session),
                               pending_elsewhere=bool(spec.pending_returns))
            # A suspended session goes on once its own request is decided.
            if not spec.pending_returns and session.status == "active" and not spec.handed_on:
                report = self._end_without_return(inbox_id, event, session, spec, repeated=repeated)
            elif not repeated:
                self._sessions.save(replace(session, forward=spec, updated_at=utc_now_iso()))
            self._inbox.bind_session([event.event_id], session.session_id)
        self._inbox.mark_done([event.event_id])
        return report

    def _end_without_return(
        self, inbox_id: str, event: Event, session: SessionRef, spec: ForwardSpec, *, repeated: bool
    ) -> RunReport:
        # Nothing else will wake this session for the task, so the worker ends it; no agent is told by a side door.
        payload = event.payload
        text = (
            f"The result of agent '{payload.get('forwarded_by')}' in inbox '{payload.get('forwarded_from')}' will not "
            f"come back: {payload.get('reason')}. This task ended without it."
        )
        key = self._reported_key(session.correlation_key, spec)
        trace_id = _run_trace_id([event], session)
        metadata = self._report_metadata([event], key, session.agent_id or "", new_id("run"), trace_id, spec)
        owed = spec.return_to is not None
        # Results first: a crash before the session is saved repeats them, one after it does not.
        if not repeated:
            if owed:
                self._give_back(inbox_id, session.agent_id or "", session, spec, [event], text=text, status="refused",
                                metadata=metadata)
            else:
                self._outbox.enqueue(
                    inbox_id=inbox_id,
                    content=f"[hibernaut] {text}",
                    metadata={**self._with_status(metadata, "refused"), "kind": "return_withheld",
                              "outcome_kind": "error", "session_id": session.session_id},
                )
            if session.task_link is not None:
                self._enqueue_task_sync(inbox_id, metadata, session.session_id,
                                        _task_fields(session.task_link, "release", "failed"), "error")
            self._sessions.save(replace(session, task_link=None, updated_at=utc_now_iso(),
                                        forward=replace(spec, return_to=None, delegation_id=None)))
        return RunReport(
            inbox_id=inbox_id,
            session_id=session.session_id,
            event_ids=(event.event_id,),
            outcome=Emit(messages=(text,), status="refused"),
            correlation_key=key,
            trace_id=trace_id,
            rejected=text,
            pending_elsewhere=owed,
        )

    def _close_and_reclaim(self, target: SessionRef) -> None:
        self._sessions.close_session(target.session_id)
        self._reclaim(target)

    def _reclaim(self, target: SessionRef) -> None:
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

    def _route(self, inbox_id: str) -> InboxRoute | None:
        return self._routes(inbox_id) if self._routes is not None else None

    @classmethod
    def expected_executor_id(
        cls,
        event: Event,
        sessions: SessionStorePort,
        *,
        binding: InboxBinding | None = None,
        project_default: str | None,
        global_default: str | None,
    ) -> str | None:
        # The agent _select_executor picks for this event's batch, known before it is claimed; None when the event
        # runs no agent (a close request, a cancelled or settled return, withdrawn work, a held result, a session of
        # another inbox, a refused agent) or none can be told.
        if event.source == RETURN_CANCELLED_SOURCE or cls._is_close_session_batch([event]):
            return None
        if settled_return(event, sessions) or withdrawn_forward(event, sessions):
            return None
        event = own_task(event, sessions)
        session = cls._session_for(event, sessions)
        if session is not None and session.inbox_id != event.inbox_id:
            return None
        if session is not None and session.forward is not None and session.forward.held is not None:
            return None
        if cls._resumes_suspended([event], session):
            return cls._suspended_holder([event], session)
        try:
            return _routed_agent(
                event, event_agent_id(event), session, binding, project_default, global_default or ""
            ) or None
        except AgentNotAllowed:
            return None

    def _select_executor(
        self,
        inbox_id: str,
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

        target_events = [e for e in batch if not self._is_approval_decision(e)] or batch
        named = next((agent for evt in reversed(target_events) if (agent := event_agent_id(evt))), None)
        target_id = self._agent_for(inbox_id, target_events[0], named, session_ref)
        selected = self._executors.get(target_id)
        if selected is None:
            raise ExecutorFailed(f"Executor '{target_id}' is not registered")
        return selected

    def _agent_for(
        self, inbox_id: str, event: Event, named: str | None, session_ref: SessionRef | None
    ) -> str:
        route = self._route(inbox_id)
        return _routed_agent(
            event, named, session_ref, route.binding if route is not None else None,
            self._project_default_executor, self._global_default_executor,
        )

    def _refuse_batch(
        self, inbox_id: str, batch: list[Event], session_ref: SessionRef | None, refusal: AgentNotAllowed
    ) -> RunReport:
        # Refused before any session, executor or workspace exists for it, and never rerouted to another agent.
        reason = str(refusal)
        ids = [evt.event_id for evt in batch]
        saved = session_ref.forward if session_ref is not None else None
        # A refused piece of work is settled with the agent that sent it. Any other refused message changes nothing
        # about what the session owes or waits for: it is answered where it came from.
        spec = run_spec(batch, session_ref)
        pieces = [piece for evt in batch if (piece := forwarded_piece(evt))]
        ongoing = saved is not None and (saved.return_to is not None or bool(saved.pending_returns))
        told = saved is not None and all(event_id in saved.owed_failures for event_id in ids)
        key = self._reported_key(self._extract_correlation_key(batch, session_ref), spec)
        trace_id = _run_trace_id(batch, session_ref)
        metadata = self._with_status(
            self._report_metadata(batch, key, "", new_id("run"), trace_id, spec), "refused"
        )
        if session_ref is not None:
            metadata["session_id"] = session_ref.session_id
        if told:
            # Refused before a crash, up to marking the events.
            pass
        elif spec is not None and pieces:
            self._give_back(inbox_id, "", session_ref, spec, batch, text=reason, status="refused", metadata=metadata)
        else:
            self._outbox.enqueue(
                inbox_id=inbox_id,
                content=f"[hibernaut] Refused {len(ids)} event(s) in inbox {inbox_id}: {reason}.",
                metadata={**metadata, "kind": "agent_refused", "outcome_kind": "error"},
            )
        if session_ref is not None and saved is not None and not told and (pieces or ongoing):
            settled = saved.delegation_id in pieces
            self._sessions.save(replace(session_ref, updated_at=utc_now_iso(), forward=replace(
                saved, return_to=None if settled else saved.return_to,
                delegation_id=None if settled else saved.delegation_id,
                owed_failures=(*saved.owed_failures, *ids),
            )))
        self._inbox.mark_failed(ids, reason)
        return RunReport(
            inbox_id=inbox_id,
            session_id=session_ref.session_id if session_ref is not None else None,
            event_ids=tuple(ids),
            outcome=Emit(messages=(reason,), status="refused"),
            correlation_key=key,
            trace_id=trace_id,
            rejected=reason,
            # The task goes on: with the agent that asked for it, or in this very session.
            pending_elsewhere=bool(pieces) or ongoing,
        )

    @staticmethod
    def _reported_key(correlation_key: str | None, spec: ForwardSpec | None) -> str | None:
        # Channels answer the conversation the work came from, not the session key a forward gave its child.
        return (spec.origin_key if spec is not None else None) or correlation_key

    def _give_back(
        self,
        inbox_id: str,
        agent_id: str,
        session_ref: SessionRef | None,
        spec: ForwardSpec,
        events: list[Event],
        *,
        text: str,
        status: str,
        metadata: dict[str, Any],
        delivery: DeliveryOutcome | None = None,
        task_fields: Mapping[str, Any] | None = None,
    ) -> tuple[str, Suspend] | None:
        # Queues the result for spec.return_to. A refused one is never answered outside instead: it waits for a person
        # in a suspended session of its own, so the agent's session goes on; returns that session and its request. One
        # nobody waits for any more goes nowhere.
        if withdrawn(spec, inbox_id, self._sessions):
            return None
        session_id = session_ref.session_id if session_ref is not None else None
        external_id = chain_external_id("return", events, 0)
        payload = return_payload(
            inbox_id=inbox_id, agent_id=agent_id, session_id=session_id, spec=spec, events=events, text=text,
            status=status, title=_task_title(events), delivery=delivery,
        )
        refusal = return_refusal(spec, inbox_id=inbox_id, agent_id=agent_id, payload=payload,
                                 permissions=self._permissions, routes=self._routes)
        if refusal is None:
            self._queue_event(return_event(spec, external_id=external_id, payload=payload, sessions=self._sessions))
            return None
        held = HeldReturn(agent_id=agent_id, external_id=external_id, payload=payload, reason=str(refusal),
                          task_fields=dict(task_fields or {}))
        # Named after the event, so a Run repeated after a crash finds the request it already made.
        holder = SessionRef(
            session_id=f"held:{external_id}", inbox_id=inbox_id, executor_id=agent_id or None, status="suspended",
            parent_session_id=session_id, forward=replace(spec, pending_returns=(), held=held),
        )
        known = self._sessions.get(holder.session_id)
        if known is not None and known.status != "suspended":
            return None
        if known is not None and known.pending_request_id:
            return holder.session_id, Suspend(
                known.pending_request_id, self._return_prompt(holder, known.pending_request_id), ""
            )
        return holder.session_id, self._ask_about_return(holder, metadata)

    @staticmethod
    def _return_prompt(holder: SessionRef, request_id: str) -> str:
        # It may be answered into the conversation the work came from, so it never carries the result.
        spec, held = holder.forward, holder.forward.held if holder.forward is not None else None
        assert spec is not None and held is not None
        choices = (
            f"It cannot go back any more. Approve to record it as settled here, reject to drop it; {spec.return_to} "
            "stops waiting for it either way."
            if out_of_hops(spec)
            else f"Approve to check again and hand it back; reject to withhold it ({spec.return_to} is then told only "
            "that it was withheld)."
        )
        return (
            f"[{held.agent_id or 'hibernaut'}] has a result ({held.payload['status']}) for {spec.return_to} that may "
            f"not go back by itself: {held.reason}.\n{choices}\nRequest {request_id}; the result is kept in session "
            f"{holder.session_id} (hibernaut_get_held_return shows it)."
        )

    def _ask_about_return(self, holder: SessionRef, metadata: dict[str, Any]) -> Suspend:
        held = holder.forward.held if holder.forward is not None else None
        assert held is not None
        request_id = new_id("req")
        asked = Suspend(request_id, self._return_prompt(holder, request_id), "")
        fields = held.task_fields
        # The card first: a crash in between asks again, instead of leaving a result nobody was asked about.
        self._outbox.enqueue(
            inbox_id=holder.inbox_id,
            content=asked.prompt,
            metadata={
                **self._with_status(metadata, "awaiting_approval"),
                "kind": "approval_request",
                "request_id": asked.request_id,
                "session_id": holder.session_id,
                "outcome_kind": "suspend",
                "approval_request_id": asked.request_id,
                **({**fields, "task_action": "update_stage", "stage": "awaiting_approval"} if fields else {}),
            },
        )
        self._sessions.save(replace(holder, pending_request_id=asked.request_id, updated_at=utc_now_iso()))
        return asked

    def _decide_return(self, inbox_id: str, batch: list[Event], holder: SessionRef) -> RunReport | None:
        spec, held = holder.forward, holder.forward.held if holder.forward is not None else None
        assert spec is not None and held is not None
        pending = holder.pending_request_id if holder.status == "suspended" else None
        answers = [e for e in batch if pending and e.payload.get("approval_request_id") == pending]
        others = [e.event_id for e in batch if e not in answers]
        if others:
            self._inbox.mark_failed(others, "This session only holds a result that waits for approval")
        if not answers:
            return None
        approved = answers[-1].payload.get("approved") is True
        key = self._reported_key(None, spec)
        trace_id = _run_trace_id(answers, holder)
        metadata = self._report_metadata(answers, key, held.agent_id, new_id("run"), trace_id, spec)
        payload = held.payload
        if not approved:
            # Only that it was withheld: the result itself stays here.
            payload = {k: v for k, v in payload.items() if k != "delivery"}
            payload |= {"status": "refused", "text": (
                f"The result of agent '{held.agent_id}' in inbox '{inbox_id}' was withheld: a person declined to hand "
                "it back."
            )}
        refusal = return_refusal(spec, inbox_id=inbox_id, agent_id=held.agent_id, payload=payload,
                                 permissions=self._permissions, routes=self._routes)
        blocked = refusal is not None and refusal.action == "block"
        ended = withdrawn(spec, inbox_id, self._sessions)
        outcome: RunOutcome = Silent()
        if blocked and approved and not ended and not out_of_hops(spec):
            retry = replace(holder, forward=replace(spec, held=replace(held, reason=str(refusal))))
            outcome = self._ask_about_return(retry, metadata)
        else:
            if ended:
                pass
            elif blocked:
                # Nothing may go back, so the waiting session only stops waiting for it.
                how = "it was settled there" if approved else "it was withheld"
                ending = cancel_event(spec, inbox_id=inbox_id, agent_id=held.agent_id, external_id=held.external_id,
                                      reason=f"{how} ({refusal})", reply=held.payload, sessions=self._sessions)
                if ending is not None:
                    self._queue_event(ending)
            else:
                self._queue_event(return_event(
                    spec, external_id=held.external_id, payload=payload, sessions=self._sessions
                ))
            if held.task_fields:
                fields = (
                    held.task_fields if approved
                    else {**held.task_fields, "task_action": "release", "stage": "failed"}
                )
                failed = fields["task_action"] == "release"
                self._enqueue_task_sync(inbox_id, metadata, holder.session_id, fields, "error" if failed else "done")
            # The result stays in the closed session.
            self._sessions.save(replace(holder, status="closed", pending_request_id=None, updated_at=utc_now_iso()))
        ids = [e.event_id for e in answers]
        self._inbox.bind_session(ids, holder.session_id)
        self._inbox.mark_done(ids)
        return RunReport(
            inbox_id=inbox_id,
            session_id=holder.session_id,
            event_ids=tuple(ids),
            outcome=outcome,
            correlation_key=key,
            trace_id=trace_id,
            pending_elsewhere=True,
        )

    async def _resume_open_run(
        self, inbox_id: str, open_run: OpenRun, wake_signal: asyncio.Event, interrupt_restarts: int
    ) -> _BatchEnd | None:
        registry = self._open_runs
        assert registry is not None
        wanted = set(open_run.event_ids)
        events = [own_task(evt, self._sessions) for evt in self._inbox.fetch_processing(inbox_id)
                  if evt.event_id in wanted]
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
        if self._on_batch_start is not None:
            self._on_batch_start(inbox_id, executor.executor_id)
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
            for found in self._runnable(self._inbox.fetch_pending(inbox_id)):
                if found.event_id in consumed_ids or found.source in (RETURN_CANCELLED_SOURCE, CLOSE_SOURCE):
                    continue
                if not self._welcome(inbox_id, found):
                    continue
                if (
                    settled_return(found, self._sessions) or withdrawn_forward(found, self._sessions)
                    or returned_piece(found) in returned_pieces(batch + folded_events)
                ):
                    continue
                candidate = own_task(found, self._sessions)
                cand_session = self._resolve_session(candidate)
                if self._batch_key(candidate, cand_session) != target_batch_key:
                    continue
                if self._is_approval_decision(candidate) or (
                    batch_target is not None and self._execution_target(candidate) != batch_target
                ) or not same_piece(candidate, batch + folded_events):
                    break
                consumed_ids.add(candidate.event_id)
                folded_events.append(candidate)
                newly_folded.append(candidate)
            if newly_folded:
                new_ids = [e.event_id for e in newly_folded]
                self._inbox.bind_session(new_ids, active_session.session_id)
                current = self._sessions.get(active_session.session_id)
                if current is not None and any(e.source == FORWARD_SOURCE for e in newly_folded):
                    self._sessions.save(replace(current, forward=run_spec(batch + folded_events, active_session)))
                if registered is not None and registry is not None:
                    registry.add_events(registered.session_id, new_ids)
            return newly_folded

        batch_correlation_key = self._extract_correlation_key(batch, active_session)
        forwards = ForwardStage(
            inbox_id=inbox_id,
            agent_id=executor.executor_id,
            session=active_session,
            correlation_key=batch_correlation_key,
            work_key=active_session.work_key,
            permissions=self._permissions,
            sessions=self._sessions,
            registered=self._executors.keys(),
            default_agent=self._default_agent,
            routes=self._routes,
            events=lambda: batch + folded_events,
            approval=lambda request_id: self._approved(request_id, batch, active_session),
            queued=lambda queued_in: [*self._inbox.fetch_pending(queued_in), *self._inbox.fetch_processing(queued_in)],
            origin=self._origin,
        )
        caps: RunCapabilities = _ScopedCapabilities(
            memory=self._memory,
            timers=self._timers,
            permissions=self._permissions,
            _poll_folded=_poll_folded,
            _is_interrupted=interrupted_flag.is_set,
            _forwards=forwards,
        )
        handle = _RunHandle(registered.run_dir, self._is_detaching) if registered is not None else None
        run_id = new_id("run")
        start_spec = run_spec(batch, active_session)
        start_key = self._reported_key(batch_correlation_key, start_spec)
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
                extra = self._report_metadata(
                    batch, start_key, executor.executor_id, run_id, _run_trace_id(batch, active_session), start_spec
                )
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
        cancel_telemetry: list[RunTelemetry] = []
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
                    on_attempt=forwards.reset,
                    cancel_telemetry=cancel_telemetry,
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
            exc.batch_settled = True
            _drop_registration()
            failed_spec = run_spec(batch + folded_events, active_session)
            failed_metadata = self._report_metadata(
                batch + folded_events, self._reported_key(batch_correlation_key, failed_spec), executor.executor_id,
                run_id, _run_trace_id(batch + folded_events, active_session), failed_spec,
            )
            inside = failed_spec is not None and (failed_spec.return_to is not None or failed_spec.handed_on)
            # Owed to the agent that asked for it, by this session or by the one it handed the work on to: the details
            # go there at most, and stay in the Run's own records.
            if exc.route is not None and not inside:
                self._alert_failed_run(
                    inbox_id, active_session.session_id, executor.executor_id, exc, len(all_failed_ids),
                    failed_metadata,
                )
            if failed_spec is not None:
                cause = exc.__cause__ or exc
                if failed_spec.return_to is not None:
                    self._give_back(
                        inbox_id, executor.executor_id, active_session, failed_spec, batch + folded_events,
                        text=f"{executor.executor_id} failed: {str(cause) or type(cause).__name__}", status="failed",
                        metadata=failed_metadata,
                    )
                if inside:
                    failed_spec = replace(failed_spec, return_to=None, delegation_id=None, owed_failures=tuple(
                        dict.fromkeys((*failed_spec.owed_failures, *all_failed_ids))
                    ))
                elif failed_spec.owed_failures:
                    # Replayed after it was given back; this failure is answered here.
                    failed_spec = replace(failed_spec, owed_failures=tuple(
                        event_id for event_id in failed_spec.owed_failures if event_id not in all_failed_ids
                    ))
                current = self._sessions.get(active_session.session_id)
                if current is not None and current.forward != failed_spec:
                    self._sessions.save(replace(current, forward=failed_spec, updated_at=utc_now_iso()))
            if claimed_link is not None:
                # Otherwise the task would stay in_progress on Taskboard with nothing working on it.
                self._release_failed_task(
                    inbox_id,
                    active_session.session_id,
                    claimed_link,
                    failed_metadata,
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
                    cancel_telemetry=cancel_telemetry[-1] if cancel_telemetry else None,
                )

        # Resolved once from what the Run actually consumed (a folded work.ready can name a new task); this link is
        # both reported and saved on the session.
        consumed = batch + folded_events
        final_link = _task_link(consumed, active_session)
        final_corr = self._extract_correlation_key(consumed, active_session)
        final_work_key = self._extract_work_key(
            consumed,
            active_session,
            final_corr,
            active_session.parent_session_id,
        )
        final_spec = run_spec(consumed, active_session)
        final_key = self._reported_key(final_corr, final_spec)
        handed_over = False
        if not carries_forwards(outcome):
            forwards.reset()
        dropped = forwards.drop_refused()
        if forwards.staged:
            if final_link is not None and not isinstance(outcome, Suspend):
                handed_over = forwards.hand_over(final_link)
            sent = forwards.own_returns()
            if sent:
                known = final_spec or with_lineage(
                    ForwardSpec(target_inbox=inbox_id, origin_key=final_corr), consumed, active_session
                )
                # A Run repeated after a crash sends the same pieces: one that is settled by now is not awaited again.
                settled = {*known.cancelled_returns, *returned_pieces(consumed)}
                final_spec = replace(known, pending_returns=tuple(dict.fromkeys(
                    (*known.pending_returns, *(piece for piece in sent if piece not in settled))
                )))
        if final_spec is not None and forwards.passes_on(final_spec):
            # From here on only the receiver owes it.
            final_spec = replace(final_spec, return_to=None, delegation_id=None, handed_on=True)
        if isinstance(outcome, Suspend) and final_spec is None:
            # The approval that resumes it carries no lineage of its own.
            final_spec = with_lineage(ForwardSpec(target_inbox=inbox_id), consumed, active_session)
        if isinstance(outcome, Suspend) and final_spec is not None:
            final_spec = replace(final_spec, from_person=forwards.from_person())
        awaiting = final_spec is not None and bool(final_spec.pending_returns)
        trace_id = _run_trace_id(consumed, active_session)
        report_metadata = self._report_metadata(
            consumed, final_key, executor.executor_id, run_id, trace_id, final_spec
        )
        # What is said about work handed on stays in the Run's own records.
        handed_on = final_spec is not None and final_spec.handed_on
        returned = handed_on and isinstance(outcome, Emit)
        held: tuple[str, Suspend] | None = None
        held_fields: dict[str, Any] = {}
        if isinstance(outcome, Emit) and final_spec is not None and final_spec.return_to is not None:
            if final_link is not None and not handed_over and not awaiting:
                fields = _emit_task_fields(final_link, outcome)
                # A finished task stays open until the decision; one that only changes stage needs none.
                held_fields = fields if fields["task_action"] != "update_stage" else {}
            held = self._give_back(
                inbox_id, executor.executor_id, active_session, final_spec, consumed,
                text="\n\n".join(outcome.messages), status=outcome.status, delivery=outcome.delivery,
                metadata=report_metadata, task_fields=held_fields,
            )
            returned = True
            # Queued or held, this session no longer owes it.
            final_spec = replace(final_spec, return_to=None, delegation_id=None)
        for event, reason in dropped:
            title = f" ({event.payload['title']})" if event.payload.get("title") else ""
            self._outbox.enqueue(
                inbox_id=inbox_id,
                content=f"[hibernaut] Did not forward work to {event.inbox_id}{title}: {reason}.",
                metadata={**self._with_status(report_metadata, "refused"), "session_id": active_session.session_id,
                          "kind": "forward_refused", "outcome_kind": "error"},
            )
        # Before the result is written, so a crash in between repeats the Run and its deduplicated forwards.
        for event in forwards.staged:
            self._queue_event(event)
        final_session_id = self._apply_outcome(
            inbox_id,
            active_session,
            outcome,
            final_corr,
            final_work_key,
            executor,
            report_metadata=report_metadata,
            task_link=final_link,
            superseded=_superseded([previous_link, claimed_link], final_link, released),
            claim_with_result=final_link is not None
            and (claimed_link is None or claimed_link.task_id != final_link.task_id),
            forward=final_spec,
            handed_over=handed_over,
            awaiting_returns=awaiting,
            returned=returned,
            task_held=held is not None and bool(held_fields),
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
            session_id=held[0] if held is not None else final_session_id,
            event_ids=tuple(all_done_ids),
            outcome=held[1] if held is not None else outcome,
            correlation_key=final_key,
            trace_id=trace_id,
            pending_elsewhere=bool(forwards.staged) or awaiting or returned or handed_on,
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
        on_attempt: Callable[[], None],
        cancel_telemetry: list[RunTelemetry] | None = None,
    ) -> RunOutcome:
        if not allow_interrupt and handle is None:
            return await self._execute_with_retry(
                executor, batch, session_ref, caps, on_attempt=on_attempt, cancel_telemetry=cancel_telemetry
            )

        # A resumable Run gets its own task, so a stop can cancel it without cancelling this worker.
        exec_task = asyncio.create_task(
            self._execute_with_retry(
                executor, batch, session_ref, caps, handle=handle, resume=resume, on_attempt=on_attempt,
                cancel_telemetry=cancel_telemetry,
            )
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
                # Finished in the same instant: the result is dropped, its Run is still recorded where it ran.
                if outcome.telemetry is not None and cancel_telemetry is not None:
                    cancel_telemetry.append(outcome.telemetry)
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
                evt.event_id not in known_ids and evt.source not in (RETURN_CANCELLED_SOURCE, CLOSE_SOURCE)
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
        cls,
        events: list[Event],
        correlation_key: str | None,
        executor_id: str,
        run_id: str,
        trace_id: str,
        spec: ForwardSpec | None = None,
    ) -> dict[str, Any]:
        # Routing hints a channel needs to answer where the work came from; extra keys are ignored by
        # channels that do not read them.
        meta: dict[str, Any] = {"run_id": run_id, TRACE_METADATA_KEY: trace_id}
        if correlation_key:
            meta["correlation_key"] = correlation_key
        if spec is not None:
            # A forwarded task answers where the chain started unless its own events say otherwise.
            for key, saved in zip(_REPLY_PAYLOAD_KEYS, (spec.reply_channel, spec.reply_target)):
                if saved:
                    meta[key] = saved
        for key in _REPLY_PAYLOAD_KEYS:
            for evt in reversed(events):
                value = evt.payload.get(key)
                if isinstance(value, str) and value:
                    meta[key] = value
                    break
        task_events = [e for e in events if not cls._is_approval_decision(e)] or events
        context: dict[str, Any] = {
            "source": task_events[0].source,
            "executor_id": executor_id,
            "trace_id": trace_id,
        }
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
            # A delegated child reuses the parent's worktree instead of opening its own; a child in another inbox
            # must not share it.
            parent = self._sessions.get(parent_session_id)
            inbox_id = events[0].inbox_id if events else None
            if parent is not None and parent.work_key and parent.inbox_id == inbox_id:
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
            routed_to = event_agent_id(event)
            if holder is not None and routed_to is not None and routed_to != holder:
                continue
            open_batch = open_batches.get(resolved_key)
            if open_batch is not None:
                if self._execution_target(event) != self._batch_target(open_batch) or not same_piece(event, open_batch):
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

    @classmethod
    def _suspended_holder(
        cls,
        batch: list[Event],
        session_ref: SessionRef | None,
    ) -> str | None:
        if not cls._resumes_suspended(batch, session_ref):
            return None
        assert session_ref is not None
        if session_ref.executor_id:
            return session_ref.executor_id
        for evt in batch:
            if evt.payload.get("approval_request_id") == session_ref.pending_request_id:
                named = event_agent_id(evt)
                if named is not None:
                    return named
        return None

    def _execution_target(self, event: Event) -> tuple[object, ...]:
        agent = event_agent_id(event)
        if agent is None and event.session_id is None and event.correlation_key is None:
            # Such events share a Run only under interrupt; the rules may send them to different agents.
            agent = self._agent_for(event.inbox_id, event, None, None)
        return (agent, *(event.payload.get(key) for key in _TARGET_PAYLOAD_KEYS))

    def _default_agent(self, inbox_id: str, tags: Sequence[str]) -> str:
        route = self._route(inbox_id)
        return resolve_agent_id(
            None, None, route.binding if route is not None else None, self._project_default_executor,
            self._global_default_executor, source=FORWARD_SOURCE, tags=tags,
        )

    def _approved(self, request_id: str, batch: list[Event], session_ref: SessionRef) -> bool:
        if session_ref.pending_request_id != request_id:
            return False
        answers = [evt for evt in batch if evt.payload.get("approval_request_id") == request_id]
        return bool(answers) and answers[-1].payload.get("approved") is True

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
        if (event.payload.get("event_type") != "approval.decided" and event.source != MCP_APPROVAL_SOURCE
                and not is_timed_resume(event)):
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
        return self._session_for(event, self._sessions)

    @staticmethod
    def _session_for(event: Event, sessions: SessionStorePort) -> SessionRef | None:
        # 4-level priority: approval_request_id -> explicit session_id -> (inbox_id, correlation_key) -> new.
        request_id = event.payload.get("approval_request_id")
        if isinstance(request_id, str) and request_id:
            suspended = sessions.find_by_request_id(request_id)
            if suspended is not None:
                return suspended
        if event.session_id:
            return sessions.get(event.session_id)
        if event.correlation_key:
            return sessions.find_by_correlation_key(event.inbox_id, event.correlation_key)
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
        on_attempt: Callable[[], None],
        cancel_telemetry: list[RunTelemetry] | None = None,
    ) -> RunOutcome:
        resumable = self._resumable(executor) if handle is not None else None
        attempt = 0
        while True:
            on_attempt()
            try:
                if resumable is not None and handle is not None:
                    if resume:
                        return await resumable.resume(batch, session_ref, caps, handle)
                    return await resumable.execute_resumable(batch, session_ref, caps, handle)
                return await executor.execute(batch, session_ref, caps)
            except asyncio.CancelledError as exc:
                # Caught here, before the exec_task boundary, so the interrupted Run is recorded on the node that ran it.
                if isinstance(exc, RunCancelled) and exc.telemetry is not None and cancel_telemetry is not None:
                    cancel_telemetry.append(exc.telemetry)
                raise
            except RunDetached:
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
        cancel_telemetry: RunTelemetry | None = None,
    ) -> None:
        ledger = self._run_ledger
        if ledger is None:
            return
        try:
            error_kind: str | None = None
            error_summary: str | None = None
            telemetry = outcome.telemetry if outcome is not None else cancel_telemetry
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
                    trace_id=_run_trace_id(events, session),
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

    def _archive_into(self, session: SessionRef, following: str) -> None:
        self._sessions.archive_session(session.session_id)
        archived = self._sessions.get(session.session_id)
        if archived is not None:
            # Whoever works for it finds the session it goes on as.
            spec = archived.forward or ForwardSpec(target_inbox=archived.inbox_id)
            self._sessions.save(replace(archived, forward=replace(spec, continued_in=following)))

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
        forward: ForwardSpec | None = None,
        handed_over: bool = False,
        awaiting_returns: bool = False,
        returned: bool = False,
        task_held: bool = False,
    ) -> str | None:
        extra: dict[str, Any] = dict(report_metadata or {})
        # Set while the task goes on with other agents: it moves to that stage instead of being completed.
        elsewhere = "forwarded" if handed_over else "awaiting_return" if awaiting_returns else None
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
                        task_link=task_link if elsewhere == "awaiting_return" else None,
                        forward=forward,
                    )
                )
                if task_link is not None:
                    fields = (
                        _task_fields(task_link, "update_stage", elsewhere) if elsewhere
                        else _task_fields(task_link, "complete")
                    )
                    self._enqueue_task_sync(inbox_id, extra, session_ref.session_id, fields)
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
                if task_link is not None and elsewhere and emit_status == "done":
                    task_fields = _task_fields(task_link, "update_stage", elsewhere)
                if task_held:
                    # The decision on the held result finishes the task.
                    task_fields = {}
                # A returned result went to the inbox that asked for it, not to the channel.
                shown = () if returned else messages
                for index, text in enumerate(shown):
                    # The task action rides on the last message, so Taskboard sees it once.
                    last = index == len(shown) - 1
                    self._outbox.enqueue(
                        inbox_id=inbox_id,
                        content=text,
                        metadata={
                            **self._with_status(extra, emit_status, delivery),
                            **({"chat_reply": outcome.chat_reply} if outcome.chat_reply else {}),
                            **(task_fields if last else {}),
                            "kind": "emit",
                            "outcome_kind": "emit",
                            "session_id": target_sid,
                        },
                    )
                if task_fields and not shown:
                    failed = task_fields["task_action"] == "release"
                    self._enqueue_task_sync(inbox_id, extra, target_sid, task_fields, "error" if failed else "done")
                keeps_link = task_fields.get("task_action") == "update_stage" and not handed_over
                emit_link = task_link if keeps_link else None
                if target_sid != session_ref.session_id:
                    # Session rotation: archive old session while new session inherits correlation_key & work_key.
                    self._archive_into(session_ref, target_sid)
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
                        forward=forward,
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
                    self._archive_into(session_ref, target_sid)
                if outcome.resume_after_seconds is not None and not repeated:
                    # Queue the wake before saving the checkpoint: a crash may leave a stale timer,
                    # which the request-id check discards, but never a checkpoint without its wake.
                    due = datetime.now(timezone.utc) + timedelta(seconds=outcome.resume_after_seconds)
                    self._timers.schedule("follow_up", to_utc_iso(due),
                                          {"event_type": "suspend.elapsed", "approval_request_id": request_id},
                                          inbox_id=inbox_id, session_id=target_sid)
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
                        forward=forward,
                    )
                )
                if repeated or outcome.resume_after_seconds is not None:
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
