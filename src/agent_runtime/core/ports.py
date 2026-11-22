from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Any, Protocol, runtime_checkable

from agent_runtime.core.models import (
    Event,
    OpenRun,
    OutboxMessage,
    PermissionDecision,
    RunOutcome,
    SessionRef,
    SessionStatus,
    TimerKind,
    TimerSpec,
)


class InboxPort(Protocol):
    # Dedup key: (inbox_id, source, external_id); duplicates return status='dropped'.
    def append(self, event: Event) -> tuple[Event, bool]: ...
    # True while the event with this dedup key is pending or processing; lets a poller skip it without a dropped row.
    def has_open_event(self, inbox_id: str, source: str, external_id: str) -> bool: ...
    def fetch_pending(self, inbox_id: str) -> list[Event]: ...
    def fetch_processing(self, inbox_id: str) -> list[Event]: ...
    # event_ids limits the claim to those still-pending events of the inbox; None claims every pending event.
    def claim_pending(self, inbox_id: str, event_ids: Collection[str] | None = None) -> list[Event]: ...
    def bind_session(self, event_ids: Sequence[str], session_id: str) -> None: ...
    def requeue_processing(self, event_ids: Sequence[str]) -> None: ...
    def mark_done(self, event_ids: Sequence[str]) -> None: ...
    def mark_failed(self, event_ids: Sequence[str], error: str) -> None: ...


class TimerPort(Protocol):
    def schedule(
        self,
        kind: TimerKind,
        due_at: str,
        payload: dict[str, Any],
        recurrence_seconds: int | None = None,
        *,
        inbox_id: str = "default",
        session_id: str | None = None,
    ) -> str: ...
    def fetch_due(self, now_iso: str) -> list[TimerSpec]: ...
    def mark_triggered(self, timer_id: str, next_due_at: str | None = None) -> None: ...


class MemoryPort(Protocol):
    def load_baseline(self) -> dict[str, str]: ...
    def read_on_demand(self, path: str) -> str: ...
    def write(self, path: str, content: str) -> None: ...


class PermissionPort(Protocol):
    def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision: ...


class OutboxPort(Protocol):
    def enqueue(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> OutboxMessage: ...
    # Only messages that are due: next_retry_at unset or <= now_iso (a to_utc_iso() string; None means now).
    def fetch_pending(self, limit: int = 20, *, now_iso: str | None = None) -> list[OutboxMessage]: ...
    def mark_sent(self, message_id: str) -> None: ...
    # Counts the failed attempt and keeps the message pending, hidden until next_retry_at.
    def schedule_retry(self, message_id: str, error: str, next_retry_at: str) -> None: ...
    # Terminal: counts the failed attempt and moves the message to 'failed'.
    def mark_failed(self, message_id: str, error: str) -> None: ...


class LockPort(Protocol):
    def acquire(self, inbox_id: str, worker_id: str, ttl_seconds: float) -> bool: ...
    def renew(self, inbox_id: str, worker_id: str, ttl_seconds: float) -> bool: ...
    def release(self, inbox_id: str, worker_id: str) -> None: ...


class SessionStorePort(Protocol):
    def get(self, session_id: str) -> SessionRef | None: ...
    def find_by_request_id(self, request_id: str) -> SessionRef | None: ...
    def find_by_correlation_key(self, inbox_id: str, correlation_key: str) -> SessionRef | None: ...
    # inbox_id narrows the count to one inbox's sessions; None counts every inbox.
    def count_active_by_work_key(self, work_key: str, inbox_id: str | None = None) -> int: ...
    def list_sessions(
        self,
        *,
        inbox_id: str | None = None,
        status: SessionStatus | None = None,
        parent_session_id: str | None = None,
    ) -> list[SessionRef]: ...
    def archive_session(self, session_id: str) -> None: ...
    def close_session(self, session_id: str) -> None: ...
    def save(self, session: SessionRef) -> None: ...


class ChannelPort(Protocol):
    async def send(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None: ...


class RunStartListener(Protocol):
    # Called once per batch after it is claimed and bound to a session, just before its executor starts.
    # A failure is logged by the worker and never fails or delays the Run. It may return replacement payloads by
    # event_id (such as a task body with its fetched attachments listed): the executor sees them in this Run only,
    # and the stored events keep their payloads.
    def on_run_started(
        self,
        inbox_id: str,
        events: Sequence[Event],
        correlation_key: str | None,
    ) -> Mapping[str, Mapping[str, Any]] | None: ...


class RunCapabilities(Protocol):
    @property
    def memory(self) -> MemoryPort: ...

    @property
    def timers(self) -> TimerPort: ...

    @property
    def permissions(self) -> PermissionPort: ...

    def poll_folded_events(self) -> list[Event]: ...

    def is_interrupted(self) -> bool: ...


class ExecutorPort(Protocol):
    @property
    def executor_id(self) -> str: ...

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome: ...


class OpenRunPort(Protocol):
    # Registers Runs of resumable executors while they are unfinished. The runtime knows only who runs them and where
    # their directory is; what is inside the directory belongs to the executor.
    def open(
        self, *, session_id: str, inbox_id: str, executor_id: str, event_ids: Sequence[str], code_version: str
    ) -> OpenRun: ...
    def list_open(self, inbox_id: str) -> list[OpenRun]: ...
    def add_events(self, session_id: str, event_ids: Sequence[str]) -> None: ...
    # The Run wrote its result: its events become 'done' and its registration goes in one transaction, then its
    # directory is deleted.
    def finish(self, session_id: str, done_event_ids: Sequence[str]) -> None: ...
    # The Run ended without a result to keep (failed, interrupted, cancelled, or not resumable here).
    def discard(self, session_id: str) -> None: ...


class RunContext(Protocol):
    @property
    def run_dir(self) -> str: ...

    # A stretch in which the Run only waits for work outside this process (an agent in an Orca terminal). A stop
    # cancels the Run inside it; cleanup then checks detached() and leaves that work and the run directory alone.
    def detachable(self) -> AbstractContextManager[None]: ...

    # Polled at resumable points of blocking work (such as delivery steps in a thread): True means a stop is
    # waiting, and the executor stops there by raising RunDetached.
    def should_detach(self) -> bool: ...

    def detached(self) -> bool: ...


@runtime_checkable
class ResumableExecutorPort(Protocol):
    # An executor whose Runs survive a restart: it keeps its own state in run.run_dir, and resume() continues a Run
    # that a stopped process detached, without sending the agent its task again.
    @property
    def executor_id(self) -> str: ...

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome: ...

    async def execute_resumable(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
        run: RunContext,
    ) -> RunOutcome: ...

    async def resume(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
        run: RunContext,
    ) -> RunOutcome: ...
