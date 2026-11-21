"""L1 Core Ports: pure abstract Protocol interfaces for the five primitives and executor.

Depends ONLY on L0 (agent_runtime.core.models) and Python standard library.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from agent_runtime.core.models import (
    Event,
    OutboxMessage,
    PermissionDecision,
    RunOutcome,
    SessionRef,
    TimerKind,
    TimerSpec,
)


class InboxPort(Protocol):
    """Unified event buffer and source of truth."""

    def append(self, event: Event) -> tuple[Event, bool]:
        """Persist an event with idempotent deduplication on (inbox_id, source, external_id).

        Returns (stored_event, is_duplicate). Duplicate events are stored or returned
        with status='dropped' and must not be returned by fetch_pending().
        """
        ...

    def fetch_pending(self, inbox_id: str) -> list[Event]:
        """Fetch all pending events for an inbox in chronological order."""
        ...

    def mark_done(self, event_ids: Sequence[str]) -> None:
        """Mark processed events as done."""
        ...


class TimerPort(Protocol):
    """Persistent timer store for Follow-up and Schedule triggers."""

    def schedule(
        self,
        kind: TimerKind,
        due_at: str,
        payload: dict[str, Any],
        recurrence_seconds: int | None = None,
        *,
        inbox_id: str = "default",
        session_id: str | None = None,
    ) -> str:
        """Create and persist a timer, returning its timer_id."""
        ...

    def fetch_due(self, now_iso: str) -> list[TimerSpec]:
        """Return all pending timers whose due_at <= now_iso."""
        ...

    def mark_triggered(self, timer_id: str, next_due_at: str | None = None) -> None:
        """Mark a follow_up timer done, or advance a recurring schedule timer to next_due_at."""
        ...


class MemoryPort(Protocol):
    """Persistent directory-structured memory with baseline + on-demand loading."""

    def load_baseline(self) -> dict[str, str]:
        """Load always-loaded baseline files: instructions.md, index.md, reported.md."""
        ...

    def read_on_demand(self, path: str) -> str:
        """Read an on-demand memory file under notes/ or progress/."""
        ...

    def write(self, path: str, content: str) -> None:
        """Write or update a memory file persistently."""
        ...


class PermissionPort(Protocol):
    """Security boundary evaluated before every tool call."""

    def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
        """Evaluate whether a tool call should be allowed, blocked, or deferred."""
        ...


class OutboxPort(Protocol):
    """Persistent outbound message queue decoupled from delivery channels."""

    def enqueue(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> OutboxMessage:
        """Insert a pending message into the outbox."""
        ...

    def fetch_pending(self, limit: int = 20) -> list[OutboxMessage]:
        """Return pending messages ordered by creation time."""
        ...

    def mark_sent(self, message_id: str) -> None:
        """Mark an outbox message as successfully delivered."""
        ...

    def mark_failed(self, message_id: str, error: str) -> None:
        """Increment retry attempts and record delivery failure."""
        ...


class LockPort(Protocol):
    """Single-Active-Worker lease lock scoped per inbox_id."""

    def acquire(self, inbox_id: str, worker_id: str, ttl_seconds: float) -> bool:
        """Attempt to acquire or take over an expired lease for inbox_id."""
        ...

    def renew(self, inbox_id: str, worker_id: str, ttl_seconds: float) -> bool:
        """Extend an active lease owned by worker_id."""
        ...

    def release(self, inbox_id: str, worker_id: str) -> None:
        """Release the lease if currently owned by worker_id."""
        ...


class SessionStorePort(Protocol):
    """Cross-run session metadata and suspend-checkpoint storage."""

    def get(self, session_id: str) -> SessionRef | None:
        """Retrieve a session by session_id."""
        ...

    def find_by_request_id(self, request_id: str) -> SessionRef | None:
        """Find a suspended session awaiting a specific approval request_id."""
        ...

    def save(self, session: SessionRef) -> None:
        """Create or update a session record."""
        ...


class ChannelPort(Protocol):
    """External delivery channel (IM, Webhook callback, interactive approval UI)."""

    async def send(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None:
        """Deliver a single message to the external channel."""
        ...


class RunCapabilities(Protocol):
    """Scoped capabilities injected into an Executor for a single Run."""

    @property
    def memory(self) -> MemoryPort: ...

    @property
    def timers(self) -> TimerPort: ...

    @property
    def permissions(self) -> PermissionPort: ...


class ExecutorPort(Protocol):
    """Agent reasoning runtime boundary (whitebox or blackbox implementation)."""

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        """Execute one wake-up Run and return an explicit Silent | Emit | Suspend outcome."""
        ...
