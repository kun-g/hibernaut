"""L0 Domain Contracts: immutable data structures and typed Run outcomes.

Strictly zero external dependencies (Python standard library only).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, TypeAlias
from uuid import uuid4

EventStatus: TypeAlias = Literal["pending", "done", "dropped"]
TimerKind: TypeAlias = Literal["follow_up", "schedule"]
TimerStatus: TypeAlias = Literal["pending", "done", "cancelled"]
OutboxStatus: TypeAlias = Literal["pending", "sent", "failed"]
PermissionAction: TypeAlias = Literal["allow", "block", "defer"]
NewEventPolicy: TypeAlias = Literal["defer", "fold", "interrupt"]


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    """Generate a prefixed unique identifier."""
    return f"{prefix}_{uuid4().hex[:12]}"


def _empty_dict() -> dict[str, Any]:
    return {}


@dataclass(frozen=True, slots=True)
class Event:
    """Canonical event persisted in the Event Inbox."""

    inbox_id: str
    source: str
    external_id: str
    payload: dict[str, Any]
    event_id: str = field(default_factory=lambda: new_id("evt"))
    occurred_at: str = field(default_factory=utc_now_iso)
    session_id: str | None = None
    status: EventStatus = "pending"


@dataclass(frozen=True, slots=True)
class SessionRef:
    """Cross-run persistent session reference and optional suspend checkpoint."""

    session_id: str
    inbox_id: str
    checkpoint: str | None = None
    pending_request_id: str | None = None
    updated_at: str = field(default_factory=utc_now_iso)


@dataclass(frozen=True, slots=True)
class TimerSpec:
    """Persistent timer record for proactive Follow-up or recurring Schedule runs."""

    inbox_id: str
    kind: TimerKind
    due_at: str
    payload: dict[str, Any]
    timer_id: str = field(default_factory=lambda: new_id("tmr"))
    recurrence_seconds: int | None = None
    session_id: str | None = None
    status: TimerStatus = "pending"


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    """Outbound user message queued in the Outbox for paced delivery."""

    inbox_id: str
    content: str
    message_id: str = field(default_factory=lambda: new_id("msg"))
    created_at: str = field(default_factory=utc_now_iso)
    status: OutboxStatus = "pending"
    attempts: int = 0
    metadata: dict[str, Any] = field(default_factory=_empty_dict)


@dataclass(frozen=True, slots=True)
class PermissionDecision:
    """Result of evaluating a tool call against the Permission boundary."""

    action: PermissionAction
    reason: str = ""


@dataclass(frozen=True, slots=True)
class Silent:
    """Run completed with nothing to report to the user."""


@dataclass(frozen=True, slots=True)
class Emit:
    """Run produced one or more outbound messages to queue in the Outbox."""

    messages: tuple[str, ...]
    session_id: str | None = None


@dataclass(frozen=True, slots=True)
class Suspend:
    """Run suspended awaiting human approval or clarification (non-blocking)."""

    request_id: str
    prompt: str
    session_checkpoint: str
    session_id: str | None = None


RunOutcome: TypeAlias = Silent | Emit | Suspend
