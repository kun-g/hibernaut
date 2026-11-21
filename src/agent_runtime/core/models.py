from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal, TypeAlias
from uuid import uuid4

EventStatus: TypeAlias = Literal["pending", "processing", "done", "failed", "dropped"]
SessionStatus: TypeAlias = Literal["active", "suspended", "archived", "closed"]
TimerKind: TypeAlias = Literal["follow_up", "schedule"]
TimerStatus: TypeAlias = Literal["pending", "done", "cancelled"]
OutboxStatus: TypeAlias = Literal["pending", "sent", "failed"]
PermissionAction: TypeAlias = Literal["allow", "block", "defer"]
NewEventPolicy: TypeAlias = Literal["defer", "fold", "interrupt"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


def _empty_dict() -> dict[str, Any]:
    return {}


@dataclass(frozen=True, slots=True)
class Event:
    inbox_id: str
    source: str
    external_id: str
    payload: dict[str, Any]
    event_id: str = field(default_factory=lambda: new_id("evt"))
    occurred_at: str = field(default_factory=utc_now_iso)
    session_id: str | None = None
    correlation_key: str | None = None
    status: EventStatus = "pending"
    attempts: int = 0
    last_error: str | None = None


@dataclass(frozen=True, slots=True)
class SessionRef:
    session_id: str
    inbox_id: str
    correlation_key: str | None = None
    executor_id: str | None = None
    executor_session_id: str | None = None
    status: SessionStatus = "active"
    work_key: str | None = None
    checkpoint: str | None = None
    pending_request_id: str | None = None
    updated_at: str = field(default_factory=utc_now_iso)


def resolve_executor_id(
    event_executor_id: str | None,
    session: SessionRef | None,
    project_default: str | None,
    global_default: str,
) -> str:
    # 4-level sticky routing: event explicit -> existing session -> project default -> global default.
    if event_executor_id:
        return event_executor_id
    if session is not None and session.executor_id:
        return session.executor_id
    if project_default:
        return project_default
    return global_default


@dataclass(frozen=True, slots=True)
class TimerSpec:
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
    inbox_id: str
    content: str
    message_id: str = field(default_factory=lambda: new_id("msg"))
    created_at: str = field(default_factory=utc_now_iso)
    status: OutboxStatus = "pending"
    attempts: int = 0
    metadata: dict[str, Any] = field(default_factory=_empty_dict)


@dataclass(frozen=True, slots=True)
class PermissionDecision:
    action: PermissionAction
    reason: str = ""


@dataclass(frozen=True, slots=True)
class Silent:
    executor_session_id: str | None = None


@dataclass(frozen=True, slots=True)
class Emit:
    messages: tuple[str, ...]
    session_id: str | None = None
    executor_session_id: str | None = None


@dataclass(frozen=True, slots=True)
class Suspend:
    request_id: str
    prompt: str
    session_checkpoint: str
    session_id: str | None = None
    executor_session_id: str | None = None


RunOutcome: TypeAlias = Silent | Emit | Suspend
