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
# How an Emit ended, as the executor states it; channels never infer it from the message text.
EmitStatus: TypeAlias = Literal["done", "failed", "stopped", "refused"]
EMIT_STATUSES: tuple[EmitStatus, ...] = ("done", "failed", "stopped", "refused")
DeliveryState: TypeAlias = Literal["local", "pr_open", "merged"]
MergeHold: TypeAlias = Literal["", "review", "ci", "other"]
# Outbox metadata key the worker fills with {title, source, executor_id, status, delivery}; status is an EmitStatus
# or "awaiting_approval", delivery the Emit's DeliveryOutcome as a dict when it has one. Nested so channels that pass
# top-level keys on are unaffected.
TASK_CONTEXT_KEY = "task_context"
# Outbox kind of a message that only syncs a Taskboard task's state (claim, complete, release); channels that show
# messages to a person skip it.
TASK_SYNC_KIND = "task_sync"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Fixed-width UTC form, so stored retry times order correctly as plain strings (SQLite compares them as TEXT).
def to_utc_iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="microseconds")


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
    # Set on a delegated child session; the child shares the parent's work_key so the worktree stays referenced.
    parent_session_id: str | None = None
    # The Taskboard task this session works on, kept across Suspend/Resume until a terminal task_action.
    task_link: TaskLink | None = None


@dataclass(frozen=True, slots=True)
class TaskLink:
    task_id: str
    trace_id: str | None = None
    hop_count: int | None = None


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
    # None means due now; otherwise a to_utc_iso() time before which the message is not fetched.
    next_retry_at: str | None = None


@dataclass(frozen=True, slots=True)
class PermissionDecision:
    action: PermissionAction
    reason: str = ""


@dataclass(frozen=True, slots=True)
class Silent:
    executor_session_id: str | None = None


@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    state: DeliveryState
    pr_url: str = ""
    # The project authorizes merging (delivery.auto_merge), so a PR left open needs a person.
    auto_merge: bool = False
    # Why an authorized merge did not happen; "" when it happened or was not authorized.
    hold: MergeHold = ""
    hold_detail: str = ""


@dataclass(frozen=True, slots=True)
class Emit:
    messages: tuple[str, ...]
    session_id: str | None = None
    executor_session_id: str | None = None
    status: EmitStatus = "done"
    # The task is finished: once the result is written, the session closes as a close-session event closes it.
    close_session: bool = False
    # Where the delivery step left the change; the worker passes it to channels as task_context["delivery"].
    delivery: DeliveryOutcome | None = None


@dataclass(frozen=True, slots=True)
class Suspend:
    request_id: str
    prompt: str
    session_checkpoint: str
    session_id: str | None = None
    executor_session_id: str | None = None


RunOutcome: TypeAlias = Silent | Emit | Suspend


@dataclass(frozen=True, slots=True)
class OpenRun:
    # A Run that has started and not yet written its result. One per session; its events stay 'processing' while
    # it is registered, so a new process resumes it instead of requeueing them.
    session_id: str
    inbox_id: str
    executor_id: str
    run_dir: str
    event_ids: tuple[str, ...]
    started_at: str
    code_version: str = ""


class RunDetached(Exception):
    # Raised by a resumable executor that stopped at a resumable point because the process is stopping; the work
    # outside this process goes on and the next process resumes the Run from its run directory.
    pass
