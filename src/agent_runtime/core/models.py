from __future__ import annotations

import asyncio
from collections.abc import Collection, Sequence
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
# Outbox metadata key the worker fills with {title, source, executor_id, trace_id, status, delivery}; status is an EmitStatus
# or "awaiting_approval", delivery the Emit's DeliveryOutcome as a dict when it has one. Nested so channels that pass
# top-level keys on are unaffected.
TASK_CONTEXT_KEY = "task_context"
# Outbox kind of a message that only syncs a Taskboard task's state (claim, complete, release); channels that show
# messages to a person skip it.
TASK_SYNC_KIND = "task_sync"
# Outbox metadata key of the Run's trace. Not the top-level "trace_id", which is the Taskboard task's trace that
# channels report back.
TRACE_METADATA_KEY = "_trace_id"
# forward_to_inbox hops one external event may take between agents before it must be answered. A return only answers a
# forward, so it uses none.
MAX_FORWARD_HOPS = 3
# The PermissionPort tool name every forward_to_inbox passes through.
FORWARD_TOOL = "inbox.forward"
FORWARD_SOURCE = "agent:forward"
RETURN_SOURCE = "agent:return"
# Ends one wait of the session it names. It carries no result, and the worker handles it without running an agent.
RETURN_CANCELLED_SOURCE = "agent:return_cancelled"
# Closes the session it names, a held result included, when the control plane could not take the inbox's lease.
CLOSE_SOURCE = "agent:close"
# Only Hibernaut queues these, and the worker trusts them for it; no public entrance accepts them.
INTERNAL_SOURCES = frozenset({FORWARD_SOURCE, RETURN_SOURCE, RETURN_CANCELLED_SOURCE, CLOSE_SOURCE})
MCP_APPROVAL_SOURCE = "mcp:approval"


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
    # Place in the Event Inbox, set by the store: which of two events came first, whatever their clocks said.
    seq: int | None = field(default=None, compare=False)


def explicit_trace_id(event: Event) -> str | None:
    for key in ("trace_id", "_trace_id"):
        value = event.payload.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def resolve_trace_id(event: Event) -> str:
    return explicit_trace_id(event) or event.event_id


def resolve_trigger_event(events: Sequence[Event]) -> Event:
    # The one event a batch descends from: the last that carries a trace, else the first (its event_id is the trace).
    if not events:
        raise ValueError("a trigger event needs at least one event")
    for event in reversed(events):
        if explicit_trace_id(event) is not None:
            return event
    return events[0]


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
    # How the work in this session reached it, kept so the hop count and the owed return survive Suspend/Resume.
    forward: ForwardSpec | None = None

    @property
    def agent_id(self) -> str | None:
        # An agent is registered under its own executor id, so the stored executor_id names the agent.
        return self.executor_id


@dataclass(frozen=True, slots=True)
class TaskLink:
    task_id: str
    trace_id: str | None = None
    hop_count: int | None = None


@dataclass(frozen=True, slots=True)
class HeldReturn:
    # An agent:return event that was refused, as it would be queued.
    agent_id: str
    external_id: str
    payload: dict[str, Any]
    reason: str
    # How the decision finishes the Taskboard task the result belongs to; empty when it has none.
    task_fields: dict[str, Any] = field(default_factory=dict[str, Any])


@dataclass(frozen=True, slots=True)
class ForwardSpec:
    target_inbox: str
    target_agent: str | None = None
    # Inbox that gets the result back as an agent:return event; None answers outside through the reply channel.
    return_to: str | None = None
    # Forwards made so far, this one included: 0 for work that came from outside.
    hop_count: int = 0
    forwarded_from: str | None = None
    parent_session_id: str | None = None
    # The trace of this work and the event its latest Run descends from: what the session sends on carries both, also
    # after an approval that names neither. Empty only in a record stored before they were kept.
    trace_id: str = ""
    parent_event_id: str = ""
    reply_channel: str | None = None
    reply_target: str | None = None
    # Correlation key of the external conversation this work belongs to.
    origin_key: str | None = None
    # The piece of work return_to waits for, named by the forward that first sent it out; None when nothing is owed.
    delegation_id: str | None = None
    # Work key of the agent in the same inbox that passed the work on: the session goes on in that worktree.
    work_key: str | None = None
    # Pieces this session sent out with a return to its own inbox and still waits for, each settled once.
    pending_returns: tuple[str, ...] = ()
    # Pieces this session no longer waits for because no result will come.
    cancelled_returns: tuple[str, ...] = ()
    # (piece, event) of every result this session has worked on: another event with the same piece comes too late.
    settled_returns: tuple[tuple[str, str], ...] = ()
    # What this session owed is owed by the agent it handed it on to: nothing it says about that work goes outside.
    handed_on: bool = False
    # Closed on request, not by its own Run: what it sent out ends with it, started or not.
    closed: bool = False
    # The session this one went on as when its Run gave it another id.
    continued_in: str | None = None
    # An operator replayed the event that brought this piece: its result counts although the piece is settled.
    replayed: bool = False
    # Failed events of this session that do not end the task outside: told to the agent it owed a result, or a
    # refused message while the task goes on.
    owed_failures: tuple[str, ...] = ()
    # Set only on the session that keeps a refused return; the result stays there after the decision.
    held: HeldReturn | None = None


@dataclass(frozen=True, slots=True)
class InboxMatchRule:
    agent: str
    source: str | None = None
    event_type: str | None = None
    tags: tuple[str, ...] = ()

    def matches(self, *, source: str | None, event_type: str | None, tags: Collection[str]) -> bool:
        # Every condition given must hold; a rule without conditions matches nothing (default_agent is for that).
        if self.source is None and self.event_type is None and not self.tags:
            return False
        if self.source is not None and self.source != source:
            return False
        if self.event_type is not None and self.event_type != event_type:
            return False
        return all(tag in tags for tag in self.tags)


@dataclass(frozen=True, slots=True)
class InboxBinding:
    # The inbox rule this binding covers, as configured (an id or a pattern).
    inbox: str
    default_agent: str
    # Agents the inbox may run; empty allows any declared agent.
    agents: tuple[str, ...] = ()
    rules: tuple[InboxMatchRule, ...] = ()

    def allows(self, agent_id: str) -> bool:
        return not self.agents or agent_id in self.agents


@dataclass(frozen=True, slots=True)
class InboxRoute:
    # What routing knows of one inbox: its project, and the binding covering it when the project declares bindings.
    project: str
    binding: InboxBinding | None = None
    has_bindings: bool = False


class AgentNotAllowed(ValueError):
    pass


class ForwardRefused(Exception):
    def __init__(self, reason: str, action: PermissionAction = "block") -> None:
        super().__init__(reason)
        # "defer" when a person may still approve the forward; "block" refuses it for good.
        self.action: PermissionAction = action


def resolve_agent_id(
    event_agent_id: str | None,
    session: SessionRef | None,
    binding: InboxBinding | None,
    project_default: str | None,
    global_default: str,
    *,
    source: str | None = None,
    event_type: str | None = None,
    tags: Collection[str] = (),
) -> str:
    # event explicit -> open session -> inbox rules -> inbox default -> project default, then global default.
    if event_agent_id:
        if binding is not None and not binding.allows(event_agent_id):
            raise AgentNotAllowed(
                f"agent '{event_agent_id}' is not allowed in inbox binding '{binding.inbox}' "
                f"(allowed: {', '.join(binding.agents)})"
            )
        return event_agent_id
    if session is not None and session.agent_id and session.status in ("active", "suspended"):
        return session.agent_id
    if binding is not None:
        for rule in binding.rules:
            if rule.matches(source=source, event_type=event_type, tags=tags):
                return rule.agent
        return binding.default_agent
    return project_default or global_default


def resolve_executor_id(
    event_executor_id: str | None,
    session: SessionRef | None,
    project_default: str | None,
    global_default: str,
) -> str:
    # Kept for callers without inbox bindings: event explicit -> existing session -> project -> global default.
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
class RunTelemetry:
    # What an executor saw of its process, kept only for the run ledger (log, usage, error class); never shown to
    # channels and not part of an outcome's equality.
    stdout: str = ""
    stderr: str = ""
    exit_code: int | None = None
    # Set by executors that already classify their failures (the runner's error_kind); the ledger takes it as given
    # instead of guessing from stderr and the exit code.
    error_kind: str | None = None
    # The execution node that ran the process; None means this process (the ledger records "local").
    runner_id: str | None = None
    # That node's own log of the run (location, digest), kept in the ledger's log; None when there is none.
    runner_log: str | None = None


@dataclass(frozen=True, slots=True)
class Silent:
    executor_session_id: str | None = None
    telemetry: RunTelemetry | None = field(default=None, compare=False)


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
    telemetry: RunTelemetry | None = field(default=None, compare=False)


@dataclass(frozen=True, slots=True)
class Suspend:
    request_id: str
    prompt: str
    session_checkpoint: str
    session_id: str | None = None
    executor_session_id: str | None = None
    telemetry: RunTelemetry | None = field(default=None, compare=False)


RunOutcome: TypeAlias = Silent | Emit | Suspend

# How a Run ended as the run ledger records it: a RunOutcome kind, or a Run that never produced one.
RunEndKind: TypeAlias = Literal["silent", "emit", "suspend", "interrupted", "error"]
# transient: retry after the route's delays; deterministic: stop at the first failure; execution: no retry, keep
# the worktree and log for a post-mortem.
FailureCategory: TypeAlias = Literal["transient", "deterministic", "execution"]


@dataclass(frozen=True, slots=True)
class FailureRoute:
    error_kind: str
    category: FailureCategory
    # Seconds to wait before each further attempt; empty means the first failure is final.
    retry_delays: tuple[float, ...] = ()


@dataclass(frozen=True, slots=True)
class RunLedgerEntry:
    run_id: str
    inbox_id: str
    session_id: str
    event_ids: tuple[str, ...]
    executor_id: str
    started_at: str
    ended_at: str
    duration_ms: int
    outcome_kind: RunEndKind
    emit_status: EmitStatus | None = None
    correlation_key: str | None = None
    trace_id: str | None = None
    work_key: str | None = None
    parent_session_id: str | None = None
    replay_of_run_id: str | None = None
    executor_session_id: str | None = None
    error_kind: str | None = None
    error_summary: str | None = None
    telemetry: RunTelemetry | None = None


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


class RunCancelled(asyncio.CancelledError):
    # A cancellation that still knows how the Run ended (a remote runner's interrupted result): it cancels like any
    # CancelledError, and the worker records its telemetry for the interrupted Run.
    def __init__(self, msg: str = "", *, telemetry: RunTelemetry | None = None) -> None:
        super().__init__(msg)
        self.telemetry = telemetry
