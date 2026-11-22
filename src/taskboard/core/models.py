from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final, Literal, get_args

TaskStatus = Literal["backlog", "ready", "in_progress", "done", "cancelled"]
DependencyKind = Literal["blocks", "related"]
NotifyKind = Literal["human_todo", "upstream_cancelled", "dependency_invalidated"]
TaskAction = Literal["claim", "update_stage", "complete", "release"]

TASK_STATUSES: Final[frozenset[str]] = frozenset(get_args(TaskStatus))
DEPENDENCY_KINDS: Final[frozenset[str]] = frozenset(get_args(DependencyKind))
NOTIFY_KINDS: Final[frozenset[str]] = frozenset(get_args(NotifyKind))
TASK_ACTIONS: Final[frozenset[str]] = frozenset(get_args(TaskAction))

MAX_HOPS: Final[int] = 8
MIN_PRIORITY: Final[int] = 0
MAX_PRIORITY: Final[int] = 3

_CROCKFORD: Final[str] = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_ISO_FORMAT: Final[str] = "%Y-%m-%dT%H:%M:%S.%fZ"
_ASSIGNEE_RE: Final[re.Pattern[str]] = re.compile(r"^(agent|human):[a-z0-9.:_-]+$")


def new_ulid() -> str:
    value = (int(time.time() * 1000) << 80) | int.from_bytes(os.urandom(10), "big")
    chars: list[str] = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


def new_task_id() -> str:
    return f"tb_tsk_{new_ulid()}"


def new_trace_id() -> str:
    return f"trc_{new_ulid()}"


def utc_now_iso() -> str:
    return datetime.now(UTC).strftime(_ISO_FORMAT)


def is_valid_assignee(assignee: str) -> bool:
    return assignee == "" or bool(_ASSIGNEE_RE.match(assignee))


def _empty_str_frozenset() -> frozenset[str]:
    return frozenset()


@dataclass(frozen=True, slots=True)
class TaskItem:
    task_id: str
    title: str
    scope: str = ""
    source_ref: str | None = None
    description: str = ""
    status: TaskStatus = "backlog"
    assignee: str = "agent:default"
    task_type: str = "general"
    stage: str = ""
    priority: int = 2
    preferred_executor: str = ""
    parent_task_id: str | None = None
    manual_override_fields: frozenset[str] = field(default_factory=_empty_str_frozenset)
    blocked_reason: str = ""
    dispatch_seq: int = 0
    version: int = 1
    last_source_event_at: int = 0
    ready_at: str | None = None
    artifact_ref: str | None = None
    reply_target: str | None = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)

    def __post_init__(self) -> None:
        if self.status not in TASK_STATUSES:
            raise ValueError(f"invalid status: {self.status!r}")
        if not is_valid_assignee(self.assignee):
            raise ValueError(f"invalid assignee prefix/format: {self.assignee!r}")
        if not (MIN_PRIORITY <= self.priority <= MAX_PRIORITY):
            raise ValueError(
                f"priority must be within [{MIN_PRIORITY}, {MAX_PRIORITY}], got {self.priority}"
            )

    def is_agent_assignee(self) -> bool:
        return self.assignee.startswith("agent:")

    def is_human_assignee(self) -> bool:
        return self.assignee.startswith("human:")


@dataclass(frozen=True, slots=True)
class TaskSourceLink:
    source_ref: str
    task_id: str
    source: str
    is_primary: bool = False
    last_synced_at: int = 0
    created_at: str = field(default_factory=utc_now_iso)


@dataclass(frozen=True, slots=True)
class TaskDependency:
    upstream_task_id: str
    downstream_task_id: str
    kind: DependencyKind = "blocks"
    created_at: str = field(default_factory=utc_now_iso)

    def __post_init__(self) -> None:
        if self.kind not in DEPENDENCY_KINDS:
            raise ValueError(f"invalid dependency kind: {self.kind!r}")


@dataclass(frozen=True, slots=True)
class TaskView:
    task: TaskItem
    incomplete_blocker_count: int


@dataclass(frozen=True, slots=True)
class FlowStepSpec:
    id: str
    title: str
    assignee: str = "agent:default"
    task_type: str = "general"
    priority: int = 2
    description: str = ""
    after: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class FlowPlan:
    title: str
    scope: str
    steps: tuple[FlowStepSpec, ...]
    initial_status: Literal["backlog", "ready"] = "ready"
    template_name: str | None = None


@dataclass(frozen=True, slots=True)
class ExpandedFlow:
    parent_task_id: str
    tasks: tuple[TaskItem, ...]
    edges: tuple[TaskDependency, ...]
