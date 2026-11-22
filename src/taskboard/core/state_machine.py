from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Final

from taskboard.core.errors import BlockedByDependencyError, IllegalTransitionError
from taskboard.core.models import TaskStatus

TERMINAL_STATUSES: Final[frozenset[TaskStatus]] = frozenset({"done", "cancelled"})

ALLOWED_TRANSITIONS: Final[Mapping[TaskStatus, frozenset[TaskStatus]]] = {
    "backlog": frozenset({"ready", "cancelled"}),
    "ready": frozenset({"in_progress", "backlog", "cancelled"}),
    "in_progress": frozenset({"done", "ready", "cancelled"}),
    "done": frozenset(),
    "cancelled": frozenset(),
}

REOPEN_TRANSITIONS: Final[Mapping[TaskStatus, frozenset[TaskStatus]]] = {
    "done": frozenset({"in_progress", "ready"}),
    "cancelled": frozenset({"backlog"}),
}


def validate_transition(
    current: TaskStatus,
    target: TaskStatus,
    *,
    task_id: str = "",
    incomplete_blockers: int = 0,
    blocking_task_ids: Sequence[str] = (),
    reopen: bool = False,
    is_late_running: bool = False,
) -> None:
    if current in TERMINAL_STATUSES:
        if is_late_running:
            raise IllegalTransitionError(
                current,
                target,
                "terminal status rejects late running update",
            )
        if not reopen:
            raise IllegalTransitionError(
                current,
                target,
                "terminal status requires explicit reopen=True",
            )
        if target not in REOPEN_TRANSITIONS.get(current, frozenset()):
            raise IllegalTransitionError(current, target, "disallowed reopen target")
        return

    # Idempotent in_progress heartbeat/retry is a no-op rather than an illegal jump.
    if current == "in_progress" and target == "in_progress":
        return

    if target not in ALLOWED_TRANSITIONS[current]:
        raise IllegalTransitionError(current, target)

    if current == "backlog" and target == "ready" and incomplete_blockers > 0:
        raise BlockedByDependencyError(task_id, blocking_task_ids)

    if current == "ready" and target == "in_progress" and incomplete_blockers > 0:
        raise BlockedByDependencyError(task_id, blocking_task_ids)
