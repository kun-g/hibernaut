from __future__ import annotations

from collections.abc import Callable, Iterable

from taskboard.core.errors import CycleDetectedError, UpstreamTaskNotFoundError
from taskboard.core.models import (
    FlowPlan,
    TaskDependency,
    TaskItem,
    new_task_id,
    utc_now_iso,
)


def detect_cycle(
    upstream_id: str,
    downstream_id: str,
    get_downstream: Callable[[str], Iterable[str]],
) -> tuple[str, ...] | None:
    if upstream_id == downstream_id:
        return (upstream_id, upstream_id)

    # DFS from downstream along existing outgoing 'blocks' edges to see if upstream is reachable.
    visited: set[str] = set()
    stack: list[tuple[str, tuple[str, ...]]] = [(downstream_id, (upstream_id, downstream_id))]

    while stack:
        curr, path = stack.pop()
        if curr == upstream_id:
            return path
        if curr in visited:
            continue
        visited.add(curr)
        for nxt in get_downstream(curr):
            if nxt == upstream_id:
                return (*path, upstream_id)
            if nxt not in visited:
                stack.append((nxt, (*path, nxt)))
    return None


def assert_no_cycle(
    upstream_id: str,
    downstream_id: str,
    get_downstream: Callable[[str], Iterable[str]],
) -> None:
    cycle_path = detect_cycle(upstream_id, downstream_id, get_downstream)
    if cycle_path is not None:
        raise CycleDetectedError(cycle_path)


def compile_flow_plan(
    plan: FlowPlan,
    *,
    now_iso: str | None = None,
) -> tuple[TaskItem, list[TaskItem], list[TaskDependency]]:
    if not plan.steps:
        raise ValueError("flow plan must contain at least one step")

    seen_ids: set[str] = set()
    for step in plan.steps:
        if not step.id:
            raise ValueError("flow step id must be non-empty")
        if step.id in seen_ids:
            raise ValueError(f"duplicate flow step id: {step.id}")
        seen_ids.add(step.id)

    ts = now_iso or utc_now_iso()
    parent_id = new_task_id()
    parent_task = TaskItem(
        task_id=parent_id,
        title=plan.title,
        scope=plan.scope,
        status="backlog",
        assignee="agent:default",
        task_type="flow_group",
        created_at=ts,
        updated_at=ts,
    )

    step_to_task_id: dict[str, str] = {step.id: new_task_id() for step in plan.steps}
    outgoing: dict[str, list[str]] = {tid: [] for tid in step_to_task_id.values()}
    incoming_count: dict[str, int] = {tid: 0 for tid in step_to_task_id.values()}
    edges: list[TaskDependency] = []

    for idx, step in enumerate(plan.steps):
        downstream_tid = step_to_task_id[step.id]
        # Default serial chain when 'after' is omitted; explicit list enables parallel/join.
        if step.after is None:
            deps: tuple[str, ...] = (plan.steps[idx - 1].id,) if idx > 0 else ()
        else:
            deps = step.after

        for dep_step_id in deps:
            if dep_step_id not in step_to_task_id:
                raise UpstreamTaskNotFoundError(dep_step_id)
            upstream_tid = step_to_task_id[dep_step_id]
            assert_no_cycle(upstream_tid, downstream_tid, lambda node: outgoing.get(node, ()))
            outgoing[upstream_tid].append(downstream_tid)
            incoming_count[downstream_tid] += 1
            edges.append(
                TaskDependency(
                    upstream_task_id=upstream_tid,
                    downstream_task_id=downstream_tid,
                    kind="blocks",
                    created_at=ts,
                )
            )

    tasks: list[TaskItem] = []
    for step in plan.steps:
        tid = step_to_task_id[step.id]
        is_root = incoming_count[tid] == 0
        status = plan.initial_status if is_root else "backlog"
        tasks.append(
            TaskItem(
                task_id=tid,
                title=step.title,
                scope=plan.scope,
                description=step.description,
                status=status,
                assignee=step.assignee,
                task_type=step.task_type,
                priority=step.priority,
                parent_task_id=parent_id,
                ready_at=ts if status == "ready" else None,
                created_at=ts,
                updated_at=ts,
            )
        )

    return parent_task, tasks, edges
