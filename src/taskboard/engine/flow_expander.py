from __future__ import annotations

from taskboard.core.dag import compile_flow_plan
from taskboard.core.models import ExpandedFlow, FlowPlan, TaskItem, new_trace_id
from taskboard.engine.dispatcher import UnlockDispatcher
from taskboard.ports import TaskStorePort


class FlowExpander:
    def __init__(self, store: TaskStorePort, dispatcher: UnlockDispatcher) -> None:
        self._store = store
        self._dispatcher = dispatcher

    def instantiate_flow(
        self,
        plan: FlowPlan,
        *,
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> ExpandedFlow:
        parent_task, tasks, edges = compile_flow_plan(plan)
        self._store.save_expanded_flow(parent_task, tasks, edges)

        effective_trace = trace_id or new_trace_id()
        ready_roots = sorted(
            (task for task in tasks if task.status == "ready"),
            key=lambda item: (item.priority, item.ready_at or ""),
        )

        updated_by_id: dict[str, TaskItem] = {}
        for root in ready_roots:
            dispatched = self._dispatcher.dispatch_ready_task(
                root,
                trace_id=effective_trace,
                hop_count=hop_count,
            )
            if dispatched is not None:
                updated_by_id[dispatched.task_id] = dispatched

        final_tasks = tuple(updated_by_id.get(t.task_id, t) for t in tasks)
        return ExpandedFlow(
            parent_task_id=parent_task.task_id,
            tasks=final_tasks,
            edges=tuple(edges),
        )
