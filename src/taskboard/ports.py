from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from taskboard.core.models import (
    DependencyKind,
    NotifyKind,
    TaskDependency,
    TaskItem,
    TaskSourceLink,
    TaskStatus,
    TaskView,
)


class TaskStorePort(Protocol):
    def get_task(self, task_id: str) -> TaskItem | None: ...

    def find_by_source_ref(self, source_ref: str) -> TaskItem | None: ...

    def link_source(
        self,
        task_id: str,
        source_ref: str,
        source: str,
        *,
        is_primary: bool = False,
        last_synced_at: int = 0,
    ) -> TaskSourceLink: ...

    def list_source_links(self, task_id: str) -> list[TaskSourceLink]: ...

    def create_task(self, task: TaskItem, *, source: str | None = None) -> TaskItem: ...

    def upsert_task(self, task: TaskItem, *, expected_version: int) -> TaskItem: ...

    def add_dependency(
        self,
        upstream_task_id: str,
        downstream_task_id: str,
        kind: DependencyKind = "blocks",
    ) -> TaskDependency: ...

    def remove_dependency(
        self,
        upstream_task_id: str,
        downstream_task_id: str,
        kind: DependencyKind = "blocks",
    ) -> bool: ...

    def count_incomplete_blockers(self, task_id: str) -> int: ...

    def list_incomplete_blocker_ids(self, task_id: str) -> list[str]: ...

    def list_unblocked_downstream(self, completed_task_id: str) -> list[TaskItem]: ...

    def list_downstream_tasks(
        self,
        upstream_task_id: str,
        *,
        kind: DependencyKind = "blocks",
    ) -> list[TaskItem]: ...

    def try_promote_to_ready(
        self,
        task_id: str,
        *,
        expected_version: int,
        ready_at: str,
    ) -> TaskItem | None: ...

    def try_claim_dispatch(
        self,
        task_id: str,
        *,
        expected_version: int,
    ) -> TaskItem | None: ...

    def list_tasks(
        self,
        *,
        scope: str | None = None,
        status: TaskStatus | None = None,
        assignee_prefix: str | None = None,
        dispatchable_only: bool = False,
        parent_task_id: str | None = None,
    ) -> list[TaskView]: ...

    def save_expanded_flow(
        self,
        parent_task: TaskItem,
        tasks: Sequence[TaskItem],
        edges: Sequence[TaskDependency],
    ) -> None: ...


class EventPublisherPort(Protocol):
    def publish_work_ready(
        self,
        task: TaskItem,
        *,
        trace_id: str,
        hop_count: int,
    ) -> None: ...

    def publish_notification(
        self,
        *,
        notify_kind: NotifyKind,
        task: TaskItem,
        reason: str,
        trace_id: str,
        hop_count: int,
        target_assignee: str | None = None,
        dedupe_key: str | None = None,
    ) -> None: ...


class AuditSinkPort(Protocol):
    def emit(
        self,
        event_name: str,
        *,
        task_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None: ...
