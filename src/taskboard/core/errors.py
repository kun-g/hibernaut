from __future__ import annotations

from collections.abc import Sequence


class TaskboardError(Exception):
    pass


class TaskNotFoundError(TaskboardError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"task not found: {task_id}")
        self.task_id = task_id


class UpstreamTaskNotFoundError(TaskboardError):
    def __init__(self, upstream_id: str) -> None:
        super().__init__(f"upstream task not found: {upstream_id}")
        self.upstream_id = upstream_id


class CycleDetectedError(TaskboardError):
    def __init__(self, cycle_path: Sequence[str]) -> None:
        path = tuple(cycle_path)
        rendered = " -> ".join(path)
        super().__init__(f"dependency cycle detected: {rendered}")
        self.cycle_path: tuple[str, ...] = path


class IllegalTransitionError(TaskboardError):
    def __init__(self, current_status: str, target_status: str, reason: str = "") -> None:
        suffix = f" ({reason})" if reason else ""
        super().__init__(f"illegal transition: {current_status} -> {target_status}{suffix}")
        self.current_status = current_status
        self.target_status = target_status
        self.reason = reason


class BlockedByDependencyError(TaskboardError):
    def __init__(self, task_id: str, blocking_task_ids: Sequence[str] = ()) -> None:
        blockers = tuple(blocking_task_ids)
        super().__init__(f"task {task_id} is blocked by incomplete dependencies: {list(blockers)}")
        self.task_id = task_id
        self.blocking_task_ids: tuple[str, ...] = blockers


class OptimisticLockError(TaskboardError):
    def __init__(self, task_id: str, expected_version: int) -> None:
        super().__init__(
            f"optimistic lock conflict for task {task_id} at expected version {expected_version}"
        )
        self.task_id = task_id
        self.expected_version = expected_version


class SchemaValidationError(TaskboardError):
    def __init__(self, schema_name: str, problems: Sequence[str]) -> None:
        items = tuple(problems)
        super().__init__(f"schema validation failed ({schema_name}): {'; '.join(items)}")
        self.schema_name = schema_name
        self.problems: tuple[str, ...] = items


class PublishFailedError(TaskboardError):
    def __init__(self, event_type: str, external_id: str, cause: BaseException) -> None:
        super().__init__(f"publishing {event_type} {external_id} failed: {cause!r}")
        self.event_type = event_type
        self.external_id = external_id


class MaxHopsExceededError(TaskboardError):
    def __init__(self, hop_count: int, max_hops: int = 8) -> None:
        super().__init__(f"hop_count {hop_count} reached/exceeded limit {max_hops}")
        self.hop_count = hop_count
        self.max_hops = max_hops
