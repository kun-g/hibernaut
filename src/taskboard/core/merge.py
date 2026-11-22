from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass, replace
from typing import Any, Final

from taskboard.core.models import TaskItem, utc_now_iso

LAYER1_EXTERNAL_MIRROR: Final[frozenset[str]] = frozenset(
    {"title", "description", "artifact_ref", "reply_target"}
)
LAYER2_MANUAL_PROTECTED: Final[frozenset[str]] = frozenset(
    {"priority", "assignee", "preferred_executor", "scope", "task_type"}
)
LAYER3_STATE_MACHINE_EXCLUSIVE: Final[frozenset[str]] = frozenset(
    {
        "status",
        "dispatch_seq",
        "version",
        "ready_at",
        "claimed_ready_at",
        "blocked_reason",
        "last_source_event_at",
    }
)
_MANUAL_EXTRA_EDITABLE: Final[frozenset[str]] = frozenset({"stage", "parent_task_id"})


@dataclass(frozen=True, slots=True)
class MergeResult:
    task: TaskItem
    applied: bool
    skipped_override_fields: tuple[str, ...] = ()
    stale_ignored: bool = False


def merge_external_update(
    task: TaskItem,
    patch: Mapping[str, Any],
    *,
    occurred_at_ms: int,
    now_iso: str | None = None,
) -> MergeResult:
    # Watermark gate: reject out-of-order stale events as strict NO-OP.
    if occurred_at_ms < task.last_source_event_at:
        return MergeResult(task=task, applied=False, skipped_override_fields=(), stale_ignored=True)

    updates: dict[str, Any] = {}
    skipped: list[str] = []

    for field_name, value in patch.items():
        if value is None and field_name not in {"artifact_ref", "reply_target"}:
            continue
        if field_name in LAYER3_STATE_MACHINE_EXCLUSIVE:
            continue
        if field_name in LAYER2_MANUAL_PROTECTED:
            if field_name in task.manual_override_fields:
                skipped.append(field_name)
                continue
            updates[field_name] = value
        elif field_name in LAYER1_EXTERNAL_MIRROR:
            updates[field_name] = value

    updates["last_source_event_at"] = max(task.last_source_event_at, occurred_at_ms)
    updates["updated_at"] = now_iso or utc_now_iso()
    updated = replace(task, **updates)
    return MergeResult(
        task=updated,
        applied=True,
        skipped_override_fields=tuple(sorted(skipped)),
        stale_ignored=False,
    )


def apply_manual_patch(
    task: TaskItem,
    patch: Mapping[str, Any],
    *,
    unlock_fields: Collection[str] = (),
    now_iso: str | None = None,
) -> TaskItem:
    forbidden = set(patch.keys()) & LAYER3_STATE_MACHINE_EXCLUSIVE
    if forbidden:
        raise ValueError(f"cannot manually patch state-machine exclusive fields: {sorted(forbidden)}")

    allowed = LAYER1_EXTERNAL_MIRROR | LAYER2_MANUAL_PROTECTED | _MANUAL_EXTRA_EDITABLE
    unknown = set(patch.keys()) - allowed
    if unknown:
        raise ValueError(f"unknown or non-editable patch fields: {sorted(unknown)}")

    overrides = set(task.manual_override_fields)
    for field_name in unlock_fields:
        overrides.discard(field_name)

    updates: dict[str, Any] = {}
    for field_name, value in patch.items():
        updates[field_name] = value
        if field_name in LAYER2_MANUAL_PROTECTED:
            overrides.add(field_name)

    updates["manual_override_fields"] = frozenset(overrides)
    updates["updated_at"] = now_iso or utc_now_iso()
    return replace(task, **updates)
