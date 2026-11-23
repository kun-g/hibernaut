from __future__ import annotations

import sys
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from taskboard.core.errors import MaxHopsExceededError, OptimisticLockError
from taskboard.core.models import (
    MAX_HOPS,
    DependencyKind,
    TaskItem,
    new_trace_id,
    utc_now_iso,
)
from taskboard.ports import AuditSinkPort, EventPublisherPort, TaskStorePort

_MAX_CAS_RETRIES = 3


class UnlockDispatcher:
    def __init__(
        self,
        store: TaskStorePort,
        publisher: EventPublisherPort | None = None,
        audit: AuditSinkPort | None = None,
    ) -> None:
        self._store = store
        self._publisher = publisher
        self._audit = audit

    def _safe_audit(
        self,
        event_name: str,
        *,
        task_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        if self._audit is None:
            return
        try:
            self._audit.emit(event_name, task_id=task_id, detail=detail)
        except Exception as exc:  # noqa: BLE001
            print(f"[taskboard] audit sink failed on {event_name}: {exc!r}", file=sys.stderr)

    def _guard_hops(self, hop_count: int, *, task_id: str) -> None:
        if hop_count >= MAX_HOPS:
            self._safe_audit(
                "max_hops_exceeded",
                task_id=task_id,
                detail={"hop_count": hop_count, "max_hops": MAX_HOPS},
            )
            raise MaxHopsExceededError(hop_count, MAX_HOPS)

    def dispatch_ready_task(
        self,
        task: TaskItem,
        *,
        trace_id: str | None = None,
        hop_count: int = 0,
        replay: bool = False,
    ) -> TaskItem | None:
        if task.status != "ready":
            return None
        if self._store.count_incomplete_blockers(task.task_id) > 0:
            return None

        self._guard_hops(hop_count, task_id=task.task_id)
        if task.claimed_ready_at is not None and task.claimed_ready_at == task.ready_at:
            # This ready episode is already claimed. Only the redelivery of the event that made the
            # claim republishes it (same external_id, deduped by Switchboard), finishing a publish
            # that failed; anyone else reaching it concurrently leaves it to its claimer.
            if not replay:
                return None
            claimed = task
        else:
            claimed = self._store.try_claim_dispatch(task.task_id, expected_version=task.version)
            if claimed is None:
                return None

        effective_trace = trace_id or new_trace_id()
        next_hop = hop_count + 1

        if self._publisher is not None:
            if claimed.is_agent_assignee():
                self._publisher.publish_work_ready(
                    claimed,
                    trace_id=effective_trace,
                    hop_count=next_hop,
                )
            elif claimed.is_human_assignee():
                self._publisher.publish_notification(
                    notify_kind="human_todo",
                    task=claimed,
                    reason="task_ready_for_human",
                    trace_id=effective_trace,
                    hop_count=next_hop,
                    target_assignee=claimed.assignee,
                    dedupe_key=f"d{claimed.dispatch_seq}",
                )

        self._safe_audit(
            "task_dispatched",
            task_id=claimed.task_id,
            detail={
                "dispatch_seq": claimed.dispatch_seq,
                "assignee": claimed.assignee,
                "trace_id": effective_trace,
                "hop_count": next_hop,
            },
        )
        return claimed

    def on_task_completed(
        self,
        completed_task_id: str,
        *,
        trace_id: str | None = None,
        hop_count: int = 0,
        replay: bool = False,
    ) -> list[TaskItem]:
        candidates = self._store.list_unblocked_downstream(completed_task_id)
        promoted: list[TaskItem] = []
        now = utc_now_iso()

        for cand in candidates:
            updated = self._store.try_promote_to_ready(
                cand.task_id,
                expected_version=cand.version,
                ready_at=now,
            )
            if updated is not None:
                promoted.append(updated)

        # Rows already ready are included so a redelivered completion finishes a fan-out that a
        # failed publish cut short.
        promoted_ids = {item.task_id for item in promoted}
        promoted.extend(
            item
            for item in self._store.list_downstream_tasks(completed_task_id, kind="blocks")
            if item.status == "ready" and item.task_id not in promoted_ids
        )

        # Zero backpressure: sort by (priority ASC, ready_at ASC) and emit immediately.
        promoted.sort(key=lambda item: (item.priority, item.ready_at or ""))
        dispatched: list[TaskItem] = []
        effective_trace = trace_id or new_trace_id()
        for item in promoted:
            sent = self.dispatch_ready_task(
                item, trace_id=effective_trace, hop_count=hop_count, replay=replay
            )
            dispatched.append(sent if sent is not None else item)
        return dispatched

    def on_dependency_added(
        self,
        upstream_task_id: str,
        downstream_task_id: str,
        *,
        kind: DependencyKind = "blocks",
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> None:
        if kind != "blocks":
            return
        upstream = self._store.get_task(upstream_task_id)
        if upstream is None or upstream.status == "done":
            return

        effective_trace = trace_id or new_trace_id()
        for _ in range(_MAX_CAS_RETRIES):
            downstream = self._store.get_task(downstream_task_id)
            if downstream is None:
                return
            if downstream.status == "ready":
                demoted = replace(
                    downstream,
                    status="backlog",
                    ready_at=None,
                    updated_at=utc_now_iso(),
                )
                try:
                    self._store.upsert_task(demoted, expected_version=downstream.version)
                except OptimisticLockError:
                    continue
                self._safe_audit(
                    "task_demoted_to_backlog",
                    task_id=downstream.task_id,
                    detail={"upstream_task_id": upstream_task_id},
                )
                return
            if downstream.status == "in_progress":
                self._guard_hops(hop_count, task_id=downstream.task_id)
                if self._publisher is not None:
                    self._publisher.publish_notification(
                        notify_kind="dependency_invalidated",
                        task=downstream,
                        reason=f"new_incomplete_blocker:{upstream_task_id}",
                        trace_id=effective_trace,
                        hop_count=hop_count + 1,
                        target_assignee=downstream.assignee,
                    )
                self._safe_audit(
                    "dependency_invalidated",
                    task_id=downstream.task_id,
                    detail={"upstream_task_id": upstream_task_id},
                )
                return
            return

    def on_dependency_removed(
        self,
        downstream_task_id: str,
        *,
        kind: DependencyKind = "blocks",
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> TaskItem | None:
        if kind != "blocks":
            return None
        if self._store.count_incomplete_blockers(downstream_task_id) > 0:
            return None

        downstream = self._store.get_task(downstream_task_id)
        if downstream is None:
            return None

        effective_trace = trace_id or new_trace_id()
        if downstream.status == "backlog":
            promoted = self._store.try_promote_to_ready(
                downstream.task_id,
                expected_version=downstream.version,
                ready_at=utc_now_iso(),
            )
            if promoted is None:
                return None
            return self.dispatch_ready_task(
                promoted,
                trace_id=effective_trace,
                hop_count=hop_count,
            )

        if downstream.status == "ready":
            return self.dispatch_ready_task(
                downstream,
                trace_id=effective_trace,
                hop_count=hop_count,
            )
        return None

    def on_task_cancelled(
        self,
        cancelled_task_id: str,
        *,
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> list[TaskItem]:
        downstream_list = self._store.list_downstream_tasks(cancelled_task_id, kind="blocks")
        affected: list[TaskItem] = []
        effective_trace = trace_id or new_trace_id()
        cancelled = self._store.get_task(cancelled_task_id)
        # Keyed on the upstream's terminal version: a replay repeats the id, a later
        # reopen-and-cancel gets a new one.
        dedupe_key = f"u{cancelled_task_id}:v{cancelled.version if cancelled else 0}"

        for item in downstream_list:
            updated = item
            for _ in range(_MAX_CAS_RETRIES):
                fresh = self._store.get_task(item.task_id)
                if fresh is None or fresh.status in {"done", "cancelled"}:
                    break
                patched = replace(
                    fresh,
                    blocked_reason="upstream_cancelled",
                    updated_at=utc_now_iso(),
                )
                try:
                    updated = self._store.upsert_task(patched, expected_version=fresh.version)
                    break
                except OptimisticLockError:
                    continue

            affected.append(updated)
            self._guard_hops(hop_count, task_id=updated.task_id)
            if self._publisher is not None:
                self._publisher.publish_notification(
                    notify_kind="upstream_cancelled",
                    task=updated,
                    reason=f"upstream_task_cancelled:{cancelled_task_id}",
                    trace_id=effective_trace,
                    hop_count=hop_count + 1,
                    target_assignee=updated.assignee,
                    dedupe_key=dedupe_key,
                )
            self._safe_audit(
                "upstream_cancelled",
                task_id=updated.task_id,
                detail={"cancelled_upstream_id": cancelled_task_id},
            )
        return affected

    def on_task_reopened(
        self,
        reopened_task_id: str,
        *,
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> None:
        downstream_list = self._store.list_downstream_tasks(reopened_task_id, kind="blocks")
        effective_trace = trace_id or new_trace_id()

        for item in downstream_list:
            for _ in range(_MAX_CAS_RETRIES):
                fresh = self._store.get_task(item.task_id)
                if fresh is None:
                    break
                if fresh.status == "ready":
                    demoted = replace(
                        fresh,
                        status="backlog",
                        ready_at=None,
                        updated_at=utc_now_iso(),
                    )
                    try:
                        self._store.upsert_task(demoted, expected_version=fresh.version)
                    except OptimisticLockError:
                        continue
                    self._safe_audit(
                        "downstream_reblocked_on_reopen",
                        task_id=fresh.task_id,
                        detail={"reopened_upstream_id": reopened_task_id},
                    )
                    break
                if fresh.status in {"in_progress", "done"}:
                    self._guard_hops(hop_count, task_id=fresh.task_id)
                    if self._publisher is not None:
                        self._publisher.publish_notification(
                            notify_kind="dependency_invalidated",
                            task=fresh,
                            reason=f"upstream_reopened:{reopened_task_id}",
                            trace_id=effective_trace,
                            hop_count=hop_count + 1,
                            target_assignee=fresh.assignee,
                        )
                    self._safe_audit(
                        "dependency_invalidated",
                        task_id=fresh.task_id,
                        detail={"reopened_upstream_id": reopened_task_id},
                    )
                    break
                break
