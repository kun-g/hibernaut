from __future__ import annotations

import sys
from collections.abc import Collection, Mapping, Sequence
from dataclasses import replace
from typing import Any, Literal

from taskboard.core.errors import (
    IllegalTransitionError,
    OptimisticLockError,
    TaskNotFoundError,
    UpstreamTaskNotFoundError,
)
from taskboard.core.merge import MergeResult, apply_manual_patch, merge_external_update
from taskboard.core.models import (
    DependencyKind,
    ExpandedFlow,
    FlowPlan,
    TaskAction,
    TaskItem,
    TaskSourceLink,
    TaskStatus,
    TaskView,
    new_task_id,
    new_trace_id,
    utc_now_iso,
)
from taskboard.core.state_machine import TERMINAL_STATUSES, validate_transition
from taskboard.engine.dispatcher import UnlockDispatcher
from taskboard.engine.flow_expander import FlowExpander
from taskboard.ports import AuditSinkPort, EventPublisherPort, TaskStorePort

_MAX_CAS_RETRIES = 3


class IngestionService:
    def __init__(
        self,
        store: TaskStorePort,
        *,
        publisher: EventPublisherPort | None = None,
        audit: AuditSinkPort | None = None,
        dispatcher: UnlockDispatcher | None = None,
        flow_expander: FlowExpander | None = None,
    ) -> None:
        self._store = store
        self._audit = audit
        self._dispatcher = dispatcher or UnlockDispatcher(store, publisher=publisher, audit=audit)
        self._flow_expander = flow_expander or FlowExpander(store, self._dispatcher)

    @property
    def dispatcher(self) -> UnlockDispatcher:
        return self._dispatcher

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

    def _resolve_task(self, *, task_id: str | None = None, source_ref: str | None = None) -> TaskItem:
        if task_id:
            found = self._store.get_task(task_id)
            if found is not None:
                return found
            raise TaskNotFoundError(task_id)
        if source_ref:
            found = self._store.find_by_source_ref(source_ref)
            if found is not None:
                return found
            raise TaskNotFoundError(source_ref)
        raise TaskNotFoundError("<missing task_id/source_ref>")

    def _resolve_upstream_ids(self, depends_on: Sequence[str]) -> list[str]:
        resolved: list[str] = []
        for ref in depends_on:
            by_id = self._store.get_task(ref)
            if by_id is not None:
                resolved.append(by_id.task_id)
                continue
            by_source = self._store.find_by_source_ref(ref)
            if by_source is not None:
                resolved.append(by_source.task_id)
                continue
            raise UpstreamTaskNotFoundError(ref)
        return resolved

    # --- Inbound event handlers ---

    def handle_work_opened(
        self,
        *,
        source: str,
        source_ref: str,
        scope: str,
        title: str,
        description: str = "",
        occurred_at_ms: int = 0,
        initial_status: Literal["backlog", "ready"] = "backlog",
        assignee: str = "agent:default",
        task_type: str = "general",
        priority: int = 2,
        depends_on: Sequence[str] = (),
        preferred_executor: str = "",
        artifact_ref: str | None = None,
        reply_target: str | None = None,
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> TaskItem:
        existing = self._store.find_by_source_ref(source_ref)
        if existing is not None:
            patch: dict[str, Any] = {
                "title": title,
                "description": description,
                "assignee": assignee,
                "task_type": task_type,
                "priority": priority,
                "preferred_executor": preferred_executor,
                "scope": scope,
            }
            if artifact_ref is not None:
                patch["artifact_ref"] = artifact_ref
            if reply_target is not None:
                patch["reply_target"] = reply_target
            result = self.handle_work_updated(
                source_ref=source_ref,
                patch=patch,
                occurred_at_ms=occurred_at_ms,
                trace_id=trace_id,
                hop_count=hop_count,
            )
            # A redelivered work.opened finishes a dispatch whose publish failed the first time.
            if result.task.status == "ready":
                dispatched = self._dispatcher.dispatch_ready_task(
                    result.task,
                    trace_id=trace_id or new_trace_id(),
                    hop_count=hop_count,
                )
                if dispatched is not None:
                    return dispatched
            return result.task

        # Strict existence check before creating task: never create ghost placeholders.
        upstream_ids = self._resolve_upstream_ids(depends_on)
        has_incomplete_upstream = any(
            (u := self._store.get_task(uid)) is None or u.status != "done"
            for uid in upstream_ids
        )

        now = utc_now_iso()
        effective_status: TaskStatus = (
            "ready" if (initial_status == "ready" and not has_incomplete_upstream) else "backlog"
        )
        task = TaskItem(
            task_id=new_task_id(),
            source_ref=source_ref,
            title=title,
            description=description,
            scope=scope,
            status=effective_status,
            assignee=assignee,
            task_type=task_type,
            priority=priority,
            preferred_executor=preferred_executor,
            last_source_event_at=occurred_at_ms,
            ready_at=now if effective_status == "ready" else None,
            artifact_ref=artifact_ref,
            reply_target=reply_target,
            created_at=now,
            updated_at=now,
        )
        created = self._store.create_task(task, source=source)

        for uid in upstream_ids:
            self._store.add_dependency(uid, created.task_id, kind="blocks")

        self._safe_audit(
            "work_opened",
            task_id=created.task_id,
            detail={"source": source, "source_ref": source_ref, "status": created.status},
        )

        if created.status == "ready":
            dispatched = self._dispatcher.dispatch_ready_task(
                created,
                trace_id=trace_id or new_trace_id(),
                hop_count=hop_count,
            )
            if dispatched is not None:
                return dispatched
        return created

    def handle_work_updated(
        self,
        *,
        source_ref: str,
        patch: Mapping[str, Any],
        occurred_at_ms: int,
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> MergeResult:
        del trace_id, hop_count
        for _ in range(_MAX_CAS_RETRIES):
            current = self._resolve_task(source_ref=source_ref)
            merged = merge_external_update(current, patch, occurred_at_ms=occurred_at_ms)
            if merged.stale_ignored:
                self._safe_audit(
                    "stale_update_ignored",
                    task_id=current.task_id,
                    detail={
                        "source_ref": source_ref,
                        "occurred_at_ms": occurred_at_ms,
                        "last_source_event_at": current.last_source_event_at,
                    },
                )
                return merged
            try:
                saved = self._store.upsert_task(merged.task, expected_version=current.version)
            except OptimisticLockError:
                continue
            if merged.skipped_override_fields:
                self._safe_audit(
                    "skipped_override_fields",
                    task_id=saved.task_id,
                    detail={"fields": list(merged.skipped_override_fields)},
                )
            return replace(merged, task=saved)
        raise OptimisticLockError(source_ref, -1)

    def handle_work_closed(
        self,
        *,
        source_ref: str,
        state: Literal["closed", "cancelled"] = "cancelled",
        reason: str = "",
        occurred_at_ms: int = 0,
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> TaskItem:
        current = self._resolve_task(source_ref=source_ref)
        if occurred_at_ms and occurred_at_ms < current.last_source_event_at:
            self._safe_audit(
                "stale_close_ignored",
                task_id=current.task_id,
                detail={"occurred_at_ms": occurred_at_ms},
            )
            return current
        if current.status in TERMINAL_STATUSES:
            # Redelivery after the terminal write: replay the fan-out the first attempt may have lost.
            return self.tb_transition_task(
                current.task_id,
                current.status,
                reason=reason or f"work.closed:{state}",
                trace_id=trace_id,
                hop_count=hop_count,
            )

        target: TaskStatus = "cancelled" if state == "cancelled" else (
            "done" if current.status == "in_progress" else "cancelled"
        )
        return self.tb_transition_task(
            current.task_id,
            target,
            reason=reason or f"work.closed:{state}",
            reopen=False,
            trace_id=trace_id,
            hop_count=hop_count,
        )

    def handle_execution_reported(
        self,
        *,
        task_action: TaskAction,
        task_id: str | None = None,
        source_ref: str | None = None,
        stage: str | None = None,
        assignee: str | None = None,
        artifact_ref: str | None = None,
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> TaskItem:
        if task_action == "claim":
            return self._handle_claim(
                task_id=task_id,
                source_ref=source_ref,
                assignee=assignee,
                stage=stage,
            )
        if task_action == "update_stage":
            return self._handle_update_stage(
                task_id=task_id,
                source_ref=source_ref,
                stage=stage or "",
                artifact_ref=artifact_ref,
            )
        if task_action == "complete":
            current = self._resolve_task(task_id=task_id, source_ref=source_ref)
            if artifact_ref is not None or stage is not None:
                self._apply_execution_metadata(
                    current.task_id,
                    stage=stage,
                    artifact_ref=artifact_ref,
                )
            return self.tb_transition_task(
                current.task_id,
                "done",
                reason="execution.reported:complete",
                reopen=False,
                trace_id=trace_id,
                hop_count=hop_count,
            )
        if task_action == "release":
            current = self._resolve_task(task_id=task_id, source_ref=source_ref)
            return self.tb_transition_task(
                current.task_id,
                "ready",
                reason="execution.reported:release",
                reopen=False,
                trace_id=trace_id,
                hop_count=hop_count,
            )
        raise ValueError(f"unsupported task_action: {task_action!r}")

    def _handle_claim(
        self,
        *,
        task_id: str | None,
        source_ref: str | None,
        assignee: str | None,
        stage: str | None,
    ) -> TaskItem:
        for _ in range(_MAX_CAS_RETRIES):
            current = self._resolve_task(task_id=task_id, source_ref=source_ref)
            blockers = self._store.list_incomplete_blocker_ids(current.task_id)
            validate_transition(
                current.status,
                "in_progress",
                task_id=current.task_id,
                incomplete_blockers=len(blockers),
                blocking_task_ids=blockers,
                is_late_running=(current.status in TERMINAL_STATUSES),
            )
            updated = replace(
                current,
                status="in_progress",
                assignee=assignee if assignee else current.assignee,
                stage=stage if stage is not None else current.stage,
                updated_at=utc_now_iso(),
            )
            try:
                return self._store.upsert_task(updated, expected_version=current.version)
            except OptimisticLockError:
                continue
        raise OptimisticLockError(task_id or source_ref or "", -1)

    def _handle_update_stage(
        self,
        *,
        task_id: str | None,
        source_ref: str | None,
        stage: str,
        artifact_ref: str | None,
    ) -> TaskItem:
        for _ in range(_MAX_CAS_RETRIES):
            current = self._resolve_task(task_id=task_id, source_ref=source_ref)
            if current.status in TERMINAL_STATUSES:
                raise IllegalTransitionError(
                    current.status,
                    "in_progress",
                    "terminal status rejects late running stage update",
                )
            updated = replace(
                current,
                stage=stage,
                artifact_ref=artifact_ref if artifact_ref is not None else current.artifact_ref,
                updated_at=utc_now_iso(),
            )
            try:
                return self._store.upsert_task(updated, expected_version=current.version)
            except OptimisticLockError:
                continue
        raise OptimisticLockError(task_id or source_ref or "", -1)

    def _apply_execution_metadata(
        self,
        task_id: str,
        *,
        stage: str | None,
        artifact_ref: str | None,
    ) -> None:
        for _ in range(_MAX_CAS_RETRIES):
            current = self._resolve_task(task_id=task_id)
            if current.status in TERMINAL_STATUSES:
                return
            updated = replace(
                current,
                stage=stage if stage is not None else current.stage,
                artifact_ref=artifact_ref if artifact_ref is not None else current.artifact_ref,
                updated_at=utc_now_iso(),
            )
            try:
                self._store.upsert_task(updated, expected_version=current.version)
                return
            except OptimisticLockError:
                continue

    # --- 7 MCP / API tool methods (Chapter 8.3) ---

    def tb_create_task(
        self,
        scope: str,
        title: str,
        *,
        description: str = "",
        initial_status: Literal["backlog", "ready"] = "backlog",
        assignee: str = "agent:default",
        task_type: str = "general",
        priority: int = 2,
        source_ref: str | None = None,
        parent_task_id: str | None = None,
        preferred_executor: str = "",
        artifact_ref: str | None = None,
        reply_target: str | None = None,
        source: str = "mcp",
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> TaskItem:
        now = utc_now_iso()
        task = TaskItem(
            task_id=new_task_id(),
            source_ref=source_ref,
            title=title,
            description=description,
            scope=scope,
            status=initial_status,
            assignee=assignee,
            task_type=task_type,
            priority=priority,
            preferred_executor=preferred_executor,
            parent_task_id=parent_task_id,
            ready_at=now if initial_status == "ready" else None,
            artifact_ref=artifact_ref,
            reply_target=reply_target,
            created_at=now,
            updated_at=now,
        )
        created = self._store.create_task(task, source=source if source_ref else None)
        if created.status == "ready":
            dispatched = self._dispatcher.dispatch_ready_task(
                created,
                trace_id=trace_id or new_trace_id(),
                hop_count=hop_count,
            )
            if dispatched is not None:
                return dispatched
        return created

    def tb_update_task(
        self,
        task_id: str,
        patch: Mapping[str, Any],
        *,
        unlock_fields: Collection[str] = (),
    ) -> TaskItem:
        for _ in range(_MAX_CAS_RETRIES):
            current = self._resolve_task(task_id=task_id)
            updated = apply_manual_patch(current, patch, unlock_fields=unlock_fields)
            try:
                return self._store.upsert_task(updated, expected_version=current.version)
            except OptimisticLockError:
                continue
        raise OptimisticLockError(task_id, -1)

    def tb_transition_task(
        self,
        task_id: str,
        target_status: TaskStatus,
        *,
        reason: str = "",
        reopen: bool = False,
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> TaskItem:
        effective_trace = trace_id or new_trace_id()
        for _ in range(_MAX_CAS_RETRIES):
            current = self._resolve_task(task_id=task_id)
            if current.status in TERMINAL_STATUSES and target_status == current.status and not reopen:
                self._replay_terminal_fanout(current, trace_id=effective_trace, hop_count=hop_count)
                return current
            blockers = self._store.list_incomplete_blocker_ids(current.task_id)
            validate_transition(
                current.status,
                target_status,
                task_id=current.task_id,
                incomplete_blockers=len(blockers),
                blocking_task_ids=blockers,
                reopen=reopen,
            )
            now = utc_now_iso()
            ready_at = now if target_status == "ready" else current.ready_at
            blocked_reason = "" if target_status in {"ready", "in_progress", "done"} else current.blocked_reason
            updated = replace(
                current,
                status=target_status,
                ready_at=ready_at,
                blocked_reason=blocked_reason,
                updated_at=now,
            )
            try:
                saved = self._store.upsert_task(updated, expected_version=current.version)
            except OptimisticLockError:
                continue

            self._safe_audit(
                "task_transitioned",
                task_id=saved.task_id,
                detail={
                    "from": current.status,
                    "to": target_status,
                    "reason": reason,
                    "reopen": reopen,
                },
            )

            if current.status == "done" and reopen and target_status in {"in_progress", "ready"}:
                self._dispatcher.on_task_reopened(
                    saved.task_id,
                    trace_id=effective_trace,
                    hop_count=hop_count,
                )

            if target_status == "done":
                self._dispatcher.on_task_completed(
                    saved.task_id,
                    trace_id=effective_trace,
                    hop_count=hop_count,
                )
            elif target_status == "cancelled":
                self._dispatcher.on_task_cancelled(
                    saved.task_id,
                    trace_id=effective_trace,
                    hop_count=hop_count,
                )
            elif target_status == "ready" and current.status in {"backlog", "done"}:
                dispatched = self._dispatcher.dispatch_ready_task(
                    saved,
                    trace_id=effective_trace,
                    hop_count=hop_count,
                )
                if dispatched is not None:
                    return dispatched
            return saved
        raise OptimisticLockError(task_id, -1)

    def _replay_terminal_fanout(self, task: TaskItem, *, trace_id: str, hop_count: int) -> None:
        # The terminal write commits before the fan-out publishes, so a publish failure leaves
        # downstream work undispatched; repeating the same terminal transition finishes it.
        self._safe_audit("terminal_fanout_replayed", task_id=task.task_id, detail={"status": task.status})
        if task.status == "done":
            self._dispatcher.on_task_completed(task.task_id, trace_id=trace_id, hop_count=hop_count)
        elif task.status == "cancelled":
            self._dispatcher.on_task_cancelled(task.task_id, trace_id=trace_id, hop_count=hop_count)

    def tb_link_source(
        self,
        task_id: str,
        source_ref: str,
        source: str,
        *,
        set_primary: bool = False,
        last_synced_at: int = 0,
    ) -> TaskSourceLink:
        return self._store.link_source(
            task_id,
            source_ref,
            source,
            is_primary=set_primary,
            last_synced_at=last_synced_at,
        )

    def tb_manage_dependency(
        self,
        op: Literal["add", "remove"],
        upstream_task_id: str,
        downstream_task_id: str,
        *,
        kind: DependencyKind = "blocks",
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> bool:
        effective_trace = trace_id or new_trace_id()
        if op == "add":
            self._store.add_dependency(upstream_task_id, downstream_task_id, kind=kind)
            self._dispatcher.on_dependency_added(
                upstream_task_id,
                downstream_task_id,
                kind=kind,
                trace_id=effective_trace,
                hop_count=hop_count,
            )
            return True
        if op == "remove":
            removed = self._store.remove_dependency(
                upstream_task_id,
                downstream_task_id,
                kind=kind,
            )
            if removed:
                self._dispatcher.on_dependency_removed(
                    downstream_task_id,
                    kind=kind,
                    trace_id=effective_trace,
                    hop_count=hop_count,
                )
            return removed
        raise ValueError(f"unsupported dependency op: {op!r}")

    def tb_instantiate_flow(
        self,
        plan: FlowPlan,
        *,
        trace_id: str | None = None,
        hop_count: int = 0,
    ) -> ExpandedFlow:
        return self._flow_expander.instantiate_flow(
            plan,
            trace_id=trace_id,
            hop_count=hop_count,
        )

    def tb_list_tasks(
        self,
        *,
        scope: str | None = None,
        status: TaskStatus | None = None,
        assignee_prefix: str | None = None,
        dispatchable_only: bool = False,
        parent_task_id: str | None = None,
    ) -> list[TaskView]:
        return self._store.list_tasks(
            scope=scope,
            status=status,
            assignee_prefix=assignee_prefix,
            dispatchable_only=dispatchable_only,
            parent_task_id=parent_task_id,
        )
