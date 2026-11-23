from __future__ import annotations

import sys
from collections.abc import Collection, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any, Literal, cast

from switchboard.envelope import CanonicalEvent, parse_utc, validate_envelope
from switchboard.ports import DeliverStatus, EventBusPort
from taskboard.adapters.json_schema import load_schema, validate_against_schema
from taskboard.core.errors import (
    IllegalTransitionError,
    MaxHopsExceededError,
    PublishFailedError,
    SchemaValidationError,
    TaskNotFoundError,
)
from taskboard.core.merge import MergeResult
from taskboard.core.models import (
    MAX_HOPS,
    NotifyKind,
    TaskAction,
    TaskItem,
)
from taskboard.engine.ingestion import IngestionService
from taskboard.ports import AuditSinkPort

__all__ = ["SwitchboardAdapter", "load_schema", "validate_against_schema"]


def _occurred_at_to_ms(event: CanonicalEvent) -> int:
    override = event.metadata.get("occurred_at_ms")
    if isinstance(override, int) and not isinstance(override, bool):
        return override
    dt = parse_utc(event.occurred_at)
    return int(dt.timestamp() * 1000)


def _source_ref(event: CanonicalEvent) -> str:
    return str(event.metadata.get("source_ref") or event.correlation_key or event.external_id)


# GitHub webhooks and plain submit_work calls carry no trace fields. Defaults go on a copy; a value the
# caller did send is kept as is, so a wrong type or range still fails the schema.
def _with_trace_defaults(event: CanonicalEvent) -> CanonicalEvent:
    missing: dict[str, Any] = {}
    if event.metadata.get("trace_id") is None:
        missing["trace_id"] = event.event_id
    if event.metadata.get("hop_count") is None:
        missing["hop_count"] = 0
    if not missing:
        return event
    return replace(event, metadata={**event.metadata, **missing})


class SwitchboardAdapter:
    def __init__(
        self,
        bus: EventBusPort,
        *,
        ingestion: IngestionService | None = None,
        audit: AuditSinkPort | None = None,
        schemas_dir: Path | None = None,
        auto_ready_sources: Collection[str] = (),
    ) -> None:
        self._bus = bus
        self._ingestion = ingestion
        self._audit = audit
        self._schemas_dir = schemas_dir
        self._auto_ready_sources = frozenset(auto_ready_sources)

    def bind_ingestion(self, ingestion: IngestionService) -> None:
        self._ingestion = ingestion

    def attach(self, consumer_id: str = "taskboard") -> None:
        self._bus.subscribe(consumer_id, self.handle_canonical_event)

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

    # --- EventPublisherPort implementation ---

    def publish_work_ready(
        self,
        task: TaskItem,
        *,
        trace_id: str,
        hop_count: int,
    ) -> None:
        if hop_count > MAX_HOPS:
            self._safe_audit(
                "max_hops_exceeded",
                task_id=task.task_id,
                detail={"hop_count": hop_count, "direction": "outbound"},
            )
            raise MaxHopsExceededError(hop_count, MAX_HOPS)

        metadata: dict[str, Any] = {
            "work_kind": "task",
            "state": "ready",
            "task_id": task.task_id,
            "dispatch_seq": task.dispatch_seq,
            "assignee": task.assignee,
            "task_type": task.task_type,
            "priority": task.priority,
            "trace_id": trace_id,
            "hop_count": hop_count,
        }
        if task.preferred_executor:
            metadata["preferred_executor"] = task.preferred_executor
        if task.artifact_ref:
            metadata["artifact_ref"] = task.artifact_ref
        if task.reply_target:
            metadata["reply_target"] = task.reply_target
        if task.parent_task_id:
            metadata["parent_task_id"] = task.parent_task_id

        external_id = f"tb:{task.task_id}:d{task.dispatch_seq}"
        try:
            validate_against_schema(
                "work_ready.metadata.schema.json",
                metadata,
                schemas_dir=self._schemas_dir,
            )
            event = CanonicalEvent(
                source="taskboard",
                event_type="work.ready",
                external_id=external_id,
                scope=task.scope or "user:main",
                correlation_key=task.source_ref or task.task_id,
                actor="taskboard",
                subject=task.title,
                body=task.description,
                metadata=metadata,
            )
            validate_envelope(event)
            self._bus.publish(event)
        except Exception as exc:
            raise PublishFailedError("work.ready", external_id, exc) from exc

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
    ) -> None:
        if hop_count > MAX_HOPS:
            self._safe_audit(
                "max_hops_exceeded",
                task_id=task.task_id,
                detail={"hop_count": hop_count, "direction": "outbound"},
            )
            raise MaxHopsExceededError(hop_count, MAX_HOPS)

        metadata: dict[str, Any] = {
            "outcome_kind": "emit",
            "run_id": f"tb_run_{task.task_id}",
            "session_id": f"tb_sess_{task.task_id}",
            "task_id": task.task_id,
            "notify_kind": notify_kind,
            "target_assignee": target_assignee or task.assignee,
            "reason": reason,
            "trace_id": trace_id,
            "hop_count": hop_count,
        }
        if task.artifact_ref:
            metadata["artifact_ref"] = task.artifact_ref
        if task.reply_target:
            metadata["reply_target"] = task.reply_target

        # A caller-supplied dedupe_key keeps the id stable when a fan-out is replayed.
        key = dedupe_key or f"v{task.version}:d{task.dispatch_seq}"
        external_id = f"tb:notify:{notify_kind}:{task.task_id}:{key}"
        try:
            validate_against_schema(
                "execution_reported.metadata.schema.json",
                metadata,
                schemas_dir=self._schemas_dir,
            )
            event = CanonicalEvent(
                source="taskboard",
                event_type="execution.reported",
                external_id=external_id,
                scope=task.scope or "user:main",
                correlation_key=task.source_ref or task.task_id,
                actor="taskboard",
                subject=f"[{notify_kind}] {task.title}",
                body=reason,
                metadata=metadata,
            )
            validate_envelope(event)
            self._bus.publish(event)
        except Exception as exc:
            raise PublishFailedError("execution.reported", external_id, exc) from exc

    # --- Inbound consumer handling ---

    def ingest_event(self, event: CanonicalEvent) -> TaskItem | MergeResult | None:
        if self._ingestion is None:
            raise RuntimeError("SwitchboardAdapter has no bound IngestionService")

        # Never consume our own emitted notifications to avoid self-loops.
        if event.source == "taskboard":
            return None

        event = _with_trace_defaults(event)
        raw_hop = event.metadata["hop_count"]
        hop_count = raw_hop if isinstance(raw_hop, int) and not isinstance(raw_hop, bool) else 0
        if hop_count >= MAX_HOPS:
            self._safe_audit(
                "max_hops_exceeded",
                task_id=cast(str | None, event.metadata.get("task_id")),
                detail={"event_id": event.event_id, "hop_count": hop_count},
            )
            raise MaxHopsExceededError(hop_count, MAX_HOPS)

        trace_id = str(event.metadata.get("trace_id") or event.event_id)
        source_ref = _source_ref(event)
        occurred_at_ms = _occurred_at_to_ms(event)

        if event.event_type == "work.opened":
            return self._open_work(event, hop_count=hop_count)

        if event.event_type == "work.updated":
            patch: dict[str, Any] = {}
            if event.subject:
                patch["title"] = event.subject
            if event.body:
                patch["description"] = event.body
            for key in ("priority", "assignee", "preferred_executor", "scope", "task_type", "artifact_ref", "reply_target"):
                if key in event.metadata and event.metadata[key] is not None:
                    patch[key] = event.metadata[key]
            try:
                updated = self._ingestion.handle_work_updated(
                    source_ref=source_ref,
                    patch=patch,
                    occurred_at_ms=occurred_at_ms,
                    trace_id=trace_id,
                    hop_count=hop_count,
                )
            except TaskNotFoundError:
                if event.source not in self._auto_ready_sources:
                    raise
                # A closed or untitled item will never get a work.opened, so a retry cannot succeed: ack it.
                if not event.subject or event.metadata.get("state") == "closed":
                    self._ignore_untracked(event, source_ref)
                    return None
                # GitHub reports a trigger label added to an existing issue as work.updated, so the first
                # event Taskboard sees for it is an update; handle_work_opened is idempotent on source_ref.
                self._safe_audit(
                    "work_updated_opened_task",
                    detail={"event_id": event.event_id, "source_ref": source_ref},
                )
                return self._open_work(event, hop_count=hop_count)
            return self._replay_ready_dispatch(updated, trace_id=trace_id, hop_count=hop_count)

        if event.event_type == "work.closed":
            raw_state = str(event.metadata.get("state") or "cancelled")
            state: Literal["closed", "cancelled"] = "closed" if raw_state == "closed" else "cancelled"
            try:
                return self._ingestion.handle_work_closed(
                    source_ref=source_ref,
                    state=state,
                    reason=str(event.metadata.get("close_reason") or event.body or ""),
                    occurred_at_ms=occurred_at_ms,
                    trace_id=trace_id,
                    hop_count=hop_count,
                )
            except TaskNotFoundError:
                # Closing an item Taskboard never tracked (e.g. older than the deployment) has nothing to do.
                if event.source not in self._auto_ready_sources:
                    raise
                self._ignore_untracked(event, source_ref)
                return None

        if event.event_type == "execution.reported":
            validate_against_schema(
                "execution_reported.metadata.schema.json",
                event.metadata,
                schemas_dir=self._schemas_dir,
            )
            task_action = event.metadata.get("task_action")
            if task_action is None:
                return None
            return self._ingestion.handle_execution_reported(
                task_action=cast(TaskAction, str(task_action)),
                task_id=cast(str | None, event.metadata.get("task_id")),
                source_ref=source_ref,
                stage=cast(str | None, event.metadata.get("stage")),
                assignee=cast(str | None, event.metadata.get("assignee")),
                artifact_ref=cast(str | None, event.metadata.get("artifact_ref")),
                trace_id=trace_id,
                hop_count=hop_count,
            )

        return None

    # The first delivery may have committed the ready task and claimed its dispatch, then failed to
    # publish; the redelivery lands here as an update. Replaying republishes the same external_id
    # (tb:<task_id>:d<seq>), which Switchboard dedupes when the first publish did get through.
    def _replay_ready_dispatch(self, result: MergeResult, *, trace_id: str, hop_count: int) -> MergeResult:
        assert self._ingestion is not None
        if result.task.status != "ready":
            return result
        dispatched = self._ingestion.dispatcher.dispatch_ready_task(
            result.task, trace_id=trace_id, hop_count=hop_count, replay=True
        )
        return result if dispatched is None else replace(result, task=dispatched)

    def _ignore_untracked(self, event: CanonicalEvent, source_ref: str) -> None:
        self._safe_audit(
            "untracked_work_ignored",
            detail={
                "event_id": event.event_id,
                "event_type": event.event_type,
                "source_ref": source_ref,
                "state": event.metadata.get("state"),
            },
        )

    def _open_work(self, event: CanonicalEvent, *, hop_count: int) -> TaskItem:
        assert self._ingestion is not None
        validate_against_schema(
            "work_opened.metadata.schema.json",
            event.metadata,
            schemas_dir=self._schemas_dir,
        )
        explicit = event.metadata.get("initial_status")
        if explicit is not None:
            initial_status: Literal["backlog", "ready"] = "ready" if explicit == "ready" else "backlog"
        else:
            initial_status = "ready" if event.source in self._auto_ready_sources else "backlog"
        depends_on = tuple(str(x) for x in (event.metadata.get("depends_on") or ()))
        return self._ingestion.handle_work_opened(
            source=event.source,
            source_ref=_source_ref(event),
            scope=event.scope,
            title=event.subject,
            description=event.body,
            occurred_at_ms=_occurred_at_to_ms(event),
            initial_status=initial_status,
            assignee=str(event.metadata.get("assignee") or "agent:default"),
            task_type=str(event.metadata.get("task_type") or "general"),
            priority=int(event.metadata.get("priority", 2)),
            depends_on=depends_on,
            preferred_executor=str(event.metadata.get("preferred_executor") or ""),
            artifact_ref=cast(str | None, event.metadata.get("artifact_ref")),
            reply_target=cast(str | None, event.metadata.get("reply_target")),
            trace_id=str(event.metadata.get("trace_id") or event.event_id),
            hop_count=hop_count,
        )

    def handle_canonical_event(self, event: CanonicalEvent) -> DeliverStatus:
        try:
            self.ingest_event(event)
            return "acked"
        except PublishFailedError as exc:
            # Outbound failure after state was committed: nack so the redelivery replays the fan-out.
            self._safe_audit(
                "outbound_publish_failed",
                task_id=cast(str | None, event.metadata.get("task_id")),
                detail={"event_id": event.event_id, "error": str(exc)},
            )
            return "nack"
        except (SchemaValidationError, MaxHopsExceededError, IllegalTransitionError) as exc:
            self._safe_audit(
                "inbound_event_rejected",
                task_id=cast(str | None, event.metadata.get("task_id")),
                detail={"event_id": event.event_id, "error": str(exc)},
            )
            return "acked"
        except Exception as exc:  # noqa: BLE001
            self._safe_audit(
                "inbound_event_failed",
                task_id=cast(str | None, event.metadata.get("task_id")),
                detail={"event_id": event.event_id, "error": repr(exc)},
            )
            return "nack"
