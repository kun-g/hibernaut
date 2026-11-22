from __future__ import annotations

import json
import re
import sys
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, cast

from switchboard.envelope import CanonicalEvent, parse_utc, validate_envelope
from switchboard.ports import DeliverStatus, EventBusPort
from taskboard.core.errors import (
    IllegalTransitionError,
    MaxHopsExceededError,
    PublishFailedError,
    SchemaValidationError,
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

_DEFAULT_SCHEMAS_DIR = Path(__file__).resolve().parents[3] / "schemas" / "taskboard"


@lru_cache(maxsize=16)
def load_schema(schema_name: str, schemas_dir: Path | None = None) -> dict[str, Any]:
    root = schemas_dir or _DEFAULT_SCHEMAS_DIR
    path = root / schema_name
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _validate_node(
    value: Any,
    schema: Mapping[str, Any],
    path: str,
    problems: list[str],
) -> None:
    expected_type = schema.get("type")
    if expected_type == "object":
        if not isinstance(value, Mapping):
            problems.append(f"{path}: expected object, got {type(value).__name__}")
            return
        mapping_value = cast(Mapping[str, Any], value)
        required = schema.get("required") or ()
        for req_key in required:
            if req_key not in mapping_value or mapping_value[req_key] is None:
                problems.append(f"{path}.{req_key}: required property missing")
        props = cast(Mapping[str, Mapping[str, Any]], schema.get("properties") or {})
        if schema.get("additionalProperties") is False:
            extra = sorted(set(mapping_value.keys()) - set(props.keys()))
            for key in extra:
                problems.append(f"{path}.{key}: additional property not allowed")
        for key, item_val in mapping_value.items():
            if key in props:
                _validate_node(item_val, props[key], f"{path}.{key}", problems)
        return

    if expected_type == "string":
        if not isinstance(value, str):
            problems.append(f"{path}: expected string, got {type(value).__name__}")
            return
        if "minLength" in schema and len(value) < int(schema["minLength"]):
            problems.append(f"{path}: length {len(value)} < minLength {schema['minLength']}")
        if "enum" in schema and value not in schema["enum"]:
            problems.append(f"{path}: {value!r} not in enum {schema['enum']}")
        if "pattern" in schema and not re.match(str(schema["pattern"]), value):
            problems.append(f"{path}: {value!r} does not match pattern {schema['pattern']!r}")
        return

    if expected_type == "integer":
        # bool is a subclass of int in Python; JSON Schema integer excludes booleans.
        if isinstance(value, bool) or not isinstance(value, int):
            problems.append(f"{path}: expected integer, got {type(value).__name__}")
            return
        if "minimum" in schema and value < int(schema["minimum"]):
            problems.append(f"{path}: {value} < minimum {schema['minimum']}")
        if "maximum" in schema and value > int(schema["maximum"]):
            problems.append(f"{path}: {value} > maximum {schema['maximum']}")
        return

    if expected_type == "array":
        if not isinstance(value, (list, tuple)):
            problems.append(f"{path}: expected array, got {type(value).__name__}")
            return
        seq_val = cast(list[Any] | tuple[Any, ...], value)
        if "minItems" in schema and len(seq_val) < int(schema["minItems"]):
            problems.append(f"{path}: length {len(seq_val)} < minItems {schema['minItems']}")
        item_schema = schema.get("items")
        if isinstance(item_schema, Mapping):
            for idx, elem in enumerate(seq_val):
                _validate_node(elem, cast(Mapping[str, Any], item_schema), f"{path}[{idx}]", problems)


def validate_against_schema(
    schema_name: str,
    payload: Mapping[str, Any],
    *,
    schemas_dir: Path | None = None,
) -> None:
    schema = load_schema(schema_name, schemas_dir)
    problems: list[str] = []
    _validate_node(payload, schema, "$", problems)
    if problems:
        raise SchemaValidationError(schema_name, problems)


def _occurred_at_to_ms(event: CanonicalEvent) -> int:
    override = event.metadata.get("occurred_at_ms")
    if isinstance(override, int) and not isinstance(override, bool):
        return override
    dt = parse_utc(event.occurred_at)
    return int(dt.timestamp() * 1000)


class SwitchboardAdapter:
    def __init__(
        self,
        bus: EventBusPort,
        *,
        ingestion: IngestionService | None = None,
        audit: AuditSinkPort | None = None,
        schemas_dir: Path | None = None,
    ) -> None:
        self._bus = bus
        self._ingestion = ingestion
        self._audit = audit
        self._schemas_dir = schemas_dir

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

        raw_hop = event.metadata.get("hop_count", 0)
        hop_count = raw_hop if isinstance(raw_hop, int) and not isinstance(raw_hop, bool) else 0
        if hop_count >= MAX_HOPS:
            self._safe_audit(
                "max_hops_exceeded",
                task_id=cast(str | None, event.metadata.get("task_id")),
                detail={"event_id": event.event_id, "hop_count": hop_count},
            )
            raise MaxHopsExceededError(hop_count, MAX_HOPS)

        trace_id = str(event.metadata.get("trace_id") or event.event_id)
        source_ref = str(event.metadata.get("source_ref") or event.correlation_key or event.external_id)
        occurred_at_ms = _occurred_at_to_ms(event)

        if event.event_type == "work.opened":
            validate_against_schema(
                "work_opened.metadata.schema.json",
                event.metadata,
                schemas_dir=self._schemas_dir,
            )
            raw_initial = str(event.metadata.get("initial_status") or "backlog")
            initial_status: Literal["backlog", "ready"] = (
                "ready" if raw_initial == "ready" else "backlog"
            )
            depends_on = tuple(str(x) for x in (event.metadata.get("depends_on") or ()))
            return self._ingestion.handle_work_opened(
                source=event.source,
                source_ref=source_ref,
                scope=event.scope,
                title=event.subject,
                description=event.body,
                occurred_at_ms=occurred_at_ms,
                initial_status=initial_status,
                assignee=str(event.metadata.get("assignee") or "agent:default"),
                task_type=str(event.metadata.get("task_type") or "general"),
                priority=int(event.metadata.get("priority", 2)),
                depends_on=depends_on,
                preferred_executor=str(event.metadata.get("preferred_executor") or ""),
                artifact_ref=cast(str | None, event.metadata.get("artifact_ref")),
                reply_target=cast(str | None, event.metadata.get("reply_target")),
                trace_id=trace_id,
                hop_count=hop_count,
            )

        if event.event_type == "work.updated":
            patch: dict[str, Any] = {}
            if event.subject:
                patch["title"] = event.subject
            if event.body:
                patch["description"] = event.body
            for key in ("priority", "assignee", "preferred_executor", "scope", "task_type", "artifact_ref", "reply_target"):
                if key in event.metadata and event.metadata[key] is not None:
                    patch[key] = event.metadata[key]
            return self._ingestion.handle_work_updated(
                source_ref=source_ref,
                patch=patch,
                occurred_at_ms=occurred_at_ms,
                trace_id=trace_id,
                hop_count=hop_count,
            )

        if event.event_type == "work.closed":
            raw_state = str(event.metadata.get("state") or "cancelled")
            state: Literal["closed", "cancelled"] = "closed" if raw_state == "closed" else "cancelled"
            return self._ingestion.handle_work_closed(
                source_ref=source_ref,
                state=state,
                reason=str(event.metadata.get("close_reason") or event.body or ""),
                occurred_at_ms=occurred_at_ms,
                trace_id=trace_id,
                hop_count=hop_count,
            )

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
