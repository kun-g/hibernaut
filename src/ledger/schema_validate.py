"""Zero-dependency ADR-0001 JSON Schema v1.0 validators for AuditRecord & RunRecord."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import re
from typing import Any, TypeGuard, cast

_AUDIT_ID_RE = re.compile(r"^aud_[A-Za-z0-9_-]+$")
_RUN_ID_RE = re.compile(r"^run_[A-Za-z0-9_-]+$")

_SERVICES = frozenset({"switchboard", "taskboard", "hibernaut", "runner", "broker", "ledger"})
_SEVERITIES = frozenset({"info", "warn", "error"})
_OUTCOME_KINDS = frozenset({"silent", "emit", "suspend", "interrupted", "error"})
_EMIT_STATUSES = frozenset({"done", "failed", "stopped", "refused"})
_ERROR_KINDS = frozenset(
    {
        "rate_limit",
        "network",
        "crash",
        "timeout",
        "config_error",
        "permission_denied",
        "tool_error",
        "bad_output",
        "unknown",
    }
)
_USAGE_SOURCES = frozenset({"magpie", "cli", "unknown"})

_AUDIT_ALLOWED_KEYS = frozenset(
    {
        "schema_version",
        "audit_id",
        "occurred_at",
        "service",
        "stage",
        "outcome",
        "severity",
        "correlation_key",
        "trace_id",
        "scope",
        "event_id",
        "external_id",
        "session_id",
        "run_id",
        "task_id",
        "actor",
        "duration_ms",
        "detail",
    }
)

_RUN_ALLOWED_KEYS = frozenset(
    {
        "schema_version",
        "run_id",
        "inbox_id",
        "session_id",
        "event_ids",
        "executor_id",
        "runner_id",
        "started_at",
        "ended_at",
        "duration_ms",
        "outcome_kind",
        "emit_status",
        "correlation_key",
        "trace_id",
        "work_key",
        "parent_session_id",
        "replay_of_run_id",
        "executor_session_id",
        "error_kind",
        "error_summary",
        "exit_code",
        "log_ref",
        "usage",
    }
)

_USAGE_ALLOWED_KEYS = frozenset(
    {
        "usage_source",
        "model",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cost_usd",
    }
)


class SchemaValidationError(ValueError):
    """Raised when a payload violates ADR-0001 v1 JSON Schema constraints."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


def _is_iso_datetime(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _is_int(value: Any) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_opt_str(value: Any) -> bool:
    return value is None or isinstance(value, str)


def validate_audit_record_dict(payload: Mapping[str, Any]) -> list[str]:
    """Validate a dict against schemas/v1/audit_record.schema.json. Returns error list."""
    errors: list[str] = []
    extra = set(payload.keys()) - _AUDIT_ALLOWED_KEYS
    if extra:
        errors.append(f"unexpected fields: {sorted(extra)}")

    if payload.get("schema_version") != "1.0":
        errors.append("schema_version must be '1.0'")
    audit_id = payload.get("audit_id")
    if not isinstance(audit_id, str) or not _AUDIT_ID_RE.match(audit_id):
        errors.append("audit_id must match ^aud_[A-Za-z0-9_-]+$")
    if not _is_iso_datetime(payload.get("occurred_at")):
        errors.append("occurred_at must be a valid RFC3339 date-time")
    if payload.get("service") not in _SERVICES:
        errors.append(f"service must be one of {sorted(_SERVICES)}")
    for req_str in ("stage", "outcome"):
        v = payload.get(req_str)
        if not isinstance(v, str) or not v:
            errors.append(f"{req_str} must be a non-empty string")
    if payload.get("severity") not in _SEVERITIES:
        errors.append(f"severity must be one of {sorted(_SEVERITIES)}")

    for opt_str in (
        "correlation_key",
        "trace_id",
        "scope",
        "event_id",
        "external_id",
        "session_id",
        "run_id",
        "task_id",
        "actor",
    ):
        if opt_str in payload and not _is_opt_str(payload[opt_str]):
            errors.append(f"{opt_str} must be string or null")

    if "duration_ms" in payload and payload["duration_ms"] is not None:
        if not _is_int(payload["duration_ms"]) or payload["duration_ms"] < 0:
            errors.append("duration_ms must be non-negative integer or null")

    if "detail" in payload and not isinstance(payload["detail"], Mapping):
        errors.append("detail must be an object")

    return errors


def validate_run_record_dict(payload: Mapping[str, Any]) -> list[str]:
    """Validate a dict against schemas/v1/run_record.schema.json. Returns error list."""
    errors: list[str] = []
    extra = set(payload.keys()) - _RUN_ALLOWED_KEYS
    if extra:
        errors.append(f"unexpected fields: {sorted(extra)}")

    if payload.get("schema_version") != "1.0":
        errors.append("schema_version must be '1.0'")
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        errors.append("run_id must match ^run_[A-Za-z0-9_-]+$")

    for req_str in ("inbox_id", "session_id", "executor_id", "runner_id"):
        v = payload.get(req_str)
        if not isinstance(v, str) or not v:
            errors.append(f"{req_str} must be a non-empty string")

    event_ids_raw = payload.get("event_ids")
    if not isinstance(event_ids_raw, list):
        errors.append("event_ids must be a non-empty list of non-empty strings")
    else:
        event_ids_list = cast(list[Any], event_ids_raw)
        if len(event_ids_list) < 1 or not all(isinstance(x, str) and bool(x) for x in event_ids_list):
            errors.append("event_ids must be a non-empty list of non-empty strings")

    for dt_field in ("started_at", "ended_at"):
        if not _is_iso_datetime(payload.get(dt_field)):
            errors.append(f"{dt_field} must be a valid RFC3339 date-time")

    dur = payload.get("duration_ms")
    if not _is_int(dur) or dur < 0:
        errors.append("duration_ms must be a non-negative integer")

    if payload.get("outcome_kind") not in _OUTCOME_KINDS:
        errors.append(f"outcome_kind must be one of {sorted(_OUTCOME_KINDS)}")

    emit_status = payload.get("emit_status")
    if emit_status is not None and emit_status not in _EMIT_STATUSES:
        errors.append(f"emit_status must be one of {sorted(_EMIT_STATUSES)} or null")

    error_kind = payload.get("error_kind")
    if error_kind is not None and error_kind not in _ERROR_KINDS:
        errors.append(f"error_kind must be one of {sorted(_ERROR_KINDS)} or null")

    error_summary = payload.get("error_summary")
    if error_summary is not None:
        if not isinstance(error_summary, str) or len(error_summary) > 500:
            errors.append("error_summary must be string (<=500 chars) or null")

    exit_code = payload.get("exit_code")
    if exit_code is not None and not _is_int(exit_code):
        errors.append("exit_code must be integer or null")

    for opt_str in (
        "correlation_key",
        "trace_id",
        "work_key",
        "parent_session_id",
        "replay_of_run_id",
        "executor_session_id",
        "log_ref",
    ):
        if opt_str in payload and not _is_opt_str(payload[opt_str]):
            errors.append(f"{opt_str} must be string or null")

    usage = payload.get("usage")
    if usage is not None:
        if not isinstance(usage, Mapping):
            errors.append("usage must be an object or null")
        else:
            usage_map = cast(Mapping[str, Any], usage)
            extra_u = set(usage_map.keys()) - _USAGE_ALLOWED_KEYS
            if extra_u:
                errors.append(f"usage unexpected fields: {sorted(extra_u)}")
            if usage_map.get("usage_source") not in _USAGE_SOURCES:
                errors.append(f"usage.usage_source must be one of {sorted(_USAGE_SOURCES)}")
            if "model" in usage_map and not _is_opt_str(usage_map["model"]):
                errors.append("usage.model must be string or null")
            for tok_field in ("input_tokens", "output_tokens", "cache_read_tokens"):
                tv = usage_map.get(tok_field)
                if tv is not None and (not _is_int(tv) or tv < 0):
                    errors.append(f"usage.{tok_field} must be non-negative integer or null")
            cost = usage_map.get("cost_usd")
            if cost is not None and (
                not isinstance(cost, (int, float)) or isinstance(cost, bool) or cost < 0
            ):
                errors.append("usage.cost_usd must be non-negative number or null")

    return errors
