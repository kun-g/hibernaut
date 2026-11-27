"""Pure-stdlib immutable domain models for Observability / Ledger (ADR-0001 v1.0)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, UTC
import secrets
import time
from types import MappingProxyType
from typing import Any, Literal, TypeAlias

AUDIT_SCHEMA_VERSION: Literal["1.0"] = "1.0"
RUN_SCHEMA_VERSION: Literal["1.0"] = "1.0"
MAX_ERROR_SUMMARY_LEN = 500

ServiceName: TypeAlias = Literal[
    "switchboard",
    "taskboard",
    "hibernaut",
    "runner",
    "broker",
    "ledger",
]

Severity: TypeAlias = Literal["info", "warn", "error"]

RunOutcomeKind: TypeAlias = Literal[
    "silent",
    "emit",
    "suspend",
    "interrupted",
    "error",
]

EmitStatus: TypeAlias = Literal["done", "failed", "stopped", "refused"]

ErrorKind: TypeAlias = Literal[
    "rate_limit",
    "network",
    "crash",
    "timeout",
    "config_error",
    "permission_denied",
    "tool_error",
    "bad_output",
    "unknown",
]

UsageSource: TypeAlias = Literal["magpie", "cli", "unknown"]

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode_base32(value: int, length: int) -> str:
    chars = ["0"] * length
    for idx in range(length - 1, -1, -1):
        chars[idx] = _CROCKFORD[value & 31]
        value >>= 5
    return "".join(chars)


def new_ledger_id(prefix: Literal["aud", "run", "trc", "pm"]) -> str:
    """Generate a lexicographically sortable ULID-style identifier with prefix."""
    ts_ms = int(time.time() * 1000)
    rand_bits = secrets.randbits(80)
    return f"{prefix}_{_encode_base32(ts_ms, 10)}{_encode_base32(rand_bits, 16)}"


def utc_now_iso() -> str:
    """Return a fixed-width RFC3339 UTC timestamp with millisecond precision."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def truncate_summary(text: str | None, limit: int = MAX_ERROR_SUMMARY_LEN) -> str | None:
    if text is None:
        return None
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: max(0, limit - 3)] + "..."


@dataclass(frozen=True, slots=True)
class AuditRecord:
    """Lightweight append-only side-channel audit event shared across all services."""

    audit_id: str
    occurred_at: str
    service: ServiceName
    stage: str
    outcome: str
    severity: Severity = "info"
    schema_version: Literal["1.0"] = AUDIT_SCHEMA_VERSION
    correlation_key: str | None = None
    trace_id: str | None = None
    scope: str | None = None
    event_id: str | None = None
    external_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    task_id: str | None = None
    actor: str | None = None
    duration_ms: int | None = None
    detail: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if not isinstance(self.detail, MappingProxyType):
            object.__setattr__(self, "detail", MappingProxyType(dict(self.detail)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "audit_id": self.audit_id,
            "occurred_at": self.occurred_at,
            "service": self.service,
            "stage": self.stage,
            "outcome": self.outcome,
            "severity": self.severity,
            "correlation_key": self.correlation_key,
            "trace_id": self.trace_id,
            "scope": self.scope,
            "event_id": self.event_id,
            "external_id": self.external_id,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "actor": self.actor,
            "duration_ms": self.duration_ms,
            "detail": dict(self.detail),
        }


@dataclass(frozen=True, slots=True)
class RunUsage:
    """Dual-source token and USD cost accounting (Magpie authoritative, CLI fallback)."""

    usage_source: UsageSource = "unknown"
    model: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cache_read_tokens: int | None = None
    cost_usd: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "usage_source": self.usage_source,
            "model": self.model,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_tokens": self.cache_read_tokens,
            "cost_usd": self.cost_usd,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RunUsage:
        source_raw = data.get("usage_source", "unknown")
        source: UsageSource = (
            source_raw if source_raw in ("magpie", "cli", "unknown") else "unknown"
        )
        return cls(
            usage_source=source,
            model=data.get("model") if isinstance(data.get("model"), str) else None,
            input_tokens=(
                int(data["input_tokens"])
                if isinstance(data.get("input_tokens"), int)
                and not isinstance(data.get("input_tokens"), bool)
                else None
            ),
            output_tokens=(
                int(data["output_tokens"])
                if isinstance(data.get("output_tokens"), int)
                and not isinstance(data.get("output_tokens"), bool)
                else None
            ),
            cache_read_tokens=(
                int(data["cache_read_tokens"])
                if isinstance(data.get("cache_read_tokens"), int)
                and not isinstance(data.get("cache_read_tokens"), bool)
                else None
            ),
            cost_usd=(
                float(data["cost_usd"])
                if isinstance(data.get("cost_usd"), (int, float))
                and not isinstance(data.get("cost_usd"), bool)
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class RunRecord:
    """Append-only execution archive for a single Hibernaut + Runner run."""

    run_id: str
    inbox_id: str
    session_id: str
    event_ids: tuple[str, ...]
    executor_id: str
    started_at: str
    ended_at: str
    duration_ms: int
    outcome_kind: RunOutcomeKind
    runner_id: str = "local"
    schema_version: Literal["1.0"] = RUN_SCHEMA_VERSION
    emit_status: EmitStatus | None = None
    correlation_key: str | None = None
    trace_id: str | None = None
    work_key: str | None = None
    parent_session_id: str | None = None
    replay_of_run_id: str | None = None
    executor_session_id: str | None = None
    error_kind: ErrorKind | None = None
    error_summary: str | None = None
    exit_code: int | None = None
    log_ref: str | None = None
    usage: RunUsage | None = None

    def __post_init__(self) -> None:
        if self.error_summary is not None and len(self.error_summary) > MAX_ERROR_SUMMARY_LEN:
            object.__setattr__(self, "error_summary", truncate_summary(self.error_summary))

    @property
    def is_success(self) -> bool:
        """Locked Q1 1.3 success definition."""
        if self.outcome_kind == "silent":
            return True
        if self.outcome_kind == "emit" and self.emit_status == "done":
            return True
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "inbox_id": self.inbox_id,
            "session_id": self.session_id,
            "event_ids": list(self.event_ids),
            "executor_id": self.executor_id,
            "runner_id": self.runner_id,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_ms": self.duration_ms,
            "outcome_kind": self.outcome_kind,
            "emit_status": self.emit_status,
            "correlation_key": self.correlation_key,
            "trace_id": self.trace_id,
            "work_key": self.work_key,
            "parent_session_id": self.parent_session_id,
            "replay_of_run_id": self.replay_of_run_id,
            "executor_session_id": self.executor_session_id,
            "error_kind": self.error_kind,
            "error_summary": self.error_summary,
            "exit_code": self.exit_code,
            "log_ref": self.log_ref,
            "usage": self.usage.to_dict() if self.usage is not None else None,
        }


@dataclass(frozen=True, slots=True)
class ExecutorStats:
    """Aggregated reliability, latency, and cost metrics per executor_id."""

    executor_id: str
    total_runs: int
    success_runs: int
    failed_runs: int
    success_rate: float
    avg_duration_ms: float
    total_input_tokens: int
    total_output_tokens: int
    total_cost_usd: float
    error_breakdown: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class PostMortemCandidate:
    """Structured candidate rule staged in notes/postmortem/ before Memory promotion."""

    candidate_id: str
    inbox_id: str
    source_run_id: str
    error_kind: ErrorKind
    title: str
    rule_text: str
    invalidation_condition: str
    recurrence_count: int = 1
    status: Literal["pending", "approved", "promoted", "rejected"] = "pending"
    created_at: str = field(default_factory=utc_now_iso)
