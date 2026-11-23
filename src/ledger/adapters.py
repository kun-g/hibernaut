"""Ready-to-wire Adapters for Switchboard AuditSinkPort & MCP Read Tools (Locked Q0/Q1)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from ledger.log_store import CompositeLogReader
from ledger.models import (
    AuditRecord,
    ErrorKind,
    ServiceName,
    Severity,
    new_ledger_id,
    utc_now_iso,
)
from ledger.ports import LedgerAuditSinkPort, RunStorePort, safe_emit_audit

_SENSITIVE_KEY_FRAGMENTS = (
    "token",
    "secret",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "private_key",
)


def redact_detail(detail: Mapping[str, Any] | None) -> dict[str, Any]:
    """Recursively scrub sensitive keys so no plaintext credential ever lands in ledger.db."""
    if not detail:
        return {}
    cleaned: dict[str, Any] = {}
    for key, value in detail.items():
        key_l = str(key).lower()
        if any(frag in key_l for frag in _SENSITIVE_KEY_FRAGMENTS):
            cleaned[str(key)] = "[REDACTED]"
        elif isinstance(value, Mapping):
            cleaned[str(key)] = redact_detail(cast(Mapping[str, Any], value))
        else:
            cleaned[str(key)] = value
    return cleaned


class SwitchboardLedgerAuditAdapter:
    """Drop-in adapter matching switchboard.ports.AuditSinkPort (Locked Q1 1.1).

    Accepts Switchboard's existing keyword signature plus optional correlation_key,
    scope, trace_id, and severity, redacts secrets, and writes Fail-Open to Ledger.
    """

    def __init__(
        self,
        sink: LedgerAuditSinkPort | None,
        *,
        service: ServiceName = "switchboard",
    ) -> None:
        self._sink = sink
        self._service: ServiceName = service

    def emit_audit(
        self,
        stage: str,
        outcome: str,
        *,
        source: str | None = None,
        event_id: str | None = None,
        external_id: str | None = None,
        consumer_id: str | None = None,
        detail: Mapping[str, Any] | None = None,
        correlation_key: str | None = None,
        scope: str | None = None,
        trace_id: str | None = None,
        severity: Severity | None = None,
        duration_ms: int | None = None,
    ) -> None:
        merged_detail = redact_detail(detail)
        if source is not None and "source" not in merged_detail:
            merged_detail["source"] = source
        inferred_sev: Severity = severity or _infer_severity(outcome)
        rec = AuditRecord(
            audit_id=new_ledger_id("aud"),
            occurred_at=utc_now_iso(),
            service=self._service,
            stage=stage,
            outcome=outcome,
            severity=inferred_sev,
            correlation_key=correlation_key,
            trace_id=trace_id,
            scope=scope,
            event_id=event_id,
            external_id=external_id,
            actor=consumer_id,
            duration_ms=duration_ms,
            detail=merged_detail,
        )
        safe_emit_audit(self._sink, rec)


def _infer_severity(outcome: str) -> Severity:
    low = outcome.lower()
    if any(w in low for w in ("fail", "error", "dead_letter", "invalid")):
        return "error"
    if any(w in low for w in ("reject", "timeout", "stale", "limit", "block", "drop")):
        return "warn"
    return "info"


class MCPReadToolHandlers:
    """Pure read-only MCP tool handlers for Observability / Ledger (Locked Q0 0.1).

    Note: Mutating `hibernaut_replay_event` stays in Hibernaut control plane, not here.
    """

    def __init__(
        self,
        run_store: RunStorePort,
        audit_sink: LedgerAuditSinkPort,
        log_reader: CompositeLogReader,
    ) -> None:
        self._runs = run_store
        self._audits = audit_sink
        self._logs = log_reader

    def hibernaut_list_runs(
        self,
        *,
        inbox_id: str | None = None,
        correlation_key: str | None = None,
        trace_id: str | None = None,
        session_id: str | None = None,
        event_id: str | None = None,
        executor_id: str | None = None,
        error_kind: ErrorKind | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        records = self._runs.list_runs(
            inbox_id=inbox_id,
            correlation_key=correlation_key,
            trace_id=trace_id,
            session_id=session_id,
            event_id=event_id,
            executor_id=executor_id,
            error_kind=error_kind,
            limit=limit,
        )
        return {"count": len(records), "runs": [r.to_dict() for r in records]}

    def hibernaut_get_run_log(
        self,
        *,
        run_id: str,
        tail_lines: int = 120,
        max_bytes: int = 65536,
    ) -> dict[str, Any]:
        run = self._runs.get_run(run_id)
        if run is None:
            return {"found": False, "run_id": run_id, "error": "run not found"}
        tail = self._logs.read_tail(
            run.log_ref,
            tail_lines=tail_lines,
            max_bytes=max_bytes,
            fallback_summary=run.error_summary,
        )
        return {
            "found": True,
            "run_id": run.run_id,
            "log_ref": tail.log_ref,
            "source": tail.source,
            "truncated": tail.truncated,
            "warning": tail.warning,
            "content": tail.content,
        }

    def hibernaut_list_audits(
        self,
        *,
        correlation_key: str | None = None,
        trace_id: str | None = None,
        scope: str | None = None,
        service: ServiceName | None = None,
        stage: str | None = None,
        severity: Severity | None = None,
        run_id: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        records = self._audits.list_audits(
            correlation_key=correlation_key,
            trace_id=trace_id,
            scope=scope,
            service=service,
            stage=stage,
            severity=severity,
            run_id=run_id,
            limit=limit,
        )
        return {"count": len(records), "audits": [r.to_dict() for r in records]}

    def hibernaut_executor_stats(
        self,
        *,
        inbox_id: str | None = None,
        since_iso: str | None = None,
    ) -> dict[str, Any]:
        stats = self._runs.summarize_by_executor(inbox_id=inbox_id, since_iso=since_iso)
        return {
            "executors": [
                {
                    "executor_id": s.executor_id,
                    "total_runs": s.total_runs,
                    "success_runs": s.success_runs,
                    "failed_runs": s.failed_runs,
                    "success_rate": s.success_rate,
                    "avg_duration_ms": s.avg_duration_ms,
                    "total_input_tokens": s.total_input_tokens,
                    "total_output_tokens": s.total_output_tokens,
                    "total_cost_usd": s.total_cost_usd,
                    "error_breakdown": dict(s.error_breakdown),
                }
                for s in stats
            ]
        }
