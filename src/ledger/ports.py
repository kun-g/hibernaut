"""Minimal Port contracts and Fail-Open side-channel helpers for Observability / Ledger."""

from __future__ import annotations

import sys
from typing import Protocol

from ledger.models import (
    AuditRecord,
    ErrorKind,
    ExecutorStats,
    RunRecord,
    ServiceName,
    Severity,
)


class LedgerAuditSinkPort(Protocol):
    def emit_audit_record(self, record: AuditRecord) -> None: ...

    def list_audits(
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
    ) -> list[AuditRecord]: ...


class RunStorePort(Protocol):
    def append_run(self, record: RunRecord) -> None: ...

    def get_run(self, run_id: str) -> RunRecord | None: ...

    def list_runs(
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
    ) -> list[RunRecord]: ...

    def summarize_by_executor(
        self,
        *,
        inbox_id: str | None = None,
        since_iso: str | None = None,
    ) -> list[ExecutorStats]: ...


class LogStorePort(Protocol):
    def write_log(self, run_id: str, content: str, *, date_str: str | None = None) -> str: ...

    def read_tail(
        self,
        log_ref: str,
        *,
        tail_lines: int = 120,
        max_bytes: int = 65536,
    ) -> str: ...


class RemoteLogFetcherPort(Protocol):
    def fetch_tail(
        self,
        runner_id: str,
        rel_path: str,
        *,
        tail_lines: int = 120,
        max_bytes: int = 65536,
    ) -> str: ...


class NoopAuditSink:
    """Drop-in no-op sink for tests or minimal deployments."""

    def emit_audit_record(self, record: AuditRecord) -> None:
        return None

    def list_audits(
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
    ) -> list[AuditRecord]:
        return []


class NoopRunStore:
    """Drop-in no-op run store for tests or minimal deployments."""

    def append_run(self, record: RunRecord) -> None:
        return None

    def get_run(self, run_id: str) -> RunRecord | None:
        return None

    def list_runs(
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
    ) -> list[RunRecord]:
        return []

    def summarize_by_executor(
        self,
        *,
        inbox_id: str | None = None,
        since_iso: str | None = None,
    ) -> list[ExecutorStats]:
        return []


def safe_emit_audit(sink: LedgerAuditSinkPort | None, record: AuditRecord) -> bool:
    """Fail-Open wrapper: never raises into the caller's main path."""
    if sink is None:
        return False
    try:
        sink.emit_audit_record(record)
        return True
    except Exception as exc:  # noqa: BLE001
        print(
            f"[ledger] audit write failed ({record.service}/{record.stage}/{record.outcome}): {exc!r}",
            file=sys.stderr,
        )
        return False


def safe_append_run(store: RunStorePort | None, record: RunRecord) -> bool:
    """Fail-Open wrapper: never raises into the Worker's finally block."""
    if store is None:
        return False
    try:
        store.append_run(record)
        return True
    except Exception as exc:  # noqa: BLE001
        print(
            f"[ledger] run append failed ({record.run_id}): {exc!r}",
            file=sys.stderr,
        )
        return False
