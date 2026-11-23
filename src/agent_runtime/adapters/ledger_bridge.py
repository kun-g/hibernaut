from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast, get_args

from agent_runtime.core.models import Event, FailureRoute, RunLedgerEntry, RunTelemetry
from ledger import (
    AuditRecord,
    CompositeLogReader,
    ErrorKind,
    LocalLogStore,
    MCPReadToolHandlers,
    RunRecord,
    RunStorePort,
    Severity,
    SQLiteLedgerStore,
    SwitchboardLedgerAuditAdapter,
    classify_error,
    extract_usage,
    new_ledger_id,
    route_error,
    safe_append_run,
    safe_emit_audit,
    utc_now_iso,
)

_ERROR_KINDS: frozenset[str] = frozenset(get_args(ErrorKind))
_SEVERITIES: frozenset[str] = frozenset(get_args(Severity))


def _log_text(telemetry: RunTelemetry) -> str:
    exit_line = f"=== exit code: {telemetry.exit_code} ===\n" if telemetry.exit_code is not None else ""
    runner_line = f"=== runner log ===\n{telemetry.runner_log}\n" if telemetry.runner_log is not None else ""
    return f"=== stdout ===\n{telemetry.stdout}\n=== stderr ===\n{telemetry.stderr}\n{exit_line}{runner_line}"


class LedgerRunRecorder:
    # RunLedgerPort on the Observability Ledger: ledger.classify_error and ledger.route_error pick the route, and each
    # finished Run is appended to ledger.db, its executor output written to a local:// log beside it.
    def __init__(
        self,
        store: RunStorePort,
        logs: LocalLogStore | None = None,
        *,
        retry_delays: tuple[float, ...] | None = None,
    ) -> None:
        self._store = store
        self._logs = logs
        # Replaces the transient lane's 5/15/45 s backoff (tests); deterministic and execution lanes never retry.
        self._retry_delays = retry_delays

    def route_failure(
        self,
        executor_id: str,
        *,
        error: BaseException | None = None,
        telemetry: RunTelemetry | None = None,
    ) -> FailureRoute:
        kind: ErrorKind
        if error is None and telemetry is not None and telemetry.error_kind in _ERROR_KINDS:
            kind = cast(ErrorKind, telemetry.error_kind)
        else:
            kind = classify_error(
                exc=error,
                executor_id=executor_id,
                exit_code=telemetry.exit_code if telemetry is not None else None,
                stderr_text=telemetry.stderr if telemetry is not None else None,
            )
        policy = route_error(kind)
        delays: tuple[float, ...] = ()
        if policy.should_retry:
            delays = self._retry_delays if self._retry_delays is not None else tuple(map(float, policy.backoff_seconds))
        return FailureRoute(error_kind=kind, category=policy.category, retry_delays=delays)

    def record_run(self, entry: RunLedgerEntry) -> None:
        log_ref: str | None = None
        telemetry = entry.telemetry
        if telemetry is not None and self._logs is not None and (
            telemetry.stdout or telemetry.stderr or telemetry.runner_log
        ):
            try:
                log_ref = self._logs.write_log(entry.run_id, _log_text(telemetry))
            except (OSError, ValueError):
                logging.getLogger(__name__).warning("Run log of %s was not written", entry.run_id, exc_info=True)
        usage = extract_usage(cli_stdout=telemetry.stdout) if telemetry is not None and telemetry.stdout else None
        error_kind = entry.error_kind
        if error_kind is not None and error_kind not in _ERROR_KINDS:
            error_kind = "unknown"
        safe_append_run(
            self._store,
            RunRecord(
                run_id=entry.run_id,
                inbox_id=entry.inbox_id,
                session_id=entry.session_id,
                event_ids=entry.event_ids,
                executor_id=entry.executor_id,
                started_at=entry.started_at,
                ended_at=entry.ended_at,
                duration_ms=entry.duration_ms,
                outcome_kind=entry.outcome_kind,
                runner_id=(telemetry.runner_id if telemetry is not None else None) or "local",
                emit_status=entry.emit_status,
                correlation_key=entry.correlation_key,
                trace_id=entry.trace_id,
                work_key=entry.work_key,
                parent_session_id=entry.parent_session_id,
                replay_of_run_id=entry.replay_of_run_id,
                executor_session_id=entry.executor_session_id,
                error_kind=cast(ErrorKind | None, error_kind),
                error_summary=entry.error_summary,
                exit_code=telemetry.exit_code if telemetry is not None else None,
                log_ref=log_ref,
                usage=usage,
            ),
        )


class MemoryLedgerAudit:
    # Memory Hub's AuditHookPort on the ledger.
    def __init__(self, store: SQLiteLedgerStore) -> None:
        self._store = store

    def emit(self, stage: str, outcome: str, detail: dict[str, Any], *, severity: str = "info") -> None:
        project_key = detail.get("project_key")
        safe_emit_audit(
            self._store,
            AuditRecord(
                audit_id=new_ledger_id("aud"),
                occurred_at=utc_now_iso(),
                service="hibernaut",
                stage=stage,
                outcome=outcome,
                severity=cast(Severity, severity) if severity in _SEVERITIES else "info",
                scope=str(project_key) if project_key is not None else None,
                detail=detail,
            ),
        )


class RunLedger:
    # The process's run ledger: ledger.db (its own SQLite file, so it never contends with the runtime database's
    # write lock), run logs under <ledger dir>/logs/runs, and the adapters the composition root hands out.
    def __init__(self, db_path: str | Path, *, retry_delays: tuple[float, ...] | None = None) -> None:
        path = Path(db_path).expanduser()
        self.db_path = path
        self.store = SQLiteLedgerStore(path)
        self.logs = LocalLogStore(path.parent)
        try:
            self.logs.cleanup()
        except OSError:
            logging.getLogger(__name__).warning("Run log cleanup failed under %s", self.logs.root_dir, exc_info=True)
        self.recorder = LedgerRunRecorder(self.store, self.logs, retry_delays=retry_delays)
        self.read_tools = MCPReadToolHandlers(self.store, self.store, CompositeLogReader(self.logs))

    def switchboard_audit(self) -> SwitchboardLedgerAuditAdapter:
        return SwitchboardLedgerAuditAdapter(self.store)

    def memory_audit(self) -> MemoryLedgerAudit:
        return MemoryLedgerAudit(self.store)

    def latest_failed_run_id(self, event_id: str) -> str | None:
        for run in self.store.list_runs(event_id=event_id):
            if not run.is_success:
                return run.run_id
        return None

    def record_replay(
        self,
        event: Event,
        *,
        replay_of_run_id: str | None,
        executor_id_override: str | None,
        force: bool,
        actor: str,
    ) -> None:
        safe_emit_audit(
            self.store,
            AuditRecord(
                audit_id=new_ledger_id("aud"),
                occurred_at=utc_now_iso(),
                service="hibernaut",
                stage="inbox.replay",
                outcome="queued",
                correlation_key=event.correlation_key,
                scope=event.inbox_id,
                event_id=event.event_id,
                external_id=event.external_id,
                session_id=event.session_id,
                run_id=replay_of_run_id,
                actor=actor,
                detail={"executor_id": executor_id_override, "force": force},
            ),
        )

    def close(self) -> None:
        self.store.close()
