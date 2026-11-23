"""Append-only SQLite Ledger store on an isolated ledger.db file (Locked Q0 0.2)."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import threading
from typing import Any, cast

from ledger.models import (
    AuditRecord,
    EmitStatus,
    ErrorKind,
    ExecutorStats,
    RunOutcomeKind,
    RunRecord,
    RunUsage,
    ServiceName,
    Severity,
)
from ledger.schema_validate import (
    SchemaValidationError,
    validate_audit_record_dict,
    validate_run_record_dict,
)

_DDL = """
CREATE TABLE IF NOT EXISTS audit_events (
    audit_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    service TEXT NOT NULL,
    stage TEXT NOT NULL,
    outcome TEXT NOT NULL,
    severity TEXT NOT NULL,
    correlation_key TEXT,
    trace_id TEXT,
    scope TEXT,
    event_id TEXT,
    external_id TEXT,
    session_id TEXT,
    run_id TEXT,
    task_id TEXT,
    actor TEXT,
    duration_ms INTEGER,
    detail_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_corr    ON audit_events(correlation_key, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_trace   ON audit_events(trace_id, occurred_at ASC);
CREATE INDEX IF NOT EXISTS idx_audit_svc     ON audit_events(service, stage, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_sev     ON audit_events(severity, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_run     ON audit_events(run_id, occurred_at ASC);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    inbox_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    executor_id TEXT NOT NULL,
    runner_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT NOT NULL,
    duration_ms INTEGER NOT NULL,
    outcome_kind TEXT NOT NULL,
    emit_status TEXT,
    correlation_key TEXT,
    trace_id TEXT,
    work_key TEXT,
    parent_session_id TEXT,
    replay_of_run_id TEXT,
    executor_session_id TEXT,
    error_kind TEXT,
    error_summary TEXT,
    exit_code INTEGER,
    log_ref TEXT,
    usage_json TEXT
);

CREATE TABLE IF NOT EXISTS run_events (
    run_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    seq_order INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, event_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_runs_inbox    ON runs(inbox_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_corr     ON runs(correlation_key, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_trace    ON runs(trace_id, started_at ASC);
CREATE INDEX IF NOT EXISTS idx_runs_session  ON runs(session_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_exec     ON runs(executor_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_err_kind ON runs(error_kind, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_run_events_ev ON run_events(event_id);
"""


class SQLiteLedgerStore:
    """Thread-safe, append-only SQLite store backed by a dedicated ledger.db file."""

    def __init__(self, db_path: str | Path, *, validate_schema: bool = True) -> None:
        self._db_path = str(db_path)
        self._validate_schema = validate_schema
        self._lock = threading.RLock()
        if self._db_path != ":memory:":
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            if self._db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(_DDL)

    @property
    def db_path(self) -> str:
        return self._db_path

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _write_tx(self) -> Generator[sqlite3.Connection, None, None]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # ------------------------------------------------------------------
    # Layer 1: AuditRecord (Append-Only)
    # ------------------------------------------------------------------
    def emit_audit_record(self, record: AuditRecord) -> None:
        if self._validate_schema:
            errs = validate_audit_record_dict(record.to_dict())
            if errs:
                raise SchemaValidationError(errs)
        detail_json = json.dumps(dict(record.detail), ensure_ascii=False, sort_keys=True)
        with self._write_tx() as conn:
            conn.execute(
                """
                INSERT INTO audit_events (
                    audit_id, schema_version, occurred_at, service, stage, outcome,
                    severity, correlation_key, trace_id, scope, event_id, external_id,
                    session_id, run_id, task_id, actor, duration_ms, detail_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.audit_id,
                    record.schema_version,
                    record.occurred_at,
                    record.service,
                    record.stage,
                    record.outcome,
                    record.severity,
                    record.correlation_key,
                    record.trace_id,
                    record.scope,
                    record.event_id,
                    record.external_id,
                    record.session_id,
                    record.run_id,
                    record.task_id,
                    record.actor,
                    record.duration_ms,
                    detail_json,
                ),
            )

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
        clauses: list[str] = []
        params: list[Any] = []
        if correlation_key is not None:
            clauses.append("correlation_key = ?")
            params.append(correlation_key)
        if trace_id is not None:
            clauses.append("trace_id = ?")
            params.append(trace_id)
        if scope is not None:
            clauses.append("scope = ?")
            params.append(scope)
        if service is not None:
            clauses.append("service = ?")
            params.append(service)
        if stage is not None:
            clauses.append("stage = ?")
            params.append(stage)
        if severity is not None:
            clauses.append("severity = ?")
            params.append(severity)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM audit_events {where} ORDER BY occurred_at DESC, audit_id DESC LIMIT ?"
        params.append(max(1, min(limit, 500)))
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_audit(r) for r in rows]

    def prune_audits_before(self, cutoff_iso: str) -> int:
        """Retention helper: delete expired audit_events older than cutoff_iso."""
        with self._write_tx() as conn:
            cur = conn.execute("DELETE FROM audit_events WHERE occurred_at < ?", (cutoff_iso,))
            return cur.rowcount

    # ------------------------------------------------------------------
    # Layer 2: RunRecord + run_events (Append-Only)
    # ------------------------------------------------------------------
    def append_run(self, record: RunRecord) -> None:
        if self._validate_schema:
            errs = validate_run_record_dict(record.to_dict())
            if errs:
                raise SchemaValidationError(errs)
        usage_json = (
            json.dumps(record.usage.to_dict(), ensure_ascii=False, sort_keys=True)
            if record.usage is not None
            else None
        )
        with self._write_tx() as conn:
            conn.execute(
                """
                INSERT INTO runs (
                    run_id, schema_version, inbox_id, session_id, executor_id, runner_id,
                    started_at, ended_at, duration_ms, outcome_kind, emit_status,
                    correlation_key, trace_id, work_key, parent_session_id,
                    replay_of_run_id, executor_session_id, error_kind, error_summary,
                    exit_code, log_ref, usage_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.run_id,
                    record.schema_version,
                    record.inbox_id,
                    record.session_id,
                    record.executor_id,
                    record.runner_id,
                    record.started_at,
                    record.ended_at,
                    record.duration_ms,
                    record.outcome_kind,
                    record.emit_status,
                    record.correlation_key,
                    record.trace_id,
                    record.work_key,
                    record.parent_session_id,
                    record.replay_of_run_id,
                    record.executor_session_id,
                    record.error_kind,
                    record.error_summary,
                    record.exit_code,
                    record.log_ref,
                    usage_json,
                ),
            )
            conn.executemany(
                "INSERT INTO run_events (run_id, event_id, seq_order) VALUES (?, ?, ?)",
                [(record.run_id, ev_id, idx) for idx, ev_id in enumerate(record.event_ids)],
            )

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            ev_rows = self._conn.execute(
                "SELECT event_id FROM run_events WHERE run_id = ? ORDER BY seq_order ASC",
                (run_id,),
            ).fetchall()
        return self._row_to_run(row, tuple(str(r["event_id"]) for r in ev_rows))

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
        clauses: list[str] = []
        params: list[Any] = []
        join = ""
        if event_id is not None:
            join = "JOIN run_events re ON re.run_id = r.run_id"
            clauses.append("re.event_id = ?")
            params.append(event_id)
        if inbox_id is not None:
            clauses.append("r.inbox_id = ?")
            params.append(inbox_id)
        if correlation_key is not None:
            clauses.append("r.correlation_key = ?")
            params.append(correlation_key)
        if trace_id is not None:
            clauses.append("r.trace_id = ?")
            params.append(trace_id)
        if session_id is not None:
            clauses.append("r.session_id = ?")
            params.append(session_id)
        if executor_id is not None:
            clauses.append("r.executor_id = ?")
            params.append(executor_id)
        if error_kind is not None:
            clauses.append("r.error_kind = ?")
            params.append(error_kind)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = (
            f"SELECT r.* FROM runs r {join} {where} "
            "ORDER BY r.started_at DESC, r.run_id DESC LIMIT ?"
        )
        params.append(max(1, min(limit, 500)))
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
            if not rows:
                return []
            run_ids = [str(r["run_id"]) for r in rows]
            placeholders = ",".join("?" for _ in run_ids)
            ev_rows = self._conn.execute(
                f"SELECT run_id, event_id FROM run_events WHERE run_id IN ({placeholders}) "
                "ORDER BY run_id, seq_order ASC",
                run_ids,
            ).fetchall()

        ev_map: dict[str, list[str]] = {rid: [] for rid in run_ids}
        for er in ev_rows:
            ev_map[str(er["run_id"])].append(str(er["event_id"]))
        return [self._row_to_run(r, tuple(ev_map[str(r["run_id"])])) for r in rows]

    def summarize_by_executor(
        self,
        *,
        inbox_id: str | None = None,
        since_iso: str | None = None,
    ) -> list[ExecutorStats]:
        clauses: list[str] = []
        params: list[Any] = []
        if inbox_id is not None:
            clauses.append("inbox_id = ?")
            params.append(inbox_id)
        if since_iso is not None:
            clauses.append("started_at >= ?")
            params.append(since_iso)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT executor_id, outcome_kind, emit_status, duration_ms, error_kind, usage_json "
                f"FROM runs {where}",
                params,
            ).fetchall()

        buckets: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            buckets.setdefault(str(row["executor_id"]), []).append(row)

        stats: list[ExecutorStats] = []
        for exec_id in sorted(buckets.keys()):
            items = buckets[exec_id]
            total = len(items)
            success = 0
            total_dur = 0
            in_tok = 0
            out_tok = 0
            cost = 0.0
            err_counts: dict[str, int] = {}
            for r in items:
                ok = (r["outcome_kind"] == "silent") or (
                    r["outcome_kind"] == "emit" and r["emit_status"] == "done"
                )
                if ok:
                    success += 1
                total_dur += int(r["duration_ms"])
                ek = r["error_kind"]
                if ek is not None:
                    err_counts[str(ek)] = err_counts.get(str(ek), 0) + 1
                if r["usage_json"]:
                    u = json.loads(str(r["usage_json"]))
                    if isinstance(u.get("input_tokens"), int):
                        in_tok += u["input_tokens"]
                    if isinstance(u.get("output_tokens"), int):
                        out_tok += u["output_tokens"]
                    if isinstance(u.get("cost_usd"), (int, float)):
                        cost += float(u["cost_usd"])
            stats.append(
                ExecutorStats(
                    executor_id=exec_id,
                    total_runs=total,
                    success_runs=success,
                    failed_runs=total - success,
                    success_rate=round(success / total, 4) if total else 0.0,
                    avg_duration_ms=round(total_dur / total, 2) if total else 0.0,
                    total_input_tokens=in_tok,
                    total_output_tokens=out_tok,
                    total_cost_usd=round(cost, 6),
                    error_breakdown=err_counts,
                )
            )
        return stats

    # ------------------------------------------------------------------
    # Row mappers
    # ------------------------------------------------------------------
    @staticmethod
    def _row_to_audit(row: sqlite3.Row) -> AuditRecord:
        detail: dict[str, Any] = json.loads(str(row["detail_json"])) if row["detail_json"] else {}
        return AuditRecord(
            audit_id=str(row["audit_id"]),
            schema_version="1.0",
            occurred_at=str(row["occurred_at"]),
            service=cast(ServiceName, str(row["service"])),
            stage=str(row["stage"]),
            outcome=str(row["outcome"]),
            severity=cast(Severity, str(row["severity"])),
            correlation_key=row["correlation_key"],
            trace_id=row["trace_id"],
            scope=row["scope"],
            event_id=row["event_id"],
            external_id=row["external_id"],
            session_id=row["session_id"],
            run_id=row["run_id"],
            task_id=row["task_id"],
            actor=row["actor"],
            duration_ms=row["duration_ms"],
            detail=detail,
        )

    @staticmethod
    def _row_to_run(row: sqlite3.Row, event_ids: tuple[str, ...]) -> RunRecord:
        usage = (
            RunUsage.from_dict(json.loads(str(row["usage_json"])))
            if row["usage_json"]
            else None
        )
        return RunRecord(
            run_id=str(row["run_id"]),
            schema_version="1.0",
            inbox_id=str(row["inbox_id"]),
            session_id=str(row["session_id"]),
            event_ids=event_ids,
            executor_id=str(row["executor_id"]),
            runner_id=str(row["runner_id"]),
            started_at=str(row["started_at"]),
            ended_at=str(row["ended_at"]),
            duration_ms=int(row["duration_ms"]),
            outcome_kind=cast(RunOutcomeKind, str(row["outcome_kind"])),
            emit_status=cast(EmitStatus, row["emit_status"]) if row["emit_status"] else None,
            correlation_key=row["correlation_key"],
            trace_id=row["trace_id"],
            work_key=row["work_key"],
            parent_session_id=row["parent_session_id"],
            replay_of_run_id=row["replay_of_run_id"],
            executor_session_id=row["executor_session_id"],
            error_kind=cast(ErrorKind, row["error_kind"]) if row["error_kind"] else None,
            error_summary=row["error_summary"],
            exit_code=row["exit_code"],
            log_ref=row["log_ref"],
            usage=usage,
        )
