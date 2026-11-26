"""Comprehensive test suite for Observability / Ledger (ADR-0001 v1.0, Q0-Q5)."""

from __future__ import annotations

import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import pytest

from ledger import (
    AuditRecord,
    CompositeLogReader,
    LocalLogStore,
    MCPReadToolHandlers,
    MemoryGateError,
    RunRecord,
    RunUsage,
    SQLiteLedgerStore,
    SwitchboardLedgerAuditAdapter,
    classify_error,
    create_postmortem_candidate,
    extract_usage,
    magpie_trace_headers,
    new_ledger_id,
    promote_to_instructions,
    route_error,
    safe_append_run,
    safe_emit_audit,
    utc_now_iso,
    validate_audit_record_dict,
    validate_run_record_dict,
)

ROOT = Path(__file__).resolve().parents[2]
SRC_LEDGER = ROOT / "src" / "ledger"
SCHEMAS_DIR = ROOT / "schemas" / "v1"


# ---------------------------------------------------------------------------
# 1. Zero-business-deps & stdlib-only AST boundary check (Q5 5.1)
# ---------------------------------------------------------------------------
def test_ledger_package_uses_stdlib_and_self_only() -> None:
    forbidden_roots = {"agent_runtime", "switchboard", "taskboard", "runner", "memory_hub"}
    stdlib_names = set(sys.stdlib_module_names)
    for py_file in sorted(SRC_LEDGER.glob("*.py")):
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    assert top not in forbidden_roots, f"{py_file} imports forbidden {top}"
                    assert top == "ledger" or top in stdlib_names, f"{py_file} imports non-stdlib {top}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                top = node.module.split(".")[0]
                assert top not in forbidden_roots, f"{py_file} imports forbidden {top}"
                assert top == "ledger" or top in stdlib_names, f"{py_file} imports non-stdlib {top}"


# ---------------------------------------------------------------------------
# 2. ADR-0001 JSON Schema v1.0 bidirectional contract tests (Q1)
# ---------------------------------------------------------------------------
def test_schema_files_exist_and_match_record_keys() -> None:
    audit_schema = json.loads((SCHEMAS_DIR / "audit_record.schema.json").read_text(encoding="utf-8"))
    run_schema = json.loads((SCHEMAS_DIR / "run_record.schema.json").read_text(encoding="utf-8"))
    assert audit_schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert run_schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert audit_schema["additionalProperties"] is False
    assert run_schema["additionalProperties"] is False

    audit = AuditRecord(
        audit_id=new_ledger_id("aud"),
        occurred_at=utc_now_iso(),
        service="switchboard",
        stage="inbound.github",
        outcome="accepted",
        correlation_key="github:kun-g/hibernaut#42",
        scope="proj:hibernaut",
    )
    assert set(audit.to_dict().keys()) == set(audit_schema["properties"].keys())
    assert validate_audit_record_dict(audit.to_dict()) == []

    run = RunRecord(
        run_id=new_ledger_id("run"),
        inbox_id="proj:hibernaut",
        session_id="sess_01",
        event_ids=("ev_1", "ev_2"),
        executor_id="sde",
        runner_id="local",
        started_at=utc_now_iso(),
        ended_at=utc_now_iso(),
        duration_ms=1250,
        outcome_kind="emit",
        emit_status="done",
        correlation_key="github:kun-g/hibernaut#42",
        usage=RunUsage(usage_source="magpie", model="claude-opus-5-5", input_tokens=100, output_tokens=50, cost_usd=0.01),
    )
    assert set(run.to_dict().keys()) == set(run_schema["properties"].keys())
    assert validate_run_record_dict(run.to_dict()) == []


def test_schema_validator_rejects_invalid_payloads() -> None:
    bad_audit = {"schema_version": "2.0", "audit_id": "bad", "extra_field": 1}
    errs = validate_audit_record_dict(bad_audit)
    assert any("unexpected fields" in e for e in errs)
    assert any("schema_version" in e for e in errs)

    bad_run: dict[str, object] = {"schema_version": "1.0", "run_id": "run_1", "event_ids": [], "duration_ms": -5}
    r_errs = validate_run_record_dict(bad_run)
    assert any("event_ids" in e for e in r_errs)
    assert any("duration_ms" in e for e in r_errs)


# ---------------------------------------------------------------------------
# 3. Independent ledger.db SQLite store, M:N run_events & Fail-Open (Q0/Q1)
# ---------------------------------------------------------------------------
def test_sqlite_ledger_append_only_mn_and_stats(tmp_path: Path) -> None:
    store = SQLiteLedgerStore(tmp_path / "ledger.db")
    try:
        # Audit events
        a1 = AuditRecord(
            audit_id=new_ledger_id("aud"),
            occurred_at="2026-11-22T10:00:00.000Z",
            service="switchboard",
            stage="inbound.github",
            outcome="accepted",
            correlation_key="github:kun-g/hibernaut#42",
            trace_id="trc_01",
            scope="proj:hibernaut",
            event_id="ev_1",
        )
        store.emit_audit_record(a1)
        assert len(store.list_audits(correlation_key="github:kun-g/hibernaut#42")) == 1
        assert len(store.list_audits(trace_id="trc_01")) == 1

        # M:N runs: ev_1 fails on grok (emit+failed), then replays on claude (emit+done)
        r1 = RunRecord(
            run_id="run_01",
            inbox_id="proj:hibernaut",
            session_id="sess_1",
            event_ids=("ev_1",),
            executor_id="grok",
            started_at="2026-11-22T10:01:00.000Z",
            ended_at="2026-11-22T10:01:04.000Z",
            duration_ms=4000,
            outcome_kind="emit",
            emit_status="failed",
            correlation_key="github:kun-g/hibernaut#42",
            error_kind="bad_output",
            error_summary="JSONDecodeError",
            exit_code=1,
            log_ref="local://2026-11-22/run_01.log",
            usage=RunUsage(usage_source="cli", input_tokens=500, output_tokens=100, cost_usd=0.01),
        )
        r2 = RunRecord(
            run_id="run_02",
            inbox_id="proj:hibernaut",
            session_id="sess_1",
            event_ids=("ev_1", "ev_2"),
            executor_id="claude",
            started_at="2026-11-22T10:05:00.000Z",
            ended_at="2026-11-22T10:05:18.000Z",
            duration_ms=18000,
            outcome_kind="emit",
            emit_status="done",
            correlation_key="github:kun-g/hibernaut#42",
            replay_of_run_id="run_01",
            log_ref="local://2026-11-22/run_02.log",
            usage=RunUsage(usage_source="magpie", input_tokens=2000, output_tokens=600, cost_usd=0.12),
        )
        store.append_run(r1)
        store.append_run(r2)

        assert not r1.is_success
        assert r2.is_success

        # Query by event_id returns both runs (append-only preserved)
        ev1_runs = store.list_runs(event_id="ev_1")
        assert [r.run_id for r in ev1_runs] == ["run_02", "run_01"]
        assert store.get_run("run_02") is not None
        assert store.get_run("run_02").event_ids == ("ev_1", "ev_2")  # type: ignore[union-attr]

        # Stats check (Locked Q1 1.3 success definition)
        stats = {s.executor_id: s for s in store.summarize_by_executor()}
        assert stats["grok"].success_rate == 0.0
        assert stats["grok"].error_breakdown == {"bad_output": 1}
        assert stats["claude"].success_rate == 1.0
        assert stats["claude"].total_cost_usd == pytest.approx(0.12)
    finally:
        store.close()


def test_fail_open_wrappers_never_raise(capsys: pytest.CaptureFixture[str]) -> None:
    class BrokenStore:
        def emit_audit_record(self, record: AuditRecord) -> None:
            raise OSError("disk full")

        def list_audits(self, **kwargs: object) -> list[AuditRecord]:
            return []

        def append_run(self, record: RunRecord) -> None:
            raise RuntimeError("sqlite locked")

        def get_run(self, run_id: str) -> RunRecord | None:
            return None

        def list_runs(self, **kwargs: object) -> list[RunRecord]:
            return []

        def summarize_by_executor(self, **kwargs: object) -> list[object]:
            return []

    broken = BrokenStore()
    a = AuditRecord(audit_id="aud_1", occurred_at=utc_now_iso(), service="hibernaut", stage="s", outcome="o")
    r = RunRecord(
        run_id="run_1",
        inbox_id="i",
        session_id="s",
        event_ids=("e1",),
        executor_id="claude",
        started_at=utc_now_iso(),
        ended_at=utc_now_iso(),
        duration_ms=10,
        outcome_kind="silent",
    )
    assert safe_emit_audit(broken, a) is False  # type: ignore[arg-type]
    assert safe_append_run(broken, r) is False  # type: ignore[arg-type]
    err = capsys.readouterr().err
    assert "[ledger] audit write failed" in err
    assert "[ledger] run append failed" in err


# ---------------------------------------------------------------------------
# 4. 3-Tier ErrorKind classifier & 3-lane policy (Q2)
# ---------------------------------------------------------------------------
def test_error_classifier_three_tiers_and_policy() -> None:
    # Tier 1: internal exception
    assert classify_error(exc=TimeoutError("watchdog")) == "timeout"
    assert classify_error(exc=PermissionError("denied")) == "permission_denied"

    # Tier 2: structured HTTP status / code from Magpie
    assert classify_error(http_status=429) == "rate_limit"
    assert classify_error(http_status=401) == "config_error"
    assert classify_error(structured_code="overloaded_error") == "rate_limit"

    # Tier 3: executor-specific stderr tail
    assert classify_error(executor_id="claude", stderr_text="apiKeySource mismatch") == "config_error"
    assert classify_error(executor_id="grok", stderr_text="xai 429 resource_exhausted") == "rate_limit"

    # Fallback: signal vs normal non-zero exit
    assert classify_error(exit_code=-9) == "crash"
    assert classify_error(exit_code=137) == "crash"
    assert classify_error(exit_code=1) == "tool_error"
    assert classify_error() == "unknown"

    # Policy check (Locked Q2 2.2)
    p_trans = route_error("rate_limit")
    assert p_trans.should_retry is True and p_trans.backoff_seconds == (5, 15, 45)
    p_det = route_error("config_error")
    assert p_det.should_retry is False and p_det.fast_fail_alert is True
    p_exec = route_error("bad_output")
    assert p_exec.should_retry is False and p_exec.preserve_worktree is True


# ---------------------------------------------------------------------------
# 5. External .log store, path traversal guard, remote fallback & dual usage (Q3)
# ---------------------------------------------------------------------------
def test_log_store_security_cleanup_and_remote_fallback(tmp_path: Path) -> None:
    ls = LocalLogStore(tmp_path)
    ref = ls.write_log("run_01", "line1\nline2\nline3\n", date_str="2026-11-22")
    assert ref == "local://2026-11-22/run_01.log"
    assert ls.read_tail(ref, tail_lines=2) == "line2\nline3\n"

    # Path traversal blocked
    with pytest.raises(ValueError):
        ls.read_tail("local://../../etc/passwd")

    # Retention cleanup removes expired logs
    ls.write_log("run_old", "old\n", date_str="2026-10-01")
    removed = ls.cleanup(
        max_age_days=14,
        now_utc=datetime(2026, 11, 22, tzinfo=timezone.utc),
    )
    assert any(p.name == "run_old.log" for p in removed)

    # Remote fetcher fallback when VPS offline
    class OfflineFetcher:
        def fetch_tail(self, runner_id: str, rel_path: str, *, tail_lines: int = 120, max_bytes: int = 65536) -> str:
            raise ConnectionError("vps offline")

    reader = CompositeLogReader(ls, OfflineFetcher())
    res = reader.read_tail("runner://vps-sg-01/2026-11-22/run_99.log", fallback_summary="tail err summary")
    assert res.source == "fallback_summary"
    assert res.content == "tail err summary"


def test_dual_source_usage_extraction() -> None:
    hdrs = magpie_trace_headers(run_id="run_1", correlation_key="github:a/b#1", trace_id="trc_1")
    assert hdrs["X-Run-Id"] == "run_1"
    assert hdrs["X-Correlation-Key"] == "github:a/b#1"

    # Magpie takes priority over CLI
    u1 = extract_usage(
        magpie_usage={"model": "claude-opus-5-5", "input_tokens": 1000, "output_tokens": 200, "cost_usd": 0.05},
        cli_json={"usage": {"input_tokens": 999, "output_tokens": 111}},
    )
    assert u1 is not None and u1.usage_source == "magpie" and u1.input_tokens == 1000

    # CLI stream-json fallback
    ndjson = '{"type":"init"}\n{"type":"result","model":"claude-sonnet","usage":{"input_tokens":320,"output_tokens":80},"total_cost_usd":0.004}\n'
    u2 = extract_usage(cli_stdout=ndjson)
    assert u2 is not None and u2.usage_source == "cli" and u2.output_tokens == 80 and u2.cost_usd == pytest.approx(0.004)


# ---------------------------------------------------------------------------
# 6. Memory anti-pollution gate (Q4 4.2) & Adapters (Q0/Q1)
# ---------------------------------------------------------------------------
def test_memory_gate_rejects_transient_and_promotes_idempotently(tmp_path: Path) -> None:
    with pytest.raises(MemoryGateError):
        create_postmortem_candidate(
            tmp_path,
            inbox_id="proj:hibernaut",
            source_run_id="run_01",
            error_kind="rate_limit",
            title="429",
            rule_text="never add transient",
            invalidation_condition="never",
        )

    cand, cpath = create_postmortem_candidate(
        tmp_path,
        inbox_id="proj:hibernaut",
        source_run_id="run_01",
        error_kind="tool_error",
        title="pytest cache flag",
        rule_text="Run pytest with -p no:cacheprovider in read-only worktrees.",
        invalidation_condition="When worktree mount becomes writable",
        recurrence_count=1,
    )
    inst = tmp_path / "instructions.md"
    # Not approved & recurrence < 2 -> blocked
    r1 = promote_to_instructions(cpath, inst, approved=False, min_recurrence=2)
    assert r1.promoted is False

    # Approved -> promoted once, second call is idempotent
    r2 = promote_to_instructions(cpath, inst, approved=True)
    assert r2.promoted is True
    assert cand.candidate_id in inst.read_text(encoding="utf-8")
    r3 = promote_to_instructions(cpath, inst, approved=True)
    assert r3.promoted is False


def test_switchboard_adapter_redacts_secrets_and_mcp_read_handlers(tmp_path: Path) -> None:
    store = SQLiteLedgerStore(tmp_path / "ledger.db")
    logs = LocalLogStore(tmp_path)
    try:
        sb_adapter = SwitchboardLedgerAuditAdapter(store)
        sb_adapter.emit_audit(
            "relay.auth",
            "auth_failed",
            source="relay",
            consumer_id="hb-mac",
            correlation_key="github:kun-g/hibernaut#42",
            scope="proj:hibernaut",
            detail={"token": "super-secret", "nested": {"api_key": "sk-123"}, "reason": "bad_sig"},
        )
        audits = store.list_audits(service="switchboard")
        assert len(audits) == 1
        assert audits[0].severity == "error"
        assert audits[0].detail["token"] == "[REDACTED]"
        assert audits[0].detail["nested"]["api_key"] == "[REDACTED]"
        assert audits[0].detail["reason"] == "bad_sig"

        ref = logs.write_log("run_mcp", "stdout line A\nstderr line B\n", date_str="2026-11-22")
        store.append_run(
            RunRecord(
                run_id="run_mcp",
                inbox_id="proj:hibernaut",
                session_id="sess_mcp",
                event_ids=("ev_mcp",),
                executor_id="claude",
                started_at="2026-11-22T12:00:00.000Z",
                ended_at="2026-11-22T12:00:02.000Z",
                duration_ms=2000,
                outcome_kind="emit",
                emit_status="done",
                log_ref=ref,
            )
        )
        mcp = MCPReadToolHandlers(store, store, CompositeLogReader(logs))
        runs_res = mcp.hibernaut_list_runs(inbox_id="proj:hibernaut")
        assert runs_res["count"] == 1
        log_res = mcp.hibernaut_get_run_log(run_id="run_mcp", tail_lines=10)
        assert log_res["found"] is True and "stderr line B" in log_res["content"]
        stats_res = mcp.hibernaut_executor_stats()
        assert stats_res["executors"][0]["executor_id"] == "claude"
    finally:
        store.close()
