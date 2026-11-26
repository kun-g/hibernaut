from __future__ import annotations

import ast
import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge, translate_run_result
from agent_runtime.core.models import (
    Emit,
    Event,
    PermissionDecision,
    RunCancelled,
    RunTelemetry,
    SessionRef,
    Silent,
    Suspend,
)
from runner.cli_drivers import extract_matching_native_session_id, format_prefixed_session_id
from runner.config import parse_runner_config
from runner.protocol import SCHEMA_VERSION, CredentialLease, InterruptRequest, ReclaimRequest, RunRequest, RunResult
from runner.supervisor import build_child_env


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_runner_zero_business_deps_ast_gate() -> None:
    forbidden = {"agent_runtime", "taskboard", "switchboard", "workshop", "memory_hub", "ledger"}
    runner_files = sorted((REPO_ROOT / "src" / "runner").rglob("*.py"))
    assert runner_files
    for py_file in runner_files:
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    assert top not in forbidden, f"{py_file} imports forbidden {alias.name}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                top = node.module.split(".")[0]
                assert top not in forbidden, f"{py_file} imports forbidden {node.module}"


def test_schemas_v1_valid_json_and_roundtrip() -> None:
    schema_dir = REPO_ROOT / "schemas" / "runner" / "v1"
    expected = {
        "runner.capabilities.schema.json",
        "runner.run_request.schema.json",
        "runner.run_result.schema.json",
        "runner.interrupt.schema.json",
        "runner.reclaim.schema.json",
        "runner.tool_authorize.schema.json",
    }
    found = {p.name for p in schema_dir.glob("*.schema.json")}
    assert expected == found
    for name in expected:
        doc = json.loads((schema_dir / name).read_text(encoding="utf-8"))
        assert doc["$schema"] == "https://json-schema.org/draft/2020-12/schema"

    req = RunRequest(
        run_id="run_01abc",
        namespace="hb",
        project_key="hibernaut",
        owner_scope="proj:hibernaut",
        work_key="github:kun-g/hibernaut#42",
        name_hint="issue-42",
        executor_id="claude",
        readonly=False,
        prompt="fix bug",
        credential_lease=CredentialLease(
            broker_url="https://broker.local/proxy",
            lease_token="tok_ephemeral",
            expires_at="2026-11-23T01:00:00Z",
        ),
    )
    assert RunRequest.from_dict(req.to_dict()) == req

    res = RunResult(
        run_id="run_01abc",
        runner_id="mac-m4",
        status="succeeded",
        executor_id="claude",
        started_at="2026-11-23T00:00:00Z",
        ended_at="2026-11-23T00:00:05Z",
        duration_ms=5000,
    )
    assert RunResult.from_dict(res.to_dict()) == res
    assert res.schema_version == SCHEMA_VERSION


def test_secret_stripping_and_driver_resume_self_check() -> None:
    dirty_env = {k: "secret-val" for k in ("GH_TOKEN", "ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN", "OPENAI_API_KEY")}
    dirty_env.update({"SAFE_VAR": "unlisted", "PATH": "/usr/bin", "LC_ALL": "C"})
    cleaned = build_child_env(dirty_env)
    assert cleaned == {"PATH": "/usr/bin", "LC_ALL": "C", "GIT_TERMINAL_PROMPT": "0"}

    assert format_prefixed_session_id("claude", "abc-123") == "claude:abc-123"
    assert extract_matching_native_session_id("claude", "claude:abc-123") == "abc-123"
    assert extract_matching_native_session_id("grok", "claude:abc-123") is None


@pytest.mark.parametrize(
    ("status", "structured", "expected_type"),
    [
        ("succeeded", {"outcome": "silent"}, Silent),
        ("succeeded", None, Emit),
        ("failed", None, Emit),
        ("timed_out", None, Emit),
        ("refused", None, Emit),
        ("succeeded", {"outcome": "suspend", "prompt": "ok?"}, Suspend),
    ],
)
def test_bridge_outcomes_carry_run_telemetry_for_the_ledger(
    status: str, structured: dict[str, Any] | None, expected_type: type
) -> None:
    result = RunResult(
        run_id="run_t",
        runner_id="vps-1",
        status=status,  # type: ignore[arg-type]
        executor_id="claude",
        started_at="2026-01-01T00:00:00Z",
        ended_at="2026-01-01T00:00:01Z",
        duration_ms=1000,
        exit_code=0 if status == "succeeded" else 2,
        error_kind=None if status == "succeeded" else "rate_limit",
        error_summary=None if status == "succeeded" else "boom on stderr",
        stdout_excerpt="tokens: 12",
        structured_output=structured,
    )
    outcome = translate_run_result(result, session_id="sess_1")
    assert isinstance(outcome, expected_type)
    assert outcome.telemetry == RunTelemetry(
        stdout="tokens: 12",
        stderr="" if status == "succeeded" else "boom on stderr",
        exit_code=0 if status == "succeeded" else 2,
        error_kind=None if status == "succeeded" else "rate_limit",
        runner_id="vps-1",
    )


def test_parse_runner_config_toml(tmp_path: Path) -> None:
    cfg = parse_runner_config(
        {
            "runner": {"runner_id": "mac-m4", "max_concurrent": 3, "worktree_backend": "git"},
            "namespaces": {
                "hb": {"prefix": "hb", "allowed_caller": "hibernaut"},
                "ws": {"prefix": "ws", "allowed_callers": ["workshop"]},
            },
            "projects": {"hibernaut": {"checkout_path": str(tmp_path)}},
        },
        base_dir=tmp_path,
    )
    assert cfg.runner_id == "mac-m4"
    assert cfg.max_concurrent == 3
    assert cfg.authorize_namespace("hb", "hibernaut").prefix == "hb"
    with pytest.raises(PermissionError):
        cfg.authorize_namespace("hb", "workshop")


def test_interrupted_result_cancels_with_the_runner_telemetry() -> None:
    result = RunResult(
        run_id="run_i",
        runner_id="vps-1",
        status="interrupted",
        executor_id="claude",
        started_at="2026-01-01T00:00:00Z",
        ended_at="2026-01-01T00:00:01Z",
        duration_ms=1000,
        exit_code=-15,
        error_summary="interrupted: new_event",
    )
    with pytest.raises(asyncio.CancelledError) as info:
        translate_run_result(result, session_id="sess_1")
    assert isinstance(info.value, RunCancelled)
    assert info.value.telemetry == RunTelemetry(stderr="interrupted: new_event", exit_code=-15, runner_id="vps-1")


class _RecordingRunner:
    def __init__(self) -> None:
        self.run_calls = 0

    async def capabilities(self) -> Any:
        raise AssertionError("not used")

    async def run(self, req: RunRequest) -> RunResult:
        self.run_calls += 1
        raise AssertionError(f"runner.run() must not be called, got {req.run_id}")

    async def interrupt(self, req: InterruptRequest) -> Any:
        raise AssertionError("not used")

    async def reclaim_workspace(self, req: ReclaimRequest) -> Any:
        raise AssertionError("not used")


class _PolicyCaps:
    def __init__(self, action: str) -> None:
        self._action = action
        self.checks: list[str] = []

    @property
    def memory(self) -> Any:
        return None

    @property
    def timers(self) -> Any:
        return None

    @property
    def permissions(self) -> Any:
        caps = self

        class _Perm:
            def check(self, tool: str, args: Any) -> PermissionDecision:
                caps.checks.append(tool)
                return PermissionDecision(action=caps._action, reason=f"{caps._action} by policy")  # type: ignore[arg-type]

        return _Perm()

    def poll_folded_events(self) -> list[Event]:
        return []

    def is_interrupted(self) -> bool:
        return False


@pytest.mark.parametrize("action", ["block", "defer"])
@pytest.mark.parametrize("calls_tools", [True, None], ids=["tool-capable", "tools-unknown"])
def test_bridge_refuses_tool_capable_or_unknown_executors_before_runner_run(action: str, calls_tools: bool | None) -> None:
    # Until per-call tool authorization reaches the runner, no CLI that may call tools is started through the bridge,
    # whatever the inbox policy says: block and defer would otherwise be bypassed.
    runner = _RecordingRunner()
    bridge = RunnerExecutorBridge(
        runner=runner,  # type: ignore[arg-type]
        executor_id="claude",
        project_key="hibernaut",
        **({} if calls_tools is None else {"executor_calls_tools": calls_tools}),
    )
    ev = Event(inbox_id="proj:hibernaut", source="github", external_id="e1", payload={"title": "Fix", "body": "Do it"})
    sess = SessionRef(session_id="sess_1", inbox_id="proj:hibernaut", correlation_key="github:kun-g/hibernaut#77")
    outcome = asyncio.run(bridge.execute([ev], sess, _PolicyCaps(action)))  # type: ignore[arg-type]
    assert runner.run_calls == 0
    assert isinstance(outcome, Emit) and outcome.status == "refused"
    assert "tool" in outcome.messages[0]
