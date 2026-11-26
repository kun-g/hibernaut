from __future__ import annotations

import asyncio
import hashlib
import http.client
import json
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

import pytest

from agent_runtime.adapters import runner_bridge, runner_pilot
from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.adapters.runner_pilot import LAUNCH_PERMISSION, ClosedRouteExecutor, ProtocolCheckExecutor
from agent_runtime.app.config import RunnerPilotConfig, load_config, parse_config
from agent_runtime.app.main import AlwaysOnDaemon, build_daemon_from_config
from agent_runtime.core.models import Emit, Event, ForwardRefused, PermissionDecision, RunCancelled, SessionRef, Silent
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine.closing import close_session
from runner.protocol import (
    InterruptRequest,
    InterruptResult,
    LogRef,
    RunnerCapabilities,
    RunRequest,
    RunResult,
    ToolAuthorizer,
)
from runner.protocol_check import (
    CHECK_SCHEMA,
    PROTOCOL_CHECK_EXECUTOR_ID,
    PROTOCOL_FILES,
    CheckRequest,
    manifest_sha256,
)
from runner.registry import NoEligibleRunnerError
from runner.transport.relay_hub import RunnerConnectionLostError
from tests.support.fakes import InMemoryPermission, RecordingChannel
from tests.support.runner_pilot_support import ROOT, RUNNER_ID
from tests.support.runner_pilot import KEY, OTHER, PILOT, _relay, pilot


COMMIT = "a" * 40


def _raw(**pilot: Any) -> dict[str, Any]:
    return {
        "runtime": {"default_executor": "cli"},
        "executors": {"cli": {"command": "true"}},
        "projects": {"hibernaut": {"repo": "x/y", "checkout": "/tmp/x", "inbox_id": ["proj:hibernaut", "hibernaut:.*"]}},
        "runner_pilot": {
            "enabled": True, "inbox_id": PILOT, "runner_id": RUNNER_ID, "project_key": "hibernaut",
            "token_file": "relay.token", **pilot,
        },
    }


def test_the_route_is_off_unless_a_deployment_turns_it_on() -> None:
    assert parse_config({}).runner_pilot == RunnerPilotConfig()
    assert parse_config({}).runner_pilot.enabled is False
    assert parse_config({"runner_pilot": {}}).runner_pilot.enabled is False
    example = load_config(ROOT / "hibernaut.example.toml")
    assert example.runner_pilot == RunnerPilotConfig()


def test_enabled_settings_are_read_and_paths_resolved(tmp_path: Path) -> None:
    pilot = parse_config(_raw(port=8800, timeout_seconds=30), base_dir=tmp_path).runner_pilot
    assert pilot == RunnerPilotConfig(
        enabled=True, inbox_id=PILOT, runner_id=RUNNER_ID, project_key="hibernaut", port=8800,
        token_file=str(tmp_path / "relay.token"), timeout_seconds=30.0,
    )


@pytest.mark.parametrize(
    "change",
    [
        {"enabled": "yes"}, {"inbox_id": ""}, {"inbox_id": "proj:hibernaut"}, {"inbox_id": "user:main"},
        {"inbox_id": "default"}, {"inbox_id": "hibernaut:.* "}, {"runner_id": ""}, {"project_key": 1},
        {"token_file": ""}, {"port": 0}, {"port": True}, {"port": 8080}, {"timeout_seconds": 0},
        {"timeout_seconds": 601}, {"timeout_seconds": float("nan")}, {"host": "0.0.0.0"}, {"executor": "claude"},
    ],
)
def test_unusable_settings_are_refused(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        parse_config(_raw(**change))


def test_the_pilot_inbox_must_be_its_own_and_the_executor_name_is_reserved() -> None:
    raw = _raw()
    raw["projects"]["hibernaut"]["inbox_id"] = PILOT
    with pytest.raises(ValueError, match="primary inbox of projects.hibernaut"):
        parse_config(raw)
    raw = _raw()
    raw["gdrive"] = {"default_inbox_id": PILOT}
    with pytest.raises(ValueError, match="gdrive.default_inbox_id"):
        parse_config(raw)
    raw = _raw(enabled=False)
    raw["executors"][PROTOCOL_CHECK_EXECUTOR_ID] = {"command": "true"}
    with pytest.raises(ValueError, match="reserved"):
        parse_config(raw)
    raw = _raw()
    raw["runtime"]["default_executor"] = "missing"
    with pytest.raises(ValueError, match="default_agent"):
        parse_config(raw)


@pytest.mark.parametrize(
    ("table", "entry"),
    [
        ("agents", {"executor": "claude", "role": "code-reviewer", "model": "m", "launcher": "l"}),
        ("agents", {"executor": "cli"}),
        ("executors", {}),
    ],
)
def test_neither_an_agent_nor_a_driver_may_take_the_executor_name(table: str, entry: dict[str, Any]) -> None:
    raw = _raw(enabled=False)
    raw.setdefault(table, {})[PROTOCOL_CHECK_EXECUTOR_ID] = entry
    with pytest.raises(ValueError, match=rf"{table}\.{PROTOCOL_CHECK_EXECUTOR_ID}: the name is reserved"):
        parse_config(raw)


def test_the_relay_port_must_not_be_the_one_of_the_general_runner() -> None:
    raw = _raw(port=8790)
    raw["executors"]["claude"] = {"command": "claude"}
    raw["runner"] = {"enabled": True, "mode": "relay", "executors": ["claude"], "listen_port": 8790}
    with pytest.raises(ValueError, match="runner_pilot.port is runner.listen_port"):
        parse_config(raw)
    for change in ({"mode": "in_process"}, {"enabled": False}, {"listen_port": 8795}):
        other = {**raw, "runner": raw["runner"] | change}
        assert parse_config(other).runner_pilot.port == 8790


def test_the_relay_port_must_not_be_the_one_of_the_memory_service() -> None:
    http = {"enabled": True, "listen_port": 8790, "tokens": {"a": {"token_env": "T"}}}
    raw = _raw(port=8790)
    with pytest.raises(ValueError, match="runner_pilot.port is memory_hub.http.listen_port"):
        parse_config({**raw, "memory_hub": {"http": http}})
    for change in ({"enabled": False}, {"listen_port": 8795}):
        assert parse_config({**raw, "memory_hub": {"http": http | change}}).runner_pilot.port == 8790


def test_demo_mode_cannot_open_the_route(tmp_path: Path) -> None:
    raw = _raw()
    raw["runtime"] |= {"db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "mem")}
    with pytest.raises(ValueError, match="--demo"):
        build_daemon_from_config(parse_config(raw), demo=True)


class _FakeRunner:
    def __init__(self, result: Callable[[RunRequest], RunResult] | BaseException) -> None:
        self.requests: list[RunRequest] = []
        self._result = result

    async def capabilities(self) -> RunnerCapabilities:
        raise NotImplementedError

    async def reclaim_workspace(self, req: Any) -> Any:
        raise NotImplementedError

    async def run(self, req: RunRequest, *, authorizer: ToolAuthorizer | None = None) -> RunResult:
        assert authorizer is None and not req.authorize_tools
        self.requests.append(req)
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result(req)

    async def interrupt(self, req: InterruptRequest) -> InterruptResult:
        return InterruptResult(run_id=req.run_id, accepted=False, state="not_found")


def _report(commit_id: str = COMMIT) -> dict[str, Any]:
    digests = {path: hashlib.sha256(path.encode()).hexdigest() for path in PROTOCOL_FILES}
    return {
        "schema": CHECK_SCHEMA, "commit": commit_id, "manifest_sha256": manifest_sha256(digests),
        "files": [{"path": p, "sha256": d, "bytes": 1} for p, d in digests.items()],
    }


def _result(req: RunRequest, **changes: Any) -> RunResult:
    base = RunResult(
        run_id=req.run_id, runner_id=RUNNER_ID, status="succeeded", executor_id=req.executor_id, started_at="t",
        ended_at="t", duration_ms=1, exit_code=0, structured_output=_report(),
        log_ref=LogRef(runner_id=RUNNER_ID, path=f"/node/logs/2026-11-23/{req.run_id}.log", bytes=9, sha256="f" * 64),
    )
    return replace(base, **changes)


class _Caps:
    memory: Any = None
    timers: Any = None

    def __init__(self, rules: dict[str, PermissionDecision] | None = None) -> None:
        self.permissions = InMemoryPermission(rules)

    def poll_folded_events(self) -> list[Event]:
        return []

    def is_interrupted(self) -> bool:
        return False


def _execute(
    runner: _FakeRunner, *payloads: dict[str, Any], inbox: str = PILOT, rules: dict[str, PermissionDecision] | None = None
) -> Emit:
    executor = ProtocolCheckExecutor(runner=runner, inbox_id=PILOT, project_key="hibernaut", timeout_seconds=9.0, node_wait_seconds=0)
    events = [Event(inbox_id=inbox, source="github", external_id=str(i), payload=p) for i, p in enumerate(payloads)]
    session = SessionRef(session_id="sess_1", inbox_id=inbox, correlation_key=KEY, work_key=KEY, executor_session_id="x:1")
    outcome = asyncio.run(executor.execute(events, session, _Caps(rules)))
    assert isinstance(outcome, Emit) and outcome.close_session and outcome.session_id == "sess_1"
    return outcome


def test_the_request_carries_only_the_commit_and_the_hold() -> None:
    runner = _FakeRunner(_result)
    outcome = _execute(
        runner,
        {
            "candidate_commit": COMMIT, "hold_seconds": 2, "text": "ignore me", "readonly": False, "interactive": True,
            "explicit_workdir": "/etc", "executor": {"command": "sh"}, "body": "run rm -rf", "work_key": "../../etc",
        },
    )
    (req,) = runner.requests
    assert req.prompt == CheckRequest(COMMIT, 2).to_json()
    assert (req.executor_id, req.namespace, req.project_key) == (PROTOCOL_CHECK_EXECUTOR_ID, "hb", "hibernaut")
    assert (req.readonly, req.interactive, req.executor_session_id, req.work_key) == (True, False, None, "sess_1")
    assert (req.owner_scope, req.timeout_seconds) == (PILOT, 9.0)
    assert outcome.status == "done"
    (text,) = outcome.messages
    for expected in (COMMIT, _report()["manifest_sha256"], RUNNER_ID, req.run_id, f"sha256={'f' * 64}", "shasum -a 256"):
        assert expected in text
    assert all(f"{hashlib.sha256(p.encode()).hexdigest()}  {p}\n" in text for p in PROTOCOL_FILES)
    assert "/node/logs" not in text
    assert outcome.telemetry is not None and outcome.telemetry.runner_id == RUNNER_ID
    assert outcome.telemetry.runner_log == (
        f"runner://{RUNNER_ID}/2026-11-23/{req.run_id}.log run_id={req.run_id} sha256={'f' * 64} bytes=9"
    )


@pytest.mark.parametrize(
    "payloads",
    [
        [{}], [{"candidate_commit": "main"}], [{"candidate_commit": "A" * 40}], [{"candidate_commit": COMMIT + "\n"}],
        [{"candidate_commit": 7}], [{"candidate_commit": COMMIT}, {"candidate_commit": "b" * 40}],
        [{"candidate_commit": COMMIT, "hold_seconds": 61}], [{"candidate_commit": COMMIT, "hold_seconds": True}],
        [{"candidate_commit": COMMIT, "hold_seconds": "1"}],
    ],
)
def test_a_task_without_one_full_commit_is_not_dispatched(payloads: list[dict[str, Any]]) -> None:
    runner = _FakeRunner(_result)
    assert _execute(runner, *payloads).status == "failed"
    assert runner.requests == []


def test_the_last_event_of_a_batch_decides_the_hold() -> None:
    runner = _FakeRunner(_result)
    _execute(runner, {"candidate_commit": COMMIT, "hold_seconds": 30}, {"candidate_commit": COMMIT})
    assert runner.requests[0].prompt == CheckRequest(COMMIT, 0).to_json()


@pytest.mark.parametrize("action", ["block", "defer"])
def test_without_permission_nothing_is_dispatched(action: Any) -> None:
    runner = _FakeRunner(_result)
    outcome = _execute(
        runner, {"candidate_commit": COMMIT}, rules={LAUNCH_PERMISSION: PermissionDecision(action=action, reason="no")}
    )
    assert (outcome.status, runner.requests) == ("failed", [])


def test_another_inbox_is_refused_by_the_executor_itself() -> None:
    runner = _FakeRunner(_result)
    assert _execute(runner, {"candidate_commit": COMMIT}, inbox=OTHER).status == "failed"
    assert runner.requests == []


def _tampered(**changes: Any) -> dict[str, Any]:
    return {**_report(), **changes}


@pytest.mark.parametrize(
    "report",
    [
        None, {}, _report("b" * 40), _tampered(manifest_sha256="0" * 64), _tampered(files=_report()["files"][:4]),
        _tampered(files=[*_report()["files"], {"path": "x", "sha256": "0" * 64}]), _tampered(files="x"),
        _tampered(files=[*_report()["files"][:4], {"path": PROTOCOL_FILES[4], "sha256": "z" * 64}]),
        _tampered(schema="other"),
    ],
)
def test_a_report_that_does_not_add_up_is_a_failure(report: dict[str, Any] | None) -> None:
    outcome = _execute(_FakeRunner(lambda req: _result(req, structured_output=report)), {"candidate_commit": COMMIT})
    assert outcome.status == "failed"
    assert outcome.telemetry is not None and outcome.telemetry.error_kind == "bad_output"


@pytest.mark.parametrize(
    ("result", "kind", "runner_id"),
    [
        (RunnerConnectionLostError(RUNNER_ID, "run_1", "disconnected"), "network", RUNNER_ID),
        (NoEligibleRunnerError("none online"), "network", None),
        (RuntimeError("runner rejected run: x"), "network", None),
        (lambda req: _result(req, status="failed", exit_code=1, error_kind="crash", structured_output=None), "crash", RUNNER_ID),
        (lambda req: _result(req, status="timed_out", error_kind="timeout", structured_output=None), "timeout", RUNNER_ID),
        (lambda req: _result(req, status="refused", error_kind="permission_denied"), "permission_denied", RUNNER_ID),
        # Must not surface as a cancellation: nothing cancelled this Run, so it would end the worker loop.
        (lambda req: _result(req, status="interrupted", structured_output=None), None, RUNNER_ID),
    ],
)
def test_every_failure_is_a_failed_result_that_names_the_node_when_known(
    result: Any, kind: str | None, runner_id: str | None
) -> None:
    outcome = _execute(_FakeRunner(result), {"candidate_commit": COMMIT})
    assert outcome.status == "failed" and outcome.telemetry is not None
    assert (outcome.telemetry.error_kind, outcome.telemetry.runner_id) == (kind, runner_id)


def test_the_closed_route_runs_nothing() -> None:
    session = SessionRef(session_id="s", inbox_id=OTHER)
    outcome = asyncio.run(ClosedRouteExecutor().execute([], session, _Caps()))
    assert isinstance(outcome, Emit) and (outcome.status, outcome.close_session) == ("failed", True)


class _FakeRelay(_FakeRunner):
    listening = True


def _daemon(
    tmp_path: Path, relay: _FakeRelay, *, enabled: bool = True, runner: dict[str, Any] | None = None
) -> AlwaysOnDaemon:
    # The bindings send every task inbox, the pilot's included, to "cli" and allow nothing else there.
    raw = _raw(enabled=enabled)
    checkout = tmp_path / "checkout"
    checkout.mkdir(exist_ok=True)
    raw["runtime"] |= {"db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "mem")}
    raw["executors"]["other"] = {"command": "true"}
    if runner is not None:
        raw["executors"]["claude"] = {"command": "claude"}
        raw["runner"] = runner
    raw["projects"]["hibernaut"] |= {
        "checkout": str(checkout),
        "inbox_bindings": [
            {"pattern": "hibernaut:.*", "agents": ["cli"], "default_agent": "cli",
             "rules": [{"source": "github", "agent": "cli"}]},
            {"pattern": "proj:hibernaut", "default_agent": "cli"},
        ],
    }
    return build_daemon_from_config(
        parse_config(raw, base_dir=tmp_path), runner_pilot=relay if enabled else None, channel=RecordingChannel(),  # type: ignore[arg-type]
    )


def test_the_general_runner_and_the_route_are_independent(tmp_path: Path) -> None:
    relay = _FakeRelay(_result)
    daemon = _daemon(tmp_path, relay, runner={
        "enabled": True, "executors": ["claude"], "state_dir": str(tmp_path / "runner"), "block_tools": [LAUNCH_PERMISSION],
    })
    try:
        pilot = daemon.pool._worker_factory(PILOT, "w")
        assert set(pilot._executors) == {PROTOCOL_CHECK_EXECUTOR_ID}
        assert isinstance(pilot._executors[PROTOCOL_CHECK_EXECUTOR_ID], ProtocolCheckExecutor)
        other = daemon.pool._worker_factory(OTHER, "w")._executors
        assert type(other["claude"]) is RunnerExecutorBridge
        assert isinstance(other[PROTOCOL_CHECK_EXECUTOR_ID], ClosedRouteExecutor)
        # A tool the deployment blocks is blocked on the route too.
        daemon.handler.ingest(inbox_id=PILOT, source="t", external_id="1", payload={"candidate_commit": COMMIT})
        _process(daemon, PILOT)
        (sent,) = _rows(daemon, "outbox", PILOT)
        assert "权限检查未放行（block）" in sent["content"]
        assert relay.requests == []
    finally:
        daemon.close()


def _projectless_daemon(tmp_path: Path, relay: _FakeRelay, *, enabled: bool = True) -> AlwaysOnDaemon:
    # One workdir binding covers every task inbox, the pilot's included; no project does.
    raw = _raw(enabled=enabled)
    (tmp_path / "ops").mkdir()
    raw["projects"]["hibernaut"] |= {"checkout": str(tmp_path), "inbox_id": "proj:hibernaut"}
    raw["runtime"] |= {
        "db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "mem"),
        "inbox_bindings": [
            {"pattern": "hibernaut:.*", "workdir": str(tmp_path / "ops"), "checkout_access": "readonly", "default_agent": "cli"}
        ],
    }
    return build_daemon_from_config(
        parse_config(raw, base_dir=tmp_path), runner_pilot=relay if enabled else None, channel=RecordingChannel(),  # type: ignore[arg-type]
    )


@pytest.mark.parametrize("enabled", [True, False])
def test_a_workdir_binding_of_a_projectless_inbox_does_not_take_the_pilot_inbox_over(tmp_path: Path, enabled: bool) -> None:
    relay = _FakeRelay(_result)
    daemon = _projectless_daemon(tmp_path, relay, enabled=enabled)
    try:
        pilot, other = (daemon.pool._worker_factory(inbox, "w") for inbox in (PILOT, OTHER))
        assert isinstance(other._executors[PROTOCOL_CHECK_EXECUTOR_ID], ClosedRouteExecutor)
        assert set(pilot._executors) == ({PROTOCOL_CHECK_EXECUTOR_ID} if enabled else set(other._executors))
        daemon.handler.ingest(inbox_id=PILOT, source="t", external_id="1", payload={"candidate_commit": COMMIT})
        if enabled:
            _process(daemon, PILOT)
            (sent,) = _rows(daemon, "outbox", PILOT)
            assert "Runner 协议文件校验通过" in sent["content"] and len(relay.requests) == 1
    finally:
        daemon.close()


def _process(daemon: AlwaysOnDaemon, inbox_id: str, executor: Any = None) -> None:
    worker = daemon.pool._worker_factory(inbox_id, "w")
    if executor is not None:
        worker._executors[executor.executor_id] = executor
    asyncio.run(worker.process_inbox(inbox_id))


def _rows(daemon: AlwaysOnDaemon, table: str, inbox_id: str) -> list[sqlite3.Row]:
    return daemon.db.fetchall(f"SELECT * FROM {table} WHERE inbox_id = ? ORDER BY rowid", (inbox_id,))


class _Forwarder:
    executor_id = "cli"

    def __init__(self, **kwargs: Any) -> None:
        self._kwargs = kwargs
        self.refused: str | None = None
        self.seen: list[Event] = []

    async def execute(self, events: Any, session: SessionRef, caps: RunCapabilities) -> Silent:
        self.seen += events
        try:
            caps.forward_to_inbox(PILOT, "核验候选提交", return_to=OTHER, **self._kwargs)
        except ForwardRefused as exc:
            self.refused = str(exc)
        return Silent()


def test_an_uncovered_pilot_inbox_takes_no_forwards(tmp_path: Path) -> None:
    raw = _raw()
    raw["projects"]["hibernaut"] |= {"checkout": str(tmp_path), "inbox_id": ["proj:hibernaut", OTHER]}
    raw["runtime"] |= {"db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "mem")}
    daemon = build_daemon_from_config(
        parse_config(raw, base_dir=tmp_path), runner_pilot=_FakeRelay(_result), channel=RecordingChannel(),  # type: ignore[arg-type]
    )
    daemon.handler.ingest(inbox_id=OTHER, source="github", external_id="1", payload={"text": "go"}, correlation_key="c1")
    asked = _Forwarder()
    _process(daemon, OTHER, asked)
    assert asked.refused is not None and "is not an inbox of this project" in asked.refused
    daemon.close()


@pytest.mark.parametrize("key", ["agent_id", "executor_id"])
def test_inbox_bindings_neither_take_the_pilot_inbox_over_nor_refuse_its_executor(tmp_path: Path, key: str) -> None:
    relay = _FakeRelay(_result)
    daemon = _daemon(tmp_path, relay)
    daemon.handler.ingest(inbox_id=PILOT, source="github", external_id="1", payload={key: "cli"}, correlation_key="c1")
    daemon.handler.ingest(
        inbox_id=PILOT, source="github", external_id="2", payload={"candidate_commit": COMMIT}, correlation_key="c2"
    )
    daemon.handler.ingest(
        inbox_id=PILOT, source="github", external_id="3", correlation_key="c3",
        payload={"candidate_commit": COMMIT, key: PROTOCOL_CHECK_EXECUTOR_ID},
    )
    _process(daemon, PILOT)
    assert [row["status"] for row in _rows(daemon, "event_inbox", PILOT)] == ["pending", "done", "done"]
    assert len(relay.requests) == 2
    daemon.close()


@pytest.mark.parametrize("build", [_daemon, _projectless_daemon])
def test_work_is_forwarded_to_the_pilot_inbox_only_for_its_own_executor(
    tmp_path: Path, build: Callable[[Path, _FakeRelay], AlwaysOnDaemon]
) -> None:
    daemon = build(tmp_path, _FakeRelay(_result))
    daemon.handler.ingest(inbox_id=OTHER, source="github", external_id="1", payload={"text": "go"}, correlation_key="c1")
    asked = _Forwarder(target_agent="cli")
    _process(daemon, OTHER, asked)
    assert asked.refused is not None and "'cli' is not allowed" in asked.refused
    assert _rows(daemon, "event_inbox", PILOT) == []
    daemon.close()


@pytest.mark.parametrize("build", [_daemon, _projectless_daemon])
def test_the_pilot_hands_its_result_back_to_the_inbox_that_asked_for_it(
    tmp_path: Path, build: Callable[[Path, _FakeRelay], AlwaysOnDaemon]
) -> None:
    # A forward carries no candidate commit, so the result is a refusal to check; what counts is that it comes back.
    relay = _FakeRelay(_result)
    daemon = build(tmp_path, relay)
    daemon.handler.ingest(inbox_id=OTHER, source="github", external_id="1", payload={"text": "go"}, correlation_key="c1")
    asked = _Forwarder()
    _process(daemon, OTHER, asked)
    assert asked.refused is None
    woken: list[str] = []
    daemon.pool.wake = woken.append  # type: ignore[method-assign]
    _process(daemon, PILOT)
    assert relay.requests == []
    [task] = _rows(daemon, "event_inbox", PILOT)
    assert task["status"] == "done"
    _, back = _rows(daemon, "event_inbox", OTHER)
    payload = json.loads(back["payload_json"])
    assert (back["source"], payload["status"], payload["forwarded_by"]) == (
        "agent:return", "failed", PROTOCOL_CHECK_EXECUTOR_ID
    )
    assert "candidate_commit" in payload["text"]
    assert payload["trace_id"] == json.loads(task["payload_json"])["trace_id"]
    assert woken == [OTHER]
    daemon.close()


def test_work_withdrawn_from_another_executor_does_not_stop_the_checks(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path, _FakeRelay(_result), enabled=False)
    daemon.handler.ingest(inbox_id=PILOT, source="github", external_id="0", payload={"text": "hi"}, correlation_key="c1")
    _process(daemon, PILOT, _Forwarder(target_agent="other"))
    daemon.handler.ingest(inbox_id=OTHER, source="github", external_id="1", payload={"text": "go"}, correlation_key="c1")
    asked = _Forwarder()
    _process(daemon, OTHER, asked)
    assert asked.refused is None
    [parent] = daemon.sessions.list_sessions(inbox_id=OTHER)
    close_session(parent, daemon.sessions, daemon.handler.queue, cause="test")
    daemon.close()
    relay = _FakeRelay(_result)
    daemon = _daemon(tmp_path, relay)
    daemon.handler.ingest(
        inbox_id=PILOT, source="github", external_id="2", payload={"candidate_commit": COMMIT}, correlation_key="c2"
    )
    _process(daemon, PILOT)
    assert [row["status"] for row in _rows(daemon, "event_inbox", PILOT)] == ["done", "done", "done"]
    assert len(relay.requests) == 1
    daemon.close()


def test_with_the_route_off_the_bindings_apply_to_the_pilot_inbox_again(tmp_path: Path) -> None:
    daemon = _daemon(tmp_path, _FakeRelay(_result), enabled=False)
    daemon.handler.ingest(inbox_id=OTHER, source="github", external_id="1", payload={"text": "go"}, correlation_key="c1")
    asked = _Forwarder(target_agent="cli")
    _process(daemon, OTHER, asked)
    assert asked.refused is None
    daemon.handler.ingest(
        inbox_id=PILOT, source="github", external_id="2", correlation_key="c2",
        payload={"agent_id": PROTOCOL_CHECK_EXECUTOR_ID, "candidate_commit": COMMIT},
    )
    done = _Forwarder(target_agent="cli")
    _process(daemon, PILOT, done)
    forwarded, addressed = _rows(daemon, "event_inbox", PILOT)
    assert forwarded["status"] == "done" and [e.event_id for e in done.seen] == [forwarded["event_id"]]
    assert addressed["status"] == "failed" and "not allowed" in addressed["last_error"]
    daemon.close()


def test_relay_admits_only_the_pilot_node(tmp_path: Path) -> None:
    relay = _relay(tmp_path)
    good: dict[str, Any] = {
        "schema_version": "runner.v1", "runner_id": RUNNER_ID, "namespaces": ["hb"], "projects": ["hibernaut"],
        "worktree_backend": "git", "interactive_surface": "none", "labels": {},
        "executors": [{"executor_id": PROTOCOL_CHECK_EXECUTOR_ID, "driver": "custom", "supports_resume": False,
                       "supports_stream_json": False, "interactive_surface": "none"}],
        "slots": {"max_concurrent": 1, "active_runs": 0, "available": 1}, "reported_at": "t",
    }
    refusal = relay._refusal  # pyright: ignore[reportPrivateUsage]
    assert refusal(RunnerCapabilities.from_dict(good)) is None
    claude = {**good["executors"][0], "executor_id": "claude"}
    for change in (
        {"runner_id": "other"}, {"namespaces": ["hb", "ws"]}, {"projects": ["hibernaut", "x"]}, {"projects": []},
        {"executors": []}, {"executors": [claude]}, {"executors": [*good["executors"], claude]},
        {"slots": {"max_concurrent": 2, "active_runs": 0, "available": 2}},
    ):
        assert refusal(RunnerCapabilities.from_dict({**good, **change})) is not None, change


def test_waiting_for_the_node_is_bounded_and_can_be_interrupted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        clock = loop.time
        elapsed = 0.0
        next_heartbeat = asyncio.Event()

        async def retry(stop: asyncio.Event, seconds: float) -> bool:
            nonlocal elapsed
            assert seconds > 0
            elapsed += seconds
            if elapsed > heartbeat_at:
                next_heartbeat.set()
                return await stop.wait()
            await asyncio.sleep(0)
            return stop.is_set()

        # Advance by the actual delay requested by the bridge, keeping other loop deadlines monotonic.
        heartbeat_at = float("inf")
        monkeypatch.setattr(loop, "time", lambda: clock() + elapsed)
        monkeypatch.setattr(runner_bridge, "_set_within", retry)
        relay = _relay(tmp_path)
        await relay.start()
        event = Event(inbox_id=PILOT, source="t", external_id="1", payload={"candidate_commit": COMMIT})
        session = SessionRef(session_id="s", inbox_id=PILOT)

        def check(**kwargs: Any) -> ProtocolCheckExecutor:
            return ProtocolCheckExecutor(
                runner=relay, inbox_id=PILOT, project_key="hibernaut", timeout_seconds=9.0, **kwargs
            )

        outcome = await check(node_wait_seconds=0.6).execute([event], session, _Caps())
        assert isinstance(outcome, Emit) and (outcome.status, outcome.close_session) == ("failed", True)
        assert outcome.telemetry is not None and outcome.telemetry.error_kind == "network"
        assert 0.5 < elapsed <= 0.6
        heartbeat_at = elapsed + runner_pilot._HEARTBEAT_SECONDS
        run = asyncio.create_task(check().execute([event], session, _Caps()))
        run.add_done_callback(lambda _: next_heartbeat.set())
        await next_heartbeat.wait()
        assert elapsed > heartbeat_at and not run.done()
        started = time.monotonic()
        run.cancel()
        with pytest.raises(asyncio.CancelledError) as cancelled:
            await run
        # It never reached a node, so the ledger must not name one.
        assert not isinstance(cancelled.value, RunCancelled)
        assert time.monotonic() - started < 5
        await relay.stop()

    asyncio.run(asyncio.wait_for(scenario(), 20))
