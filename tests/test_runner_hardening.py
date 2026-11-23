from __future__ import annotations

import asyncio
import dataclasses
import json
import time
from pathlib import Path
from typing import Any

import pytest

from runner.cli_drivers import TemplateCLIDriver
from runner.daemon import RunnerDaemon
from runner.registry import NoEligibleRunnerError, WorkspaceAffinityStore
from runner.supervisor import ProcessSupervisor
from runner.transport.relay_hub import RunnerConnectionLostError, RunnerRelayHub
from runner.transport.ws_protocol import WSConnectionClosed, WSProtocolError, client_handshake, recv_json, send_json
from tests.test_runner_safety import PY, _config, _count_drivers, _init_repo, _req

_ENV_SCRIPT = "import json, os; print(json.dumps({'env': dict(os.environ)}))"


def _env_driver(extra: dict[str, str] | None = None) -> dict[str, TemplateCLIDriver]:
    return {
        "env": TemplateCLIDriver(
            executor_id="env", driver_kind="custom", command=PY, args=("-c", _ENV_SCRIPT), env=dict(extra or {})
        )
    }


def test_child_env_carries_no_credentials_from_the_runner_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    for key in ("AWS_SECRET_ACCESS_KEY", "ANTHROPIC_AUTH_TOKEN", "SOME_SERVICE_PASSWORD", "UNLISTED_SETTING"):
        monkeypatch.setenv(key, "placeholder-not-a-secret")
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_env_driver({"CLAUDE_CONFIG_DIR": str(tmp_path / "cfg")}))
    try:
        res = asyncio.run(daemon.run(_req("run_env", executor_id="env"), caller_id="hibernaut"))
    finally:
        daemon.close()
    assert res.status == "succeeded", res.error_summary
    child_env = json.loads(res.stdout_excerpt)["env"]
    for key in ("AWS_SECRET_ACCESS_KEY", "ANTHROPIC_AUTH_TOKEN", "SOME_SERVICE_PASSWORD", "UNLISTED_SETTING"):
        assert key not in child_env
    assert child_env["PATH"] and child_env["HOME"]
    assert child_env["CLAUDE_CONFIG_DIR"] == str(tmp_path / "cfg")


@pytest.mark.parametrize("key", ["AWS_SECRET_ACCESS_KEY", "MY_SERVICE_TOKEN", "DB_PASSWORD", "GCP_CREDENTIALS"])
def test_driver_env_may_not_add_credentials(tmp_path: Path, key: str) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_env_driver({key: "placeholder"}))
    try:
        res = asyncio.run(daemon.run(_req("run_env_extra", executor_id="env"), caller_id="hibernaut"))
    finally:
        daemon.close()
    assert (res.status, res.error_kind) == ("refused", "permission_denied")
    assert key in (res.error_summary or "")


@pytest.mark.parametrize("ending", ["timeout", "interrupt"])
def test_stdin_backpressure_does_not_block_timeout_or_interrupt(tmp_path: Path, ending: str) -> None:
    supervisor = ProcessSupervisor(runner_id="r", logs_dir=tmp_path / "logs")
    cancel = asyncio.Event()

    async def scenario() -> tuple[str, float]:
        if ending == "interrupt":
            asyncio.get_running_loop().call_later(0.3, cancel.set)
        started = time.monotonic()
        out = await asyncio.wait_for(
            supervisor.execute(
                run_id="run_stdin",
                argv=[PY, "-c", "import time; time.sleep(30)"],
                cwd=tmp_path,
                env={"PATH": "/usr/bin:/bin"},
                stdin_text="x" * (8 * 1024 * 1024),  # far above any pipe buffer, and the child never reads it
                timeout_seconds=0.5 if ending == "timeout" else 30.0,
                idle_timeout_seconds=None,
                cancel_event=cancel,
                grace_seconds=0.5,
            ),
            timeout=10.0,
        )
        return out.status, time.monotonic() - started

    status, elapsed = asyncio.run(scenario())
    assert status == ("timed_out" if ending == "timeout" else "interrupted")
    assert elapsed < 5.0


def test_interactive_request_is_refused_without_an_orca_terminal_launcher(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    marker = tmp_path / "ran"
    drivers = {
        "touch": TemplateCLIDriver(
            executor_id="touch",
            driver_kind="claude",
            command=PY,
            args=("-c", f"import pathlib; pathlib.Path({str(marker)!r}).write_text('1')"),
        )
    }
    cfg = dataclasses.replace(_config(tmp_path, repo), interactive_surface="orca")
    daemon = RunnerDaemon(cfg, drivers=drivers)
    try:
        caps = daemon.capabilities_sync()
        res = asyncio.run(
            daemon.run(dataclasses.replace(_req("run_inter", executor_id="touch"), interactive=True), caller_id="hibernaut")
        )
    finally:
        daemon.close()
    # Built-in drivers only launch headless subprocesses; they must not claim an Orca terminal.
    assert [e.interactive_surface for e in caps.executors] == ["none"]
    assert (res.status, res.error_kind) == ("refused", "config_error")
    assert not marker.exists()


async def _raw_agent(host: str, port: int, caps: dict[str, Any]) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, int]:
    reader, writer = await asyncio.open_connection(host, port)
    await client_handshake(reader, writer, host=f"{host}:{port}", path="/ws/runner")
    await send_json(writer, {"type": "hello", "token": "tok", "capabilities": caps, "ready_work_keys": []}, mask=True)
    welcome = await recv_json(reader, writer, expect_masked=False, send_masked=True)
    return reader, writer, int(welcome["conn_epoch"])


def test_silent_node_is_dropped_after_heartbeat_timeout_and_its_runs_fail(tmp_path: Path) -> None:
    # The node completes the handshake, then never answers again and never closes its socket.
    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=0.1)
    caps_daemon = RunnerDaemon(_config(tmp_path, repo, runner_id="mute-1"), drivers=_count_drivers())

    async def scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        reader, writer, _ = await _raw_agent(host, port, caps_daemon.capabilities_sync().to_dict())
        run = asyncio.create_task(hub.run(_req("run_mute", executor_id="count", prompt="unused")))
        try:
            with pytest.raises(RunnerConnectionLostError):
                await asyncio.wait_for(run, 3.0)
            rec = hub.registry.get("mute-1")
            assert rec is not None and not rec.online and rec.reserved_slots == 0
            # The hub closed its side of the silent connection: reading ends in EOF, not in a timeout.
            ended_by: str
            try:
                while True:
                    await asyncio.wait_for(recv_json(reader, writer, expect_masked=False, send_masked=True), 2.0)
            except TimeoutError:
                ended_by = "timeout"
            except (asyncio.IncompleteReadError, ConnectionError, WSConnectionClosed, WSProtocolError):
                ended_by = "closed"
            assert ended_by == "closed"
        finally:
            writer.close()
            await hub.stop()

    try:
        # Bounded: if the hub kept the silent socket open, teardown would wait on it forever instead of failing.
        asyncio.run(asyncio.wait_for(scenario(), 15.0))
    finally:
        caps_daemon.close()
        affinity.close()


def test_unbound_workspace_is_claimed_before_the_first_ack(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    caps_a = RunnerDaemon(_config(tmp_path, repo, runner_id="a-1", max_concurrent=1), drivers=_count_drivers())
    caps_b = RunnerDaemon(_config(tmp_path, repo, runner_id="b-1", max_concurrent=1), drivers=_count_drivers())
    first = _req("run_w1", executor_id="count", prompt="unused", work_key="gh#9")
    second = _req("run_w2", executor_id="count", prompt="unused", work_key="gh#9")

    async def scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        ra, wa, epoch_a = await _raw_agent(host, port, caps_a.capabilities_sync().to_dict())
        rb, wb, _ = await _raw_agent(host, port, caps_b.capabilities_sync().to_dict())
        try:
            run1 = asyncio.create_task(hub.run(first))
            submit = await asyncio.wait_for(recv_json(ra, wa, expect_masked=False, send_masked=True), 2.0)
            assert submit["request"]["run_id"] == "run_w1"
            # a-1 has not acknowledged yet; the same workspace must not be sent to b-1.
            with pytest.raises(NoEligibleRunnerError):
                await asyncio.wait_for(hub.run(second), 2.0)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(recv_json(rb, wb, expect_masked=False, send_masked=True), 0.3)
            assert affinity.lookup(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#9") == "a-1"

            # A rejection proves a-1 never ran it, so the claim is released.
            await send_json(
                wa,
                {"type": "run.accepted", "conn_epoch": epoch_a, "run_id": "run_w1", "accepted": False, "reason": "test"},
                mask=True,
            )
            with pytest.raises(RuntimeError):
                await asyncio.wait_for(run1, 2.0)
            assert affinity.lookup(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#9") is None
        finally:
            wa.close()
            wb.close()
            await hub.stop()

    try:
        asyncio.run(scenario())
    finally:
        caps_a.close()
        caps_b.close()
        affinity.close()
