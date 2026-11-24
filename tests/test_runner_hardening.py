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
from tests.test_runner_safety import PY, _config, _count_drivers, _count_started, _hold_count, _init_repo, _req

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
            # a-1 has not acknowledged yet; the same workspace must not be sent to b-1 (nor anywhere else).
            run2 = asyncio.create_task(hub.run(second))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(recv_json(rb, wb, expect_masked=False, send_masked=True), 0.3)
            assert not run2.done()
            assert affinity.lookup(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#9") == "a-1"

            # A rejection proves a-1 never ran it, so the claim is released.
            await send_json(
                wa,
                {"type": "run.accepted", "conn_epoch": epoch_a, "run_id": "run_w1", "accepted": False, "reason": "test"},
                mask=True,
            )
            with pytest.raises(RuntimeError):
                await asyncio.wait_for(run1, 2.0)
            # Only then is the second write dispatched, as a fresh claim.
            nxt = await asyncio.wait_for(recv_json(ra, wa, expect_masked=False, send_masked=True), 2.0)
            assert nxt["request"]["run_id"] == "run_w2"
            assert affinity.lookup(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#9") == "a-1"
            run2.cancel()
        finally:
            wa.close()
            wb.close()
            await hub.stop()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 20.0))
    finally:
        caps_a.close()
        caps_b.close()
        affinity.close()


@pytest.mark.parametrize("source", ["inherited", "driver"])
@pytest.mark.parametrize(
    "proxy",
    ["http://proxy-user:placeholder-pass@proxy.example:8080", "proxy-user:placeholder-pass@proxy.example:8080"],
    ids=["with-scheme", "schemeless"],
)
def test_proxy_url_with_user_credentials_never_reaches_the_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str, proxy: str
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    if source == "inherited":
        monkeypatch.setenv("HTTPS_PROXY", proxy)
        drivers = _env_driver()
    else:
        monkeypatch.delenv("HTTPS_PROXY", raising=False)
        drivers = _env_driver({"HTTPS_PROXY": proxy})
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=drivers)
    try:
        res = asyncio.run(daemon.run(_req("run_proxy", executor_id="env"), caller_id="hibernaut"))
    finally:
        daemon.close()
    assert (res.status, res.error_kind) == ("refused", "permission_denied")
    assert "HTTPS_PROXY" in (res.error_summary or "")
    assert "placeholder-pass" not in (res.error_summary or "")
    assert "placeholder-pass" not in res.stdout_excerpt


@pytest.mark.parametrize("proxy", ["http://proxy.example:8080", "proxy.example:8080"], ids=["with-scheme", "schemeless"])
def test_proxy_url_without_credentials_still_reaches_the_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, proxy: str
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    monkeypatch.setenv("HTTPS_PROXY", proxy)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_env_driver())
    try:
        res = asyncio.run(daemon.run(_req("run_proxy_ok", executor_id="env"), caller_id="hibernaut"))
    finally:
        daemon.close()
    assert res.status == "succeeded", res.error_summary
    assert json.loads(res.stdout_excerpt)["env"]["HTTPS_PROXY"] == proxy


def test_rejected_first_claim_on_a_multi_slot_node_does_not_split_the_workspace(tmp_path: Path) -> None:
    # a-1 has two slots, so without serialising, the second write for gh#7 also goes to a-1 while the first is
    # unacknowledged; a-1 then rejects the first, which moves to b-1, and both nodes would write gh#7.
    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    caps_a = RunnerDaemon(_config(tmp_path, repo, runner_id="a-1", max_concurrent=2), drivers=_count_drivers())
    caps_b = RunnerDaemon(_config(tmp_path, repo, runner_id="b-1", max_concurrent=1), drivers=_count_drivers())
    first = _req("run_c1", executor_id="count", prompt="unused", work_key="gh#7")
    second = _req("run_c2", executor_id="count", prompt="unused", work_key="gh#7")

    def bound() -> str | None:
        return affinity.lookup(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#7")

    async def scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        ra, wa, epoch_a = await _raw_agent(host, port, caps_a.capabilities_sync().to_dict())
        rb, wb, epoch_b = await _raw_agent(host, port, caps_b.capabilities_sync().to_dict())
        try:
            run1 = asyncio.create_task(hub.run(first))
            submit = await asyncio.wait_for(recv_json(ra, wa, expect_masked=False, send_masked=True), 2.0)
            assert submit["request"]["run_id"] == "run_c1"
            run2 = asyncio.create_task(hub.run(second))
            # While a-1 has not confirmed the first claim, the second write for gh#7 is not dispatched anywhere.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(recv_json(ra, wa, expect_masked=False, send_masked=True), 0.3)

            await send_json(
                wa,
                {"type": "run.accepted", "conn_epoch": epoch_a, "run_id": "run_c1", "accepted": False, "reason": "slots_full"},
                mask=True,
            )
            moved = await asyncio.wait_for(recv_json(rb, wb, expect_masked=False, send_masked=True), 2.0)
            assert moved["request"]["run_id"] == "run_c1"
            await send_json(wb, {"type": "run.accepted", "conn_epoch": epoch_b, "run_id": "run_c1", "accepted": True}, mask=True)

            # gh#7 now lives on b-1, which is full: the second write waits for b-1 instead of going to a-1.
            with pytest.raises(NoEligibleRunnerError):
                await asyncio.wait_for(run2, 2.0)
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(recv_json(ra, wa, expect_masked=False, send_masked=True), 0.3)
            assert bound() == "b-1"
            run1.cancel()
        finally:
            wa.close()
            wb.close()
            await hub.stop()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 20.0))
    finally:
        caps_a.close()
        caps_b.close()
        affinity.close()


# ---- review round 11 ----------------------------------------------------------------------------------------------

import threading  # noqa: E402

from runner.protocol import InterruptRequest, InterruptResult, ReclaimRequest, ReclaimResult, RunResult  # noqa: E402
from runner.transport import relay_client  # noqa: E402
from runner.transport.relay_client import RunnerRelayAgent  # noqa: E402


def _git_out(*args: str, cwd: Path) -> str:
    import subprocess

    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def test_hub_returns_the_finished_result_for_a_retried_run_id_instead_of_running_it_again(tmp_path: Path) -> None:
    repo_a, repo_b = tmp_path / "repo-a", tmp_path / "repo-b"
    _init_repo(repo_a)
    _init_repo(repo_b)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    daemon_a = RunnerDaemon(_config(tmp_path, repo_a, runner_id="a-1", max_concurrent=1), drivers=_count_drivers())
    daemon_b = RunnerDaemon(_config(tmp_path, repo_b, runner_id="b-1", max_concurrent=1), drivers=_count_drivers())
    counter = tmp_path / "count"
    req = _req("run_once", executor_id="count", prompt=str(counter), readonly=True)

    async def scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        agents = [
            RunnerRelayAgent(daemon=d, hub_url=f"ws://{host}:{port}/ws/runner", token="tok") for d in (daemon_a, daemon_b)
        ]
        for agent in agents:
            await agent.start()
            await agent.wait_connected()
        try:
            first = await asyncio.wait_for(hub.run(req), 15.0)
            assert first.runner_id == "a-1"
            # a-1 is now full and b-1 idle: a retry of the same run_id must not start the CLI again on b-1.
            assert hub.registry.reserve_slot("a-1")
            again = await asyncio.wait_for(hub.run(req), 15.0)
            assert again.runner_id == "a-1"
            assert counter.read_text(encoding="utf-8").count("x") == 1
        finally:
            for agent in agents:
                await agent.stop()
            await hub.stop()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 40.0))
    finally:
        daemon_a.close()
        daemon_b.close()
        affinity.close()


def test_daemon_checks_caller_and_workspace_before_reusing_a_run_id(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_count_drivers())
    counter = tmp_path / "count"
    original = _req("run_shared_id", executor_id="count", prompt=str(counter), owner_scope="proj:a", work_key="gh#1")
    other_workspace = dataclasses.replace(original, owner_scope="proj:b", work_key="gh#2")

    async def scenario() -> None:
        with _hold_count(counter):
            running = asyncio.create_task(daemon.run(original, caller_id="hibernaut"))
            await _count_started(counter)
            # In flight: another workspace (or another caller) may not join this run.
            joined = await daemon.run(other_workspace, caller_id="hibernaut")
            assert (joined.status, joined.error_kind) == ("refused", "permission_denied")
            assert "run_id" in (joined.error_summary or "")
            stranger = await daemon.run(original, caller_id="workshop")
            assert (stranger.status, stranger.error_kind) == ("refused", "permission_denied")
        done = await running
        assert done.status == "succeeded"
        # Finished: same rules, and the original caller still gets the original result.
        assert (await daemon.run(other_workspace, caller_id="hibernaut")).status == "refused"
        assert (await daemon.run(original, caller_id="workshop")).status == "refused"
        assert await daemon.run(original, caller_id="hibernaut") == done

    try:
        asyncio.run(scenario())
    finally:
        daemon.close()
    assert counter.read_text(encoding="utf-8").count("x") == 1


def test_hub_refuses_a_run_id_reused_for_another_request(tmp_path: Path) -> None:
    from runner.transport.relay_hub import RunIdConflictError

    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    daemon = RunnerDaemon(_config(tmp_path, repo, runner_id="h-1"), drivers=_count_drivers())
    counter = tmp_path / "count"
    original = _req("run_hub_id", executor_id="count", prompt=str(counter), work_key="gh#1")
    conflicting = dataclasses.replace(original, work_key="gh#2")

    async def scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        agent = RunnerRelayAgent(daemon=daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="tok")
        await agent.start()
        await agent.wait_connected()
        try:
            with _hold_count(counter):
                running = asyncio.create_task(hub.run(original))
                await _count_started(counter)
                with pytest.raises(RunIdConflictError):
                    await asyncio.wait_for(hub.run(conflicting), 2.0)
                with pytest.raises(RunIdConflictError):
                    await asyncio.wait_for(hub.run(original, caller_id="workshop"), 2.0)
            done = await asyncio.wait_for(running, 15.0)
            with pytest.raises(RunIdConflictError):
                await asyncio.wait_for(hub.run(conflicting), 2.0)
            assert (await asyncio.wait_for(hub.run(original), 5.0)) == done
        finally:
            await agent.stop()
            await hub.stop()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 40.0))
    finally:
        daemon.close()
        affinity.close()
    assert counter.read_text(encoding="utf-8").count("x") == 1


def test_hub_ignores_replies_from_a_connection_the_request_was_not_sent_on(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    caps_a = RunnerDaemon(_config(tmp_path, repo, runner_id="a-1", max_concurrent=2), drivers=_count_drivers())
    caps_b = RunnerDaemon(_config(tmp_path, repo, runner_id="b-1", max_concurrent=1), drivers=_count_drivers())
    req = _req("run_bound", executor_id="count", prompt="unused", work_key="gh#3")

    def result_of(runner_id: str) -> dict[str, Any]:
        return RunResult(
            run_id=req.run_id,
            runner_id=runner_id,
            status="succeeded",
            executor_id="count",
            started_at="2026-11-23T00:00:00+00:00",
            ended_at="2026-11-23T00:00:01+00:00",
            duration_ms=1000,
        ).to_dict()

    async def scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        ra, wa, epoch_a = await _raw_agent(host, port, caps_a.capabilities_sync().to_dict())
        rb, wb, epoch_b = await _raw_agent(host, port, caps_b.capabilities_sync().to_dict())
        try:
            run = asyncio.create_task(hub.run(req))
            submit = await asyncio.wait_for(recv_json(ra, wa, expect_masked=False, send_masked=True), 2.0)
            assert submit["request"]["run_id"] == req.run_id

            # b-1 answers for a run it was never sent: neither the ack nor the result may land.
            await send_json(wb, {"type": "run.accepted", "conn_epoch": epoch_b, "run_id": req.run_id, "accepted": True}, mask=True)
            await send_json(wb, {"type": "run.completed", "conn_epoch": epoch_b, "result": result_of("b-1")}, mask=True)
            await asyncio.sleep(0.3)
            assert not run.done()

            await send_json(wa, {"type": "run.accepted", "conn_epoch": epoch_a, "run_id": req.run_id, "accepted": True}, mask=True)
            interrupt = asyncio.create_task(hub.interrupt(InterruptRequest(run_id=req.run_id, reason="user_cancel")))
            rpc = await asyncio.wait_for(recv_json(ra, wa, expect_masked=False, send_masked=True), 2.0)
            reply = InterruptResult(run_id=req.run_id, accepted=True, state="cancelling").to_dict()
            await send_json(wb, {"type": "rpc.reply", "conn_epoch": epoch_b, "rpc_id": rpc["rpc_id"], "result": reply}, mask=True)
            await asyncio.sleep(0.3)
            assert not interrupt.done(), "an rpc reply from b-1 answered a request sent to a-1"
            await send_json(wa, {"type": "rpc.reply", "conn_epoch": epoch_a, "rpc_id": rpc["rpc_id"], "result": reply}, mask=True)
            assert (await asyncio.wait_for(interrupt, 2.0)).accepted

            await send_json(wa, {"type": "run.completed", "conn_epoch": epoch_a, "result": result_of("a-1")}, mask=True)
            assert (await asyncio.wait_for(run, 2.0)).runner_id == "a-1"
        finally:
            wa.close()
            wb.close()
            await hub.stop()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 20.0))
    finally:
        caps_a.close()
        caps_b.close()
        affinity.close()


def test_reclaim_holds_the_workspace_until_its_thread_finishes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_count_drivers())
    entered, go = threading.Event(), threading.Event()

    def slow_reclaim(**_: Any) -> ReclaimResult:
        entered.set()
        go.wait(10)
        return ReclaimResult(reclaimed=True, status="reclaimed", reason="test")

    monkeypatch.setattr(daemon.worktrees, "reclaim", slow_reclaim)
    counter = tmp_path / "count"

    async def scenario() -> None:
        reclaim = asyncio.create_task(
            daemon.reclaim_workspace(
                ReclaimRequest(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#1"),
                caller_id="hibernaut",
            )
        )
        assert await asyncio.to_thread(entered.wait, 5)
        # The reclaim checked the workspace first; a run that arrives now must not mount the directory it deletes.
        for readonly in (False, True):
            res = await daemon.run(
                _req(f"run_during_reclaim_{int(readonly)}", executor_id="count", prompt=str(counter), readonly=readonly),
                caller_id="hibernaut",
            )
            assert (res.status, res.error_kind) == ("refused", "rate_limit")
            assert (res.error_summary or "").startswith("workspace_busy")
        go.set()
        assert (await reclaim).reclaimed
        after = await daemon.run(_req("run_after_reclaim", executor_id="count", prompt=str(counter)), caller_id="hibernaut")
        assert after.status == "succeeded"

    try:
        asyncio.run(asyncio.wait_for(scenario(), 30.0))
    finally:
        go.set()
        daemon.close()


def test_cancelled_run_keeps_the_workspace_until_its_worktree_thread_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_count_drivers())
    entered, go = threading.Event(), threading.Event()
    original_resolve = daemon.worktrees.resolve

    def slow_resolve(**kwargs: Any) -> Any:
        entered.set()
        go.wait(10)
        return original_resolve(**kwargs)

    monkeypatch.setattr(daemon.worktrees, "resolve", slow_resolve)
    counter = tmp_path / "count"

    async def scenario() -> None:
        first = asyncio.create_task(daemon.run(_req("run_cut", executor_id="count", prompt=str(counter)), caller_id="hibernaut"))
        assert await asyncio.to_thread(entered.wait, 5)
        first.cancel()  # e.g. the relay connection that carried it went away
        await asyncio.sleep(0.2)
        assert not first.done(), "the run let go of the workspace while its worktree thread was still creating it"
        busy = await daemon.run(_req("run_meanwhile", executor_id="count", prompt=str(counter)), caller_id="hibernaut")
        assert (busy.error_summary or "").startswith("workspace_busy")
        go.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        retry = await daemon.run(_req("run_retry", executor_id="count", prompt=str(counter)), caller_id="hibernaut")
        assert retry.status == "succeeded", retry.error_summary
        # The record and the directory agree after the interrupted creation and the retry.
        cfg = _config(tmp_path, repo)
        again = original_resolve(
            policy=cfg.namespaces["hb"], project=cfg.projects["hibernaut"], owner_scope="proj:hb", work_key="gh#1", readonly=False
        )
        assert again.workdir.is_dir()
        assert str(again.workdir.resolve()) in _git_out("worktree", "list", "--porcelain", cwd=repo.resolve())

    try:
        asyncio.run(asyncio.wait_for(scenario(), 30.0))
    finally:
        go.set()
        daemon.close()


def test_readonly_run_removes_its_detached_worktree_when_it_ends(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    cfg = _config(tmp_path, repo)
    daemon = RunnerDaemon(cfg, drivers=_count_drivers())
    counter = tmp_path / "count"
    ro_dir = daemon.worktrees.readonly_workdir(
        policy=cfg.namespaces["hb"], project=cfg.projects["hibernaut"], owner_scope="proj:hb", work_key="gh#1"
    )
    try:
        res = asyncio.run(daemon.run(_req("run_ro_gone", executor_id="count", prompt=str(counter), readonly=True), caller_id="hibernaut"))
    finally:
        daemon.close()
    assert res.status == "succeeded"
    assert not ro_dir.exists()
    assert str(ro_dir) not in _git_out("worktree", "list", "--porcelain", cwd=repo)


# ---- review round 12 ----------------------------------------------------------------------------------------------

from datetime import datetime, timedelta, timezone  # noqa: E402

from runner.config import NamespacePolicy  # noqa: E402
from runner.transport.in_process import InProcessRunnerClient  # noqa: E402
from tests.test_runner_safety import _drivers  # noqa: E402


def _cwd_drivers() -> dict[str, TemplateCLIDriver]:
    return {"cwd": _drivers()["cwd"]}


def _write_run_dir(res: RunResult) -> Path:
    assert res.status == "succeeded", res.error_summary
    return Path(json.loads(res.stdout_excerpt)["cwd"]).resolve()


def test_readonly_run_never_touches_a_write_workspace_whose_name_matches_its_directory(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    cfg = _config(tmp_path, repo)
    daemon = RunnerDaemon(cfg, drivers=_cwd_drivers())
    ro_dir = daemon.worktrees.readonly_workdir(
        policy=cfg.namespaces["hb"], project=cfg.projects["hibernaut"], owner_scope="proj:hb", work_key="gh#1"
    )
    # A legal write hint that names exactly the directory the readonly run of (proj:hb, gh#1) derives.
    hint = ro_dir.name.removeprefix("hb-")

    async def scenario() -> None:
        write = await daemon.run(
            dataclasses.replace(_req("run_w_clash", executor_id="cwd", owner_scope="proj:x", work_key="gh#9"), name_hint=hint),
            caller_id="hibernaut",
        )
        write_dir = _write_run_dir(write)
        (write_dir / "wip.txt").write_text("uncommitted\n", encoding="utf-8")
        ro = await daemon.run(_req("run_ro_clash", executor_id="cwd", readonly=True), caller_id="hibernaut")
        assert ro.status == "succeeded", ro.error_summary
        assert _write_run_dir(ro) != write_dir
        assert (write_dir / "wip.txt").read_text(encoding="utf-8") == "uncommitted\n"
        daemon.worktrees.assert_writable(
            policy=cfg.namespaces["hb"], project=cfg.projects["hibernaut"], owner_scope="proj:x", work_key="gh#9", workdir=write_dir
        )

    try:
        asyncio.run(scenario())
    finally:
        daemon.close()


def test_readonly_run_refuses_a_directory_that_is_a_registered_write_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_cwd_drivers())

    async def scenario() -> None:
        write_dir = _write_run_dir(
            await daemon.run(_req("run_w_owned", executor_id="cwd", owner_scope="proj:x", work_key="gh#9"), caller_id="hibernaut")
        )
        (write_dir / "wip.txt").write_text("uncommitted\n", encoding="utf-8")
        monkeypatch.setattr(daemon.worktrees, "readonly_workdir", lambda **_: write_dir)
        ro = await daemon.run(_req("run_ro_owned", executor_id="cwd", readonly=True), caller_id="hibernaut")
        assert (ro.status, ro.error_kind) == ("refused", "permission_denied")
        assert write_dir.is_dir()
        assert (write_dir / "wip.txt").read_text(encoding="utf-8") == "uncommitted\n"

    try:
        asyncio.run(scenario())
    finally:
        daemon.close()


def test_reconnecting_node_reports_a_workspace_it_was_still_creating(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_a, repo_b = tmp_path / "repo-a", tmp_path / "repo-b"
    _init_repo(repo_a)
    _init_repo(repo_b)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    daemon_a = RunnerDaemon(_config(tmp_path, repo_a, runner_id="a-1"), drivers=_count_drivers())
    daemon_b = RunnerDaemon(_config(tmp_path, repo_b, runner_id="b-1"), drivers=_count_drivers())
    entered, go = threading.Event(), threading.Event()
    original_resolve = daemon_a.worktrees.resolve

    def slow_resolve(**kwargs: Any) -> Any:
        entered.set()
        go.wait(10)
        return original_resolve(**kwargs)

    monkeypatch.setattr(daemon_a.worktrees, "resolve", slow_resolve)
    announced = []
    ready_keys = daemon_a.worktrees.list_ready_work_keys

    def advertised_keys():
        rows = ready_keys()
        announced.append([row["work_key"] for row in rows])
        return rows

    monkeypatch.setattr(daemon_a.worktrees, "list_ready_work_keys", advertised_keys)
    draining = asyncio.Event()
    cancel_and_wait = relay_client._cancel_and_wait

    async def observe_disconnect(tasks):
        if tasks:
            draining.set()
        await cancel_and_wait(tasks)

    monkeypatch.setattr(relay_client, "_cancel_and_wait", observe_disconnect)
    counter = tmp_path / "count"

    def bound() -> str | None:
        return affinity.lookup(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#5")

    async def scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        url = f"ws://{host}:{port}/ws/runner"
        agent_a = RunnerRelayAgent(daemon=daemon_a, hub_url=url, token="tok", reconnect_delays=(0.01,))
        await agent_a.start()
        await agent_a.wait_connected()
        epoch = agent_a.conn_epoch
        agent_b = RunnerRelayAgent(daemon=daemon_b, hub_url=url, token="tok")
        await agent_b.start()
        await agent_b.wait_connected()
        try:
            first = asyncio.create_task(hub.run(_req("run_creating", executor_id="count", prompt=str(counter), work_key="gh#5")))
            assert await asyncio.to_thread(entered.wait, 5)
            assert bound() == "a-1"
            hub._drop_connection(hub._conns["a-1"], "network drop")  # pyright: ignore[reportPrivateUsage]
            with pytest.raises(RunnerConnectionLostError):
                await asyncio.wait_for(first, 5.0)
            await asyncio.wait_for(draining.wait(), 5)
            assert announced == [[]] and bound() == "a-1"
            go.set()
            await agent_a.wait_connected(10.0)
            assert agent_a.conn_epoch > epoch
            assert announced == [[], ["gh#5"]] and bound() == "a-1"
            second = await asyncio.wait_for(
                hub.run(_req("run_after_drop", executor_id="count", prompt=str(counter), work_key="gh#5")), 15.0
            )
            assert second.runner_id == "a-1"
        finally:
            go.set()
            await agent_a.stop()
            await agent_b.stop()
            await hub.stop()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 40.0))
    finally:
        daemon_a.close()
        daemon_b.close()
        affinity.close()


@pytest.mark.parametrize(
    "change",
    [{"prompt": "another prompt"}, {"interactive": True}, {"executor_session_id": "sess-other"}],
    ids=["prompt", "interactive", "executor_session_id"],
)
def test_daemon_refuses_a_run_id_reused_with_different_execution_fields(tmp_path: Path, change: dict[str, Any]) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_count_drivers())
    counter = tmp_path / "count"
    original = _req("run_fields", executor_id="count", prompt=str(counter))
    changed = dataclasses.replace(original, **change)

    async def scenario() -> None:
        with _hold_count(counter):
            running = asyncio.create_task(daemon.run(original, caller_id="hibernaut"))
            await _count_started(counter)
            joined = await daemon.run(changed, caller_id="hibernaut")
            assert (joined.status, joined.error_kind) == ("refused", "permission_denied")
            assert "run_id" in (joined.error_summary or "")
        done = await running
        assert done.status == "succeeded"
        again = await daemon.run(changed, caller_id="hibernaut")
        assert (again.status, again.error_kind) == ("refused", "permission_denied")
        assert await daemon.run(original, caller_id="hibernaut") == done

    try:
        asyncio.run(scenario())
    finally:
        daemon.close()
    assert counter.read_text(encoding="utf-8").count("x") == 1


def test_hub_refuses_a_run_id_reused_with_a_different_prompt_or_interactive(tmp_path: Path) -> None:
    from runner.transport.relay_hub import RunIdConflictError

    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    daemon = RunnerDaemon(_config(tmp_path, repo, runner_id="h-1"), drivers=_count_drivers())
    counter = tmp_path / "count"
    original = _req("run_hub_fields", executor_id="count", prompt=str(counter))
    variants = [dataclasses.replace(original, prompt="another prompt"), dataclasses.replace(original, interactive=True)]

    async def scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        agent = RunnerRelayAgent(daemon=daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="tok")
        await agent.start()
        await agent.wait_connected()
        try:
            with _hold_count(counter):
                running = asyncio.create_task(hub.run(original))
                await _count_started(counter)
                for variant in variants:
                    with pytest.raises(RunIdConflictError):
                        await asyncio.wait_for(hub.run(variant), 2.0)
            done = await asyncio.wait_for(running, 15.0)
            for variant in variants:
                with pytest.raises(RunIdConflictError):
                    await asyncio.wait_for(hub.run(variant), 2.0)
            assert (await asyncio.wait_for(hub.run(original), 5.0)) == done
        finally:
            await agent.stop()
            await hub.stop()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 40.0))
    finally:
        daemon.close()
        affinity.close()


def test_run_refused_as_workspace_busy_runs_when_retried_after_the_workspace_frees(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_count_drivers())
    counter = tmp_path / "count"

    async def scenario() -> None:
        with _hold_count(counter):
            holder = asyncio.create_task(daemon.run(_req("run_holder", executor_id="count", prompt=str(counter)), caller_id="hibernaut"))
            await _count_started(counter)
            retried = _req("run_retried", executor_id="count", prompt=str(counter))
            busy = await daemon.run(retried, caller_id="hibernaut")
            assert (busy.error_summary or "").startswith("workspace_busy")
            assert not daemon.knows_run(retried.run_id)
        assert (await holder).status == "succeeded"
        again = await daemon.run(retried, caller_id="hibernaut")
        assert again.status == "succeeded", again.error_summary

    try:
        asyncio.run(scenario())
    finally:
        daemon.close()
    assert counter.read_text(encoding="utf-8").count("x") == 2


def _shared_config(tmp_path: Path, repo: Path, runner_id: str = "shared-1") -> Any:
    cfg = _config(tmp_path, repo, runner_id=runner_id)
    return dataclasses.replace(
        cfg, namespaces={"hb": NamespacePolicy(namespace="hb", prefix="hb", allowed_callers=("hibernaut", "workshop"))}
    )


def test_another_caller_cannot_interrupt_or_read_a_run_in_process(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_shared_config(tmp_path, repo), drivers=_count_drivers())
    owner = InProcessRunnerClient(daemon, caller_id="hibernaut", bound_namespace="hb")
    stranger = InProcessRunnerClient(daemon, caller_id="workshop", bound_namespace="hb")
    counter = tmp_path / "count"
    req = _req("run_owned", executor_id="count", prompt=str(counter))

    async def scenario() -> None:
        with _hold_count(counter):
            running = asyncio.create_task(owner.run(req))
            await _count_started(counter)
            res = await stranger.interrupt(InterruptRequest(run_id=req.run_id, reason="user_cancel"))
            assert (res.accepted, res.state) == (False, "not_found")
        done = await running
        assert done.status == "succeeded"
        assert daemon.query_run(req.run_id, caller_id="workshop") is None
        assert daemon.query_run(req.run_id, caller_id="hibernaut") == done

    try:
        asyncio.run(scenario())
    finally:
        daemon.close()


def test_another_caller_cannot_interrupt_or_read_a_run_through_the_relay(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    daemon = RunnerDaemon(_shared_config(tmp_path, repo, runner_id="s-1"), drivers=_count_drivers())
    counter = tmp_path / "count"
    req = _req("run_relay_owned", executor_id="count", prompt=str(counter))

    async def scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        agent = RunnerRelayAgent(daemon=daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="tok")
        await agent.start()
        await agent.wait_connected()
        try:
            with _hold_count(counter):
                running = asyncio.create_task(hub.run(req, caller_id="hibernaut"))
                await _count_started(counter)
                res = await hub.interrupt(InterruptRequest(run_id=req.run_id, reason="user_cancel"), caller_id="workshop")
                assert (res.accepted, res.state) == (False, "not_found")
            done = await asyncio.wait_for(running, 15.0)
            assert done.status == "succeeded"
            assert await hub.query_run("s-1", req.run_id, caller_id="workshop") is None
            assert await hub.query_run("s-1", req.run_id, caller_id="hibernaut") == done
        finally:
            await agent.stop()
            await hub.stop()

    try:
        asyncio.run(asyncio.wait_for(scenario(), 40.0))
    finally:
        daemon.close()
        affinity.close()


def test_no_process_is_left_when_the_run_log_cannot_be_opened(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    logs = tmp_path / "logs"
    now = datetime.now(timezone.utc)
    for day in (now, now + timedelta(days=1)):
        (logs / day.strftime("%Y-%m-%d") / "run_nolog.log").mkdir(parents=True)  # opening it for writing fails
    supervisor = ProcessSupervisor(runner_id="r", logs_dir=logs)

    async def unexpected_spawn(*args, **kwargs):
        pytest.fail("the supervisor started a child before it could open the run log")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", unexpected_spawn)

    async def scenario() -> None:
        with pytest.raises(OSError):
            await supervisor.execute(
                run_id="run_nolog",
                argv=[PY, "-c", "pass"],
                cwd=tmp_path,
                env={"PATH": "/usr/bin:/bin"},
                stdin_text=None,
                timeout_seconds=60.0,
                idle_timeout_seconds=None,
                cancel_event=asyncio.Event(),
                grace_seconds=0.5,
            )

    asyncio.run(scenario())


def test_output_line_longer_than_the_stream_limit_is_read_in_full(tmp_path: Path) -> None:
    supervisor = ProcessSupervisor(runner_id="r", logs_dir=tmp_path / "logs")
    lines: list[str] = []
    size = 300_000

    async def scenario() -> Any:
        return await supervisor.execute(
            run_id="run_long_line",
            argv=[PY, "-c", f"import sys; sys.stdout.write('a' * {size} + '\\n'); sys.stdout.write('tail\\n')"],
            cwd=tmp_path,
            env={"PATH": "/usr/bin:/bin"},
            stdin_text=None,
            timeout_seconds=10.0,
            idle_timeout_seconds=None,
            cancel_event=asyncio.Event(),
            grace_seconds=0.5,
            on_stdout_line=lines.append,
        )

    out = asyncio.run(scenario())
    assert (out.status, out.exit_code) == ("succeeded", 0)
    assert out.log_ref.bytes == size + 1 + len("tail\n")
    assert [len(line) for line in lines] == [size + 1, len("tail\n")]
    assert out.stdout_excerpt.startswith("a" * 1000)


def test_a_failed_output_reader_fails_the_run(tmp_path: Path) -> None:
    supervisor = ProcessSupervisor(runner_id="r", logs_dir=tmp_path / "logs")

    def broken(_: str) -> None:
        raise RuntimeError("stream consumer broke")

    async def scenario() -> Any:
        return await supervisor.execute(
            run_id="run_reader_broke",
            argv=[PY, "-c", "print('hello')"],
            cwd=tmp_path,
            env={"PATH": "/usr/bin:/bin"},
            stdin_text=None,
            timeout_seconds=10.0,
            idle_timeout_seconds=None,
            cancel_event=asyncio.Event(),
            grace_seconds=0.5,
            on_stdout_line=broken,
        )

    out = asyncio.run(scenario())
    assert out.status == "failed"
    assert "output" in out.stderr_excerpt
