from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Sequence

import pytest

from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.core.models import Event, PermissionDecision, SessionRef
from runner.cli_drivers import TemplateCLIDriver
from runner.config import NamespacePolicy, ProjectCheckout, RunnerConfig
from runner.daemon import RunnerDaemon
from runner.protocol import ReclaimRequest, RunRequest, RunResult
from runner.registry import WorkspaceAffinityStore
from runner.transport.in_process import InProcessRunnerClient
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport import relay_hub as relay_hub_module
from runner.transport.relay_hub import RunnerRelayHub
from runner.transport.ws_protocol import (
    OP_CLOSE,
    WSConnectionClosed,
    WSProtocolError,
    client_handshake,
    encode_frame,
    recv_json,
    send_json,
    server_handshake,
)
from runner.worktree import CommandOutput, CreatedWorktree, GitWorktreeBackend, OrcaWorktreeBackend, WorktreeManager

PY = sys.executable

# Leader writes "<leader_pid> <grandchild_pid>" to the path in argv[1]; the grandchild ignores SIGTERM.
_SPAWN_SCRIPT = (
    "import os, subprocess, sys, time\n"
    "child = subprocess.Popen([sys.executable, '-c', "
    "'import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)'])\n"
    "time.sleep(0.2)\n"
    "open(sys.argv[1] + '.tmp', 'w').write(f'{os.getpid()} {child.pid}')\n"
    "os.replace(sys.argv[1] + '.tmp', sys.argv[1])\n"
    "time.sleep(60)\n"
)


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def _init_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git("init", "-b", "main", cwd=repo)
    _git("config", "user.email", "test@example.com", cwd=repo)
    _git("config", "user.name", "Test", cwd=repo)
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    _git("add", "README.md", cwd=repo)
    _git("commit", "-m", "init", cwd=repo)
    bare = repo.parent / f"{repo.name}-remote.git"
    subprocess.run(["git", "init", "--bare", str(bare)], check=True, capture_output=True)
    _git("remote", "add", "origin", str(bare), cwd=repo)
    _git("push", "-u", "origin", "main", cwd=repo)


def _config(tmp_path: Path, repo: Path, runner_id: str = "runner-safety", max_concurrent: int = 4) -> RunnerConfig:
    return RunnerConfig(
        runner_id=runner_id,
        max_concurrent=max_concurrent,
        worktree_backend="git",
        interactive_surface="none",
        namespaces={"hb": NamespacePolicy(namespace="hb", prefix="hb", allowed_callers=("hibernaut",))},
        projects={"hibernaut": ProjectCheckout(project_key="hibernaut", checkout_path=repo, default_base_ref="origin/main")},
        state_dir=tmp_path / f"state-{runner_id}",
        logs_dir=tmp_path / f"logs-{runner_id}",
    )


def _drivers() -> dict[str, TemplateCLIDriver]:
    return {
        "spawn": TemplateCLIDriver(executor_id="spawn", driver_kind="custom", command=PY, args=("-c", _SPAWN_SCRIPT, "{prompt}")),
        "cwd": TemplateCLIDriver(
            executor_id="cwd",
            driver_kind="custom",
            command=PY,
            args=("-c", "import json, os; print(json.dumps({'cwd': os.getcwd()}))"),
        ),
        "look": TemplateCLIDriver(
            executor_id="look",
            driver_kind="custom",
            command=PY,
            args=(
                "-c",
                "import json, os, pathlib, subprocess; print(json.dumps({'cwd': os.getcwd(), "
                "'mutated': pathlib.Path('mutated.txt').exists(), 'readme': pathlib.Path('README.md').read_text(), "
                "'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()}))",
            ),
        ),
        "mutate": TemplateCLIDriver(
            executor_id="mutate",
            driver_kind="custom",
            command=PY,
            args=("-c", "from pathlib import Path; Path('mutated.txt').write_text('oops'); Path('README.md').write_text('changed')"),
        ),
    }


def _req(run_id: str, *, executor_id: str, prompt: str = "", work_key: str = "gh#1", owner_scope: str = "proj:hb", readonly: bool = False) -> RunRequest:
    return RunRequest(
        run_id=run_id,
        namespace="hb",
        project_key="hibernaut",
        owner_scope=owner_scope,
        work_key=work_key,
        executor_id=executor_id,
        readonly=readonly,
        prompt=prompt,
        timeout_seconds=60.0,
    )


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_dead(pid: int, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return not _alive(pid)


async def _wait_dead_async(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while _alive(pid) and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    return not _alive(pid)


async def _wait_pids(pidfile: Path, timeout: float = 10.0) -> tuple[int, int]:
    deadline = time.monotonic() + timeout
    while not pidfile.exists():
        if time.monotonic() > deadline:
            raise AssertionError(f"child never wrote {pidfile}")
        await asyncio.sleep(0.05)
    leader, grandchild = pidfile.read_text(encoding="utf-8").split()
    return int(leader), int(grandchild)


def _kill_quietly(*pids: int) -> None:
    for pid in pids:
        try:
            os.kill(pid, 9)
        except ProcessLookupError:
            pass


def test_cancelled_run_reaps_process_group_and_keeps_workspace_busy_until_reaped(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_drivers())
    pidfile = tmp_path / "pids-cancel"
    pids: list[int] = []

    async def _scenario() -> None:
        task = asyncio.create_task(daemon.run(_req("run_cancel_1", executor_id="spawn", prompt=str(pidfile)), caller_id="hibernaut"))
        pids.extend(await _wait_pids(pidfile))

        # While run_cancel_1 owns the worktree, a second run_id for the same workspace must not start a second writer.
        busy = await daemon.run(_req("run_cancel_2", executor_id="cwd"), caller_id="hibernaut")
        assert busy.status == "refused"
        assert busy.error_summary is not None and "workspace_busy" in busy.error_summary

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        leader, grandchild = pids
        assert not _alive(leader), "cancelled run left its CLI process running"
        assert _wait_dead(grandchild), "cancelled run left a process of its group running"
        assert daemon.capabilities_sync().slots.active_runs == 0
        cancelled = daemon.query_run("run_cancel_1")
        assert cancelled is not None and cancelled.status == "interrupted"

        after = await daemon.run(_req("run_cancel_3", executor_id="cwd"), caller_id="hibernaut")
        assert after.status == "succeeded"

    try:
        asyncio.run(_scenario())
    finally:
        _kill_quietly(*pids)
        daemon.close()


def test_bridge_cancelled_by_worker_interrupts_run_and_waits_for_reap(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_drivers())
    client = InProcessRunnerClient(daemon, caller_id="hibernaut", bound_namespace="hb")
    bridge = RunnerExecutorBridge(runner=client, executor_id="spawn", project_key="hibernaut", executor_calls_tools=False)
    pidfile = tmp_path / "pids-bridge"
    pids: list[int] = []

    class _Caps:
        memory: Any = None
        timers: Any = None
        permissions: Any = PermissionDecision(action="allow")

        def poll_folded_events(self) -> list[Event]:
            return []

        def is_interrupted(self) -> bool:
            return False

    ev = Event(inbox_id="proj:hibernaut", source="github", external_id="e1", payload={"body": str(pidfile)})
    sess = SessionRef(session_id="sess_1", inbox_id="proj:hibernaut", correlation_key="github:kun-g/hibernaut#5")

    async def _scenario() -> None:
        task = asyncio.create_task(bridge.execute([ev], sess, _Caps()))  # type: ignore[arg-type]
        pids.extend(await _wait_pids(pidfile))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        leader, grandchild = pids
        assert not _alive(leader), "bridge returned while the runner's CLI process was still running"
        assert _wait_dead(grandchild)
        assert daemon.capabilities_sync().slots.active_runs == 0

    try:
        asyncio.run(_scenario())
    finally:
        _kill_quietly(*pids)
        daemon.close()


def test_readonly_runs_use_own_worktree_and_restore_mutations(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_drivers())
    primary = repo.resolve()

    async def _look(run_id: str, **kw: Any) -> dict[str, Any]:
        res = await daemon.run(_req(run_id, executor_id="look", readonly=True, **kw), caller_id="hibernaut")
        assert res.status == "succeeded", res.error_summary
        seen: dict[str, Any] = json.loads(res.stdout_excerpt)
        # The readonly worktree lives only for its run.
        assert not Path(seen["cwd"]).exists()
        return seen

    async def _scenario() -> None:
        ro_a = await _look("run_ro_a", owner_scope="proj:a")
        ro_b = await _look("run_ro_b", owner_scope="proj:b")
        cwd_a, cwd_b = Path(ro_a["cwd"]).resolve(), Path(ro_b["cwd"]).resolve()
        assert cwd_a != primary and cwd_b != primary
        assert cwd_a != cwd_b, "two inbox_ids mounted the same directory"

        mutated = await daemon.run(_req("run_ro_mut", executor_id="mutate", owner_scope="proj:a", readonly=True), caller_id="hibernaut")
        assert mutated.status == "failed" and mutated.error_kind == "permission_denied"
        assert not (primary / "mutated.txt").exists()
        assert _git("status", "--porcelain", "--untracked-files=all", cwd=primary) == ""
        # The next readonly run of the same tuple starts clean.
        again = await _look("run_ro_again", owner_scope="proj:a")
        assert again["mutated"] is False and again["readme"] == "hello\n"

        # A readonly run of a task that has a write worktree sees that worktree's committed HEAD, in its own directory.
        write = await daemon.run(_req("run_w", executor_id="cwd", owner_scope="proj:a", work_key="gh#7"), caller_id="hibernaut")
        write_dir = Path(json.loads(write.stdout_excerpt)["cwd"]).resolve()
        (write_dir / "feature.txt").write_text("work\n", encoding="utf-8")
        _git("add", "feature.txt", cwd=write_dir)
        _git("commit", "-m", "feature", cwd=write_dir)
        ro_task = await _look("run_ro_task", owner_scope="proj:a", work_key="gh#7")
        assert Path(ro_task["cwd"]).resolve() not in (write_dir, primary)
        assert ro_task["head"] == _git("rev-parse", "HEAD", cwd=write_dir)

    try:
        asyncio.run(_scenario())
    finally:
        daemon.close()


def test_superseded_connection_fails_inflight_runs_and_releases_slot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Without wait_closed the old handler only sees EOF after the hub has swapped in the new connection, which is
    # the ordering in which the old handler's cleanup used to skip the in-flight runs.
    async def _close_without_waiting(writer: asyncio.StreamWriter, code: int = 1000, reason: str = "", *, mask: bool) -> None:
        writer.write(encode_frame(OP_CLOSE, code.to_bytes(2, "big") + reason.encode("utf-8"), mask=mask))
        writer.close()

    monkeypatch.setattr(relay_hub_module, "send_close", _close_without_waiting)
    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    daemon = RunnerDaemon(_config(tmp_path, repo, runner_id="mac-1"), drivers=_drivers())
    pidfile = tmp_path / "pids-relay"
    pids: list[int] = []

    async def _scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        url = f"ws://{host}:{port}/ws/runner"
        old_agent = RunnerRelayAgent(daemon=daemon, hub_url=url, token="tok")
        await old_agent.start()
        await old_agent.wait_connected()

        run_task = asyncio.create_task(hub.run(_req("run_relay_1", executor_id="spawn", prompt=str(pidfile))))
        pids.extend(await _wait_pids(pidfile))

        new_agent = RunnerRelayAgent(daemon=daemon, hub_url=url, token="tok")
        await new_agent.start()
        await new_agent.wait_connected()
        await old_agent.wait_superseded(timeout=3.0)

        with pytest.raises(ConnectionError):
            await asyncio.wait_for(run_task, timeout=5.0)
        rec = hub.registry.get("mac-1")
        assert rec is not None and rec.reserved_slots == 0
        leader, grandchild = pids
        # The old agent cancels the run when its connection goes; the reap runs in the background on this loop.
        assert await _wait_dead_async(leader) and await _wait_dead_async(grandchild)
        for _ in range(100):
            if daemon.capabilities_sync().slots.active_runs == 0:
                break
            await asyncio.sleep(0.05)
        assert daemon.capabilities_sync().slots.active_runs == 0

        await new_agent.stop()
        await hub.stop()

    try:
        asyncio.run(_scenario())
    finally:
        _kill_quietly(*pids)
        daemon.close()
        affinity.close()


def test_orca_backend_uses_receipt_path_and_branch(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    primary = repo.resolve()
    orca_home = (tmp_path / "orca-worktrees").resolve()
    calls: list[list[str]] = []

    def _fake_orca(cmd: Sequence[str], cwd: Path) -> CommandOutput:
        argv = list(cmd)
        if argv[0] != "orca":
            proc = subprocess.run(argv, cwd=str(cwd), capture_output=True, text=True, check=False)
            return CommandOutput(returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)
        calls.append(argv)
        if argv[1:3] == ["worktree", "create"]:
            name = argv[argv.index("--name") + 1]
            branch = argv[argv.index("--branch") + 1]
            base = argv[argv.index("--base-branch") + 1]
            path = orca_home / name
            _git("worktree", "add", "-b", branch, str(path), base, cwd=primary)
            receipt = {"result": {"worktree": {"branch": f"refs/heads/{branch}", "path": str(path)}}}
            return CommandOutput(returncode=0, stdout=json.dumps(receipt), stderr="")
        if argv[1:3] == ["worktree", "rm"]:
            target = argv[argv.index("--worktree") + 1].removeprefix("path:")
            _git("worktree", "remove", target, cwd=primary)
            return CommandOutput(returncode=0, stdout="{}", stderr="")
        raise AssertionError(f"unexpected orca call {argv}")

    mgr = WorktreeManager(db_path=tmp_path / "ws.db", backend=OrcaWorktreeBackend(run_cmd=_fake_orca))
    policy = NamespacePolicy(namespace="hb", prefix="hb", allowed_callers=("hibernaut",))
    project = ProjectCheckout(project_key="hibernaut", checkout_path=repo, default_base_ref="origin/main")
    try:
        resolved = mgr.resolve(policy=policy, project=project, owner_scope="proj:hb", work_key="gh#3", readonly=False, name_hint="issue-3")
        create = calls[0]
        assert create[create.index("--repo") + 1] == f"path:{primary}"
        assert create[create.index("--base-branch") + 1] == _git("rev-parse", "origin/main", cwd=primary)
        assert "--no-parent" in create
        assert create[create.index("--setup") + 1] == "skip"
        assert resolved.workdir == orca_home / "hb-issue-3"
        assert resolved.branch == "hb/issue-3"
        rec = mgr.get_record(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#3")
        assert rec is not None and rec.workdir == orca_home / "hb-issue-3"
        mgr.assert_writable(policy=policy, project=project, owner_scope="proj:hb", work_key="gh#3", workdir=resolved.workdir)

        result = mgr.reclaim(policy=policy, project=project, owner_scope="proj:hb", work_key="gh#3")
        assert result.reclaimed is True
        assert calls[-1][calls[-1].index("--worktree") + 1] == f"path:{orca_home / 'hb-issue-3'}"
        assert not (orca_home / "hb-issue-3").exists()
    finally:
        mgr.close()


@pytest.mark.parametrize(
    "receipt",
    [
        {"result": {"worktree": {"branch": "refs/heads/hb/issue-4", "path": "relative/hb-issue-4"}}},
        {"result": {"worktree": {"branch": "refs/heads/other", "path": "/tmp/hb-issue-4"}}},
        {"result": {}},
    ],
)
def test_orca_backend_rejects_bad_receipt(tmp_path: Path, receipt: dict[str, Any]) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)

    def _fake_orca(cmd: Sequence[str], cwd: Path) -> CommandOutput:
        if cmd[0] != "orca":
            proc = subprocess.run(list(cmd), cwd=str(cwd), capture_output=True, text=True, check=False)
            return CommandOutput(returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)
        return CommandOutput(returncode=0, stdout=json.dumps(receipt), stderr="")

    mgr = WorktreeManager(db_path=tmp_path / "ws.db", backend=OrcaWorktreeBackend(run_cmd=_fake_orca))
    policy = NamespacePolicy(namespace="hb", prefix="hb", allowed_callers=("hibernaut",))
    project = ProjectCheckout(project_key="hibernaut", checkout_path=repo, default_base_ref="origin/main")
    try:
        with pytest.raises(RuntimeError):
            mgr.resolve(policy=policy, project=project, owner_scope="proj:hb", work_key="gh#4", readonly=False, name_hint="issue-4")
        assert mgr.get_record(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#4") is None
    finally:
        mgr.close()


def test_interrupt_retries_until_run_is_registered(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers=_drivers())
    client = InProcessRunnerClient(daemon, caller_id="hibernaut", bound_namespace="hb")
    pidfile = tmp_path / "pids-flag"
    pids: list[int] = []
    flag = {"on": False}

    class _Caps:
        memory: Any = None
        timers: Any = None
        permissions: Any = PermissionDecision(action="allow")

        def poll_folded_events(self) -> list[Event]:
            return []

        def is_interrupted(self) -> bool:
            return flag["on"]

    bridge = RunnerExecutorBridge(runner=client, executor_id="spawn", project_key="hibernaut", executor_calls_tools=False)
    ev = Event(inbox_id="proj:hibernaut", source="github", external_id="e2", payload={"body": str(pidfile)})
    sess = SessionRef(session_id="sess_2", inbox_id="proj:hibernaut", correlation_key="github:kun-g/hibernaut#6")

    async def _scenario() -> None:
        # The interrupt flag is already set before the runner has registered the run: the bridge keeps asking.
        flag["on"] = True
        task = asyncio.create_task(bridge.execute([ev], sess, _Caps()))  # type: ignore[arg-type]
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=15.0)
        assert daemon.capabilities_sync().slots.active_runs == 0
        if pidfile.exists():
            pids.extend(int(p) for p in pidfile.read_text(encoding="utf-8").split())
            assert all(_wait_dead(p) for p in pids)

    try:
        asyncio.run(_scenario())
    finally:
        _kill_quietly(*pids)
        daemon.close()


# Appends one line per execution to the path in argv[1], then reports success after a short delay.
_COUNT_SCRIPT = (
    "import json, sys, time\n"
    "open(sys.argv[1], 'a').write('x\\n')\n"
    "time.sleep(1.0)\n"
    "print(json.dumps({'outcome': 'emit', 'message': 'ok'}))\n"
)


def _count_drivers() -> dict[str, TemplateCLIDriver]:
    return {"count": TemplateCLIDriver(executor_id="count", driver_kind="custom", command=PY, args=("-c", _COUNT_SCRIPT, "{prompt}"))}


def test_agent_accepts_duplicate_submit_of_inflight_run_when_slots_full(tmp_path: Path) -> None:
    # A plain WebSocket server stands in for the hub so the agent sees the same run.submit twice.
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo, runner_id="full-1", max_concurrent=1), drivers=_count_drivers())
    counter = tmp_path / "count-agent"
    req = _req("run_dup_1", executor_id="count", prompt=str(counter))

    async def _scenario() -> None:
        frames: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        ready = asyncio.Event()
        conn: dict[str, asyncio.StreamWriter] = {}

        async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await server_handshake(reader, writer, expected_path="/ws/runner")
            await recv_json(reader, writer, expect_masked=True, send_masked=False)
            await send_json(writer, {"type": "welcome", "runner_id": "full-1", "conn_epoch": 1, "heartbeat_sec": 30}, mask=False)
            conn["w"] = writer
            ready.set()
            try:
                while True:
                    await frames.put(await recv_json(reader, writer, expect_masked=True, send_masked=False))
            except (WSConnectionClosed, WSProtocolError, OSError, asyncio.IncompleteReadError):
                pass
            finally:
                writer.close()

        server = await asyncio.start_server(_handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        agent = RunnerRelayAgent(daemon=daemon, hub_url=f"ws://127.0.0.1:{port}/ws/runner", token="tok")
        await agent.start()
        await asyncio.wait_for(ready.wait(), 5.0)
        submit = {"type": "run.submit", "conn_epoch": 1, "caller_id": "hibernaut", "request": req.to_dict()}

        await send_json(conn["w"], submit, mask=False)
        first = await asyncio.wait_for(frames.get(), 5.0)
        assert first["type"] == "run.accepted" and first["accepted"] is True
        while not counter.exists():
            await asyncio.sleep(0.02)
        assert daemon.capabilities_sync().slots.available == 0

        await send_json(conn["w"], submit, mask=False)
        second = await asyncio.wait_for(frames.get(), 5.0)
        assert second["type"] == "run.accepted"
        assert second["accepted"] is True, f"in-flight run_id rejected: {second.get('reason')}"

        completed = [await asyncio.wait_for(frames.get(), 10.0) for _ in range(2)]
        assert [f["type"] for f in completed] == ["run.completed", "run.completed"]
        assert {f["result"]["status"] for f in completed} == {"succeeded"}
        assert counter.read_text(encoding="utf-8") == "x\n", "the duplicate submit started a second execution"

        await agent.stop()
        server.close()
        await server.wait_closed()

    try:
        asyncio.run(_scenario())
    finally:
        daemon.close()


def test_hub_keeps_waiting_after_ack_timeout(tmp_path: Path) -> None:
    # A raw agent acknowledges only after the hub's ack timeout; the hub must still return the result.
    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0, ack_timeout_seconds=0.2)
    caps_daemon = RunnerDaemon(_config(tmp_path, repo, runner_id="late-1"), drivers=_count_drivers())
    req = _req("run_late_ack", executor_id="count", prompt="unused", readonly=True)

    async def _scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        reader, writer = await asyncio.open_connection(host, port)
        await client_handshake(reader, writer, host=f"{host}:{port}", path="/ws/runner")
        await send_json(
            writer,
            {"type": "hello", "token": "tok", "capabilities": caps_daemon.capabilities_sync().to_dict(), "ready_work_keys": []},
            mask=True,
        )
        welcome = await recv_json(reader, writer, expect_masked=False, send_masked=True)
        epoch = welcome["conn_epoch"]

        run_task = asyncio.create_task(hub.run(req))
        submit = await recv_json(reader, writer, expect_masked=False, send_masked=True)
        assert submit["type"] == "run.submit"
        await asyncio.sleep(0.6)
        await send_json(writer, {"type": "run.accepted", "conn_epoch": epoch, "run_id": req.run_id, "accepted": True}, mask=True)
        result = RunResult(
            run_id=req.run_id,
            runner_id="late-1",
            status="succeeded",
            executor_id="count",
            started_at="2026-11-23T00:00:00+00:00",
            ended_at="2026-11-23T00:00:01+00:00",
            duration_ms=1000,
        )
        await send_json(writer, {"type": "run.completed", "conn_epoch": epoch, "result": result.to_dict()}, mask=True)

        got = await asyncio.wait_for(run_task, 5.0)
        assert got.status == "succeeded"
        rec = hub.registry.get("late-1")
        assert rec is not None and rec.reserved_slots == 0

        writer.close()
        await hub.stop()

    try:
        asyncio.run(_scenario())
    finally:
        caps_daemon.close()
        affinity.close()


def test_hub_overlapping_waiters_on_one_run_id_share_one_submission(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="tok", affinity=affinity, heartbeat_seconds=30.0)
    daemon = RunnerDaemon(_config(tmp_path, repo, runner_id="share-1", max_concurrent=2), drivers=_count_drivers())
    counter = tmp_path / "count-hub"
    req = _req("run_shared", executor_id="count", prompt=str(counter))

    async def _scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        agent = RunnerRelayAgent(daemon=daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="tok")
        await agent.start()
        await agent.wait_connected()

        impatient = asyncio.create_task(asyncio.wait_for(hub.run(req), 0.4))
        await asyncio.sleep(0.1)
        patient = asyncio.create_task(hub.run(req))
        await asyncio.sleep(0.1)
        rec = hub.registry.get("share-1")
        assert rec is not None and rec.reserved_slots == 1, "the second waiter reserved another slot"

        with pytest.raises(asyncio.TimeoutError):
            await impatient
        got = await asyncio.wait_for(patient, 10.0)
        assert got.status == "succeeded"
        assert counter.read_text(encoding="utf-8") == "x\n"
        assert rec.reserved_slots == 0

        await agent.stop()
        await hub.stop()

    try:
        asyncio.run(_scenario())
    finally:
        daemon.close()
        affinity.close()


def test_worktree_commands_do_not_block_the_event_loop(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)

    class _SlowBackend(GitWorktreeBackend):
        def create(self, *, primary_checkout: Path, workdir: Path, branch: str, base_ref: str) -> CreatedWorktree:
            time.sleep(1.0)  # a slow `orca worktree create` / `git worktree add`
            return super().create(primary_checkout=primary_checkout, workdir=workdir, branch=branch, base_ref=base_ref)

    cfg = _config(tmp_path, repo)
    mgr = WorktreeManager(db_path=cfg.state_dir / "runner_workspaces.db", backend=_SlowBackend())
    daemon = RunnerDaemon(cfg, drivers=_drivers(), worktree_manager=mgr)

    async def _scenario() -> None:
        gaps: list[float] = []
        stop = asyncio.Event()

        async def _ticker() -> None:
            last = time.monotonic()
            while not stop.is_set():
                await asyncio.sleep(0.02)
                now = time.monotonic()
                gaps.append(now - last)
                last = now

        ticker = asyncio.create_task(_ticker())
        await asyncio.sleep(0.1)
        res = await daemon.run(_req("run_slow_create", executor_id="cwd", work_key="gh#11"), caller_id="hibernaut")
        stop.set()
        await ticker
        assert res.status == "succeeded"
        assert max(gaps) < 0.5, f"event loop stalled for {max(gaps):.2f}s during worktree creation"

    try:
        asyncio.run(_scenario())
    finally:
        daemon.close()


def test_concurrent_resolves_with_same_hint_get_distinct_worktrees(tmp_path: Path) -> None:
    # Resolves now run in worker threads, so two work_keys can allocate names while the other is still creating.
    repo = tmp_path / "repo"
    _init_repo(repo)

    class _SlowBackend(GitWorktreeBackend):
        def create(self, *, primary_checkout: Path, workdir: Path, branch: str, base_ref: str) -> CreatedWorktree:
            time.sleep(0.5)
            return super().create(primary_checkout=primary_checkout, workdir=workdir, branch=branch, base_ref=base_ref)

    cfg = _config(tmp_path, repo)
    mgr = WorktreeManager(db_path=cfg.state_dir / "runner_workspaces.db", backend=_SlowBackend())
    daemon = RunnerDaemon(cfg, drivers=_drivers(), worktree_manager=mgr)

    def _with_hint(run_id: str, work_key: str) -> RunRequest:
        return dataclasses.replace(_req(run_id, executor_id="cwd", work_key=work_key), name_hint="same-hint")

    async def _scenario() -> None:
        a, b = await asyncio.gather(
            daemon.run(_with_hint("run_hint_a", "gh#21"), caller_id="hibernaut"),
            daemon.run(_with_hint("run_hint_b", "gh#22"), caller_id="hibernaut"),
        )
        assert a.status == "succeeded" and b.status == "succeeded", (a.error_summary, b.error_summary)
        dirs = {Path(json.loads(r.stdout_excerpt)["cwd"]).resolve().name for r in (a, b)}
        assert dirs == {"hb-same-hint", "hb-same-hint-2"}

    try:
        asyncio.run(_scenario())
    finally:
        daemon.close()
