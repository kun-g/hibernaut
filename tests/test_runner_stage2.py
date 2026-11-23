from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

from runner.cli_drivers import TemplateCLIDriver
from runner.config import NamespacePolicy, ProjectCheckout, RunnerConfig
from runner.daemon import RunnerDaemon
from runner.protocol import InterruptRequest, ReclaimRequest, RunRequest
from runner.registry import NodeAffinityOfflineError, WorkspaceAffinityStore
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport.relay_hub import RunnerRelayHub


def _init_git_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True, capture_output=True)
    bare = repo.parent / f"{repo.name}-remote.git"
    subprocess.run(["git", "init", "--bare", str(bare)], check=True, capture_output=True)
    subprocess.run(["git", "remote", "add", "origin", str(bare)], cwd=repo, check=True)
    subprocess.run(["git", "push", "-u", "origin", "main"], cwd=repo, check=True, capture_output=True)


def _node_daemon(tmp_path: Path, runner_id: str, repo: Path, *, max_concurrent: int = 1) -> RunnerDaemon:
    cfg = RunnerConfig(
        runner_id=runner_id,
        max_concurrent=max_concurrent,
        worktree_backend="git",
        interactive_surface="none",
        namespaces={
            "hb": NamespacePolicy(namespace="hb", prefix="hb", allowed_callers=("hibernaut",)),
            "ws": NamespacePolicy(namespace="ws", prefix="ws", allowed_callers=("workshop",)),
        },
        projects={"hibernaut": ProjectCheckout(project_key="hibernaut", checkout_path=repo)},
        state_dir=tmp_path / f"state_{runner_id}",
        logs_dir=tmp_path / f"logs_{runner_id}",
    )
    py = sys.executable
    drivers = {
        "claude": TemplateCLIDriver(
            executor_id="claude",
            driver_kind="custom",
            command=py,
            args=(
                "-c",
                f"import json, sys; print(json.dumps({{'node': '{runner_id}', 'prompt': sys.argv[1], 'session_id': 'sid1'}}))",
                "{prompt}",
            ),
        ),
        "slow": TemplateCLIDriver(
            executor_id="slow",
            driver_kind="custom",
            command=py,
            args=("-c", "import time; time.sleep(10)"),
        ),
    }
    return RunnerDaemon(cfg, drivers=drivers)


def test_stage2_multinode_affinity_offline_drift_interrupt_reclaim_and_epoch(tmp_path: Path) -> None:
    repo_mac = tmp_path / "repo_mac"
    repo_vps = tmp_path / "repo_vps"
    _init_git_repo(repo_mac)
    _init_git_repo(repo_vps)

    affinity = WorkspaceAffinityStore(tmp_path / "hub_affinity.db")
    hub = RunnerRelayHub(shared_token="secret-relay-token", affinity=affinity, heartbeat_seconds=0.5)
    mac_daemon = _node_daemon(tmp_path, "mac-m4", repo_mac, max_concurrent=1)
    vps_daemon = _node_daemon(tmp_path, "vps-01", repo_vps, max_concurrent=2)

    async def _scenario() -> None:
        host, port = await hub.start("127.0.0.1", 0)
        url = f"ws://{host}:{port}/ws/runner"

        mac_agent = RunnerRelayAgent(daemon=mac_daemon, hub_url=url, token="secret-relay-token")
        vps_agent = RunnerRelayAgent(daemon=vps_daemon, hub_url=url, token="secret-relay-token")
        await mac_agent.start()
        await vps_agent.start()
        await mac_agent.wait_connected()
        await vps_agent.wait_connected()
        assert mac_agent.conn_epoch == 1

        # 1. Unpinned write run picks vps-01 (higher available slots: 2 vs 1) and binds affinity in SQLite
        res1 = await hub.run(
            RunRequest(
                run_id="run_s2_1",
                namespace="hb",
                project_key="hibernaut",
                owner_scope="proj:hb",
                work_key="gh#42",
                name_hint="issue-42",
                executor_id="claude",
                readonly=False,
                prompt="hello-s2",
            )
        )
        assert res1.status == "succeeded"
        assert res1.runner_id == "vps-01"
        assert res1.executor_session_id == "claude:sid1"
        assert (
            affinity.lookup(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#42")
            == "vps-01"
        )

        # 2. run.query recovers completed result by run_id
        queried = await hub.query_run("vps-01", "run_s2_1")
        assert queried is not None and queried.run_id == "run_s2_1" and queried.status == "succeeded"

        # 3. Cross-network interrupt kills live subprocess on remote node
        slow_task = asyncio.create_task(
            hub.run(
                RunRequest(
                    run_id="run_s2_slow",
                    namespace="hb",
                    project_key="hibernaut",
                    owner_scope="proj:hb",
                    work_key="gh#99",
                    executor_id="slow",
                    readonly=True,
                    prompt="",
                    timeout_seconds=10.0,
                )
            )
        )
        await asyncio.sleep(0.25)
        ires = await hub.interrupt(InterruptRequest(run_id="run_s2_slow", reason="user_cancel", grace_seconds=0.5))
        assert ires.accepted is True
        assert ires.state == "cancelling"
        slow_res = await slow_task
        assert slow_res.status == "interrupted"

        # 4. conn_epoch anti-split-brain: second agent with same runner_id supersedes old with Close 4009
        mac_agent_new = RunnerRelayAgent(daemon=mac_daemon, hub_url=url, token="secret-relay-token")
        await mac_agent_new.start()
        await mac_agent_new.wait_connected()
        await mac_agent.wait_superseded(timeout=3.0)
        assert mac_agent_new.conn_epoch == 2

        # 5. 2A Offline affinity rule: stop vps-01 -> write task for gh#42 raises NodeAffinityOfflineError
        await vps_agent.stop()
        await asyncio.sleep(0.15)
        with pytest.raises(NodeAffinityOfflineError):
            await hub.run(
                RunRequest(
                    run_id="run_s2_off_write",
                    namespace="hb",
                    project_key="hibernaut",
                    owner_scope="proj:hb",
                    work_key="gh#42",
                    executor_id="claude",
                    readonly=False,
                    prompt="must-wait",
                )
            )

        # Pure remote readonly task (readonly=True, remote_readonly_ok=True) drifts to online mac-m4
        drift_res = await hub.run(
            RunRequest(
                run_id="run_s2_off_ro",
                namespace="hb",
                project_key="hibernaut",
                owner_scope="proj:hb",
                work_key="gh#42",
                executor_id="claude",
                readonly=True,
                remote_readonly_ok=True,
                prompt="drift-ok",
            )
        )
        assert drift_res.status == "succeeded"
        assert drift_res.runner_id == "mac-m4"

        # 6. Reconnect vps-01 & reclaim workspace over WS -> unbinds affinity
        vps_agent2 = RunnerRelayAgent(daemon=vps_daemon, hub_url=url, token="secret-relay-token")
        await vps_agent2.start()
        await vps_agent2.wait_connected()
        reclaim = await hub.reclaim_workspace(
            ReclaimRequest(
                namespace="hb",
                project_key="hibernaut",
                owner_scope="proj:hb",
                work_key="gh#42",
            )
        )
        assert reclaim.reclaimed is True
        assert (
            affinity.lookup(namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#42")
            is None
        )

        await mac_agent_new.stop()
        await vps_agent2.stop()
        await hub.stop()

    asyncio.run(_scenario())
    mac_daemon.close()
    vps_daemon.close()
    affinity.close()
