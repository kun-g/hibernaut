from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.core.models import Emit, Event, PermissionDecision, SessionRef
from runner.cli_drivers import StreamEvent, TemplateCLIDriver
from runner.config import NamespacePolicy, ProjectCheckout
from runner.daemon import RunnerDaemon
from runner.protocol import InterruptRequest, RunRequest, RunnerCapabilities
from runner.transport.in_process import InProcessRunnerClient
from runner.worktree import GitWorktreeBackend, WorkspaceIsolationError, WorktreeManager
from tests.support.runner_stage1 import _init_git_repo, _make_config


def test_worktree_prefix_isolation_collision_and_local_reclaim(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_git_repo(repo, with_remote=True)
    mgr = WorktreeManager(db_path=tmp_path / "ws.db", backend=GitWorktreeBackend(), worktree_root=tmp_path / "wts")
    hb_pol = NamespacePolicy(namespace="hb", prefix="hb", allowed_callers=("hibernaut",))
    ws_pol = NamespacePolicy(namespace="ws", prefix="ws", allowed_callers=("workshop",))
    proj = ProjectCheckout(project_key="hibernaut", checkout_path=repo)

    r1 = mgr.resolve(policy=hb_pol, project=proj, owner_scope="proj:hb", work_key="gh#42", readonly=False, name_hint="issue-42")
    assert r1.workdir.name == "hb-issue-42"
    assert r1.branch == "hb/issue-42"
    assert r1.created is True

    # Collision on same name_hint in different owner_scope auto-suffixes -2
    r2 = mgr.resolve(policy=hb_pol, project=proj, owner_scope="proj:hb2", work_key="gh#42", readonly=False, name_hint="issue-42")
    assert r2.workdir.name == "hb-issue-42-2"
    assert r2.branch == "hb/issue-42-2"

    # Malicious name_hint cannot escape namespace prefix
    r_ws = mgr.resolve(policy=ws_pol, project=proj, owner_scope="ws:pipe", work_key="item-1", readonly=False, name_hint="../hb-evil")
    assert r_ws.workdir.name.startswith("ws-")

    # Write isolation blocks primary checkout and cross-namespace paths
    with pytest.raises(WorkspaceIsolationError):
        mgr.assert_writable(policy=hb_pol, project=proj, owner_scope="proj:hb", work_key="gh#42", workdir=repo)
    with pytest.raises(WorkspaceIsolationError):
        mgr.assert_writable(policy=hb_pol, project=proj, owner_scope="proj:hb", work_key="gh#42", workdir=r_ws.workdir)

    # Local git reclaim: dirty tree blocks reclaim
    (r1.workdir / "dirty.txt").write_text("uncommitted\n", encoding="utf-8")
    blocked_dirty = mgr.reclaim(policy=hb_pol, project=proj, owner_scope="proj:hb", work_key="gh#42")
    assert blocked_dirty.reclaimed is False
    assert "dirty_working_tree" in blocked_dirty.blockers

    # Commit locally without pushing -> unpushed_commits blocks reclaim
    subprocess.run(["git", "add", "dirty.txt"], cwd=r1.workdir, check=True)
    subprocess.run(["git", "commit", "-m", "local work"], cwd=r1.workdir, check=True, capture_output=True)
    blocked_unpushed = mgr.reclaim(policy=hb_pol, project=proj, owner_scope="proj:hb", work_key="gh#42")
    assert blocked_unpushed.reclaimed is False
    assert "unpushed_commits" in blocked_unpushed.blockers

    # Passing control-plane verified merged_head_sha (squash merge, 0C) succeeds with zero gh dependency
    head_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=r1.workdir, check=True, capture_output=True, text=True).stdout.strip()
    ok = mgr.reclaim(policy=hb_pol, project=proj, owner_scope="proj:hb", work_key="gh#42", merged_head_sha=head_sha)
    assert ok.reclaimed is True
    assert ok.branch_kept == "hb/issue-42"
    assert not r1.workdir.exists()
    mgr.close()


def test_daemon_run_idempotency_idle_watchdog_interrupt_and_readonly(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_git_repo(repo, with_remote=True)
    cfg = _make_config(tmp_path, repo, max_concurrent=2)

    events_seen: list[StreamEvent] = []

    class _RecorderPipe:
        def on_event(self, event: StreamEvent) -> None:
            events_seen.append(event)

        async def send_stdin(self, text: str) -> None:
            del text

    py = sys.executable
    drivers = {
        "echo": TemplateCLIDriver(
            executor_id="echo",
            driver_kind="custom",
            command=py,
            args=("-c", "import json, sys; print(json.dumps({'outcome': 'emit', 'message': 'hi:' + sys.argv[1], 'session_id': 's99'}))", "{prompt}"),
            resume_args=("-c", "import json, sys; print(json.dumps({'outcome': 'emit', 'message': 'resumed:' + sys.argv[1] + ':' + sys.argv[2], 'session_id': sys.argv[1]}))", "{native_session_id}", "{prompt}"),
        ),
        "stall": TemplateCLIDriver(
            executor_id="stall",
            driver_kind="custom",
            command=py,
            args=("-c", "import time; time.sleep(10)"),
            default_idle_timeout_seconds=0.2,
        ),
        "mutate": TemplateCLIDriver(
            executor_id="mutate",
            driver_kind="custom",
            command=py,
            args=("-c", "from pathlib import Path; Path('mutated.txt').write_text('oops')"),
        ),
    }

    daemon = RunnerDaemon(cfg, drivers=drivers, stream_pipe=_RecorderPipe())
    hb_client = InProcessRunnerClient(daemon, caller_id="hibernaut", bound_namespace="hb")

    async def _run_all() -> None:
        caps = await hb_client.capabilities()
        assert isinstance(caps, RunnerCapabilities)
        assert caps.slots.available == 2

        # 1. Run + idempotency + resume prefix self-check
        req1 = RunRequest(
            run_id="run_echo1",
            namespace="hb",
            project_key="hibernaut",
            owner_scope="proj:hb",
            work_key="gh#10",
            name_hint="issue-10",
            executor_id="echo",
            readonly=False,
            prompt="hello",
        )
        res1 = await hb_client.run(req1)
        assert res1.status == "succeeded"
        assert res1.executor_session_id == "echo:s99"
        assert res1.log_ref is not None and Path(res1.log_ref.path).exists()
        assert events_seen

        # Same run_id returns cached result without re-running
        res1_dup = await hb_client.run(req1)
        assert res1_dup == res1

        # Resume with matching prefix uses resume_args; mismatched prefix starts fresh
        req2 = RunRequest(
            run_id="run_echo2",
            namespace="hb",
            project_key="hibernaut",
            owner_scope="proj:hb",
            work_key="gh#10",
            executor_id="echo",
            executor_session_id="echo:s99",
            readonly=False,
            prompt="again",
        )
        res2 = await hb_client.run(req2)
        assert res2.structured_output is not None
        assert res2.structured_output["message"] == "resumed:s99:again"

        req3 = RunRequest(
            run_id="run_echo3",
            namespace="hb",
            project_key="hibernaut",
            owner_scope="proj:hb",
            work_key="gh#10",
            executor_id="echo",
            executor_session_id="claude:foreign-uuid",
            readonly=False,
            prompt="fresh",
        )
        res3 = await hb_client.run(req3)
        assert res3.structured_output is not None
        assert res3.structured_output["message"] == "hi:fresh"

        # 2. Idle watchdog kills stalled process
        res_stall = await hb_client.run(
            RunRequest(
                run_id="run_stall1",
                namespace="hb",
                project_key="hibernaut",
                owner_scope="proj:hb",
                work_key="gh#10",
                executor_id="stall",
                readonly=True,
                prompt="",
                timeout_seconds=10.0,
            )
        )
        assert res_stall.status == "timed_out"
        assert res_stall.error_kind == "timeout"
        assert res_stall.error_summary is not None and "idle" in res_stall.error_summary

        # 3. Interrupt kills active run cleanly
        long_task = asyncio.create_task(
            hb_client.run(
                RunRequest(
                    run_id="run_int1",
                    namespace="hb",
                    project_key="hibernaut",
                    owner_scope="proj:hb",
                    work_key="gh#10",
                    executor_id="stall",
                    readonly=True,
                    prompt="",
                    timeout_seconds=10.0,
                    idle_timeout_seconds=10.0,
                )
            )
        )
        await asyncio.sleep(0.15)
        int_res = await hb_client.interrupt(InterruptRequest(run_id="run_int1", reason="new_event", grace_seconds=0.5))
        assert int_res.accepted is True
        assert int_res.state == "cancelling"
        res_int = await long_task
        assert res_int.status == "interrupted"

        # 4. Interactive=True refused when interactive_surface == "none" (3C)
        res_inter = await hb_client.run(
            RunRequest(
                run_id="run_inter1",
                namespace="hb",
                project_key="hibernaut",
                owner_scope="proj:hb",
                work_key="gh#10",
                executor_id="echo",
                readonly=False,
                interactive=True,
                prompt="hi",
            )
        )
        assert res_inter.status == "refused"

        # 5. Unauthorized caller cannot forge hb namespace (3B)
        bad_res = await daemon.run(
            RunRequest(
                run_id="run_forged1",
                namespace="hb",
                project_key="hibernaut",
                owner_scope="proj:hb",
                work_key="gh#10",
                executor_id="echo",
                readonly=True,
                prompt="hi",
            ),
            caller_id="workshop",
        )
        assert bad_res.status == "refused"
        assert bad_res.error_kind == "permission_denied"

        # 6. Readonly mutation detected and discarded
        res_mut = await hb_client.run(
            RunRequest(
                run_id="run_mut1",
                namespace="hb",
                project_key="hibernaut",
                owner_scope="proj:hb",
                work_key="gh#10",
                executor_id="mutate",
                readonly=True,
                prompt="",
            )
        )
        assert res_mut.status == "failed"
        assert res_mut.error_kind == "permission_denied"

    asyncio.run(_run_all())
    daemon.close()


def test_hb_runner_executor_bridge(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_git_repo(repo, with_remote=True)
    cfg = _make_config(tmp_path, repo)
    py = sys.executable
    drivers = {
        "claude": TemplateCLIDriver(
            executor_id="claude",
            driver_kind="custom",
            command=py,
            args=("-c", "import json; print(json.dumps({'outcome': 'emit', 'message': 'done from runner', 'session_id': 'c1'}))"),
        ),
    }
    daemon = RunnerDaemon(cfg, drivers=drivers)
    client = InProcessRunnerClient(daemon, caller_id="hibernaut", bound_namespace="hb")
    bridge = RunnerExecutorBridge(runner=client, executor_id="claude", project_key="hibernaut", executor_calls_tools=False)

    class _DummyCaps:
        @property
        def memory(self) -> Any:
            return None

        @property
        def timers(self) -> Any:
            return None

        @property
        def permissions(self) -> Any:
            return PermissionDecision(action="allow")

        def poll_folded_events(self) -> list[Event]:
            return []

        def is_interrupted(self) -> bool:
            return False

    ev = Event(inbox_id="proj:hibernaut", source="github", external_id="e1", payload={"title": "Fix", "body": "Do it"})
    sess = SessionRef(session_id="sess_1", inbox_id="proj:hibernaut", correlation_key="github:kun-g/hibernaut#77")
    outcome = asyncio.run(bridge.execute([ev], sess, _DummyCaps()))  # type: ignore[arg-type]
    assert isinstance(outcome, Emit)
    assert outcome.messages == ("done from runner",)
    assert outcome.executor_session_id == "claude:c1"
    daemon.close()
