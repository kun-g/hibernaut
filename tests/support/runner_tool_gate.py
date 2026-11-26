from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.core.models import Event, PermissionDecision, SessionRef, Suspend
from runner.cli_drivers import TemplateCLIDriver
from runner.config import RunnerConfig
from runner.daemon import RunnerDaemon
from runner import tool_hook
from runner.protocol import RunRequest, ToolCall, ToolDecision, WorktreeRef
from runner.registry import WorkspaceAffinityStore
from runner.tool_gate import ToolGate
from runner.transport.in_process import InProcessRunnerClient
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport.relay_hub import RunnerRelayHub
from runner.worktree import ResolvedWorktree, WorktreeManager
from tests.support.fake_gated_cli import install_gated_cli
from tests.support.runner_safety import _config, _init_repo


INBOX = "proj:hibernaut"
CLEANUP = {"command": "deploy --now"}
TOOL_HOOK = Path(tool_hook.__file__).resolve()


class Policy:
    def __init__(self, decisions: dict[str, str] | None = None) -> None:
        self.decisions = decisions or {}
        self.checks: list[tuple[str, dict[str, Any]]] = []

    def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
        self.checks.append((tool_name, args))
        action = self.decisions.get(tool_name, "allow")
        return PermissionDecision(action=action, reason=f"{action} by policy")  # type: ignore[arg-type]


class Caps:
    memory: Any = None
    timers: Any = None

    def __init__(self, policy: Policy) -> None:
        self.permissions = policy
        self.interrupted = False

    def poll_folded_events(self) -> list[Event]:
        return []

    def is_interrupted(self) -> bool:
        return self.interrupted


class Node:
    def __init__(self, tmp_path: Path, *, runner_id: str = "runner-gate", env: dict[str, str] | None = None,
                 real_worktree: bool = False, arm_timeout: float = 60.0, **driver: Any) -> None:
        repo = tmp_path / "repo"
        worktrees = None
        if real_worktree:
            _init_repo(repo)
        else:
            repo.mkdir(parents=True)
            worktrees = Mock(spec_set=WorktreeManager)
            worktrees.resolve.side_effect = lambda *, work_key, **_: ResolvedWorktree(
                repo, "task", False, WorktreeRef(work_key, branch="task"))
            worktrees.list_ready_work_keys.return_value = []
        self.log = tmp_path / "cli.log"
        command = install_gated_cli(tmp_path / "bin")
        fields: dict[str, Any] = {
            "executor_id": "claude",
            "driver_kind": "claude",
            "command": str(command),
            "args": ("-p",),
            "resume_args": ("-p", "--resume", "{native_session_id}"),
            "env": {"FAKE_LOG": str(self.log), **(env or {})},
            "supports_stream_json": True,
            "pass_prompt_via_stdin": True,
            "calls_tools": True,
            "tool_hook": "claude",
            **driver,
        }
        self.config: RunnerConfig = _config(tmp_path, repo, runner_id=runner_id)
        self.daemon = RunnerDaemon(self.config, drivers={"claude": TemplateCLIDriver(**fields)},
                                   worktree_manager=worktrees, hook_arm_timeout_seconds=arm_timeout)
        self.client = InProcessRunnerClient(self.daemon, caller_id="hibernaut", bound_namespace="hb")

    def entries(self, kind: str) -> list[dict[str, Any]]:
        if not self.log.exists():
            return []
        return [e for e in map(json.loads, self.log.read_text(encoding="utf-8").splitlines()) if e["kind"] == kind]


def plan(*calls: tuple[str, dict[str, Any]]) -> str:
    return "PLAN: " + json.dumps([{"tool": t, "args": a} for t, a in calls])


def event(body: str, **payload: Any) -> Event:
    return Event(inbox_id=INBOX, source="github", external_id="e1", payload={"body": body, **payload})


def session(**kw: Any) -> SessionRef:
    return SessionRef(session_id="sess_1", inbox_id=INBOX, correlation_key="github:kun-g/hibernaut#7", **kw)


def bridge_to(runner: Any, **kw: Any) -> RunnerExecutorBridge:
    return RunnerExecutorBridge(
        runner=runner, executor_id="claude", project_key="hibernaut",
        **{"executor_calls_tools": True, "authorize_tools": True, **kw},
    )


def execute(bridge: RunnerExecutorBridge, events: list[Event], sess: SessionRef, caps: Caps) -> Any:
    return asyncio.run(bridge.execute(events, sess, caps))  # type: ignore[arg-type]


def suspended(outcome: Suspend) -> SessionRef:
    return session(
        status="suspended", pending_request_id=outcome.request_id, checkpoint=outcome.session_checkpoint,
        executor_session_id=outcome.executor_session_id,
    )


def answer(outcome: Suspend, approved: bool, **payload: Any) -> Event:
    return Event(
        inbox_id=INBOX, source="lark", external_id="a1",
        payload={"approval_request_id": outcome.request_id, "approved": approved, **payload},
    )


async def with_relay(node: Node, tmp_path: Path, use: Any, **hub_args: Any) -> Any:
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
    hub = RunnerRelayHub(shared_token="t0k", affinity=affinity, **hub_args)
    host, port = await hub.start()
    agent = RunnerRelayAgent(daemon=node.daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="t0k")
    await agent.start()
    try:
        await agent.wait_connected()
        return await use(hub)
    finally:
        await agent.stop()
        await hub.stop()
        affinity.close()


def request(**kw: Any) -> RunRequest:
    fields: dict[str, Any] = {
        "run_id": "run_1", "namespace": "hb", "project_key": "hibernaut", "owner_scope": INBOX, "work_key": "github:x#1",
        "executor_id": "claude", "prompt": plan(("Bash", CLEANUP)), "timeout_seconds": 30.0, "readonly": False,
        "interactive": False,
    }
    return RunRequest(**{**fields, **kw})


async def allow(call: ToolCall) -> ToolDecision:
    return ToolDecision(action="allow")


def with_gate(use: Any, authorizer: Any = allow, stopped: bool = False, **kw: Any) -> None:
    async def scenario() -> None:
        stop = asyncio.Event()
        if stopped:
            stop.set()
        gate = ToolGate(run_id="run_1", authorizer=authorizer, stopped=stop.is_set, on_defer=stop.set, **kw)
        binding = await gate.open()
        try:
            assert binding.socket_path.parent.stat().st_mode & 0o077 == 0
            await use(gate, binding)
        finally:
            await gate.close()
        assert not binding.socket_path.parent.exists()

    asyncio.run(scenario())
