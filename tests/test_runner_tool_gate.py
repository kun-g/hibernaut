from __future__ import annotations

import asyncio
import dataclasses
import json
import shlex
import sys
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.core.models import Emit, Event, PermissionDecision, RunCancelled, SessionRef, Suspend
from runner.cli_drivers import TemplateCLIDriver, default_cli_drivers
from runner.config import RunnerConfig
from runner.daemon import RunnerDaemon
from runner import tool_hook
from runner.protocol import ExecutorCapability, RunRequest, RunResult, ToolCall, ToolDecision, run_identity
from runner.registry import WorkspaceAffinityStore
from runner.tool_gate import MAX_TOOL_CALL_BYTES, ToolGate, ToolGateBinding
from runner.transport.in_process import InProcessRunnerClient
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport.relay_hub import RunnerRelayHub
from runner.transport.ws_protocol import client_handshake, recv_json, send_json
from tests.fake_gated_cli import install_gated_cli
from tests.test_runner_safety import _config, _init_repo

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
    def __init__(self, tmp_path: Path, *, runner_id: str = "runner-gate", env: dict[str, str] | None = None, **driver: Any) -> None:
        repo = tmp_path / "repo"
        if not repo.exists():
            _init_repo(repo)
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
        self.daemon = RunnerDaemon(self.config, drivers={"claude": TemplateCLIDriver(**fields)})
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


# The three decisions of PermissionPort.check(), through the bridge, the daemon and the CLI's own hook.


def test_allow_lets_the_call_run(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "allow"})
    outcome = execute(bridge_to(node.client), [event(plan(("Bash", CLEANUP)))], session(), Caps(policy))
    assert isinstance(outcome, Emit) and outcome.status == "done"
    assert outcome.messages == ("all done",)
    assert policy.checks == [("Bash", CLEANUP)]
    assert [e["tool"] for e in node.entries("ran")] == ["Bash"]


def test_block_refuses_the_call_and_tells_the_executor_why(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "block"})
    outcome = execute(
        bridge_to(node.client), [event(plan(("Bash", CLEANUP), ("Read", {"file_path": "README.md"})))], session(), Caps(policy)
    )
    assert isinstance(outcome, Emit) and outcome.status == "done"
    assert [e["tool"] for e in node.entries("ran")] == ["Read"]
    assert [(e["tool"], e["reason"]) for e in node.entries("denied")] == [("Bash", "block by policy")]


def test_defer_stops_the_run_and_suspends_for_approval(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "defer"})
    body = plan(("Bash", CLEANUP), ("Write", {"path": "after.txt"}))
    outcome = execute(bridge_to(node.client), [event(body)], session(), Caps(policy))
    assert isinstance(outcome, Suspend)
    assert outcome.request_id in outcome.prompt and "Bash" in outcome.prompt and "deploy --now" in outcome.prompt
    assert "defer by policy" in outcome.prompt
    assert outcome.executor_session_id == "claude:native-1"
    assert outcome.telemetry is not None and outcome.telemetry.runner_id == "runner-gate"
    assert json.loads(outcome.session_checkpoint)["deferred_tool"] == {"tool_name": "Bash", "args": CLEANUP}
    assert node.entries("ran") == []
    assert node.daemon.capabilities_sync().slots.active_runs == 0
    assert policy.checks == [("Bash", CLEANUP)], "nothing is asked, and nothing runs, after the deferred call"


# Fail-closed: no hook, no run.


def test_bridge_without_an_authorizer_never_starts_a_tool_capable_executor(tmp_path: Path) -> None:
    node = Node(tmp_path)
    outcome = execute(bridge_to(node.client, authorize_tools=False), [event(plan(("Bash", CLEANUP)))], session(), Caps(Policy()))
    assert isinstance(outcome, Emit) and outcome.status == "refused"
    assert node.entries("start") == []


def test_daemon_refuses_a_tool_capable_executor_without_an_authorizer(tmp_path: Path) -> None:
    node = Node(tmp_path)
    # A bridge that wrongly declares the executor tool-free submits without an authorizer.
    outcome = execute(
        bridge_to(node.client, executor_calls_tools=False), [event(plan(("Bash", CLEANUP)))], session(), Caps(Policy())
    )
    assert isinstance(outcome, Emit) and outcome.status == "refused"
    assert "authoriz" in outcome.messages[0]
    assert node.entries("start") == []


@pytest.mark.parametrize("calls_tools", [True, None])
def test_daemon_refuses_an_executor_whose_calls_it_cannot_hook(tmp_path: Path, calls_tools: bool | None) -> None:
    node = Node(tmp_path, tool_hook=None, calls_tools=calls_tools)
    outcome = execute(
        bridge_to(node.client, executor_calls_tools=calls_tools), [event(plan(("Bash", CLEANUP)))], session(), Caps(Policy())
    )
    assert isinstance(outcome, Emit) and outcome.status == "refused"
    assert node.entries("start") == []


# A hook that is configured but never loads leaves the CLI unchecked.


def test_run_whose_hook_never_loaded_is_not_delivered(tmp_path: Path) -> None:
    node = Node(tmp_path, env={"FAKE_IGNORES_HOOKS": "1"})
    outcome = execute(bridge_to(node.client), [event(plan(("Bash", CLEANUP)))], session(), Caps(Policy()))
    assert isinstance(outcome, Emit) and outcome.status == "failed"
    assert "permission_denied" in outcome.messages[0] and "all done" not in outcome.messages[0]


def test_run_is_stopped_when_its_hook_does_not_report_in_time(tmp_path: Path) -> None:
    node = Node(tmp_path, env={"FAKE_IGNORES_HOOKS": "1"})
    node.daemon = RunnerDaemon(
        node.config, drivers={"claude": node.daemon._drivers["claude"]}, hook_arm_timeout_seconds=0.5  # pyright: ignore[reportPrivateUsage]
    )
    client = InProcessRunnerClient(node.daemon, caller_id="hibernaut", bound_namespace="hb")
    body = plan(("sleep", {"seconds": 60}), ("Bash", CLEANUP))
    outcome = execute(bridge_to(client), [event(body)], session(), Caps(Policy()))
    assert isinstance(outcome, Emit) and outcome.status == "failed" and "permission_denied" in outcome.messages[0]
    assert node.entries("ran") == []


@pytest.mark.parametrize("decision", [RuntimeError("policy store is down"), None])
def test_call_is_refused_when_the_policy_gives_no_answer(tmp_path: Path, decision: Exception | None) -> None:
    class Broken(Policy):
        def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
            if decision is not None:
                raise decision
            return PermissionDecision(action="maybe", reason="")  # type: ignore[arg-type]

    node = Node(tmp_path)
    outcome = execute(bridge_to(node.client), [event(plan(("Bash", CLEANUP)))], session(), Caps(Broken()))
    assert isinstance(outcome, Emit)
    assert node.entries("ran") == [] and len(node.entries("denied")) == 1


def test_readonly_run_that_wrote_through_an_allowed_call_is_still_discarded(tmp_path: Path) -> None:
    node = Node(tmp_path)
    outcome = execute(bridge_to(node.client), [event(plan(("Write", {"path": "x.txt"})), readonly=True)], session(), Caps(Policy()))
    assert isinstance(outcome, Emit) and outcome.status == "failed" and "readonly" in outcome.messages[0]


# Resuming after the person answered.


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


def deferred_run(node: Node, body: str, policy: Policy, **payload: Any) -> tuple[RunnerExecutorBridge, Suspend]:
    bridge = bridge_to(node.client)
    outcome = execute(bridge, [event(body, **payload)], session(), Caps(policy))
    assert isinstance(outcome, Suspend)
    return bridge, outcome


def test_approved_call_runs_once_in_the_resumed_native_session(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "defer"})
    bridge, first = deferred_run(node, plan(("Bash", CLEANUP)), policy)
    outcome = execute(bridge, [answer(first, True, comment="go ahead")], suspended(first), Caps(policy))
    assert isinstance(outcome, Emit) and outcome.messages == ("all done",)
    assert [e["args"] for e in node.entries("ran")] == [CLEANUP]
    resumed = node.entries("start")[-1]
    assert resumed["session"] == "native-1" and "已批准" in resumed["prompt"] and "go ahead" in resumed["prompt"]


def test_approval_covers_one_call_with_the_same_arguments(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "defer"})
    body = plan(("Bash", {**CLEANUP, "description": "first wording"}))
    bridge, first = deferred_run(node, body, policy)
    again = plan(("Bash", {**CLEANUP, "description": "second wording"}), ("Bash", CLEANUP))
    state = json.loads(first.session_checkpoint)
    state["prompt"] = again
    second = execute(
        bridge, [answer(first, True)], dataclasses.replace(suspended(first), checkpoint=json.dumps(state)), Caps(policy)
    )
    assert isinstance(second, Suspend) and second.request_id != first.request_id
    assert len(node.entries("ran")) == 1


def test_changed_arguments_are_asked_about_again_and_the_unused_approval_is_kept(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "defer"})
    bridge, first = deferred_run(node, plan(("Bash", CLEANUP)), policy)
    state = json.loads(first.session_checkpoint)
    state["prompt"] = plan(("Bash", {"command": "deploy --everywhere"}))
    second = execute(
        bridge, [answer(first, True)], dataclasses.replace(suspended(first), checkpoint=json.dumps(state)), Caps(policy)
    )
    assert isinstance(second, Suspend) and "everywhere" in second.prompt
    assert node.entries("ran") == []
    assert [g["request_id"] for g in json.loads(second.session_checkpoint)["grants"]] == [first.request_id]


def test_rejected_call_is_refused_with_the_comment_and_the_run_goes_on(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "defer"})
    bridge, first = deferred_run(node, plan(("Bash", CLEANUP), ("Read", {})), policy)
    outcome = execute(bridge, [answer(first, False, comment="not today")], suspended(first), Caps(policy))
    assert isinstance(outcome, Emit) and outcome.status == "done"
    assert [e["tool"] for e in node.entries("ran")] == ["Read"]
    assert "not today" in node.entries("denied")[-1]["reason"]


def test_block_outranks_an_approval(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "defer"})
    bridge, first = deferred_run(node, plan(("Bash", CLEANUP)), policy)
    policy.decisions["Bash"] = "block"
    execute(bridge, [answer(first, True)], suspended(first), Caps(policy))
    assert node.entries("ran") == []


def test_retry_of_the_resumed_run_does_not_use_the_approval_twice(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "defer"})
    bridge, first = deferred_run(node, plan(("Bash", CLEANUP)), policy)
    events, sess = [answer(first, True)], suspended(first)
    execute(bridge, events, sess, Caps(policy))
    assert isinstance(execute(bridge, events, sess, Caps(policy)), Suspend)
    assert len(node.entries("ran")) == 1


def test_resumed_run_keeps_the_readonly_bound_of_the_deferred_one(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Write": "defer"})
    bridge, first = deferred_run(node, plan(("Write", {"path": "x.txt"})), policy, readonly=True)
    outcome = execute(bridge, [answer(first, True)], suspended(first), Caps(policy))
    assert isinstance(outcome, Emit) and outcome.status == "failed" and "readonly" in outcome.messages[0]


def test_same_call_deferred_again_before_the_answer_keeps_the_open_request(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "defer"})
    bridge, first = deferred_run(node, plan(("Bash", CLEANUP)), policy)
    second = execute(bridge, [event(plan(("Bash", CLEANUP)))], suspended(first), Caps(policy))
    assert isinstance(second, Suspend) and second.request_id == first.request_id


# The same over the relay.


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


@pytest.mark.parametrize("action", ["allow", "block", "defer"])
def test_relay_carries_each_decision(tmp_path: Path, action: str) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": action})

    async def use(hub: RunnerRelayHub) -> Any:
        return await bridge_to(hub).execute([event(plan(("Bash", CLEANUP)))], session(), Caps(policy))  # type: ignore[arg-type]

    outcome = asyncio.run(with_relay(node, tmp_path, use))
    assert policy.checks == [("Bash", CLEANUP)]
    if action == "defer":
        assert isinstance(outcome, Suspend) and outcome.executor_session_id == "claude:native-1"
    else:
        assert isinstance(outcome, Emit) and outcome.messages == ("all done",)
    assert len(node.entries("ran")) == (1 if action == "allow" else 0)


def request(**kw: Any) -> RunRequest:
    fields: dict[str, Any] = {
        "run_id": "run_1", "namespace": "hb", "project_key": "hibernaut", "owner_scope": INBOX, "work_key": "github:x#1",
        "executor_id": "claude", "prompt": plan(("Bash", CLEANUP)), "timeout_seconds": 30.0, "readonly": False,
        "interactive": False,
    }
    return RunRequest(**{**fields, **kw})


async def allow(call: ToolCall) -> ToolDecision:
    return ToolDecision(action="allow")


def test_relay_hub_refuses_a_run_that_needs_authorization_without_an_authorizer(tmp_path: Path) -> None:
    node = Node(tmp_path)

    async def use(hub: RunnerRelayHub) -> None:
        with pytest.raises(PermissionError):
            await hub.run(request(authorize_tools=True))
        refused = await hub.run(request(run_id="run_2"))
        assert refused.status == "refused" and refused.error_kind == "permission_denied"

    asyncio.run(with_relay(node, tmp_path, use))
    assert node.entries("start") == []


def test_router_skips_a_node_that_cannot_hold_the_executors_calls(tmp_path: Path) -> None:
    node = Node(tmp_path, tool_hook=None)

    async def use(hub: RunnerRelayHub) -> None:
        with pytest.raises(Exception, match="no online runner"):
            await hub.run(request(), authorizer=allow)

    asyncio.run(with_relay(node, tmp_path, use))


def test_relay_call_is_refused_when_the_authorizer_fails(tmp_path: Path) -> None:
    node = Node(tmp_path)

    async def broken(call: ToolCall) -> ToolDecision:
        raise RuntimeError("control plane is down")

    async def use(hub: RunnerRelayHub) -> RunResult:
        return await hub.run(request(), authorizer=broken)

    assert asyncio.run(with_relay(node, tmp_path, use)).status == "succeeded"
    assert node.entries("ran") == []
    assert "control plane is down" in node.entries("denied")[0]["reason"]


def test_relay_answers_a_call_only_on_the_connection_its_run_was_sent_on(tmp_path: Path) -> None:
    node, asked = Node(tmp_path), []

    async def record(call: ToolCall) -> ToolDecision:
        asked.append(call.tool_name)
        return ToolDecision(action="allow")

    async def use(hub: RunnerRelayHub) -> None:
        run = asyncio.create_task(hub.run(request(prompt=plan(("sleep", {"seconds": 2}))), authorizer=record))
        while not node.entries("start"):
            await asyncio.sleep(0.05)
        host, port = hub._server.sockets[0].getsockname()[:2]  # type: ignore[union-attr]  # pyright: ignore
        reader, writer = await asyncio.open_connection(host, port)
        await client_handshake(reader, writer, host=f"{host}:{port}", path="/ws/runner")
        caps = {**node.daemon.capabilities_sync().to_dict(), "runner_id": "runner-other"}
        await send_json(writer, {"type": "hello", "token": "t0k", "capabilities": caps, "ready_work_keys": []}, mask=True)
        epoch = (await recv_json(reader, writer, expect_masked=False, send_masked=True))["conn_epoch"]
        forged = ToolCall(run_id="run_1", call_id="call_x", tool_name="Forged", args={})
        await send_json(writer, {"type": "tool.authorize", "conn_epoch": epoch, "call": forged.to_dict()}, mask=True)
        assert (await run).status == "succeeded"
        writer.close()

    asyncio.run(with_relay(node, tmp_path, use))
    assert asked == []


def test_bridge_waits_for_a_node_to_come_online(tmp_path: Path) -> None:
    node = Node(tmp_path)

    async def scenario() -> Any:
        affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
        hub = RunnerRelayHub(shared_token="t0k", affinity=affinity)
        host, port = await hub.start()
        agent = RunnerRelayAgent(daemon=node.daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="t0k")
        bridge = bridge_to(hub, node_wait_seconds=20.0)
        run = asyncio.create_task(bridge.execute([event(plan(("Bash", CLEANUP)))], session(), Caps(Policy())))  # type: ignore[arg-type]
        await asyncio.sleep(0.3)
        assert not run.done()
        await agent.start()
        try:
            return await run
        finally:
            await agent.stop()
            await hub.stop()
            affinity.close()

    outcome = asyncio.run(scenario())
    assert isinstance(outcome, Emit) and outcome.messages == ("all done",)


def test_bridge_waiting_for_a_node_stops_on_interrupt(tmp_path: Path) -> None:
    async def scenario() -> None:
        affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")
        hub, caps = RunnerRelayHub(shared_token="t0k", affinity=affinity), Caps(Policy())
        run = asyncio.create_task(bridge_to(hub, node_wait_seconds=60.0).execute([event("hi")], session(), caps))  # type: ignore[arg-type]
        await asyncio.sleep(0.2)
        caps.interrupted = True
        with pytest.raises(RunCancelled):
            await asyncio.wait_for(run, 5.0)
        affinity.close()

    asyncio.run(scenario())


# The gate and the hook script on their own.


async def ask_gate(path: Path, line: bytes) -> dict[str, Any]:
    reader, writer = await asyncio.open_unix_connection(str(path))
    writer.write(line)
    try:
        await writer.drain()
        reply = await reader.readline()
    finally:
        writer.close()
    return json.loads(reply)


def call_line(name: str = "Bash", args: Any = None) -> bytes:
    return json.dumps({"type": "tool.authorize", "tool_name": name, "args": {} if args is None else args}).encode() + b"\n"


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


@pytest.mark.parametrize(
    "line",
    [
        b"not json\n", b"[]\n", b'{"type": "other"}\n', b'{"type": "tool.authorize"}\n',
        b'{"type": "tool.authorize", "tool_name": "Bash", "args": []}\n',
        b'{"type": "tool.authorize", "tool_name": "Bash", "args": {"x": "\\ud800"}}\n',
        b'{"type": "tool.authorize", "tool_name": "Bash", "args": {"x": "' + b"y" * (MAX_TOOL_CALL_BYTES + 10) + b'"}}\n',
    ],
)
def test_gate_refuses_what_it_cannot_read(line: bytes) -> None:
    async def use(gate: ToolGate, binding: ToolGateBinding) -> None:
        try:
            reply = await ask_gate(binding.socket_path, line)
        except (OSError, ValueError):
            return  # dropped without an answer, which the hook treats as a refusal
        assert reply["action"] == "block"

    with_gate(use)


def test_gate_refuses_every_call_after_a_deferred_one_even_if_asked_at_the_same_time() -> None:
    async def slow_defer(call: ToolCall) -> ToolDecision:
        await asyncio.sleep(0.1)
        return ToolDecision(action="defer" if call.tool_name == "Bash" else "allow")

    async def use(gate: ToolGate, binding: ToolGateBinding) -> None:
        replies = await asyncio.gather(
            ask_gate(binding.socket_path, call_line("Bash")), ask_gate(binding.socket_path, call_line("Read"))
        )
        assert sorted(r["action"] for r in replies) in (["allow", "defer"], ["block", "defer"])
        assert (await ask_gate(binding.socket_path, call_line("Read")))["action"] == "block"
        assert gate.deferred is not None and gate.deferred[0].tool_name == "Bash"

    with_gate(use, slow_defer)


def test_gate_refuses_calls_of_a_run_being_stopped() -> None:
    async def use(gate: ToolGate, binding: ToolGateBinding) -> None:
        assert (await ask_gate(binding.socket_path, call_line()))["action"] == "block"

    with_gate(use, stopped=True)


def test_gate_refuses_a_call_nobody_answers_in_time() -> None:
    async def never(call: ToolCall) -> ToolDecision:
        await asyncio.sleep(60)
        return ToolDecision(action="allow")

    async def use(gate: ToolGate, binding: ToolGateBinding) -> None:
        assert (await ask_gate(binding.socket_path, call_line()))["action"] == "block"

    with_gate(use, never, decision_timeout_seconds=0.1)


async def run_hook(style: str, path: Path, payload: Any, event_name: str = "pre") -> tuple[int, str, str]:
    raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-I", str(TOOL_HOOK), style, str(path), event_name,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate(raw)
    return proc.returncode or 0, out.decode(), err.decode()


PAYLOADS = {
    "claude": {"tool_name": "Bash", "tool_input": CLEANUP, "user_email": "someone@example.com", "cwd": "/private"},
    "grok": {"toolName": "run_terminal_command", "toolInput": CLEANUP, "userEmail": "someone@example.com"},
    "cursor-agent": {"tool_name": "Shell", "tool_input": CLEANUP, "user_email": "someone@example.com"},
}


@pytest.mark.parametrize("action", ["allow", "block", "defer"])
@pytest.mark.parametrize("style", ["claude", "grok", "cursor-agent"])
def test_hook_answers_each_cli_in_its_own_format(style: str, action: str) -> None:
    asked: list[ToolCall] = []

    async def decide(call: ToolCall) -> ToolDecision:
        asked.append(call)
        return ToolDecision(action=action, reason="because")  # type: ignore[arg-type]

    async def use(gate: ToolGate, binding: ToolGateBinding) -> None:
        code, out, _ = await run_hook(style, binding.socket_path, PAYLOADS[style])
        reply = json.loads(out)
        assert code == 0
        if style == "cursor-agent":
            assert reply["permission"] == ("allow" if action == "allow" else "deny")
            assert action == "allow" or "because" in reply["agent_message"]
        else:
            assert reply["hookSpecificOutput"]["permissionDecision"] == ("allow" if action == "allow" else "deny")
            assert "because" in reply["hookSpecificOutput"]["permissionDecisionReason"]
            assert (reply.get("continue") is False) == (action == "defer")
        assert asked[0].args == CLEANUP and "example.com" not in json.dumps(asked[0].to_dict())

    with_gate(use, decide)


@pytest.mark.parametrize(
    "payload",
    [b"", b"not json", {"tool_name": "Bash"}, {"tool_input": {}}, {**PAYLOADS["grok"], "toolInputTruncated": True}],
)
def test_hook_refuses_a_payload_it_cannot_judge(payload: Any) -> None:
    async def use(gate: ToolGate, binding: ToolGateBinding) -> None:
        code, out, _ = await run_hook("grok", binding.socket_path, payload)
        assert code == 2 and out == ""

    with_gate(use)


def test_hook_refuses_when_the_gate_is_gone(tmp_path: Path) -> None:
    assert asyncio.run(run_hook("claude", tmp_path / "gone.sock", PAYLOADS["claude"]))[0] == 2


def test_hook_arms_the_gate() -> None:
    async def use(gate: ToolGate, binding: ToolGateBinding) -> None:
        assert not gate.armed.is_set()
        assert (await run_hook("claude", binding.socket_path, {}, "arm"))[0] == 0
        assert gate.armed.is_set()

    with_gate(use)


# What each CLI is started with.


def gated_argv(executor_id: str, tmp_path: Path, **kw: Any) -> tuple[str, ...]:
    config = tmp_path / "config"
    config.mkdir()
    binding = ToolGateBinding(socket_path=tmp_path / "it's a gate.sock", config_dir=config)
    req = request(executor_id=executor_id, authorize_tools=True, **kw)
    return default_cli_drivers()[executor_id].build_command(req, tmp_path, tool_gate=binding).argv


def test_claude_is_started_with_the_hook_and_nothing_allowed_by_default(tmp_path: Path) -> None:
    argv = gated_argv("claude", tmp_path, executor_session_id="claude:native-9")
    settings = json.loads(argv[argv.index("--settings") + 1])
    assert settings["disableAllHooks"] is False
    assert settings["hooks"]["PreToolUse"][0]["matcher"] == "*"
    hook = settings["hooks"]["PreToolUse"][0]["hooks"][0]
    assert hook["onFailure"] == "block" and hook["command"].endswith("|| exit 2")
    assert shlex.split(hook["command"])[:6] == [
        sys.executable, "-I", str(TOOL_HOOK), "claude", str(tmp_path / "it's a gate.sock"), "pre",
    ]
    assert argv[argv.index("--permission-mode") + 1] == "dontAsk"
    assert argv[argv.index("--resume") + 1] == "native-9"


def test_grok_is_started_with_a_fail_closed_hook_and_no_way_around_it(tmp_path: Path) -> None:
    argv = gated_argv("grok", tmp_path)
    front = Path(argv[argv.index("--agent") + 1]).read_text(encoding="utf-8").split("---\n")[1]
    hooks = json.loads(next(line for line in front.splitlines() if line.startswith("hooks: "))[len("hooks: "):])
    assert hooks["PreToolUse"][0]["hooks"][0]["failClosed"] is True
    assert "--no-subagents" in argv and argv[argv.index("--disallowed-tools") + 1] == "workflow"


def test_cursor_is_started_with_a_fail_closed_hook_plugin(tmp_path: Path) -> None:
    argv = gated_argv("cursor-agent", tmp_path)
    plugin = Path(argv[argv.index("--plugin-dir") + 1])
    assert json.loads((plugin / ".cursor-plugin" / "plugin.json").read_text())["name"] == "runner-gate"
    hooks = json.loads((plugin / "hooks" / "hooks.json").read_text())
    assert hooks["hooks"]["preToolUse"][0]["failClosed"] is True


def test_only_executors_with_a_hook_announce_tool_authorization() -> None:
    announced = {d.executor_id: d.capability().tool_authorization for d in default_cli_drivers().values()}
    assert announced == {"claude": True, "grok": True, "cursor-agent": True, "dsh": False}
    with pytest.raises(ValueError):
        default_cli_drivers()["dsh"].build_command(
            request(executor_id="dsh"), Path("."), tool_gate=ToolGateBinding(Path("s"), Path("c"))
        )


def test_run_without_a_gate_starts_the_cli_as_before(tmp_path: Path) -> None:
    for name, driver in default_cli_drivers().items():
        argv = driver.build_command(request(executor_id=name), tmp_path).argv
        assert not {"--settings", "--agent", "--plugin-dir", "--force"} & set(argv)


# Stream-json reply text and wire compatibility.


def test_reply_of_a_stream_longer_than_the_excerpt_is_still_found(tmp_path: Path) -> None:
    script = (
        "import json\n"
        "for _ in range(400): print(json.dumps({'type': 'assistant', 'text': 'x' * 1000}))\n"
        "print(json.dumps({'type': 'result', 'session_id': 'n', 'result': 'the answer'}))\n"
    )
    repo = tmp_path / "repo"
    _init_repo(repo)
    driver = TemplateCLIDriver(
        executor_id="s", driver_kind="custom", command=sys.executable, args=("-c", script, "{prompt}"), supports_stream_json=True
    )
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers={"s": driver})
    result = asyncio.run(daemon.run(request(executor_id="s"), caller_id="hibernaut"))
    assert result.status == "succeeded" and result.final_text == "the answer"
    assert RunResult.from_dict(result.to_dict()).final_text == "the answer"


def test_authorization_fields_are_optional_on_the_wire_and_off_by_default() -> None:
    raw = request().to_dict()
    raw.pop("authorize_tools")
    assert RunRequest.from_dict(raw).authorize_tools is False
    cap = default_cli_drivers()["claude"].capability().to_dict()
    cap.pop("tool_authorization")
    assert ExecutorCapability.from_dict(cap).tool_authorization is False
    with pytest.raises(ValueError):
        ToolDecision(action="maybe")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ToolCall.from_dict({"run_id": "run_1", "call_id": "c", "tool_name": "Bash", "args": []})
    call = ToolCall(run_id="run_1", call_id="c", tool_name="Bash", args=CLEANUP)
    assert ToolCall.from_dict(call.to_dict()) == call
    assert run_identity(request(), "a") != run_identity(request(authorize_tools=True), "a")


def test_schemas_list_every_field_that_is_sent() -> None:
    schemas = Path(__file__).resolve().parents[1] / "schemas" / "runner" / "v1"

    def properties(name: str) -> Any:
        return json.loads((schemas / f"runner.{name}.schema.json").read_text(encoding="utf-8"))

    assert set(request().to_dict()) <= set(properties("run_request")["properties"])
    result = RunResult(run_id="run_1", runner_id="r", status="succeeded", executor_id="claude", started_at="", ended_at="", duration_ms=0)
    assert set(result.to_dict()) <= set(properties("run_result")["properties"])
    cap = properties("capabilities")["properties"]["executors"]["items"]["properties"]
    assert set(default_cli_drivers()["claude"].capability().to_dict()) <= set(cap)
    call, decision = properties("tool_authorize")["oneOf"]
    assert set(ToolCall(run_id="run_1", call_id="c", tool_name="t", args={}).to_dict()) == set(call["required"])
    assert set(ToolDecision(action="allow").to_dict()) == set(decision["required"])
