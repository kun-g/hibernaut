from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.core.models import Emit, PermissionDecision, Suspend
from runner.cli_drivers import TemplateCLIDriver
from runner.daemon import RunnerDaemon
from runner.protocol import RunResult, ToolCall, ToolDecision
from runner.registry import WorkspaceAffinityStore
from runner.tool_gate import ToolGate, ToolGateBinding
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport.relay_hub import RunnerRelayHub
from runner.transport.ws_protocol import client_handshake, recv_json, send_json
from tests.support.runner_safety import _config, _init_repo
from tests.support.runner_tool_gate import (
    CLEANUP,
    Caps,
    Node,
    Policy,
    TOOL_HOOK,
    answer,
    bridge_to,
    event,
    execute,
    plan,
    request,
    session,
    suspended,
    with_gate,
    with_relay,
)


def test_allow_lets_the_call_run(tmp_path: Path) -> None:
    node, policy = Node(tmp_path, real_worktree=True), Policy({"Bash": "allow"})
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


def test_run_whose_hook_never_loaded_is_not_delivered(tmp_path: Path) -> None:
    node = Node(tmp_path, env={"FAKE_IGNORES_HOOKS": "1"})
    outcome = execute(bridge_to(node.client), [event(plan(("Bash", CLEANUP)))], session(), Caps(Policy()))
    assert isinstance(outcome, Emit) and outcome.status == "failed"
    assert "permission_denied" in outcome.messages[0] and "all done" not in outcome.messages[0]


def test_run_is_stopped_when_its_hook_does_not_report_in_time(tmp_path: Path) -> None:
    node = Node(tmp_path, env={"FAKE_IGNORES_HOOKS": "1"}, arm_timeout=0.5)
    body = plan(("sleep", {"seconds": 60}), ("Bash", CLEANUP))
    outcome = execute(bridge_to(node.client), [event(body)], session(), Caps(Policy()))
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
    node = Node(tmp_path, real_worktree=True)
    outcome = execute(bridge_to(node.client), [event(plan(("Write", {"path": "x.txt"})), readonly=True)], session(), Caps(Policy()))
    assert isinstance(outcome, Emit) and outcome.status == "failed" and "readonly" in outcome.messages[0]


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


def test_native_session_of_a_stopped_run_is_known_even_if_the_cli_printed_nothing(tmp_path: Path) -> None:
    node, policy = Node(tmp_path, env={"FAKE_PRINTS_ONLY_A_RESULT": "1"}), Policy({"Bash": "defer"})
    bridge, first = deferred_run(node, plan(("Bash", CLEANUP)), policy)
    assert first.executor_session_id == "claude:native-1"
    execute(bridge, [answer(first, True)], suspended(first), Caps(policy))
    assert "--resume" in node.entries("start")[-1]["argv"]


def test_rejected_call_is_refused_with_the_comment_and_the_run_goes_on(tmp_path: Path) -> None:
    node, policy = Node(tmp_path), Policy({"Bash": "defer"})
    bridge, first = deferred_run(node, plan(("Bash", CLEANUP), ("Read", {})), policy)
    outcome = execute(bridge, [answer(first, False, comment="not today")], suspended(first), Caps(policy))
    assert isinstance(outcome, Emit) and outcome.status == "done"
    assert [e["tool"] for e in node.entries("ran")] == ["Read"]
    assert "not today" in node.entries("denied")[-1]["reason"]


def test_resumed_run_keeps_the_readonly_bound_of_the_deferred_one(tmp_path: Path) -> None:
    node, policy = Node(tmp_path, real_worktree=True), Policy({"Write": "defer"})
    bridge, first = deferred_run(node, plan(("Write", {"path": "x.txt"})), policy, readonly=True)
    outcome = execute(bridge, [answer(first, True)], suspended(first), Caps(policy))
    assert isinstance(outcome, Emit) and outcome.status == "failed" and "readonly" in outcome.messages[0]


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
        assert (await run_hook("claude", binding.socket_path, {"session_id": "n-1", "cwd": "/private"}, "arm"))[0] == 0
        assert gate.armed.is_set() and gate.native_session_id == "n-1"

    with_gate(use)


@pytest.mark.parametrize("payload", [b"", b"not json", {}, {"session_id": ["x"]}, {"session_id": "x" * 5000}])
def test_hook_arms_the_gate_without_a_usable_session_id(payload: Any) -> None:
    async def use(gate: ToolGate, binding: ToolGateBinding) -> None:
        assert (await run_hook("claude", binding.socket_path, payload, "arm"))[0] == 0
        assert gate.armed.is_set() and gate.native_session_id is None

    with_gate(use)


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
