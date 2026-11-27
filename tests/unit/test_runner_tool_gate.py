from __future__ import annotations

import asyncio
import dataclasses
import json
import shlex
import sys
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.core.models import Emit, RunCancelled, Suspend
from runner.cli_drivers import default_cli_drivers
from runner.protocol import (
    ExecutorCapability,
    RunRequest,
    RunResult,
    ToolAuthorizer,
    ToolCall,
    ToolDecision,
    run_identity,
)
from runner.registry import WorkspaceAffinityStore
from runner.tool_gate import MAX_TOOL_CALL_BYTES, ToolGate, ToolGateBinding
from runner.transport.relay_hub import RunnerRelayHub
from tests.support.runner_tool_gate import (
    CLEANUP,
    Caps,
    Node,
    Policy,
    TOOL_HOOK,
    allow,
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


class ApprovalRunner:
    def __init__(self):
        self.allowed = []

    async def run(self, req: RunRequest, *, authorizer: ToolAuthorizer) -> RunResult:
        assert req.authorize_tools
        calls = next(json.loads(line.removeprefix("PLAN: ")) for line in req.prompt.splitlines() if line.startswith("PLAN: "))
        status = "succeeded"
        for i, spec in enumerate(calls):
            call = ToolCall(req.run_id, str(i), spec["tool"], spec["args"])
            decision = await authorizer(call)
            if decision.action == "defer":
                status = "interrupted"
                break
            if decision.action == "allow":
                self.allowed.append(call)
        return RunResult(run_id=req.run_id, runner_id="runner-gate", status=status, executor_id=req.executor_id,
                         executor_session_id="claude:native-1", started_at="t", ended_at="t", duration_ms=1,
                         final_text="all done")


def approval_scenario(body=plan(("Bash", CLEANUP))):  # noqa: B008 -- read, never mutated
    runner, policy = ApprovalRunner(), Policy({"Bash": "defer"})
    bridge = bridge_to(runner)
    first = execute(bridge, [event(body)], session(), Caps(policy))
    assert isinstance(first, Suspend) and not runner.allowed
    return runner, policy, bridge, first


def test_approval_covers_one_call_with_the_same_arguments() -> None:
    runner, policy, bridge, first = approval_scenario(plan(("Bash", {**CLEANUP, "description": "first wording"})))
    again = plan(("Bash", {**CLEANUP, "description": "second wording"}), ("Bash", CLEANUP))
    state = json.loads(first.session_checkpoint)
    state["prompt"] = again
    second = execute(
        bridge, [answer(first, True)], dataclasses.replace(suspended(first), checkpoint=json.dumps(state)), Caps(policy)
    )
    assert isinstance(second, Suspend) and second.request_id != first.request_id
    assert len(runner.allowed) == 1


def test_changed_arguments_are_asked_about_again_and_the_unused_approval_is_kept() -> None:
    runner, policy, bridge, first = approval_scenario()
    state = json.loads(first.session_checkpoint)
    state["prompt"] = plan(("Bash", {"command": "deploy --everywhere"}))
    second = execute(
        bridge, [answer(first, True)], dataclasses.replace(suspended(first), checkpoint=json.dumps(state)), Caps(policy)
    )
    assert isinstance(second, Suspend) and "everywhere" in second.prompt
    assert runner.allowed == []
    assert [g["request_id"] for g in json.loads(second.session_checkpoint)["grants"]] == [first.request_id]


def test_block_outranks_an_approval() -> None:
    runner, policy, bridge, first = approval_scenario()
    policy.decisions["Bash"] = "block"
    execute(bridge, [answer(first, True)], suspended(first), Caps(policy))
    assert runner.allowed == []


def test_retry_of_the_resumed_run_does_not_use_the_approval_twice() -> None:
    runner, policy, bridge, first = approval_scenario()
    events, sess = [answer(first, True)], suspended(first)
    execute(bridge, events, sess, Caps(policy))
    assert isinstance(execute(bridge, events, sess, Caps(policy)), Suspend)
    assert len(runner.allowed) == 1


def test_same_call_deferred_again_before_the_answer_keeps_the_open_request() -> None:
    _, policy, bridge, first = approval_scenario()
    second = execute(bridge, [event(plan(("Bash", CLEANUP)))], suspended(first), Caps(policy))
    assert isinstance(second, Suspend) and second.request_id == first.request_id


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
    schemas = Path(__file__).resolve().parents[2] / "schemas" / "runner" / "v1"

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
