# The read-only remote agent node (docs/runner.md §10): the node's service, what its CLI may read, and Hibernaut's side.
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import plistlib
import shlex
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.adapters.executor_cli import LocalCLIExecutor
from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.adapters.runner_pilot import RunnerPilotRelay
from agent_runtime.app.bootstrap import open_runner
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.app.projects import ProjectRouter
from runner.agent_service import AGENT_TOOLS, agent_driver, load_agent_service_config
from runner.daemon import RunnerDaemon
from runner.protocol import RunnerCapabilities, ToolCall, ToolDecision
from runner.service import load_service_config
from runner.tool_gate import ToolGateBinding
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport.ws_protocol import recv_json, send_json, server_handshake
from runner.workspace_guard import workspace_read_refusal
from tests.fake_gated_cli import install_gated_cli
from tests.runner_pilot_support import (
    ROOT,
    RUNNER_ID,
    SRC,
    commit,
    free_port,
    git,
    protocol_repo,
    write_token,
)
from tests.test_runner_composition import raw_config, relay
from tests.test_runner_pilot import OTHER, PILOT, Deployment, _runner_run
from tests.test_runner_safety import _config, _init_repo, _req
from tests.test_runner_tool_gate import Caps, Node, Policy, bridge_to, event, execute, plan, request, session

REMOTE_ID = "remote-node"
AGENT = "remote-claude"
INBOX = "hibernaut:83"


# --- what the node's CLI may read


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    root = tmp_path / "ws"
    (root / "src").mkdir(parents=True)
    (root / "src" / "a.py").write_text("a")
    (tmp_path / "secret").mkdir()
    (tmp_path / "secret" / "token").write_text("t")
    (root / "link").symlink_to(tmp_path / "secret" / "token")
    (root / "linkdir").symlink_to(tmp_path / "secret")
    (root / "loop").symlink_to(root / "loop")
    (tmp_path / "ws-other").mkdir()
    return root


@pytest.mark.parametrize(
    ("tool", "args"),
    [
        ("Read", {"file_path": "src/a.py"}),
        ("Read", {"file_path": "{ws}/src/a.py", "offset": 1, "limit": 20}),
        ("Read", {"file_path": "{ws}/src/../src/a.py"}),
        ("Read", {"file_path": "src/missing.py"}),
        ("Grep", {"pattern": "../token", "glob": "*.py", "output_mode": "content", "-n": True}),
        ("Grep", {"pattern": "x", "path": "src"}),
        ("Grep", {"pattern": "x", "path": "{ws}"}),
        ("Glob", {"pattern": "**/*.py"}),
        ("Glob", {"pattern": "*.py", "path": "src"}),
    ],
)
def test_reads_inside_the_workspace_are_left_to_the_policy(workdir: Path, tool: str, args: dict[str, Any]) -> None:
    filled = {k: v.replace("{ws}", str(workdir)) if isinstance(v, str) else v for k, v in args.items()}
    assert workspace_read_refusal(workdir, tool, filled) is None


@pytest.mark.parametrize(
    ("tool", "args"),
    [
        ("Read", {"file_path": "{tmp}/secret/token"}),
        ("Read", {"file_path": "../secret/token"}),
        ("Read", {"file_path": "{ws}/../secret/token"}),
        ("Read", {"file_path": "{ws}-other/x"}),
        ("Read", {"file_path": "link"}),
        ("Read", {"file_path": "linkdir/token"}),
        ("Read", {"file_path": "linkdir/../ws/src/a.py/../../../secret/token"}),
        ("Read", {"file_path": "loop"}),
        ("Read", {"file_path": "~/.ssh/id_ed25519"}),
        ("Read", {"file_path": "~"}),
        # The CLI trims a path before it opens it; judged untrimmed these would be relative.
        ("Read", {"file_path": " {tmp}/secret/token"}),
        ("Read", {"file_path": "\t{tmp}/secret/token"}),
        ("Read", {"file_path": "\ufeff{tmp}/secret/token"}),
        ("Read", {"file_path": " ~/x"}),
        ("Read", {"file_path": "src/a.py\x00"}),
        ("Read", {"file_path": ["src/a.py"]}),
        ("Read", {"file_path": None}),
        ("Read", {}),
        ("Read", {"file_path": "src/a.py", "also": "{tmp}/secret/token"}),
        ("Grep", {"pattern": "x", "path": "{tmp}/secret"}),
        ("Grep", {"pattern": "x", "path": "linkdir"}),
        ("Grep", {"pattern": "x", "paths": ["/"]}),
        ("Glob", {"pattern": "{tmp}/secret/*"}),
        ("Glob", {"pattern": "~/*"}),
        ("Glob", {"pattern": "src/../../secret/*"}),
        ("Glob", {"pattern": "{src,../secret}/*"}),
        ("Glob", {"pattern": " /etc/*"}),
        ("Glob", {"pattern": "*", "path": "/"}),
        ("Grep", {"pattern": "x", "glob": "/etc/*"}),
        ("Bash", {"command": "cat README.md"}),
        ("Write", {"file_path": "src/a.py", "content": ""}),
        ("WebFetch", {"url": "https://example.com"}),
        ("mcp__any__read", {"path": "src/a.py"}),
        ("read", {"file_path": "src/a.py"}),
    ],
)
def test_anything_else_is_refused_by_the_node(workdir: Path, tool: str, args: dict[str, Any]) -> None:
    def fill(value: Any) -> Any:
        return value.replace("{ws}", str(workdir)).replace("{tmp}", str(workdir.parent)) if isinstance(value, str) else value

    assert workspace_read_refusal(workdir, tool, {k: fill(v) for k, v in args.items()})


def test_the_node_refuses_before_hibernaut_is_asked(tmp_path: Path) -> None:
    node = Node(tmp_path)
    node.daemon = RunnerDaemon(
        node.config, drivers={"claude": agent_driver("claude", str(_fake_claude(tmp_path / "gated", node.log)))},
        tool_guard=workspace_read_refusal,
    )
    asked: list[ToolCall] = []

    async def authorize(call: ToolCall) -> ToolDecision:
        asked.append(call)
        return ToolDecision(action="allow")

    outside = str(tmp_path / "relay.token")
    req = request(readonly=True, prompt=plan(("Read", {"file_path": outside}), ("Bash", {"command": "id"}), ("Read", {"file_path": "a"})))
    result = asyncio.run(node.daemon.run(req, caller_id="hibernaut", authorizer=authorize))
    assert result.status == "succeeded", result.error_summary
    assert [(c.tool_name, c.args) for c in asked] == [("Read", {"file_path": "a"})]
    assert [e["args"] for e in node.entries("ran")] == [{"file_path": "a"}]
    assert [e["tool"] for e in node.entries("denied")] == ["Read", "Bash"]


# --- the node's service


def _fake_claude(directory: Path, log: Path) -> Path:
    fake = install_gated_cli(directory, "fake")
    wrapper = directory / "claude"
    wrapper.write_text(f'#!/bin/sh\nFAKE_LOG="{log}" exec "{fake}" "$@"\n', encoding="utf-8")
    wrapper.chmod(0o755)
    return wrapper


def agent_toml(path: Path, *, repo: Path, port: int, token_file: Path, command: Path) -> Path:
    path.write_text(
        f'[runner]\nrunner_id = "{REMOTE_ID}"\nmax_concurrent = 1\nworktree_backend = "git"\n'
        'interactive_surface = "none"\nreadonly_only = true\nstate_dir = "state"\nlogs_dir = "logs"\n'
        '[namespaces.hb]\nprefix = "hb"\nallowed_callers = ["hibernaut"]\n'
        f'[projects.hibernaut]\ncheckout_path = "{repo}"\ndefault_base_ref = "main"\n'
        f'[executors.{AGENT}]\ncli = "claude"\ncommand = "{command}"\n'
        f'[relay]\nurl = "ws://127.0.0.1:{port}/ws/runner"\ntoken_file = "{token_file}"\n',
        encoding="utf-8",
    )
    return path


@pytest.fixture
def node_config(tmp_path: Path) -> Path:
    protocol_repo(tmp_path / "repo")
    write_token(tmp_path / "relay.token")
    return agent_toml(
        tmp_path / "agent-runner.toml", repo=tmp_path / "repo", port=free_port(), token_file=tmp_path / "relay.token",
        command=_fake_claude(tmp_path / "bin", tmp_path / "cli.log"),
    )


def test_the_agent_service_offers_exactly_one_gated_executor(node_config: Path) -> None:
    cfg = load_agent_service_config(node_config)
    assert (cfg.runner.max_concurrent, cfg.runner.readonly_only, list(cfg.runner.projects)) == (1, True, ["hibernaut"])
    daemon = cfg.open_daemon()
    try:
        (executor,) = daemon.capabilities_sync().executors
        assert (executor.executor_id, executor.driver, executor.tool_authorization) == (AGENT, "claude", True)
    finally:
        daemon.close()
    assert cfg.tool_guard is workspace_read_refusal


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("max_concurrent = 1", "max_concurrent = 2"),
        ("readonly_only = true", "readonly_only = false"),
        ('worktree_backend = "git"', 'worktree_backend = "orca"'),
        ('["hibernaut"]', '["hibernaut", "other"]'),
        ("[relay]", '[projects.other]\ncheckout_path = "/tmp"\n[relay]'),
        ("[relay]", '[executors.second]\ncli = "claude"\ncommand = "/bin/sh"\n[relay]'),
        (f'[executors.{AGENT}]\ncli = "claude"', f'[executors.{AGENT}]\ncli = "grok"'),
        (f'[executors.{AGENT}]\ncli = "claude"', f"[executors.{AGENT}]"),
        (f"[executors.{AGENT}]", "[executors.runner-protocol-check]"),
        (f"[executors.{AGENT}]", '[executors."not a name"]'),
        ('cli = "claude"', 'cli = "claude"\nargs = ["--add-dir", "/"]'),
        ('cli = "claude"', 'cli = "claude"\nenv = { A = "b" }'),
        ('cli = "claude"', 'cli = "claude"\nmodel = "--add-dir"'),
        ('cli = "claude"\ncommand = "/', 'cli = "claude"\ncommand = "'),
        ("ws://127.0.0.1", "ws://relay.example.com"),
        ("ws://127.0.0.1", "ws://0.0.0.0"),
        ("ws://127.0.0.1", "wss://127.0.0.1"),
        ("[relay]", "[other]\na = 1\n[relay]"),
    ],
)
def test_the_agent_service_refuses_anything_wider(node_config: Path, old: str, new: str) -> None:
    text = node_config.read_text(encoding="utf-8")
    assert old in text
    node_config.write_text(text.replace(old, new, 1), encoding="utf-8")
    with pytest.raises(ValueError):
        load_agent_service_config(node_config)


def test_neither_service_takes_the_other_ones_configuration(node_config: Path) -> None:
    with pytest.raises(ValueError, match="executors"):
        load_service_config(node_config)
    text = node_config.read_text(encoding="utf-8")
    start, end = text.index("[executors."), text.index("[relay]")
    node_config.write_text(text[:start] + text[end:], encoding="utf-8")
    load_service_config(node_config)
    with pytest.raises(ValueError, match="executors"):
        load_agent_service_config(node_config)


def _agent_cli(config: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "runner.agent_service", "--config", str(config), *args],
        env={**os.environ, "PYTHONPATH": str(SRC)}, capture_output=True, text=True, timeout=30,
    )


def test_check_prints_capabilities_and_a_bad_command_or_token_exits_2(node_config: Path) -> None:
    proc = _agent_cli(node_config, "--check")
    assert proc.returncode == 0, proc.stderr
    caps = RunnerCapabilities.from_dict(json.loads(proc.stdout))
    assert (caps.runner_id, caps.slots.max_concurrent, [e.executor_id for e in caps.executors]) == (REMOTE_ID, 1, [AGENT])
    token = (node_config.parent / "relay.token").read_text().strip()
    (node_config.parent / "relay.token").chmod(0o644)
    loose = _agent_cli(node_config, "--check")
    (node_config.parent / "relay.token").chmod(0o600)
    (node_config.parent / "bin" / "claude").unlink()
    missing = _agent_cli(node_config, "--check")
    assert (loose.returncode, missing.returncode) == (2, 2)
    assert "chmod 600" in loose.stderr and "not an executable file" in missing.stderr
    assert token not in proc.stdout + proc.stderr + loose.stderr + missing.stderr


def test_the_cli_is_started_confined_and_only_under_the_gate(tmp_path: Path) -> None:
    driver = agent_driver(AGENT, "/opt/cli/claude", model="haiku")
    assert (driver.calls_tools, driver.tool_hook) == (True, "claude")
    gate = ToolGateBinding(socket_path=tmp_path / "s", config_dir=tmp_path)
    for sid in (None, f"{AGENT}:native-1"):
        spec = driver.build_command(request(executor_id=AGENT, executor_session_id=sid, prompt="@/etc/hosts"), tmp_path, tool_gate=gate)
        argv = list(spec.argv)
        assert argv[0] == "/opt/cli/claude" and "@/etc/hosts" not in argv and spec.stdin_text == "@/etc/hosts"
        assert "--restricted" in argv and "--strict-mcp-config" in argv and "--disable-slash-commands" in argv
        assert argv[argv.index("--tools") + 1] == ",".join(AGENT_TOOLS) == "Read,Grep,Glob"
        assert argv[argv.index("--permission-mode") + 1] == "dontAsk" and argv[argv.index("--model") + 1] == "haiku"
        assert not {"--add-dir", "--mcp-config", "--dangerously-skip-permissions"} & set(argv)
        assert ("--resume" in argv) is (sid is not None)
        # Without it "@<path>" in an event's text attaches that file to the prompt, with no tool call to authorize.
        assert spec.env["CLAUDE_CODE_DISABLE_ATTACHMENTS"] == "1"
    node = Node(tmp_path / "n")
    daemon = RunnerDaemon(node.config, drivers={AGENT: driver})
    refused = asyncio.run(daemon.run(request(executor_id=AGENT, readonly=True), caller_id="hibernaut"))
    assert (refused.status, refused.error_kind) == ("refused", "permission_denied")
    daemon.close()


def test_the_pilot_relay_does_not_admit_the_agent_node(node_config: Path, tmp_path: Path) -> None:
    pilot = RunnerPilotRelay(runner_id=REMOTE_ID, project_key="hibernaut", port=free_port(), token_file=tmp_path / "relay.token")
    daemon = load_agent_service_config(node_config).open_daemon()
    try:
        assert pilot._refusal(daemon.capabilities_sync()) == "the pilot node must offer the protocol check only"
    finally:
        daemon.close()


# --- a link that goes silent without closing, as a forwarded connection does


@pytest.mark.parametrize("stage", ["handshake", "after welcome"])
def test_the_node_gives_up_a_silent_connection_and_dials_again(tmp_path: Path, stage: str) -> None:
    _init_repo(tmp_path / "repo")
    daemon = RunnerDaemon(_config(tmp_path, tmp_path / "repo"), drivers={})

    async def scenario() -> None:
        dials = 0
        again = asyncio.Event()

        async def hub(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            nonlocal dials
            dials += 1
            if dials == 2:
                again.set()
            if stage == "after welcome":
                await server_handshake(reader, writer, expected_path="/ws/runner")
                await recv_json(reader, writer, expect_masked=True, send_masked=False)
                await send_json(
                    writer, {"type": "welcome", "runner_id": "r", "conn_epoch": dials, "heartbeat_sec": 0.05}, mask=False
                )
            await reader.read()
            writer.close()

        server = await asyncio.start_server(hub, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        agent = RunnerRelayAgent(
            daemon=daemon, hub_url=f"ws://127.0.0.1:{port}/ws/runner", token="t" * 32, reconnect_delays=(0.05,),
            handshake_timeout_seconds=0.3, min_silence_seconds=0.3,
        )
        await agent.start()
        try:
            await asyncio.wait_for(again.wait(), timeout=10)
        finally:
            await agent.stop()
            server.close()

    asyncio.run(scenario())
    daemon.close()


def test_heartbeats_keep_an_idle_connection(tmp_path: Path) -> None:
    from runner.registry import WorkspaceAffinityStore
    from runner.transport.relay_hub import RunnerRelayHub

    _init_repo(tmp_path / "repo")
    daemon = RunnerDaemon(_config(tmp_path, tmp_path / "repo"), drivers={})
    affinity = WorkspaceAffinityStore(tmp_path / "a.db")

    async def scenario() -> None:
        hub = RunnerRelayHub(shared_token="t" * 32, affinity=affinity, heartbeat_seconds=0.2)
        host, port = await hub.start()
        agent = RunnerRelayAgent(
            daemon=daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="t" * 32, min_silence_seconds=0.6
        )
        await agent.start()
        await agent.wait_connected()
        await asyncio.sleep(1.5)
        assert agent.conn_epoch == 1 and len(hub.registry.list_online()) == 1
        await agent.stop()
        await hub.stop()

    asyncio.run(scenario())
    affinity.close()
    daemon.close()


# --- Hibernaut's side


def test_relay_token_file_and_readonly_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIBERNAUT_RUNNER_TOKEN", "left-in-the-environment")
    write_token(tmp_path / "relay.token")
    cfg = parse_config(raw_config(tmp_path, relay(token_file="relay.token", readonly_only=True)), base_dir=tmp_path)
    assert cfg.runner.token_file == str((tmp_path / "relay.token").resolve()) and cfg.runner.readonly_only
    handle = open_runner(cfg)
    assert handle is not None and "HIBERNAUT_RUNNER_TOKEN" not in os.environ
    bridge = handle.bridge("claude", cfg.projects["hibernaut"])
    assert bridge._readonly is True  # type: ignore[attr-defined]
    asyncio.run(handle.stop())
    (tmp_path / "relay.token").chmod(0o640)
    with pytest.raises(ValueError, match="chmod 600"):
        open_runner(cfg)
    assert parse_config(raw_config(tmp_path, relay())).runner.readonly_only is False


@pytest.mark.parametrize(
    ("section", "message"),
    [
        ({"token_file": "a", "token_env": "B"}, "token_file"),
        ({"token_file": ""}, "runner.token_file"),
        ({"token_file": 1}, "runner.token_file"),
        ({"readonly_only": "yes"}, "runner.readonly_only"),
    ],
)
def test_unusable_relay_settings_are_refused(tmp_path: Path, section: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_config(raw_config(tmp_path, relay(**section)), base_dir=tmp_path)


def test_the_two_relays_do_not_share_a_token(tmp_path: Path) -> None:
    pilot = {
        "enabled": True, "inbox_id": "hibernaut:81", "runner_id": "p", "project_key": "hibernaut", "port": 8791,
        "token_file": "./relay.token",
    }
    raw = raw_config(tmp_path, relay(token_file="relay.token", listen_port=8790), runner_pilot=pilot)
    with pytest.raises(ValueError, match="token_file"):
        parse_config(raw, base_dir=tmp_path)
    raw["runner"]["token_file"] = "other.token"
    parse_config(raw, base_dir=tmp_path)


def test_readonly_only_makes_every_run_readonly(tmp_path: Path) -> None:
    node = Node(tmp_path)
    outcome = execute(bridge_to(node.client, readonly=True), [event(plan(("Bash", {"command": "ls"})))], session(), Caps(Policy("allow")))
    assert outcome.messages == ("all done",)
    (start,) = node.entries("start")
    assert ".readonly" in Path(start["cwd"]).parts


def test_only_the_named_executor_leaves_this_machine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIBERNAUT_RUNNER_TOKEN", "t" * 32)
    raw = raw_config(tmp_path, relay(executors=[AGENT]))
    raw["executors"][AGENT] = {"command": "/usr/bin/false"}
    (tmp_path / "ops").mkdir()
    raw["runtime"]["inbox_bindings"] = [
        {"pattern": "ops:.*", "workdir": str(tmp_path / "ops"), "checkout_access": "readonly", "default_agent": "claude"}
    ]
    daemon = build_daemon_from_config(parse_config(raw))
    try:
        executors = daemon.pool._worker_factory("proj:hibernaut", "w")._executors  # type: ignore[attr-defined]
        assert type(executors[AGENT]) is RunnerExecutorBridge and type(executors["claude"]) is LocalCLIExecutor
        # Outside every project there is no relay for it, and its local command does nothing.
        daemon.handler.ingest(
            inbox_id="ops:1", source="t", external_id="1", payload={"text": "x", "agent_id": AGENT}, correlation_key="ops:1"
        )
        asyncio.run(daemon.pool._worker_factory("ops:1", "w").process_inbox("ops:1"))  # type: ignore[attr-defined]
        (sent,) = daemon.db.fetchall("SELECT content FROM outbox WHERE inbox_id = 'ops:1'")
        assert sent["content"].startswith(f"[CLI:{AGENT} exit=1]")
    finally:
        daemon.close()


# --- deployment examples


def _example(marker: str) -> dict[str, Any]:
    text = (ROOT / "deploy/runner/REMOTE.md").read_text(encoding="utf-8")
    return tomllib.loads(text.split(f"<!-- {marker} -->", 1)[1].split("```toml", 1)[1].split("```", 1)[0])


def test_remote_examples_fit_the_services_and_each_other(tmp_path: Path) -> None:
    cfg = load_agent_service_config(ROOT / "deploy/runner/agent-runner.example.toml")
    node = plistlib.loads((ROOT / "deploy/runner/com.hibernaut.runner-agent.plist.example").read_bytes())
    assert node["ProgramArguments"][1:4] == ["-m", "runner.agent_service", "--config"]
    assert node["KeepAlive"] is True and node["RunAtLoad"] is True
    assert set(node["EnvironmentVariables"]) == {"PYTHONPATH", "PATH"}
    raw = _example("hibernaut.toml")
    hb = parse_config(
        raw_config(tmp_path, raw["runner"], runner_pilot=raw["runner_pilot"])
        | {"executors": {"claude": {"command": "claude"}, **raw["executors"]}}
    )
    assert hb.runner.enabled and hb.runner.mode == "relay" and hb.runner.readonly_only
    assert hb.runner.listen_host == "127.0.0.1" and hb.runner.listen_port != hb.runner_pilot.port
    assert hb.runner.executors == tuple(cfg.drivers) == (AGENT,)
    assert hb.runner.allow_tools == frozenset(AGENT_TOOLS)
    # Wherever the relay does not take the executor (no project, Runner off), its local command does nothing.
    assert hb.executors[AGENT].command == "/usr/bin/false" and hb.executors[AGENT].max_concurrency == 1
    tunnel = plistlib.loads((ROOT / "deploy/runner/com.hibernaut.runner-tunnel.plist.example").read_bytes())
    argv = tunnel["ProgramArguments"]
    local, remote = cfg.relay_url.split("/")[2], f"127.0.0.1:{hb.runner.listen_port}"
    assert argv[0] == "/usr/bin/ssh" and argv[argv.index("-R") + 1] == f"{local}:{remote}"
    options = {argv[i + 1] for i, a in enumerate(argv) if a == "-o"}
    assert {
        "BatchMode=yes", "ExitOnForwardFailure=yes", "StrictHostKeyChecking=yes", "IdentitiesOnly=yes",
        "ServerAliveInterval=15", "ServerAliveCountMax=3",
    } <= options
    assert "-N" in argv and "-A" not in argv


def _first_order() -> dict[str, Any]:
    text = (ROOT / "deploy/runner/REMOTE.md").read_text(encoding="utf-8")
    body = text.split("/events", 1)[1].split("-d '", 1)[1].split("'", 1)[0]
    order: dict[str, Any] = json.loads(body)
    return order


def _first_order_query() -> str:
    text = (ROOT / "deploy/runner/REMOTE.md").read_text(encoding="utf-8")
    return text.split('sqlite3 ledger.db "', 1)[1].split('"', 1)[0]


def test_the_documented_first_order_reaches_the_remote_executor_in_the_example_project() -> None:
    document = (ROOT / "deploy/runner/REMOTE.md").read_text(encoding="utf-8")
    added = document.split("<!-- hibernaut.toml -->", 1)[1].split("```toml", 1)[1].split("```", 1)[0]
    cfg = parse_config(tomllib.loads((ROOT / "hibernaut.example.toml").read_text(encoding="utf-8") + textwrap.dedent(added)))
    order = _first_order()
    inbox = order["inbox_id"]
    # The pilot takes its own inbox over; this one has to be the project's by the project's own rules.
    assert inbox != cfg.runner_pilot.inbox_id
    project = ProjectRouter(cfg.projects.values()).resolve(inbox)
    assert project is not None and project.name == "hibernaut"
    node = load_agent_service_config(ROOT / "deploy/runner/agent-runner.example.toml")
    assert [project.name] == list(node.runner.projects)
    binding = project.route(inbox).binding
    assert binding is None or binding.allows(AGENT)
    assert order["payload"]["agent_id"] == AGENT
    # The two places where the order is looked up again name the same inbox.
    assert f"inbox_id = '{inbox}'" in _first_order_query()
    assert f"owner {inbox})" in document and "hibernaut:runner-agent" not in document.replace(inbox, "")


@pytest.mark.parametrize(
    ("inbox", "answer"),
    [
        (None, "hibernaut True"),
        ("proj:hibernaut:sde:1", "hibernaut False"),
        ("hibernaut:runner-agent", "None None"),
    ],
)
def test_the_documented_check_tells_whether_an_inbox_can_be_ordered_from(tmp_path: Path, inbox: str | None, answer: str) -> None:
    document = (ROOT / "deploy/runner/REMOTE.md").read_text(encoding="utf-8")
    added = document.split("<!-- hibernaut.toml -->", 1)[1].split("```toml", 1)[1].split("```", 1)[0]
    (tmp_path / "hibernaut.toml").write_text(
        (ROOT / "hibernaut.example.toml").read_text(encoding="utf-8") + textwrap.dedent(added), encoding="utf-8"
    )
    check = document.split("## 验证（首单）", 1)[1].split("```bash\n", 1)[1].split("```", 1)[0].strip()
    argv = shlex.split(check)
    assert argv[:3] == ["PYTHONPATH=src", "python", "-c"] and argv[4:] == ["hibernaut.toml", _first_order()["inbox_id"]]
    done = subprocess.run(
        [sys.executable, "-c", argv[3], "hibernaut.toml", inbox or argv[5]],
        cwd=tmp_path, env={**os.environ, "PYTHONPATH": str(ROOT / "src")}, capture_output=True, text=True, timeout=60,
    )
    assert (done.returncode, done.stdout.strip()) == (0, answer), done.stderr


# --- end to end: Hibernaut, the pilot node and the remote node as separate processes


class RemoteDeployment(Deployment):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.node = root / "node"
        self.node_repo = self.node / "repo"
        protocol_repo(self.node_repo)
        (self.node_repo / "README.md").write_text("only on the node", encoding="utf-8")
        commit(self.node_repo, "node only")
        self.node_token = write_token(self.node / "relay.token")
        (root / "node-relay.token").touch(mode=0o600)
        (root / "node-relay.token").write_text(self.node_token, encoding="ascii")
        self.remote_port = free_port()
        self.cli_log, self.node_log = self.node / "cli.log", self.node / "node.log"
        self.node_config = agent_toml(
            self.node / "agent-runner.toml", repo=self.node_repo, port=self.remote_port,
            token_file=self.node / "relay.token", command=_fake_claude(self.node / "bin", self.cli_log),
        )
        self.node_proc: subprocess.Popen[bytes] | None = None
        self.runner_enabled = True

    def write(self, *, enabled: bool = True, extra: str = "") -> None:
        super().write(enabled=enabled, extra=(
            f'[executors.{AGENT}]\ncommand = "/usr/bin/false"\nmax_concurrency = 1\n'
            f'[runner]\nenabled = {str(self.runner_enabled).lower()}\nmode = "relay"\nexecutors = ["{AGENT}"]\n'
            'readonly_only = true\nallow_tools = ["Read", "Grep", "Glob"]\nnode_wait_seconds = 8\n'
            f'listen_port = {self.remote_port}\ntoken_file = "node-relay.token"\n{extra}'
        ))

    def stop_hb(self) -> None:
        assert self.hb is not None
        self.hb.terminate()
        self.hb.terminate()
        self.hb.wait(timeout=20)
        self.hb = None

    def start_node(self) -> None:
        with self.node_log.open("ab") as stream:
            self.node_proc = subprocess.Popen(
                [sys.executable, "-m", "runner.agent_service", "--config", str(self.node_config)], cwd=self.node,
                env={**os.environ, "PYTHONPATH": str(SRC), "GH_TOKEN": "synthetic-service-token"},
                stdout=stream, stderr=stream,
            )
        deadline = time.monotonic() + 20
        while "relay connected" not in self.node_log.read_text():
            assert self.node_proc.poll() is None and time.monotonic() < deadline, self.node_log.read_text()
            time.sleep(0.05)

    def stop_node(self) -> int:
        assert self.node_proc is not None
        self.node_proc.send_signal(signal.SIGTERM)
        return self.node_proc.wait(timeout=20)

    def close(self) -> None:
        if self.node_proc is not None and self.node_proc.poll() is None:
            self.stop_node()
        super().close()

    def cli(self, kind: str) -> list[dict[str, Any]]:
        if not self.cli_log.exists():
            return []
        return [e for e in map(json.loads, self.cli_log.read_text().splitlines()) if e["kind"] == kind]

    def ask(self, *calls: tuple[str, dict[str, Any]], **payload: Any) -> str:
        return self.post(INBOX, agent_id=AGENT, **{"text": plan(*calls), **payload})

    def node_run_log(self, run: sqlite3.Row, *, owner: str = INBOX) -> Path:
        line = next(x for x in self.ledger_log(run).splitlines() if x.startswith("runner://"))
        location, *pairs = line.split()
        facts = dict(p.split("=", 1) for p in pairs)
        path = self.node / "logs" / location.removeprefix(f"runner://{REMOTE_ID}/")
        assert path.name == f"{facts['run_id']}.log"
        assert hashlib.sha256(path.read_bytes()).hexdigest() == facts["sha256"]
        assert f"run {facts['run_id']} accepted (executor {AGENT}, owner {owner})" in self.node_log.read_text()
        return path


@pytest.fixture
def remote(tmp_path: Path) -> Iterator[RemoteDeployment]:
    deployment = RemoteDeployment(tmp_path)
    try:
        yield deployment
    finally:
        deployment.close()
        logs = [p for p in (deployment.hb_log, deployment.runner_log, deployment.node_log, *tmp_path.rglob("run_*.log")) if p.exists()]
        secrets = (deployment.token, deployment.node_token, "synthetic-service-token")
        assert [p.name for p in logs if any(s in p.read_text() for s in secrets)] == []


def test_a_query_runs_on_the_remote_node_beside_the_pilot_and_the_original_path(remote: RemoteDeployment) -> None:
    remote.write()
    remote.start_hb()
    remote.start_runner()
    remote.start_node()
    token_path = str(remote.node / "relay.token")
    # No "readonly" in the event: the route itself is read-only.
    remote.finished(remote.ask(
        ("Read", {"file_path": "README.md"}), ("Read", {"file_path": token_path}), ("Bash", {"command": "git log"}),
        ("Grep", {"pattern": "node"}),
    ))
    assert remote.outbox(INBOX) == ["all done"]
    (run,) = remote.runs(INBOX)
    assert (run["executor_id"], run["runner_id"], run["outcome_kind"], run["emit_status"]) == (AGENT, REMOTE_ID, "emit", "done")
    assert "all done" in remote.node_run_log(run).read_text()
    (start,) = remote.cli("start")
    cwd = Path(start["cwd"])
    assert cwd.is_relative_to(remote.node.resolve()) and ".readonly" in cwd.parts and not cwd.exists()
    assert [(e["tool"], e["args"]) for e in remote.cli("ran")] == [("Read", {"file_path": "README.md"}), ("Grep", {"pattern": "node"})]
    assert [e["tool"] for e in remote.cli("denied")] == ["Read", "Bash"]
    assert len(git("worktree", "list", cwd=remote.node_repo).splitlines()) == 1
    assert remote.rows("SELECT * FROM workspace_affinity", db="runner/affinity.db") == []

    # The pilot and the original path, at the same time and afterwards.
    asked, checked, plain = remote.ask(("Read", {"file_path": "README.md"})), remote.check(), remote.post(OTHER)
    remote.finished(asked, checked, plain)
    assert (remote.runs(INBOX)[-1]["runner_id"], remote.runs(PILOT)[-1]["runner_id"], remote.runs(OTHER)[-1]["runner_id"]) == (
        REMOTE_ID, RUNNER_ID, "local")
    assert "Runner 协议文件校验通过" in remote.outbox(PILOT)[-1] and _runner_run(remote, remote.runs(PILOT)[-1]).is_file()
    remote.assert_original_path("proj:hibernaut")
    # An event that names nobody stays on the original executor, even in the inbox the remote one has been used in.
    remote.assert_original_path(INBOX)
    # Nor does another executor's event reach its command by naming it as the CLI to start.
    remote.finished(remote.post(OTHER, executor=AGENT))
    assert remote.outbox(OTHER)[-1].startswith(f"[CLI:{AGENT} exit=1]")
    assert len(remote.cli("start")) == 2


def test_the_documented_first_order_is_found_again_in_the_ledger_and_on_the_node(remote: RemoteDeployment) -> None:
    remote.write()
    example = tomllib.loads((ROOT / "hibernaut.example.toml").read_text(encoding="utf-8"))["projects"]["hibernaut"]
    text = remote.config.read_text(encoding="utf-8")
    rules = 'inbox_id = ["proj:hibernaut", "hibernaut:.*"]'
    assert rules in text
    # The example project's inboxes, and its bindings with this deployment's only other agent in their place.
    bindings = "".join(
        f"[[projects.hibernaut.inbox_bindings]]\npattern = {json.dumps(b['pattern'])}\ndefault_agent = \"cli\"\nagents = [\"cli\"]\n"
        for b in example["inbox_bindings"]
    )
    remote.config.write_text(text.replace(rules, f"inbox_id = {json.dumps(example['inbox_id'])}\n{bindings}"), encoding="utf-8")
    remote.start_hb()
    remote.start_node()
    order = _first_order()
    data = remote._post(order)
    assert data is not None
    remote.finished(str(data["event_id"]))
    (row,) = remote.rows(_first_order_query(), db="ledger.db")
    assert (row["executor_id"], row["runner_id"], row["outcome_kind"], row["emit_status"]) == (AGENT, REMOTE_ID, "emit", "done")
    assert remote.node_run_log(row, owner=order["inbox_id"]).is_file()


def test_a_run_cut_off_with_its_node_is_run_again_once_the_node_is_back(remote: RemoteDeployment) -> None:
    remote.write()
    remote.start_hb()
    remote.start_node()
    held = remote.ask(("sleep", {"seconds": 8}))
    remote.until(lambda: remote.cli("start"))
    assert remote.stop_node() == 0
    assert len(git("worktree", "list", cwd=remote.node_repo).splitlines()) == 1
    remote.assert_original_path(OTHER)
    remote.node_log.write_text("")
    remote.start_node()
    remote.until(lambda: remote.status(held) == "done", timeout=60)
    assert remote.outbox(INBOX) == ["all done"] and len(remote.cli("start")) == 2
    (run,) = remote.runs(INBOX)
    assert (run["runner_id"], run["emit_status"]) == (REMOTE_ID, "done") and remote.node_run_log(run).is_file()


def test_without_a_node_only_its_own_runs_fail(remote: RemoteDeployment) -> None:
    remote.write()
    remote.start_hb()
    remote.start_runner()
    asked = remote.ask(("Read", {"file_path": "README.md"}))
    remote.finished(remote.check())
    remote.assert_original_path(OTHER)
    remote.until(lambda: remote.status(asked) == "failed")
    (run,) = remote.runs(INBOX)
    # Nothing ran: the ledger's runner_id is its default for a Run no node answered for, as docs/runner.md §10 says.
    assert (run["outcome_kind"], run["runner_id"]) == ("error", "local")
    assert "no online runner matches" in run["error_summary"]
    assert remote.cli("start") == [] and "all done" not in remote.outbox(INBOX)


def test_turning_the_runner_off_restores_every_route_and_never_runs_the_executor_here(remote: RemoteDeployment) -> None:
    remote.write()
    remote.start_hb()
    remote.start_runner()
    remote.start_node()
    remote.finished(remote.ask(("Read", {"file_path": "README.md"})))
    remote.stop_hb()
    remote.runner_enabled = False
    remote.write()
    remote.start_hb()
    remote.until(lambda: "relay connection lost" in remote.node_log.read_text())
    remote.finished(remote.ask(("Read", {"file_path": "README.md"})))
    assert remote.outbox(INBOX)[-1].startswith(f"[CLI:{AGENT} exit=1]")
    assert remote.runs(INBOX)[-1]["emit_status"] == "failed" and len(remote.cli("start")) == 1
    remote.finished(remote.check())
    assert remote.runs(PILOT)[-1]["runner_id"] == RUNNER_ID
    remote.assert_original_path(OTHER)
    remote.assert_original_path("proj:hibernaut")


def test_the_node_refuses_a_writable_run_whatever_hibernaut_asks(tmp_path: Path) -> None:
    log = tmp_path / "cli.log"
    _init_repo(tmp_path / "repo")
    write_token(tmp_path / "relay.token")
    config = agent_toml(
        tmp_path / "agent-runner.toml", repo=tmp_path / "repo", port=free_port(), token_file=tmp_path / "relay.token",
        command=_fake_claude(tmp_path / "bin", log),
    )
    daemon = load_agent_service_config(config).open_daemon()

    async def allow(call: ToolCall) -> ToolDecision:
        return ToolDecision(action="allow")

    result = asyncio.run(daemon.run(_req("run_w", executor_id=AGENT), caller_id="hibernaut", authorizer=allow))
    assert (result.status, result.error_kind) == ("refused", "permission_denied") and not log.exists()
    daemon.close()
