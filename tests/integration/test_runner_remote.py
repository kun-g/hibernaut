# The read-only remote agent node (docs/runner.md §10): the node's service, what its CLI may read, and Hibernaut's side.
from __future__ import annotations

import asyncio
import json
import os
import shlex
import subprocess
import sys
import textwrap
import tomllib
import uuid
from pathlib import Path

import pytest

from agent_runtime.adapters.executor_cli import LocalCLIExecutor
from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.adapters.runner_pilot import RunnerPilotRelay
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from runner.agent_service import agent_driver, load_agent_service_config
from runner.daemon import RunnerDaemon
from runner.protocol import RunnerCapabilities, ToolCall, ToolDecision
from runner.service import load_service_config
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport.ws_protocol import recv_json, send_json, server_handshake
from runner.workspace_guard import workspace_read_refusal
from tests.support.runner_pilot_support import ROOT, RUNNER_ID, SRC, free_port, git, protocol_repo, write_token
from tests.support.runner_composition import raw_config, relay
from tests.support.runner_pilot import OTHER, PILOT, _runner_run
from tests.support.runner_safety import _config, _init_repo, _req
from tests.support.runner_tool_gate import Caps, Node, Policy, bridge_to, event, execute, plan, request, session
from tests.support.runner_remote import (
    AGENT,
    INBOX,
    REMOTE_ID,
    RemoteDeployment,
    _fake_claude,
    _first_order,
    _first_order_query,
    agent_toml,
)
from tests.support.runner_remote import remote  # noqa: F401 -- pytest fixture, requested by parameter name


def test_the_node_refuses_before_hibernaut_is_asked(tmp_path: Path) -> None:
    node = Node(tmp_path, real_worktree=True)
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


def test_the_pilot_relay_does_not_admit_the_agent_node(node_config: Path, tmp_path: Path) -> None:
    pilot = RunnerPilotRelay(runner_id=REMOTE_ID, project_key="hibernaut", port=free_port(), token_file=tmp_path / "relay.token")
    daemon = load_agent_service_config(node_config).open_daemon()
    try:
        assert pilot._refusal(daemon.capabilities_sync()) == "the pilot node must offer the protocol check only"
    finally:
        daemon.close()


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


def test_readonly_only_makes_every_run_readonly(tmp_path: Path) -> None:
    node = Node(tmp_path, real_worktree=True)
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
    # The dropped connection must enter the transient retry lane; shorten only that lane's backoff.
    remote.launch_hb(("-c", "from functools import partial; from agent_runtime.app import main; "
                            "main.RunLedger = partial(main.RunLedger, retry_delays=(0.05, 0.15, 0.45)); "
                            "raise SystemExit(main.main())"))
    remote.start_node()
    release = remote.node / f"release-cli-{uuid.uuid4().hex}"
    assert not release.exists()
    held = remote.ask(("wait_for_file", {"path": str(release)}))
    remote.until(lambda: remote.cli("waiting"))
    assert remote.stop_node() == 0
    assert len(git("worktree", "list", cwd=remote.node_repo).splitlines()) == 1
    remote.assert_original_path(OTHER)
    assert remote.cli("released") == []
    release.touch(exist_ok=False)
    remote.node_log.write_text("")
    remote.start_node()
    remote.until(lambda: remote.status(held) == "done", timeout=60)
    assert remote.outbox(INBOX) == ["all done"] and len(remote.cli("start")) == 2
    assert len(remote.cli("released")) == 1
    (run,) = remote.runs(INBOX)
    assert (run["runner_id"], run["emit_status"]) == (REMOTE_ID, "done") and remote.node_run_log(run).is_file()


def test_without_a_node_only_its_own_runs_fail(remote: RemoteDeployment) -> None:
    remote.write(node_wait_seconds=0.1)
    remote.start_hb()
    remote.start_runner()
    asked = remote.ask(("Read", {"file_path": "README.md"}))
    remote.finished(remote.check())
    remote.assert_original_path(OTHER)
    # A failed Run's events are marked before its ledger row is written (a done Run's only after it).
    remote.until(lambda: remote.status(asked) == "failed" and remote.runs(INBOX))
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
