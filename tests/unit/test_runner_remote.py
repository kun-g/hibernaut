# The read-only remote agent node (docs/runner.md §10): the node's service, what its CLI may read, and Hibernaut's side.
from __future__ import annotations

import asyncio
import os
import plistlib
import textwrap
import tomllib
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.app.bootstrap import open_runner
from agent_runtime.app.config import parse_config
from agent_runtime.app.projects import ProjectRouter
from runner.agent_service import AGENT_TOOLS, agent_driver, load_agent_service_config
from runner.daemon import RunnerDaemon
from runner.tool_gate import ToolGateBinding
from runner.workspace_guard import workspace_read_refusal
from tests.support.runner_pilot_support import ROOT, write_token
from tests.support.runner_composition import raw_config, relay
from tests.support.runner_tool_gate import Node, request
from tests.support.runner_remote import AGENT, _first_order, _first_order_query, remote


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
