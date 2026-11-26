from __future__ import annotations

import asyncio
import io
import os
import socket
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.adapters.executor_cli import LocalCLIExecutor
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.app import main as main_module
from agent_runtime.app.bootstrap import RunnerHandle, open_runner
from agent_runtime.app.config import RunnerLinkConfig, parse_config
from agent_runtime.app.main import build_daemon_from_config
from tests.support.runner_composition import INBOX, TOKEN_ENV, in_process, raw_config, relay


def test_runner_is_off_by_default(tmp_path: Path) -> None:
    assert parse_config({}).runner == RunnerLinkConfig() and not RunnerLinkConfig().enabled
    assert parse_config({"runner": {"enabled": False}}).runner.enabled is False
    assert open_runner(parse_config(raw_config(tmp_path, None))) is None


@pytest.mark.parametrize("section", [None, {"enabled": False, "mode": "relay", "allow_tools": ["Bash"]}])
def test_disabled_runner_leaves_the_composition_root_as_it_was(
    tmp_path: Path, section: dict[str, Any] | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "t0k")
    daemon = build_daemon_from_config(parse_config(raw_config(tmp_path, section)))
    try:
        assert daemon.runner is None
        worker = daemon.pool._worker_factory(INBOX, "w")  # type: ignore[attr-defined]
        assert type(worker._executors["claude"]) is LocalCLIExecutor
        assert worker._permissions.check("Bash", {}).action == "defer"
        assert os.environ[TOKEN_ENV] == "t0k"
        assert not (tmp_path / "runner").exists()
    finally:
        daemon.close()


def test_enabled_runner_replaces_only_the_listed_cli_executors(tmp_path: Path) -> None:
    raw = raw_config(tmp_path, in_process(executors=["claude"]))
    raw["executors"]["claude-local"] = raw["executors"]["claude"]
    daemon = build_daemon_from_config(parse_config(raw))
    try:
        worker = daemon.pool._worker_factory(INBOX, "w")  # type: ignore[attr-defined]
        assert type(worker._executors["claude"]) is RunnerExecutorBridge
        assert type(worker._executors["claude-local"]) is LocalCLIExecutor
        assert worker._permissions.check("Bash", {"command": "ls"}).action == "allow"
        assert worker._permissions.check("Bash", {"command": "rm -rf /"}).action == "block"
        assert worker._permissions.check("Write", {}).action == "defer"
        # An inbox of no project has no checkout on the runner.
        other = daemon.pool._worker_factory("proj:unknown", "w")  # type: ignore[attr-defined]
        assert type(other._executors["claude"]) is LocalCLIExecutor
    finally:
        daemon.close()


def test_allowed_and_blocked_tools_extend_the_default_policy(tmp_path: Path) -> None:
    raw = raw_config(tmp_path, in_process(allow_tools=["Bash", "Read"], block_tools=["Read", "WebFetch"]))
    daemon = build_daemon_from_config(parse_config(raw))
    try:
        check = daemon.pool._worker_factory(INBOX, "w")._permissions.check  # type: ignore[attr-defined]
        assert {t: check(t, {}).action for t in ("Bash", "Read", "WebFetch", "Edit", "sde.launch", "deploy_production")} == {
            "Bash": "allow", "Read": "block", "WebFetch": "block", "Edit": "defer", "sde.launch": "allow",
            "deploy_production": "defer",
        }
        assert AutoModePermissionAdapter().check("sde.launch", {}).action == "allow"
    finally:
        daemon.close()


def test_relay_needs_its_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    with pytest.raises(ValueError, match=TOKEN_ENV):
        build_daemon_from_config(parse_config(raw_config(tmp_path, relay())))


def test_relay_that_cannot_listen_fails_the_start(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, "t0k")
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        daemon = build_daemon_from_config(parse_config(raw_config(tmp_path, relay(listen_port=taken.getsockname()[1]))))
        with pytest.raises(OSError):
            asyncio.run(daemon.run(stop_event=asyncio.Event()))
        daemon.close()


@pytest.mark.parametrize(
    ("section", "message"),
    [
        ("on", "must be a table"),
        ({"enable": True}, "unknown keys: enable"),
        ({"enabled": "yes"}, "runner.enabled"),
        ({"mode": "remote"}, "runner.mode"),
        ({"timeout_seconds": 0}, "runner.timeout_seconds"),
        ({"max_concurrent": 0}, "runner.max_concurrent"),
        ({"listen_port": 70000}, "runner.listen_port"),
        ({"worktree_backend": "svn"}, "runner.worktree_backend"),
        ({"allow_tools": "Bash"}, "runner.allow_tools"),
        ({"block_tools": [""]}, "runner.block_tools"),
        ({"node_wait_seconds": -1}, "runner.node_wait_seconds"),
        ({"runner_id": " "}, "runner.runner_id"),
        ({"token_env": ""}, "runner.token_env"),
        ({"enabled": True, "executors": ["missing"]}, "missing"),
        ({"enabled": True, "executors": ["sde"]}, "role"),
        ({"enabled": True, "executors": ["dsh"]}, "no tool hook"),
        ({"enabled": True, "executors": []}, "runner.executors"),
    ],
)
def test_unusable_settings_are_refused(tmp_path: Path, section: Any, message: str) -> None:
    raw = raw_config(tmp_path, None)
    raw["executors"]["dsh"] = {"command": "dsh", "args": ["-c"]}
    raw["executors"]["claude-code"] = {}
    raw["agents"] = {"sde": {"executor": "claude-code", "role": "sde", "review": False}}
    raw["runner"] = section
    with pytest.raises(ValueError, match=message):
        parse_config(raw)


@pytest.mark.parametrize(
    "args",
    [
        ["--permission-mode", "plan"], ["--permission-mode=plan"], ["--settings", "{}"], ["--dangerously-skip-permissions"],
        ["--bare"],
    ],
)
def test_arguments_that_would_override_the_hook_are_refused(tmp_path: Path, args: list[str]) -> None:
    raw = raw_config(tmp_path, in_process())
    raw["executors"]["claude"]["args"] = ["-p", "{prompt}", *args]
    with pytest.raises(ValueError, match=args[0].split("=")[0]):
        parse_config(raw)
    raw["executors"]["claude"]["args"] = ["-p", "{prompt}"]
    raw["executors"]["claude"]["resume_args"] = ["-p", "{prompt}", *args]
    with pytest.raises(ValueError, match="resume_args"):
        parse_config(raw)


def test_by_default_every_gateable_cli_executor_runs_on_the_runner(tmp_path: Path) -> None:
    raw = raw_config(tmp_path, {"enabled": True})
    raw["executors"]["dsh"] = {"command": "dsh", "args": ["-c"]}
    raw["executors"]["grok"] = {"command": "grok", "args": ["-p", "{prompt}"]}
    raw["agents"] = {"ops": {"executor": "claude"}}
    assert parse_config(raw).runner.executors == ("claude", "grok", "ops")


def test_relay_node_drivers_are_the_nodes_own(tmp_path: Path) -> None:
    raw = raw_config(tmp_path, relay(executors=["claude"]))
    raw["executors"]["claude"]["args"] = ["-p", "{prompt}", "--permission-mode", "plan"]
    assert parse_config(raw).runner.executors == ("claude",)


def test_mcp_stdio_companion_does_not_open_a_second_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "hibernaut.toml"
    path.write_text(
        f'[runtime]\ndb_path = "{tmp_path}/hb.db"\nmemory_root = "{tmp_path}/memory"\n'
        f'[executors.claude]\ncommand = "claude"\nargs = ["-p", "{{prompt}}"]\n'
        '[runner]\nenabled = true\nmode = "relay"\n',
        encoding="utf-8",
    )
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    monkeypatch.setattr("sys.argv", ["hibernaut", "--config", str(path), "--mcp-stdio"])
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    main_module.main()
    assert not (tmp_path / "runner").exists()


def test_handle_start_and_stop_are_idempotent(tmp_path: Path) -> None:
    async def scenario() -> None:
        handle = open_runner(parse_config(raw_config(tmp_path, in_process())))
        assert isinstance(handle, RunnerHandle)
        await handle.start()
        await handle.start()
        assert handle.started
        await handle.stop()
        await handle.stop()
        assert not handle.started

    asyncio.run(scenario())
