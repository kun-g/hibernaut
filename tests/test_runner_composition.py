from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import socket
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.adapters.executor_cli import LocalCLIExecutor
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.app import main as main_module
from agent_runtime.app.bootstrap import RunnerHandle, open_runner
from agent_runtime.app.config import HibernautConfig, RunnerLinkConfig, load_config, parse_config
from agent_runtime.app.main import AlwaysOnDaemon, build_daemon_from_config, serve_configured
from runner.transport.relay_client import RunnerRelayAgent
from tests.fake_gated_cli import install_gated_cli
from tests.test_runner_safety import _init_repo
from tests.test_runner_tool_gate import CLEANUP, Node, plan

INBOX = "proj:hibernaut"
TOKEN_ENV = "HIBERNAUT_RUNNER_TOKEN"


def raw_config(tmp_path: Path, runner: dict[str, Any] | None, **sections: Any) -> dict[str, Any]:
    repo = tmp_path / "repo"
    if not repo.exists():
        _init_repo(repo)
    command = install_gated_cli(tmp_path / "bin")
    raw: dict[str, Any] = {
        "runtime": {
            "db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory"), "max_concurrency": 2,
            "default_agent": "claude", "worktree_sweep_interval_seconds": 0,
        },
        "executors": {
            "claude": {
                "command": str(command), "args": ["-p", "{prompt}", "--output-format", "json"],
                "resume_args": ["-p", "{prompt}", "--resume", "{executor_session_id}", "--output-format", "json"],
                "env": {"FAKE_LOG": str(tmp_path / "cli.log")},
            },
        },
        "projects": {
            "hibernaut": {
                "inbox_id": INBOX, "repo": "kun-g/hibernaut", "checkout": str(repo), "worktree_backend": "git",
            },
        },
    }
    raw.update(sections)
    if runner is not None:
        raw["runner"] = runner
    return raw


def in_process(**kw: Any) -> dict[str, Any]:
    return {"enabled": True, "allow_tools": ["Bash"], **kw}


def relay(**kw: Any) -> dict[str, Any]:
    return {"enabled": True, "mode": "relay", "listen_port": 0, "allow_tools": ["Bash"], **kw}


def entries(tmp_path: Path, kind: str) -> list[dict[str, Any]]:
    log = tmp_path / "cli.log"
    if not log.exists():
        return []
    return [e for e in map(json.loads, log.read_text(encoding="utf-8").splitlines()) if e["kind"] == kind]


async def until(condition: Callable[[], Any], timeout: float = 20.0) -> Any:
    async with asyncio.timeout(timeout):
        while not (value := condition()):
            await asyncio.sleep(0.02)
        return value


@contextlib.asynccontextmanager
async def running(daemon: AlwaysOnDaemon) -> AsyncIterator[list[Sent]]:
    sent: list[Sent] = []

    async def capture(inbox_id: str, content: str, metadata: dict[str, Any]) -> None:
        sent.append(Sent(content, metadata))

    daemon.channel.send = capture  # type: ignore[method-assign]
    stop = asyncio.Event()
    run = asyncio.create_task(daemon.run(stop_event=stop, outbox_interval_seconds=0.02))
    try:
        if daemon.runner is not None:
            await until(lambda: daemon.runner is not None and daemon.runner.started)
        yield sent
    finally:
        stop.set()
        await asyncio.wait_for(run, 20)
        daemon.close()


@dataclass(frozen=True)
class Sent:
    content: str
    metadata: dict[str, Any]


def post(daemon: AlwaysOnDaemon, external_id: str, payload: dict[str, Any]) -> None:
    daemon.handler.ingest(
        inbox_id=INBOX, source="github", external_id=external_id, payload=payload, correlation_key="github:kun-g/hibernaut#7"
    )


# Off by default: nothing about the composition root changes.


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


# in_process: a Run through AgentWorker, RunnerExecutorBridge, RunnerDaemon and the CLI's hook.


def test_in_process_runner_executes_a_run_through_the_worker(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(raw_config(tmp_path, in_process())))

    async def scenario() -> None:
        async with running(daemon) as sent:
            post(daemon, "e1", {"body": plan(("Bash", CLEANUP))})
            await until(lambda: [m for m in sent if m.content == "all done"])
        assert daemon.runner is not None and not daemon.runner.started

    asyncio.run(scenario())
    assert [e["args"] for e in entries(tmp_path, "ran")] == [CLEANUP]
    (start,) = entries(tmp_path, "start")
    assert Path(start["cwd"]).resolve() != (tmp_path / "repo").resolve()
    assert (tmp_path / "runner").is_dir()


def test_in_process_deferred_call_suspends_until_the_approval_arrives(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(raw_config(tmp_path, in_process(allow_tools=[]))))

    async def scenario() -> None:
        async with running(daemon) as sent:
            post(daemon, "e1", {"body": plan(("Bash", CLEANUP))})
            (asked,) = await until(lambda: [m for m in sent if m.metadata.get("kind") == "approval_request"])
            assert "deploy --now" in asked.content
            assert entries(tmp_path, "ran") == []
            stored = daemon.sessions.find_by_request_id(asked.metadata["approval_request_id"])
            assert stored is not None and stored.status == "suspended" and stored.executor_session_id == "claude:native-1"
            post(daemon, "a1", {"approval_request_id": asked.metadata["approval_request_id"], "approved": True})
            await until(lambda: [m for m in sent if m.content == "all done"])

    asyncio.run(scenario())
    assert [e["args"] for e in entries(tmp_path, "ran")] == [CLEANUP]
    assert [e["session"] for e in entries(tmp_path, "start")] == ["native-1", "native-1"]
    assert "--resume" in entries(tmp_path, "start")[1]["argv"]


# relay: the same Run on a node that dials in.


def test_relay_runner_executes_a_run_on_a_connected_node(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, "t0k")
    node = Node(tmp_path / "node")
    daemon = build_daemon_from_config(parse_config(raw_config(tmp_path, relay())))
    assert TOKEN_ENV not in os.environ, "the relay token must not reach executor subprocesses"

    async def scenario() -> tuple[str, int]:
        async with running(daemon) as sent:
            assert daemon.runner is not None and daemon.runner.address is not None
            host, port = daemon.runner.address
            # Queued before any node is online: the Run waits for one.
            post(daemon, "e1", {"body": plan(("Bash", CLEANUP))})
            await asyncio.sleep(0.3)
            agent = RunnerRelayAgent(daemon=node.daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="t0k")
            await agent.start()
            try:
                await until(lambda: [m for m in sent if m.content == "all done"])
            finally:
                await agent.stop()
        return host, port

    host, port = asyncio.run(scenario())
    assert [e["args"] for e in node.entries("ran")] == [CLEANUP]
    assert (tmp_path / "runner" / "affinity.db").is_file()
    with pytest.raises(OSError):
        socket.create_connection((host, port), timeout=1).close()


def test_relay_deferred_call_suspends_until_the_approval_arrives(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TOKEN_ENV, "t0k")
    node = Node(tmp_path / "node")
    daemon = build_daemon_from_config(parse_config(raw_config(tmp_path, relay(allow_tools=[]))))

    async def scenario() -> None:
        async with running(daemon) as sent:
            assert daemon.runner is not None and daemon.runner.address is not None
            host, port = daemon.runner.address
            agent = RunnerRelayAgent(daemon=node.daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="t0k")
            await agent.start()
            try:
                post(daemon, "e1", {"body": plan(("Bash", CLEANUP))})
                (asked,) = await until(lambda: [m for m in sent if m.metadata.get("kind") == "approval_request"])
                post(daemon, "a1", {"approval_request_id": asked.metadata["approval_request_id"], "approved": True})
                await until(lambda: [m for m in sent if m.content == "all done"])
            finally:
                await agent.stop()

    asyncio.run(scenario())
    assert [e["args"] for e in node.entries("ran")] == [CLEANUP]


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


# Stop and reload.


def test_a_forced_stop_ends_the_run_before_the_runner_closes(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(raw_config(tmp_path, in_process())))

    async def scenario() -> None:
        stop, force = asyncio.Event(), asyncio.Event()
        run = asyncio.create_task(daemon.run(stop_event=stop, force_stop_event=force, outbox_interval_seconds=0.02))
        post(daemon, "e1", {"body": plan(("sleep", {"seconds": 60}))})
        await until(lambda: entries(tmp_path, "start"))
        stop.set()
        force.set()
        await asyncio.wait_for(run, 20)
        assert len(daemon.inbox.fetch_pending(INBOX)) == 1
        daemon.close()

    asyncio.run(scenario())


@contextlib.asynccontextmanager
async def serving(tmp_path: Path, raw: dict[str, Any], stderr: io.StringIO) -> AsyncIterator[Callable[[dict[str, Any]], None]]:
    path = tmp_path / "hibernaut.toml"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]

    def write(updated: dict[str, Any]) -> None:
        tables = [("channels", {"host": "127.0.0.1", "port": port}), ("runtime", updated["runtime"])]
        tables += [(f"{s}.{n}", t) for s in ("executors", "projects") for n, t in updated[s].items()]
        if "runner" in updated:
            tables.append(("runner", updated["runner"]))
        lines: list[str] = []
        for name, table in tables:
            flat = {k: v for k, v in table.items() if not isinstance(v, dict)}
            lines += [f"[{name}]", *(f"{k} = {json.dumps(v)}" for k, v in flat.items())]
            for key, nested in ((k, v) for k, v in table.items() if isinstance(v, dict)):
                lines += [f"[{name}.{key}]", *(f"{k} = {json.dumps(v)}" for k, v in nested.items())]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    write(raw)
    config = load_config(path)
    daemon = build_daemon_from_config(config)
    stop = asyncio.Event()
    with contextlib.redirect_stderr(stderr):
        task = asyncio.create_task(
            serve_configured(path, config, daemon, stop, reload_interval=0.02, outbox_interval_seconds=0.02)
        )
        try:
            yield write
        finally:
            stop.set()
            await asyncio.wait_for(task, 20)
            daemon.close()


def test_reload_keeps_the_runner_and_gives_it_the_new_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[AlwaysOnDaemon] = []
    real = build_daemon_from_config

    def record(config: HibernautConfig, **kw: Any) -> AlwaysOnDaemon:
        built.append(real(config, **kw))
        return built[-1]

    monkeypatch.setattr(main_module, "build_daemon_from_config", record)
    monkeypatch.setattr(f"{__name__}.build_daemon_from_config", record)
    stderr = io.StringIO()
    raw = raw_config(tmp_path, in_process())

    async def scenario() -> None:
        async with serving(tmp_path, raw, stderr) as write:
            first = built[0].runner
            assert first is not None
            await until(lambda: first.started)
            changed = json.loads(json.dumps(raw))
            changed["executors"]["claude"]["env"]["FAKE_LOG"] = str(tmp_path / "second.log")
            write(changed)
            await until(lambda: "Reloaded" in stderr.getvalue())
            assert built[1].runner is first and first.started
            post(built[1], "e1", {"body": plan(("Bash", CLEANUP))})
            await until(lambda: (tmp_path / "second.log").exists() and "ran" in (tmp_path / "second.log").read_text())
            # The runner itself is process-wide.
            changed["runner"]["max_concurrent"] = 1
            write(changed)
            await until(lambda: "[runner] changes require a restart" in stderr.getvalue())
            # An executor the runner cannot gate keeps the current generation.
            changed["runner"].pop("max_concurrent")
            changed["executors"]["claude"]["args"] = ["-p", "{prompt}", "--permission-mode", "plan"]
            write(changed)
            await until(lambda: "--permission-mode" in stderr.getvalue())
            assert len(built) == 2
        assert not first.started

    asyncio.run(scenario())


# What [runner] accepts.


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


def test_session_of_the_runner_is_not_resumed_by_the_local_executor_once_the_runner_is_off(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(raw_config(tmp_path, in_process())))

    async def run_once(target: AlwaysOnDaemon, external_id: str) -> None:
        async with running(target) as sent:
            post(target, external_id, {"body": "hello"})
            await until(lambda: [m for m in sent if "all done" in m.content])

    asyncio.run(run_once(daemon, "e1"))
    later = build_daemon_from_config(parse_config(raw_config(tmp_path, None)))
    asyncio.run(run_once(later, "e2"))
    assert len(entries(tmp_path, "start")) == 2
    assert "--resume" not in entries(tmp_path, "start")[1]["argv"]


def test_event_without_a_session_runs_and_is_resumed_in_its_own_worktree(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(raw_config(tmp_path, in_process(allow_tools=[]))))

    async def scenario() -> None:
        async with running(daemon) as sent:
            daemon.handler.ingest(
                inbox_id=INBOX, source="cli", external_id="e1", payload={"body": plan(("Bash", CLEANUP))}
            )
            (asked,) = await until(lambda: [m for m in sent if m.metadata.get("kind") == "approval_request"])
            post(daemon, "a1", {"approval_request_id": asked.metadata["approval_request_id"], "approved": True})
            await until(lambda: [m for m in sent if m.content == "all done"])

    asyncio.run(scenario())
    first, second = entries(tmp_path, "start")
    assert first["cwd"] == second["cwd"]
    assert len(entries(tmp_path, "ran")) == 1
