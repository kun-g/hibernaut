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

from agent_runtime.app import main as main_module
from agent_runtime.app.config import HibernautConfig, load_config, parse_config
from agent_runtime.app.main import AlwaysOnDaemon, build_daemon_from_config, serve_configured
from runner.transport.relay_client import RunnerRelayAgent
from tests.support.fake_gated_cli import install_gated_cli
from tests.support.runner_safety import _init_repo
from tests.support.runner_tool_gate import CLEANUP, Node, plan
from tests.support.runner_composition import INBOX, TOKEN_ENV, in_process, raw_config, relay


def runnable_config(tmp_path: Path, runner: dict[str, Any] | None) -> dict[str, Any]:
    repo = tmp_path / "repo"
    if not repo.exists():
        _init_repo(repo)
    install_gated_cli(tmp_path / "bin")
    return raw_config(tmp_path, runner)


def entries(tmp_path: Path, kind: str) -> list[dict[str, Any]]:
    log = tmp_path / "cli.log"
    if not log.exists():
        return []
    return [e for e in map(json.loads, log.read_text(encoding="utf-8").splitlines()) if e["kind"] == kind]


async def until(condition: Callable[[], Any], timeout: float = 45.0) -> Any:
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


def test_in_process_runner_executes_a_run_through_the_worker(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(runnable_config(tmp_path, in_process())))

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
    daemon = build_daemon_from_config(parse_config(runnable_config(tmp_path, in_process(allow_tools=[]))))

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


def test_a_forced_stop_ends_the_run_before_the_runner_closes(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(runnable_config(tmp_path, in_process())))

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
    raw = runnable_config(tmp_path, in_process())

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
            assert built[1].pool.limiter is built[0].pool.limiter
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


def test_session_of_the_runner_is_not_resumed_by_the_local_executor_once_the_runner_is_off(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(runnable_config(tmp_path, in_process())))

    async def run_once(target: AlwaysOnDaemon, external_id: str) -> None:
        async with running(target) as sent:
            post(target, external_id, {"body": "hello"})
            await until(lambda: [m for m in sent if "all done" in m.content])

    asyncio.run(run_once(daemon, "e1"))
    later = build_daemon_from_config(parse_config(runnable_config(tmp_path, None)))
    asyncio.run(run_once(later, "e2"))
    assert len(entries(tmp_path, "start")) == 2
    assert "--resume" not in entries(tmp_path, "start")[1]["argv"]


def test_event_without_a_session_runs_and_is_resumed_in_its_own_worktree(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(runnable_config(tmp_path, in_process(allow_tools=[]))))

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
