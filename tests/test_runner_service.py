from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import plistlib
import signal
import subprocess
import sys
import time
import tomllib
from pathlib import Path
from typing import Any

import pytest

from runner.check_driver import CHECK_PROGRAM, ProtocolCheckDriver
from runner.daemon import RunnerDaemon
from runner.protocol import RunnerCapabilities, RunRequest
from runner.protocol_check import (
    CHECK_SCHEMA,
    PROTOCOL_CHECK_EXECUTOR_ID,
    PROTOCOL_FILES,
    CheckRequest,
    manifest_sha256,
)
from runner.registry import WorkspaceAffinityStore
from runner.service import load_service_config, service_drivers
from runner.transport.auth import read_relay_token
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport.relay_hub import RunnerRelayHub
from runner.transport.ws_protocol import WSConnectionClosed, client_handshake, recv_json, send_json
from tests.runner_pilot_support import (
    ROOT,
    commit,
    free_port,
    git,
    protocol_repo,
    recomputed_manifest,
    report_of,
    runner_toml,
    start_service,
    write_token,
)
from tests.test_runner_safety import _config, _init_repo, _req


def _check(repo: Path, request: str | bytes, **env: str) -> subprocess.CompletedProcess[str]:
    argv = [sys.executable, "-I", "-S", "-B", str(CHECK_PROGRAM)]
    text = request.decode() if isinstance(request, bytes) else request
    return subprocess.run(argv, cwd=repo, input=text, env={**os.environ, **env}, capture_output=True, text=True, timeout=30)


# --- the fixed check program


def test_report_names_the_commit_and_recomputes_with_git_and_sha256_alone(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    head = protocol_repo(repo)
    proc = _check(repo, CheckRequest(head).to_json())
    assert proc.returncode == 0, proc.stderr
    report = report_of(proc.stdout)
    assert (report["schema"], report["commit"]) == (CHECK_SCHEMA, head)
    files = {f["path"]: f["sha256"] for f in report["files"]}  # type: ignore[index, attr-defined]
    assert tuple(files) == PROTOCOL_FILES
    assert report["manifest_sha256"] == recomputed_manifest(repo, head) == manifest_sha256(files)
    # The documented shell recipe gives the same digest.
    shell = (
        f"for f in {' '.join(PROTOCOL_FILES)}; do printf '%s  %s\\n' "
        f'"$(git cat-file blob {head}:$f | shasum -a 256 | cut -d" " -f1)" "$f"; done | shasum -a 256'
    )
    assert subprocess.check_output(["sh", "-c", shell], cwd=repo, text=True).split()[0] == report["manifest_sha256"]
    # Deterministic, and independent of the worktree's files and of the hold.
    (repo / PROTOCOL_FILES[0]).write_text("tampered", encoding="utf-8")
    assert _check(repo, CheckRequest(head, hold_seconds=1).to_json()).stdout == proc.stdout


def test_this_checkout_passes_the_check() -> None:
    head = git("rev-parse", "HEAD", cwd=ROOT)
    proc = _check(ROOT, CheckRequest(head).to_json())
    assert proc.returncode == 0, proc.stderr


def _missing(repo: Path) -> None:
    (repo / PROTOCOL_FILES[1]).unlink()


def _symlink(repo: Path) -> None:
    (repo / PROTOCOL_FILES[2]).unlink()
    (repo / PROTOCOL_FILES[2]).symlink_to("runner.interrupt.schema.json")


def _not_json(repo: Path) -> None:
    (repo / PROTOCOL_FILES[3]).write_text("{", encoding="utf-8")


def _duplicate_key(repo: Path) -> None:
    (repo / PROTOCOL_FILES[3]).write_text('{"$schema": 1, "$schema": 2}', encoding="utf-8")


def _wrong_id(repo: Path) -> None:
    (repo / PROTOCOL_FILES[4]).write_bytes((repo / PROTOCOL_FILES[0]).read_bytes())


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        (_missing, "runner.interrupt.schema.json is missing"),
        (_symlink, "runner.reclaim.schema.json has mode 120000"),
        (_not_json, "runner.run_request.schema.json is not valid JSON"),
        (_duplicate_key, "duplicate key"),
        (_wrong_id, "runner.run_result.schema.json has an $id of another file"),
    ],
)
def test_a_commit_whose_protocol_files_differ_fails_without_a_report(tmp_path: Path, change: Any, problem: str) -> None:
    repo = tmp_path / "repo"
    good = protocol_repo(repo)
    change(repo)
    bad = commit(repo, "broken")
    proc = _check(repo, CheckRequest(bad).to_json())
    assert (proc.returncode, proc.stdout) == (1, "") and problem in proc.stderr
    assert _check(repo, CheckRequest(good).to_json()).returncode == 0


def test_other_files_in_the_protocol_directory_are_not_part_of_the_check(tmp_path: Path) -> None:
    # The protocol grows (runner.tool_authorize.schema.json): the five files stay checkable, with the same digest.
    repo = tmp_path / "repo"
    before = report_of(_check(repo, CheckRequest(protocol_repo(repo)).to_json()).stdout)
    (repo / "schemas/runner/v1/sixth.schema.json").write_text("not a schema", encoding="utf-8")
    proc = _check(repo, CheckRequest(commit(repo, "one more")).to_json())
    assert proc.returncode == 0, proc.stderr
    after = report_of(proc.stdout)
    assert (after["files"], after["manifest_sha256"]) == (before["files"], before["manifest_sha256"])


def test_only_the_request_format_and_a_commit_of_this_repository_are_accepted(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    head = protocol_repo(repo)
    git("tag", "-a", "v1", "-m", "tag", cwd=repo)
    tag = git("rev-parse", "v1", cwd=repo)
    tree = git("rev-parse", "HEAD^{tree}", cwd=repo)
    base = {"schema": CHECK_SCHEMA, "commit": head, "hold_seconds": 0}
    bad: list[str] = [
        "", "{", "[]", json.dumps({**base, "path": "/etc/passwd"}), json.dumps({"commit": head}),
        *(json.dumps({**base, "commit": c}) for c in ("HEAD", head[:12], head.upper(), "--help", f"{head}\n", tag, tree, "0" * 40, 7)),
        *(json.dumps({**base, "hold_seconds": h}) for h in (-1, 61, 1.5, True, "1")),
        json.dumps({**base, "schema": "other"}), json.dumps(base) + " " * 2000,
    ]
    for request in bad:
        proc = _check(repo, request)
        assert (proc.returncode, proc.stdout) == (1, ""), request
        assert proc.stderr.startswith("protocol check failed:"), request


def test_replace_refs_cannot_swap_the_checked_content(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    head = protocol_repo(repo)
    before = _check(repo, CheckRequest(head).to_json()).stdout
    (repo / PROTOCOL_FILES[0]).write_text("{}", encoding="utf-8")
    git("replace", head, commit(repo, "other"), cwd=repo)
    assert _check(repo, CheckRequest(head).to_json()).stdout == before


# --- the driver inside a readonly-only daemon


def _run_request(head: str, **changes: Any) -> RunRequest:
    req = RunRequest(
        run_id="run_check", namespace="hb", project_key="hibernaut", owner_scope="hibernaut:pilot", work_key="w",
        executor_id=PROTOCOL_CHECK_EXECUTOR_ID, readonly=True, prompt=CheckRequest(head).to_json(), timeout_seconds=30.0,
    )
    return dataclasses.replace(req, **changes)


def test_readonly_daemon_runs_the_check_and_refuses_a_writable_workspace(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    head = protocol_repo(repo)
    # A module planted in the worktree under check must not be imported by the program.
    (repo / "json.py").write_text("raise SystemExit('planted module ran')", encoding="utf-8")
    head = commit(repo, "planted")
    cfg = dataclasses.replace(
        _config(tmp_path, repo, max_concurrent=1), readonly_only=True,
        projects={"hibernaut": dataclasses.replace(_config(tmp_path, repo).projects["hibernaut"], default_base_ref="main")},
    )
    daemon = RunnerDaemon(cfg, drivers=service_drivers())
    try:
        ok = asyncio.run(daemon.run(_run_request(head), caller_id="hibernaut"))
        assert ok.status == "succeeded", ok.error_summary
        assert ok.structured_output is not None and ok.structured_output["commit"] == head
        assert ok.log_ref is not None and Path(ok.log_ref.path).is_file()
        write = asyncio.run(daemon.run(_run_request(head, run_id="run_w", readonly=False), caller_id="hibernaut"))
        assert (write.status, write.error_kind) == ("refused", "permission_denied")
        other = asyncio.run(daemon.run(_run_request(head, run_id="run_o", executor_id="claude"), caller_id="hibernaut"))
        assert (other.status, other.error_kind) == ("refused", "config_error")
        missing = asyncio.run(daemon.run(_run_request("0" * 40, run_id="run_m"), caller_id="hibernaut"))
        assert (missing.status, missing.structured_output) == ("failed", None)
        assert "is not in this repository" in (missing.error_summary or "")
    finally:
        daemon.close()
    assert len(git("worktree", "list", cwd=repo).splitlines()) == 1


def test_driver_command_line_is_fixed_and_ignores_everything_but_the_prompt() -> None:
    req = _run_request("0" * 40, system_prompt_append="x", output_schema={"a": 1}, executor_session_id="claude:1")
    spec = ProtocolCheckDriver().build_command(req, Path("/anywhere"))
    assert spec.argv == (sys.executable, "-I", "-S", "-B", str(CHECK_PROGRAM))
    assert spec.stdin_text == req.prompt and spec.env == {}
    assert list(service_drivers()) == [PROTOCOL_CHECK_EXECUTOR_ID]


# --- service configuration and token


@pytest.fixture
def service(tmp_path: Path) -> tuple[Path, int, str]:
    repo = tmp_path / "repo"
    protocol_repo(repo)
    token_file = tmp_path / "relay.token"
    token = write_token(token_file)
    port = free_port()
    return runner_toml(tmp_path / "runner.toml", repo=repo, port=port, token_file=token_file), port, token


def test_service_config_accepts_the_pilot_shape(service: tuple[Path, int, str]) -> None:
    cfg = load_service_config(service[0])
    assert (cfg.runner.max_concurrent, cfg.runner.readonly_only) == (1, True)
    assert cfg.relay_url == f"ws://127.0.0.1:{service[1]}/ws/runner"
    assert cfg.runner.state_dir == service[0].parent.resolve() / "state"


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("max_concurrent = 1", "max_concurrent = 2"),
        ("readonly_only = true", "readonly_only = false"),
        ("readonly_only = true", 'readonly_only = "yes"'),
        ('worktree_backend = "git"', 'worktree_backend = "orca"'),
        ('interactive_surface = "none"', 'interactive_surface = "orca"'),
        ('["hibernaut"]', '["hibernaut", "other"]'),
        ("[namespaces.hb]", '[namespaces.ws]\nprefix = "ws"\nallowed_callers = ["x"]\n[namespaces.hb]'),
        ("ws://127.0.0.1", "ws://0.0.0.0"),
        ("ws://127.0.0.1", "ws://localhost"),
        ("ws://127.0.0.1", "wss://relay.example.com"),
        ("ws://127.0.0.1", "ws://user:pw@127.0.0.1"),
        ("/ws/runner", "/ws/other"),
        ("[relay]", '[executors.claude]\ncommand = "claude"\n[relay]'),
        ("[relay]", "[relay]\nextra = 1"),
    ],
)
def test_service_config_refuses_anything_wider_than_the_pilot(service: tuple[Path, int, str], old: str, new: str) -> None:
    path = service[0]
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new, 1), encoding="utf-8")
    with pytest.raises(ValueError):
        load_service_config(path)


def test_deploy_examples_match_the_service() -> None:
    cfg = load_service_config(ROOT / "deploy/runner/runner.example.toml")
    pilot = tomllib.loads(
        "\n".join(
            line.removeprefix("# ")
            for line in (ROOT / "hibernaut.example.toml").read_text(encoding="utf-8").split("# [runner_pilot]")[1].splitlines()
            if " = " in line
        )
    )
    assert pilot["enabled"] is False
    assert pilot["runner_id"] == cfg.runner.runner_id and [pilot["project_key"]] == list(cfg.runner.projects)
    assert cfg.relay_url == f"ws://127.0.0.1:{pilot['port']}/ws/runner"
    plist = plistlib.loads((ROOT / "deploy/runner/com.hibernaut.runner.plist.example").read_bytes())
    assert plist["ProgramArguments"][1:4] == ["-m", "runner.service", "--config"]
    assert plist["KeepAlive"] is True and plist["RunAtLoad"] is True
    assert set(plist["EnvironmentVariables"]) == {"PYTHONPATH", "PATH"}


def test_relay_token_must_be_a_private_long_token(tmp_path: Path) -> None:
    path = tmp_path / "t"
    token = write_token(path)
    assert read_relay_token(path) == token
    for mode in (0o640, 0o604):
        path.chmod(mode)
        with pytest.raises(ValueError, match="chmod 600"):
            read_relay_token(path)
    path.chmod(0o600)
    for text in ("", "short", "x" * 20 + " " + "y" * 20, "ä" * 40):
        path.write_text(text, encoding="utf-8")
        with pytest.raises(ValueError) as err:
            read_relay_token(path)
        assert not text or text not in str(err.value)
    with pytest.raises(ValueError):
        read_relay_token(tmp_path / "missing")
    with pytest.raises(ValueError):
        read_relay_token(tmp_path)


def _service_cli(config: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "runner.service", "--config", str(config), *args],
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")}, capture_output=True, text=True, timeout=30,
    )


def test_check_option_prints_capabilities_without_connecting(service: tuple[Path, int, str]) -> None:
    proc = _service_cli(service[0], "--check")
    assert proc.returncode == 0, proc.stderr
    caps = json.loads(proc.stdout)
    assert [e["executor_id"] for e in caps["executors"]] == [PROTOCOL_CHECK_EXECUTOR_ID]
    assert caps["slots"]["max_concurrent"] == 1
    assert service[2] not in proc.stdout + proc.stderr


def test_bad_configuration_exits_2_naming_no_token(service: tuple[Path, int, str]) -> None:
    (service[0].parent / "relay.token").chmod(0o644)
    proc = _service_cli(service[0])
    assert proc.returncode == 2 and "chmod 600" in proc.stderr and service[2] not in proc.stderr


def test_service_process_reconnects_after_the_relay_restarts_and_stops_on_sigterm(service: tuple[Path, int, str]) -> None:
    config, port, token = service
    log = config.parent / "service.log"
    proc = start_service(config, log)

    async def scenario() -> None:
        for _ in range(2):
            affinity = WorkspaceAffinityStore(Path(":memory:"))
            hub = RunnerRelayHub(shared_token=token, affinity=affinity)
            await hub.start("127.0.0.1", port)
            deadline = time.monotonic() + 20
            while not hub.registry.list_online():
                assert time.monotonic() < deadline and proc.poll() is None, log.read_text()
                await asyncio.sleep(0.05)
            res = await hub.run(_run_request(git("rev-parse", "HEAD", cwd=config.parent / "repo")))
            assert res.status == "succeeded", res.error_summary
            assert res.log_ref is not None
            assert "synthetic-service-token" not in Path(res.log_ref.path).read_text()
            await hub.stop()
            affinity.close()

    try:
        second = _service_cli(config)
        asyncio.run(scenario())
        assert second.returncode == 2 and "another runner service" in second.stderr
    finally:
        proc.send_signal(signal.SIGTERM)
        code = proc.wait(timeout=20)
    assert code == 0
    assert token not in log.read_text() and "synthetic-service-token" not in log.read_text()


# --- Relay hardening


def test_agent_survives_a_relay_that_hangs_up_in_every_stage_of_the_handshake(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_repo(repo)
    daemon = RunnerDaemon(_config(tmp_path, repo), drivers={})
    affinity = WorkspaceAffinityStore(tmp_path / "a.db")
    port = free_port()
    stages: list[int] = []

    async def scenario() -> None:
        async def broken(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            stage = len(stages)
            stages.append(stage)
            await reader.readuntil(b"\r\n\r\n")  # unread bytes would turn the close into a reset
            if stage == 1:
                writer.write(b"HTTP/1.1 101 Switching")
            elif stage == 2:
                writer.write(b"HTTP/1.1 101 ok\r\nbad\r\n\r\n")
            writer.close()

        server = await asyncio.start_server(broken, "127.0.0.1", port)
        agent = RunnerRelayAgent(
            daemon=daemon, hub_url=f"ws://127.0.0.1:{port}/ws/runner", token="t" * 32, reconnect_delays=(0.02,)
        )
        await agent.start()
        while len(stages) < 4:
            await asyncio.sleep(0.02)
        server.close()
        await server.wait_closed()
        hub = RunnerRelayHub(shared_token="t" * 32, affinity=affinity)
        await hub.start("127.0.0.1", port)
        try:
            await agent.wait_connected(5.0)
        finally:
            await agent.stop()
            await hub.stop()

    try:
        asyncio.run(asyncio.wait_for(scenario(), timeout=20))
    finally:
        daemon.close()
        affinity.close()


def _caps(runner_id: str = "n1", slots: int = 1) -> dict[str, Any]:
    raw = {
        "schema_version": "runner.v1", "runner_id": runner_id, "namespaces": ["hb"], "projects": ["hibernaut"],
        "worktree_backend": "git", "interactive_surface": "none", "labels": {}, "executors": [],
        "slots": {"max_concurrent": slots, "active_runs": 0, "available": slots}, "reported_at": "2026-01-01T00:00:00+00:00",
    }
    return RunnerCapabilities.from_dict(raw).to_dict()


async def _hello(port: int, token: str, caps: dict[str, Any]) -> tuple[Any, Any, dict[str, Any]]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    await client_handshake(reader, writer, host=f"127.0.0.1:{port}", path="/ws/runner")
    await send_json(writer, {"type": "hello", "token": token, "capabilities": caps, "ready_work_keys": []}, mask=True)
    return reader, writer, await recv_json(reader, writer, expect_masked=False, send_masked=True)


def test_relay_refuses_bad_tokens_and_nodes_it_does_not_admit(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    token = "k" * 40
    unhandled: list[dict[str, Any]] = []

    def admit(caps: RunnerCapabilities) -> str | None:
        if caps.runner_id != "n1":
            return "unknown runner"
        return None if caps.slots.max_concurrent == 1 else "one slot only"

    async def scenario() -> None:
        asyncio.get_running_loop().set_exception_handler(lambda _loop, ctx: unhandled.append(ctx))
        affinity = WorkspaceAffinityStore(tmp_path / "a.db")
        hub = RunnerRelayHub(shared_token=token, affinity=affinity, admit=admit)
        _, port = await hub.start()
        try:
            for bad, caps in ((("x" * 40), _caps()), ("ключ" * 10, _caps()), ("", _caps()), (token, _caps("n2")), (token, _caps(slots=2))):
                with pytest.raises(WSConnectionClosed) as closed:
                    await _hello(port, bad, caps)
                assert closed.value.code == 4001
            # Hanging up half way through the handshake or a frame is not an error worth a traceback.
            for junk in (b"", b"GET /ws", b"G" * 100_000):
                _, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.write(junk)
                writer.close()
            assert hub.registry.list_online() == []
            reader, writer, welcome = await _hello(port, token, _caps())
            assert welcome["type"] == "welcome" and [n.runner_id for n in hub.registry.list_online()] == ["n1"]
            # A heartbeat cannot widen what the hello was admitted with.
            await send_json(writer, {"type": "pong", "conn_epoch": welcome["conn_epoch"], "capabilities": _caps(slots=8)}, mask=True)
            with pytest.raises(WSConnectionClosed):
                await recv_json(reader, writer, expect_masked=False, send_masked=True)
            while hub.registry.list_online():
                await asyncio.sleep(0.02)
            writer.close()
            await asyncio.sleep(0.1)
        finally:
            await hub.stop()
            affinity.close()

    asyncio.run(asyncio.wait_for(scenario(), timeout=20))
    assert unhandled == []
    assert token not in caplog.text and "x" * 40 not in caplog.text


def test_lost_connection_error_names_the_node(tmp_path: Path) -> None:
    from runner.transport.relay_hub import RunnerConnectionLostError

    async def scenario() -> None:
        affinity = WorkspaceAffinityStore(tmp_path / "a.db")
        hub = RunnerRelayHub(shared_token="k" * 40, affinity=affinity)
        _, port = await hub.start()
        caps = _caps()
        caps["executors"] = [{"executor_id": "e", "driver": "custom", "supports_resume": False, "supports_stream_json": False, "interactive_surface": "none"}]
        reader, writer, _welcome = await _hello(port, "k" * 40, caps)
        run = asyncio.create_task(hub.run(_req("run_lost", executor_id="e", readonly=True)))
        assert (await recv_json(reader, writer, expect_masked=False, send_masked=True))["type"] == "run.submit"
        writer.close()
        with pytest.raises(RunnerConnectionLostError) as lost:
            await run
        assert (lost.value.runner_id, lost.value.run_id) == ("n1", "run_lost")
        await hub.stop()
        affinity.close()

    asyncio.run(asyncio.wait_for(scenario(), timeout=20))
