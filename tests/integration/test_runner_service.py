from __future__ import annotations

import asyncio
import dataclasses
import io
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from runner import protocol_check
from runner.check_driver import CHECK_PROGRAM
from runner.daemon import RunnerDaemon
from runner.protocol_check import (
    CHECK_SCHEMA,
    PROTOCOL_CHECK_EXECUTOR_ID,
    PROTOCOL_FILES,
    CheckRequest,
    manifest_sha256,
)
from runner.registry import WorkspaceAffinityStore
from runner.service import service_drivers
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport.relay_hub import RunnerRelayHub
from tests.support.runner_pilot_support import (
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
from tests.support.runner_safety import _config, _init_repo
from tests.support.runner_service import _run_request


def _check(repo: Path, request: str | bytes, **env: str) -> subprocess.CompletedProcess[str]:
    argv = [sys.executable, "-I", "-S", "-B", str(CHECK_PROGRAM)]
    text = request.decode() if isinstance(request, bytes) else request
    return subprocess.run(argv, cwd=repo, input=text, env={**os.environ, **env}, capture_output=True, text=True, timeout=30)


def test_report_names_the_commit_and_recomputes_with_git_and_sha256_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
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
    delays = []
    monkeypatch.chdir(repo)
    monkeypatch.setattr(protocol_check, "time", SimpleNamespace(sleep=delays.append))
    with io.TextIOWrapper(io.BytesIO(CheckRequest(head, hold_seconds=1).to_json().encode())) as stdin:
        monkeypatch.setattr(sys, "stdin", stdin)
        assert protocol_check.main() == 0
    assert delays == [1] and capsys.readouterr().out == proc.stdout


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


def test_only_the_request_format_and_a_commit_of_this_repository_are_accepted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
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
    monkeypatch.chdir(repo)
    for request in bad:
        with io.TextIOWrapper(io.BytesIO(request.encode())) as stdin:
            monkeypatch.setattr(sys, "stdin", stdin)
            assert protocol_check.main() == 1, request
        out = capsys.readouterr()
        assert out.out == "", request
        assert out.err.startswith("protocol check failed:"), request


def test_replace_refs_cannot_swap_the_checked_content(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    head = protocol_repo(repo)
    before = _check(repo, CheckRequest(head).to_json()).stdout
    (repo / PROTOCOL_FILES[0]).write_text("{}", encoding="utf-8")
    git("replace", head, commit(repo, "other"), cwd=repo)
    assert _check(repo, CheckRequest(head).to_json()).stdout == before


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


@pytest.fixture
def service(tmp_path: Path) -> tuple[Path, int, str]:
    repo = tmp_path / "repo"
    protocol_repo(repo)
    token_file = tmp_path / "relay.token"
    token = write_token(token_file)
    port = free_port()
    return runner_toml(tmp_path / "runner.toml", repo=repo, port=port, token_file=token_file), port, token


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
