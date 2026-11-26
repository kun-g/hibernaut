from __future__ import annotations

import ast
import asyncio
import dataclasses
import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from runner.daemon import RunnerDaemon
from runner.protocol import ReclaimRequest
from runner.registry import WorkspaceAffinityStore
from runner.supervisor import CHILD_ENV_ALLOWED_PREFIXES, CHILD_ENV_ALLOWLIST
from runner.transport.relay_client import RunnerRelayAgent
from runner.transport.relay_hub import RunnerRelayHub
from tests.support.runner_safety import _config, _drivers, _git, _init_repo, _req
from tests.support.runner_workspace_env import MARK


SYNTHETIC = {
    name: f"{MARK}-{name}"
    for name in (
        "GH_TOKEN",
        "GITHUB_TOKEN",
        "AWS_SECRET_ACCESS_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "SOME_SERVICE_PASSWORD",
        "LC_SERVICE_TOKEN",
        "SSH_AUTH_SOCK",
        "GIT_ASKPASS",
        "ORCA_AGENT_HOOK_TOKEN",
        "UNLISTED_SETTING",
    )
} | {
    # Credentials git itself would read from its environment.
    "GIT_CONFIG_COUNT": "1",
    "GIT_CONFIG_KEY_0": "http.extraHeader",
    "GIT_CONFIG_VALUE_0": f"Authorization: {MARK}-header",
}
# Set again by the shell, the OS or git between the runner and the recording point, whatever the runner's own value.
_LOCAL_NAMES = {"PWD", "OLDPWD", "SHLVL", "_", "__CF_USER_TEXT_ENCODING", "GIT_TERMINAL_PROMPT"}
_SHIM = """#!{python} -IS
import os, sys
with open({log!r}, "a", encoding="utf-8") as fp:
    fp.write(repr({{"argv": sys.argv, "env": dict(os.environ)}}) + "\\n")
"""
_GIT_SHIM = _SHIM + "os.execv({git!r}, [{git!r}, *sys.argv[1:]])\n"
_ORCA_SHIM = _SHIM + """
import json, subprocess
args = sys.argv[1:]
def opt(name):
    return args[args.index(name) + 1]
if args[:2] == ["worktree", "create"]:
    path = os.path.join({root!r}, opt("--name"))
    subprocess.run([{git!r}, "worktree", "add", "-q", "-b", opt("--branch"), path, opt("--base-branch")], check=True)
    print(json.dumps({{"result": {{"worktree": {{"branch": "refs/heads/" + opt("--branch"), "path": path}}}}}}))
else:
    subprocess.run([{git!r}, "worktree", "remove", opt("--worktree").removeprefix("path:")], check=True)
"""


@dataclasses.dataclass
class _Workspace:
    tmp: Path
    repo: Path
    calls_log: Path

    def calls(self) -> list[dict[str, object]]:
        if not self.calls_log.exists():
            return []
        return [ast.literal_eval(line) for line in self.calls_log.read_text(encoding="utf-8").splitlines()]

    def call_count(self) -> int:
        return len(self.calls_log.read_bytes().splitlines()) if self.calls_log.exists() else 0

    def daemon(self, backend: str = "git") -> RunnerDaemon:
        cfg = dataclasses.replace(_config(self.tmp, self.repo), worktree_backend=backend)  # type: ignore[arg-type]
        return RunnerDaemon(cfg, drivers=_drivers())

    def all_files_text(self) -> str:
        roots = [p for p in self.tmp.iterdir() if p.name.startswith(("logs-", "state-"))]
        return "".join(
            p.read_bytes().decode("utf-8", "replace") for root in roots for p in root.rglob("*") if p.is_file()
        )


def _write_shim(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, record_env: bool) -> _Workspace:
    repo = tmp_path / "repo"
    _init_repo(repo)
    _git("config", "maintenance.auto", "false", cwd=repo)
    log = tmp_path / "workspace-calls.log"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    orca_root = tmp_path / "orca-worktrees"
    orca_root.mkdir()
    # Use the git selected for the test session before installing the recorder in front of it.
    real_git = shutil.which("git")
    assert real_git is not None
    values = {"python": sys.executable, "log": str(log), "git": real_git, "root": str(orca_root)}
    if record_env:
        git_script = _GIT_SHIM.format(**values)
        hook = _SHIM.format(**values)
    else:
        # Refusal tests count actual launches across process boundaries, including Git hooks.
        hook = f"#!/bin/sh\nprintf 'call\\n' >> {shlex.quote(str(log))}\n"
        git_script = hook + f'exec {shlex.quote(real_git)} "$@"\n'
    _write_shim(bin_dir / "git", git_script)
    _write_shim(bin_dir / "orca", _ORCA_SHIM.format(**values))
    _write_shim(repo / ".git" / "hooks" / "post-checkout", hook)
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    for name, value in SYNTHETIC.items():
        monkeypatch.setenv(name, value)
    return _Workspace(tmp=tmp_path, repo=repo, calls_log=log)


@pytest.fixture
def ws(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Workspace:
    return workspace(tmp_path, monkeypatch, record_env=False)


@pytest.fixture
def traced_ws(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Workspace:
    return workspace(tmp_path, monkeypatch, record_env=True)


def _reclaim(merged: str | None = None) -> ReclaimRequest:
    return ReclaimRequest(
        namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#1", merged_head_sha=merged
    )


@pytest.mark.parametrize("backend", ["git", "orca"])
def test_every_workspace_command_gets_the_restricted_environment(
    traced_ws: _Workspace, caplog: pytest.LogCaptureFixture, backend: str
) -> None:
    ws = traced_ws
    caplog.set_level(logging.DEBUG)
    daemon = ws.daemon(backend)

    async def scenario() -> list[str]:
        written = await daemon.run(_req("run_write", executor_id="cwd"), caller_id="hibernaut")
        readonly = await daemon.run(_req("run_read", executor_id="look", readonly=True), caller_id="hibernaut")
        mutated = await daemon.run(_req("run_mutate", executor_id="mutate", readonly=True), caller_id="hibernaut")
        assert written.status == "succeeded", written.error_summary
        assert readonly.status == "succeeded", readonly.error_summary
        assert mutated.error_summary == "readonly run mutated worktree; result discarded"
        reclaimed = await daemon.reclaim_workspace(_reclaim(), caller_id="hibernaut")
        assert reclaimed.reclaimed, reclaimed.reason
        return [json.dumps(r.to_dict()) for r in (written, readonly, mutated, reclaimed)]

    try:
        reports = asyncio.run(scenario())
    finally:
        daemon.close()

    calls = ws.calls()
    commands = {" ".join(map(str, c["argv"][1:3])) for c in calls if Path(str(c["argv"][0])).name == "git"}  # type: ignore[index]
    # Creation, validation, fingerprint, readonly restore and reclaim all went through the recorder.
    assert {"rev-parse --verify", "worktree list", "worktree add", "status --porcelain", "checkout --force",
            "clean -ffdx", "worktree remove", "branch -r"} <= commands
    if backend == "orca":
        orca = [c for c in calls if Path(str(c["argv"][0])).name == "orca"]  # type: ignore[index]
        assert [c["argv"][2] for c in orca] == ["create", "rm"]  # type: ignore[index]
    for call in calls:
        env = call["env"]
        assert isinstance(env, dict)
        # Names git and macOS add on their own (GIT_DIR, SDKROOT) are not in the runner's environment.
        inherited = {k for k in env if k in os.environ} - _LOCAL_NAMES
        extra = {k for k in inherited if k not in CHILD_ENV_ALLOWLIST and not k.startswith(CHILD_ENV_ALLOWED_PREFIXES)}
        assert not extra, (call["argv"], extra)
        assert MARK not in json.dumps(env), call["argv"]
        assert env["PATH"] and env["HOME"]
    for text in (*reports, caplog.text, ws.all_files_text()):
        assert MARK not in text


@pytest.mark.parametrize("readonly", [False, True])
def test_run_is_refused_before_any_workspace_command_on_a_proxy_with_credentials(
    ws: _Workspace, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, readonly: bool
) -> None:
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("HTTPS_PROXY", f"http://user:{MARK}@proxy.invalid:8080")
    daemon = ws.daemon()
    try:
        res = asyncio.run(daemon.run(_req("run_x", executor_id="cwd", readonly=readonly), caller_id="hibernaut"))
        assert (res.status, res.error_kind) == ("refused", "permission_denied")
        assert "HTTPS_PROXY" in (res.error_summary or "")
        assert ws.call_count() == 0
        for text in (json.dumps(res.to_dict()), caplog.text, ws.all_files_text()):
            assert MARK not in text
        # A refusal ran nothing: the same request runs once the configuration is fixed.
        monkeypatch.delenv("HTTPS_PROXY")
        again = asyncio.run(daemon.run(_req("run_x", executor_id="cwd", readonly=readonly), caller_id="hibernaut"))
        assert again.status == "succeeded", again.error_summary
    finally:
        daemon.close()


def test_reclaim_is_blocked_on_a_proxy_with_credentials_and_works_once_fixed(
    ws: _Workspace, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    daemon = ws.daemon()
    try:
        created = asyncio.run(daemon.run(_req("run_w", executor_id="cwd"), caller_id="hibernaut"))
        workdir = Path(json.loads(created.stdout_excerpt)["cwd"])
        before = ws.call_count()
        monkeypatch.setenv("ALL_PROXY", f"user:{MARK}@proxy.invalid:8080")
        blocked = asyncio.run(daemon.reclaim_workspace(_reclaim(), caller_id="hibernaut"))
        assert (blocked.reclaimed, blocked.status, blocked.blockers) == (False, "blocked", ("child_env_refused",))
        assert "ALL_PROXY" in blocked.reason and MARK not in blocked.reason + caplog.text
        assert ws.call_count() == before and workdir.exists()
        rec = daemon.worktrees.get_record(
            namespace="hb", project_key="hibernaut", owner_scope="proj:hb", work_key="gh#1"
        )
        assert rec is not None and rec.status == "ready"
        monkeypatch.delenv("ALL_PROXY")
        done = asyncio.run(daemon.reclaim_workspace(_reclaim(), caller_id="hibernaut"))
        assert done.reclaimed and not workdir.exists()
    finally:
        daemon.close()


def test_environment_refused_midway_through_a_reclaim_leaves_the_workspace_reclaimable(
    ws: _Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    daemon = ws.daemon()
    try:
        asyncio.run(daemon.run(_req("run_w", executor_id="cwd"), caller_id="hibernaut"))
        real = daemon.worktrees._claim_for_reclaim  # type: ignore[attr-defined]

        def claim_then_break(rec: object) -> str | None:
            token = real(rec)  # type: ignore[arg-type]
            monkeypatch.setenv("ALL_PROXY", f"user:{MARK}@proxy.invalid:8080")
            return token

        monkeypatch.setattr(daemon.worktrees, "_claim_for_reclaim", claim_then_break)
        blocked = asyncio.run(daemon.reclaim_workspace(_reclaim(), caller_id="hibernaut"))
        assert (blocked.status, blocked.blockers) == ("blocked", ("child_env_refused",))
        monkeypatch.setattr(daemon.worktrees, "_claim_for_reclaim", real)
        monkeypatch.delenv("ALL_PROXY")
        assert asyncio.run(daemon.reclaim_workspace(_reclaim(), caller_id="hibernaut")).reclaimed
    finally:
        daemon.close()


def test_refused_reclaim_over_the_relay_answers_instead_of_dropping_the_connection(
    ws: _Workspace, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    daemon = ws.daemon()
    affinity = WorkspaceAffinityStore(tmp_path / "affinity.db")

    async def scenario() -> None:
        hub = RunnerRelayHub(shared_token="t" * 32, affinity=affinity)
        host, port = await hub.start()
        agent = RunnerRelayAgent(daemon=daemon, hub_url=f"ws://{host}:{port}/ws/runner", token="t" * 32)
        await agent.start()
        try:
            await agent.wait_connected()
            assert (await hub.run(_req("run_w", executor_id="cwd"))).status == "succeeded"
            monkeypatch.setenv("ALL_PROXY", f"user:{MARK}@proxy.invalid:8080")
            res = await hub.reclaim_workspace(_reclaim())
            assert (res.status, res.blockers) == ("blocked", ("child_env_refused",))
            assert agent.conn_epoch == 1
        finally:
            await agent.stop()
            await hub.stop()

    try:
        asyncio.run(scenario())
    finally:
        daemon.close()
        affinity.close()


def test_no_runner_module_starts_a_subprocess_with_the_inherited_environment() -> None:
    # Every process the runner package starts goes through one of the two call sites that pass env= explicitly.
    src = Path(__file__).resolve().parents[2] / "src" / "runner"
    sites = subprocess.run(
        [sys.executable, "-c", (
            "import ast, pathlib, sys\n"
            "for p in sorted(pathlib.Path(sys.argv[1]).rglob('*.py')):\n"
            "    for n in ast.walk(ast.parse(p.read_text())):\n"
            "        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in "
            "('run', 'Popen', 'check_output', 'check_call', 'call', 'create_subprocess_exec', 'create_subprocess_shell',"
            " 'system', 'popen') and isinstance(n.func.value, (ast.Name, ast.Attribute)) and "
            "ast.unparse(n.func.value) in ('subprocess', 'asyncio', 'os') and ast.unparse(n.func) != 'asyncio.run':\n"
            "            print(p.name, ast.unparse(n.func), any(k.arg == 'env' for k in n.keywords))\n"
        ), str(src)], check=True, capture_output=True, text=True,
    ).stdout.splitlines()
    assert sites and all(line.endswith(" True") for line in sites), sites
