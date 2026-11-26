from __future__ import annotations

import asyncio
import atexit
import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from functools import cache
from pathlib import Path

from runner.cli_drivers import TemplateCLIDriver
from runner.config import NamespacePolicy, ProjectCheckout, RunnerConfig
from runner.protocol import RunRequest


PY = sys.executable
# Publish both PIDs only once the grandchild ignores TERM. The cancellation matrix covers descendants
# that do and don't keep the leader's output pipes open.
_SPAWN_SCRIPT = (
    "import os, signal, sys\n"
    "leader = os.getpid()\n"
    "if os.fork() == 0:\n"
    "    signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
    "    if '--hold-pipes' not in sys.argv: os.close(1); os.close(2)\n"
    "    with open(sys.argv[1] + '.tmp', 'w') as f: f.write(f'{leader} {os.getpid()}')\n"
    "    os.replace(sys.argv[1] + '.tmp', sys.argv[1])\n"
    "signal.alarm(60)\n"
    "signal.pause()\n"
)


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


@cache
def _seed_repos() -> tuple[Path, Path]:
    tmp = tempfile.TemporaryDirectory(prefix="runner_seed_")
    atexit.register(tmp.cleanup)
    repo, bare = Path(tmp.name) / "repo", Path(tmp.name) / "remote.git"
    repo.mkdir()
    _git("init", "-b", "main", cwd=repo)
    _git("config", "user.email", "test@example.com", cwd=repo)
    _git("config", "user.name", "Test", cwd=repo)
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    _git("add", "README.md", cwd=repo)
    _git("-c", "maintenance.auto=false", "commit", "-m", "init", cwd=repo)
    _git("init", "--bare", str(bare), cwd=repo)
    _git("remote", "add", "origin", str(bare), cwd=repo)
    _git("-c", "maintenance.auto=false", "push", "-u", "origin", "main", cwd=repo)
    return repo, bare


def _init_repo(repo: Path) -> None:
    seed, origin = _seed_repos()
    bare = repo.with_name(f"{repo.name}-remote.git")
    # Mutations, including pushes to origin and the registration of linked worktrees, belong to one test.
    shutil.copytree(seed, repo, symlinks=True)
    shutil.copytree(origin, bare, symlinks=True)
    _git("remote", "set-url", "origin", str(bare), cwd=repo)


def _config(tmp_path: Path, repo: Path, runner_id: str = "runner-safety", max_concurrent: int = 4) -> RunnerConfig:
    return RunnerConfig(
        runner_id=runner_id,
        max_concurrent=max_concurrent,
        worktree_backend="git",
        interactive_surface="none",
        namespaces={"hb": NamespacePolicy(namespace="hb", prefix="hb", allowed_callers=("hibernaut",))},
        projects={"hibernaut": ProjectCheckout(project_key="hibernaut", checkout_path=repo, default_base_ref="origin/main")},
        state_dir=tmp_path / f"state-{runner_id}",
        logs_dir=tmp_path / f"logs-{runner_id}",
    )


def _drivers() -> dict[str, TemplateCLIDriver]:
    return {
        "spawn": TemplateCLIDriver(executor_id="spawn", driver_kind="custom", command=PY, args=("-c", _SPAWN_SCRIPT, "{prompt}")),
        "cwd": TemplateCLIDriver(
            executor_id="cwd",
            driver_kind="custom",
            command=PY,
            args=("-c", "import json, os; print(json.dumps({'cwd': os.getcwd()}))"),
        ),
        "look": TemplateCLIDriver(
            executor_id="look",
            driver_kind="custom",
            command=PY,
            args=(
                "-c",
                "import json, os, pathlib, subprocess; print(json.dumps({'cwd': os.getcwd(), "
                "'mutated': pathlib.Path('mutated.txt').exists(), 'readme': pathlib.Path('README.md').read_text(), "
                "'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()}))",
            ),
        ),
        "mutate": TemplateCLIDriver(
            executor_id="mutate",
            driver_kind="custom",
            command=PY,
            args=("-c", "from pathlib import Path; Path('mutated.txt').write_text('oops'); Path('README.md').write_text('changed')"),
        ),
    }


def _req(run_id: str, *, executor_id: str, prompt: str = "", work_key: str = "gh#1", owner_scope: str = "proj:hb", readonly: bool = False) -> RunRequest:
    return RunRequest(
        run_id=run_id,
        namespace="hb",
        project_key="hibernaut",
        owner_scope=owner_scope,
        work_key=work_key,
        executor_id=executor_id,
        readonly=readonly,
        prompt=prompt,
        timeout_seconds=60.0,
    )


# Publish the count after opening the optional gate, so the test can immediately release the child.
_COUNT_SCRIPT = """\
import json, sys
from pathlib import Path
counter = Path(sys.argv[1])
gate = counter.with_suffix('.gate')
reader = gate.open('rb', buffering=0) if gate.exists() else None
with counter.open('a') as out:
    out.write('x\\n')
if reader is not None:
    with reader:
        reader.read(1)
print(json.dumps({'outcome': 'emit', 'message': 'ok'}))
"""


@contextmanager
def _hold_count(counter: Path):
    gate = counter.with_suffix(".gate")
    os.mkfifo(gate)
    # RDWR keeps entering and releasing nonblocking even if the child fails to start.
    with os.fdopen(os.open(gate, os.O_RDWR), "wb", buffering=0) as writer:
        try:
            yield
        finally:
            gate.unlink()
            writer.write(b"1")


async def _count_started(counter: Path) -> None:
    async with asyncio.timeout(10):
        while not counter.exists():
            await asyncio.sleep(0.01)


def _count_drivers() -> dict[str, TemplateCLIDriver]:
    return {"count": TemplateCLIDriver(executor_id="count", driver_kind="custom", command=PY, args=("-c", _COUNT_SCRIPT, "{prompt}"))}
