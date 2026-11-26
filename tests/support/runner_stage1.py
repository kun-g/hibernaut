from __future__ import annotations

import subprocess
from pathlib import Path

from runner.config import NamespacePolicy, ProjectCheckout, RunnerConfig


def _init_git_repo(repo: Path, *, with_remote: bool = True) -> Path | None:
    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True, capture_output=True)
    if not with_remote:
        return None
    bare = repo.parent / f"{repo.name}-remote.git"
    subprocess.run(["git", "init", "--bare", str(bare)], check=True, capture_output=True)
    subprocess.run(["git", "remote", "add", "origin", str(bare)], cwd=repo, check=True)
    subprocess.run(["git", "push", "-u", "origin", "main"], cwd=repo, check=True, capture_output=True)
    return bare


def _make_config(tmp_path: Path, repo: Path, *, max_concurrent: int = 2, interactive_surface: str = "none") -> RunnerConfig:
    return RunnerConfig(
        runner_id="runner-test-01",
        max_concurrent=max_concurrent,
        worktree_backend="git",
        interactive_surface="orca" if interactive_surface == "orca" else "none",
        namespaces={
            "hb": NamespacePolicy(namespace="hb", prefix="hb", allowed_callers=("hibernaut",)),
            "ws": NamespacePolicy(namespace="ws", prefix="ws", allowed_callers=("workshop",)),
        },
        projects={
            "hibernaut": ProjectCheckout(project_key="hibernaut", checkout_path=repo, default_base_ref="origin/main"),
        },
        state_dir=tmp_path / "state",
        logs_dir=tmp_path / "logs",
    )
