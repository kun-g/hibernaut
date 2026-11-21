from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.ports import OutboxPort

WorktreeBackend = Literal["orca", "git"]

_GITHUB_ISSUE_RE = re.compile(r"^github:[^#]+#(\d+)$")
_ISSUE_RE = re.compile(r"^issue:(\d+)$")
_PR_RE = re.compile(r"^(?:github-pr:[^#]+#|pr:)(\d+)$")


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str], Path | None], CommandResult]


@dataclass(frozen=True, slots=True)
class ReclaimResult:
    reclaimed: bool
    reason: str
    workdir: str | None = None


def _default_runner(cmd: Sequence[str], cwd: Path | None) -> CommandResult:
    proc = subprocess.run(
        list(cmd),
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandResult(returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)


def derive_worktree_names(
    work_key: str,
    task_seq: int,
    existing_dir_names: set[str],
) -> tuple[str, str]:
    # Minimal readable names without repo prefix or hash: hb-issue-<N>, hb-pr-<N>, hb-task-<seq>.
    pr_match = _PR_RE.match(work_key)
    issue_match = _GITHUB_ISSUE_RE.match(work_key) or _ISSUE_RE.match(work_key)

    if pr_match:
        base_suffix = f"pr-{pr_match.group(1)}"
    elif issue_match:
        base_suffix = f"issue-{issue_match.group(1)}"
    else:
        base_suffix = f"task-{task_seq}"

    dir_name = f"hb-{base_suffix}"
    branch_name = f"hb/{base_suffix}"
    if dir_name not in existing_dir_names:
        return dir_name, branch_name

    counter = 2
    while f"{dir_name}-{counter}" in existing_dir_names:
        counter += 1
    return f"{dir_name}-{counter}", f"{branch_name}-{counter}"


class OrcaWorktreeManager:
    def __init__(
        self,
        *,
        workspace_store: SQLiteWorkspaceStore,
        backend: WorktreeBackend = "orca",
        create_terminal: bool = False,
        runner: CommandRunner | None = None,
        worktree_base_dir: str | Path | None = None,
    ) -> None:
        self._store = workspace_store
        self._backend: WorktreeBackend = backend
        self._create_terminal = create_terminal
        self._runner: CommandRunner = runner or _default_runner
        self._worktree_base_dir = (
            Path(worktree_base_dir).resolve() if worktree_base_dir is not None else None
        )

    def resolve_workdir(
        self,
        *,
        work_key: str,
        primary_checkout: str | Path,
        explicit_workdir: str | Path | None = None,
    ) -> Path:
        # Priority 1: explicit read-only / review override skips worktree creation entirely.
        if explicit_workdir is not None:
            return Path(explicit_workdir).resolve()

        primary = Path(primary_checkout).resolve()
        existing = self._store.get(work_key)

        branches = self._registered_worktrees(primary)
        if existing is not None:
            if Path(existing.repo_checkout).resolve() != primary or existing.status == "reclaiming":
                raise RuntimeError(f"Cannot reuse workspace {work_key!r} in {primary}: {existing}")
            if existing.branch in branches:
                actual = self._worktree_path(branches, primary, existing.branch)
                if existing.status != "ready" or str(actual) != existing.workdir:
                    self._store.mark_ready(work_key, workdir=str(actual), branch=existing.branch)
                return actual
            if existing.status == "ready":
                raise RuntimeError(f"No registered worktree for {existing.branch!r} in {primary}")

        # Priority 3: dynamic creation with Safety 1 (creating intent) + Safety 2 (path persistence).
        self._runner(["git", "fetch", "origin"], primary)
        base_commit = self._resolve_base_commit(primary)

        base_dir = self._worktree_base_dir if self._worktree_base_dir is not None else primary.parent
        if existing is None:
            existing_names = {Path(r.workdir).name for r in self._store.list_by_repo(str(primary))}
            existing_names.update(name.replace("hb/", "hb-", 1) for name in branches if name.startswith("hb/"))
            if base_dir.is_dir():
                existing_names.update(p.name for p in base_dir.iterdir() if p.is_dir())
            dir_name, branch_name = derive_worktree_names(work_key, self._store.next_task_seq(), existing_names)
            expected_workdir = (base_dir / dir_name).resolve()
            self._store.reserve_creating(
                work_key=work_key, repo_checkout=str(primary), workdir=str(expected_workdir), branch=branch_name,
            )
        else:
            expected_workdir = Path(existing.workdir)
            dir_name, branch_name = expected_workdir.name, existing.branch

        if self._backend == "orca":
            self._create_via_orca(
                primary=primary,
                dir_name=dir_name,
                base_commit=base_commit,
                branch_name=branch_name,
            )
        else:
            self._create_via_git(
                primary=primary,
                expected_workdir=expected_workdir,
                base_commit=base_commit,
                branch_name=branch_name,
            )

        actual_workdir = self._worktree_path(self._registered_worktrees(primary), primary, branch_name)
        self._store.mark_ready(work_key, workdir=str(actual_workdir), branch=branch_name)
        if self._backend == "orca" and self._create_terminal:
            self._runner(["orca", "terminal", "create", "--workdir", str(actual_workdir)], primary)
        return actual_workdir

    def reclaim(
        self,
        work_key: str,
        *,
        notify_outbox: OutboxPort | None = None,
        inbox_id: str | None = None,
    ) -> ReclaimResult:
        # Safety 3: dual ownership check (DB record + 'hb-' basename prefix) + 3 safety gates.
        record = self._store.get(work_key)
        if record is None:
            return ReclaimResult(reclaimed=False, reason="workspace not managed in workspaces table")

        workdir = Path(record.workdir)
        if not workdir.name.startswith("hb-"):
            msg = f"Refused reclaim for '{workdir}': directory name lacks 'hb-' prefix"
            self._alert(notify_outbox, inbox_id, msg)
            return ReclaimResult(reclaimed=False, reason=msg, workdir=record.workdir)

        # Gate 1: no open PR for this branch.
        pr_check = self._runner(
            ["gh", "pr", "list", "--head", record.branch, "--state", "open", "--json", "number"],
            workdir if workdir.is_dir() else Path(record.repo_checkout),
        )
        if pr_check.returncode == 0 and pr_check.stdout.strip():
            try:
                open_prs = json.loads(pr_check.stdout)
            except json.JSONDecodeError:
                open_prs = []
            if isinstance(open_prs, list) and len(open_prs) > 0:
                msg = f"Skipped reclaim for '{workdir}': branch '{record.branch}' has open PR(s)"
                self._alert(notify_outbox, inbox_id, msg)
                return ReclaimResult(reclaimed=False, reason=msg, workdir=record.workdir)

        # Gate 2: no uncommitted changes.
        if workdir.is_dir():
            status_res = self._runner(["git", "status", "--porcelain"], workdir)
            if status_res.returncode != 0 or status_res.stdout.strip():
                msg = f"Skipped reclaim for '{workdir}': uncommitted changes present"
                self._alert(notify_outbox, inbox_id, msg)
                return ReclaimResult(reclaimed=False, reason=msg, workdir=record.workdir)

            # Gate 3: no unpushed local commits.
            unpushed = self._runner(
                ["git", "log", "--branches", "--not", "--remotes", "--oneline"],
                workdir,
            )
            if unpushed.returncode != 0 or unpushed.stdout.strip():
                msg = f"Skipped reclaim for '{workdir}': unpushed commits present"
                self._alert(notify_outbox, inbox_id, msg)
                return ReclaimResult(reclaimed=False, reason=msg, workdir=record.workdir)

        self._store.mark_reclaiming(work_key)
        primary = Path(record.repo_checkout)
        if self._backend == "orca":
            rm_res = self._runner(["orca", "worktree", "remove", str(workdir)], primary)
        else:
            rm_res = self._runner(["git", "worktree", "remove", str(workdir)], primary)
        if rm_res.returncode != 0:
            msg = f"Failed to remove worktree '{workdir}': {rm_res.stderr.strip()}"
            self._alert(notify_outbox, inbox_id, msg)
            return ReclaimResult(reclaimed=False, reason=msg, workdir=record.workdir)

        self._store.delete(work_key)
        return ReclaimResult(reclaimed=True, reason="reclaimed", workdir=record.workdir)

    def _resolve_base_commit(self, primary: Path) -> str:
        res = self._runner(["git", "rev-parse", "origin/HEAD"], primary)
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
        fallback = self._runner(["git", "rev-parse", "HEAD"], primary)
        if fallback.returncode == 0 and fallback.stdout.strip():
            return fallback.stdout.strip()
        return "HEAD"

    def _create_via_orca(
        self,
        *,
        primary: Path,
        dir_name: str,
        base_commit: str,
        branch_name: str,
    ) -> None:
        cmd = [
            "orca",
            "worktree",
            "create",
            "--repo",
            f"path:{primary}",
            "--name",
            dir_name,
            "--base-branch",
            base_commit,
            "--branch",
            branch_name,
            "--no-parent",
            "--setup",
            "skip",
        ]
        res = self._runner(cmd, primary)
        if res.returncode != 0:
            raise RuntimeError(f"orca worktree create failed: {res.stderr.strip()}")

    def _create_via_git(
        self,
        *,
        primary: Path,
        expected_workdir: Path,
        base_commit: str,
        branch_name: str,
    ) -> None:
        cmd = ["git", "worktree", "add", "-b", branch_name, str(expected_workdir), base_commit]
        res = self._runner(cmd, primary)
        if res.returncode != 0:
            raise RuntimeError(f"git worktree add failed: {res.stderr.strip()}")

    def _registered_worktrees(self, primary: Path) -> dict[str, list[Path]]:
        result = self._runner(["git", "worktree", "list", "--porcelain", "-z"], primary)
        if result.returncode != 0:
            raise RuntimeError(f"Cannot list worktrees in {primary}: {result.stderr.strip()}")
        branches: dict[str, list[Path]] = {}
        for record in result.stdout.split("\0\0"):
            fields = dict(field.split(" ", 1) for field in record.split("\0") if " " in field)
            ref = fields.get("branch", "")
            if ref.startswith("refs/heads/") and "worktree" in fields:
                branches.setdefault(ref.removeprefix("refs/heads/"), []).append(Path(fields["worktree"]))
        return branches

    @staticmethod
    def _worktree_path(branches: dict[str, list[Path]], primary: Path, branch: str) -> Path:
        paths = branches.get(branch, [])
        if len(paths) == 1:
            path = paths[0].resolve()
            if path != primary and (path / ".git").is_file():
                return path
        raise RuntimeError(f"Expected one local linked worktree for {branch!r} in {primary}, found {paths}")

    def _alert(
        self,
        outbox: OutboxPort | None,
        inbox_id: str | None,
        message: str,
    ) -> None:
        if outbox is not None and inbox_id:
            outbox.enqueue(inbox_id, message, metadata={"kind": "reclaim_alert"})
