from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Final, Literal, Protocol, Sequence, cast

from runner.config import NamespacePolicy, ProjectCheckout
from runner.protocol import ReclaimResult, WorktreeRef, utc_now_iso

_SAFE_HINT_RE: Final = re.compile(r"[^a-z0-9_-]+")
WorkspaceStatus = Literal["creating", "ready", "reclaiming"]


class WorkspaceIsolationError(PermissionError):
    pass


@dataclass(frozen=True, slots=True)
class CommandOutput:
    returncode: int
    stdout: str
    stderr: str


@dataclass(frozen=True, slots=True)
class WorkspaceRecord:
    namespace: str
    project_key: str
    owner_scope: str
    work_key: str
    dir_name: str
    workdir: Path
    branch: str
    status: WorkspaceStatus
    updated_at: str


@dataclass(frozen=True, slots=True)
class CreatedWorktree:
    path: Path
    branch: str


@dataclass(frozen=True, slots=True)
class ResolvedWorktree:
    workdir: Path
    branch: str
    created: bool
    ref: WorktreeRef


class WorktreeBackend(Protocol):
    def create(self, *, primary_checkout: Path, workdir: Path, branch: str, base_ref: str) -> CreatedWorktree: ...

    def remove(self, *, primary_checkout: Path, workdir: Path) -> None: ...


CommandRunner = Callable[[Sequence[str], Path], CommandOutput]


def _run_cmd(cmd: Sequence[str], cwd: Path) -> CommandOutput:
    env = {k: v for k, v in os.environ.items() if k not in {"GH_TOKEN", "GITHUB_TOKEN"}}
    env["GIT_TERMINAL_PROMPT"] = "0"
    proc = subprocess.run(
        list(cmd),
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return CommandOutput(returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)


def _resolve_commit(primary_checkout: Path, base_ref: str, run_cmd: CommandRunner = _run_cmd) -> str:
    for ref in (base_ref, "HEAD"):
        res = run_cmd(["git", "rev-parse", "--verify", f"{ref}^{{commit}}"], primary_checkout)
        if res.returncode == 0 and res.stdout.strip():
            return res.stdout.strip()
    raise RuntimeError(f"cannot resolve {base_ref!r} or HEAD in {primary_checkout}")


class GitWorktreeBackend:
    def create(self, *, primary_checkout: Path, workdir: Path, branch: str, base_ref: str) -> CreatedWorktree:
        commit = _resolve_commit(primary_checkout, base_ref)
        res = _run_cmd(
            ["git", "worktree", "add", "-b", branch, str(workdir), commit],
            cwd=primary_checkout,
        )
        if res.returncode != 0:
            raise RuntimeError(f"git worktree add failed: {res.stderr.strip() or res.stdout.strip()}")
        return CreatedWorktree(path=workdir.resolve(), branch=branch)

    def remove(self, *, primary_checkout: Path, workdir: Path) -> None:
        res = _run_cmd(["git", "worktree", "remove", str(workdir)], cwd=primary_checkout)
        if res.returncode != 0:
            raise RuntimeError(f"git worktree remove failed: {res.stderr.strip() or res.stdout.strip()}")


class OrcaWorktreeBackend:
    def __init__(self, *, run_cmd: CommandRunner = _run_cmd) -> None:
        self._run_cmd = run_cmd

    def create(self, *, primary_checkout: Path, workdir: Path, branch: str, base_ref: str) -> CreatedWorktree:
        commit = _resolve_commit(primary_checkout, base_ref, self._run_cmd)
        res = self._run_cmd(
            [
                "orca",
                "worktree",
                "create",
                "--repo",
                f"path:{primary_checkout}",
                "--name",
                workdir.name,
                "--base-branch",
                commit,
                "--branch",
                branch,
                "--no-parent",
                "--setup",
                "skip",
                "--json",
            ],
            primary_checkout,
        )
        if res.returncode != 0:
            raise RuntimeError(f"orca worktree create failed: {res.stderr.strip() or res.stdout.strip()}")
        # Orca decides where the worktree lives (and may suffix the branch); only its receipt says where.
        try:
            worktree = cast(dict[str, Any], json.loads(res.stdout)["result"]["worktree"])
            ref, path = worktree["branch"], worktree["path"]
        except (ValueError, KeyError, TypeError) as exc:
            raise RuntimeError("invalid Orca worktree creation receipt") from exc
        if not isinstance(ref, str) or not isinstance(path, str) or not Path(path).is_absolute():
            raise RuntimeError("invalid branch or path in Orca worktree creation receipt")
        actual_branch = ref.removeprefix("refs/heads/")
        if not re.fullmatch(re.escape(branch) + r"(?:-\d+)?", actual_branch):
            raise RuntimeError(f"Orca returned unrelated branch {actual_branch!r} for {branch!r}")
        return CreatedWorktree(path=Path(path).resolve(), branch=actual_branch)

    def remove(self, *, primary_checkout: Path, workdir: Path) -> None:
        res = self._run_cmd(
            ["orca", "worktree", "rm", "--worktree", f"path:{workdir}"],
            primary_checkout,
        )
        if res.returncode != 0:
            raise RuntimeError(f"orca worktree rm failed: {res.stderr.strip() or res.stdout.strip()}")


def sanitize_name_hint(work_key: str, name_hint: str | None) -> str:
    raw = (name_hint or "").strip().lower()
    if ".." in raw or "/" in raw or "\\" in raw:
        raw = _SAFE_HINT_RE.sub("-", raw)
    cleaned = _SAFE_HINT_RE.sub("-", raw).strip("-_")
    if not cleaned:
        cleaned = f"task-{hashlib.sha256(work_key.encode('utf-8')).hexdigest()[:12]}"
    return cleaned[:48]


def worktree_fingerprint(workdir: Path) -> str:
    head = _run_cmd(["git", "rev-parse", "HEAD"], cwd=workdir).stdout.strip()
    status = _run_cmd(["git", "status", "--porcelain", "--untracked-files=all"], cwd=workdir).stdout
    hasher = hashlib.sha256()
    hasher.update(head.encode("utf-8"))
    hasher.update(b"\x00")
    for line in sorted(status.splitlines()):
        hasher.update(line.encode("utf-8"))
        hasher.update(b"\x00")
        rel = line[3:].strip()
        if " -> " in rel:
            rel = rel.split(" -> ", 1)[1]
        fpath = workdir / rel
        if fpath.is_file() and not fpath.is_symlink():
            hasher.update(fpath.read_bytes())
        hasher.update(b"\x01")
    return hasher.hexdigest()


def restore_readonly_worktree(workdir: Path, head_sha: str | None) -> None:
    # Only for runner-owned readonly worktrees, which hold no work of their own.
    target = head_sha or "HEAD"
    for cmd in (["git", "checkout", "--force", "--detach", target], ["git", "clean", "-ffdx"]):
        res = _run_cmd(cmd, cwd=workdir)
        if res.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd)} failed in {workdir}: {res.stderr.strip()}")


def _registered_worktree_paths(primary_checkout: Path) -> set[Path]:
    res = _run_cmd(["git", "worktree", "list", "--porcelain"], cwd=primary_checkout)
    return {
        Path(line[len("worktree ") :].strip()).resolve()
        for line in res.stdout.splitlines()
        if line.startswith("worktree ")
    }


def _registered_worktrees(primary_checkout: Path) -> dict[str, Path]:
    res = _run_cmd(["git", "worktree", "list", "--porcelain"], cwd=primary_checkout)
    mapping: dict[str, Path] = {}
    cur_path: Path | None = None
    for line in res.stdout.splitlines():
        if line.startswith("worktree "):
            cur_path = Path(line[len("worktree ") :].strip()).resolve()
        elif line.startswith("branch refs/heads/") and cur_path is not None:
            branch = line[len("branch refs/heads/") :].strip()
            mapping[branch] = cur_path
    return mapping


def _local_branches(primary_checkout: Path) -> set[str]:
    res = _run_cmd(["git", "for-each-ref", "--format=%(refname:short)", "refs/heads"], cwd=primary_checkout)
    return {line.strip() for line in res.stdout.splitlines() if line.strip()}


def _git_head_sha(workdir: Path) -> str | None:
    res = _run_cmd(["git", "rev-parse", "HEAD"], cwd=workdir)
    return res.stdout.strip() if res.returncode == 0 and res.stdout.strip() else None


def _git_is_dirty(workdir: Path) -> bool:
    res = _run_cmd(["git", "status", "--porcelain", "--untracked-files=all"], cwd=workdir)
    return bool(res.stdout.strip())


def _head_is_on_remote(workdir: Path, head_sha: str) -> bool:
    res = _run_cmd(["git", "branch", "-r", "--contains", head_sha], cwd=workdir)
    return res.returncode == 0 and bool(res.stdout.strip())


class WorktreeManager:
    def __init__(
        self,
        *,
        db_path: Path,
        backend: WorktreeBackend,
        worktree_root: Path | None = None,
        reclaim_stale_seconds: float = 600.0,
    ) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._backend = backend
        self._worktree_root = worktree_root
        self._reclaim_stale_seconds = reclaim_stale_seconds
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS workspaces (
                namespace TEXT NOT NULL,
                project_key TEXT NOT NULL,
                owner_scope TEXT NOT NULL,
                work_key TEXT NOT NULL,
                dir_name TEXT NOT NULL,
                workdir TEXT NOT NULL,
                branch TEXT NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (namespace, project_key, owner_scope, work_key)
            )
            """
        )
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def list_ready_work_keys(self) -> list[dict[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT namespace, project_key, owner_scope, work_key FROM workspaces WHERE status = 'ready'"
            ).fetchall()
        return [
            {"namespace": r[0], "project_key": r[1], "owner_scope": r[2], "work_key": r[3]}
            for r in rows
        ]

    def get_record(
        self, *, namespace: str, project_key: str, owner_scope: str, work_key: str
    ) -> WorkspaceRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT namespace, project_key, owner_scope, work_key, dir_name, workdir, branch, status, updated_at
                FROM workspaces
                WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
                """,
                (namespace, project_key, owner_scope, work_key),
            ).fetchone()
        if row is None:
            return None
        return WorkspaceRecord(
            namespace=row[0],
            project_key=row[1],
            owner_scope=row[2],
            work_key=row[3],
            dir_name=row[4],
            workdir=Path(row[5]),
            branch=row[6],
            status=row[7],
            updated_at=row[8],
        )

    def assert_writable(
        self,
        *,
        policy: NamespacePolicy,
        project: ProjectCheckout,
        owner_scope: str,
        work_key: str,
        workdir: Path,
    ) -> None:
        resolved = workdir.resolve()
        primary = project.checkout_path.resolve()
        if resolved == primary:
            raise WorkspaceIsolationError(f"refusing write to primary checkout {primary}")
        expected_prefix = f"{policy.prefix}-"
        if not resolved.name.startswith(expected_prefix):
            raise WorkspaceIsolationError(
                f"workdir {resolved.name!r} violates namespace prefix {expected_prefix!r}"
            )
        rec = self.get_record(
            namespace=policy.namespace,
            project_key=project.project_key,
            owner_scope=owner_scope,
            work_key=work_key,
        )
        if rec is None or rec.workdir.resolve() != resolved or rec.status != "ready":
            raise WorkspaceIsolationError(
                f"workdir {resolved} is not owned as ready by ({policy.namespace}, {owner_scope}, {work_key})"
            )

    def resolve(
        self,
        *,
        policy: NamespacePolicy,
        project: ProjectCheckout,
        owner_scope: str,
        work_key: str,
        readonly: bool,
        name_hint: str | None = None,
    ) -> ResolvedWorktree:
        primary = project.checkout_path.resolve()
        if readonly:
            return self._resolve_readonly(policy=policy, project=project, owner_scope=owner_scope, work_key=work_key)
        with self._lock:
            existing = self.get_record(
                namespace=policy.namespace,
                project_key=project.project_key,
                owner_scope=owner_scope,
                work_key=work_key,
            )
            registered = _registered_worktrees(primary)
            if existing is not None:
                if existing.status == "creating":
                    actual = registered.get(existing.branch)
                    if actual is not None and actual.exists():
                        self._mark_ready(existing, actual)
                        return ResolvedWorktree(
                            workdir=actual,
                            branch=existing.branch,
                            created=False,
                            ref=self._build_ref(work_key, existing.branch, actual),
                        )
                    self._delete_record(policy.namespace, project.project_key, owner_scope, work_key)
                elif existing.status == "ready" and existing.workdir.exists():
                    return ResolvedWorktree(
                        workdir=existing.workdir,
                        branch=existing.branch,
                        created=False,
                        ref=self._build_ref(work_key, existing.branch, existing.workdir),
                    )
                elif existing.status == "ready":
                    actual = registered.get(existing.branch)
                    if actual is not None and actual.exists():
                        self._mark_ready(existing, actual)
                        return ResolvedWorktree(
                            workdir=actual,
                            branch=existing.branch,
                            created=False,
                            ref=self._build_ref(work_key, existing.branch, actual),
                        )
                    self._delete_record(policy.namespace, project.project_key, owner_scope, work_key)

            base_dir = self._base_dir(primary)
            hint = sanitize_name_hint(work_key, name_hint)
            existing_branches = _local_branches(primary) | set(registered.keys())
            dir_name, branch = self._allocate_names(policy.prefix, hint, base_dir, existing_branches)
            target_dir = (base_dir / dir_name).resolve()

            now = utc_now_iso()
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                INSERT INTO workspaces (
                    namespace, project_key, owner_scope, work_key, dir_name, workdir, branch, status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'creating', ?)
                """,
                (policy.namespace, project.project_key, owner_scope, work_key, dir_name, str(target_dir), branch, now),
            )
            self._conn.commit()

        try:
            created = self._backend.create(
                primary_checkout=primary,
                workdir=target_dir,
                branch=branch,
                base_ref=project.default_base_ref,
            )
        except Exception:
            with self._lock:
                self._delete_record(policy.namespace, project.project_key, owner_scope, work_key)
            raise

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                UPDATE workspaces
                SET workdir = ?, branch = ?, status = 'ready', updated_at = ?
                WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
                """,
                (
                    str(created.path),
                    created.branch,
                    utc_now_iso(),
                    policy.namespace,
                    project.project_key,
                    owner_scope,
                    work_key,
                ),
            )
            self._conn.commit()

        return ResolvedWorktree(
            workdir=created.path,
            branch=created.branch,
            created=True,
            ref=self._build_ref(work_key, created.branch, created.path),
        )

    def readonly_workdir(self, *, policy: NamespacePolicy, project: ProjectCheckout, owner_scope: str, work_key: str) -> Path:
        digest = hashlib.sha256(
            "\0".join((policy.namespace, project.project_key, owner_scope, work_key)).encode("utf-8")
        ).hexdigest()[:16]
        return self._base_dir(project.checkout_path.resolve()) / f"{policy.prefix}-ro-{digest}"

    def _resolve_readonly(
        self, *, policy: NamespacePolicy, project: ProjectCheckout, owner_scope: str, work_key: str
    ) -> ResolvedWorktree:
        # A readonly run never mounts the primary checkout or another task's worktree: it gets a detached,
        # runner-owned worktree per (namespace, project, owner_scope, work_key), reset before every run. It shows the
        # task worktree's committed HEAD when there is one, otherwise the project's base ref.
        primary = project.checkout_path.resolve()
        with self._lock:
            rec = self.get_record(
                namespace=policy.namespace, project_key=project.project_key, owner_scope=owner_scope, work_key=work_key
            )
            target: str | None = None
            if rec is not None and rec.status == "ready" and rec.workdir.exists():
                target = _git_head_sha(rec.workdir)
            if target is None:
                target = _resolve_commit(primary, project.default_base_ref)
            workdir = self.readonly_workdir(policy=policy, project=project, owner_scope=owner_scope, work_key=work_key)
            if workdir.resolve() in _registered_worktree_paths(primary) and workdir.exists():
                restore_readonly_worktree(workdir, target)
            else:
                _run_cmd(["git", "worktree", "prune"], cwd=primary)
                if workdir.exists():
                    raise WorkspaceIsolationError(f"readonly workdir {workdir} exists but is not a worktree of {primary}")
                res = _run_cmd(["git", "worktree", "add", "--detach", str(workdir), target], cwd=primary)
                if res.returncode != 0:
                    raise RuntimeError(f"git worktree add --detach failed: {res.stderr.strip() or res.stdout.strip()}")
            workdir = workdir.resolve()
            return ResolvedWorktree(
                workdir=workdir,
                branch="HEAD",
                created=False,
                ref=self._build_ref(work_key, "HEAD", workdir),
            )

    def _remove_readonly(self, *, policy: NamespacePolicy, project: ProjectCheckout, owner_scope: str, work_key: str) -> bool:
        primary = project.checkout_path.resolve()
        workdir = self.readonly_workdir(policy=policy, project=project, owner_scope=owner_scope, work_key=work_key)
        if not workdir.exists():
            return False
        res = _run_cmd(["git", "worktree", "remove", "--force", str(workdir)], cwd=primary)
        if res.returncode != 0:
            raise RuntimeError(f"git worktree remove failed: {res.stderr.strip() or res.stdout.strip()}")
        return True

    def _base_dir(self, primary: Path) -> Path:
        base_dir = (self._worktree_root or (primary.parent / f".{primary.name}-worktrees")).resolve()
        base_dir.mkdir(parents=True, exist_ok=True)
        return base_dir

    def reclaim(
        self,
        *,
        policy: NamespacePolicy,
        project: ProjectCheckout,
        owner_scope: str,
        work_key: str,
        merged_head_sha: str | None = None,
    ) -> ReclaimResult:
        primary = project.checkout_path.resolve()
        try:
            removed_readonly = self._remove_readonly(
                policy=policy, project=project, owner_scope=owner_scope, work_key=work_key
            )
        except RuntimeError as exc:
            return ReclaimResult(reclaimed=False, status="error", reason=str(exc), blockers=("remove_failed",))
        with self._lock:
            rec = self.get_record(
                namespace=policy.namespace,
                project_key=project.project_key,
                owner_scope=owner_scope,
                work_key=work_key,
            )
            if rec is None:
                if removed_readonly:
                    return ReclaimResult(
                        reclaimed=True,
                        status="reclaimed",
                        reason="readonly worktree removed; no task worktree recorded",
                        blockers=(),
                        branch_kept=None,
                    )
                return ReclaimResult(
                    reclaimed=False,
                    status="not_found",
                    reason="workspace record not found",
                    blockers=(),
                    branch_kept=None,
                )
            expected_prefix = f"{policy.prefix}-"
            if not rec.dir_name.startswith(expected_prefix):
                return ReclaimResult(
                    reclaimed=False,
                    status="blocked",
                    reason=f"prefix mismatch: expected {expected_prefix!r}",
                    blockers=("prefix_mismatch",),
                    branch_kept=rec.branch,
                )
            claim_token = self._claim_for_reclaim(rec)
            if claim_token is None:
                return ReclaimResult(
                    reclaimed=False,
                    status="blocked",
                    reason="workspace is already being reclaimed",
                    blockers=("reclaim_in_progress",),
                    branch_kept=rec.branch,
                )

        blockers = self._local_git_blockers(rec.workdir, merged_head_sha=merged_head_sha)
        if blockers:
            self._release_reclaim(rec, claim_token)
            return ReclaimResult(
                reclaimed=False,
                status="blocked",
                reason="local git safety gates refused reclaim: " + ", ".join(blockers),
                blockers=tuple(blockers),
                branch_kept=rec.branch,
            )

        try:
            if rec.workdir.exists():
                self._backend.remove(primary_checkout=primary, workdir=rec.workdir)
        except Exception as exc:
            self._release_reclaim(rec, claim_token)
            return ReclaimResult(
                reclaimed=False,
                status="error",
                reason=str(exc),
                blockers=("remove_failed",),
                branch_kept=rec.branch,
            )

        self._finish_reclaim(rec, claim_token)
        return ReclaimResult(
            reclaimed=True,
            status="reclaimed",
            reason="reclaimed after local git safety checks",
            blockers=(),
            branch_kept=rec.branch,
        )

    def _local_git_blockers(self, workdir: Path, *, merged_head_sha: str | None) -> list[str]:
        if not workdir.exists():
            return []
        blockers: list[str] = []
        if _git_is_dirty(workdir):
            blockers.append("dirty_working_tree")
        head_sha = _git_head_sha(workdir)
        if head_sha is None:
            blockers.append("unreadable_head")
        else:
            matches_merged = merged_head_sha is not None and head_sha == merged_head_sha.strip()
            if not matches_merged and not _head_is_on_remote(workdir, head_sha):
                blockers.append("unpushed_commits")
        return blockers

    def _allocate_names(
        self, prefix: str, hint: str, base_dir: Path, existing_branches: set[str]
    ) -> tuple[str, str]:
        idx = 1
        while True:
            suffix = "" if idx == 1 else f"-{idx}"
            dir_name = f"{prefix}-{hint}{suffix}"
            branch = f"{prefix}/{hint}{suffix}"
            if not (base_dir / dir_name).exists() and branch not in existing_branches:
                return dir_name, branch
            idx += 1

    def _claim_for_reclaim(self, rec: WorkspaceRecord) -> str | None:
        now_iso = utc_now_iso()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            cur = self._conn.execute(
                """
                UPDATE workspaces
                SET status = 'reclaiming', updated_at = ?
                WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
                  AND updated_at = ?
                  AND (status = 'ready' OR status = 'creating')
                """,
                (now_iso, rec.namespace, rec.project_key, rec.owner_scope, rec.work_key, rec.updated_at),
            )
            self._conn.commit()
            if cur.rowcount == 1:
                return now_iso
            if rec.status == "reclaiming" and self._is_stale(rec.updated_at):
                cur2 = self._conn.execute(
                    """
                    UPDATE workspaces
                    SET status = 'reclaiming', updated_at = ?
                    WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
                      AND updated_at = ? AND status = 'reclaiming'
                    """,
                    (now_iso, rec.namespace, rec.project_key, rec.owner_scope, rec.work_key, rec.updated_at),
                )
                self._conn.commit()
                if cur2.rowcount == 1:
                    return now_iso
            return None

    def _release_reclaim(self, rec: WorkspaceRecord, claim_token: str) -> None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                UPDATE workspaces
                SET status = 'ready', updated_at = ?
                WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
                  AND status = 'reclaiming' AND updated_at = ?
                """,
                (utc_now_iso(), rec.namespace, rec.project_key, rec.owner_scope, rec.work_key, claim_token),
            )
            self._conn.commit()

    def _finish_reclaim(self, rec: WorkspaceRecord, claim_token: str) -> None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                DELETE FROM workspaces
                WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
                  AND status = 'reclaiming' AND updated_at = ?
                """,
                (rec.namespace, rec.project_key, rec.owner_scope, rec.work_key, claim_token),
            )
            self._conn.commit()

    def _mark_ready(self, rec: WorkspaceRecord, actual: Path) -> None:
        self._conn.execute("BEGIN IMMEDIATE")
        self._conn.execute(
            """
            UPDATE workspaces
            SET workdir = ?, status = 'ready', updated_at = ?
            WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
            """,
            (str(actual), utc_now_iso(), rec.namespace, rec.project_key, rec.owner_scope, rec.work_key),
        )
        self._conn.commit()

    def _delete_record(self, namespace: str, project_key: str, owner_scope: str, work_key: str) -> None:
        self._conn.execute("BEGIN IMMEDIATE")
        self._conn.execute(
            """
            DELETE FROM workspaces
            WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
            """,
            (namespace, project_key, owner_scope, work_key),
        )
        self._conn.commit()

    def _is_stale(self, updated_at: str) -> bool:
        try:
            from datetime import datetime

            ts = datetime.fromisoformat(updated_at).timestamp()
            return (time.time() - ts) >= self._reclaim_stale_seconds
        except ValueError:
            return False

    def _build_ref(self, work_key: str, branch: str, workdir: Path) -> WorktreeRef:
        return WorktreeRef(
            work_key=work_key,
            branch=branch,
            head_sha=_git_head_sha(workdir),
            dirty=_git_is_dirty(workdir),
        )
