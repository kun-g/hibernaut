from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, cast

from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore, WorkspaceRecord
from agent_runtime.core.ports import OutboxPort, SessionStorePort

WorktreeBackend = Literal["orca", "git"]

_GITHUB_ISSUE_RE = re.compile(r"^github:[^#]+#(\d+)$")
_ISSUE_RE = re.compile(r"^issue:(\d+)$")
_PR_RE = re.compile(r"^(?:github-pr:[^#]+#|pr:)(\d+)$")
_NAMING_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str], Path | None], CommandResult]


class WorkspaceIsolationError(RuntimeError):
    pass


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
        session_store: SessionStorePort | None = None,
        backend: WorktreeBackend = "orca",
        create_terminal: bool = False,
        runner: CommandRunner | None = None,
        worktree_base_dir: str | Path | None = None,
        reclaim_stale_after_seconds: float = 600.0,
        usage: Callable[[str], list[str]] | None = None,
        primary_inbox_id: str | None = None,
        isolate_inboxes: bool = False,
    ) -> None:
        # isolate_inboxes: the project serves several inboxes from one checkout, so each (inbox, work_key) writes
        # only in its own managed hb-* worktree. The primary inbox keeps the bare work_key, as before isolation.
        if isolate_inboxes and not primary_inbox_id:
            raise ValueError("isolate_inboxes needs the project's primary_inbox_id")
        self._primary_inbox_id = primary_inbox_id
        self._isolate = isolate_inboxes
        self._store = workspace_store
        # Reports who else still needs a task's worktree (reviewers, queued events, an unfinished delivery).
        self._usage = usage
        self._session_store = session_store
        self._reclaim_stale_after_seconds = reclaim_stale_after_seconds
        self._backend: WorktreeBackend = backend
        self._create_terminal = create_terminal
        self._runner: CommandRunner = runner or _default_runner
        self._worktree_base_dir = (
            Path(worktree_base_dir).resolve() if worktree_base_dir is not None else None
        )

    @property
    def isolates_inboxes(self) -> bool:
        return self._isolate

    def workspace_key(self, inbox_id: str | None, work_key: str) -> str:
        # The workspaces-table key for a task; equal to work_key unless another inbox of an isolated project asks.
        if not self._isolate or inbox_id is None or inbox_id == self._primary_inbox_id:
            return work_key
        return f"{inbox_id}/{work_key}"

    def resolve_workdir(
        self,
        *,
        work_key: str,
        primary_checkout: str | Path,
        explicit_workdir: str | Path | None = None,
        inbox_id: str | None = None,
    ) -> Path:
        if not self._isolate:
            # Priority 1: explicit read-only / review override skips worktree creation entirely.
            if explicit_workdir is not None:
                return Path(explicit_workdir).resolve()
            return self._resolve_managed(work_key, work_key, Path(primary_checkout).resolve(), inbox_id)
        if inbox_id is None:
            raise WorkspaceIsolationError(f"workspace for {work_key!r} requested without its inbox_id")
        key = self.workspace_key(inbox_id, work_key)
        primary = Path(primary_checkout).resolve()
        if explicit_workdir is not None:
            # Only the task's own managed worktree may be named explicitly; anything else could be shared.
            record = self._owned_record(key, inbox_id)
            requested = Path(explicit_workdir).resolve()
            if record is None or record.status != "ready" or Path(record.workdir).resolve() != requested:
                raise WorkspaceIsolationError(
                    f"explicit_workdir {requested} refused for {inbox_id!r}: a project serving several inboxes "
                    f"writes only in the managed hb-* worktree of {work_key!r} owned by that inbox"
                )
        workdir = self._resolve_managed(key, work_key, primary, inbox_id)
        self._check_isolated(workdir, primary, inbox_id)
        return workdir

    def assert_writable(self, *, inbox_id: str, work_key: str, workdir: str | Path) -> None:
        # Re-checked before a resumed or retried launch, whose workdir comes from a checkpoint.
        if not self._isolate:
            return
        key = self.workspace_key(inbox_id, work_key)
        record = self._owned_record(key, inbox_id)
        path = Path(workdir).resolve()
        if record is None or record.status != "ready" or Path(record.workdir).resolve() != path:
            raise WorkspaceIsolationError(
                f"{path} is not the managed worktree of {work_key!r} owned by {inbox_id!r}; refusing to write there"
            )
        self._check_isolated(path, Path(record.repo_checkout).resolve(), inbox_id)

    def _owned_record(self, key: str, inbox_id: str) -> WorkspaceRecord | None:
        record = self._store.get(key)
        if record is None:
            return None
        if record.inbox_id is None:
            # A row from before ownership was recorded: only the inbox whose sessions alone used it adopts it.
            others = self._store.session_inboxes(key) - {inbox_id}
            if others or not self._store.claim_owner(key, inbox_id):
                raise WorkspaceIsolationError(
                    f"workspace {key!r} at {record.workdir} was used by {', '.join(sorted(others)) or 'another inbox'}; "
                    f"{inbox_id!r} cannot write there"
                )
            return self._store.get(key)
        if record.inbox_id != inbox_id:
            raise WorkspaceIsolationError(
                f"workspace {key!r} at {record.workdir} belongs to {record.inbox_id!r}, not {inbox_id!r}"
            )
        return record

    @staticmethod
    def _check_isolated(workdir: Path, primary: Path, inbox_id: str) -> None:
        if workdir == primary or not workdir.name.startswith("hb-"):
            raise WorkspaceIsolationError(
                f"{workdir} is not a managed hb-* worktree of {primary}; {inbox_id!r} cannot write there"
            )

    def _resolve_managed(self, key: str, work_key: str, primary: Path, inbox_id: str | None) -> Path:
        # inbox_id is recorded as the owner of a new row; only isolation enforces it on existing rows.
        existing = self._owned_record(key, inbox_id) if self._isolate and inbox_id is not None else self._store.get(key)

        branches = self._registered_worktrees(primary)
        if existing is not None:
            if Path(existing.repo_checkout).resolve() != primary or existing.status == "reclaiming":
                raise RuntimeError(f"Cannot reuse workspace {key!r} in {primary}: {existing}")
            if existing.branch in branches:
                actual = self._worktree_path(branches, primary, existing.branch)
                if existing.status != "ready" or str(actual) != existing.workdir:
                    self._store.mark_ready(key, workdir=str(actual), branch=existing.branch)
                return actual
            if existing.status == "ready":
                raise RuntimeError(f"No registered worktree for {existing.branch!r} in {primary}")

        # Priority 3: dynamic creation with Safety 1 (creating intent) + Safety 2 (path persistence).
        self._runner(["git", "fetch", "origin"], primary)
        base_commit = self._resolve_base_commit(primary)

        base_dir = self._worktree_base_dir if self._worktree_base_dir is not None else primary.parent
        if existing is None:
            # Naming and reservation are one step: concurrent inboxes with the same work_key get distinct names.
            with _NAMING_LOCK:
                existing_names = {Path(r.workdir).name for r in self._store.list_by_repo(str(primary))}
                refs = self._runner(["git", "for-each-ref", "--format=%(refname:strip=2)", "refs/heads/hb/"], primary)
                if refs.returncode != 0:
                    raise RuntimeError(f"Cannot list local branches in {primary}: {refs.stderr.strip()}")
                existing_names.update(name.replace("hb/", "hb-", 1) for name in refs.stdout.splitlines())
                if base_dir.is_dir():
                    existing_names.update(p.name for p in base_dir.iterdir() if p.is_dir())
                dir_name, branch_name = derive_worktree_names(work_key, self._store.next_task_seq(), existing_names)
                expected_workdir = (base_dir / dir_name).resolve()
                self._store.reserve_creating(
                    work_key=key, repo_checkout=str(primary), workdir=str(expected_workdir), branch=branch_name,
                    inbox_id=inbox_id,
                )
        else:
            expected_workdir = Path(existing.workdir)
            dir_name, branch_name = expected_workdir.name, existing.branch

        reported_path = None
        if self._backend == "orca":
            branch_name, reported_path = self._create_via_orca(
                primary=primary,
                dir_name=dir_name,
                base_commit=base_commit,
                branch_name=branch_name,
            )
            if branch_name in branches:
                raise RuntimeError(f"Orca returned a previously registered worktree for {branch_name!r}")
        else:
            self._create_via_git(
                primary=primary,
                expected_workdir=expected_workdir,
                base_commit=base_commit,
                branch_name=branch_name,
            )

        actual_workdir = self._worktree_path(self._registered_worktrees(primary), primary, branch_name)
        if reported_path is not None and reported_path != actual_workdir:
            raise RuntimeError(f"Orca returned {reported_path} for {branch_name!r}, Git registered {actual_workdir}")
        self._store.mark_ready(key, workdir=str(actual_workdir), branch=branch_name)
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
        # work_key is the sessions' key; with inbox_id it maps to the inbox's workspace, without it it is the
        # workspaces-table key itself (as hibernaut_list_workspaces shows it).
        key = self.workspace_key(inbox_id, work_key)
        record = self._store.get(key)
        if record is None:
            return ReclaimResult(reclaimed=False, reason="workspace not managed in workspaces table")
        if self._isolate and inbox_id is not None and record.inbox_id not in (None, inbox_id):
            msg = f"Refused reclaim for '{record.workdir}': owned by {record.inbox_id!r}, not {inbox_id!r}"
            self._alert(notify_outbox, inbox_id, msg)
            return ReclaimResult(reclaimed=False, reason=msg, workdir=record.workdir)
        sessions_key = work_key
        if inbox_id is None and record.inbox_id and key.startswith(f"{record.inbox_id}/"):
            sessions_key = key.removeprefix(f"{record.inbox_id}/")

        owner = (record.inbox_id or inbox_id or self._primary_inbox_id) if self._isolate else None
        workdir = Path(record.workdir)
        refusal = self._refusal(sessions_key, record.branch, record.repo_checkout, workdir, owner, key)
        if refusal is not None:
            self._alert(notify_outbox, inbox_id, refusal)
            return ReclaimResult(reclaimed=False, reason=refusal, workdir=record.workdir)

        stale_before = (
            datetime.now(timezone.utc) - timedelta(seconds=self._reclaim_stale_after_seconds)
        ).isoformat()
        claim = self._store.claim_for_reclaim(
            key,
            seen_updated_at=record.updated_at,
            stale_before=stale_before,
        )
        if claim is None:
            return ReclaimResult(
                reclaimed=False,
                reason="workspace changed or is being reclaimed by another caller",
                workdir=record.workdir,
            )
        # The gates ran before the claim; a session, event or commit may have arrived since.
        late = self._refusal(sessions_key, record.branch, record.repo_checkout, workdir, owner, key)
        if late is not None:
            self._store.release_reclaim(key, claim=claim, status=record.status)
            msg = f"{late} (on recheck after claiming the worktree)"
            self._alert(notify_outbox, inbox_id, msg)
            return ReclaimResult(reclaimed=False, reason=msg, workdir=record.workdir)
        primary = Path(record.repo_checkout)
        heartbeat = _ClaimHeartbeat(
            self._store,
            key,
            claim,
            interval_seconds=self._reclaim_stale_after_seconds / 3,
        )
        try:
            if self._backend == "orca":
                rm_res = self._runner(["orca", "worktree", "remove", str(workdir)], primary)
            else:
                rm_res = self._runner(["git", "worktree", "remove", str(workdir)], primary)
        except BaseException:
            self._store.release_reclaim(key, claim=heartbeat.stop(), status=record.status)
            raise
        claim = heartbeat.stop()
        if rm_res.returncode != 0:
            self._store.release_reclaim(key, claim=claim, status=record.status)
            msg = f"Failed to remove worktree '{workdir}': {rm_res.stderr.strip()}"
            self._alert(notify_outbox, inbox_id, msg)
            return ReclaimResult(reclaimed=False, reason=msg, workdir=record.workdir)

        # Only the claim holder may drop the record; a takeover may have re-registered the key.
        if not self._store.finish_reclaim(key, claim=claim):
            msg = f"Reclaim of '{workdir}' lost its claim; record left to its new owner"
            self._alert(notify_outbox, inbox_id, msg)
            return ReclaimResult(reclaimed=False, reason=msg, workdir=record.workdir)
        return ReclaimResult(reclaimed=True, reason="reclaimed", workdir=record.workdir)

    def _refusal(self, work_key: str, branch: str, repo_checkout: str, workdir: Path, owner: str | None, storage_key: str) -> str | None:
        # Gate 0: no active or suspended session, child included, still references work_key.
        if self._session_store is not None:
            active_refs = self._session_store.count_active_by_work_key(work_key, owner)
            if active_refs > 0:
                list_fn = getattr(self._session_store, "list_active_by_work_key", None)
                occupying = list_fn(work_key, owner) if callable(list_fn) else []
                suffix = f" ({', '.join(occupying)})" if occupying else f" ({active_refs} active)"
                return f"Skipped reclaim for '{work_key}': active/suspended session(s) still referencing worktree{suffix}"

        # Gate 1: no reviewer, queued event or unfinished delivery still needs the worktree.
        if self._usage is not None:
            blockers = self._usage(work_key)
            if storage_key != work_key:
                blockers = [*blockers, *self._usage(storage_key)]
            if blockers:
                return f"Skipped reclaim for '{work_key}': still in use: {'; '.join(blockers)}"

        if not workdir.name.startswith("hb-"):
            return f"Refused reclaim for '{workdir}': directory name lacks 'hb-' prefix"

        cwd = workdir if workdir.is_dir() else Path(repo_checkout)
        # Gate 2: GitHub confirms there is no open PR for this branch.
        open_prs = self._pr_list(branch, "open", "number", cwd)
        if open_prs is None:
            return f"Skipped reclaim for '{workdir}': could not check for open PRs on '{branch}'"
        if open_prs:
            return f"Skipped reclaim for '{workdir}': branch '{branch}' has open PR(s)"

        if workdir.is_dir():
            # Gate 3: nothing staged, unstaged or untracked.
            status_res = self._runner(["git", "status", "--porcelain", "--untracked-files=all"], workdir)
            if status_res.returncode != 0 or status_res.stdout.strip():
                return f"Skipped reclaim for '{workdir}': uncommitted changes present"

            # Gate 4: every commit of this worktree is on a remote, or its PR was squash-merged at exactly this head.
            unpushed = self._runner(["git", "log", "HEAD", "--not", "--remotes", "--oneline"], workdir)
            if unpushed.returncode != 0:
                return f"Skipped reclaim for '{workdir}': could not list unpushed commits"
            if unpushed.stdout.strip() and not self._squash_merged(branch, workdir):
                return f"Skipped reclaim for '{workdir}': unpushed commits present"
        return None

    def _pr_list(self, branch: str, state: str, fields: str, cwd: Path) -> list[dict[str, object]] | None:
        res = self._runner(["gh", "pr", "list", "--head", branch, "--state", state, "--json", fields], cwd)
        if res.returncode != 0:
            return None
        if not res.stdout.strip():
            return []
        try:
            rows: object = json.loads(res.stdout)
        except json.JSONDecodeError:
            return None
        if not isinstance(rows, list):
            return None
        return [cast(dict[str, object], r) for r in cast(list[object], rows) if isinstance(r, dict)]

    def _squash_merged(self, branch: str, workdir: Path) -> bool:
        # A squash merge leaves the branch's own commits off every remote; the evidence is a merged PR whose head is
        # this worktree's HEAD and whose merge commit a remote branch contains.
        head = self._runner(["git", "rev-parse", "HEAD"], workdir)
        if head.returncode != 0 or not head.stdout.strip():
            return False
        merged = self._pr_list(branch, "merged", "number,headRefOid,mergeCommit", workdir) or []
        for pr in merged:
            commit = pr.get("mergeCommit")
            oid = cast(dict[str, object], commit).get("oid") if isinstance(commit, dict) else None
            if pr.get("headRefOid") != head.stdout.strip() or not isinstance(oid, str) or not oid:
                continue
            if self._on_remote(oid, workdir):
                return True
        return False

    def _on_remote(self, oid: str, workdir: Path) -> bool:
        def contains() -> bool:
            res = self._runner(["git", "branch", "-r", "--contains", oid], workdir)
            return res.returncode == 0 and bool(res.stdout.strip())

        if contains():
            return True
        self._runner(["git", "fetch", "--quiet", "--all"], workdir)
        return contains()

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
    ) -> tuple[str, Path]:
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
            "--json",
        ]
        res = self._runner(cmd, primary)
        if res.returncode != 0:
            raise RuntimeError(f"orca worktree create failed: {res.stderr.strip()}")
        try:
            worktree = json.loads(res.stdout)["result"]["worktree"]
            ref, path = worktree["branch"], worktree["path"]
        except (ValueError, KeyError, TypeError) as exc:
            raise RuntimeError("Invalid Orca worktree creation receipt") from exc
        if not isinstance(ref, str) or not isinstance(path, str) or not Path(path).is_absolute():
            raise RuntimeError("Invalid branch or path in Orca worktree creation receipt")
        branch = ref.removeprefix("refs/heads/")
        if not re.fullmatch(re.escape(branch_name) + r"(?:-\d+)?", branch):
            raise RuntimeError(f"Orca returned unrelated branch {branch!r} for {branch_name!r}")
        return branch, Path(path).resolve()

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


_TRANSIENT_SQLITE_ERRORS = {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}


class _ClaimHeartbeat:
    # Keeps a reclaim claim fresh while a slow removal runs, so no caller treats a live
    # claim as stale and removes the same path concurrently.
    def __init__(
        self,
        store: SQLiteWorkspaceStore,
        work_key: str,
        claim: str,
        *,
        interval_seconds: float,
    ) -> None:
        self._store = store
        self._work_key = work_key
        self._claim = claim
        self._interval = interval_seconds
        self._stopped = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        delay = self._interval
        while not self._stopped.wait(delay):
            try:
                renewed = self._store.renew_reclaim(self._work_key, claim=self._claim)
            except sqlite3.OperationalError as exc:
                # A transient lock must not end the heartbeat; retry soon, well inside
                # the stale window, so the live claim is not taken over.
                if getattr(exc, "sqlite_errorcode", None) not in _TRANSIENT_SQLITE_ERRORS:
                    raise
                delay = self._interval / 4
                continue
            if renewed is None:
                return
            self._claim = renewed
            delay = self._interval

    def stop(self) -> str:
        self._stopped.set()
        self._thread.join()
        return self._claim
