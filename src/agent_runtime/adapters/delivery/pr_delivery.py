from __future__ import annotations

import dataclasses
import json
import os
import re
import shlex
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

from agent_runtime.adapters.storage_sqlite.deliveries import SQLiteDeliveryStore
from agent_runtime.core.models import Event
from agent_runtime.core.ports import PermissionPort

ChangeKind = Literal["breaking", "feature", "fix", "test_docs"]
CHANGE_KINDS: tuple[ChangeKind, ...] = ("breaking", "feature", "fix", "test_docs")
Bump = Literal["major", "minor", "patch", "skip"]
BUMP_FOR_KIND: dict[str, Bump] = {"breaking": "major", "feature": "minor", "fix": "patch", "test_docs": "skip"}
DeliveryState = Literal["local", "pr_open", "merged"]
MergeMethod = Literal["squash", "merge", "rebase"]
MERGE_METHODS: tuple[MergeMethod, ...] = ("squash", "merge", "rebase")
IssueLink = Literal["closes", "refs", "none"]
ISSUE_LINKS: tuple[IssueLink, ...] = ("closes", "refs", "none")
_ISSUE_SOURCES = frozenset({"github:issue_poll", "github:issues", "github:issue_comment"})
_REPO = r"[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9._-]+"
_ISSUE_KEY = re.compile(rf"github:(?P<repo>{_REPO})#(?P<number>[1-9][0-9]*)")
_ISSUE_REF = re.compile(rf"(?P<repo>{_REPO})#(?P<number>[1-9][0-9]*)")
OPEN_PR_TOOL = "delivery.open_pr"
MERGE_TOOL = "delivery.merge"
DEFAULT_VERSION_PATTERN = r"""__version__\s*=\s*["'](?P<version>[0-9]+\.[0-9]+\.[0-9]+)["']"""
# Tokens in the daemon environment never reach git, gh or the checks: remote calls authenticate through the host's
# own credential store (gh auth login, a git credential helper, an SSH agent) or a trusted wrapper in
# gh_command / git_command, outside this process.
TOKEN_ENV = ("GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN")
_SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
_MAX_MAIN_MOVES = 3
_TAIL = 4000
# No CI result for this long while the base has moved past the PR: GitHub may never run CI for it.
_EMPTY_CHECKS_GRACE = 300.0
CIResult = Literal["passed", "failed", "stale"]


def bump_version(version: str, bump: Bump) -> str:
    m = _SEMVER.fullmatch(version.strip())
    if m is None:
        raise ValueError(f"version {version!r} is not MAJOR.MINOR.PATCH")
    major, minor, patch = (int(x) for x in m.groups())
    if bump == "major":
        return f"{major + 1}.0.0"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    if bump == "patch":
        return f"{major}.{minor}.{patch + 1}"
    return f"{major}.{minor}.{patch}"


@dataclass(frozen=True, slots=True)
class DeliveryConfig:
    repo: str = ""
    # Two separate grants: pushing the task branch and opening its PR, and merging that PR.
    open_pr: bool = False
    auto_merge: bool = False
    base_branch: str = "main"
    remote: str = "origin"
    version_file: str = ""
    version_pattern: str = DEFAULT_VERSION_PATTERN
    title_template: str = "{title}"
    checks: tuple[tuple[str, ...], ...] = ()
    check_timeout_seconds: float = 1800.0
    require_review: bool = True
    require_ci: bool = True
    ci_timeout_seconds: float = 1800.0
    ci_poll_seconds: float = 30.0
    merge_method: MergeMethod = "squash"
    git_command: str = "git"
    gh_command: str = "gh"
    # Close the SDE's interactive terminal once the PR is verified and recorded (never for local-only delivery).
    close_terminal: bool = False
    # Merge conflicts with the base and failing checks go back to the developer this many times per delivery.
    max_handbacks: int = 2
    # After a merge, delete the source branch on the remote and, when the worktree is reclaimed, locally.
    delete_branch: bool = True
    # How a PR for a task from a GitHub Issue names that issue: closes it on merge, only refers to it, or neither.
    issue_link: IssueLink = "refs"


_DELIVERY_KEYS = frozenset(f.name for f in dataclasses.fields(DeliveryConfig)) - {"repo"}


def _flag(where: str, raw: Mapping[str, Any], key: str, default: bool) -> bool:
    value = raw.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"{where}: {key} must be true or false")
    return value


def _text(where: str, raw: Mapping[str, Any], key: str, default: str) -> str:
    value = raw.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}: {key} must be a non-empty string")
    return value.strip()


def _seconds(where: str, raw: Mapping[str, Any], key: str, default: float) -> float:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0 or value == float("inf"):
        raise ValueError(f"{where}: {key} must be a positive number of seconds")
    return float(value)


def _count(where: str, raw: Mapping[str, Any], key: str, default: int) -> int:
    value = raw.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{where}: {key} must be a non-negative integer")
    return value


def parse_delivery(where: str, raw: object, *, repo: str) -> DeliveryConfig:
    if not isinstance(raw, dict):
        raise ValueError(f"{where} must be a table")
    item = cast(dict[str, Any], raw)
    unknown = sorted(set(item) - _DELIVERY_KEYS)
    if unknown:
        raise ValueError(f"{where}: unknown key(s) {', '.join(unknown)}")
    d = DeliveryConfig()
    open_pr = _flag(where, item, "open_pr", d.open_pr)
    auto_merge = _flag(where, item, "auto_merge", d.auto_merge)
    if auto_merge and not open_pr:
        raise ValueError(f"{where}: auto_merge needs open_pr = true")
    version_file = str(item.get("version_file", "")).strip()
    if version_file:
        posix = PurePosixPath(version_file)
        if posix.is_absolute() or ".." in posix.parts or "\\" in version_file:
            raise ValueError(f"{where}: version_file must be a path inside the repository, got {version_file!r}")
        version_file = str(posix)
    pattern = _text(where, item, "version_pattern", d.version_pattern)
    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"{where}: version_pattern is not a valid regular expression ({exc})") from None
    if "version" not in compiled.groupindex:
        raise ValueError(f"{where}: version_pattern needs a named group (?P<version>...)")
    title_template = _text(where, item, "title_template", d.title_template)
    try:
        title_template.format(title="t", version="0.0.0")
    except (KeyError, IndexError, ValueError) as exc:
        raise ValueError(f"{where}: title_template may use only {{title}} and {{version}} ({exc})") from None
    checks_raw = item.get("checks", [])
    if not isinstance(checks_raw, list):
        raise ValueError(f"{where}: checks must be a list of commands, each a list of strings")
    checks: list[tuple[str, ...]] = []
    for cmd in cast(list[object], checks_raw):
        argv = cast(list[object], cmd) if isinstance(cmd, list) else None
        if not argv or not all(isinstance(a, str) and a for a in argv):
            raise ValueError(f"{where}: checks must be a list of commands, each a non-empty list of strings")
        checks.append(tuple(str(a) for a in argv))
    method = item.get("merge_method", d.merge_method)
    if method not in MERGE_METHODS:
        raise ValueError(f"{where}: merge_method must be one of {', '.join(MERGE_METHODS)}")
    issue_link = item.get("issue_link", d.issue_link)
    if not isinstance(issue_link, str) or issue_link not in ISSUE_LINKS:
        raise ValueError(f"{where}: issue_link must be one of {', '.join(ISSUE_LINKS)}, got {issue_link!r}")
    return DeliveryConfig(
        repo=repo,
        open_pr=open_pr,
        auto_merge=auto_merge,
        base_branch=_text(where, item, "base_branch", d.base_branch),
        remote=_text(where, item, "remote", d.remote),
        version_file=version_file,
        version_pattern=pattern,
        title_template=title_template,
        checks=tuple(checks),
        check_timeout_seconds=_seconds(where, item, "check_timeout_seconds", d.check_timeout_seconds),
        require_review=_flag(where, item, "require_review", d.require_review),
        require_ci=_flag(where, item, "require_ci", d.require_ci),
        ci_timeout_seconds=_seconds(where, item, "ci_timeout_seconds", d.ci_timeout_seconds),
        ci_poll_seconds=_seconds(where, item, "ci_poll_seconds", d.ci_poll_seconds),
        merge_method=method,
        git_command=_text(where, item, "git_command", d.git_command),
        gh_command=_text(where, item, "gh_command", d.gh_command),
        close_terminal=_flag(where, item, "close_terminal", d.close_terminal),
        max_handbacks=_count(where, item, "max_handbacks", d.max_handbacks),
        delete_branch=_flag(where, item, "delete_branch", d.delete_branch),
        issue_link=cast(IssueLink, issue_link),
    )


def _issue_number(raw: object) -> int | None:
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw if raw > 0 else None
    if isinstance(raw, str) and raw.strip().isdigit():
        number = int(raw.strip())
        return number if number > 0 else None
    return None


def _key_issue(correlation_key: str | None) -> str:
    m = _ISSUE_KEY.fullmatch(correlation_key or "")
    return f"{m.group('repo')}#{m.group('number')}" if m else ""


def _event_issue(evt: Event) -> str:
    from_key = _key_issue(evt.correlation_key)
    if evt.source not in _ISSUE_SOURCES:
        return from_key
    issue = evt.payload.get("issue")
    number = _issue_number(cast(dict[str, Any], issue).get("number")) if isinstance(issue, dict) else None
    repo_obj = evt.payload.get("repository")
    repo = cast(dict[str, Any], repo_obj).get("full_name") if isinstance(repo_obj, dict) else None
    if number is None:
        return ""
    if not isinstance(repo, str):
        return f"{from_key.split('#', 1)[0]}#{number}" if from_key else ""
    ref = f"{repo}#{number}"
    return ref if _ISSUE_REF.fullmatch(ref) else ""


def _about_pull_request(evt: Event) -> bool:
    # GitHub sends issues / issue_comment events for pull requests too (issue.pull_request is set), and Switchboard
    # marks relayed PR events in its metadata; all of them carry the PR's number in the github:<repo>#<n> key.
    if evt.source.startswith("github:pull_request"):
        return True
    issue = evt.payload.get("issue")
    if isinstance(issue, dict) and "pull_request" in issue:
        return True
    meta = evt.payload.get("metadata")
    if isinstance(meta, dict):
        info = cast(dict[str, Any], meta)
        event = info.get("github_event")
        return info.get("work_kind") == "pull_request" or (isinstance(event, str)
                                                            and event.startswith("pull_request"))
    return False


def source_issue(events: Sequence[Event], correlation_key: str | None = None) -> str:
    # The GitHub Issue a task came from, as "owner/repo#n", or "" when the task did not come from one. A task
    # started by events about a pull request names no issue, and its key is not read as one.
    if any(_about_pull_request(e) for e in events):
        return ""
    for evt in events:
        ref = _event_issue(evt)
        if ref:
            return ref
    return _key_issue(correlation_key)


@dataclass(frozen=True, slots=True)
class DeliveryRequest:
    work_key: str
    workdir: Path
    title: str
    summary: str
    test_evidence: str
    change_kind: str
    rationale: str
    # pass | off | pending, as the SDE delivered it
    review: str
    review_detail: str = ""
    # "owner/repo#n" of the GitHub Issue the task came from (source_issue); "" for any other source.
    issue: str = ""


def _strs() -> list[str]:
    return []


def _checks() -> list[dict[str, Any]]:
    return []


def _detail() -> dict[str, Any]:
    return {}


@dataclass(slots=True)
class DeliveryRecord:
    state: DeliveryState = "local"
    branch: str = ""
    change_kind: str = ""
    bump: str = ""
    base_sha: str = ""
    base_version: str = ""
    version: str = ""
    head_sha: str = ""
    checked_sha: str = ""
    checks: list[dict[str, Any]] = field(default_factory=_checks)
    pushed_sha: str = ""
    # The head GitHub reports for the PR, confirmed equal to pushed_sha after the PR step.
    verified_sha: str = ""
    pr_number: int = 0
    pr_url: str = ""
    ci: str = ""
    # When the wait for CI on ci_head began (epoch seconds), so a wait resumed after a restart keeps its deadline.
    ci_head: str = ""
    ci_since: float = 0.0
    merge_sha: str = ""
    attempts: int = 0
    # Per attempt: final outcomes that a retry cannot change (not authorized, review not passed, CI failed).
    notes: list[str] = field(default_factory=_strs)
    # Recoverable: approving a retry runs the step again from the recorded state.
    problem: str = ""
    # The permission tool that deferred to a person in this attempt.
    approval: str = ""
    # Set with problem when the developer can fix it (a base merge conflict, failing checks): what went wrong.
    handback: dict[str, Any] = field(default_factory=_detail)
    history: list[str] = field(default_factory=_strs)
    # Where the developer conversation can be resumed, recorded before its terminal closes.
    session_id: str = ""
    native_session_id: str = ""
    terminal: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}

    @staticmethod
    def from_dict(raw: Mapping[str, Any] | None) -> DeliveryRecord:
        if not raw:
            return DeliveryRecord()
        known = {f.name for f in dataclasses.fields(DeliveryRecord)}
        try:
            return DeliveryRecord(**{k: v for k, v in raw.items() if k in known})
        except TypeError:
            return DeliveryRecord()

    def version_line(self) -> str:
        source = f" for {self.change_kind}" if self.change_kind else ""
        if not self.bump:
            return "not decided"
        if not self.version:
            return f"{self.bump}{source} (not applied: no version_file configured)"
        if self.version == self.base_version:
            return f"unchanged {self.version} ({self.bump}{source})"
        return f"{self.base_version} -> {self.version} ({self.bump}{source})"

    def render(self) -> list[str]:
        label = {"local": "local commits only (no PR)", "pr_open": "PR created", "merged": "merged"}[self.state]
        lines = [f"Delivery: {self.state} - {label}"]
        if self.pr_url:
            lines.append(f"PR: {self.pr_url}")
        lines.append(f"Version: {self.version_line()}")
        if self.base_sha:
            lines.append(f"Base: {self.base_sha[:12]}")
        if self.checks:
            lines.append("Checks: " + "; ".join(
                f"`{c['command']}` " + ("passed" if c["exit_code"] == 0 else f"failed (exit {c['exit_code']})")
                for c in self.checks
            ))
        if self.ci:
            lines.append(f"CI: {self.ci}")
        if self.merge_sha:
            lines.append(f"Merge commit: {self.merge_sha}")
        lines += [f"Note: {n}" for n in self.notes]
        lines += [f"History: {h}" for h in self.history[-5:]]
        if self.problem:
            lines.append(f"Problem: {self.problem}")
        return lines


class DeliveryInterrupted(Exception):
    pass


class DeliveryDetached(Exception):
    # The process is stopping and the delivery stopped at a checkpoint; the record holds everything a later
    # delivery of the same task needs to continue from there.
    pass


class _Problem(Exception):
    pass


class _Handback(_Problem):
    def __init__(self, message: str, detail: dict[str, Any]) -> None:
        super().__init__(message)
        self.detail = detail


_LOCKS: dict[str, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def remote_env(base: Mapping[str, str]) -> dict[str, str]:
    env = {k: v for k, v in base.items() if k not in TOKEN_ENV}
    env.update({"GIT_TERMINAL_PROMPT": "0", "GH_PROMPT_DISABLED": "1", "GH_NO_UPDATE_NOTIFIER": "1"})
    return env


def _tail(text: str, limit: int = _TAIL) -> str:
    text = text.strip()
    return text if len(text) <= limit else "..." + text[-limit:]


def _never() -> bool:
    return False


def merged_head(config: DeliveryConfig | None, store: SQLiteDeliveryStore, work_key: str) -> str | None:
    # The head this delivery saw GitHub merge: the evidence that lets a worktree reclaim delete the task's local
    # branch. None when branches are kept or nothing was merged.
    if config is None or not config.delete_branch:
        return None
    rec = DeliveryRecord.from_dict(store.get(work_key))
    return rec.pushed_sha if rec.state == "merged" and rec.pushed_sha else None


class PRDelivery:
    # Hibernaut's delivery step for a finished SDE task: merge the latest remote base into the task branch, set the
    # SemVer version from the reported change kind, run the configured checks, push without force, create or reuse
    # the PR and, only when separately authorized, merge it after review and CI. Every step is idempotent, so a
    # retry resumes from what is already true locally and remotely.
    def __init__(
        self,
        config: DeliveryConfig,
        store: SQLiteDeliveryStore,
        *,
        env: Mapping[str, str] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._config = config
        self._store = store
        self._env = remote_env(os.environ if env is None else env)
        self._sleep = sleep
        # Wall clock: a CI deadline outlives the process that started the wait.
        self._clock = clock
        self._pattern = re.compile(config.version_pattern)

    @property
    def config(self) -> DeliveryConfig:
        return self._config

    def record(self, work_key: str) -> DeliveryRecord:
        return DeliveryRecord.from_dict(self._store.get(work_key))

    def record_session(self, work_key: str, *, session_id: str, native_session_id: str, terminal: str) -> None:
        rec = self.record(work_key)
        rec.session_id, rec.native_session_id, rec.terminal = session_id, native_session_id, terminal
        self._store.save(work_key, rec.to_dict())

    def unverified(self, rec: DeliveryRecord) -> list[str]:
        # What is still missing before the change counts as delivered to this project's target: an open PR whose
        # head GitHub reports at the pushed and checked commit, or the merge itself where merging is authorized.
        cfg = self._config
        if not cfg.open_pr:
            return ["this project delivers local commits, so there is no PR to confirm"]
        gaps: list[str] = []
        if rec.problem:
            gaps.append(f"delivery problem: {rec.problem}")
        if rec.state not in ("pr_open", "merged") or not rec.pr_url:
            gaps.append("no PR is recorded")
        elif cfg.auto_merge and rec.state != "merged":
            gaps.append("merging is authorized but the PR is not merged")
        if not rec.pushed_sha or rec.verified_sha != rec.pushed_sha:
            gaps.append("GitHub has not confirmed the PR head at the pushed commit")
        if cfg.checks and (rec.checked_sha != rec.pushed_sha or not rec.checks
                           or any(c.get("exit_code") != 0 for c in rec.checks)):
            gaps.append("the required checks have not passed on the pushed commit")
        return gaps

    def instructions(self) -> str:
        cfg = self._config
        version = (
            f" and sets the version in {cfg.version_file} from your change_kind; do not edit the version yourself"
            if cfg.version_file else ""
        )
        if not cfg.open_pr:
            return ("## Delivery\nThis project delivers local commits: Hibernaut does not push the task branch or open "
                    "a pull request. Report change_kind and change_rationale for the whole change.")
        merge = ("merges it after the independent review and CI pass" if cfg.auto_merge
                 else "leaves merging to a person")
        return (
            f"## Delivery\nAfter you report done (and the review passes), Hibernaut merges the latest "
            f"{cfg.remote}/{cfg.base_branch} into this branch{version}, runs the repository checks, pushes the branch, "
            f"opens the pull request and {merge}. Commit your work; do not push, open a pull request or merge."
        )

    def deliver(
        self,
        req: DeliveryRequest,
        *,
        permissions: PermissionPort,
        approved: frozenset[str] = frozenset(),
        is_interrupted: Callable[[], bool] = _never,
        should_detach: Callable[[], bool] = _never,
    ) -> DeliveryRecord:
        # Raises DeliveryDetached at a checkpoint once should_detach() is true; every step before it is recorded.
        cfg = self._config
        rec = self.record(req.work_key)

        def stop() -> bool:
            if should_detach():
                raise DeliveryDetached("delivery stopped at a checkpoint: the process is stopping")
            return is_interrupted()

        rec.notes, rec.problem, rec.approval, rec.handback = [], "", "", {}
        rec.attempts += 1
        if rec.state == "merged":
            rec.notes.append("already merged; nothing to do")
            self._store.save(req.work_key, rec.to_dict())
            return rec
        try:
            self._publish(req, rec, permissions, approved, stop)
            if rec.state == "merged":
                self._delete_remote_branch(req, rec)
        except _Problem as exc:
            rec.problem = str(exc)
            if isinstance(exc, _Handback):
                rec.handback = exc.detail
        finally:
            self._store.save(req.work_key, rec.to_dict())
        return rec

    def _publish(
        self, req: DeliveryRequest, rec: DeliveryRecord, permissions: PermissionPort, approved: frozenset[str],
        is_interrupted: Callable[[], bool],
    ) -> None:
        cfg = self._config
        if req.change_kind not in BUMP_FOR_KIND:
            raise _Problem(f"unknown change kind {req.change_kind!r} (expected {', '.join(CHANGE_KINDS)})")
        rec.change_kind, rec.bump = req.change_kind, BUMP_FOR_KIND[req.change_kind]
        if not cfg.open_pr:
            rec.notes.append("not pushed: this project does not authorize opening pull requests "
                             "(delivery.open_pr = false)")
            return
        if not self._gate(OPEN_PR_TOOL, req, rec, permissions, approved):
            return
        if rec.pushed_sha and self._merged_elsewhere(req, rec):
            return
        self._prepare(req, rec, is_interrupted)
        self._push(req, rec)
        self._open_pr(req, rec)
        self._confirm(req, rec)
        if rec.state != "pr_open":
            return
        if not cfg.auto_merge:
            rec.notes.append("not merged: this project does not authorize merging (delivery.auto_merge = false)")
            return
        self._merge(req, rec, permissions, approved, is_interrupted)

    # --- permission ---

    def _gate(
        self, tool: str, req: DeliveryRequest, rec: DeliveryRecord, permissions: PermissionPort,
        approved: frozenset[str],
    ) -> bool:
        if tool in approved:
            return True
        cfg = self._config
        decision = permissions.check(tool, {
            "repo": cfg.repo, "remote": cfg.remote, "base": cfg.base_branch, "work_key": req.work_key,
            "workdir": str(req.workdir), "branch": rec.branch, "pr": rec.pr_url, "review": req.review,
        })
        if decision.action == "allow":
            return True
        if decision.action == "defer":
            rec.approval = tool
            rec.problem = f"{tool} needs approval: {decision.reason}"
            return False
        rec.notes.append(f"{tool} blocked: {decision.reason}")
        return False

    # --- commands ---

    def _run(self, argv: list[str], cwd: Path, *, timeout: float = 300.0) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(argv, cwd=cwd, env=self._env, capture_output=True, text=True, timeout=timeout)
        except FileNotFoundError:
            raise _Problem(f"{argv[0]} is not installed or not on PATH") from None
        except subprocess.TimeoutExpired:
            raise _Problem(f"`{shlex.join(argv[:3])} ...` timed out after {timeout:g}s") from None

    def _git(self, cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        proc = self._run([self._config.git_command, *args], cwd)
        if check and proc.returncode != 0:
            raise _Problem(f"git {args[0]} failed: {_tail(proc.stderr or proc.stdout, 800)}")
        return proc

    def _gh(self, cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        repo = ("-R", self._config.repo) if self._config.repo else ()
        proc = self._run([self._config.gh_command, *args, *repo], cwd)
        if check and proc.returncode != 0:
            raise _Problem(f"gh {' '.join(args[:2])} failed: {_tail(proc.stderr or proc.stdout, 800)}")
        return proc

    @contextmanager
    def _repo_lock(self, cwd: Path) -> Generator[None]:
        # Fetching, merging the base and merging the PR are serialized per repository in this process, so two task
        # worktrees never update the shared remote-tracking ref or read the base version at the same time.
        common = self._git(cwd, "rev-parse", "--git-common-dir").stdout.strip()
        key = str((cwd / common).resolve())
        with _LOCKS_GUARD:
            lock = _LOCKS.setdefault(key, threading.Lock())
        with lock:
            yield

    @staticmethod
    def _checkpoint(is_interrupted: Callable[[], bool]) -> None:
        if is_interrupted():
            raise DeliveryInterrupted("delivery interrupted by a new event")

    # --- prepare: base, version, checks ---

    def _base_ref(self) -> str:
        return f"refs/remotes/{self._config.remote}/{self._config.base_branch}"

    def _fetch_base(self, cwd: Path) -> str:
        cfg = self._config
        self._git(cwd, "fetch", "--quiet", cfg.remote, f"+refs/heads/{cfg.base_branch}:{self._base_ref()}")
        return self._git(cwd, "rev-parse", "--verify", f"{self._base_ref()}^{{commit}}").stdout.strip()

    def _prepare(self, req: DeliveryRequest, rec: DeliveryRecord, is_interrupted: Callable[[], bool]) -> None:
        cfg, cwd = self._config, req.workdir
        self._checkpoint(is_interrupted)
        dirty = self._git(cwd, "status", "--porcelain", "--untracked-files=all").stdout
        if dirty.strip():
            raise _Problem("the worktree has uncommitted changes; commit or remove them, then approve to retry:\n"
                           + _tail(dirty, 1500))
        head_ref = self._git(cwd, "symbolic-ref", "--quiet", "--short", "HEAD", check=False)
        branch = head_ref.stdout.strip()
        if head_ref.returncode != 0 or not branch:
            raise _Problem("the worktree is on a detached HEAD; check out the task branch, then approve to retry")
        if branch == cfg.base_branch:
            raise _Problem(f"the worktree is on {cfg.base_branch}; delivery needs a task branch")
        rec.branch = branch
        with self._repo_lock(cwd):
            base_sha = self._fetch_base(cwd)
            merged = self._git(cwd, "merge-base", "--is-ancestor", base_sha, "HEAD", check=False).returncode == 0
            if not merged:
                self._integrate(cwd, branch, base_sha)
            self._apply_version(req, rec, base_sha)
            rec.base_sha = base_sha
        rec.head_sha = self._git(cwd, "rev-parse", "HEAD").stdout.strip()
        self._run_checks(rec, cwd, is_interrupted, f"{cfg.remote}/{cfg.base_branch}")

    def _integrate(self, cwd: Path, branch: str, base_sha: str) -> None:
        cfg = self._config
        ref = self._base_ref()
        label = f"{cfg.remote}/{cfg.base_branch}"
        proc = self._git(cwd, "merge", "--no-ff", "--no-edit", "-m", f"Merge {label} into {branch}", ref, check=False)
        if proc.returncode == 0:
            return
        listed = self._git(cwd, "diff", "--name-only", "--diff-filter=U", "-z", check=False).stdout
        conflicted = [p for p in listed.split("\0") if p]
        if conflicted == [cfg.version_file] and self._resolve_version_conflict(cwd):
            return
        # HEAD is still the task branch's tip while the merge is in progress.
        commits = self._git(cwd, "log", "--oneline", "--no-merges", "-n", "20", f"HEAD..{base_sha}", "--",
                            *conflicted, check=False).stdout.strip() if conflicted else ""
        self._git(cwd, "merge", "--abort", check=False)
        raise _Handback(
            f"merging {label} into {branch} conflicts in {', '.join(conflicted) or '(unknown files)'}; "
            "resolve it on the task branch, then approve to retry",
            {"kind": "conflict", "base": label, "base_sha": base_sha, "branch": branch, "files": conflicted,
             "commits": commits},
        )

    def _resolve_version_conflict(self, cwd: Path) -> bool:
        # Only the version line conflicts (another PR set it first): keep the base's side of the conflicting hunks
        # and every other edit; the bump below is recomputed from the base's version.
        vf = self._config.version_file
        stages: list[str] = []
        with tempfile.TemporaryDirectory() as tmp:
            for n in (2, 1, 3):
                shown = self._git(cwd, "show", f":{n}:{vf}", check=False)
                if shown.returncode != 0:
                    return False
                path = Path(tmp) / f"stage{n}"
                path.write_text(shown.stdout, encoding="utf-8")
                stages.append(str(path))
            merged = self._git(cwd, "merge-file", "-p", "--theirs", *stages, check=False)
            if merged.returncode != 0:
                return False
        (cwd / vf).write_text(merged.stdout, encoding="utf-8")
        self._git(cwd, "add", "--", vf)
        self._git(cwd, "commit", "--no-edit", "--quiet")
        return True

    def _read_version(self, text: str, where: str) -> tuple[str, re.Match[str]]:
        m = self._pattern.search(text)
        if m is None:
            raise _Problem(f"no version matching version_pattern in {where}")
        version = m.group("version")
        if _SEMVER.fullmatch(version) is None:
            raise _Problem(f"version {version!r} in {where} is not MAJOR.MINOR.PATCH")
        return version, m

    def _version_at(self, cwd: Path, sha: str) -> str:
        cfg = self._config
        shown = self._git(cwd, "show", f"{sha}:{cfg.version_file}", check=False)
        if shown.returncode != 0:
            raise _Problem(f"{cfg.version_file} is missing on {cfg.remote}/{cfg.base_branch} ({sha[:12]})")
        return self._read_version(shown.stdout, f"{cfg.version_file} on {cfg.remote}/{cfg.base_branch}")[0]

    def _apply_version(self, req: DeliveryRequest, rec: DeliveryRecord, base_sha: str) -> None:
        cfg, cwd = self._config, req.workdir
        if not cfg.version_file:
            rec.base_version = rec.version = ""
            return
        bump = BUMP_FOR_KIND[req.change_kind]
        base_version = self._version_at(cwd, base_sha)
        target = bump_version(base_version, bump)
        path = cwd / cfg.version_file
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise _Problem(f"cannot read {cfg.version_file} in the worktree ({exc.strerror})") from None
        current, m = self._read_version(text, f"{cfg.version_file} on {rec.branch}")
        if current != target:
            path.write_text(text[: m.start("version")] + target + text[m.end("version"):], encoding="utf-8")
            self._git(cwd, "add", "--", cfg.version_file)
            self._git(cwd, "commit", "--quiet", "-m",
                      f"Set version {target} ({bump} for {req.change_kind}, from {base_version})")
        if rec.version and rec.version != target:
            rec.history.append(f"version recomputed from {base_version} on {cfg.remote}/{cfg.base_branch}: "
                               f"{rec.version} -> {target}")
        rec.base_version, rec.version = base_version, target

    def _run_checks(self, rec: DeliveryRecord, cwd: Path, is_interrupted: Callable[[], bool], label: str) -> None:
        cfg = self._config
        if rec.checked_sha == rec.head_sha and rec.checks and all(c["exit_code"] == 0 for c in rec.checks):
            return
        results: list[dict[str, Any]] = []
        for argv in cfg.checks:
            self._checkpoint(is_interrupted)
            try:
                proc = subprocess.run(list(argv), cwd=cwd, env=self._env, capture_output=True, text=True,
                                      timeout=cfg.check_timeout_seconds)
                code, output = proc.returncode, proc.stdout + proc.stderr
            except FileNotFoundError:
                code, output = 127, f"{argv[0]}: command not found"
            except subprocess.TimeoutExpired:
                code, output = 124, f"timed out after {cfg.check_timeout_seconds:g}s"
            results.append({"command": shlex.join(argv), "exit_code": code, "output": _tail(output)})
        rec.checks, rec.checked_sha = results, rec.head_sha
        failed = [r for r in results if r["exit_code"] != 0]
        if failed:
            detail = "\n".join(f"$ {r['command']} (exit {r['exit_code']})\n{_tail(r['output'], 1500)}" for r in failed)
            message = f"repository checks failed on {rec.head_sha[:12]}; nothing was pushed:\n{detail}"
            if all(r["exit_code"] == 127 for r in failed):
                # A missing check command is the host's to fix, not the developer's.
                raise _Problem(message)
            raise _Handback(message, {"kind": "checks", "base": label, "base_sha": rec.base_sha,
                                      "branch": rec.branch, "head": rec.head_sha, "failed": detail})

    # --- push and PR ---

    def _push(self, req: DeliveryRequest, rec: DeliveryRecord) -> None:
        cfg, cwd = self._config, req.workdir
        listed = self._git(cwd, "ls-remote", cfg.remote, f"refs/heads/{rec.branch}").stdout.split()
        if (listed[0] if listed else "") != rec.head_sha:
            # Never forced: a branch that gained commits elsewhere is reported instead of overwritten.
            proc = self._git(cwd, "push", "--quiet", cfg.remote, f"HEAD:refs/heads/{rec.branch}", check=False)
            if proc.returncode != 0:
                raise _Problem(f"push of {rec.branch} to {cfg.remote} was rejected: {_tail(proc.stderr, 800)}")
        rec.pushed_sha = rec.head_sha
        self._store.save(req.work_key, rec.to_dict())

    def _title(self, req: DeliveryRequest, rec: DeliveryRecord) -> str:
        title = " ".join(req.title.split())[:120] or f"Task {req.work_key}"
        if rec.version and rec.version != rec.base_version:
            return self._config.title_template.format(title=title, version=rec.version)
        return title

    def _issue_line(self, issue: str) -> str:
        # GitHub closes the issue on merge for "Closes", and only links it for "Refs".
        cfg = self._config
        m = _ISSUE_REF.fullmatch(issue)
        if cfg.issue_link == "none" or m is None:
            return ""
        same_repo = bool(cfg.repo) and m.group("repo").lower() == cfg.repo.lower()
        ref = f"#{m.group('number')}" if same_repo else issue
        return f"{'Closes' if cfg.issue_link == 'closes' else 'Refs'} {ref}"

    def _body(self, req: DeliveryRequest, rec: DeliveryRecord) -> str:
        cfg = self._config
        checks = "\n".join(
            f"- `{c['command']}`: " + ("passed" if c["exit_code"] == 0 else f"failed (exit {c['exit_code']})")
            for c in rec.checks
        ) or "- (no checks configured)"
        version = rec.version_line()
        if rec.version:
            version += (f"\n\nComputed from `{cfg.version_file}` on {cfg.remote}/{cfg.base_branch} at "
                        f"{rec.base_sha[:12]}. If another pull request changes the version first, Hibernaut "
                        "recomputes it before merging.")
        review = f"review={req.review}" + (f"\n\n{req.review_detail}" if req.review_detail else "")
        issue = self._issue_line(req.issue)
        return "\n\n".join([
            *([issue] if issue else []),
            f"## Summary\n{req.summary or '(none)'}",
            f"## Change\nKind: {req.change_kind}\n\nRationale: {req.rationale or '(none)'}",
            f"## Version\n{version}",
            f"## Checks (run by Hibernaut on {rec.head_sha[:12]})\n{checks}",
            f"## Review\n{review}",
            f"## Test evidence (from the developer)\n```text\n{_tail(req.test_evidence, 20000)}\n```",
            f"Delivered by Hibernaut for task `{req.work_key}`.",
        ])

    def _list_prs(self, cwd: Path, branch: str) -> list[dict[str, Any]]:
        proc = self._gh(cwd, "pr", "list", "--head", branch, "--base", self._config.base_branch, "--state", "all",
                        "--json", "number,url,state,headRefOid,mergeCommit", "--limit", "50")
        try:
            data: object = json.loads(proc.stdout or "[]")
        except json.JSONDecodeError:
            raise _Problem(f"gh pr list returned no JSON: {_tail(proc.stdout, 300)}") from None
        if not isinstance(data, list):
            raise _Problem("gh pr list returned no list")
        return [cast(dict[str, Any], p) for p in cast(list[object], data) if isinstance(p, dict)]

    @staticmethod
    def _merge_oid(pr: Mapping[str, Any]) -> str:
        commit = pr.get("mergeCommit")
        oid = cast(dict[str, Any], commit).get("oid") if isinstance(commit, dict) else None
        return oid if isinstance(oid, str) else ""

    def _adopt_merged(self, rec: DeliveryRecord, prs: list[dict[str, Any]], head: str) -> bool:
        for pr in prs:
            if pr.get("state") == "MERGED" and pr.get("headRefOid") == head:
                rec.state, rec.pr_number, rec.pr_url = "merged", int(pr["number"]), str(pr["url"])
                rec.merge_sha = self._merge_oid(pr)
                rec.history.append(f"PR #{rec.pr_number} was merged outside this delivery")
                return True
        return False

    def _merged_elsewhere(self, req: DeliveryRequest, rec: DeliveryRecord) -> bool:
        # Checked before preparing: once the pushed head is merged, merging the base back in would bump again.
        return self._adopt_merged(rec, self._list_prs(req.workdir, rec.branch), rec.pushed_sha)

    def _open_pr(self, req: DeliveryRequest, rec: DeliveryRecord) -> None:
        cwd = req.workdir
        prs = self._list_prs(cwd, rec.branch)
        if self._adopt_merged(rec, prs, rec.head_sha):
            return
        title, body = self._title(req, rec), self._body(req, rec)
        existing = next((p for p in prs if p.get("state") == "OPEN"), None)
        if existing is not None:
            rec.pr_number, rec.pr_url = int(existing["number"]), str(existing["url"])
            rec.state = "pr_open"
            self._store.save(req.work_key, rec.to_dict())
            self._gh(cwd, "pr", "edit", str(rec.pr_number), "--title", title, "--body", body)
            return
        proc = self._gh(cwd, "pr", "create", "--base", self._config.base_branch, "--head", rec.branch,
                        "--title", title, "--body", body)
        url = next((line.strip() for line in reversed(proc.stdout.splitlines()) if line.strip()), "")
        number = url.rstrip("/").rsplit("/", 1)[-1]
        if not number.isdigit():
            raise _Problem(f"gh pr create did not return a PR URL: {_tail(proc.stdout, 300)}")
        rec.pr_number, rec.pr_url, rec.state = int(number), url, "pr_open"
        rec.history.append(f"PR #{rec.pr_number} created")

    def _confirm(self, req: DeliveryRequest, rec: DeliveryRecord) -> None:
        # A gh result alone is not evidence: GitHub must report the pushed commit as the PR's head.
        if rec.state == "merged" and rec.verified_sha == rec.pushed_sha:
            return
        listed = self._git(req.workdir, "ls-remote", self._config.remote, f"refs/heads/{rec.branch}").stdout.split()
        remote = listed[0] if listed else ""
        view = self._gh(req.workdir, "pr", "view", str(rec.pr_number), "--json", "headRefOid,state,url")
        try:
            data = cast(dict[str, Any], json.loads(view.stdout))
        except (json.JSONDecodeError, TypeError):
            raise _Problem(f"gh pr view returned no JSON: {_tail(view.stdout, 300)}") from None
        head = str(data.get("headRefOid") or "")
        if rec.state == "merged":
            head = head or rec.pushed_sha
        elif remote != rec.pushed_sha or head != rec.pushed_sha:
            rec.verified_sha = ""
            raise _Problem(
                f"PR #{rec.pr_number} is not at the pushed commit: expected {rec.pushed_sha[:12]}, "
                f"{self._config.remote}/{rec.branch} is {remote[:12] or '(missing)'}, the PR head is {head[:12] or '(unknown)'}"
            )
        rec.verified_sha = head

    # --- merge ---

    def _merge(
        self, req: DeliveryRequest, rec: DeliveryRecord, permissions: PermissionPort, approved: frozenset[str],
        is_interrupted: Callable[[], bool],
    ) -> None:
        cfg, cwd = self._config, req.workdir
        if cfg.require_review and req.review != "pass":
            rec.notes.append(f"not merged: merging needs a passing independent review (review={req.review})")
            return
        if not self._gate(MERGE_TOOL, req, rec, permissions, approved):
            return
        for _ in range(_MAX_MAIN_MOVES):
            ci = self._wait_for_ci(req, rec, cwd, is_interrupted)
            if ci == "failed":
                return
            with self._repo_lock(cwd):
                base_sha = self._fetch_base(cwd)
                moved = ci == "stale" or (
                    base_sha != rec.base_sha and bool(cfg.version_file)
                    and self._version_at(cwd, base_sha) != rec.base_version
                )
                if not moved:
                    method = f"--{cfg.merge_method}"
                    self._gh(cwd, "pr", "merge", str(rec.pr_number), method, "--match-head-commit", rec.head_sha)
                    view = self._gh(cwd, "pr", "view", str(rec.pr_number), "--json", "state,mergeCommit")
                    try:
                        data = cast(dict[str, Any], json.loads(view.stdout))
                    except (json.JSONDecodeError, TypeError):
                        raise _Problem(f"gh pr view returned no JSON: {_tail(view.stdout, 300)}") from None
                    if data.get("state") != "MERGED":
                        raise _Problem(f"PR #{rec.pr_number} is {data.get('state')} after merging")
                    rec.state, rec.merge_sha = "merged", self._merge_oid(data)
                    rec.verified_sha = rec.pushed_sha
                    rec.history.append(f"PR #{rec.pr_number} merged as {rec.merge_sha[:12]}")
                    return
            # The base's version changed after this PR computed its own (a concurrent PR merged first), or the PR
            # conflicts with the base and GitHub will not run CI for it.
            self._prepare(req, rec, is_interrupted)
            self._push(req, rec)
            self._open_pr(req, rec)
            self._confirm(req, rec)
            if rec.state == "merged":
                return
        raise _Problem(f"{cfg.remote}/{cfg.base_branch} kept changing its version; approve to retry the merge")

    def _delete_remote_branch(self, req: DeliveryRequest, rec: DeliveryRecord) -> None:
        # Runs only once GitHub reports the PR MERGED. A failure never undoes the merge: it is a note in the message.
        cfg = self._config
        if not cfg.delete_branch or not rec.branch:
            return
        label = f"{cfg.remote}/{rec.branch}"

        def remote_tip() -> str:
            listed = self._git(req.workdir, "ls-remote", cfg.remote, f"refs/heads/{rec.branch}").stdout.split()
            return listed[0] if listed else ""

        try:
            tip = remote_tip()
            if not tip:
                rec.history.append(f"{label} was already deleted")
                return
            if tip != rec.pushed_sha:
                # Commits pushed after the merged head would be lost with the branch.
                rec.notes.append(f"branch kept: {label} is at {tip[:12]}, not the merged head {rec.pushed_sha[:12]}")
                return
            proc = self._git(req.workdir, "push", "--quiet", cfg.remote, "--delete", rec.branch, check=False)
            if proc.returncode != 0:
                if not remote_tip():
                    rec.history.append(f"{label} was already deleted")
                    return
                rec.notes.append(f"branch kept: deleting {label} failed: {_tail(proc.stderr or proc.stdout, 800)}")
                return
            rec.history.append(f"deleted {label}")
        except _Problem as exc:
            rec.notes.append(f"branch kept: {exc}")

    def _pr_checks(self, cwd: Path, number: int) -> list[dict[str, Any]]:
        proc = self._gh(cwd, "pr", "checks", str(number), "--json", "name,state,bucket", check=False)
        try:
            data: object = json.loads(proc.stdout) if proc.stdout.strip() else None
        except json.JSONDecodeError:
            data = None
        if isinstance(data, list):
            return [cast(dict[str, Any], c) for c in cast(list[object], data) if isinstance(c, dict)]
        if "no checks reported" in proc.stderr:
            return []
        raise _Problem(f"gh pr checks failed: {_tail(proc.stderr or proc.stdout, 800)}")

    def _conflicting(self, cwd: Path, number: int) -> bool:
        view = self._gh(cwd, "pr", "view", str(number), "--json", "mergeable,mergeStateStatus", check=False)
        try:
            data: object = json.loads(view.stdout) if view.returncode == 0 else None
        except json.JSONDecodeError:
            data = None
        if not isinstance(data, dict):
            return False
        info = cast(dict[str, Any], data)
        return info.get("mergeStateStatus") == "DIRTY" or info.get("mergeable") == "CONFLICTING"

    def _behind(self, cwd: Path) -> bool:
        with self._repo_lock(cwd):
            base_sha = self._fetch_base(cwd)
        return self._git(cwd, "merge-base", "--is-ancestor", base_sha, "HEAD", check=False).returncode != 0

    def _wait_for_ci(
        self, req: DeliveryRequest, rec: DeliveryRecord, cwd: Path, is_interrupted: Callable[[], bool]
    ) -> CIResult:
        cfg = self._config
        if not cfg.require_ci:
            rec.ci = "not required"
            return "passed"
        label = f"{cfg.remote}/{cfg.base_branch}"
        if rec.ci_head != rec.head_sha or not rec.ci_since:
            # A new head starts a new wait; the same head keeps the wait (and deadline) it began before a restart.
            rec.ci_head, rec.ci_since = rec.head_sha, self._clock()
            self._store.save(req.work_key, rec.to_dict())
        start = rec.ci_since
        deadline = start + cfg.ci_timeout_seconds
        grace = min(_EMPTY_CHECKS_GRACE, cfg.ci_timeout_seconds / 2)
        while True:
            self._checkpoint(is_interrupted)
            checks = self._pr_checks(cwd, rec.pr_number)
            failed = [str(c.get("name")) for c in checks if c.get("bucket") in ("fail", "cancel")]
            if failed:
                rec.ci = f"failed on {rec.head_sha[:12]}: {', '.join(failed)}"
                rec.notes.append(f"not merged: CI failed ({', '.join(failed)})")
                return "failed"
            # GitHub runs no pull_request CI for a PR that conflicts with its base: waiting would only time out.
            if self._conflicting(cwd, rec.pr_number):
                rec.history.append(f"PR #{rec.pr_number} conflicts with {label} while waiting for CI; "
                                   f"merging {label} again")
                return "stale"
            pending = [str(c.get("name")) for c in checks if c.get("bucket") not in ("pass", "skipping")]
            if checks and not pending:
                rec.ci = f"passed on {rec.head_sha[:12]} ({len(checks)} check(s))"
                return "passed"
            if not checks and self._clock() - start >= grace and self._behind(cwd):
                rec.history.append(f"no CI reported for PR #{rec.pr_number} after {grace:g}s and {label} has moved; "
                                   f"merging {label} again")
                return "stale"
            remaining = deadline - self._clock()
            if remaining <= 0:
                waiting = f"pending: {', '.join(pending)}" if pending else "no checks reported"
                rec.ci = f"not finished on {rec.head_sha[:12]} ({waiting})"
                # An approved retry waits a full period again.
                rec.ci_since = 0.0
                raise _Problem(f"CI has not finished for PR #{rec.pr_number} after {cfg.ci_timeout_seconds:g}s "
                               f"({waiting}); approve to check again")
            # In slices of at most a second, each a checkpoint, so a stop does not wait out a whole poll interval.
            left = min(cfg.ci_poll_seconds, remaining)
            while left > 0:
                self._sleep(min(1.0, left))
                left -= 1.0
                if left > 0:
                    self._checkpoint(is_interrupted)
