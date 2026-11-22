from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_runtime.adapters.executor_cli.claude_code import (
    ClaudeCodeError,
    ClaudeCodeProfile,
    child_env,
    run_claude_code,
)
from agent_runtime.adapters.executor_cli.orca import OrcaVisibleRunner, terminal_title
from agent_runtime.adapters.executor_cli.process import join_text, last_text, target_events
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.models import Emit, Event, PermissionDecision, RunOutcome, SessionRef, Suspend, new_id
from agent_runtime.core.ports import RunCapabilities

CODER_REVIEWER_ROLE = "coder-reviewer"
_READ_ONLY_TOOLS = (
    "Read",
    "Grep",
    "Glob",
    "Bash(git diff:*)",
    "Bash(git log:*)",
    "Bash(git show:*)",
    "Bash(git status:*)",
    "Bash(git blame:*)",
)
_WRITE_TOOLS = ("Edit", "Write", "NotebookEdit", "MultiEdit")
_MAX_DIFF_CHARS = 200_000

REVIEW_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": ["pass", "changes_requested"]},
        "summary": {"type": "string"},
        "blocking": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "location": {"type": "string"},
                    "trigger": {"type": "string"},
                    "impact": {"type": "string"},
                    "fix": {"type": "string"},
                },
                "required": ["location", "trigger", "impact"],
            },
        },
        "notes": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["verdict", "summary", "blocking"],
}

_ROLE_ARGS = (
    "--no-session-persistence",
    "--allowedTools", ",".join(_READ_ONLY_TOOLS),
    "--disallowedTools", ",".join(_WRITE_TOOLS),
    "--json-schema", json.dumps(REVIEW_SCHEMA),
)
# Interactive: no file-writing tool at all; the result goes back through the launcher's handoff tool.
_VISIBLE_ROLE_ARGS = (
    "--allowedTools", ",".join(_READ_ONLY_TOOLS),
    "--disallowedTools", ",".join(_WRITE_TOOLS),
)

_REVIEW_RULES = """You are coder-reviewer, an independent code reviewer. Another agent wrote the change below; judge it, do not fix it.

Rules:
- Review only. Do not create, edit, stage, commit, stash or delete anything. Any change to the working tree voids this review.
- Judge the change against all three inputs: the task requirements, the code diff, and the test evidence. Read surrounding code when the diff alone is not enough.
- verdict "pass" only when no blocking issue remains; otherwise "changes_requested" with at least one blocking item.
- A blocking item is something that must change before merge: a bug, a missed requirement, a broken contract, a security problem, or a requirement the test evidence does not show to work.
- Every blocking item gives location (path:line), trigger (the inputs or state that make it go wrong), impact (what breaks for whom), and optionally fix.
- Style preferences and optional improvements go to notes, never to blocking.
- The developer fixes the blocking items and submits again for a fresh, independent review."""


class ReviewAborted(Exception):
    pass


@dataclass(frozen=True, slots=True)
class ReviewRequest:
    workdir: Path
    requirements: str
    test_evidence: str
    base_ref: str
    # Earlier findings plus the diff since that review; empty on a first review.
    previous_round: str = ""
    # What the review is for, shown in the visible terminal's title.
    label: str = ""


class CoderReviewerExecutor:
    def __init__(
        self,
        *,
        profile: ClaudeCodeProfile,
        workspaces: SQLiteWorkspaceStore,
        interrupt_poll_interval: float = 0.05,
        visible: OrcaVisibleRunner | None = None,
    ) -> None:
        self._profile = profile
        self._workspaces = workspaces
        self._interrupt_poll_interval = interrupt_poll_interval
        # Set in Orca worktree deployments: every review runs in its own visible terminal.
        self._visible = visible

    @property
    def executor_id(self) -> str:
        return self._profile.name

    @property
    def profile(self) -> ClaudeCodeProfile:
        return self._profile

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        session_id = session.session_id if session is not None else new_id("sess")
        try:
            request = self._review_input(events, session)
            decision = self.check_launch(caps, request.workdir)
            if decision.action == "block":
                return self._emit(session_id, decision.reason)
            if decision.action == "defer":
                return Suspend(
                    request_id=new_id("req"),
                    prompt=decision.reason,
                    session_checkpoint="{}",
                    session_id=session_id,
                )
            review = await self.review(request, caps)
            return Emit(messages=(self.format_review(review),), session_id=session_id,
                        executor_session_id=review.get("native_session_id"))
        except (ReviewAborted, ClaudeCodeError) as exc:
            return self._emit(session_id, str(exc))

    def check_launch(self, caps: RunCapabilities, workdir: Path) -> PermissionDecision:
        p = self._profile
        return caps.permissions.check(
            "coder_reviewer.launch",
            {
                "executor": p.name,
                "workdir": str(workdir),
                "harness": p.harness,
                "launcher": p.launcher.name,
                "billing": p.billing,
                "model": p.model,
                "effort": p.effort,
            },
        )

    async def review(self, request: ReviewRequest, caps: RunCapabilities) -> dict[str, Any]:
        # Callers run check_launch() first; raises ReviewAborted or ClaudeCodeError when no verdict is reached.
        p = self._profile
        env = child_env(p, os.environ)
        evidence = await asyncio.to_thread(_collect_diff, request.workdir, request.base_ref)
        before = await asyncio.to_thread(worktree_fingerprint, request.workdir)
        extra: dict[str, str] = {}
        if self._visible is not None:
            # A fresh conversation every time: a re-review gets the earlier findings in its prompt, not its context.
            reply = await self._visible.converse(
                p,
                title=terminal_title(p.name, CODER_REVIEWER_ROLE, request.label or request.workdir.name),
                workdir=request.workdir,
                prompt=self._prompt(request, evidence),
                schema=REVIEW_SCHEMA,
                role_args=_VISIBLE_ROLE_ARGS,
                is_interrupted=caps.is_interrupted,
            )
            raw: object = reply.data
            extra = {"terminal": reply.handle, "native_session_id": reply.native_session_id}
        else:
            result = await run_claude_code(
                p,
                cwd=request.workdir,
                prompt=self._prompt(request, evidence),
                permission_mode="dontAsk",
                role_args=_ROLE_ARGS,
                env=env,
                is_interrupted=caps.is_interrupted,
                poll_interval=self._interrupt_poll_interval,
            )
            raw = result.get("structured_output")
        after = await asyncio.to_thread(worktree_fingerprint, request.workdir)
        if after != before:
            raise ReviewAborted(
                f"the working tree in {request.workdir} changed during the review; findings discarded, "
                "check the change and submit again"
            )
        return {**_validate_review(raw), **extra}

    def _emit(self, session_id: str, detail: str) -> Emit:
        return Emit(messages=(f"[{self._profile.name}] review failed: {detail}",), session_id=session_id)

    def _review_input(self, events: Sequence[Event], session: SessionRef | None) -> ReviewRequest:
        target = target_events(events)
        requirements = join_text(target, ("requirements", "text"))
        if not requirements:
            raise ReviewAborted("payload needs the task requirements in 'requirements' or 'text'")
        explicit = last_text(target, "explicit_workdir")
        if explicit is not None:
            workdir = Path(explicit).resolve()
        else:
            # A review must never create a worktree; it reads the one the developer already used.
            work_key = (
                last_text(target, "review_of")
                or target[0].correlation_key
                or (session.correlation_key if session is not None else None)
            )
            record = self._workspaces.get(work_key) if work_key else None
            if record is None or record.status != "ready":
                raise ReviewAborted(
                    f"no ready workspace for '{work_key}'; pass 'explicit_workdir' or 'review_of' "
                    "with the developer's correlation_key"
                )
            workdir = Path(record.workdir)
        if not workdir.is_dir():
            raise ReviewAborted(f"workdir {workdir} does not exist")
        subject = last_text(target, "review_of") or target[0].correlation_key or workdir.name
        return ReviewRequest(
            workdir=workdir,
            requirements=requirements,
            test_evidence=join_text(target, ("test_evidence",)) or "(none provided)",
            base_ref=last_text(target, "base_ref") or "",
            label=f"review of {subject}",
        )

    def _prompt(self, request: ReviewRequest, evidence: str) -> str:
        parts = [
            _REVIEW_RULES,
            f"## Task requirements\n{request.requirements}",
            f"## Test evidence\n{request.test_evidence}",
            f"## Code diff\n{evidence}",
        ]
        if request.previous_round:
            parts.append(request.previous_round)
        return "\n\n".join(parts)

    def format_review(self, review: Mapping[str, Any]) -> str:
        p = self._profile
        lines = [
            f"[{p.name}] verdict={review['verdict']} "
            f"(harness={p.harness} launcher={p.launcher.name} billing={p.billing} model={p.model} effort={p.effort})",
            f"Summary: {review['summary']}",
        ]
        if review.get("terminal"):
            lines.insert(1, f"Orca terminal: {review['terminal']} (Claude Code session {review.get('native_session_id')})")
        blocking: list[dict[str, str]] = review["blocking"]
        if blocking:
            lines.append("Blocking:")
            lines.append(format_blocking(blocking))
            lines.append(f"After fixing, submit again with executor_id={p.name} for a fresh review.")
        notes: list[str] = review.get("notes", [])
        if notes:
            lines.append("Notes:")
            lines.extend(f"- {note}" for note in notes)
        return "\n".join(lines)


def format_blocking(blocking: Sequence[Mapping[str, str]]) -> str:
    lines: list[str] = []
    for i, item in enumerate(blocking, 1):
        lines.append(f"{i}. {item['location']}")
        lines.append(f"   Trigger: {item['trigger']}")
        lines.append(f"   Impact: {item['impact']}")
        if item.get("fix"):
            lines.append(f"   Fix: {item['fix']}")
    return "\n".join(lines)


def _validate_review(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ReviewAborted("the reviewer returned no structured verdict")
    data: dict[str, Any] = dict(raw)
    verdict = data.get("verdict")
    summary = data.get("summary")
    blocking_raw = data.get("blocking")
    notes_raw = data.get("notes", [])
    if verdict not in ("pass", "changes_requested") or not isinstance(summary, str) or not isinstance(blocking_raw, list):
        raise ReviewAborted("the reviewer returned a malformed verdict")
    blocking: list[dict[str, str]] = []
    for item in blocking_raw:
        if not isinstance(item, dict):
            raise ReviewAborted("a blocking item is not an object")
        entry = {str(k): str(v).strip() for k, v in item.items() if isinstance(v, str)}
        if not all(entry.get(k) for k in ("location", "trigger", "impact")):
            raise ReviewAborted("a blocking item lacks location, trigger or impact")
        blocking.append(entry)
    if (verdict == "pass") == bool(blocking):
        raise ReviewAborted(f"verdict '{verdict}' contradicts {len(blocking)} blocking item(s)")
    notes = [str(n) for n in notes_raw] if isinstance(notes_raw, list) else []
    return {"verdict": verdict, "summary": summary.strip(), "blocking": blocking, "notes": notes}


def _git(workdir: Path, *args: str, env: Mapping[str, str] | None = None) -> str:
    proc = subprocess.run(
        ["git", *args], cwd=str(workdir), capture_output=True, text=True, check=False,
        env=dict(env) if env is not None else None,
    )
    if proc.returncode != 0:
        raise ReviewAborted(f"git {' '.join(args[:2])} failed in {workdir}: {proc.stderr.strip()[:500]}")
    return proc.stdout


def worktree_fingerprint(workdir: Path) -> str:
    head = _git(workdir, "rev-parse", "HEAD").strip()
    index = Path(_git(workdir, "rev-parse", "--path-format=absolute", "--git-path", "index").strip())
    # Snapshot tracked and untracked (non-ignored) files through a scratch index; the real index stays untouched.
    with tempfile.TemporaryDirectory(prefix="hibernaut-review-") as tmp:
        scratch = Path(tmp) / "index"
        if index.is_file():
            shutil.copyfile(index, scratch)
        env = {**os.environ, "GIT_INDEX_FILE": str(scratch)}
        _git(workdir, "add", "-A", env=env)
        tree = _git(workdir, "write-tree", env=env).strip()
    return f"{head}:{tree}"


def snapshot_diff(workdir: Path, before: str, after: str) -> str:
    # Fingerprints are "<head>:<tree>"; their tree objects stay in the object store, so two snapshots diff directly.
    diff = _git(workdir, "diff", before.split(":", 1)[1], after.split(":", 1)[1])
    if len(diff) > _MAX_DIFF_CHARS:
        diff = diff[:_MAX_DIFF_CHARS] + f"\n[diff truncated at {_MAX_DIFF_CHARS} characters]"
    return diff


def _collect_diff(workdir: Path, base_ref: str) -> str:
    candidates = (base_ref,) if base_ref else ("origin/HEAD", "main")
    base = ""
    for ref in candidates:
        probe = subprocess.run(
            ["git", "merge-base", ref, "HEAD"], cwd=str(workdir), capture_output=True, text=True, check=False
        )
        if probe.returncode == 0 and probe.stdout.strip():
            base, base_ref = probe.stdout.strip(), ref
            break
    if not base:
        raise ReviewAborted(f"cannot find a merge base with {' or '.join(candidates)} in {workdir}; pass 'base_ref'")
    commits = _git(workdir, "log", "--oneline", f"{base}..HEAD").strip()
    stat = _git(workdir, "diff", "--stat", base).strip()
    untracked = _git(workdir, "ls-files", "--others", "--exclude-standard").strip()
    diff = _git(workdir, "diff", base)
    if not diff.strip() and not untracked:
        raise ReviewAborted(f"no changes between {base_ref} ({base[:12]}) and {workdir}")
    if len(diff) > _MAX_DIFF_CHARS:
        diff = diff[:_MAX_DIFF_CHARS] + f"\n[diff truncated at {_MAX_DIFF_CHARS} characters; run git diff {base} for the rest]"
    return "\n\n".join(
        (
            f"Workdir: {workdir}\nBase: {base_ref} (merge base {base})",
            f"Commits since base:\n{commits or '(none, uncommitted changes only)'}",
            f"Untracked files:\n{untracked or '(none)'}",
            f"Diffstat:\n{stat}",
            f"```diff\n{diff}\n```",
        )
    )
