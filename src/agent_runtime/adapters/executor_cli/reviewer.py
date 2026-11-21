from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Suspend, new_id
from agent_runtime.core.ports import RunCapabilities

CODER_REVIEWER_ROLE = "coder-reviewer"
HARNESSES = ("claude-code",)
BILLINGS = ("subscription", "api")
EFFORTS = ("low", "medium", "high", "xhigh", "max")
_CLAUDE_MODEL_RE = re.compile(r"(claude-[a-z0-9][a-z0-9.-]*(\[1m\])?|opus|sonnet|haiku|fable)")
# Any of these makes Claude Code bill an API key or a cloud provider instead of the subscription.
_PAID_ENV_KEYS = (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
)
_API_KEY_ENV = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")
_CREDENTIAL_ENV_RE = re.compile(r"(KEY|TOKEN|SECRET|PASSWORD)", re.IGNORECASE)
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
_STREAM_LIMIT = 16 * 1024 * 1024

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

_REVIEW_RULES = """You are coder-reviewer, an independent code reviewer. Another agent wrote the change below; judge it, do not fix it.

Rules:
- Review only. Do not create, edit, stage, commit, stash or delete anything. Any change to the working tree voids this review.
- Judge the change against all three inputs: the task requirements, the code diff, and the test evidence. Read surrounding code when the diff alone is not enough.
- verdict "pass" only when no blocking issue remains; otherwise "changes_requested" with at least one blocking item.
- A blocking item is something that must change before merge: a bug, a missed requirement, a broken contract, a security problem, or a requirement the test evidence does not show to work.
- Every blocking item gives location (path:line), trigger (the inputs or state that make it go wrong), impact (what breaks for whom), and optionally fix.
- Style preferences and optional improvements go to notes, never to blocking.
- The developer fixes the blocking items and submits again for a fresh, independent review."""


def _empty_env() -> dict[str, str]:
    return {}


@dataclass(frozen=True, slots=True)
class ReviewerProfile:
    name: str
    harness: str = "claude-code"
    billing: str = "subscription"
    model: str = "claude-opus-5-5"
    effort: str = "high"
    command: str = "claude"
    timeout_seconds: float = 1800.0
    env: dict[str, str] = field(default_factory=_empty_env)

    def __post_init__(self) -> None:
        where = f"executors.{self.name}"
        if self.harness not in HARNESSES:
            raise ValueError(f"{where}: unsupported harness '{self.harness}' (supported: {', '.join(HARNESSES)})")
        if self.billing not in BILLINGS:
            raise ValueError(f"{where}: unsupported billing '{self.billing}' (supported: {', '.join(BILLINGS)})")
        if self.effort not in EFFORTS:
            raise ValueError(f"{where}: unsupported effort '{self.effort}' (supported: {', '.join(EFFORTS)})")
        if not _CLAUDE_MODEL_RE.fullmatch(self.model):
            raise ValueError(f"{where}: model '{self.model}' is not a Claude model, which harness '{self.harness}' requires")
        if not self.command.strip():
            raise ValueError(f"{where}: command must not be empty")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError(f"{where}: timeout_seconds must be finite and positive")
        for key in self.env:
            if key in _PAID_ENV_KEYS or _CREDENTIAL_ENV_RE.search(key):
                raise ValueError(
                    f"{where}: env.{key} looks like a credential or billing switch; credentials stay in the "
                    "daemon environment and never in hibernaut.toml"
                )


def parse_reviewer_profile(name: str, item: Mapping[str, Any]) -> ReviewerProfile:
    allowed = {"role", "harness", "billing", "model", "effort", "command", "timeout_seconds", "env"}
    unknown = sorted(set(item) - allowed)
    if unknown:
        raise ValueError(f"executors.{name}: {', '.join(unknown)} not supported for role '{CODER_REVIEWER_ROLE}'")
    env_raw = item.get("env", {})
    if not isinstance(env_raw, Mapping):
        raise ValueError(f"executors.{name}: env must be a table")
    env = {str(k): str(v) for k, v in env_raw.items()}
    defaults = ReviewerProfile(name=name)
    return ReviewerProfile(
        name=name,
        harness=str(item.get("harness", defaults.harness)),
        billing=str(item.get("billing", defaults.billing)),
        model=str(item.get("model", defaults.model)),
        effort=str(item.get("effort", defaults.effort)),
        command=str(item.get("command", defaults.command)),
        timeout_seconds=float(item.get("timeout_seconds", defaults.timeout_seconds)),
        env=env,
    )


class ReviewAborted(Exception):
    pass


@dataclass(frozen=True, slots=True)
class _ReviewInput:
    workdir: Path
    requirements: str
    test_evidence: str
    base_ref: str


class CoderReviewerExecutor:
    def __init__(
        self,
        *,
        profile: ReviewerProfile,
        workspaces: SQLiteWorkspaceStore,
        interrupt_poll_interval: float = 0.05,
    ) -> None:
        self._profile = profile
        self._workspaces = workspaces
        self._interrupt_poll_interval = interrupt_poll_interval

    @property
    def executor_id(self) -> str:
        return self._profile.name

    @property
    def profile(self) -> ReviewerProfile:
        return self._profile

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        session_id = session.session_id if session is not None else new_id("sess")
        try:
            review = self._review_input(events, session)
            decision = caps.permissions.check(
                "coder_reviewer.launch",
                {
                    "executor": self._profile.name,
                    "workdir": str(review.workdir),
                    "harness": self._profile.harness,
                    "billing": self._profile.billing,
                    "model": self._profile.model,
                    "effort": self._profile.effort,
                },
            )
            if decision.action == "block":
                return self._emit(session_id, decision.reason)
            if decision.action == "defer":
                return Suspend(
                    request_id=new_id("req"),
                    prompt=decision.reason,
                    session_checkpoint="{}",
                    session_id=session_id,
                )
            env = self._child_env()
            evidence = await asyncio.to_thread(_collect_diff, review.workdir, review.base_ref)
            before = await asyncio.to_thread(worktree_fingerprint, review.workdir)
            result = await self._run_harness(review, evidence, env, caps)
            after = await asyncio.to_thread(worktree_fingerprint, review.workdir)
            if after != before:
                raise ReviewAborted(
                    f"the working tree in {review.workdir} changed during the review; findings discarded, "
                    "check the change and submit again"
                )
            return Emit(messages=(self._format(result),), session_id=session_id)
        except ReviewAborted as exc:
            return self._emit(session_id, str(exc))

    def _emit(self, session_id: str, detail: str) -> Emit:
        return Emit(messages=(f"[{self._profile.name}] review failed: {detail}",), session_id=session_id)

    def _review_input(self, events: Sequence[Event], session: SessionRef | None) -> _ReviewInput:
        target = [e for e in events if not _is_approval(e)] or list(events)
        requirements = _join(target, ("requirements", "text"))
        if not requirements:
            raise ReviewAborted("payload needs the task requirements in 'requirements' or 'text'")
        explicit = _last_text(target, "explicit_workdir")
        if explicit is not None:
            workdir = Path(explicit).resolve()
        else:
            # A review must never create a worktree; it reads the one the developer already used.
            work_key = (
                _last_text(target, "review_of")
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
        return _ReviewInput(
            workdir=workdir,
            requirements=requirements,
            test_evidence=_join(target, ("test_evidence",)) or "(none provided)",
            base_ref=_last_text(target, "base_ref") or "",
        )

    def _child_env(self) -> dict[str, str]:
        env = dict(os.environ)
        if self._profile.billing == "subscription":
            for key in _PAID_ENV_KEYS:
                env.pop(key, None)
        elif not any(env.get(key) for key in _API_KEY_ENV):
            raise ReviewAborted(
                "billing 'api' needs ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN in the daemon environment"
            )
        env.update(self._profile.env)
        return env

    def _cli_args(self) -> list[str]:
        p = self._profile
        return [
            "-p",
            "--output-format", "stream-json",
            "--verbose",
            "--model", p.model,
            "--effort", p.effort,
            "--no-session-persistence",
            "--permission-mode", "dontAsk",
            "--allowedTools", ",".join(_READ_ONLY_TOOLS),
            "--disallowedTools", ",".join(_WRITE_TOOLS),
            "--json-schema", json.dumps(REVIEW_SCHEMA),
        ]

    def _prompt(self, review: _ReviewInput, evidence: str) -> str:
        return "\n\n".join(
            (
                _REVIEW_RULES,
                f"## Task requirements\n{review.requirements}",
                f"## Test evidence\n{review.test_evidence}",
                f"## Code diff\n{evidence}",
            )
        )

    async def _run_harness(
        self,
        review: _ReviewInput,
        evidence: str,
        env: dict[str, str],
        caps: RunCapabilities,
    ) -> dict[str, Any]:
        try:
            proc = await asyncio.create_subprocess_exec(
                self._profile.command,
                *self._cli_args(),
                cwd=str(review.workdir),
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_STREAM_LIMIT,
            )
        except OSError as exc:
            raise ReviewAborted(f"cannot start '{self._profile.command}': {exc.strerror or exc}") from exc
        consume = asyncio.create_task(self._consume(proc, self._prompt(review, evidence)))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._profile.timeout_seconds
        try:
            while True:
                if caps.is_interrupted():
                    raise asyncio.CancelledError("review interrupted by new event")
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise ReviewAborted(f"timed out after {self._profile.timeout_seconds}s")
                done, _ = await asyncio.wait({consume}, timeout=min(self._interrupt_poll_interval, remaining))
                if done:
                    return consume.result()
        finally:
            if not consume.done():
                consume.cancel()
            await _terminate(proc)

    async def _consume(self, proc: asyncio.subprocess.Process, prompt: str) -> dict[str, Any]:
        assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None
        stdin = proc.stdin

        async def feed() -> None:
            try:
                stdin.write(prompt.encode("utf-8"))
                await stdin.drain()
                stdin.close()
            except (BrokenPipeError, ConnectionResetError):
                pass

        stderr_task = asyncio.create_task(proc.stderr.read())
        feed_task = asyncio.create_task(feed())
        try:
            return await self._read_stream(proc, stderr_task)
        finally:
            for task in (feed_task, stderr_task):
                if not task.done():
                    task.cancel()

    async def _read_stream(
        self, proc: asyncio.subprocess.Process, stderr_task: asyncio.Task[bytes]
    ) -> dict[str, Any]:
        assert proc.stdout is not None
        initialized = False
        result: dict[str, Any] | None = None
        while line := await proc.stdout.readline():
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(message, dict):
                continue
            message = dict(message)
            if message.get("type") == "system" and message.get("subtype") == "init":
                # init precedes the first model request, so a wrong account or model is stopped at session start.
                self._check_init(message)
                initialized = True
            elif message.get("type") == "result":
                result = message
        returncode = await proc.wait()
        stderr = (await stderr_task).decode("utf-8", errors="replace").strip()
        if not initialized:
            raise ReviewAborted(f"{self._profile.harness} exited ({returncode}) before reporting its session: {stderr[-2000:]}")
        if result is None:
            raise ReviewAborted(f"{self._profile.harness} exited ({returncode}) without a result: {stderr[-2000:]}")
        if result.get("is_error") or result.get("subtype") != "success":
            raise ReviewAborted(f"{self._profile.harness} reported an error: {str(result.get('result', ''))[:2000]}")
        return _validate_review(result.get("structured_output"))

    def _check_init(self, init: Mapping[str, Any]) -> None:
        p = self._profile
        source = str(init.get("apiKeySource", ""))
        if p.billing == "subscription" and source != "none":
            raise ReviewAborted(
                f"billing is 'subscription' but Claude Code would authenticate with '{source}'; "
                "stopped at session start. Remove that credential source or set billing = \"api\""
            )
        if p.billing == "api" and source in ("", "none"):
            raise ReviewAborted("billing is 'api' but Claude Code found no API key; stopped at session start")
        actual = str(init.get("model", ""))
        if p.model.startswith("claude-") and actual.removesuffix("[1m]") != p.model.removesuffix("[1m]"):
            raise ReviewAborted(f"configured model '{p.model}' but Claude Code selected '{actual}'")

    def _format(self, review: Mapping[str, Any]) -> str:
        p = self._profile
        lines = [
            f"[{p.name}] verdict={review['verdict']} "
            f"(harness={p.harness} billing={p.billing} model={p.model} effort={p.effort})",
            f"Summary: {review['summary']}",
        ]
        blocking: list[dict[str, str]] = review["blocking"]
        if blocking:
            lines.append("Blocking:")
            for i, item in enumerate(blocking, 1):
                lines.append(f"{i}. {item['location']}")
                lines.append(f"   Trigger: {item['trigger']}")
                lines.append(f"   Impact: {item['impact']}")
                if item.get("fix"):
                    lines.append(f"   Fix: {item['fix']}")
            lines.append(f"After fixing, submit again with executor_id={p.name} for a fresh review.")
        notes: list[str] = review.get("notes", [])
        if notes:
            lines.append("Notes:")
            lines.extend(f"- {note}" for note in notes)
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


async def _terminate(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    try:
        proc.terminate()
        await asyncio.wait_for(proc.wait(), timeout=2.0)
    except (ProcessLookupError, asyncio.TimeoutError):
        try:
            proc.kill()
            await proc.wait()
        except ProcessLookupError:
            pass


def _is_approval(event: Event) -> bool:
    request_id = event.payload.get("approval_request_id")
    return isinstance(request_id, str) and bool(request_id)


def _last_text(events: Sequence[Event], key: str) -> str | None:
    for evt in reversed(events):
        val = evt.payload.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def _join(events: Sequence[Event], keys: Sequence[str]) -> str:
    parts: list[str] = []
    for evt in events:
        for key in keys:
            val = evt.payload.get(key)
            if isinstance(val, str) and val.strip():
                parts.append(val.strip())
                break
    return "\n\n".join(parts)
