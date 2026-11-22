from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from agent_runtime.adapters.executor_cli.claude_code import (
    ClaudeCodeError,
    ClaudeCodeProfile,
    child_env,
    run_claude_code,
)
from agent_runtime.adapters.executor_cli.orca import OrcaVisibleRunner, terminal_title
from agent_runtime.adapters.executor_cli.process import is_approval, join_text, last_text, target_events
from agent_runtime.adapters.executor_cli.reviewer import (
    CoderReviewerExecutor,
    ReviewAborted,
    ReviewRequest,
    format_blocking,
    snapshot_diff,
    worktree_fingerprint,
)
from agent_runtime.adapters.executor_cli.worktree import OrcaWorktreeManager
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Suspend, new_id
from agent_runtime.core.ports import RunCapabilities

SDE_ROLE = "sde"
SDE_OPTION_KEYS = frozenset({"review", "reviewer", "max_review_rounds"})
_CHECKPOINT_KEY = "sde"
_DEV_TOOLS = ("Read", "Grep", "Glob", "Edit", "Write", "MultiEdit", "NotebookEdit", "Bash")
# Delivery stays in the task worktree; publishing is left to the operator.
_DEV_DENIED = ("Bash(git push:*)", "Bash(gh pr create:*)", "Bash(gh pr merge:*)")

SDE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["done", "needs_input"]},
        "summary": {"type": "string"},
        "test_evidence": {"type": "string"},
        "question": {"type": "string"},
    },
    "required": ["status", "summary", "test_evidence"],
}

_VISIBLE_ROLE_ARGS = (
    "--allowedTools", ",".join(_DEV_TOOLS),
    "--disallowedTools", ",".join(_DEV_DENIED),
)
_ROLE_ARGS = (*_VISIBLE_ROLE_ARGS, "--json-schema", json.dumps(SDE_SCHEMA))

_SDE_RULES = """You are sde, a software development engineer. Implement the task below in the current git worktree.

Work test-first:
- For a bug or new behaviour, first write a failing test that fails for the right reason, run it and keep its raw output.
- Make the smallest change that makes it pass, run it again and keep the raw output.
- Cover every acceptance criterion that asks for a regression test.
- Then run the repository's full checks (see AGENTS.md, CLAUDE.md or README) and fix whatever fails.
- Documentation or configuration with no applicable test: say why no test applies.
- Commit on the current branch. Do not push, open or merge pull requests, or touch other worktrees.
- If you cannot continue without a decision from a person, return status "needs_input" with one question.

Return test_evidence with each test's command and raw output, failing first and then passing, followed by the
full-check results. The summary says what changed and where."""


@dataclass(frozen=True, slots=True)
class SDEOptions:
    review: bool = True
    reviewer: str = "coder-reviewer"
    max_review_rounds: int = 3


def _no_blocking() -> list[dict[str, str]]:
    return []


def _no_history() -> list[str]:
    return []


@dataclass(slots=True)
class _State:
    stage: Literal["develop", "launch_pending", "needs_input", "review_pending"]
    workdir: str
    base_ref: str
    requirements: str
    summary: str = ""
    test_evidence: str = ""
    rounds: int = 0
    history: list[str] = field(default_factory=_no_history)
    blocking: list[dict[str, str]] = field(default_factory=_no_blocking)
    last_review: str = ""
    reviewed_snapshot: str = ""
    reason: str = ""
    # Messages that arrived while suspended; folded into the requirements when the run resumes.
    followups: list[str] = field(default_factory=_no_history)
    pending_prompt: str = ""
    work_key: str = ""
    # Orca terminal of the last visible development turn.
    terminal: str = ""

    def dump(self) -> str:
        return json.dumps({_CHECKPOINT_KEY: {f: getattr(self, f) for f in self.__slots__}}, ensure_ascii=False)

    @staticmethod
    def load(checkpoint: str | None) -> _State | None:
        try:
            raw: object = json.loads(checkpoint or "")
        except json.JSONDecodeError:
            return None
        if not isinstance(raw, dict):
            return None
        data = cast(dict[str, Any], raw).get(_CHECKPOINT_KEY)
        if not isinstance(data, dict):
            return None
        try:
            return _State(**cast(dict[str, Any], data))
        except TypeError:
            return None


def _approval(events: Sequence[Event], request_id: str | None) -> tuple[bool, str] | None:
    for evt in events:
        if request_id and evt.payload.get("approval_request_id") == request_id:
            comment = evt.payload.get("comment")
            return evt.payload.get("approved") is True, comment.strip() if isinstance(comment, str) else ""
    return None


class SDEExecutor:
    def __init__(
        self,
        *,
        profile: ClaudeCodeProfile,
        options: SDEOptions,
        reviewer: CoderReviewerExecutor | None,
        worktree_manager: OrcaWorktreeManager,
        inbox_checkout_map: Mapping[str, str | Path],
        interrupt_poll_interval: float = 0.05,
        visible: OrcaVisibleRunner | None = None,
    ) -> None:
        if options.review and reviewer is None:
            raise ValueError(f"executors.{profile.name}: review is on but no reviewer was given")
        self._profile = profile
        self._options = options
        self._reviewer = reviewer
        self._worktrees = worktree_manager
        self._checkouts = {k: Path(v).resolve() for k, v in inbox_checkout_map.items()}
        self._poll = interrupt_poll_interval
        # Set in Orca worktree deployments: development runs in a visible terminal bound to the task session.
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
        native = session.executor_session_id if session is not None and session.executor_id == self.executor_id else None
        pending_id = session.pending_request_id if session is not None else None
        targets = target_events(events)
        try:
            resumed = _State.load(session.checkpoint) if session is not None and pending_id else None
            prompt: str | None
            approved_launch = False
            if resumed is not None:
                state = resumed
                answer = _approval(events, pending_id)
                if answer is None:
                    # Not the awaited decision: record it and keep the same request open.
                    text = join_text(targets, ("requirements", "text"))
                    if text:
                        state.followups.append(text)
                    return self._suspend(session_id, native, state, request_id=pending_id)
                approved, comment = answer
                # Messages claimed in the same batch as the decision count as follow-ups too.
                batched = join_text([e for e in events if not is_approval(e)], ("requirements", "text"))
                if batched:
                    state.followups.append(batched)
                if not approved:
                    if state.stage == "review_pending":
                        return self._deliver(session_id, native, state, review="pending",
                                             note=f"retry declined; {state.reason}")
                    why = "question declined" if state.stage == "needs_input" else "launch declined"
                    return self._stopped(session_id, native, state, why)
                followups = self._take_followups(state)
                if state.stage == "needs_input":
                    reply = "\n\n".join(p for p in (comment, followups) if p)
                    prompt = f"## Answer from a person\n{reply or '(approved without comment)'}"
                elif state.stage == "launch_pending":
                    prompt = state.pending_prompt + (f"\n\n## Follow-up from a person\n{followups}" if followups else "")
                    approved_launch = True
                else:
                    state.rounds = 0
                    prompt = self._fix_prompt(state) if state.blocking else None
                    if followups:
                        # New requirements must be developed before the retried review sees them.
                        prompt = (prompt + "\n\n" if prompt else "") + f"## Follow-up from a person\n{followups}"
            else:
                state = await self._new_state(events, targets, session)
                prompt = self._first_prompt(state, caps)
            return await self._run(session_id, native, state, caps, prompt, approved_launch=approved_launch)
        except (ClaudeCodeError, ReviewAborted, RuntimeError, OSError) as exc:
            return self._fail(session_id, native, str(exc))

    @staticmethod
    def _take_followups(state: _State) -> str:
        text = "\n\n".join(state.followups)
        if text:
            state.requirements = f"{state.requirements}\n\n## Follow-up from a person\n{text}"
        state.followups = []
        return text

    def _launch_args(self) -> dict[str, Any]:
        p = self._profile
        return {"executor": p.name, "harness": p.harness, "launcher": p.launcher.name, "billing": p.billing,
                "model": p.model, "effort": p.effort, "review": self._options.review}

    async def _new_state(self, events: Sequence[Event], targets: Sequence[Event], session: SessionRef | None) -> _State:
        requirements = join_text(targets, ("requirements", "text"))
        if not requirements:
            raise ReviewAborted("payload needs the task requirements in 'requirements' or 'text'")
        inbox_id = events[0].inbox_id
        explicit = last_text(targets, "explicit_workdir")
        work_key = (
            events[0].correlation_key
            or (session.correlation_key if session is not None else None)
            or f"task:{inbox_id}:{events[0].event_id}"
        )
        checkout = self._checkouts.get(inbox_id)
        if explicit is None and checkout is None:
            raise ReviewAborted(f"no primary checkout configured for '{inbox_id}' and no 'explicit_workdir'")
        workdir = await asyncio.to_thread(
            self._worktrees.resolve_workdir,
            work_key=work_key,
            primary_checkout=checkout or Path(str(explicit)),
            explicit_workdir=explicit,
        )
        if not workdir.is_dir():
            raise ReviewAborted(f"workdir {workdir} does not exist")
        return _State(stage="develop", workdir=str(workdir), base_ref=last_text(targets, "base_ref") or "",
                      requirements=requirements, work_key=work_key)

    def _first_prompt(self, state: _State, caps: RunCapabilities) -> str:
        parts = [_SDE_RULES]
        baseline = caps.memory.load_baseline()
        for name in ("instructions.md", "index.md"):
            text = baseline.get(name, "").strip()
            if text:
                parts.append(f"## {name}\n{text}")
        parts.append(f"## Task requirements\n{state.requirements}")
        return "\n\n".join(parts)

    def _fix_prompt(self, state: _State) -> str:
        return (
            f"## Review {state.history[-1] if state.history else 'round'}: changes requested\n"
            "An independent reviewer found the blocking items below. Fix every one, test-first (a failing test "
            "for each bug before the fix), rerun the full checks, commit, and report fresh test evidence.\n\n"
            + format_blocking(state.blocking)
        )

    async def _develop(
        self, session_id: str, native: str | None, state: _State, prompt: str, caps: RunCapabilities
    ) -> tuple[dict[str, Any], str | None]:
        p = self._profile
        if self._visible is not None:
            # Fix rounds and later events continue the task's conversation: its live terminal, else --resume.
            reply = await self._visible.converse(
                p,
                title=terminal_title(p.name, SDE_ROLE, state.work_key or Path(state.workdir).name),
                workdir=Path(state.workdir),
                prompt=prompt,
                schema=SDE_SCHEMA,
                role_args=_VISIBLE_ROLE_ARGS,
                is_interrupted=caps.is_interrupted,
                session_id=session_id,
                native=native,
            )
            state.terminal = reply.handle
            return _validate_report(reply.data), reply.native_session_id
        role_args: tuple[str, ...] = (*_ROLE_ARGS, "--resume", native) if native else _ROLE_ARGS
        result = await run_claude_code(
            p,
            cwd=Path(state.workdir),
            prompt=prompt,
            permission_mode="dontAsk",
            role_args=role_args,
            env=child_env(p, os.environ),
            is_interrupted=caps.is_interrupted,
            poll_interval=self._poll,
        )
        raw_sid = result.get("session_id")
        sid = raw_sid if isinstance(raw_sid, str) and raw_sid else native
        return _validate_report(result.get("structured_output")), sid

    async def _run(
        self,
        session_id: str,
        native: str | None,
        state: _State,
        caps: RunCapabilities,
        prompt: str | None,
        *,
        approved_launch: bool = False,
    ) -> RunOutcome:
        # prompt=None goes straight to review (retrying one that could not complete).
        while True:
            if prompt is not None:
                # Every developer launch passes the gate, fix rounds included; an approved deferral runs once.
                if not approved_launch:
                    decision = caps.permissions.check("sde.launch", {**self._launch_args(), "workdir": state.workdir})
                    if decision.action == "block":
                        return self._stopped(session_id, native, state, f"launch blocked: {decision.reason}")
                    if decision.action == "defer":
                        state.stage, state.reason, state.pending_prompt = "launch_pending", decision.reason, prompt
                        return self._suspend(session_id, native, state)
                approved_launch = False
                state.pending_prompt = ""
                report, native = await self._develop(session_id, native, state, prompt, caps)
                if report["status"] == "needs_input":
                    state.stage, state.reason = "needs_input", report["question"]
                    return self._suspend(session_id, native, state)
                state.summary, state.test_evidence = report["summary"], report["test_evidence"]
            reviewer = self._reviewer
            if not self._options.review or reviewer is None:
                return self._deliver(session_id, native, state, review="off")
            workdir = Path(state.workdir)
            decision = reviewer.check_launch(caps, workdir)
            if decision.action != "allow":
                return self._pending(session_id, native, state, f"review launch {decision.action}: {decision.reason}")
            try:
                snapshot = await asyncio.to_thread(worktree_fingerprint, workdir)
                previous = ""
                if state.blocking and state.reviewed_snapshot:
                    delta = await asyncio.to_thread(snapshot_diff, workdir, state.reviewed_snapshot, snapshot)
                    previous = (
                        f"## Previous review round\n{state.last_review}\n\n"
                        f"## Changes since the previous review\n```diff\n{delta}\n```"
                    )
                subject = state.work_key or workdir.name
                review = await reviewer.review(
                    ReviewRequest(workdir=workdir, requirements=state.requirements,
                                  test_evidence=state.test_evidence, base_ref=state.base_ref,
                                  previous_round=previous,
                                  label=f"review of {subject} round {len(state.history) + 1}"),
                    caps,
                )
            except (ReviewAborted, ClaudeCodeError) as exc:
                return self._pending(session_id, native, state, f"the review could not complete: {exc}")
            blocking: list[dict[str, str]] = review["blocking"]
            state.rounds += 1
            state.history.append(
                f"round {len(state.history) + 1} {review['verdict']}"
                + (f" ({len(blocking)} blocking)" if blocking else "")
            )
            state.last_review = reviewer.format_review(review)
            state.reviewed_snapshot = snapshot
            state.blocking = blocking
            if review["verdict"] == "pass":
                return self._deliver(session_id, native, state, review="pass")
            if state.rounds >= self._options.max_review_rounds:
                return self._pending(
                    session_id, native, state,
                    f"still changes_requested after {state.rounds} round(s)\n{format_blocking(blocking)}",
                )
            prompt = self._fix_prompt(state)

    def _pending(self, session_id: str, native: str | None, state: _State, reason: str) -> Suspend:
        state.stage, state.reason = "review_pending", reason
        return self._suspend(session_id, native, state)

    def _suspend(self, session_id: str, native: str | None, state: _State, request_id: str | None = None) -> Suspend:
        name = self._profile.name
        if state.stage == "needs_input":
            prompt = (
                f"[{name}] needs input: {state.reason}\n"
                "Answer by replying on this task or approving with a comment, then approve; reject to stop the task."
            )
        elif state.stage == "launch_pending":
            prompt = (
                f"[{name}] launch needs approval: {state.reason}\n"
                f"Approve to start this development run in {state.workdir}; reject to stop the task."
            )
        else:
            retry = "with another fix round" if state.blocking else "the review"
            prompt = (
                f"[{name}] review pending: {state.reason}\n"
                f"The change in {state.workdir} has not passed review. "
                f"Approve to retry {retry}; reject to deliver it marked unreviewed."
            )
        return Suspend(
            request_id=request_id or new_id("req"),
            prompt=prompt,
            session_checkpoint=state.dump(),
            session_id=session_id,
            executor_session_id=native,
        )

    def _settings(self) -> str:
        p = self._profile
        return f"harness={p.harness} launcher={p.launcher.name} billing={p.billing} model={p.model} effort={p.effort}"

    def _deliver(self, session_id: str, native: str | None, state: _State, *, review: str, note: str = "") -> Emit:
        name = self._profile.name
        if review == "pass":
            status = f"review=pass after {state.rounds} round(s)"
        elif review == "off":
            status = "review=off"
        else:
            status = f"review=pending ({note})"
        lines = [
            f"[{name}] delivered ({status}; {self._settings()})",
            f"Workdir: {state.workdir}",
            *([f"Orca terminal: {state.terminal} (Claude Code session {native})"] if state.terminal else []),
            f"Summary: {state.summary or '(none)'}",
            f"Test evidence:\n{state.test_evidence or '(none)'}",
        ]
        if state.history:
            lines.append(f"Review history: {'; '.join(state.history)}")
        if state.last_review:
            lines.append(f"Last review:\n{state.last_review}")
        if state.followups:
            lines.append("Unprocessed follow-ups:\n" + "\n\n".join(state.followups))
        return Emit(messages=("\n".join(lines),), session_id=session_id, executor_session_id=native)

    def _stopped(self, session_id: str, native: str | None, state: _State, why: str) -> Emit:
        lines = [f"[{self._profile.name}] stopped: {why} ({self._settings()})", f"Workdir: {state.workdir}"]
        if state.stage == "needs_input":
            lines.append(f"Question: {state.reason}")
        if state.summary:
            lines.append(f"Summary so far: {state.summary}")
        if state.test_evidence:
            lines.append(f"Test evidence so far:\n{state.test_evidence}")
        if state.history:
            lines.append(f"Review history: {'; '.join(state.history)}")
        if state.followups:
            lines.append("Unprocessed follow-ups:\n" + "\n\n".join(state.followups))
        return Emit(messages=("\n".join(lines),), session_id=session_id, executor_session_id=native)

    def _fail(self, session_id: str, native: str | None, detail: str) -> Emit:
        return Emit(messages=(f"[{self._profile.name}] development failed: {detail}",),
                    session_id=session_id, executor_session_id=native)


def _validate_report(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ClaudeCodeError("the developer returned no structured report")
    data = cast(dict[str, Any], raw)
    status = data.get("status")
    summary = data.get("summary")
    evidence = data.get("test_evidence")
    question = data.get("question")
    if status not in ("done", "needs_input") or not isinstance(summary, str) or not isinstance(evidence, str):
        raise ClaudeCodeError("the developer returned a malformed report")
    if status == "needs_input":
        if not isinstance(question, str) or not question.strip():
            raise ClaudeCodeError("the developer asked for input without a question")
        return {"status": status, "summary": summary.strip(), "test_evidence": evidence.strip(), "question": question.strip()}
    if not evidence.strip():
        raise ClaudeCodeError("the developer reported done without test evidence")
    return {"status": status, "summary": summary.strip(), "test_evidence": evidence.strip(), "question": ""}

