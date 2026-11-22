from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from agent_runtime.adapters.executor_cli.executor import CLITemplate, render_cli_args
from agent_runtime.adapters.executor_cli.orca import (
    OrcaCommandTerminal,
    TerminalRunFailed,
    TerminalStartFailed,
    TerminalUnavailable,
    terminal_title,
)
from agent_runtime.adapters.executor_cli.process import ProcessTimeout, supervise
from agent_runtime.adapters.executor_cli.reviewer import (
    CODE_REVIEWER_ROLE,
    REVIEW_LAUNCH_TOOL,
    ReviewAborted,
    ReviewRequest,
    plain_review_contract,
    render_review,
    review_inputs,
    review_prompt,
    validate_review,
    worktree_fingerprint,
)
from agent_runtime.core.models import PermissionDecision
from agent_runtime.core.ports import RunCapabilities

_FENCE = re.compile(r"```[ \t]*(?:json)?[ \t]*\r?\n(.*?)```", re.DOTALL | re.IGNORECASE)
_MAX_STDERR_CHARS = 500
_log = logging.getLogger(__name__)


def _no_handles() -> list[str]:
    return []


@dataclass(slots=True)
class _Where:
    # Where one review's runs went: Orca terminals, or a subprocess after Orca was unavailable (fallback says why).
    handles: list[str] = field(default_factory=_no_handles)
    fallback: str = ""


class CLIReviewer:
    # Reviews through a plain [executors.<id>] CLI template. Nothing restricts its tools, so read-only is checked
    # afterwards: a review that changed the worktree is discarded.
    def __init__(
        self,
        *,
        template: CLITemplate,
        terminal: OrcaCommandTerminal | TerminalUnavailable | None = None,
        interrupt_poll_interval: float = 0.05,
    ) -> None:
        self._template = template
        # Used only when the template sets terminal = "orca"; Unavailable or None then means a subprocess run.
        self._terminal = terminal
        self._poll = interrupt_poll_interval

    @property
    def executor_id(self) -> str:
        return self._template.name

    def check_launch(self, caps: RunCapabilities, workdir: Path) -> PermissionDecision:
        t = self._template
        return caps.permissions.check(
            REVIEW_LAUNCH_TOOL, {"executor": t.name, "workdir": str(workdir), "command": t.command},
        )

    async def review(self, request: ReviewRequest, caps: RunCapabilities) -> dict[str, Any]:
        evidence, guidance = await review_inputs(request)
        prompt = f"{review_prompt(request, guidance, evidence)}\n\n{plain_review_contract()}"
        subject = request.label or request.workdir.name
        where = self._where()
        before = await asyncio.to_thread(worktree_fingerprint, request.workdir)
        stdout = await self._run(prompt, request.workdir, caps, where, subject)
        await self._unchanged(request.workdir, before)
        try:
            return {**validate_review(extract_verdict(stdout)), **self._extras(where)}
        except ReviewAborted as exc:
            # One retry with the reason: a model that wrapped or misshaped its JSON usually fixes it when told.
            retry = (f"{prompt}\n\n## Your previous output was rejected\n{exc}. Output exactly one JSON object that "
                     "matches the schema above, and nothing else.")
        stdout = await self._run(retry, request.workdir, caps, where, f"{subject} format retry")
        await self._unchanged(request.workdir, before)
        try:
            return {**validate_review(extract_verdict(stdout)), **self._extras(where)}
        except ReviewAborted as exc:
            raise ReviewAborted(f"{self._template.name} returned no valid verdict after one retry: {exc}") from exc

    def _where(self) -> _Where | None:
        if self._template.terminal != "orca":
            return None
        terminal = self._terminal
        if isinstance(terminal, TerminalUnavailable):
            return _Where(fallback=terminal.reason)
        if terminal is None:
            return _Where(fallback="no Orca terminal runner was given to this reviewer")
        return _Where()

    def _extras(self, where: _Where | None) -> dict[str, Any]:
        if where is None:
            return {}
        return {"cli_terminal": "fallback" if where.fallback else "orca", "cli_terminal_fallback": where.fallback,
                "cli_terminals": list(where.handles)}

    async def _unchanged(self, workdir: Path, before: str) -> None:
        after = await asyncio.to_thread(worktree_fingerprint, workdir)
        if after != before:
            raise ReviewAborted(
                f"the working tree in {workdir} changed during the review by {self._template.name}; findings "
                "discarded, check the change and submit again"
            )

    async def _run(self, prompt: str, workdir: Path, caps: RunCapabilities, where: _Where | None, subject: str) -> str:
        if where is not None and not where.fallback and isinstance(self._terminal, OrcaCommandTerminal):
            try:
                return await self._run_in_terminal(self._terminal, prompt, workdir, caps, where, subject)
            except TerminalStartFailed as exc:
                # Nothing ran: the same run goes to a subprocess, and later runs of this review follow it.
                _log.warning("%s: Orca could not open a terminal, running as a subprocess: %s", self._template.name, exc)
                where.fallback = f"Orca could not open a terminal ({exc})"
        return await self._run_subprocess(prompt, workdir, caps)

    async def _run_in_terminal(
        self, terminal: OrcaCommandTerminal, prompt: str, workdir: Path, caps: RunCapabilities, where: _Where,
        subject: str,
    ) -> str:
        t = self._template
        try:
            run = await terminal.run(
                title=terminal_title(t.name, CODE_REVIEWER_ROLE, subject), workdir=workdir,
                argv=[t.command, *render_cli_args(t.args, prompt=prompt)], env=t.env,
                timeout_seconds=t.timeout_seconds, is_interrupted=caps.is_interrupted,
            )
        except ProcessTimeout as exc:
            raise ReviewAborted(f"{t.name}: '{t.command}' {exc}") from exc
        except TerminalRunFailed as exc:
            raise ReviewAborted(f"{t.name}: {exc}") from exc
        where.handles.append(run.handle)
        if run.exit_code is None:
            raise ReviewAborted(f"{t.name}: {run.error or f'no exit code from Orca terminal {run.handle}'}")
        return self._stdout(run.exit_code, run.stdout, run.stderr)

    async def _run_subprocess(self, prompt: str, workdir: Path, caps: RunCapabilities) -> str:
        t = self._template
        try:
            proc = await asyncio.create_subprocess_exec(
                t.command, *render_cli_args(t.args, prompt=prompt),
                cwd=str(workdir), env={**os.environ, **t.env},
                stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
        except OSError as exc:
            raise ReviewAborted(f"{t.name}: cannot start '{t.command}': {exc.strerror or exc}") from exc
        try:
            stdout, stderr = await supervise(
                proc, proc.communicate(), is_interrupted=caps.is_interrupted,
                timeout_seconds=t.timeout_seconds, poll_interval=self._poll,
            )
        except ProcessTimeout as exc:
            raise ReviewAborted(f"{t.name}: '{t.command}' timed out after {t.timeout_seconds:g}s") from exc
        return self._stdout(proc.returncode, stdout, stderr)

    def _stdout(self, code: int | None, stdout: bytes, stderr: bytes) -> str:
        t = self._template
        if code != 0:
            detail = stderr.decode("utf-8", "replace").strip()[:_MAX_STDERR_CHARS] or "(no stderr)"
            raise ReviewAborted(f"{t.name}: '{t.command}' exited with code {code}: {detail}")
        return stdout.decode("utf-8", "replace")

    def format_review(self, review: Mapping[str, Any]) -> str:
        settings = f"cli={self._template.command}"
        mode = review.get("cli_terminal")
        if not mode:
            return render_review(review, executor_id=self._template.name, settings=settings)
        lines = render_review(review, executor_id=self._template.name, settings=f"{settings} terminal={mode}").split("\n")
        if mode == "fallback":
            lines.insert(1, f"Terminal fallback: {review.get('cli_terminal_fallback')}; ran as a subprocess")
        elif review.get("cli_terminals"):
            lines.insert(1, f"Orca terminal: {', '.join(review['cli_terminals'])}")
        return "\n".join(lines)


def extract_verdict(stdout: str) -> object:
    # Accepts a bare JSON object, a ```json block, or a {"result": "<text holding either>"} envelope.
    text = stdout.strip()
    if not text:
        raise ReviewAborted("the reviewer printed nothing")
    found = _json_in(text)
    if isinstance(found, dict) and "verdict" not in found:
        envelope = cast(dict[str, Any], found)
        structured = envelope.get("structured_output")
        if isinstance(structured, dict):
            return cast(dict[str, Any], structured)
        result = envelope.get("result")
        if isinstance(result, str):
            inner = _json_in(result.strip())
            if inner is None:
                raise ReviewAborted("the reviewer's result envelope holds no JSON object")
            return inner
    if found is None:
        raise ReviewAborted("the reviewer's output is neither a JSON object nor a ```json block")
    return found


def _json_in(text: str) -> object | None:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # The last parsable fenced block wins: a model may quote the schema before its answer.
    for block in reversed(_FENCE.findall(text)):
        try:
            return json.loads(block)
        except json.JSONDecodeError:
            continue
    return None
