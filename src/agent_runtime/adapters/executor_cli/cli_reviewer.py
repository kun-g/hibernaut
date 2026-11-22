from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from agent_runtime.adapters.executor_cli.executor import CLITemplate, render_cli_args
from agent_runtime.adapters.executor_cli.process import ProcessTimeout, supervise
from agent_runtime.adapters.executor_cli.reviewer import (
    REVIEW_LAUNCH_TOOL,
    REVIEW_SCHEMA,
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
_COMPACT_SCHEMA = json.dumps(REVIEW_SCHEMA, separators=(",", ":"))


class CLIReviewer:
    # Reviews through a plain [executors.<id>] CLI template. Nothing restricts its tools, so read-only is checked
    # afterwards: a review that changed the worktree is discarded.
    def __init__(self, *, template: CLITemplate, interrupt_poll_interval: float = 0.05) -> None:
        self._template = template
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
        before = await asyncio.to_thread(worktree_fingerprint, request.workdir)
        stdout = await self._run(prompt, request.workdir, caps)
        await self._unchanged(request.workdir, before)
        try:
            return validate_review(extract_verdict(stdout))
        except ReviewAborted as exc:
            # One retry with the reason: a model that wrapped or misshaped its JSON usually fixes it when told.
            retry = (f"{prompt}\n\n## Your previous output was rejected\n{exc}. Output exactly one JSON object that "
                     "matches the schema above, and nothing else.")
        stdout = await self._run(retry, request.workdir, caps)
        await self._unchanged(request.workdir, before)
        try:
            return validate_review(extract_verdict(stdout))
        except ReviewAborted as exc:
            raise ReviewAborted(f"{self._template.name} returned no valid verdict after one retry: {exc}") from exc

    async def _unchanged(self, workdir: Path, before: str) -> None:
        after = await asyncio.to_thread(worktree_fingerprint, workdir)
        if after != before:
            raise ReviewAborted(
                f"the working tree in {workdir} changed during the review by {self._template.name}; findings "
                "discarded, check the change and submit again"
            )

    async def _run(self, prompt: str, workdir: Path, caps: RunCapabilities) -> str:
        t = self._template
        try:
            proc = await asyncio.create_subprocess_exec(
                t.command, *render_cli_args(t.args, prompt=prompt, review_schema=_COMPACT_SCHEMA),
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
        if proc.returncode != 0:
            detail = stderr.decode("utf-8", "replace").strip()[:_MAX_STDERR_CHARS] or "(no stderr)"
            raise ReviewAborted(f"{t.name}: '{t.command}' exited with code {proc.returncode}: {detail}")
        return stdout.decode("utf-8", "replace")

    def format_review(self, review: Mapping[str, Any]) -> str:
        return render_review(review, executor_id=self._template.name, settings=f"cli={self._template.command}")


def extract_verdict(stdout: str) -> object:
    # Accepts a bare JSON object, a ```json block, or an envelope: {"structured_output" | "structuredOutput": {...}}
    # (the object wins) or {"result" | "text": "<text holding either>"} (grok --json-schema prints the latter pair).
    text = stdout.strip()
    if not text:
        raise ReviewAborted("the reviewer printed nothing")
    found = _json_in(text)
    if isinstance(found, dict) and "verdict" not in found:
        envelope = cast(dict[str, Any], found)
        for key in ("structured_output", "structuredOutput"):
            structured = envelope.get(key)
            if isinstance(structured, dict):
                return cast(dict[str, Any], structured)
        for key in ("result", "text"):
            body = envelope.get(key)
            if isinstance(body, str):
                inner = _json_in(body.strip())
                if inner is None:
                    raise ReviewAborted(f"the reviewer's {key} envelope holds no JSON object")
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
