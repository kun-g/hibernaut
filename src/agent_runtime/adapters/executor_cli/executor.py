from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from agent_runtime.adapters.executor_cli.orca import OrcaVisibleRunner
from agent_runtime.adapters.executor_cli.process import ProcessTimeout, last_text, supervise, target_events
from agent_runtime.adapters.executor_cli.worktree import OrcaWorktreeManager, WorkspaceIsolationError
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore
from agent_runtime.core.models import (
    Emit,
    Event,
    RunOutcome,
    SessionRef,
    Silent,
    Suspend,
    new_id,
)
from agent_runtime.core.ports import RunCapabilities


def _empty_env() -> dict[str, str]:
    return {}


@dataclass(frozen=True, slots=True)
class CLITemplate:
    name: str
    command: str
    args: tuple[str, ...] = ()
    resume_args: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=_empty_env)
    timeout_seconds: float = 300.0
    orca_agent: str | None = None
    # Only CLIReviewer reads it: "orca" runs the review in a visible Orca terminal of the worktree.
    terminal: Literal["none", "orca"] = "none"


CLI_TERMINALS: tuple[Literal["none", "orca"], ...] = ("none", "orca")

DEFAULT_CLI_TEMPLATES: dict[str, CLITemplate] = {
    "claude": CLITemplate(name="claude", command="claude", args=("-p",)),
    "cursor-agent": CLITemplate(name="cursor-agent", command="cursor-agent", args=("-p",)),
    "grok": CLITemplate(name="grok", command="grok", args=("-p",)),
    "dsh": CLITemplate(name="dsh", command="dsh", args=("-c",)),
}


_PLACEHOLDER = re.compile(r"\{(prompt|executor_session_id|review_schema)\}")


def render_cli_args(
    args: Sequence[str], *, prompt: str, executor_session_id: str = "", review_schema: str | None = None,
) -> list[str]:
    # {prompt} is substituted in place; without one the prompt is the last argument. One pass per token, so a
    # placeholder quoted inside the prompt stays as written. {review_schema} is filled only for a review; any
    # other run passes it through literally.
    values = {"prompt": prompt, "executor_session_id": executor_session_id}
    if review_schema is not None:
        values["review_schema"] = review_schema
    rendered: list[str] = []
    prompt_inserted = False
    for token in args:
        if "{prompt}" in token:
            prompt_inserted = True
        rendered.append(_PLACEHOLDER.sub(lambda m: values.get(m.group(1), m.group(0)), token))
    if not prompt_inserted:
        rendered.append(prompt)
    return rendered


class LocalCLIExecutor:
    def __init__(
        self,
        *,
        worktree_manager: OrcaWorktreeManager,
        inbox_checkout_map: Mapping[str, str | Path],
        templates: Mapping[str, CLITemplate] | None = None,
        default_executor: str = "claude",
        interrupt_poll_interval: float = 0.05,
        orca_workspace: bool = False,
        terminal_bindings: SQLiteTerminalBindingStore | None = None,
        visible_runner: OrcaVisibleRunner | None = None,
    ) -> None:
        self._orca_workspace = orca_workspace
        self._visible = visible_runner or OrcaVisibleRunner(bindings=terminal_bindings)
        self._worktree_manager = worktree_manager
        # Looked up per run: the configured map also answers inboxes matched by a project's pattern.
        self._inbox_checkout_map = inbox_checkout_map
        merged = dict(DEFAULT_CLI_TEMPLATES)
        if templates:
            merged.update(templates)
        self._templates = merged
        self._default_executor = default_executor
        self._interrupt_poll_interval = interrupt_poll_interval

    @property
    def executor_id(self) -> str:
        return self._default_executor

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        if not events:
            return Silent()

        inbox_id = events[0].inbox_id
        session_id = session.session_id if session is not None else new_id("sess")

        targets = target_events(events)
        explicit_workdir = self._extract_explicit_workdir(targets)
        work_key = (
            events[0].correlation_key
            or (session.correlation_key if session is not None else None)
            or f"task:{inbox_id}:{events[0].event_id}"
        )
        configured = self._inbox_checkout_map.get(inbox_id)
        primary_checkout = Path(configured).resolve() if configured is not None else None
        if explicit_workdir is None and primary_checkout is None:
            return Emit(
                messages=(f"[LocalCLIExecutor] No primary checkout configured for '{inbox_id}'",),
                session_id=session_id,
                status="failed",
            )

        try:
            workdir = await asyncio.to_thread(
                self._worktree_manager.resolve_workdir,
                work_key=work_key,
                primary_checkout=primary_checkout or Path(str(explicit_workdir)),
                explicit_workdir=explicit_workdir,
                inbox_id=inbox_id,
            )
        except WorkspaceIsolationError as exc:
            return Emit(messages=(f"[LocalCLIExecutor] refused: {exc}",), session_id=session_id, status="refused")

        executor_name = self._extract_executor(targets) or self._default_executor
        template = self._templates.get(executor_name)
        if template is None:
            return Emit(
                messages=(f"[LocalCLIExecutor] Unknown CLI template '{executor_name}'",),
                session_id=session_id,
                status="failed",
            )

        prompt = self._build_prompt(events, session, caps)
        if self._orca_workspace and template.orca_agent and explicit_workdir is None:
            config_dir = template.env.get("CLAUDE_CONFIG_DIR")
            return await self._visible.run(
                executor_id=template.name,
                agent=template.orca_agent,
                command=template.command,
                workdir=workdir,
                prompt=prompt,
                session=session,
                session_id=session_id,
                events=events,
                caps=caps,
                timeout=template.timeout_seconds,
                transcript_dirs=(config_dir,) if config_dir else (),
            )
        cli_args = self._render_cli_args(template, prompt=prompt, session=session)
        env = dict(os.environ)
        env.update(template.env)

        proc = await asyncio.create_subprocess_exec(
            template.command,
            *cli_args,
            cwd=str(workdir),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout_bytes, stderr_bytes, returncode = await self._wait_with_interrupt(
            proc,
            caps=caps,
            timeout_seconds=template.timeout_seconds,
        )

        stdout = stdout_bytes.decode("utf-8", errors="replace").strip()
        stderr = stderr_bytes.decode("utf-8", errors="replace").strip()

        if returncode != 0:
            detail = stderr or stdout or "unknown error"
            return Emit(
                messages=(f"[CLI:{template.name} exit={returncode}] {detail}",),
                session_id=session_id,
                status="failed",
            )

        return self._parse_outcome(stdout, session_id=session_id)

    def _render_cli_args(
        self,
        template: CLITemplate,
        *,
        prompt: str,
        session: SessionRef | None,
    ) -> list[str]:
        # Only resume natively when executor_id matches and executor_session_id is present.
        can_resume = (
            session is not None
            and session.executor_id == template.name
            and bool(session.executor_session_id)
            and bool(template.resume_args)
        )
        raw_args = template.resume_args if can_resume else template.args
        exec_sid = session.executor_session_id if (can_resume and session is not None) else ""
        return render_cli_args(raw_args, prompt=prompt, executor_session_id=exec_sid or "")

    async def _wait_with_interrupt(
        self,
        proc: asyncio.subprocess.Process,
        *,
        caps: RunCapabilities,
        timeout_seconds: float,
    ) -> tuple[bytes, bytes, int]:
        try:
            stdout, stderr = await supervise(
                proc,
                proc.communicate(),
                is_interrupted=caps.is_interrupted,
                timeout_seconds=timeout_seconds,
                poll_interval=self._interrupt_poll_interval,
            )
        except ProcessTimeout:
            return b"", f"CLI timed out after {timeout_seconds}s".encode("utf-8"), 124
        return stdout, stderr, int(proc.returncode or 0)

    def _extract_explicit_workdir(self, events: Sequence[Event]) -> str | None:
        return last_text(events, "explicit_workdir")

    def _extract_executor(self, events: Sequence[Event]) -> str | None:
        return last_text(events, "executor")

    def _build_prompt(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> str:
        baseline = caps.memory.load_baseline()
        parts: list[str] = []
        for name in ("instructions.md", "index.md", "reported.md"):
            text = baseline.get(name, "").strip()
            if text:
                parts.append(f"## {name}\n{text}")
        if session is not None and session.checkpoint:
            parts.append(f"## session_checkpoint\n{session.checkpoint}")
        serialized_events = [
            {
                "source": e.source,
                "external_id": e.external_id,
                "correlation_key": e.correlation_key,
                "payload": e.payload,
            }
            for e in events
        ]
        parts.append(f"## events\n{json.dumps(serialized_events, ensure_ascii=False)}")
        return "\n\n".join(parts)

    def _parse_outcome(self, stdout: str, *, session_id: str) -> RunOutcome:
        if not stdout:
            return Silent()
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            return Emit(messages=(stdout,), session_id=session_id)

        if isinstance(parsed, dict):
            raw_exec_sid = parsed.get("executor_session_id") or parsed.get("session_id")
            native_sid = str(raw_exec_sid) if isinstance(raw_exec_sid, str) and raw_exec_sid else None
            kind = str(parsed.get("outcome", "")).lower()
            if kind == "silent":
                return Silent(executor_session_id=native_sid)
            if kind == "suspend":
                return Suspend(
                    request_id=str(parsed.get("request_id") or new_id("req")),
                    prompt=str(parsed.get("prompt") or "需要人工确认"),
                    session_checkpoint=str(parsed.get("session_checkpoint") or "{}"),
                    session_id=session_id,
                    executor_session_id=native_sid,
                )
            if kind == "emit":
                msgs = parsed.get("messages")
                if isinstance(msgs, list) and msgs:
                    return Emit(
                        messages=tuple(str(m) for m in msgs),
                        session_id=session_id,
                        executor_session_id=native_sid,
                    )
                return Silent(executor_session_id=native_sid)
            # Support native Claude Code `--output-format json` envelope (`{"result": "...", "session_id": "..."}`).
            if isinstance(parsed.get("result"), str):
                text = parsed["result"].strip()
                if text:
                    return Emit(
                        messages=(text,),
                        session_id=session_id,
                        executor_session_id=native_sid,
                    )
                return Silent(executor_session_id=native_sid)

        return Emit(messages=(stdout,), session_id=session_id)

