from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import re
import shlex
import stat
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore, TerminalBinding
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Silent, Suspend, new_id
from agent_runtime.core.ports import RunCapabilities

_log = logging.getLogger(__name__)
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
UNCONFIRMED_CHECKPOINT = json.dumps({"orca_continuation": "unconfirmed"})
_SUBMIT_WAIT_SECONDS = "10"
_RESUME_READY_MS = "60000"


async def _orca(*args: str, cwd: Path) -> dict[str, Any]:
    proc = await asyncio.create_subprocess_exec(
        "orca", *args, "--json", cwd=cwd,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=90)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    if proc.returncode:
        raise RuntimeError((stderr or stdout).decode(errors="replace")[:2000])
    reply = json.loads(stdout)
    if not reply.get("ok") or not isinstance(reply.get("result"), dict):
        raise ValueError("Missing Orca command result")
    return reply["result"]


def _result(path: Path, session_id: str) -> RunOutcome | None:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("Invalid result file")
        data = stream.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError("Orca result exceeds 1 MiB")
    body = json.loads(data)
    match body:
        case {"outcome": "silent"}:
            return Silent()
        case {"outcome": "emit", "messages": list(messages)} if messages and all(isinstance(m, str) for m in messages):
            return Emit(messages=tuple(messages), session_id=session_id)
        case {"outcome": "suspend", "prompt": str(prompt)}:
            return Suspend(request_id=str(body.get("request_id") or new_id("req")), prompt=prompt,
                           session_checkpoint=str(body.get("session_checkpoint", "{}")), session_id=session_id)
        case _:
            raise ValueError("Invalid Orca run outcome")


def default_transcript_roots(extra: Sequence[str] = ()) -> list[Path]:
    candidates = [*extra, os.environ.get("CLAUDE_CONFIG_DIR", ""), os.environ.get("ORCA_CLAUDE_CONFIG_DIR", ""),
                  str(Path.home() / ".claude")]
    roots: list[Path] = []
    for raw in candidates:
        if raw and Path(raw).expanduser() not in roots:
            roots.append(Path(raw).expanduser())
    return roots


def find_native_session(roots: Sequence[Path], marker: str, since: float, prefer: str | None) -> str | None:
    # The Run's unique marker is in the delivered prompt, so the transcript holding it is the conversation
    # that received this Run. Zero or several candidates mean the binding cannot be confirmed.
    needle = marker.encode()
    hits: set[str] = set()
    for root in roots:
        for path in root.glob("projects/*/*.jsonl"):
            if not _UUID_RE.fullmatch(path.stem):
                continue
            try:
                if path.stat().st_mtime < since or needle not in path.read_bytes():
                    continue
            except OSError:
                continue
            hits.add(path.stem)
    if prefer in hits:
        return prefer
    return next(iter(hits)) if len(hits) == 1 else None


class ContinuationUnconfirmed(Exception):
    pass


@dataclass(frozen=True, slots=True)
class _Plan:
    mode: Literal["launch", "reuse", "resume"]
    binding: TerminalBinding | None = None
    native: str | None = None


def _approval(events: Sequence[Event], request_id: str) -> bool | None:
    for evt in events:
        if evt.payload.get("approval_request_id") == request_id:
            return evt.payload.get("approved") is True
    return None


def _with_native(outcome: RunOutcome, native: str | None) -> RunOutcome:
    if native is None:
        return outcome
    return dataclasses.replace(outcome, executor_session_id=native)


class OrcaVisibleRunner:
    def __init__(
        self,
        *,
        bindings: SQLiteTerminalBindingStore | None = None,
        transcript_roots: Sequence[Path] | None = None,
        poll_interval: float = 0.25,
    ) -> None:
        self._bindings = bindings
        self._roots = list(transcript_roots) if transcript_roots is not None else None
        self._poll_interval = poll_interval

    async def run(
        self,
        *,
        executor_id: str,
        agent: str,
        command: str,
        workdir: Path,
        prompt: str,
        session: SessionRef | None,
        session_id: str,
        events: Sequence[Event],
        caps: RunCapabilities,
        timeout: float,
        transcript_dirs: Sequence[str] = (),
    ) -> RunOutcome:
        decision = caps.permissions.check("orca.agent.launch", {"agent": agent, "workdir": str(workdir), "prompt": prompt})
        if decision.action == "block":
            return Emit(messages=(decision.reason,), session_id=session_id)
        if decision.action == "defer":
            return Suspend(request_id=new_id("req"), prompt=decision.reason, session_checkpoint="{}", session_id=session_id)

        approved_fresh = False
        if session is not None and session.pending_request_id and session.checkpoint == UNCONFIRMED_CHECKPOINT:
            answer = _approval(events, session.pending_request_id)
            if answer is False:
                return Emit(messages=("Continuation declined; no new conversation was started.",), session_id=session_id)
            approved_fresh = answer is True
        try:
            plan = _Plan("launch") if approved_fresh else await self._plan(executor_id, agent, workdir, session)
        except ContinuationUnconfirmed as exc:
            return self._unconfirmed(session_id, str(exc))
        roots = self._roots if self._roots is not None else default_transcript_roots(transcript_dirs)
        return await self._execute(plan, executor_id, agent, command, workdir, prompt, session_id, caps, timeout, roots)

    async def _plan(self, executor_id: str, agent: str, workdir: Path, session: SessionRef | None) -> _Plan:
        prior = session is not None and session.executor_id == executor_id
        binding: TerminalBinding | None = None
        if self._bindings is not None and session is not None:
            binding = self._bindings.get(session.session_id)
            if binding is None and prior and session.executor_session_id:
                binding = self._bindings.find_by_native(executor_id, session.executor_session_id)
            if binding is not None and binding.executor_id != executor_id:
                binding = None
        # The worker records the executor on a session before its first Run, so only a missing binding
        # (sessions from before bindings existed carry a marker row) identifies a new conversation.
        if binding is None and not (prior and session is not None and session.executor_session_id):
            return _Plan("launch")
        if binding is not None and binding.workdir and binding.workdir != str(workdir):
            raise ContinuationUnconfirmed(
                f"the bound conversation ran in {binding.workdir} but this Run resolved {workdir}"
            )
        if binding is not None and binding.handle and await self._live(binding, agent, workdir):
            return _Plan("reuse", binding)
        native = (session.executor_session_id if prior and session is not None else None) or (
            binding.native_session_id if binding is not None else None
        )
        if native:
            return _Plan("resume", binding, native)
        where = f"terminal {binding.handle} is closed" if binding is not None and binding.handle else "no terminal was recorded"
        raise ContinuationUnconfirmed(f"{where} and no native session ID is known")

    async def _live(self, binding: TerminalBinding, agent: str, workdir: Path) -> bool:
        # `terminal list` holds only running terminals; `show` still answers for closed handles, and its
        # `orphaned` flag only means no visible pane has adopted the terminal yet.
        try:
            listed = (await _orca("terminal", "list", "--worktree", f"path:{workdir}", "--limit", "500",
                                  cwd=workdir)).get("terminals", [])
        except (OSError, TimeoutError, ValueError, RuntimeError):
            return False
        terms = [cast(dict[str, Any], t) for t in cast(list[object], listed) if isinstance(t, dict)]
        term = next((t for t in terms if t.get("handle") == binding.handle), None)
        # A new process incarnation behind the same handle is a different conversation.
        return (
            term is not None
            and term.get("incarnationId") == binding.incarnation_id
            and term.get("agentIdentity") == agent
            and term.get("connected") is not False
            and term.get("writable") is not False
        )

    def _unconfirmed(self, session_id: str, detail: str) -> Suspend:
        return Suspend(
            request_id=new_id("req"),
            prompt=(
                f"Cannot confirm the agent conversation bound to session {session_id}: {detail}. "
                "Nothing was sent. Approve to start a new conversation, or reject to skip this Run."
            ),
            session_checkpoint=UNCONFIRMED_CHECKPOINT,
            session_id=session_id,
        )

    def _save(self, binding: TerminalBinding) -> None:
        if self._bindings is not None:
            self._bindings.save(binding)

    async def _execute(
        self,
        plan: _Plan,
        executor_id: str,
        agent: str,
        command: str,
        workdir: Path,
        prompt: str,
        session_id: str,
        caps: RunCapabilities,
        timeout: float,
        roots: Sequence[Path],
    ) -> RunOutcome:
        with tempfile.TemporaryDirectory(prefix="hibernaut-result-") as directory:
            path = Path(directory) / "result.json"
            marker = Path(directory).name
            prompt += (
                "\n\n## Runtime handoff\nShow progress and the final summary in this terminal. "
                "As the final action, write a UTF-8 JSON object to " + str(path.with_suffix(".tmp")) +
                " and atomically rename that file to " + str(path) + ". "
                'Use {"outcome":"emit","messages":["your summary"]}, {"outcome":"silent"}, '
                'or {"outcome":"suspend","prompt":"question","session_checkpoint":"context"}.'
            )
            binding = plan.binding or TerminalBinding(
                session_id=session_id, executor_id=executor_id, agent=agent, workdir=str(workdir)
            )
            binding = dataclasses.replace(binding, session_id=session_id, native_session_id=plan.native or binding.native_session_id)
            handle: str | None = None
            completed = False
            started = time.time() - 1
            try:
                if plan.mode == "launch":
                    launch = asyncio.create_task(_orca(
                        "agent", "launch", "--agent", agent, "--worktree", f"path:{workdir}",
                        "--surface", "terminal", "--prompt", prompt, cwd=workdir))
                    receipt: dict[str, Any] = {}
                    # Cancellation may race the launch receipt. Recover its handle before releasing the workspace.
                    try:
                        receipt = await asyncio.shield(launch)
                    finally:
                        if not launch.cancelled():
                            receipt = await launch
                            outcome = receipt.get("outcome", {})
                            if outcome.get("kind") == "terminal" and isinstance(outcome.get("handle"), str):
                                handle = outcome["handle"]
                    if not handle:
                        raise ValueError("Orca did not return a terminal handle")
                    binding = dataclasses.replace(binding, handle=handle, incarnation_id=None, native_session_id=None)
                    self._save(binding)
                    if receipt.get("prompt", {}).get("outcome") not in (None, "handed-to-terminal"):
                        raise ValueError("Orca did not deliver the task prompt")
                    binding = dataclasses.replace(binding, incarnation_id=await self._incarnation(handle, workdir))
                    self._save(binding)
                else:
                    if plan.mode == "resume":
                        binding = await self._resume(binding, command, agent, workdir)
                    await self._send(binding, prompt, workdir)
                    # Only a terminal that took this Run's prompt is closed if the Run does not finish.
                    handle = binding.handle
                async with asyncio.timeout(timeout):
                    while True:
                        if caps.is_interrupted():
                            raise asyncio.CancelledError
                        result = _result(path, session_id)
                        if result is not None:
                            completed = True
                            native = await asyncio.to_thread(
                                find_native_session, roots, marker, started, binding.native_session_id
                            )
                            if native is None:
                                _log.warning("Session %s: no transcript confirms the native session of %s", session_id, handle)
                            binding = dataclasses.replace(binding, native_session_id=native)
                            self._save(binding)
                            return _with_native(result, native)
                        await asyncio.sleep(self._poll_interval)
            except ContinuationUnconfirmed as exc:
                return self._unconfirmed(session_id, str(exc))
            except (OSError, TimeoutError, ValueError, RuntimeError) as exc:
                # A lost receipt can still mean the agent started. Suspend instead of delivering it twice.
                detail = f"Check Orca in {workdir} ({handle or 'launch receipt unavailable'}): {type(exc).__name__} {exc}"
                return Suspend(request_id=new_id("req"), prompt=detail, session_checkpoint="{}", session_id=session_id)
            finally:
                if handle and not completed:
                    # Keep the native ID so the next Run resumes the conversation this terminal held.
                    native = await asyncio.to_thread(
                        find_native_session, roots, marker, started, binding.native_session_id
                    )
                    self._save(dataclasses.replace(binding, handle=None, incarnation_id=None, native_session_id=native))
                    try:
                        await _orca("terminal", "close", "--terminal", handle, cwd=workdir)
                    except (OSError, TimeoutError, ValueError, RuntimeError):
                        _log.exception("Could not close Orca terminal %s", handle)

    async def _incarnation(self, handle: str, workdir: Path) -> str | None:
        try:
            show = (await _orca("terminal", "show", "--terminal", handle, cwd=workdir)).get("terminal", {})
        except (OSError, TimeoutError, ValueError, RuntimeError):
            return None
        value = show.get("incarnationId")
        return value if isinstance(value, str) else None

    async def _resume(
        self, binding: TerminalBinding, command: str, agent: str, workdir: Path
    ) -> TerminalBinding:
        native = binding.native_session_id
        assert native is not None
        if not _UUID_RE.fullmatch(native):
            raise ContinuationUnconfirmed(f"native session ID '{native}' is not a Claude Code session ID")
        created = (await _orca("terminal", "create", "--worktree", f"path:{workdir}", "--title", "hibernaut resume",
                               "--command", shlex.join([command, "--resume", native]), cwd=workdir)).get("terminal", {})
        handle = created.get("handle")
        if not isinstance(handle, str):
            raise ContinuationUnconfirmed("Orca returned no terminal for the resumed conversation")
        try:
            await _orca("terminal", "wait", "--terminal", handle, "--for", "tui-idle",
                        "--timeout-ms", _RESUME_READY_MS, cwd=workdir)
            show = (await _orca("terminal", "show", "--terminal", handle, cwd=workdir)).get("terminal", {})
        except (OSError, TimeoutError, ValueError, RuntimeError) as exc:
            await _close_quietly(handle, workdir)
            raise ContinuationUnconfirmed(f"resuming {native} did not reach a ready agent: {exc}") from exc
        if show.get("agentIdentity") != agent:
            await _close_quietly(handle, workdir)
            raise ContinuationUnconfirmed(f"resuming {native} did not start agent '{agent}'")
        incarnation = show.get("incarnationId") or created.get("incarnationId")
        binding = dataclasses.replace(binding, handle=handle,
                                      incarnation_id=incarnation if isinstance(incarnation, str) else None)
        self._save(binding)
        return binding

    async def _send(self, binding: TerminalBinding, prompt: str, workdir: Path) -> None:
        assert binding.handle is not None
        sent = (await _orca("terminal", "send", "--terminal", binding.handle, "--text", prompt, "--enter",
                            "--wait-submit", _SUBMIT_WAIT_SECONDS, cwd=workdir)).get("send", {})
        incarnation = sent.get("prompt", {}).get("processIncarnation")
        if sent.get("accepted") is not True:
            raise ContinuationUnconfirmed(f"terminal {binding.handle} refused the prompt")
        if binding.incarnation_id and incarnation and incarnation != binding.incarnation_id:
            raise ValueError(f"terminal {binding.handle} changed process before delivery was confirmed")


async def _close_quietly(handle: str, workdir: Path) -> None:
    try:
        await _orca("terminal", "close", "--terminal", handle, cwd=workdir)
    except (OSError, TimeoutError, ValueError, RuntimeError):
        _log.exception("Could not close Orca terminal %s", handle)
