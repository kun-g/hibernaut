from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import os
import re
import shlex
import shutil
import stat
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from agent_runtime.adapters.executor_cli.claude_code import (
    PROBE_ROLE_ARGS,
    ClaudeCodeError,
    ClaudeCodeProfile,
    env_overrides,
    harness_args,
    interactive_args,
)
from agent_runtime.adapters.executor_cli.process import ProcessTimeout
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore, TerminalBinding
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Silent, Suspend, new_id
from agent_runtime.core.ports import RunCapabilities

_log = logging.getLogger(__name__)
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
UNCONFIRMED_CHECKPOINT = json.dumps({"orca_continuation": "unconfirmed"})
_SUBMIT_WAIT_SECONDS = "10"
_RESUME_READY_MS = "60000"
ROLE_AGENT = "claude"
GATE_SCRIPT = Path(__file__).with_name("claude_gate.py")
CLI_TERMINAL_SCRIPT = Path(__file__).with_name("cli_terminal.py")
HANDOFF_SCRIPT = Path(__file__).with_name("handoff_mcp.py")
HANDOFF_SERVER = "hibernaut_handoff"
HANDOFF_TOOLS = (f"mcp__{HANDOFF_SERVER}__get_task", f"mcp__{HANDOFF_SERVER}__submit_result")
_PROBE_TIMEOUT_SECONDS = 120
_WRAPPER_DEADLINE_MARGIN_SECONDS = 60
# One lease file per gate terminal: the gate ends its agent once the file is gone, and Hibernaut removes it whenever it
# closes that terminal, so a close Orca reports but never delivers still ends the agent.
LEASE_ROOT = Path(tempfile.gettempdir()) / "hibernaut-terminal-leases"
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]")
_ORCA_ERRORS = (OSError, TimeoutError, ValueError, RuntimeError, KeyError)


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


@dataclass(frozen=True, slots=True)
class RoleReply:
    data: dict[str, Any]
    native_session_id: str
    handle: str


def _read_json(path: Path) -> bytes | None:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError(f"{path} is not a regular file")
        data = stream.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError(f"{path} exceeds 1 MiB")
    return data


def _handoff(prompt: str, turn: str) -> str:
    return (
        f"{prompt}\n\n## Result handoff\n"
        "A person may watch this terminal and answer questions here; keep progress visible. As the final action, "
        f"call the {HANDOFF_TOOLS[1]} tool with turn \"{turn}\" and your result; that call is the only way the "
        "result is read, and its input schema is the result format. If it rejects the result, fix it and call again."
    )


def _instruction(turn: str) -> str:
    return (
        f"Call the {HANDOFF_TOOLS[0]} tool with turn \"{turn}\" and carry out the task it returns. "
        f"Finish by calling {HANDOFF_TOOLS[1]} with turn \"{turn}\" and your result."
    )


def with_handoff(role_args: Sequence[str], root: Path) -> list[str]:
    # The launcher's handoff server is the agent's only channel for the task and the result, so no directory
    # outside the worktree is added (that opens a trust dialog) and no file-writing permission is needed.
    args = list(role_args)
    if "--allowedTools" in args:
        i = args.index("--allowedTools") + 1
        args[i] = ",".join((args[i], *HANDOFF_TOOLS))
    else:
        args += ["--allowedTools", ",".join(HANDOFF_TOOLS)]
    server = {"type": "stdio", "command": sys.executable, "args": ["-I", str(HANDOFF_SCRIPT), str(root)]}
    return [*args, "--mcp-config", json.dumps({"mcpServers": {HANDOFF_SERVER: server}})]


@dataclass(frozen=True, slots=True)
class TerminalUnavailable:
    # An interactive role with no terminal backend for this inbox: its runs stop with this reason, never headless.
    reason: str


def terminal_title(executor_id: str, role: str, subject: str) -> str:
    who = executor_id if executor_id == role else f"{executor_id} ({role})"
    return f"{who} · {subject}"


class OrcaVisibleRunner:
    def __init__(
        self,
        *,
        bindings: SQLiteTerminalBindingStore | None = None,
        transcript_roots: Sequence[Path] | None = None,
        poll_interval: float = 0.25,
        handoff_root: Path | None = None,
        liveness_interval: float = 5.0,
    ) -> None:
        self._bindings = bindings
        self._roots = list(transcript_roots) if transcript_roots is not None else None
        self._poll_interval = poll_interval
        self._handoff_root = handoff_root or Path(tempfile.gettempdir()) / "hibernaut-handoff"
        self._liveness_interval = liveness_interval

    async def converse(
        self,
        profile: ClaudeCodeProfile,
        *,
        title: str,
        workdir: Path,
        prompt: str,
        schema: Mapping[str, Any],
        role_args: Sequence[str],
        is_interrupted: Callable[[], bool],
        session_id: str | None = None,
        native: str | None = None,
    ) -> RoleReply:
        # One turn of a role conversation in a visible Orca terminal. With session_id the conversation bound to
        # that session continues (live terminal, else --resume); without it every call starts a fresh one.
        workdir = Path(os.path.realpath(workdir))
        binding: TerminalBinding | None = None
        if session_id is not None and self._bindings is not None:
            binding = self._bindings.get(session_id)
            if binding is not None and (binding.executor_id != profile.name or binding.workdir != str(workdir)):
                binding = None
        if session_id is not None:
            native = native or (binding.native_session_id if binding is not None else None)
        else:
            native = None
        root = self._handoff_root / (_SAFE_NAME_RE.sub("_", session_id) if session_id else uuid.uuid4().hex)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        root = Path(os.path.realpath(root))
        (root / "schema.json").write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
        turn = Path(tempfile.mkdtemp(prefix="turn-", dir=root))
        result_path = turn / "result.json"
        (turn / "prompt.md").write_text(_handoff(prompt, turn.name), encoding="utf-8")
        instruction = _instruction(turn.name)
        handle: str | None = None
        completed = False
        deadline = asyncio.timeout(profile.timeout_seconds)
        try:
            async with deadline:
                if binding is not None and binding.handle and await self._live(binding, ROLE_AGENT, workdir):
                    handle = binding.handle
                    await self._send(binding, instruction, workdir)
                    native = binding.native_session_id or native
                else:
                    fresh = native is None
                    native = native or str(uuid.uuid4())
                    continuation = ("--session-id", native) if fresh else ("--resume", native)
                    args = [*with_handoff(role_args, root), *continuation, "--name", title]
                    handle, incarnation = await self._start(profile, title, workdir, turn, args, instruction)
                    if session_id is not None:
                        binding = TerminalBinding(session_id=session_id, executor_id=profile.name, agent=ROLE_AGENT,
                                                  workdir=str(workdir), handle=handle, incarnation_id=incarnation,
                                                  native_session_id=native)
                        self._save(binding)
                    await self._await_gate(turn / "gate-status.json", handle, workdir, is_interrupted)
                data = await self._await_result(result_path, handle, workdir, is_interrupted)
                completed = True
                return RoleReply(data=data, native_session_id=native, handle=handle)
        except TimeoutError as exc:
            if not deadline.expired():
                raise ClaudeCodeError(f"Orca did not answer in {workdir}: {exc}") from exc
            raise ClaudeCodeError(
                f"timed out after {profile.timeout_seconds}s in Orca terminal {handle or '(not started)'}"
            ) from exc
        except ContinuationUnconfirmed as exc:
            raise ClaudeCodeError(str(exc)) from exc
        except _ORCA_ERRORS as exc:
            raise ClaudeCodeError(f"Orca terminal {handle or '(not started)'} in {workdir}: {type(exc).__name__} {exc}") from exc
        finally:
            shutil.rmtree(turn, ignore_errors=True)
            if session_id is None:
                shutil.rmtree(root, ignore_errors=True)
            if handle and not completed:
                # Close the run's terminal; the native ID stays bound so the next Run resumes this conversation.
                if session_id is not None and binding is not None:
                    self._save(dataclasses.replace(binding, handle=None, incarnation_id=None, native_session_id=native))
                await _close_quietly(handle, workdir)

    async def release(self, session_id: str, workdir: Path) -> str:
        # Ends the visible terminal of a conversation whose work is delivered. The binding keeps the native ID, so a
        # later event resumes the conversation with --resume; the worktree is untouched.
        binding = self._bindings.get(session_id) if self._bindings is not None else None
        if binding is None or not binding.handle:
            return "no live terminal is bound to this session"
        workdir = Path(os.path.realpath(workdir))
        _drop_lease(binding.handle)
        try:
            await _orca("terminal", "close", "--terminal", binding.handle, cwd=workdir)
        except _ORCA_ERRORS as exc:
            # The binding keeps the handle: a live terminal must not look closed, or a resume would run beside it.
            if await _still_listed(binding.handle, workdir):
                return f"could not close {binding.handle}: {type(exc).__name__} {exc}"
        self._save(dataclasses.replace(binding, handle=None, incarnation_id=None))
        return f"closed {binding.handle}"

    async def _start(
        self, profile: ClaudeCodeProfile, title: str, workdir: Path, turn: Path, args: Sequence[str], instruction: str
    ) -> tuple[str, str | None]:
        launcher = profile.launcher
        unset, values = env_overrides(profile)
        spec = {
            "title": title,
            "profile": {"name": profile.name, "launcher": launcher.name, "launcher_command": launcher.command,
                        "billing": profile.billing, "model": profile.model, "effort": profile.effort,
                        "credential_proxy_url": profile.credential_proxy_url},
            "unset": list(unset),
            "set": values,
            "probe": [launcher.command, *harness_args(profile, permission_mode="dontAsk", role_args=PROBE_ROLE_ARGS)],
            "probe_timeout": _PROBE_TIMEOUT_SECONDS,
            # `--` ends the options: --allowedTools and --mcp-config take several values and would swallow it.
            "exec": [launcher.command, *interactive_args(profile, permission_mode="dontAsk", role_args=args), "--",
                     instruction],
            "status": str(turn / "gate-status.json"),
        }
        lease = _new_lease()
        spec["lease"] = str(lease)
        spec_path = turn / "gate.json"
        fd = os.open(spec_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(spec, stream)
        # The gate exits with the agent, and `exec` ends the terminal with it, so an exited agent is a closed terminal.
        command = shlex.join(["exec", sys.executable, "-I", str(GATE_SCRIPT), str(spec_path)])
        try:
            handle, incarnation = await _create_terminal(workdir, title, command)
        except BaseException:
            lease.unlink(missing_ok=True)
            raise
        _bind_lease(handle, lease)
        return handle, incarnation

    async def _poll(
        self, read: Callable[[], Any], handle: str, workdir: Path, is_interrupted: Callable[[], bool], waiting_for: str
    ) -> Any:
        try:
            return await _await_file(read, handle, workdir, is_interrupted,
                                     poll_interval=self._poll_interval, liveness_interval=self._liveness_interval)
        except _TerminalGone:
            raise ClaudeCodeError(f"Orca terminal {handle} exited before {waiting_for}") from None

    async def _await_gate(self, path: Path, handle: str, workdir: Path, is_interrupted: Callable[[], bool]) -> None:
        def read() -> dict[str, Any] | None:
            raw = _read_json(path)
            return cast(dict[str, Any], json.loads(raw)) if raw is not None else None

        status = await self._poll(read, handle, workdir, is_interrupted, "its session check finished")
        if status.get("ok") is not True:
            raise ClaudeCodeError(str(status.get("error") or "the session check failed"))

    async def _await_result(
        self, path: Path, handle: str, workdir: Path, is_interrupted: Callable[[], bool]
    ) -> dict[str, Any]:
        invalid: list[bytes] = []

        def read() -> dict[str, Any] | None:
            raw = _read_json(path)
            if raw is None:
                return None
            try:
                body: object = json.loads(raw)
            except json.JSONDecodeError:
                # A write may still be in progress; the same bytes twice are a final, invalid result.
                if invalid and invalid[-1] == raw:
                    raise ClaudeCodeError(f"the agent wrote invalid JSON to {path}") from None
                invalid.append(raw)
                return None
            if not isinstance(body, dict):
                raise ClaudeCodeError(f"the agent's result in {path} is not a JSON object")
            return cast(dict[str, Any], body)

        return await self._poll(read, handle, workdir, is_interrupted, "it returned its result")

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
            return Emit(messages=(decision.reason,), session_id=session_id, status="refused")
        if decision.action == "defer":
            return Suspend(request_id=new_id("req"), prompt=decision.reason, session_checkpoint="{}", session_id=session_id)

        approved_fresh = False
        if session is not None and session.pending_request_id and session.checkpoint == UNCONFIRMED_CHECKPOINT:
            answer = _approval(events, session.pending_request_id)
            if answer is False:
                return Emit(messages=("Continuation declined; no new conversation was started.",), session_id=session_id,
                            status="stopped")
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


def _new_lease() -> Path:
    LEASE_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    lease = LEASE_ROOT / f"lease-{uuid.uuid4().hex}"
    lease.touch(mode=0o600, exist_ok=False)
    return lease


def _lease_index(handle: str) -> Path:
    return LEASE_ROOT / f"handle-{_SAFE_NAME_RE.sub('_', handle)}"


def _bind_lease(handle: str, lease: Path) -> None:
    _lease_index(handle).write_text(lease.name, encoding="utf-8")


def _drop_lease(handle: str) -> None:
    index = _lease_index(handle)
    try:
        name = index.read_text(encoding="utf-8").strip()
    except OSError:
        return
    if name.startswith("lease-") and _SAFE_NAME_RE.search(name) is None:
        (LEASE_ROOT / name).unlink(missing_ok=True)
    index.unlink(missing_ok=True)


async def _still_listed(handle: str, workdir: Path) -> bool:
    # A terminal whose command already ended (a finished CLI run ends its own) counts as closed; when Orca cannot
    # answer, it does not.
    try:
        return await _terminal_alive(handle, workdir)
    except _ORCA_ERRORS:
        return True


async def _close_quietly(handle: str, workdir: Path) -> bool:
    _drop_lease(handle)
    try:
        await _orca("terminal", "close", "--terminal", handle, cwd=workdir)
    except _ORCA_ERRORS:
        if not await _still_listed(handle, workdir):
            _log.info("Orca terminal %s had already ended", handle)
            return True
        _log.exception("Could not close Orca terminal %s", handle)
        return False
    return True


async def close_terminals(handles: Sequence[str], workdir: Path) -> str:
    workdir = Path(os.path.realpath(workdir))
    closed = [h for h in handles if await _close_quietly(h, workdir)]
    failed = [h for h in handles if h not in closed]
    parts = [f"closed {', '.join(closed)}"] if closed else []
    if failed:
        parts.append(f"could not close {', '.join(failed)}")
    return "; ".join(parts)


async def _create_terminal(workdir: Path, title: str, command: str) -> tuple[str, str | None]:
    create = asyncio.create_task(_orca("terminal", "create", "--worktree", f"path:{workdir}", "--title", title,
                                       "--command", command, cwd=workdir))
    try:
        created = (await asyncio.shield(create)).get("terminal", {})
    except asyncio.CancelledError:
        # Cancellation may race the create receipt. Recover its handle so the terminal is closed, not leaked.
        with contextlib.suppress(*_ORCA_ERRORS):
            late = (await create).get("terminal", {}).get("handle")
            if isinstance(late, str):
                await _close_quietly(late, workdir)
        raise
    handle = created.get("handle")
    if not isinstance(handle, str):
        raise ValueError("Orca returned no terminal handle")
    incarnation = created.get("incarnationId")
    return handle, incarnation if isinstance(incarnation, str) else None


async def _terminal_alive(handle: str, workdir: Path) -> bool:
    listed = (await _orca("terminal", "list", "--worktree", f"path:{workdir}", "--limit", "500",
                          cwd=workdir)).get("terminals", [])
    return any(isinstance(t, dict) and cast(dict[str, Any], t).get("handle") == handle
               for t in cast(list[object], listed))


class _TerminalGone(Exception):
    pass


async def _await_file(
    read: Callable[[], Any], handle: str, workdir: Path, is_interrupted: Callable[[], bool], *,
    poll_interval: float, liveness_interval: float,
) -> Any:
    # Polls read() until it returns a value; raises _TerminalGone when the terminal ended without one.
    loop = asyncio.get_running_loop()
    next_check = loop.time() + liveness_interval
    while True:
        if is_interrupted():
            raise asyncio.CancelledError("visible run interrupted by a new event")
        value = read()
        if value is not None:
            return value
        if loop.time() >= next_check:
            if not await _terminal_alive(handle, workdir):
                # The file may have landed just before the terminal ended.
                value = read()
                if value is not None:
                    return value
                raise _TerminalGone(handle)
            next_check = loop.time() + liveness_interval
        await asyncio.sleep(poll_interval)


class TerminalStartFailed(Exception):
    pass


class TerminalRunFailed(Exception):
    pass


@dataclass(frozen=True, slots=True)
class TerminalRun:
    handle: str
    stdout: bytes
    stderr: bytes
    # None when the command could not start; error then says why.
    exit_code: int | None
    error: str = ""


class OrcaCommandTerminal:
    # Runs one non-interactive command in a visible Orca terminal of the worktree. The terminal shows the output as
    # it comes and ends with the command; the result is read only from files the wrapper writes, never from the screen.
    def __init__(self, *, root: Path | None = None, poll_interval: float = 0.25, liveness_interval: float = 5.0) -> None:
        self._root = root or Path(tempfile.gettempdir()) / "hibernaut-cli-terminal"
        self._poll_interval = poll_interval
        self._liveness_interval = liveness_interval

    async def run(
        self,
        *,
        title: str,
        workdir: Path,
        argv: Sequence[str],
        env: Mapping[str, str],
        timeout_seconds: float,
        is_interrupted: Callable[[], bool],
    ) -> TerminalRun:
        # Raises TerminalStartFailed when no terminal opened (nothing ran), ProcessTimeout on the deadline,
        # TerminalRunFailed when the terminal ended early or Orca failed, CancelledError on interrupt.
        workdir = Path(os.path.realpath(workdir))
        self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
        run_dir = Path(tempfile.mkdtemp(prefix="run-", dir=self._root))
        files = {name: str(run_dir / name) for name in ("stdout", "stderr", "exit.json")}
        # The argv holds the prompt; it reaches the wrapper through this private file, not the command line.
        # The wrapper's own deadline outlasts this one, so it only fires when Hibernaut is no longer waiting.
        spec = {"title": title, "cwd": str(workdir), "argv": list(argv), "env": dict(env),
                "timeout_seconds": timeout_seconds + _WRAPPER_DEADLINE_MARGIN_SECONDS, **files}
        spec_path = run_dir / "spec.json"
        fd = os.open(spec_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(spec, stream)
        command = shlex.join(["exec", sys.executable, "-I", str(CLI_TERMINAL_SCRIPT), str(spec_path)])
        handle: str | None = None
        finished = False
        deadline = asyncio.timeout(timeout_seconds)
        try:
            try:
                handle, _ = await _create_terminal(workdir, title, command)
            except _ORCA_ERRORS as exc:
                raise TerminalStartFailed(f"{type(exc).__name__} {exc}".strip()) from exc
            exit_path = Path(files["exit.json"])

            def read() -> dict[str, Any] | None:
                raw = _read_json(exit_path)
                return cast(dict[str, Any], json.loads(raw)) if raw is not None else None

            try:
                async with deadline:
                    status = await _await_file(read, handle, workdir, is_interrupted, poll_interval=self._poll_interval,
                                               liveness_interval=self._liveness_interval)
            except _TerminalGone:
                raise TerminalRunFailed(f"Orca terminal {handle} was closed before '{argv[0]}' finished") from None
            except TimeoutError as exc:
                if deadline.expired():
                    raise ProcessTimeout(f"timed out after {timeout_seconds:g}s in Orca terminal {handle}") from exc
                raise TerminalRunFailed(f"Orca terminal {handle}: TimeoutError {exc}") from exc
            except _ORCA_ERRORS as exc:
                raise TerminalRunFailed(f"Orca terminal {handle}: {type(exc).__name__} {exc}") from exc
            finished = True
            code = status.get("exit")
            return TerminalRun(
                handle=handle,
                stdout=Path(files["stdout"]).read_bytes() if os.path.exists(files["stdout"]) else b"",
                stderr=Path(files["stderr"]).read_bytes() if os.path.exists(files["stderr"]) else b"",
                exit_code=code if isinstance(code, int) else None,
                error=str(status.get("error") or ""),
            )
        finally:
            if handle and not finished:
                await _close_quietly(handle, workdir)
            shutil.rmtree(run_dir, ignore_errors=True)
