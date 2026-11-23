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
from typing import TYPE_CHECKING, Any, Literal, cast

from agent_runtime.adapters.executor_cli.claude_code import (
    PROBE_ROLE_ARGS,
    ClaudeCodeError,
    ClaudeCodeProfile,
    env_overrides,
    harness_args,
    interactive_args,
)
from agent_runtime.adapters.executor_cli.peer_handoff import (
    PeerHandoff,
    parse_result,
    resend_message,
    task_message,
)
from agent_runtime.adapters.executor_cli.process import ProcessTimeout
from agent_runtime.adapters.executor_cli.run_state import TurnSlot, detached, parked
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore, TerminalBinding
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Silent, Suspend, new_id
from agent_runtime.core.ports import RunCapabilities

if TYPE_CHECKING:
    from ccpeer import Session

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
# A peer-handoff session replies with SendMessage and must start under ~/.claude, where the receiver is registered.
PEER_HANDOFF_TOOLS = ("SendMessage",)
PEER_UNSET_ENV = ("CLAUDE_CONFIG_DIR",)
_EXIT_TIMEOUT_SECONDS = 30.0
_PROBE_TIMEOUT_SECONDS = 120
_WRAPPER_DEADLINE_MARGIN_SECONDS = 60
# One lease file per gate terminal: the gate ends its agent once the file is gone, and Hibernaut removes it whenever it
# closes that terminal, so a close Orca reports but never delivers still ends the agent.
LEASE_ROOT = Path(tempfile.gettempdir()) / "hibernaut-terminal-leases"
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9_.-]")
_ORCA_ERRORS = (OSError, TimeoutError, ValueError, RuntimeError, KeyError)
_STALE_HANDLE = "terminal_handle_stale"


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


def with_peer_handoff(role_args: Sequence[str]) -> list[str]:
    args = list(role_args)
    if "--allowedTools" in args:
        i = args.index("--allowedTools") + 1
        have = args[i].split(",")
        args[i] = ",".join((*have, *(t for t in PEER_HANDOFF_TOOLS if t not in have)))
    else:
        args += ["--allowedTools", ",".join(PEER_HANDOFF_TOOLS)]
    return args


def _pid_alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


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
        peer: PeerHandoff | None = None,
        notify: Callable[[str, str], None] | None = None,
    ) -> None:
        self._bindings = bindings
        self._roots = list(transcript_roots) if transcript_roots is not None else None
        self._poll_interval = poll_interval
        self._handoff_root = handoff_root or Path(tempfile.gettempdir()) / "hibernaut-handoff"
        self._liveness_interval = liveness_interval
        # With peer set, role conversations get their tasks and return results as peer messages (handoff = "peer");
        # without it, through the launcher's handoff MCP server.
        self._peer = peer
        # Writes a notice for a person to the Outbox of an inbox (a held peer message waits for approval).
        self._notify = notify
        self._exit_timeout = peer.config.exit_timeout if peer is not None else _EXIT_TIMEOUT_SECONDS

    @property
    def handoff(self) -> str:
        return "peer" if self._peer is not None else "mcp"

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
        schema_name: str = "result",
        inbox_id: str | None = None,
        slot: TurnSlot | None = None,
        slot_label: str = "turn",
        slot_extra: Mapping[str, Any] | None = None,
    ) -> RoleReply:
        # One turn of a role conversation in a visible Orca terminal. With session_id the conversation bound to
        # that session continues (live terminal, else --resume); without it every call starts a fresh one.
        # With a slot (peer handoff only) the turn is recorded in the Run's directory and its wait can be detached;
        # a turn the slot recorded before a restart is waited on again, never sent twice.
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
        if self._peer is not None:
            return await self._converse_peer(
                self._peer, profile, title=title, workdir=workdir, prompt=prompt, schema=schema,
                schema_name=schema_name, role_args=role_args, is_interrupted=is_interrupted, session_id=session_id,
                native=native, binding=binding, inbox_id=inbox_id, slot=slot, slot_label=slot_label,
                slot_extra=slot_extra or {},
            )
        if binding is not None and binding.handle and binding.peer_socket:
            # A peer-handoff session has no handoff MCP server to fetch a turn from; it is ended and resumed with one.
            await _close_quietly(binding.handle, workdir)
            binding = dataclasses.replace(binding, handle=None, incarnation_id=None, peer_name=None, peer_pid=None,
                                          peer_socket=None, peer_attempt=None)
            self._save(binding)
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

    async def _converse_peer(
        self,
        peer: PeerHandoff,
        profile: ClaudeCodeProfile,
        *,
        title: str,
        workdir: Path,
        prompt: str,
        schema: Mapping[str, Any],
        schema_name: str,
        role_args: Sequence[str],
        is_interrupted: Callable[[], bool],
        session_id: str | None,
        native: str | None,
        binding: TerminalBinding | None,
        inbox_id: str | None,
        slot: TurnSlot | None = None,
        slot_label: str = "turn",
        slot_extra: Mapping[str, Any] | None = None,
    ) -> RoleReply:
        # The task goes to the session as a peer message and the result comes back through the receiver's inbox, so
        # nothing the session needs lives in a directory this process owns.
        await asyncio.to_thread(peer.receiver)
        saved = slot.take(slot_label) if slot is not None else None
        if saved is not None and slot is not None:
            picked = await self._pick_up(peer, profile, saved, slot, workdir=workdir, schema=schema,
                                         is_interrupted=is_interrupted, binding=binding, inbox_id=inbox_id)
            if picked is not None:
                return picked
        task_id = session_id or new_id("task")
        attempt = new_id("att")
        turns_root = slot.turns_dir if slot is not None else self._handoff_root
        turns_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        turn = Path(tempfile.mkdtemp(prefix="turn-", dir=turns_root))
        handle: str | None = None
        incarnation: str | None = None
        completed = False
        deadline = asyncio.timeout(profile.timeout_seconds)
        try:
            async with deadline:
                target: Session | None = None
                if binding is not None and binding.handle and await self._live(binding, ROLE_AGENT, workdir):
                    native = binding.native_session_id or native
                    if native:
                        target = await asyncio.to_thread(peer.find, native, workdir)
                    if target is None:
                        # A live terminal whose session cannot be messaged is ended, so a resume never runs beside it.
                        await _close_quietly(binding.handle, workdir)
                        binding = dataclasses.replace(binding, handle=None, incarnation_id=None)
                        self._save(binding)
                    else:
                        handle, incarnation = binding.handle, binding.incarnation_id
                if target is None:
                    fresh = native is None
                    native = native or str(uuid.uuid4())
                    continuation = ("--session-id", native) if fresh else ("--resume", native)
                    args = [*with_peer_handoff(role_args), *continuation, "--name", title]
                    launched = time.time() - 1
                    handle, incarnation = await self._start(profile, title, workdir, turn, args, None,
                                                            unset_extra=PEER_UNSET_ENV)
                    if session_id is not None:
                        binding = TerminalBinding(session_id=session_id, executor_id=profile.name, agent=ROLE_AGENT,
                                                  workdir=str(workdir), handle=handle, incarnation_id=incarnation,
                                                  native_session_id=native)
                        self._save(binding)
                    await self._await_gate(turn / "gate-status.json", handle, workdir, is_interrupted)
                    target = await self._await_registration(peer, native, title, launched, handle, workdir,
                                                            is_interrupted)
                    # Recorded as the session answers, in case the harness gave the resumed conversation a new ID.
                    native = target.session_id or native
                if binding is not None:
                    binding = dataclasses.replace(binding, native_session_id=native, peer_name=target.name,
                                                  peer_pid=target.pid,
                                                  peer_socket=target.socket_path, peer_attempt=attempt)
                    self._save(binding)
                assert handle is not None and native is not None
                since = time.time() - 1
                body = task_message(prompt, task_id=task_id, attempt_id=attempt, reply_to=peer.name,
                                    schema_name=schema_name, schema=schema)
                loop = asyncio.get_running_loop()
                when = deadline.when()
                record: dict[str, Any] = {
                    **(slot_extra or {}), "kind": "peer", "task_id": task_id, "attempt_id": attempt,
                    "native": native, "handle": handle, "incarnation": incarnation, "workdir": str(workdir),
                    "since": since, "addresses": peer.addresses(target), "sent": [],
                    "deadline_at": time.time() + (when - loop.time() if when is not None else profile.timeout_seconds),
                }
                if slot is not None:
                    # Recorded before the task goes out: a resumed Run waits for this attempt instead of sending it.
                    slot.record(slot_label, record)
                sent = [await asyncio.to_thread(peer.send, target, body)]
                if slot is not None:
                    slot.record(slot_label, {**record, "sent": sent})
                with parked(slot):
                    data = await self._await_peer_result(peer, target, sent, since, schema, task_id, attempt, handle,
                                                         workdir, is_interrupted, inbox_id)
                completed = True
                if slot is not None:
                    slot.record(slot_label, {**record, "sent": sent, "result": data})
                return RoleReply(data=data, native_session_id=native, handle=handle)
        except TimeoutError as exc:
            if not deadline.expired():
                raise ClaudeCodeError(f"Orca did not answer in {workdir}: {exc}") from exc
            raise ClaudeCodeError(
                f"timed out after {profile.timeout_seconds}s in Orca terminal {handle or '(not started)'}"
            ) from exc
        except _ORCA_ERRORS as exc:
            raise ClaudeCodeError(f"Orca terminal {handle or '(not started)'} in {workdir}: {type(exc).__name__} {exc}") from exc
        finally:
            shutil.rmtree(turn, ignore_errors=True)
            # A detached turn keeps its terminal and binding: the agent goes on and the next process waits for it.
            if handle and not completed and not detached(slot):
                if binding is not None:
                    self._save(dataclasses.replace(binding, handle=None, incarnation_id=None, native_session_id=native,
                                                   peer_pid=None, peer_socket=None))
                await _close_quietly(handle, workdir)

    async def _pick_up(
        self,
        peer: PeerHandoff,
        profile: ClaudeCodeProfile,
        saved: Mapping[str, Any],
        slot: TurnSlot,
        *,
        workdir: Path,
        schema: Mapping[str, Any],
        is_interrupted: Callable[[], bool],
        binding: TerminalBinding | None,
        inbox_id: str | None,
    ) -> RoleReply | None:
        # Continues a turn recorded before a restart. Returns None when it has to run again: its terminal and
        # session are gone and no result of it arrived.
        try:
            task_id, attempt = str(saved["task_id"]), str(saved["attempt_id"])
            native, handle = str(saved["native"]), str(saved["handle"])
            since, deadline_at = float(saved["since"]), float(saved["deadline_at"])
            addresses = [str(a) for a in cast(list[object], saved["addresses"])]
            sent = [str(m) for m in cast(list[object], saved.get("sent") or [])]
            recorded_dir = str(saved["workdir"])
        except (KeyError, TypeError, ValueError):
            slot.note("the recorded turn could not be read, so it was sent again")
            return None
        if os.path.realpath(recorded_dir) != str(workdir):
            slot.note(f"the recorded turn ran in {recorded_dir}, not {workdir}, so it was sent again")
            return None
        result = saved.get("result")
        if isinstance(result, dict):
            return RoleReply(data=cast(dict[str, Any], result), native_session_id=native, handle=handle)
        incarnation = saved.get("incarnation")
        probe = TerminalBinding(session_id="", executor_id=profile.name, agent=ROLE_AGENT, workdir=str(workdir),
                                handle=handle, incarnation_id=incarnation if isinstance(incarnation, str) else None)
        target: Session | None = None
        if await self._live(probe, ROLE_AGENT, workdir):
            target = await asyncio.to_thread(peer.find, native, workdir)
        if target is None:
            # The session is gone; a result it sent before ending is still in the receiver's inbox.
            await asyncio.to_thread(peer.ingest)
            for reply in await asyncio.to_thread(peer.replies_at, addresses, since):
                data, _ = parse_result(reply.body, schema, task_id=task_id, attempt_id=attempt)
                if data is not None:
                    slot.record(str(saved.get("label")), {**saved, "result": data})
                    return RoleReply(data=data, native_session_id=native, handle=handle)
            slot.note(f"Orca terminal {handle} ended without a result for attempt {attempt}, so the turn ran again")
            return None
        remaining = deadline_at - time.time()
        completed = False
        try:
            async with asyncio.timeout(max(remaining, 0.0)):
                with parked(slot):
                    data = await self._await_peer_result(peer, target, sent, since, schema, task_id, attempt, handle,
                                                         workdir, is_interrupted, inbox_id)
            completed = True
            slot.record(str(saved.get("label")), {**saved, "result": data})
            return RoleReply(data=data, native_session_id=native, handle=handle)
        except TimeoutError as exc:
            raise ClaudeCodeError(f"timed out after {profile.timeout_seconds}s in Orca terminal {handle}") from exc
        except _ORCA_ERRORS as exc:
            raise ClaudeCodeError(f"Orca terminal {handle} in {workdir}: {type(exc).__name__} {exc}") from exc
        finally:
            if not completed and not slot.run.detached():
                if binding is not None and binding.handle == handle:
                    self._save(dataclasses.replace(binding, handle=None, incarnation_id=None, native_session_id=native,
                                                   peer_pid=None, peer_socket=None))
                await _close_quietly(handle, workdir)

    async def _await_registration(
        self, peer: PeerHandoff, native: str, name: str, launched: float, handle: str, workdir: Path,
        is_interrupted: Callable[[], bool],
    ) -> Session:
        loop = asyncio.get_running_loop()
        give_up = loop.time() + peer.config.register_timeout
        next_check = loop.time() + self._liveness_interval
        while True:
            if is_interrupted():
                raise asyncio.CancelledError("visible run interrupted by a new event")
            found = await asyncio.to_thread(peer.find, native, workdir, name=name, since=launched)
            if found is not None:
                return found
            now = loop.time()
            if now >= give_up:
                raise ClaudeCodeError(
                    f"Claude Code session {native} in Orca terminal {handle} did not register for peer messages "
                    f"within {peer.config.register_timeout:g}s (looked in "
                    f"{', '.join(str(d) for d in peer.config.config_dirs)})"
                )
            if now >= next_check:
                if not await _terminal_alive(handle, workdir):
                    raise ClaudeCodeError(f"Orca terminal {handle} exited before its session registered")
                next_check = now + self._liveness_interval
            await asyncio.sleep(self._poll_interval)

    async def _await_peer_result(
        self,
        peer: PeerHandoff,
        target: Session,
        sent: list[str],
        since: float,
        schema: Mapping[str, Any],
        task_id: str,
        attempt: str,
        handle: str,
        workdir: Path,
        is_interrupted: Callable[[], bool],
        inbox_id: str | None,
    ) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        next_check = loop.time() + self._liveness_interval
        resends = 0
        gone = False
        while True:
            if is_interrupted():
                raise asyncio.CancelledError("visible run interrupted by a new event")
            await asyncio.to_thread(peer.ingest)
            for receipt in await asyncio.to_thread(peer.receipts, sent, target, since):
                self._held(receipt.record, target, handle, inbox_id)
            for reply in await asyncio.to_thread(peer.replies, target, since):
                data, error = parse_result(reply.body, schema, task_id=task_id, attempt_id=attempt)
                if data is not None:
                    return data
                resends += 1
                _log.warning("Peer result %s from %s rejected (%d): %s", reply.msg_id, target.label(), resends, error)
                left = peer.config.max_resends - resends
                if left < 0:
                    raise ClaudeCodeError(
                        f"session {target.name or target.session_id} sent {resends} result messages that were not "
                        f"accepted (limit {peer.config.max_resends} resends); the last: {error}"
                    )
                notice = resend_message(error, task_id=task_id, attempt_id=attempt, reply_to=peer.name, tries_left=left)
                sent.append(await asyncio.to_thread(peer.send, target, notice))
            if gone:
                raise ClaudeCodeError(f"Orca terminal {handle} exited before it returned its result")
            if loop.time() >= next_check:
                # One more read after the terminal is gone: the result may have landed just before it ended.
                gone = not await _terminal_alive(handle, workdir)
                next_check = loop.time() + self._liveness_interval
                if gone:
                    continue
            await asyncio.sleep(self._poll_interval)

    def _held(self, record: Mapping[str, Any], target: Session, handle: str, inbox_id: str | None) -> None:
        action = str(record.get("action") or record.get("status") or "control")
        text = (
            f"[hibernaut] Claude Code session {target.name or target.session_id} (pid {target.pid}) answered a "
            f"Hibernaut message with a '{action}' receipt (message {record.get('orig_msg_id') or '?'}). If it is held "
            f"for approval, approve it in Orca terminal {handle}; the turn waits until the session gets it."
        )
        _log.warning("%s", text)
        if self._notify is not None and inbox_id is not None:
            try:
                self._notify(inbox_id, text)
            except Exception:
                _log.exception("Could not write the held-message notice to the Outbox")

    async def _exit_session(self, binding: TerminalBinding, workdir: Path) -> bool:
        # Asks Claude Code to quit the way a person would, then waits for the terminal and its process to end.
        assert binding.handle is not None
        try:
            await _orca("terminal", "send", "--terminal", binding.handle, "--text", "/exit", "--enter",
                        "--wait-submit", _SUBMIT_WAIT_SECONDS, cwd=workdir)
        except _ORCA_ERRORS as exc:
            _log.info("Could not send /exit to %s: %s", binding.handle, exc)
            return False
        loop = asyncio.get_running_loop()
        give_up = loop.time() + self._exit_timeout
        while loop.time() < give_up:
            try:
                listed = await _terminal_alive(binding.handle, workdir)
            except _ORCA_ERRORS:
                listed = True
            if not listed and not _pid_alive(binding.peer_pid):
                return True
            await asyncio.sleep(self._poll_interval)
        return False

    async def release(self, session_id: str, workdir: Path) -> str:
        # Ends the visible terminal of a conversation whose work is delivered. The binding keeps the native ID, so a
        # later event resumes the conversation with --resume; the worktree is untouched.
        binding = self._bindings.get(session_id) if self._bindings is not None else None
        if binding is None or not binding.handle:
            return "no live terminal is bound to this session"
        workdir = Path(os.path.realpath(workdir))
        if binding.peer_socket and await self._exit_session(binding, workdir):
            _drop_lease(binding.handle)
            self._save(dataclasses.replace(binding, handle=None, incarnation_id=None, peer_pid=None, peer_socket=None,
                                           peer_attempt=None))
            return f"exited {binding.handle} (Claude Code ended on /exit)"
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
        self, profile: ClaudeCodeProfile, title: str, workdir: Path, turn: Path, args: Sequence[str],
        instruction: str | None, *, unset_extra: Sequence[str] = (),
    ) -> tuple[str, str | None]:
        launcher = profile.launcher
        unset, values = env_overrides(profile)
        unset = (*unset, *(k for k in unset_extra if k not in unset))
        spec = {
            "title": title,
            "profile": {"name": profile.name, "launcher": launcher.name, "launcher_command": launcher.command,
                        "launcher_env": launcher.env, "env": profile.env,
                        "billing": profile.billing, "model": profile.model, "effort": profile.effort,
                        "credential_proxy_url": profile.credential_proxy_url},
            "unset": list(unset),
            "set": values,
            "probe": [launcher.command, *harness_args(profile, permission_mode="dontAsk", role_args=PROBE_ROLE_ARGS)],
            "probe_timeout": _PROBE_TIMEOUT_SECONDS,
            # `--` ends the options: --allowedTools and --mcp-config take several values and would swallow it.
            # Without an instruction the session starts idle and gets its task as a peer message.
            "exec": [launcher.command, *interactive_args(profile, permission_mode="dontAsk", role_args=args),
                     *(("--", instruction) if instruction is not None else ())],
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


async def _close_cli_terminal(handle: str, workdir: Path) -> None:
    # Orca keeps the tab of a command that ended, so the handle is closed even when its process is gone. What happens
    # here never changes the run's result.
    async def close() -> None:
        try:
            await _orca("terminal", "close", "--terminal", handle, cwd=workdir)
        except Exception as exc:
            ended = False
            if _STALE_HANDLE in str(exc):
                with contextlib.suppress(Exception):
                    ended = not await _still_listed(handle, workdir)
            if ended:
                _log.info("Orca terminal %s was already closed", handle)
            else:
                _log.warning("Could not close Orca terminal %s: %s", handle, f"{type(exc).__name__} {exc}".strip())

    closing = asyncio.ensure_future(close())
    try:
        await asyncio.shield(closing)
    except asyncio.CancelledError:
        # A cancellation that lands here must not leave the tab behind.
        await closing
        raise


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
    # it comes and is closed once the run is over, whatever its result; the result is read only from files the wrapper
    # writes, never from the screen.
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
        slot: TurnSlot | None = None,
        slot_label: str = "cli",
        slot_extra: Mapping[str, Any] | None = None,
    ) -> TerminalRun:
        # Raises TerminalStartFailed when no terminal opened (nothing ran), ProcessTimeout on the deadline,
        # TerminalRunFailed when the terminal ended early or Orca failed, CancelledError on interrupt.
        # With a slot the run directory lives in the Run's directory and survives a detach with its terminal.
        workdir = Path(os.path.realpath(workdir))
        root = slot.turns_dir if slot is not None else self._root
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        run_dir = Path(tempfile.mkdtemp(prefix="run-", dir=root))
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
        try:
            try:
                handle, _ = await _create_terminal(workdir, title, command)
            except _ORCA_ERRORS as exc:
                raise TerminalStartFailed(f"{type(exc).__name__} {exc}".strip()) from exc
            if slot is not None:
                slot.record(slot_label, {**(slot_extra or {}), "kind": "cli", "handle": handle, "dir": str(run_dir),
                                         "workdir": str(workdir), "deadline_at": time.time() + timeout_seconds,
                                         "timeout_seconds": timeout_seconds})
            return await self._wait(handle, run_dir, workdir, argv[0], timeout_seconds, is_interrupted, slot)
        finally:
            if not detached(slot):
                await self._end(handle, run_dir, workdir)

    async def pick_up(
        self, slot: TurnSlot, label: str, workdir: Path, is_interrupted: Callable[[], bool]
    ) -> TerminalRun | None:
        # The command a stopped process left running for this label: its result if it already ended, else waits
        # for it. None when nothing usable is recorded, or the terminal ended without a result: run it again.
        saved = slot.take(label)
        if saved is None:
            return None
        workdir = Path(os.path.realpath(workdir))
        try:
            handle, run_dir = str(saved["handle"]), Path(os.path.realpath(str(saved["dir"])))
            deadline_at, timeout_seconds = float(saved["deadline_at"]), float(saved["timeout_seconds"])
        except (KeyError, TypeError, ValueError):
            slot.note("the recorded review command could not be read, so it ran again")
            return None
        if Path(os.path.realpath(slot.run.run_dir)) not in run_dir.parents:
            slot.note(f"the recorded review command's directory {run_dir} is outside this Run, so it ran again")
            return None
        if not (run_dir / "exit.json").exists() and not await _still_listed(handle, workdir):
            slot.note(f"Orca terminal {handle} ended without a result, so the review command ran again")
            await self._end(handle, run_dir, workdir)
            return None
        try:
            return await self._wait(handle, run_dir, workdir, "the review command",
                                    max(deadline_at - time.time(), 0.0), is_interrupted, slot,
                                    timeout_label=timeout_seconds)
        finally:
            if not slot.run.detached():
                await self._end(handle, run_dir, workdir)

    async def _end(self, handle: str | None, run_dir: Path, workdir: Path) -> None:
        # The directory goes last, whatever happens to the close: its removal stops a wrapper the close never reached.
        try:
            if handle:
                await _close_cli_terminal(handle, workdir)
        finally:
            shutil.rmtree(run_dir, ignore_errors=True)

    async def _wait(
        self, handle: str, run_dir: Path, workdir: Path, what: str, timeout_seconds: float,
        is_interrupted: Callable[[], bool], slot: TurnSlot | None, *, timeout_label: float | None = None,
    ) -> TerminalRun:
        exit_path = run_dir / "exit.json"

        def read() -> dict[str, Any] | None:
            raw = _read_json(exit_path)
            return cast(dict[str, Any], json.loads(raw)) if raw is not None else None

        deadline = asyncio.timeout(timeout_seconds)
        try:
            async with deadline:
                with parked(slot):
                    status = await _await_file(read, handle, workdir, is_interrupted, poll_interval=self._poll_interval,
                                               liveness_interval=self._liveness_interval)
        except _TerminalGone:
            raise TerminalRunFailed(f"Orca terminal {handle} was closed before '{what}' finished") from None
        except TimeoutError as exc:
            if deadline.expired():
                shown = timeout_label if timeout_label is not None else timeout_seconds
                raise ProcessTimeout(f"timed out after {shown:g}s in Orca terminal {handle}") from exc
            raise TerminalRunFailed(f"Orca terminal {handle}: TimeoutError {exc}") from exc
        except _ORCA_ERRORS as exc:
            raise TerminalRunFailed(f"Orca terminal {handle}: {type(exc).__name__} {exc}") from exc
        code = status.get("exit")
        stdout, stderr = run_dir / "stdout", run_dir / "stderr"
        return TerminalRun(
            handle=handle,
            stdout=stdout.read_bytes() if stdout.exists() else b"",
            stderr=stderr.read_bytes() if stderr.exists() else b"",
            exit_code=code if isinstance(code, int) else None,
            error=str(status.get("error") or ""),
        )
