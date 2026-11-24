from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Final, Mapping, cast

from runner.protocol import ToolAuthorizer, ToolCall, ToolDecision

_log = logging.getLogger(__name__)

MAX_TOOL_CALL_BYTES: Final = 512 * 1024
DECISION_TIMEOUT_SECONDS: Final = 60.0
_SESSION_ID_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
# sun_path holds 104 bytes on macOS.
_MAX_SOCKET_PATH_BYTES: Final = 100


@dataclass(frozen=True, slots=True)
class ToolGateBinding:
    socket_path: Path
    # Private to the run and outside its worktree: the driver writes the CLI's hook configuration here.
    config_dir: Path


class ToolGate:
    def __init__(
        self,
        *,
        run_id: str,
        authorizer: ToolAuthorizer,
        stopped: Callable[[], bool],
        on_defer: Callable[[], None],
        decision_timeout_seconds: float = DECISION_TIMEOUT_SECONDS,
        # The node's own limit: says why a call does not run here, whatever the authorizer would answer.
        guard: Callable[[str, Mapping[str, Any]], str | None] | None = None,
    ) -> None:
        self._run_id = run_id
        self._authorizer = authorizer
        self._stopped = stopped
        self._on_defer = on_defer
        self._guard = guard
        self._decision_timeout = decision_timeout_seconds
        self._lock = asyncio.Lock()
        self._handlers: set[asyncio.Task[None]] = set()
        self._server: asyncio.AbstractServer | None = None
        self._dir: Path | None = None
        self.armed = asyncio.Event()
        self.native_session_id: str | None = None
        self.deferred: tuple[ToolCall, ToolDecision] | None = None

    async def open(self) -> ToolGateBinding:
        self._dir = _private_dir()
        sock = self._dir / "gate.sock"
        config_dir = self._dir / "config"
        try:
            config_dir.mkdir(mode=0o700)
            self._server = await asyncio.start_unix_server(self._serve, path=str(sock), limit=MAX_TOOL_CALL_BYTES)
        except BaseException:
            await self.close()
            raise
        return ToolGateBinding(socket_path=sock, config_dir=config_dir)

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
        handlers = list(self._handlers)
        for task in handlers:
            task.cancel()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)
        if self._dir is not None:
            shutil.rmtree(self._dir, ignore_errors=True)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = cast("asyncio.Task[None]", asyncio.current_task())
        self._handlers.add(task)
        try:
            decision = await self._handle(reader)
            writer.write(json.dumps(decision.to_dict()).encode("utf-8") + b"\n")
            await writer.drain()
        except OSError:
            pass  # the hook went away; without an answer it refuses the call itself
        finally:
            self._handlers.discard(task)
            writer.close()

    async def _handle(self, reader: asyncio.StreamReader) -> ToolDecision:
        try:
            line = await reader.readline()
        except ValueError:
            return _block(f"tool call is larger than {MAX_TOOL_CALL_BYTES} bytes")
        try:
            raw: Any = json.loads(line)
            # Lone surrogates cannot be sent on or stored; the call is judged as it was made or not at all.
            json.dumps(raw, ensure_ascii=False).encode("utf-8")
        except (ValueError, RecursionError):
            return _block("malformed tool call")
        if not isinstance(raw, dict):
            return _block("malformed tool call")
        frame = cast(dict[str, Any], raw)
        if frame.get("type") == "hook.armed":
            self.armed.set()
            session = frame.get("session_id")
            # The first report is the run's own session; a subagent's would come later.
            if self.native_session_id is None and isinstance(session, str) and _SESSION_ID_RE.fullmatch(session):
                self.native_session_id = session
            return ToolDecision(action="allow")
        if frame.get("type") != "tool.authorize":
            return _block("malformed tool call")
        self.armed.set()
        return await self._authorize(frame)

    async def _authorize(self, frame: Mapping[str, Any]) -> ToolDecision:
        tool_name, args = frame.get("tool_name"), frame.get("args")
        if not isinstance(tool_name, str) or not tool_name or not isinstance(args, Mapping):
            return _block("malformed tool call")
        call = ToolCall(
            run_id=self._run_id,
            call_id=f"call_{uuid.uuid4().hex}",
            tool_name=tool_name,
            args=dict(cast(Mapping[str, Any], args)),
        )
        # One call at a time: once a call is deferred, no other call of the run is asked for or let through.
        async with self._lock:
            if (refusal := self._refusal()) is not None:
                return refusal
            if self._guard is not None and (reason := self._guard(call.tool_name, call.args)) is not None:
                _log.warning("run %s: %s call refused on this node: %s", self._run_id, call.tool_name, reason)
                return _block(reason)
            try:
                decision = await asyncio.wait_for(self._authorizer(call), timeout=self._decision_timeout)
            except TimeoutError:
                return _block(f"tool authorization was not answered within {self._decision_timeout:g}s")
            except Exception as exc:  # noqa: BLE001
                return _block(f"tool authorization failed: {exc}")
            if decision.action == "defer":
                if not self._stopped():
                    self.deferred = (call, decision)
                    self._on_defer()
                return decision
            return self._refusal() or decision

    def _refusal(self) -> ToolDecision | None:
        if self.deferred is not None:
            return _block("the run is waiting for approval of an earlier tool call")
        if self._stopped():
            return _block("the run is being stopped")
        return None


def _block(reason: str) -> ToolDecision:
    return ToolDecision(action="block", reason=reason)


def _private_dir() -> Path:
    path = Path(tempfile.mkdtemp(prefix="rtg-"))
    if len(os.fsencode(path / "gate.sock")) <= _MAX_SOCKET_PATH_BYTES:
        return path
    path.rmdir()
    return Path(tempfile.mkdtemp(prefix="rtg-", dir="/tmp"))
