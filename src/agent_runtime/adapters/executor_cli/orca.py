from __future__ import annotations

import asyncio
import json
import logging
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from agent_runtime.core.models import Emit, RunOutcome, Silent, Suspend, new_id
from agent_runtime.core.ports import RunCapabilities


async def _orca(*args: str, cwd: Path) -> dict[str, Any]:
    proc = await asyncio.create_subprocess_exec(
        "orca", *args, "--json", cwd=cwd,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
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


async def run_visible(agent: str, workdir: Path, prompt: str, session_id: str,
                      caps: RunCapabilities, timeout: float) -> RunOutcome:
    decision = caps.permissions.check("orca.agent.launch", {"agent": agent, "workdir": str(workdir), "prompt": prompt})
    if decision.action == "block":
        return Emit(messages=(decision.reason,), session_id=session_id)
    if decision.action == "defer":
        return Suspend(request_id=new_id("req"), prompt=decision.reason, session_checkpoint="{}", session_id=session_id)

    with tempfile.TemporaryDirectory(prefix="hibernaut-result-") as directory:
        path = Path(directory) / "result.json"
        prompt += (
            "\n\n## Runtime handoff\nShow progress and the final summary in this terminal. "
            "As the final action, write a UTF-8 JSON object to " + str(path.with_suffix(".tmp")) +
            " and atomically rename that file to " + str(path) + ". "
            'Use {"outcome":"emit","messages":["your summary"]}, {"outcome":"silent"}, '
            'or {"outcome":"suspend","prompt":"question","session_checkpoint":"context"}.'
        )
        handle = None
        completed = False
        launch = asyncio.create_task(_orca("agent", "launch", "--agent", agent, "--worktree", f"path:{workdir}",
                                           "--surface", "terminal", "--prompt", prompt, cwd=workdir))
        try:
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
            if receipt.get("prompt", {}).get("outcome") not in (None, "handed-to-terminal"):
                raise ValueError("Orca did not deliver the task prompt")
            async with asyncio.timeout(timeout):
                while True:
                    if caps.is_interrupted():
                        raise asyncio.CancelledError
                    result = _result(path, session_id)
                    if result is not None:
                        completed = True
                        return result
                    await asyncio.sleep(0.25)
        except (OSError, TimeoutError, ValueError, RuntimeError) as exc:
            # A lost receipt can still mean the agent started. Suspend instead of launching it twice.
            detail = f"Check Orca in {workdir} ({handle or 'launch receipt unavailable'}): {type(exc).__name__} {exc}"
            return Suspend(request_id=new_id("req"), prompt=detail, session_checkpoint="{}", session_id=session_id)
        finally:
            if handle and not completed:
                try:
                    await _orca("terminal", "close", "--terminal", handle, cwd=workdir)
                except (OSError, TimeoutError, ValueError, RuntimeError):
                    logging.getLogger(__name__).exception("Could not close Orca terminal %s", handle)
