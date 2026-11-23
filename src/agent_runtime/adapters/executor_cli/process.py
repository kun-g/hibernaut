from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

from agent_runtime.core.models import Event, SessionRef
from agent_runtime.engine.forwarding import carried_spec

T = TypeVar("T")


class ProcessTimeout(Exception):
    pass


async def supervise(
    proc: asyncio.subprocess.Process,
    work: Awaitable[T],
    *,
    is_interrupted: Callable[[], bool],
    timeout_seconds: float,
    poll_interval: float,
) -> T:
    # Interrupt raises CancelledError so the Run writes nothing; the process never outlives this call.
    task = asyncio.ensure_future(work)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    try:
        while True:
            if is_interrupted():
                raise asyncio.CancelledError("CLI subprocess interrupted by new event")
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise ProcessTimeout(f"timed out after {timeout_seconds}s")
            done, _ = await asyncio.wait({task}, timeout=min(poll_interval, remaining))
            if done:
                return task.result()
    finally:
        if not task.done():
            task.cancel()
        await terminate(proc)


async def terminate(proc: asyncio.subprocess.Process, grace_seconds: float = 2.0) -> None:
    if proc.returncode is not None:
        return
    try:
        proc.terminate()
        await asyncio.wait_for(proc.wait(), timeout=grace_seconds)
    except (ProcessLookupError, asyncio.TimeoutError):
        try:
            proc.kill()
            await proc.wait()
        except ProcessLookupError:
            pass


def is_approval(event: Event) -> bool:
    request_id = event.payload.get("approval_request_id")
    return isinstance(request_id, str) and bool(request_id)


def target_events(events: Sequence[Event]) -> list[Event]:
    # An approval reply resumes the batch's Run and only supplies the target when it runs alone.
    return [e for e in events if not is_approval(e)] or list(events)


def relayed_work_key(events: Sequence[Event], session: SessionRef | None) -> str | None:
    # Work passed on inside one inbox goes on in the worktree of the agent that passed it, and so does whatever
    # follows in the same session.
    relayed = carried_spec(events[0]) if events else None
    if relayed is not None and relayed.work_key:
        return relayed.work_key
    return session.forward.work_key if session is not None and session.forward is not None else None


def last_text(events: Sequence[Event], key: str) -> str | None:
    for evt in reversed(events):
        val = evt.payload.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def join_text(events: Sequence[Event], keys: Sequence[str]) -> str:
    parts: list[str] = []
    for evt in events:
        for key in keys:
            val = evt.payload.get(key)
            if isinstance(val, str) and val.strip():
                parts.append(val.strip())
                break
    return "\n\n".join(parts)
