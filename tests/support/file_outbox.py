from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from pathlib import Path

from agent_runtime.core.models import DeliveryOutcome, Emit, Event, RunOutcome, SessionRef, Suspend
from agent_runtime.core.ports import RunCapabilities


INBOX = "proj:hibernaut"
PR_URL = "https://github.com/kun-g/hibernaut/pull/53"


def _md(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.md"))


class _ScriptedExecutor:
    executor_id = "scripted"

    def __init__(self) -> None:
        self.calls: list[tuple[list[Event], SessionRef | None]] = []

    async def execute(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
        _ = caps
        self.calls.append((list(events), session))
        payload = events[-1].payload
        if payload.get("approval_request_id"):
            return Emit(messages=(f"resumed approved={payload.get('approved')}",))
        mode = payload.get("mode")
        if mode == "suspend":
            return Suspend(request_id="req_file_1", prompt="Deploy to prod?", session_checkpoint="{}")
        if mode == "failed":
            return Emit(messages=("[sde] development failed: payload needs the task requirements",), status="failed")
        if mode == "pr_open":
            return Emit(messages=("[sde] delivered",), delivery=DeliveryOutcome(
                state="pr_open", pr_url=PR_URL, auto_merge=True, hold="review",
                hold_detail="merging needs a passing independent review (review=pending)"))
        return Emit(messages=("all good",))


def _header(path: Path) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in path.read_text().split("## ", 1)[0].splitlines():
        if line.startswith("- ") and "：" in line:
            key, value = line[2:].split("：", 1)
            fields[key] = value
    return fields


async def _until(check: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.02)
