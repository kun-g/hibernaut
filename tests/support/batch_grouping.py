from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence

from agent_runtime.core.models import Event, RunOutcome, SessionRef, Silent
from agent_runtime.core.ports import RunCapabilities


class _RecordingExecutor:
    executor_id = "recording"

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.started: list[list[str]] = []
        self.during_run: Callable[[list[str]], Awaitable[None]] | None = None

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = session
        batch = [evt.external_id for evt in events]
        self.started.append(list(batch))
        if self.during_run is not None:
            await self.during_run(batch)
        batch += [evt.external_id for evt in caps.poll_folded_events()]
        self.calls.append(batch)
        return Silent()
