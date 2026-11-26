from collections.abc import Sequence

from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef
from agent_runtime.core.ports import RunCapabilities


class _FinishingExecutor:
    executor_id = "sde"

    def __init__(self, *, close: bool) -> None:
        self.close = close

    async def execute(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
        _ = caps
        return Emit(messages=("[sde] delivered",), session_id=session.session_id if session else None,
                    close_session=self.close)
