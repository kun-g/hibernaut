from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta

from agent_runtime.core.models import Event, utc_now_iso
from agent_runtime.core.ports import InboxPort, TimerPort


class TimerScheduler:
    def __init__(
        self,
        *,
        timers: TimerPort,
        inbox: InboxPort,
        on_wake: Callable[[str], None] | None = None,
    ) -> None:
        self._timers = timers
        self._inbox = inbox
        self._on_wake = on_wake

    def tick(self, now_iso: str | None = None) -> int:
        current_iso = now_iso or utc_now_iso()
        due_timers = self._timers.fetch_due(current_iso)
        triggered_count = 0

        for timer in due_timers:
            next_due_at: str | None = None
            if timer.kind == "schedule" and timer.recurrence_seconds is not None:
                base_dt = datetime.fromisoformat(timer.due_at)
                next_due_at = (base_dt + timedelta(seconds=timer.recurrence_seconds)).isoformat()

            self._timers.mark_triggered(timer.timer_id, next_due_at=next_due_at)

            # A named timer is a source of its own, so an inbox rule can send it to one agent.
            name = timer.payload.get("timer_name")
            event = Event(
                inbox_id=timer.inbox_id,
                source=f"timer:{name.strip() if isinstance(name, str) and name.strip() else timer.kind}",
                external_id=f"{timer.timer_id}:{timer.due_at}",
                payload={
                    "timer_id": timer.timer_id,
                    "timer_kind": timer.kind,
                    "due_at": timer.due_at,
                    **timer.payload,
                },
                session_id=timer.session_id,
            )
            _, is_dup = self._inbox.append(event)
            if not is_dup and self._on_wake is not None:
                self._on_wake(timer.inbox_id)
            triggered_count += 1

        return triggered_count

    async def run_forever(
        self,
        interval_seconds: float = 5.0,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        stopper = stop_event or asyncio.Event()
        while not stopper.is_set():
            self.tick()
            try:
                await asyncio.wait_for(stopper.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue
