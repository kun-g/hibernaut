from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta, UTC

from agent_runtime.core.models import OutboxMessage, to_utc_iso
from agent_runtime.core.ports import ChannelPort, OutboxPort

# Same steps as Switchboard's downlink delivery, so a restart of either side is ridden out the same way.
DEFAULT_RETRY_BACKOFF_SECONDS: tuple[float, ...] = (5.0, 15.0, 45.0, 135.0, 405.0)


def _utc_now() -> datetime:
    return datetime.now(UTC)


class OutboxDispatcher:
    def __init__(
        self,
        *,
        outbox: OutboxPort,
        channel: ChannelPort,
        min_interval_seconds: float = 0.0,
        max_attempts: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        retry_backoff_seconds: Sequence[float] = DEFAULT_RETRY_BACKOFF_SECONDS,
        utc_clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        backoff = tuple(float(s) for s in retry_backoff_seconds)
        if any(s < 0 for s in backoff):
            raise ValueError("retry_backoff_seconds must not be negative")
        # One first attempt plus one retry per backoff step, unless the caller caps it explicitly.
        attempts_cap = len(backoff) + 1 if max_attempts is None else max_attempts
        if attempts_cap < 1:
            raise ValueError("max_attempts must be at least 1")
        self._outbox = outbox
        self._channel = channel
        self._min_interval_seconds = min_interval_seconds
        self._max_attempts = attempts_cap
        self._retry_backoff_seconds = backoff
        self._clock = clock
        self._utc_clock = utc_clock
        self._last_sent_at: dict[str, float] = {}

    async def dispatch_once(self, limit: int = 20) -> int:
        pending = self._outbox.fetch_pending(limit=limit, now_iso=to_utc_iso(self._utc_clock()))
        sent_count = 0

        for msg in pending:
            if msg.attempts >= self._max_attempts:
                # Rows a build without a final state left pending would otherwise fill every batch.
                self._outbox.mark_failed(msg.message_id, "retry attempts exhausted")
                continue

            now = self._clock()
            last_sent = self._last_sent_at.get(msg.inbox_id)
            if (
                last_sent is not None
                and self._min_interval_seconds > 0.0
                and (now - last_sent) < self._min_interval_seconds
            ):
                # Pacing / anti-spam throttle: defer until min_interval_seconds elapses.
                continue

            try:
                await self._channel.send(msg.inbox_id, msg.content, msg.metadata)
            except Exception as exc:  # noqa: BLE001
                self._record_failure(msg, str(exc))
            else:
                self._outbox.mark_sent(msg.message_id)
                self._last_sent_at[msg.inbox_id] = self._clock()
                sent_count += 1

        return sent_count

    def _record_failure(self, msg: OutboxMessage, error: str) -> None:
        if msg.attempts + 1 >= self._max_attempts or not self._retry_backoff_seconds:
            self._outbox.mark_failed(msg.message_id, error)
            return
        step = min(msg.attempts, len(self._retry_backoff_seconds) - 1)
        due = self._utc_clock() + timedelta(seconds=self._retry_backoff_seconds[step])
        self._outbox.schedule_retry(msg.message_id, error, to_utc_iso(due))

    async def run_forever(
        self,
        interval_seconds: float = 1.0,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        stopper = stop_event or asyncio.Event()
        while not stopper.is_set():
            await self.dispatch_once()
            try:
                await asyncio.wait_for(stopper.wait(), timeout=interval_seconds)
            except TimeoutError:
                continue
