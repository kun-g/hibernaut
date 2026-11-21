"""L2 OutboxDispatcher: consumes Outbox messages with per-inbox pacing and retry limits."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from agent_runtime.core.ports import ChannelPort, OutboxPort


class OutboxDispatcher:
    """Delivers pending Outbox messages to ChannelPort with throttling and failure retries."""

    def __init__(
        self,
        *,
        outbox: OutboxPort,
        channel: ChannelPort,
        min_interval_seconds: float = 0.0,
        max_attempts: int = 3,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._outbox = outbox
        self._channel = channel
        self._min_interval_seconds = min_interval_seconds
        self._max_attempts = max_attempts
        self._clock = clock
        self._last_sent_at: dict[str, float] = {}

    async def dispatch_once(self, limit: int = 20) -> int:
        """Attempt delivery of pending Outbox messages; returns number successfully sent."""
        pending = self._outbox.fetch_pending(limit=limit)
        sent_count = 0

        for msg in pending:
            if msg.attempts >= self._max_attempts:
                continue

            now = self._clock()
            last_sent = self._last_sent_at.get(msg.inbox_id)
            if (
                last_sent is not None
                and self._min_interval_seconds > 0.0
                and (now - last_sent) < self._min_interval_seconds
            ):
                # Pacing / anti-spam throttle: defer until min_interval_seconds elapses
                continue

            try:
                await self._channel.send(msg.inbox_id, msg.content, msg.metadata)
            except Exception as exc:  # noqa: BLE001
                self._outbox.mark_failed(msg.message_id, str(exc))
            else:
                self._outbox.mark_sent(msg.message_id)
                self._last_sent_at[msg.inbox_id] = self._clock()
                sent_count += 1

        return sent_count

    async def run_forever(
        self,
        interval_seconds: float = 1.0,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """Continuously dispatch pending Outbox messages until stop_event is set."""
        stopper = stop_event or asyncio.Event()
        while not stopper.is_set():
            await self.dispatch_once()
            try:
                await asyncio.wait_for(stopper.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue
