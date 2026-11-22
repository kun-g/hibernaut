from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteOutboxAdapter
from agent_runtime.engine import OutboxDispatcher
from tests.fakes import InMemoryOutbox

START = datetime(2026, 11, 22, 8, 0, 0, tzinfo=UTC)
FAR_FUTURE = "2999-01-01T00:00:00.000000+00:00"


class FlakyChannel:
    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0
        self.sent: list[str] = []

    async def send(self, inbox_id: str, content: str, metadata: dict[str, Any]) -> None:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ConnectionRefusedError(f"refused #{self.calls}")
        self.sent.append(content)


class UtcClock:
    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def iso(at: datetime) -> str:
    return at.isoformat(timespec="microseconds")


class BackoffMixin:
    assertEqual: Any
    assertIsNone: Any

    def make_outbox(self) -> Any:
        raise NotImplementedError

    def row(self, outbox: Any, message_id: str) -> tuple[str, int, str | None]:
        raise NotImplementedError

    def setUp(self) -> None:
        self.clock = UtcClock()

    def dispatcher(self, outbox: Any, channel: FlakyChannel, **kwargs: Any) -> OutboxDispatcher:
        return OutboxDispatcher(outbox=outbox, channel=channel, utc_clock=self.clock, **kwargs)

    async def test_failure_keeps_the_message_pending_until_its_backoff_is_due(self) -> None:
        outbox = self.make_outbox()
        channel = FlakyChannel(fail_times=1)
        dispatcher = self.dispatcher(outbox, channel)
        msg = outbox.enqueue("u1", "report")

        self.assertEqual(await dispatcher.dispatch_once(), 0)
        self.assertEqual(self.row(outbox, msg.message_id), ("pending", 1, iso(START + timedelta(seconds=5))))

        self.clock.advance(4.9)
        self.assertEqual(await dispatcher.dispatch_once(), 0)
        self.assertEqual(channel.calls, 1)

        self.clock.advance(0.1)
        self.assertEqual(await dispatcher.dispatch_once(), 1)
        self.assertEqual(channel.sent, ["report"])
        self.assertEqual(self.row(outbox, msg.message_id)[0], "sent")

    async def test_failed_only_after_every_backoff_step_is_used(self) -> None:
        outbox = self.make_outbox()
        dispatcher = self.dispatcher(outbox, FlakyChannel(fail_times=100))
        msg = outbox.enqueue("u1", "report")

        for attempt, delay in enumerate((5.0, 15.0, 45.0, 135.0, 405.0), start=1):
            await dispatcher.dispatch_once()
            self.assertEqual(
                self.row(outbox, msg.message_id), ("pending", attempt, iso(self.clock.now + timedelta(seconds=delay)))
            )
            self.clock.advance(delay)

        await dispatcher.dispatch_once()
        status, attempts, _ = self.row(outbox, msg.message_id)
        self.assertEqual((status, attempts), ("failed", 6))
        self.assertEqual(outbox.fetch_pending(now_iso=FAR_FUTURE), [])

    async def test_explicit_max_attempts_still_caps_the_retries(self) -> None:
        outbox = self.make_outbox()
        dispatcher = self.dispatcher(outbox, FlakyChannel(fail_times=100), max_attempts=3, retry_backoff_seconds=(1.0, 2.0))
        msg = outbox.enqueue("u1", "report")
        for delay in (1.0, 2.0):
            await dispatcher.dispatch_once()
            self.clock.advance(delay)
        await dispatcher.dispatch_once()
        self.assertEqual(self.row(outbox, msg.message_id)[:2], ("failed", 3))

    async def test_rows_already_past_the_cap_are_failed_instead_of_filling_the_batch(self) -> None:
        outbox = self.make_outbox()
        channel = FlakyChannel(fail_times=0)
        stuck = outbox.enqueue("u1", "stuck")
        for _ in range(3):
            outbox.schedule_retry(stuck.message_id, "old failure", iso(START))
        fresh = outbox.enqueue("u1", "fresh")

        sent = await self.dispatcher(outbox, channel, max_attempts=3).dispatch_once(limit=1)
        self.assertEqual(sent, 0)
        self.assertEqual(self.row(outbox, stuck.message_id)[0], "failed")
        self.assertEqual(await self.dispatcher(outbox, channel, max_attempts=3).dispatch_once(limit=1), 1)
        self.assertEqual(self.row(outbox, fresh.message_id)[0], "sent")


class InMemoryBackoffTests(BackoffMixin, unittest.IsolatedAsyncioTestCase):
    def make_outbox(self) -> Any:
        return InMemoryOutbox()

    def row(self, outbox: Any, message_id: str) -> tuple[str, int, str | None]:
        msg = outbox.get(message_id)
        return msg.status, msg.attempts, msg.next_retry_at


class SQLiteBackoffTests(BackoffMixin, unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "h.db"
        self.db = SQLiteDatabase(self.path)

    def tearDown(self) -> None:
        self.db.close()
        self.tmp.cleanup()

    def make_outbox(self) -> Any:
        return SQLiteOutboxAdapter(self.db)

    def row(self, outbox: Any, message_id: str) -> tuple[str, int, str | None]:
        found = self.db.fetchone("SELECT status, attempts, next_retry_at FROM outbox WHERE message_id = ?", (message_id,))
        assert found is not None
        return str(found["status"]), int(found["attempts"]), found["next_retry_at"]

    async def test_restart_resumes_from_the_stored_attempts_and_next_retry_at(self) -> None:
        msg = SQLiteOutboxAdapter(self.db).enqueue("u1", "report")
        for _ in range(2):
            await self.dispatcher(SQLiteOutboxAdapter(self.db), FlakyChannel(fail_times=100)).dispatch_once()
            self.clock.advance(60)
        self.db.close()

        self.db = SQLiteDatabase(self.path)
        outbox = SQLiteOutboxAdapter(self.db)
        due_at = iso(START + timedelta(seconds=60 + 15))
        self.assertEqual(self.row(outbox, msg.message_id), ("pending", 2, due_at))
        (restored,) = outbox.fetch_pending(now_iso=due_at)
        self.assertEqual((restored.attempts, restored.next_retry_at), (2, due_at))

        # The third failure takes the third step (45s), not the first one again.
        await self.dispatcher(outbox, FlakyChannel(fail_times=100)).dispatch_once()
        self.assertEqual(self.row(outbox, msg.message_id), ("pending", 3, iso(self.clock.now + timedelta(seconds=45))))


if __name__ == "__main__":
    unittest.main()
