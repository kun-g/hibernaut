from __future__ import annotations

import threading
import time
import unittest

from switchboard.envelope import CanonicalEvent
from switchboard.store_sqlite import ReplayError
from tests.support.switchboard_support import Harness, issue_payload, make_harness, send_github


def closed_event(number: int, scope: str) -> CanonicalEvent:
    return CanonicalEvent(
        source="github",
        event_type="work.closed",
        external_id=f"issue:kun-g/hibernaut#{number}:closed:t",
        scope=scope,
        correlation_key=f"github:kun-g/hibernaut#{number}",
        subject=f"issue {number}",
        metadata={"work_kind": "issue", "state": "closed"},
    )


class FanOutIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h: Harness = make_harness(start_http=False)
        self.board = self.h.board
        self.taskboard_seen: list[str] = []
        self.hibernaut_seen: list[str] = []
        self.failing: set[str] = set()
        self.board.bus.subscribe("taskboard", lambda e: self.taskboard_seen.append(e.event_id) or None)
        self.board.bus.subscribe("hibernaut", self._hibernaut)

    def tearDown(self) -> None:
        self.h.close()

    def _hibernaut(self, event: CanonicalEvent) -> None:
        self.hibernaut_seen.append(event.event_id)
        if event.event_id in self.failing:
            raise RuntimeError("hibernaut unavailable")

    def _status(self, event_id: str, consumer: str) -> tuple[str, int]:
        row = self.board.store.get_delivery(event_id, consumer)
        assert row is not None
        return row.status, row.attempts

    def test_dead_letter_isolation_ordering_and_replay(self) -> None:
        e1, e2, e3 = closed_event(1, "proj:a"), closed_event(2, "proj:a"), closed_event(3, "proj:b")
        for event in (e1, e2, e3):
            self.assertEqual(self.board.bus.publish(event).consumers, ("taskboard", "hibernaut"))
        self.failing.add(e1.event_id)

        self.board.worker.run_once()
        self.assertEqual(self._status(e1.event_id, "taskboard"), ("acked", 1))
        self.assertEqual(self._status(e2.event_id, "taskboard"), ("acked", 1))
        self.assertEqual(self._status(e1.event_id, "hibernaut"), ("retry", 1))
        self.assertEqual(self._status(e2.event_id, "hibernaut"), ("pending", 0))
        self.assertEqual(self._status(e3.event_id, "hibernaut"), ("acked", 1))

        # Backoff is respected: nothing is due one second before the 5s retry.
        self.h.clock.advance(4)
        self.assertEqual(self.board.worker.run_once(), 0)
        self.h.clock.advance(1)
        self.board.worker.run_once()
        self.assertEqual(self._status(e1.event_id, "hibernaut"), ("retry", 2))

        for delay in (15, 45, 135, 405):
            self.h.clock.advance(delay)
            self.board.worker.run_once()
        self.assertEqual(self._status(e1.event_id, "hibernaut"), ("dead_letter", 6))
        row = self.board.store.get_delivery(e1.event_id, "hibernaut")
        assert row is not None
        self.assertIn("hibernaut unavailable", row.last_error or "")

        # Head-of-line: e2 waits behind the dead letter in the same scope.
        self.assertEqual(self._status(e2.event_id, "hibernaut"), ("pending", 0))
        self.assertNotIn(e2.event_id, self.hibernaut_seen)
        self.assertEqual(self.hibernaut_seen.count(e1.event_id), 6)

        # Other scopes keep flowing.
        e4 = closed_event(4, "proj:b")
        self.board.bus.publish(e4)
        self.board.worker.run_once()
        self.assertEqual(self._status(e4.event_id, "hibernaut"), ("acked", 1))

        alerts = self.board.store.list_outbound()
        self.assertEqual(len(alerts), 1)
        alert = alerts[0].message
        self.assertEqual((alert.channel, alert.kind, dict(alert.target)), ("feishu", "text", {"chat_id": "oc_alerts"}))
        self.assertIn(e1.event_id, alert.body_md)
        self.assertIn("hibernaut", alert.body_md)
        self.board.gateway.run_once()
        self.assertEqual(self.h.http.calls[0]["json"]["receive_id"], "oc_alerts")

        with self.assertRaises(ReplayError):
            self.board.worker.replay_delivery(e2.event_id, "hibernaut")

        self.failing.clear()
        replayed = self.board.worker.replay_delivery(e1.event_id, "hibernaut")
        self.assertEqual((replayed.status, replayed.attempts), ("pending", 0))
        self.hibernaut_seen.clear()
        self.board.worker.run_once()
        self.assertEqual(self.hibernaut_seen, [e1.event_id, e2.event_id])
        self.assertEqual(self._status(e1.event_id, "hibernaut"), ("acked", 1))
        self.assertEqual(self._status(e2.event_id, "hibernaut"), ("acked", 1))
        self.assertEqual(self.taskboard_seen.count(e1.event_id), 1)

    def test_offline_consumer_keeps_pending_without_attempts(self) -> None:
        self.board.transport.unregister("hibernaut")
        event = closed_event(9, "proj:a")
        self.board.bus.publish(event)
        for _ in range(3):
            self.board.worker.run_once()
            self.h.clock.advance(10)
        self.assertEqual(self._status(event.event_id, "hibernaut"), ("pending", 0))
        self.assertEqual(self._status(event.event_id, "taskboard"), ("acked", 1))

    def test_nack_and_timeout_statuses_count_as_failures(self) -> None:
        self.board.bus.subscribe("hibernaut", lambda e: "nack")
        event = closed_event(10, "proj:a")
        self.board.bus.publish(event)
        self.board.worker.run_once()
        self.assertEqual(self._status(event.event_id, "hibernaut"), ("retry", 1))

        def timeout(_: CanonicalEvent) -> None:
            raise TimeoutError("slow consumer")

        self.board.bus.subscribe("hibernaut", timeout)
        self.h.clock.advance(5)
        self.board.worker.run_once()
        row = self.board.store.get_delivery(event.event_id, "hibernaut")
        assert row is not None
        self.assertEqual((row.status, row.attempts), ("retry", 2))
        self.assertIn("slow consumer", row.last_error or "")


class BackgroundWorkerTests(unittest.TestCase):
    def test_webhook_wakes_worker_thread(self) -> None:
        h = make_harness()
        delivered = threading.Event()
        h.board.bus.subscribe("taskboard", lambda e: delivered.set() or None)
        h.board.worker.start()
        try:
            started = time.monotonic()
            status, body = send_github(h.url, issue_payload())
            self.assertEqual(status, 202)
            self.assertTrue(delivered.wait(3))
            self.assertLess(time.monotonic() - started, 3)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                row = h.board.store.get_delivery(body["event_id"], "taskboard")
                if row is not None and row.status == "acked":
                    break
                time.sleep(0.02)
            row = h.board.store.get_delivery(body["event_id"], "taskboard")
            assert row is not None
            self.assertEqual(row.status, "acked")
        finally:
            h.board.worker.stop()
            h.close()


if __name__ == "__main__":
    unittest.main()
