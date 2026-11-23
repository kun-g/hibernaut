from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agent_runtime.adapters.channels import WebhookHTTPServer, create_webhook_server
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteInboxAdapter
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit, Event, Suspend
from agent_runtime.engine.handler import EventHandler
from agent_runtime.engine.worker import RunReport
from tests.fakes import InMemoryInbox, RecordingChannel


class _GatedInbox(InMemoryInbox):
    def __init__(self, gate: threading.Barrier | threading.Event) -> None:
        super().__init__()
        self.entered = threading.Event()
        self._gate = gate

    def append(self, event: Event) -> tuple[Event, bool]:
        self.entered.set()
        if isinstance(self._gate, threading.Barrier):
            self._gate.wait()
        else:
            self._gate.wait(timeout=5.0)
        return super().append(event)


def _post_event(port: int, external_id: str) -> tuple[int, dict[str, object]]:
    body = json.dumps({"inbox_id": "proj:stress", "source": "stress", "external_id": external_id})
    req = urllib.request.Request(
        url=f"http://127.0.0.1:{port}/events",
        data=body.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10.0) as resp:
        return int(resp.status), json.loads(resp.read().decode("utf-8"))


class TestWebhookServerConcurrency(unittest.TestCase):
    def _serve(self, handler: EventHandler) -> tuple[int, WebhookHTTPServer]:
        server = create_webhook_server("127.0.0.1", 0, handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        return server.server_address[1], server

    def test_requests_are_handled_in_parallel(self) -> None:
        # Both requests must be inside ingest at once to pass the barrier.
        inbox = _GatedInbox(threading.Barrier(2, timeout=5.0))
        port, server = self._serve(EventHandler(inbox))
        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses = [s for s, _ in pool.map(lambda k: _post_event(port, k), ["a", "b"])]
        server.shutdown()
        self.assertEqual(statuses, [202, 202])

    def test_burst_of_concurrent_posts_is_accepted_and_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory(prefix="webhook_burst_") as tmp:
            db = SQLiteDatabase(Path(tmp) / "burst.db")
            inbox = SQLiteInboxAdapter(db)
            port, server = self._serve(EventHandler(inbox))
            keys = [f"evt-{idx % 100}" for idx in range(400)]
            with ThreadPoolExecutor(max_workers=64) as pool:
                results = list(pool.map(lambda k: _post_event(port, k), keys))
            server.shutdown()

            self.assertTrue(all(status == 202 for status, _ in results))
            self.assertEqual(sum(1 for _, body in results if not body["duplicate"]), 100)
            self.assertEqual(len(inbox.fetch_pending("proj:stress")), 100)
            db.close()

    def test_shutdown_waits_for_in_flight_requests(self) -> None:
        release = threading.Event()
        inbox = _GatedInbox(release)
        port, server = self._serve(EventHandler(inbox))
        results: list[int] = []
        client = threading.Thread(target=lambda: results.append(_post_event(port, "slow")[0]))
        client.start()
        self.assertTrue(inbox.entered.wait(timeout=5.0))

        stopper = threading.Thread(target=server.shutdown)
        stopper.start()
        stopper.join(timeout=0.05)
        self.assertTrue(stopper.is_alive())

        release.set()
        stopper.join(timeout=5.0)
        client.join(timeout=5.0)
        self.assertFalse(stopper.is_alive())
        self.assertEqual(results, [202])
        self.assertEqual(len(inbox.fetch_pending("proj:stress")), 1)


class TestWebhooksEndToEnd(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="webhooks_e2e_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def test_http_events_suspend_and_resume(self) -> None:
        channel = RecordingChannel()
        daemon = build_daemon(
            db_path=self.root / "wh.db",
            memory_root=self.root / "mem_wh",
            max_concurrency=2,
            channel=channel,
        )
        server = create_webhook_server("127.0.0.1", 0, daemon.handler)
        port = server.server_address[1]
        server_thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        server_thread.start()

        stop = asyncio.Event()
        daemon_task = asyncio.create_task(
            daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
        )

        def _post(path: str, body_bytes: bytes, headers: dict[str, str] | None = None) -> tuple[int, dict[str, object]]:
            req = urllib.request.Request(
                url=f"http://127.0.0.1:{port}{path}",
                data=body_bytes,
                method="POST",
                headers={"Content-Type": "application/json", **(headers or {})},
            )
            try:
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    return int(resp.status), json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as err:
                return int(err.code), json.loads(err.read().decode("utf-8"))

        async def _reported(kind: type[Suspend] | type[Emit]) -> list[RunReport]:
            # Not wait_until_idle(): a 202 means the event is stored, its wake may not have reached the loop yet.
            async with asyncio.timeout(10):
                while not (found := [r for r in daemon.pool.reports if isinstance(r.outcome, kind)]):
                    await asyncio.sleep(0.02)
            return found

        try:
            # 1. Trigger a Suspend on proj:ops via generic /events route
            status, _ = await asyncio.to_thread(
                _post,
                "/events",
                json.dumps(
                    {
                        "inbox_id": "proj:ops",
                        "source": "im",
                        "external_id": "ops-mail-1",
                        "payload": {
                            "action": "send_email",
                            "to": "release@example.com",
                            "subject": "Ship",
                        },
                    }
                ).encode("utf-8"),
            )
            self.assertEqual(status, 202)
            suspend_reports = await _reported(Suspend)
            self.assertEqual(len(suspend_reports), 1)
            assert isinstance(suspend_reports[0].outcome, Suspend)
            req_id = suspend_reports[0].outcome.request_id

            approval = json.dumps({
                "inbox_id": "proj:ops", "external_id": "ops-approval-1",
                "payload": {"approval_request_id": req_id, "approved": True},
            }).encode("utf-8")
            status, _ = await asyncio.to_thread(_post, "/events", approval)
            self.assertEqual(status, 202)
            emit_reports = await _reported(Emit)
            await daemon.dispatcher.dispatch_once()
            self.assertEqual(len(emit_reports), 1)
            assert isinstance(emit_reports[0].outcome, Emit)
            self.assertIn("release@example.com", emit_reports[0].outcome.messages[0])

            for path in ("/webhooks/github", "/webhooks/feishu", "/webhooks/unknown"):
                with self.subTest(path=path):
                    status, _ = await asyncio.to_thread(_post, path, b"{}")
                    self.assertEqual(status, 404)
        finally:
            stop.set()
            await daemon_task
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=2.0)
            daemon.close()

if __name__ == "__main__":
    unittest.main()
