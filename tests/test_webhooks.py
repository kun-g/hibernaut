from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agent_runtime.adapters.channels import (
    FeishuWebhookAdapter,
    GitHubWebhookAdapter,
    WebhookHTTPServer,
    create_webhook_server,
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteInboxAdapter
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Suspend, new_id
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine.handler import EventHandler
from tests.fakes import InMemoryInbox, RecordingChannel


def _github_signature(secret: str, raw_body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


class _TrackingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str | None, str | None]] = []

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = caps
        incoming_sid = session.session_id if session is not None else None
        self.calls.append((events[0].inbox_id, incoming_sid, events[0].correlation_key))
        active_sid = incoming_sid or new_id("sess")
        return Emit(messages=(f"ok:{active_sid}",), session_id=active_sid)


class TestGitHubWebhookAdapter(unittest.TestCase):
    def test_signature_verification_dedup_and_correlation_key(self) -> None:
        inbox = InMemoryInbox()
        handler = EventHandler(inbox=inbox)
        secret = "top-secret-gh"
        adapter = GitHubWebhookAdapter(
            handler,
            webhook_secret=secret,
            repo_inbox_map={"kun-g/hibernaut": "proj:hibernaut"},
        )

        issue_body = json.dumps(
            {
                "action": "opened",
                "repository": {"full_name": "kun-g/hibernaut"},
                "issue": {"number": 42, "title": "Bug"},
            }
        ).encode("utf-8")

        # Invalid signature -> 401
        bad = adapter.handle(
            issue_body,
            {
                "X-GitHub-Event": "issues",
                "X-GitHub-Delivery": "deliv-1",
                "X-Hub-Signature-256": "sha256=deadbeef",
            },
        )
        self.assertEqual(bad.status_code, 401)
        self.assertEqual(inbox.fetch_pending("proj:hibernaut"), [])

        # Valid signature -> 202 + correlation_key extracted
        headers = {
            "X-GitHub-Event": "issues",
            "X-GitHub-Delivery": "deliv-1",
            "X-Hub-Signature-256": _github_signature(secret, issue_body),
        }
        ok = adapter.handle(issue_body, headers)
        self.assertEqual(ok.status_code, 202)
        self.assertFalse(ok.body["duplicate"])
        self.assertEqual(ok.body["inbox_id"], "proj:hibernaut")
        self.assertEqual(ok.body["correlation_key"], "github:kun-g/hibernaut#42")

        # Same X-GitHub-Delivery -> idempotent duplicate (dropped)
        dup = adapter.handle(issue_body, headers)
        self.assertEqual(dup.status_code, 202)
        self.assertTrue(dup.body["duplicate"])
        self.assertEqual(len(inbox.fetch_pending("proj:hibernaut")), 1)

        # Push event has no issue/PR number -> correlation_key is None
        push_body = json.dumps(
            {"ref": "refs/heads/main", "repository": {"full_name": "kun-g/other"}}
        ).encode("utf-8")
        push_resp = adapter.handle(
            push_body,
            {
                "X-GitHub-Event": "push",
                "X-GitHub-Delivery": "deliv-push-1",
                "X-Hub-Signature-256": _github_signature(secret, push_body),
            },
        )
        self.assertEqual(push_resp.status_code, 202)
        self.assertEqual(push_resp.body["inbox_id"], "proj:kun-g/other")
        self.assertIsNone(push_resp.body["correlation_key"])


class TestFeishuWebhookAdapter(unittest.TestCase):
    def test_url_verification_token_check_and_thread_correlation_key(self) -> None:
        inbox = InMemoryInbox()
        handler = EventHandler(inbox=inbox)
        adapter = FeishuWebhookAdapter(
            handler,
            verification_token="feishu-secret-tok",
            chat_inbox_map={"oc_chat_1": "proj:hibernaut"},
        )

        # Bad token -> 401
        bad = adapter.handle(
            json.dumps({"type": "url_verification", "token": "wrong", "challenge": "abc"}).encode()
        )
        self.assertEqual(bad.status_code, 401)

        # Valid Challenge handshake -> 200 OK with challenge echoed
        ok_challenge = adapter.handle(
            json.dumps(
                {"type": "url_verification", "token": "feishu-secret-tok", "challenge": "xyz-999"}
            ).encode()
        )
        self.assertEqual(ok_challenge.status_code, 200)
        self.assertEqual(ok_challenge.body, {"challenge": "xyz-999"})

        # Feishu v2.0 thread reply -> maps chat_id + thread_id into correlation_key
        msg_payload = {
            "schema": "2.0",
            "header": {
                "event_id": "feishu-evt-1",
                "event_type": "im.message.receive_v1",
                "token": "feishu-secret-tok",
            },
            "event": {
                "message": {
                    "chat_id": "oc_chat_1",
                    "message_id": "om_msg_2",
                    "root_id": "om_root_1",
                    "thread_id": "omt_thread_7",
                    "content": '{"text":"hello"}',
                }
            },
        }
        resp = adapter.handle(json.dumps(msg_payload).encode("utf-8"))
        self.assertEqual(resp.status_code, 202)
        self.assertEqual(resp.body["inbox_id"], "proj:hibernaut")
        self.assertEqual(resp.body["correlation_key"], "feishu:oc_chat_1:omt_thread_7")


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
        thread = threading.Thread(target=server.serve_forever, daemon=True)
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
        stopper.join(timeout=0.3)
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

    async def test_multi_route_http_github_continues_session_and_feishu_resumes_suspend(self) -> None:
        channel = RecordingChannel()
        daemon = build_daemon(
            db_path=self.root / "wh.db",
            memory_root=self.root / "mem_wh",
            max_concurrency=2,
            channel=channel,
            github_webhook_secret="gh-sec",
            github_repo_inbox_map={"kun-g/hibernaut": "proj:hibernaut"},
            feishu_verification_token="fs-tok",
            feishu_chat_inbox_map={"oc_ops": "proj:ops"},
        )
        server = create_webhook_server(
            "127.0.0.1",
            0,
            daemon.handler,
            github_adapter=daemon.github_webhook,
            feishu_adapter=daemon.feishu_webhook,
        )
        port = server.server_address[1]
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
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
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            suspend_reports = [r for r in daemon.pool.reports if isinstance(r.outcome, Suspend)]
            self.assertEqual(len(suspend_reports), 1)
            assert isinstance(suspend_reports[0].outcome, Suspend)
            req_id = suspend_reports[0].outcome.request_id

            # 2. Feishu interactive approval card callback via /webhooks/feishu resumes Suspend!
            feishu_card = json.dumps(
                {
                    "schema": "2.0",
                    "header": {
                        "event_id": "card-evt-1",
                        "event_type": "card.action.trigger",
                        "token": "fs-tok",
                    },
                    "event": {
                        "context": {"open_chat_id": "oc_ops", "open_message_id": "om_card_1"},
                        "action": {
                            "value": {
                                "approval_request_id": req_id,
                                "approved": True,
                            }
                        },
                    },
                }
            ).encode("utf-8")
            fs_status, _ = await asyncio.to_thread(_post, "/webhooks/feishu", feishu_card)
            self.assertEqual(fs_status, 202)
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            await daemon.dispatcher.dispatch_once()

            emit_reports = [r for r in daemon.pool.reports if isinstance(r.outcome, Emit)]
            self.assertEqual(len(emit_reports), 1)
            assert isinstance(emit_reports[0].outcome, Emit)
            self.assertIn("release@example.com", emit_reports[0].outcome.messages[0])

            # 3. Unknown path returns 404
            nf_status, _ = await asyncio.to_thread(_post, "/webhooks/unknown", b"{}")
            self.assertEqual(nf_status, 404)
        finally:
            stop.set()
            await daemon_task
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=2.0)
            daemon.close()

    async def test_github_pr_events_auto_continue_same_session(self) -> None:
        tracker = _TrackingExecutor()
        daemon = build_daemon(
            db_path=self.root / "gh_pr.db",
            memory_root=self.root / "mem_gh_pr",
            max_concurrency=2,
            executor_factory=lambda _inbox_id: tracker,
            github_webhook_secret="sec-pr",
            github_repo_inbox_map={"kun-g/hibernaut": "proj:hibernaut"},
        )
        assert daemon.github_webhook is not None
        stop = asyncio.Event()
        daemon_task = asyncio.create_task(
            daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
        )
        try:
            pr_open = json.dumps(
                {
                    "action": "opened",
                    "repository": {"full_name": "kun-g/hibernaut"},
                    "pull_request": {"number": 7, "title": "Add webhooks"},
                }
            ).encode("utf-8")
            r1 = daemon.github_webhook.handle(
                pr_open,
                {
                    "X-GitHub-Event": "pull_request",
                    "X-GitHub-Delivery": "pr-deliv-1",
                    "X-Hub-Signature-256": _github_signature("sec-pr", pr_open),
                },
            )
            self.assertEqual(r1.status_code, 202)
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            first_sid = daemon.pool.reports[0].session_id
            self.assertIsNotNone(first_sid)

            pr_review = json.dumps(
                {
                    "action": "submitted",
                    "repository": {"full_name": "kun-g/hibernaut"},
                    "pull_request": {"number": 7},
                }
            ).encode("utf-8")
            r2 = daemon.github_webhook.handle(
                pr_review,
                {
                    "X-GitHub-Event": "pull_request_review",
                    "X-GitHub-Delivery": "pr-deliv-2",
                    "X-Hub-Signature-256": _github_signature("sec-pr", pr_review),
                },
            )
            self.assertEqual(r2.status_code, 202)
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            self.assertEqual(tracker.calls[0], ("proj:hibernaut", None, "github:kun-g/hibernaut#7"))
            self.assertEqual(
                tracker.calls[1],
                ("proj:hibernaut", first_sid, "github:kun-g/hibernaut#7"),
            )
        finally:
            stop.set()
            await daemon_task
            daemon.close()


if __name__ == "__main__":
    unittest.main()
