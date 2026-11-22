from __future__ import annotations

import asyncio
import io
import json
import subprocess
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from email.message import Message
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import GitHubIssuePoller, PollTarget, WebhookHTTPServer
from agent_runtime.app.main import build_daemon
from agent_runtime.engine.handler import EventHandler
from tests.fakes import InMemoryInbox

LOGGER = "agent_runtime.adapters.channels.github_poller"


def _issue(number: int, updated_at: str = "2026-11-22T00:00:00Z") -> dict[str, Any]:
    return {"number": number, "title": f"Issue {number}", "body": "", "labels": [], "updatedAt": updated_at, "url": ""}


def _gh_failure() -> subprocess.CalledProcessError:
    return subprocess.CalledProcessError(
        1,
        ["gh", "issue", "list"],
        output="",
        stderr="error connecting to api.github.com\ncheck your internet connection",
    )


class _FlakyFetcher:
    def __init__(self, failures: list[BaseException], issues: list[dict[str, Any]]) -> None:
        self._failures = list(failures)
        self._issues = issues
        self.calls = 0
        self._lock = threading.Lock()

    def __call__(self, _target: PollTarget) -> list[dict[str, Any]]:
        with self._lock:
            self.calls += 1
            if self._failures:
                raise self._failures.pop(0)
        return self._issues


class TestGitHubPollerResilience(unittest.IsolatedAsyncioTestCase):
    async def test_transient_failures_are_logged_retried_and_polling_recovers(self) -> None:
        inbox = InMemoryInbox()
        fetcher = _FlakyFetcher([_gh_failure(), _gh_failure()], [_issue(7)])
        poller = GitHubIssuePoller(
            EventHandler(inbox=inbox),
            targets=[PollTarget(repo="kun-g/hibernaut", inbox_id="proj:hibernaut")],
            fetcher=fetcher,
            retry_initial_seconds=0.01,
            retry_max_seconds=0.05,
        )
        stop = asyncio.Event()
        with self.assertLogs(LOGGER, level="WARNING") as logs:
            # A 30s interval proves recovery comes from the retry delay, not the regular schedule.
            task = asyncio.create_task(poller.run_forever(interval_seconds=30.0, stop_event=stop))
            for _ in range(200):
                if inbox.fetch_pending("proj:hibernaut"):
                    break
                await asyncio.sleep(0.01)
            stop.set()
            await asyncio.wait_for(task, timeout=2.0)

        self.assertEqual(fetcher.calls, 3)
        self.assertEqual(len(inbox.fetch_pending("proj:hibernaut")), 1)
        output = "\n".join(logs.output)
        self.assertIn("kun-g/hibernaut", output)
        self.assertIn("exit 1", output)
        self.assertIn("error connecting to api.github.com", output)
        self.assertIn("retry in", output)

    async def test_api_error_diagnostics_include_status_and_body(self) -> None:
        body = io.BytesIO(b'{"message": "API rate limit exceeded"}')
        err = urllib.error.HTTPError("https://api.github.com/x", 403, "Forbidden", Message(), body)
        poller = GitHubIssuePoller(
            EventHandler(inbox=InMemoryInbox()),
            targets=[PollTarget(repo="kun-g/hibernaut")],
            mode="api",
            fetcher=_FlakyFetcher([err], []),
            retry_initial_seconds=0.01,
        )
        stop = asyncio.Event()
        with self.assertLogs(LOGGER, level="WARNING") as logs:
            task = asyncio.create_task(poller.run_forever(interval_seconds=30.0, stop_event=stop))
            await asyncio.sleep(0.1)
            stop.set()
            await asyncio.wait_for(task, timeout=2.0)
        output = "\n".join(logs.output)
        self.assertIn("HTTP 403", output)
        self.assertIn("API rate limit exceeded", output)

    def test_one_failing_target_does_not_block_others(self) -> None:
        inbox = InMemoryInbox()

        def fetch(target: PollTarget) -> list[dict[str, Any]]:
            if target.repo == "kun-g/broken":
                raise urllib.error.URLError("timed out")
            return [_issue(1)]

        poller = GitHubIssuePoller(
            EventHandler(inbox=inbox),
            targets=[PollTarget(repo="kun-g/broken", inbox_id="proj:broken"), PollTarget(repo="kun-g/ok", inbox_id="proj:ok")],
            fetcher=fetch,
        )
        with self.assertLogs(LOGGER, level="WARNING"):
            with self.assertRaises(Exception):
                poller.poll_once()
        self.assertEqual(len(inbox.fetch_pending("proj:ok")), 1)

    def test_retry_delay_backs_off_and_is_capped(self) -> None:
        poller = GitHubIssuePoller(
            EventHandler(inbox=InMemoryInbox()),
            targets=[],
            retry_initial_seconds=5.0,
            retry_max_seconds=60.0,
        )
        self.assertEqual([poller.retry_delay(n) for n in range(1, 7)], [5.0, 10.0, 20.0, 40.0, 60.0, 60.0])


class TestDaemonSurvivesPollFailures(unittest.IsolatedAsyncioTestCase):
    async def test_events_accepted_during_outage_then_polling_recovers_and_shutdown_is_clean(self) -> None:
        with tempfile.TemporaryDirectory(prefix="poll_outage_") as tmp:
            root = Path(tmp)
            daemon = build_daemon(db_path=root / "d.db", memory_root=root / "mem", ingest_only=True)
            outage = threading.Event()
            outage.set()
            calls: list[int] = []

            def fetch(_target: PollTarget) -> list[dict[str, Any]]:
                calls.append(1)
                if outage.is_set():
                    raise _gh_failure()
                return [_issue(42)]

            daemon.github_poller = GitHubIssuePoller(
                daemon.handler,
                targets=[PollTarget(repo="kun-g/hibernaut", inbox_id="proj:hibernaut")],
                fetcher=fetch,
                retry_initial_seconds=0.02,
                retry_max_seconds=0.05,
            )
            daemon.poll_interval_seconds = 30.0
            server = WebhookHTTPServer(("127.0.0.1", 0), daemon.http_handler())
            port = server.server_address[1]
            stop = asyncio.Event()

            def post() -> int:
                body = json.dumps({"inbox_id": "proj:other", "source": "test", "external_id": "evt-1"})
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}/events",
                    data=body.encode(),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    return int(resp.status)

            with self.assertLogs(LOGGER, level="WARNING"):
                task = asyncio.create_task(
                    daemon.run(server=server, stop_event=stop, timer_interval_seconds=0.05, outbox_interval_seconds=0.05)
                )
                try:
                    for _ in range(200):
                        if len(calls) >= 3:
                            break
                        await asyncio.sleep(0.01)
                    self.assertGreaterEqual(len(calls), 3)
                    self.assertFalse(task.done())
                    self.assertEqual(await asyncio.to_thread(post), 202)
                    self.assertEqual(len(daemon.inbox.fetch_pending("proj:other")), 1)

                    outage.clear()
                    for _ in range(200):
                        if daemon.inbox.fetch_pending("proj:hibernaut"):
                            break
                        await asyncio.sleep(0.01)
                    self.assertEqual(len(daemon.inbox.fetch_pending("proj:hibernaut")), 1)
                finally:
                    stop.set()
                    await asyncio.wait_for(task, timeout=5.0)
                    server.server_close()
                    daemon.close()
            self.assertIsNone(task.exception())


if __name__ == "__main__":
    unittest.main()
