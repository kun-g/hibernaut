import json
from datetime import datetime, timedelta, timezone

from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter, SQLiteTimerAdapter
from agent_runtime.core.models import Emit, Suspend, to_utc_iso
from agent_runtime.engine.scheduler import TimerScheduler
from agent_runtime.engine.worker import AgentWorker
from tests.support.fakes import InMemoryInbox, InMemoryLock, InMemoryMemoryStore, InMemoryOutbox
from tests.support.sde_delivery import PASS, SDEDeliveryCase, dev, event


class TestDeliveryRetry(SDEDeliveryCase):
    def start_delivery(self, failures, error, **delivery_options):
        self.state_file.write_text(json.dumps({**self.gh_state(), "fail": {"create": failures}, "fail_error": error}))
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})], reviewer=[PASS])
        self.inbox, self.outbox, self.lock = InMemoryInbox(), InMemoryOutbox(), InMemoryLock()
        self.timers = SQLiteTimerAdapter(self.db)
        self.scheduler = TimerScheduler(inbox=self.inbox, timers=self.timers)
        self.worker = AgentWorker(worker_id="w", inbox=self.inbox, outbox=self.outbox, timers=self.timers,
                                  memory=InMemoryMemoryStore(), permissions=self.caps.permissions, lock=self.lock,
                                  sessions=SQLiteSessionStoreAdapter(self.db),
                                  executor=self.executor(delivery=self.delivery(auto_merge=True, **delivery_options)))
        self.inbox.append(event(self.payload()))

    async def advance_retry(self, minutes):
        start = datetime.now(timezone.utc)
        [report] = await self.worker.process_inbox("proj:x")
        assert isinstance(report.outcome, Suspend)
        assert not [m for m in self.outbox.fetch_pending() if m.metadata.get("kind") == "approval_request"], \
            "transient delivery failure asked a person"
        assert self.lock.acquire("proj:x", "another-worker", 60)
        self.lock.release("proj:x", "another-worker")
        assert self.scheduler.tick(to_utc_iso(start + timedelta(minutes=minutes, seconds=-1))) == 0
        assert self.scheduler.tick(to_utc_iso(datetime.now(timezone.utc) + timedelta(minutes=minutes))) == 1

    async def test_two_transient_failures_then_merge_without_approval(self):
        self.start_delivery(2, "Could not resolve host: github.com")
        await self.advance_retry(1)
        await self.advance_retry(5)
        [report] = await self.worker.process_inbox("proj:x")
        assert isinstance(report.outcome, Emit) and report.outcome.delivery.state == "merged"
        assert len(self.gh_calls("create")) == 3
        assert len(self.gh_calls("merge")) == len(self.gh_state()["prs"]) == 1
        assert len(self.calls("sde")) == len(self.calls("reviewer")) == 1
        assert self.check_log.read_text().splitlines() == ["ran"]

    async def test_persistent_failure_asks_once_with_delays_and_latest_error(self):
        self.start_delivery(4, "HTTP 502: Bad Gateway")
        for delay in (1, 5, 15):
            await self.advance_retry(delay)
        state = self.gh_state()
        state["fail_error"] = "HTTP 503: temporarily unavailable"
        self.state_file.write_text(json.dumps(state))
        [report] = await self.worker.process_inbox("proj:x")
        assert isinstance(report.outcome, Suspend)
        [ask] = [m for m in self.outbox.fetch_pending() if m.metadata.get("kind") == "approval_request"]
        assert "3 automatic retries (1, 5, 15 min)" in ask.content
        assert "HTTP 503: temporarily unavailable" in ask.content
        assert self.scheduler.tick("9999") == 0
        assert len(self.gh_calls("create")) == 4

    async def test_forbidden_asks_immediately(self):
        self.start_delivery(1, "HTTP 403: Resource not accessible by integration")
        [report] = await self.worker.process_inbox("proj:x")
        assert isinstance(report.outcome, Suspend)
        [ask] = [m for m in self.outbox.fetch_pending() if m.metadata.get("kind") == "approval_request"]
        assert "HTTP 403" in ask.content
        assert self.scheduler.tick("9999") == 0
        assert len(self.gh_calls("create")) == 1

    async def test_git_fetch_timeout_resumes_before_checks_or_push(self):
        proxy = self.root / "git-wrapper"
        proxy.write_text('#!/bin/sh\nif [ "$1" = fetch ] && [ ! -f "$0.failed" ]; then\n'
                         '  touch "$0.failed"; echo "fatal: connection timed out" >&2; exit 1\n'
                         'fi\nexec git "$@"\n')
        proxy.chmod(0o700)
        self.start_delivery(0, "", git_command=str(proxy))
        await self.advance_retry(1)
        assert not self.check_log.exists()
        [report] = await self.worker.process_inbox("proj:x")
        assert isinstance(report.outcome, Emit) and report.outcome.delivery.state == "merged"

    async def test_github_secondary_rate_limit_is_retryable_even_with_http_403(self):
        self.start_delivery(1, "HTTP 403: You have exceeded a secondary rate limit.")
        await self.advance_retry(1)
        [report] = await self.worker.process_inbox("proj:x")
        assert isinstance(report.outcome, Emit) and report.outcome.delivery.state == "merged"
