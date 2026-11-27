import json
from datetime import datetime, timedelta, UTC

from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter, SQLiteTimerAdapter
from agent_runtime.core.models import Emit, Suspend, to_utc_iso
from agent_runtime.engine.scheduler import TimerScheduler
from agent_runtime.engine.worker import AgentWorker
from tests.support.fakes import InMemoryInbox, InMemoryLock, InMemoryMemoryStore, InMemoryOutbox
from tests.support.sde_delivery import PASS, SDEDeliveryCase, dev, event


class TestDeliveryRetry(SDEDeliveryCase):
    def start_delivery(self, failures, error, step="create", **delivery_options):
        self.state_file.write_text(json.dumps({**self.gh_state(), "fail": {step: failures}, "fail_error": error}))
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
        start = datetime.now(UTC)
        [report] = await self.worker.process_inbox("proj:x")
        assert isinstance(report.outcome, Suspend)
        assert not [m for m in self.outbox.fetch_pending() if m.metadata.get("kind") == "approval_request"], \
            "transient delivery failure asked a person"
        assert self.lock.acquire("proj:x", "another-worker", 60)
        self.lock.release("proj:x", "another-worker")
        assert self.scheduler.tick(to_utc_iso(start + timedelta(minutes=minutes, seconds=-1))) == 0
        assert self.scheduler.tick(to_utc_iso(datetime.now(UTC) + timedelta(minutes=minutes))) == 1

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
        self.start_delivery(4, 'Post "https://api.github.com/graphql": EOF', step="checks")
        for delay in (1, 5, 15):
            await self.advance_retry(delay)
        state = self.gh_state()
        state["fail_error"] = "connection closed by peer"
        self.state_file.write_text(json.dumps(state))
        [report] = await self.worker.process_inbox("proj:x")
        assert isinstance(report.outcome, Suspend)
        [ask] = [m for m in self.outbox.fetch_pending() if m.metadata.get("kind") == "approval_request"]
        summary, approve, reject, details = ask.content.split("\n", 3)
        assert all(s in summary for s in ("PR #1", "检查状态", "连接中断"))
        assert "批准：" in approve and "合并" in approve
        assert "拒绝：" in reject and "合并" in reject and "结束" in reject
        assert details.lstrip().startswith("详情")
        assert "1、5、15 分钟" in details and state["fail_error"] in details
        assert self.scheduler.tick("9999") == 0
        assert len(self.gh_calls("checks")) == 4

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
