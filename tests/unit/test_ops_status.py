from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, UTC
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from agent_runtime.adapters.executor_cli.sde import WAIT_IDLE
from agent_runtime.adapters.executor_cli.steward_tools import StewardTools
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.adapters.storage_sqlite.ops_status import RunSummary, SQLiteOpsStatus
from agent_runtime.app.reload import ReloadStatus
from agent_runtime.app.system_tools import SystemToolbox
from agent_runtime.core.models import TASK_CONTEXT_KEY, ConfiguredTimer, Event, SessionRef, to_utc_iso
from agent_runtime.engine.scheduler import TimerScheduler

# Placeholders, not real credentials.
SECRET = "placeholder-ops-secret-4711"
GH_TOKEN = "ghp_placeholder0000000000"
BODY = "the full text of a person's message, which no summary repeats"
TIMERS = (ConfiguredTimer("model-policy", "sys:model-policy", 900, {"probe": BODY}, True),
          ConfiguredTimer("idle-work", "sys:idle-work", 180, {}, True),
          ConfiguredTimer("off", "sys:off", 60, {}, False))


def sde_checkpoint(stage: str, reason: str) -> str:
    # The fields _State requires, so the checkpoint loads the way the SDE wrote it.
    return json.dumps({"sde": {"stage": stage, "reason": reason, "requirements": BODY, "workdir": "/w", "base_ref": "b"}})


class OpsStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.db = SQLiteDatabase(str(root / "x.db"))
        self.addCleanup(self.db.close)
        self.inbox, self.outbox = SQLiteInboxAdapter(self.db), SQLiteOutboxAdapter(self.db)
        self.sessions, self.timers = SQLiteSessionStoreAdapter(self.db), SQLiteTimerAdapter(self.db)
        self.runs: dict[str, RunSummary] = {}
        self.reader = SQLiteOpsStatus(
            self.db, timers=lambda: TIMERS, sde_agents=lambda: frozenset({"sde"}),
            delivers=lambda inbox: inbox.startswith("proj:a"), runs=self.runs.get,
        )
        self.enterContext(patch.dict(os.environ, {"OPS_TEST_TOKEN": SECRET}))
        self.fill()

    def event(self, inbox: str, source: str, external_id: str, **payload: Any) -> Event:
        stored, _ = self.inbox.append(Event(inbox_id=inbox, source=source, external_id=external_id,
                                            payload={"body": BODY, "text": BODY, **payload}))
        return stored

    def failed_event(self, event: Event, error: str) -> None:
        self.inbox.claim_pending(event.inbox_id, [event.event_id])
        self.inbox.mark_failed([event.event_id], error)

    def suspend(self, session: str, inbox: str, checkpoint: str, request: str, agent: str = "sde") -> None:
        self.sessions.save(SessionRef(session_id=session, inbox_id=inbox, executor_id=agent, status="suspended",
                                      checkpoint=checkpoint, pending_request_id=request, work_key=session))

    def result(self, inbox: str, title: str, status: str, agent: str = "sde", **delivery: Any) -> None:
        context: dict[str, Any] = {"title": title, "executor_id": agent, "status": status, "source": "agent:forward"}
        if delivery:
            context["delivery"] = {"state": "local", "pr_url": "", "auto_merge": True, "hold": "", "hold_detail": "",
                                   **delivery}
        self.outbox.enqueue(inbox, f"{BODY} {SECRET}", {"correlation_key": "k", "session_id": "s",
                                                        TASK_CONTEXT_KEY: context})

    def fill(self) -> None:
        self.failed_event(self.event("proj:a:1", "agent:forward", "e1", title="修除法"),
                  f"claude exited 1 with {SECRET} and {GH_TOKEN}\nTraceback line two")
        self.event("proj:a:2", "agent:forward", "e2", title="还在排队")
        self.suspend("s-idle", "proj:a:3", sde_checkpoint("needs_input", f"{WAIT_IDLE} 等用户离开电脑再计时"), "r1")
        self.suspend("s-ask", "proj:a:4", sde_checkpoint("needs_input", "选 A 还是 B？"), "r2")
        self.suspend("s-review", "proj:a:5", sde_checkpoint("no_reviewer", "no review candidate is available"), "r3")
        self.suspend("s-retry", "proj:a:6", sde_checkpoint("delivery_retry", "gh: connection reset"), "r4")
        self.timers.schedule("follow_up", "2099-01-01T00:00:00+00:00", {"approval_request_id": "r4"},
                             inbox_id="proj:a:6", session_id="s-retry")
        self.suspend("s-steward", "proj:b", "{}", "r5", agent="steward")
        self.outbox.enqueue("proj:b", f"approve forwarding to proj:a? {SECRET}\n{BODY}",
                            {"request_id": "r5", "kind": "approval_request", "session_id": "s-steward"})
        self.result("proj:a:7", "合并的", "done", state="merged", pr_url="https://github.com/acme/a/pull/7")
        self.result("proj:a:8", "等合并", "done", state="pr_open", pr_url="https://github.com/acme/a/pull/8",
                    hold="ci", hold_detail="CI failed: test_x")
        self.result("proj:a:9", "没改动的", "done")
        self.result("proj:c:1", "无交付步骤", "done")
        self.result("proj:a:10", "失败的", "failed")
        self.result("user:main", "Steward 的答复", "done", agent="steward")
        stuck = self.outbox.enqueue("user:main", BODY, {TASK_CONTEXT_KEY: {"title": "发不出去"}, "kind": "emit"})
        self.outbox.mark_failed(stuck.message_id, f"feishu 99991663 token {SECRET} invalid")
        retrying = self.outbox.enqueue("user:main", BODY, {"kind": "blocker_alert"})
        self.outbox.schedule_retry(retrying.message_id, "timeout", "2099-01-01T00:00:00+00:00")
        self.timers.reconcile(TIMERS)
        scheduler = TimerScheduler(timers=self.timers, inbox=self.inbox)
        start = datetime.now(UTC)
        # A timer fires again only once its last event has been taken.
        for n, end in enumerate(("done", "failed", None), start=1):
            scheduler.tick(to_utc_iso(start + timedelta(seconds=900 * n + 1)))
            if end is None:
                continue
            [fired] = self.inbox.claim_pending("sys:model-policy")
            if end == "done":
                self.inbox.mark_done([fired.event_id])
                self.runs[fired.event_id] = RunSummary(result="silent", error="", seconds=3.5)
            else:
                self.inbox.mark_failed([fired.event_id], f"exit 2: {SECRET}")
                self.runs[fired.event_id] = RunSummary(result="error", error="model_policy.py exited 2", seconds=1.0)

    def report(self, **kwargs: Any) -> dict[str, Any]:
        return self.reader.report(hours=24, timer_runs=2, **kwargs)

    def test_new_r_failed_tasks_with_their_error_and_no_payload(self) -> None:
        failed = self.report()["failed"]
        # The timer firing that failed is one of them.
        self.assertEqual(failed["count"], 3)
        firing, error, ended = failed["items"]
        self.assertEqual((firing["inbox"], firing["source"]), ("sys:model-policy", "timer:model-policy"))
        self.assertEqual((error["inbox"], error["what"], error["how"]), ("proj:a:1", "修除法", "run error"))
        self.assertTrue(error["error"].startswith("claude exited 1"))
        self.assertNotIn("Traceback", error["error"])
        self.assertEqual((ended["inbox"], ended["what"], ended["how"]), ("proj:a:10", "失败的", "ended failed"))

    def test_new_r_suspended_tasks_say_what_they_wait_for(self) -> None:
        found = {item["session"]: item for item in self.report()["suspended"]["items"]}
        self.assertEqual({s: i["waits_for"] for s, i in found.items()}, {
            "s-idle": "user_away", "s-ask": "answer", "s-review": "approval", "s-retry": "retry",
            "s-steward": "approval",
        })
        self.assertIn("等用户离开电脑", found["s-idle"]["why"])
        self.assertEqual(found["s-ask"]["why"], "选 A 还是 B？")
        self.assertEqual(found["s-retry"]["retry_at"], "2099-01-01T00:00:00+00:00")
        self.assertEqual(found["s-steward"]["why"], f"approve forwarding to proj:a? {SECRET}")
        self.assertEqual(found["s-review"]["stage"], "no_reviewer")

    def test_new_r_deliveries_merged_waiting_unchanged_failed(self) -> None:
        deliveries = self.report()["deliveries"]
        self.assertEqual(deliveries["by_result"], {"merged": 1, "pr_open": 1, "no_change": 1, "no_delivery_step": 1,
                                                   "failed": 1})
        found = {item["what"]: item for item in deliveries["items"]}
        self.assertEqual(found["合并的"]["pr"], "https://github.com/acme/a/pull/7")
        self.assertEqual(found["等合并"]["hold"], "ci: CI failed: test_x")
        self.assertNotIn("Steward 的答复", found)

    def test_new_r_messages_that_did_not_go_out(self) -> None:
        undelivered = self.report()["undelivered_messages"]
        self.assertEqual(undelivered["count"], 2)
        states = {item["state"]: item for item in undelivered["items"]}
        self.assertEqual(states["failed"]["what"], "发不出去")
        self.assertIn("99991663", states["failed"]["error"])
        self.assertEqual((states["retrying"]["kind"], states["retrying"]["next_retry_at"]),
                         ("blocker_alert", "2099-01-01T00:00:00+00:00"))

    def test_new_r_each_timer_s_last_firings_and_their_results(self) -> None:
        timers = {t["name"]: t for t in self.report()["timers"]}
        self.assertEqual(list(timers), ["model-policy", "idle-work", "off"])
        policy = timers["model-policy"]
        self.assertEqual((policy["inbox"], policy["every_seconds"], policy["enabled"]), ("sys:model-policy", 900, True))
        self.assertTrue(policy["next_due"])
        self.assertEqual([f["status"] for f in policy["fired"]], ["pending", "failed"])
        self.assertEqual(policy["fired"][1]["result"], "error")
        self.assertTrue(policy["fired"][1]["error"].startswith("exit 2"))
        older = self.reader.report(hours=24, timer_runs=3)["timers"][0]["fired"][2]
        self.assertEqual((older["status"], older["result"], older["seconds"]), ("done", "silent", 3.5))
        self.assertLess(policy["fired"][1]["at"], policy["fired"][0]["at"])
        # Its first firing waits for its Run, so the later ticks queued no more.
        self.assertEqual([(f["status"], "result" in f) for f in timers["idle-work"]["fired"]], [("pending", False)])
        self.assertEqual((timers["off"]["enabled"], timers["off"]["next_due"]), (False, None))

    def test_new_r_it_says_what_time_it_covers_and_what_it_cannot_see(self) -> None:
        report = self.report()
        self.assertEqual(report["window"]["hours"], 24)
        self.assertLess(report["window"]["from"], report["window"]["to"])
        self.assertTrue(report["not_covered"])
        old = self.reader.report(hours=1, timer_runs=1, now=datetime.now(UTC) + timedelta(hours=2))
        self.assertEqual((old["failed"]["count"], old["deliveries"]["by_result"]), (0, {}))
        # What waits now waits whenever it began.
        self.assertEqual(old["suspended"]["count"], 5)
        without = SQLiteOpsStatus(self.db, timers=lambda: TIMERS, sde_agents=frozenset, delivers=lambda _i: False)
        self.assertTrue(any("ledger" in line for line in without.report(hours=1, timer_runs=1)["not_covered"]))

    def test_c2_the_tool_answers_with_no_payload_and_no_secret(self) -> None:
        box = SystemToolbox(reloads=ReloadStatus(), allowed=lambda: True, status=self.reader)
        said = box.status(hours=24, timer_runs=5)
        self.assertEqual(json.loads(said)["failed"]["count"], 3)
        for hidden in (BODY, SECRET, GH_TOKEN, "requirements"):
            self.assertNotIn(hidden, said)
        with self.assertRaisesRegex(Exception, "no system tools"):
            SystemToolbox(reloads=ReloadStatus(), allowed=lambda: False, status=self.reader).status(hours=1, timer_runs=1)


class ToolTests(unittest.IsolatedAsyncioTestCase):
    def test_c1_it_is_one_more_read_only_tool_with_named_values(self) -> None:
        found = {t["name"]: t["inputSchema"] for t in StewardTools.definitions(projectless=True, system=True)}
        self.assertEqual(set(found["runtime_status"]["properties"]), {"hours", "timer_runs"})
        self.assertIs(found["runtime_status"]["additionalProperties"], False)
        self.assertNotIn("runtime_status", {t["name"] for t in StewardTools.definitions(projectless=True)})
        self.assertEqual(AutoModePermissionAdapter().check("runtime_status", {}).action, "allow")

    async def test_c1_every_call_passes_the_permission_gate_and_takes_bounded_values(self) -> None:
        seen: list[tuple[str, dict[str, Any]]] = []

        class Gate(AutoModePermissionAdapter):
            def check(self, tool_name: str, args: dict[str, Any]):  # type: ignore[no-untyped-def]
                seen.append((tool_name, dict(args)))
                return super().check(tool_name, args)

        class Box:
            def status(self, *, hours: int, timer_runs: int) -> str:
                return json.dumps({"hours": hours, "timer_runs": timer_runs})

        tools = make_tools(Gate(), Box())
        self.assertEqual(await tools.call("runtime_status", {}), ('{"hours": 24, "timer_runs": 5}', False))
        self.assertEqual(await tools.call("runtime_status", {"hours": 72, "timer_runs": 3}),
                         ('{"hours": 72, "timer_runs": 3}', False))
        self.assertEqual(seen, [("runtime_status", {}), ("runtime_status", {"hours": 72, "timer_runs": 3})])
        for wrong in ({"hours": 0}, {"hours": 24 * 31}, {"timer_runs": 21}, {"hours": "1"}, {"sql": "x"}):
            answer, failed = await tools.call("runtime_status", wrong)
            self.assertTrue(failed, (wrong, answer))
        blocked = make_tools(AutoModePermissionAdapter(block_tools=frozenset({"runtime_status"})), Box())
        answer, failed = await blocked.call("runtime_status", {})
        self.assertTrue(failed)
        self.assertIn("not permitted", answer)


def make_tools(gate: AutoModePermissionAdapter, box: Any) -> StewardTools:
    class System:
        def check(self) -> str:
            return "{}"

        def onboard(self, *, repo: str, name: str | None, description: str | None) -> str:
            return "{}"

        def complete(self, *, name: str, description: str | None) -> str:
            return "{}"

        def status(self, *, hours: int, timer_runs: int) -> str:
            return box.status(hours=hours, timer_runs=timer_runs)

        def hides(self, path: Path) -> bool:
            return False

    # The tools read nothing but the gate from the Run.
    caps = SimpleNamespace(permissions=gate)
    return StewardTools(caps=caps, checkout=Path("."), inbox_id="sys:ops", sessions=None,  # type: ignore[arg-type]
                        events=None, in_project=lambda _i: False, projectless=True,  # type: ignore[arg-type]
                        system=System())


if __name__ == "__main__":
    unittest.main()
