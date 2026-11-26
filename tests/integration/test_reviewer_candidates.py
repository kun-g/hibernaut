import asyncio
import dataclasses
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import CLIReviewer, CLITemplate, CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeError, ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.project_guidance import pin_head
from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, ReviewQuotaExhausted, ReviewRequest
from agent_runtime.adapters.executor_cli.reviewer_pool import ReviewerPool, SwitchNotice, read_availability
from agent_runtime.adapters.executor_cli.run_state import STATE_FILE
from agent_runtime.adapters.executor_cli.sde import STATE_FORMAT, _State
from agent_runtime.adapters.storage_sqlite import SQLiteReviewerStore
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import Event
from tests.support.empty_guidance import empty_project_guidance
from tests.support.fakes import InMemoryPermission
from tests.support.cli_reviewer import CLIPlan, TerminalCase, cli_command
from tests.support.coder_reviewer import CHANGES_REQUESTED, fake_command, make_repo
from tests.support.pr_delivery import VERSION_FILE, DeliveryCase
from tests.support.sde_executor import BUGGY, FIXED, PASS, dev, make_base_repo
from tests.support.reviewer_candidates import GROK_PATTERN


# The whole line grok-reviewer printed on 2026-11-23.
GROK_402 = "API error (status 402 Payment Required): Grok Build usage balance exhausted"
OUT_OF_QUOTA = {"stderr": f"{GROK_402}\n", "exit": 1}
ORDER = ["grok-reviewer", "cursor-reviewer", "gpt-reviewer"]
POOL_LOG = "agent_runtime.adapters.executor_cli.reviewer_pool"


class FlowCase(unittest.IsolatedAsyncioTestCase):
    # The whole path from the configuration: a daemon's worker, a scripted developer and real reviewer commands.
    def setUp(self):
        self.enterContext(empty_project_guidance())
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.repo = make_base_repo(self.root)
        self.plan, self.log = self.root / "plan.json", self.root / "calls.jsonl"
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan)}))
        self.table = self.root / "availability.json"
        self.reviewers = {name: CLIPlan(self.root / name) for name in (*ORDER, "extra")}
        for name, plan in self.reviewers.items():
            plan.plan.parent.mkdir()
            plan.set(json.dumps(PASS))
        self.develop(dev(FIXED))
        self.command = cli_command(self.root)
        self.n = 0

    def develop(self, *steps):
        self.plan.write_text(json.dumps({"sde": list(steps), "reviewer": []}))

    def unavailable(self, **reasons):
        self.table.write_text(json.dumps({
            "version": 1, "generated_at": "2026-11-24T07:00:00+00:00",
            "executors": {n.replace("_", "-"): {"available": False, "reason": r} for n, r in reasons.items()},
        }))

    def config(self, reviewer=None, *, names=(*ORDER, "extra"), project=None, sde=None, **availability):
        executors = {name: {"command": self.command, "env": self.reviewers[name].env(), "args": ["-p"],
                            "quota_errors": [GROK_PATTERN]} for name in names}
        return parse_config({
            "runtime": {"db_path": str(self.root / "hb.db"), "memory_root": str(self.root / "mem"),
                        "default_executor": "sde"},
            "launchers": {"fake": {"command": fake_command(self.root)}},
            "availability": {"file": str(self.table), **availability},
            "projects": {"x": {"inbox_id": ["proj:x", "proj:x:.+"], "repo": "o/x", "checkout": str(self.repo),
                               "worktree_backend": "git", **(project or {})}},
            "executors": {**executors, "sde": {
                "role": "sde", "launcher": "fake", "interactive": False,
                "reviewer": ORDER if reviewer is None else reviewer, **(sde or {})}},
        })

    def start(self, reviewer=None, **options):
        # A new process, or a new generation, on the same database.
        daemon = build_daemon_from_config(self.config(reviewer, **options))
        self.addCleanup(daemon.db.close)
        return daemon

    async def send(self, daemon, payload, key="task-1", inbox="proj:x"):
        self.n += 1
        daemon.handler.ingest(inbox_id=inbox, source="test", external_id=f"e{self.n}", payload=payload,
                              correlation_key=key)
        await self.run_inbox(daemon, inbox)

    async def run_inbox(self, daemon, inbox="proj:x"):
        return await daemon.pool._worker_factory(inbox, "w").process_inbox(inbox)  # pyright: ignore

    async def task(self, daemon, key="task-1", inbox="proj:x", **extra):
        await self.send(daemon, {"executor_id": "sde", "text": "divide must reject a zero divisor", "title": "Zero",
                                 "base_ref": "main", **extra}, key, inbox)

    async def decide(self, daemon, approved=True, **extra):
        card = self.cards(daemon)[-1]
        await self.send(daemon, {"approval_request_id": card.metadata["request_id"], "approved": approved, **extra})

    def outbox(self, daemon):
        return daemon.outbox.fetch_pending(100)

    def cards(self, daemon):
        return [m for m in self.outbox(daemon) if m.metadata.get("kind") == "approval_request"]

    def notices(self, daemon):
        return [m for m in self.outbox(daemon) if m.metadata.get("kind") == "reviewer_switch"]

    def delivered(self, daemon):
        (text,) = [m.content for m in self.outbox(daemon) if "[sde] delivered" in m.content][-1:]
        return text

    def workdir(self, daemon):
        return daemon.workspaces.get("task-1").workdir

    def ran(self):
        return {name: len(plan.calls()) for name, plan in self.reviewers.items() if plan.calls()}

    def developer_turns(self):
        return len(self.log.read_text().splitlines())


class TestChoosing(FlowCase):
    async def test_the_first_candidate_reviews_while_all_are_available(self):
        daemon = self.start()
        with self.assertLogs(POOL_LOG, "WARNING") as logs:
            await self.task(daemon)
        self.assertEqual(self.ran(), {"grok-reviewer": 1})
        # No table yet: everything counts as available, with one line in the log.
        self.assertEqual(len(logs.output), 1)
        self.assertIn(f"{self.table} is missing", logs.output[0])
        text = self.delivered(daemon)
        self.assertIn("Review history: round 1 pass by grok-reviewer\n", text)
        self.assertIn("Reviewers: chose grok-reviewer; not switched\n", text)
        self.assertEqual(self.notices(daemon), [])

    async def test_a_candidate_the_table_rules_out_is_passed_over_and_the_reason_is_told(self):
        self.unavailable(grok_reviewer="剩余 8%，低于 15%")
        daemon = self.start()
        with self.assertNoLogs(POOL_LOG, "WARNING"):
            await self.task(daemon)
        self.assertEqual(self.ran(), {"cursor-reviewer": 1})
        text = self.delivered(daemon)
        self.assertIn("round 1 pass by cursor-reviewer", text)
        self.assertIn("Reviewers: chose cursor-reviewer (grok-reviewer is not available: 剩余 8%，低于 15%); not switched",
                      text)
        # The table is what switched; its writer tells people about that.
        self.assertEqual(self.notices(daemon), [])

    async def test_a_table_that_cannot_be_read_rules_nothing_out(self):
        entry = {"grok-reviewer": {"available": False, "reason": "x"}}
        broken = {
            "half": '{"version": 1, "executors": {"grok-rev',
            "list": "[]",
            "version": json.dumps({"version": 2, "executors": entry}),
            "executors": json.dumps({"version": 1, "executors": ["grok-reviewer"]}),
            "no executors": json.dumps({"version": 1}),
            "available": json.dumps({"version": 1, "executors": {"grok-reviewer": {"available": "no"}}}),
            "binary": b"\xff\xfe\x00",
            "deep": "[" * 200_000,
            "too big": json.dumps({"version": 1, "executors": entry, "pad": "x" * 2_000_000}),
        }
        for name, raw in broken.items():
            with self.subTest(name), self.assertLogs(POOL_LOG, "WARNING") as logs:
                self.table.write_bytes(raw if isinstance(raw, bytes) else raw.encode())
                self.assertEqual(read_availability(self.table), {})
                self.assertEqual(len(logs.output), 1)
        with self.subTest("a directory"), self.assertLogs(POOL_LOG, "WARNING"):
            self.assertEqual(read_availability(self.root), {})
        with self.subTest("a pipe"), self.assertLogs(POOL_LOG, "WARNING"):
            os.mkfifo(self.root / "pipe")
            self.assertEqual(read_availability(self.root / "pipe"), {})
        self.table.write_text(json.dumps({"version": 1, "executors": entry, "more": 1}))
        self.assertEqual(read_availability(self.table), {"grok-reviewer": "x"})
        with self.subTest("end to end"), self.assertLogs(POOL_LOG, "WARNING"):
            self.table.write_text(broken["half"])
            await self.task(self.start())
            self.assertEqual(self.ran(), {"grok-reviewer": 1})

    async def test_a_reason_that_cannot_be_stored_or_shown_as_it_is_stops_nothing(self):
        self.table.write_text('{"version": 1, "executors": {"grok-reviewer": {"available": false, '
                              '"reason": "low \\ud800\\u0000\\u001b[31m\\n- fake: line"}}}')
        daemon = self.start()
        await self.task(daemon)
        self.assertEqual(self.ran(), {"cursor-reviewer": 1})
        self.assertIn("(grok-reviewer is not available: low [31m - fake: line)", self.delivered(daemon))

    async def test_an_executor_the_table_does_not_list_is_available_however_old_the_table(self):
        self.table.write_text(json.dumps({"version": 1, "generated_at": "2001-01-01T00:00:00+00:00", "executors": {
            "grok-reviewer": {"available": False, "reason": "old"}, "gpt-reviewer": {"available": True}}}))
        daemon = self.start()
        await self.task(daemon)
        self.assertEqual(self.ran(), {"cursor-reviewer": 1})

    async def test_a_single_reviewer_ignores_the_table_and_its_output_is_unchanged(self):
        self.unavailable(grok_reviewer="剩余 8%")
        daemon = self.start("grok-reviewer")
        await self.task(daemon)
        self.assertEqual(self.ran(), {"grok-reviewer": 1})
        text = self.delivered(daemon)
        self.assertIn("Review history: round 1 pass\n", text)
        self.assertNotIn("Reviewers:", text)

    async def test_a_list_of_one_is_chosen_by_availability(self):
        self.unavailable(grok_reviewer="剩余 8%")
        daemon = self.start(["grok-reviewer"])
        await self.task(daemon)
        self.assertEqual(self.ran(), {})
        self.assertIn("no review candidate is available", self.cards(daemon)[-1].content)


class TestStickiness(FlowCase):
    async def test_later_rounds_keep_the_reviewer_whatever_the_table_says_meanwhile(self):
        self.develop(dev(BUGGY), dev(FIXED))
        self.reviewers["grok-reviewer"].set(json.dumps(CHANGES_REQUESTED), json.dumps(PASS))
        daemon = self.start()
        with patch.object(CLIReviewer, "review", autospec=True, side_effect=self.reviews_then_rules_out_grok()):
            await self.task(daemon)
        self.assertEqual(self.ran(), {"grok-reviewer": 2})
        self.assertIn("round 1 changes_requested (1 blocking) by grok-reviewer; round 2 pass by grok-reviewer",
                      self.delivered(daemon))

    def reviews_then_rules_out_grok(self):
        original = CLIReviewer.review

        async def review(reviewer, *args, **kwargs):
            result = await original(reviewer, *args, **kwargs)
            self.unavailable(grok_reviewer="剩余 8%")
            return result
        return review

    async def test_a_suspended_task_comes_back_to_its_reviewer_after_a_restart_with_another_list(self):
        self.reviewers["grok-reviewer"].set({"stderr": "crashed\n", "exit": 3}, json.dumps(PASS))
        first = self.start()
        await self.task(first)
        self.assertIn("exited with code 3: crashed", self.cards(first)[-1].content)
        self.unavailable(grok_reviewer="剩余 8%")
        again = self.start(["extra", "cursor-reviewer"])
        await self.decide(again)
        self.assertEqual(self.ran(), {"grok-reviewer": 2})
        self.assertIn("round 1 pass by grok-reviewer", self.delivered(again))

    async def test_a_list_that_became_a_single_reviewer_leaves_the_task_its_own(self):
        self.reviewers["grok-reviewer"].set({"stderr": "crashed\n", "exit": 3}, json.dumps(PASS))
        await self.task(self.start())
        again = self.start("extra")
        await self.decide(again)
        self.assertEqual(self.ran(), {"grok-reviewer": 2})

    async def test_another_developer_on_the_same_session_chooses_from_its_own_list(self):
        # R4: sde-gpt leaves gpt-reviewer out of its list, whoever had the task before.
        lookup = {name: CLIReviewer(template=CLITemplate(name=name, command="x")) for name in ORDER}.get
        store = SQLiteReviewerStore(self.start().db)
        pools = {owner: ReviewerPool(configured=names, lookup=lookup, store=store, owner=owner, table="",
                                     cooldown_seconds=60, notify=Mock())
                 for owner, names in (("sde", ("gpt-reviewer",)), ("sde-gpt", ("grok-reviewer", "cursor-reviewer")))}
        notice = SwitchNotice("proj:x", "task", {})
        self.assertEqual([pools[owner].pick("s", round_no=1, tried={}, notice=notice).reviewer.executor_id
                          for owner in ("sde", "sde-gpt", "sde")], ["gpt-reviewer", "grok-reviewer", "gpt-reviewer"])

    async def test_a_task_begun_with_a_single_reviewer_keeps_it_when_a_list_is_configured(self):
        self.reviewers["grok-reviewer"].set({"stderr": "crashed\n", "exit": 3}, json.dumps(PASS))
        await self.task(self.start("grok-reviewer"))
        again = self.start(["extra", "cursor-reviewer"])
        await self.decide(again)
        self.assertEqual(self.ran(), {"grok-reviewer": 2})

    async def test_a_single_reviewer_that_is_replaced_is_replaced_as_before(self):
        self.reviewers["grok-reviewer"].set({"stderr": "crashed\n", "exit": 3})
        await self.task(self.start("grok-reviewer"))
        again = self.start("extra")
        await self.decide(again)
        self.assertEqual(self.ran(), {"grok-reviewer": 1, "extra": 1})

    async def test_a_reviewer_that_is_gone_from_the_configuration_is_replaced_by_the_next(self):
        self.reviewers["grok-reviewer"].set({"stderr": "crashed\n", "exit": 3})
        await self.task(self.start())
        again = self.start(["extra"], names=("cursor-reviewer", "gpt-reviewer", "extra"))
        await self.decide(again)
        # The task's own list still decides.
        self.assertEqual(self.ran(), {"grok-reviewer": 1, "cursor-reviewer": 1})
        self.assertIn("grok-reviewer is not available: not configured any more", self.delivered(again))

    async def test_a_run_resumed_after_a_restart_comes_back_to_its_reviewer_with_and_without_its_state(self):
        self.develop(dev(FIXED), dev({"calc.py": "x = 2\n"}))
        daemon = self.start()
        await self.task(daemon)
        session = daemon.sessions.find_by_correlation_key("proj:x", "task-1")
        self.unavailable(grok_reviewer="剩余 8%")
        state = _State(stage="develop", workdir=self.workdir(daemon), base_ref="main", requirements="r", test_evidence="e",
                       work_key="task-1", inbox_id="proj:x")
        in_review = {"format": STATE_FORMAT, "phase": "review", "session_id": session.session_id, "native": None,
                     "prompt": None, "approved": [], "turn": None, "state": state.fields()}
        for n, (text, turns, said) in enumerate((
            (json.dumps(in_review), 1, "round 1 pass by grok-reviewer"),
            ("{not json", 2, "Restart: Resumed after a restart without a usable state"),
        )):
            with self.subTest(text[:9]):
                stored, _ = daemon.inbox.append(self.event(session.session_id, f"stopped-{n}"))
                daemon.inbox.claim_pending("proj:x")
                run = daemon.open_runs.open(session_id=session.session_id, inbox_id="proj:x", executor_id="sde",
                                            event_ids=[stored.event_id], code_version="0")
                Path(run.run_dir, STATE_FILE).write_text(text)
                again = self.start(["extra", "cursor-reviewer"])
                await self.run_inbox(again)
                self.assertEqual((self.ran(), self.developer_turns()), ({"grok-reviewer": n + 2}, turns))
                self.assertIn(said, self.delivered(again))

    def event(self, session_id, external_id):
        return Event(inbox_id="proj:x", source="test", external_id=external_id, correlation_key="task-1", session_id=session_id,
                     payload={"executor_id": "sde", "text": "divide must reject a zero divisor", "base_ref": "main"})

    async def test_a_new_task_under_the_same_key_chooses_again(self):
        daemon = self.start()
        await self.task(daemon)
        daemon.sessions.close_session(daemon.sessions.find_by_correlation_key("proj:x", "task-1").session_id)
        self.unavailable(grok_reviewer="剩余 8%")
        self.develop(dev({"calc.py": "x = 2\n"}))
        await self.task(daemon)
        self.assertEqual(self.ran(), {"grok-reviewer": 1, "cursor-reviewer": 1})

    async def test_a_reload_that_changes_the_list_leaves_the_running_task_alone(self):
        self.develop(dev(BUGGY), dev(FIXED))
        self.reviewers["grok-reviewer"].set(json.dumps(CHANGES_REQUESTED), OUT_OF_QUOTA)
        old = self.start()
        original = CLIReviewer.review

        async def review(reviewer, *args, **kwargs):
            # The new generation is built, and puts its own list in force, while round 1 is under way.
            result = await original(reviewer, *args, **kwargs)
            await self.task(self.start(["extra"]), "task-2", "proj:x:b")
            return result

        with patch.object(CLIReviewer, "review", autospec=True, side_effect=[None]) as patched:
            calls = []

            async def once(reviewer, *args, **kwargs):
                calls.append(reviewer.executor_id)
                return await (review if len(calls) == 1 else original)(reviewer, *args, **kwargs)
            patched.side_effect = once
            await self.task(old)
        self.assertEqual(calls, ["grok-reviewer", "extra", "grok-reviewer", "cursor-reviewer"])


class TestQuota(FlowCase):
    async def test_groks_402_moves_the_round_and_the_rest_of_the_task_to_the_next_candidate(self):
        self.develop(dev(BUGGY), dev(FIXED))
        self.reviewers["grok-reviewer"].set(OUT_OF_QUOTA)
        self.reviewers["cursor-reviewer"].set(json.dumps(CHANGES_REQUESTED), json.dumps(PASS))
        daemon = self.start()
        await self.task(daemon)
        self.assertEqual(self.ran(), {"grok-reviewer": 1, "cursor-reviewer": 2})
        self.assertEqual(self.cards(daemon), [])
        # The round was reviewed again as it was: the developer was not sent back for it.
        self.assertEqual(self.developer_turns(), 2)
        text = self.delivered(daemon)
        self.assertIn("review=pass after 2 round(s)", text)
        self.assertIn("Review history: round 1 changes_requested (1 blocking) by cursor-reviewer; "
                      "round 2 pass by cursor-reviewer\n", text)
        self.assertIn(f"Reviewers: chose grok-reviewer; round 1: grok-reviewer ran out of quota ({GROK_402}), "
                      "switched to cursor-reviewer\n", text)
        self.assertNotIn("not switched", text)
        (notice,) = self.notices(daemon)
        self.assertIn(f"grok-reviewer ran out of quota ({GROK_402}), switched to cursor-reviewer", notice.content)
        self.assertEqual(notice.metadata["correlation_key"], "task-1")
        self.assertEqual(notice.metadata["outcome_kind"], "emit")
        self.assertTrue(notice.metadata["session_id"])

    async def test_the_notice_answers_where_the_task_came_from(self):
        self.reviewers["grok-reviewer"].set(OUT_OF_QUOTA)
        daemon = self.start()
        await self.task(daemon, reply_channel="feishu", reply_target="oc_1")
        (notice,) = self.notices(daemon)
        self.assertEqual((notice.metadata["reply_channel"], notice.metadata["reply_target"]), ("feishu", "oc_1"))
        self.assertEqual(notice.metadata["task_context"]["title"], "Zero")

    async def test_other_tasks_pass_over_a_marked_reviewer_after_a_restart_until_the_mark_runs_out(self):
        self.reviewers["grok-reviewer"].set(OUT_OF_QUOTA, json.dumps(PASS))
        await self.task(self.start())
        again = self.start()
        await self.task(again, "task-2", "proj:x:b")
        self.assertEqual(self.ran(), {"grok-reviewer": 1, "cursor-reviewer": 2})
        text = self.delivered(again)
        self.assertIn("Reviewers: chose cursor-reviewer (grok-reviewer is not available: ran out of quota "
                      f"({GROK_402}), not used until ", text)
        with patch(f"{POOL_LOG}.time.time", return_value=time.time() + 3601):
            await self.task(again, "task-3", "proj:x:c")
        self.assertEqual(self.ran(), {"grok-reviewer": 2, "cursor-reviewer": 2})

    async def test_the_mark_lasts_as_long_as_configured(self):
        self.reviewers["grok-reviewer"].set(OUT_OF_QUOTA, json.dumps(PASS))
        daemon = self.start(quota_cooldown_seconds=0.001)
        await self.task(daemon)
        await self.task(daemon, "task-2", "proj:x:b")
        # Within the round that met the error the reviewer is not tried again, however short its mark.
        self.assertEqual(self.ran(), {"grok-reviewer": 2, "cursor-reviewer": 1})

    async def test_any_other_failure_waits_for_a_person_as_before(self):
        failures = {
            "another exit": {"stderr": "auth expired\n", "exit": 1},
            "the words without a failure": {"stdout": GROK_402, "stderr": GROK_402},
        }
        for n, (name, step) in enumerate(failures.items()):
            with self.subTest(name):
                key = f"task-{n}"
                self.reviewers["grok-reviewer"].set(step)
                daemon = self.start()
                await self.task(daemon, key, f"proj:x:{n}")
                self.assertIn("review pending: the review could not complete", self.cards(daemon)[-1].content)
                self.assertNotIn("cursor-reviewer", self.ran())
                self.assertEqual(daemon.db.fetchall("SELECT * FROM reviewer_quota_marks"), [])

    async def test_without_patterns_nothing_is_a_quota_error(self):
        self.reviewers["grok-reviewer"].set(OUT_OF_QUOTA)
        config = self.config()
        config.executors["grok-reviewer"] = dataclasses.replace(config.executors["grok-reviewer"], quota_errors=())
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.db.close)
        await self.task(daemon)
        self.assertEqual(self.ran(), {"grok-reviewer": 1})
        self.assertIn(GROK_402, self.cards(daemon)[-1].content)

    async def test_a_single_reviewer_out_of_quota_waits_as_before_and_is_marked_for_the_lists(self):
        self.reviewers["grok-reviewer"].set(OUT_OF_QUOTA)
        daemon = self.start("grok-reviewer")
        await self.task(daemon)
        card = self.cards(daemon)[-1].content
        self.assertEqual(card.splitlines()[0], "[sde] review pending: the review could not complete: grok-reviewer: "
                                               f"'{self.command}' exited with code 1: {GROK_402}")
        self.assertNotIn("Reviewers:", card)
        self.assertEqual(self.notices(daemon), [])
        await self.task(self.start(), "task-2", "proj:x:b")
        self.assertEqual(self.ran(), {"grok-reviewer": 1, "cursor-reviewer": 1})

    async def test_a_reviewer_that_changed_the_worktree_before_it_ran_out_is_not_silently_replaced(self):
        self.reviewers["grok-reviewer"].set({**OUT_OF_QUOTA, "write": {"stray.py": "x\n"}})
        daemon = self.start()
        await self.task(daemon)
        card = self.cards(daemon)[-1].content
        self.assertIn("ran out of quota", card)
        self.assertIn("changed during the review", card)
        self.assertEqual(self.ran(), {"grok-reviewer": 1})
        # Once a person has looked, the next candidate takes over.
        Path(self.workdir(daemon), "stray.py").unlink()
        await self.decide(daemon)
        self.assertEqual(self.ran(), {"grok-reviewer": 1, "cursor-reviewer": 1})
        self.assertIn("switched to cursor-reviewer", self.delivered(daemon))

    async def test_two_tasks_that_meet_the_error_at_once_both_go_on_with_the_next(self):
        both = asyncio.Barrier(2)
        original = CLIReviewer.review

        async def review(reviewer, *args, **kwargs):
            if reviewer.executor_id == "grok-reviewer":
                await both.wait()
            return await original(reviewer, *args, **kwargs)

        self.reviewers["grok-reviewer"].set(OUT_OF_QUOTA)
        daemon = self.start()
        with patch.object(CLIReviewer, "review", autospec=True, side_effect=review):
            await asyncio.wait_for(asyncio.gather(self.task(daemon, "task-1", "proj:x:a"),
                                                  self.task(daemon, "task-2", "proj:x:b")), 60)
        self.assertEqual(self.ran(), {"grok-reviewer": 2, "cursor-reviewer": 2})
        self.assertEqual(len([m for m in self.outbox(daemon) if "[sde] delivered" in m.content]), 2)
        self.assertEqual(sorted(m.metadata["correlation_key"] for m in self.notices(daemon)), ["task-1", "task-2"])
        self.assertEqual(len(daemon.db.fetchall("SELECT * FROM reviewer_quota_marks")), 1)


class TestNobodyAvailable(FlowCase):
    async def test_the_task_waits_with_every_reason_and_approval_chooses_by_the_table_again(self):
        self.unavailable(grok_reviewer="剩余 8%，低于 15%", gpt_reviewer="预测重置前用完")
        self.reviewers["cursor-reviewer"].set(OUT_OF_QUOTA)
        daemon = self.start()
        await self.task(daemon)
        (card,) = self.cards(daemon)
        for part in ("[sde] review pending: no review candidate is available",
                     "- grok-reviewer: 剩余 8%，低于 15%", "- gpt-reviewer: 预测重置前用完",
                     f"- cursor-reviewer: ran out of quota ({GROK_402}), not used until "):
            self.assertIn(part, card.content)
        # A card asks a person to act, on the same path as every other approval.
        self.assertEqual((card.metadata["outcome_kind"], card.metadata["task_context"]["status"]),
                         ("suspend", "awaiting_approval"))
        self.assertEqual(self.notices(daemon), [])

        # Approved with the table unchanged it waits again, without another developer turn.
        again = self.start()
        await self.decide(again)
        self.assertEqual(len(self.cards(again)), 2)
        self.assertEqual(self.ran(), {"cursor-reviewer": 1})

        self.unavailable(grok_reviewer="剩余 8%，低于 15%")
        await self.decide(again)
        self.assertEqual(self.ran(), {"cursor-reviewer": 1, "gpt-reviewer": 1})
        self.assertEqual(self.developer_turns(), 1)
        text = self.delivered(again)
        self.assertIn("round 1 pass by gpt-reviewer", text)
        self.assertIn("switched to gpt-reviewer", text)

    async def test_after_a_fix_round_only_the_review_is_retried(self):
        self.develop(dev(BUGGY), dev(FIXED))
        self.reviewers["grok-reviewer"].set(json.dumps(CHANGES_REQUESTED), OUT_OF_QUOTA)
        self.unavailable(cursor_reviewer="a", gpt_reviewer="b")
        daemon = self.start()
        await self.task(daemon)
        self.unavailable()
        await self.decide(daemon)
        self.assertEqual(self.developer_turns(), 2)
        self.assertEqual(self.ran(), {"grok-reviewer": 2, "cursor-reviewer": 1})
        text = self.delivered(daemon)
        self.assertIn("review=pass after 2 round(s)", text)
        self.assertIn("round 1 changes_requested (1 blocking) by grok-reviewer; round 2 pass by cursor-reviewer", text)

    async def test_a_changed_list_counts_once_a_person_approves(self):
        self.unavailable(grok_reviewer="a", cursor_reviewer="b", gpt_reviewer="c")
        await self.task(self.start())
        again = self.start([*ORDER, "extra"])
        await self.decide(again)
        self.assertEqual(self.ran(), {"extra": 1})

    async def test_a_single_reviewer_configured_meanwhile_is_used_as_it_is(self):
        self.unavailable(grok_reviewer="a", cursor_reviewer="b", gpt_reviewer="c")
        await self.task(self.start())
        again = self.start("grok-reviewer")
        await self.decide(again)
        self.assertEqual(self.ran(), {"grok-reviewer": 1})

    async def test_a_follow_up_is_developed_before_the_review(self):
        self.unavailable(grok_reviewer="a", cursor_reviewer="b", gpt_reviewer="c")
        daemon = self.start()
        await self.task(daemon)
        await self.send(daemon, {"text": "name the argument in the message"})
        self.assertEqual(len(self.cards(daemon)), 1)
        self.unavailable()
        await self.decide(daemon)
        self.assertEqual(self.developer_turns(), 2)
        self.assertEqual(self.ran(), {"grok-reviewer": 1})

    async def test_rejecting_delivers_it_unreviewed(self):
        self.unavailable(grok_reviewer="a", cursor_reviewer="b", gpt_reviewer="c")
        daemon = self.start()
        await self.task(daemon)
        await self.decide(daemon, approved=False)
        self.assertIn("review=pending (retry declined; no review candidate is available", self.delivered(daemon))
        self.assertEqual(self.ran(), {})

    async def test_rejecting_at_once_is_not_an_approval_of_a_reviewer(self):
        self.reviewers["grok-reviewer"].set({"stderr": "boom", "exit": 2})
        daemon = self.start()
        await self.task(daemon)
        self.assertIn("Reviewers: chose grok-reviewer", self.cards(daemon)[-1].content)


class TestReviewersReportQuota(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.repo = make_repo(self.root)
        self.cli = CLIPlan(self.root)
        self.enterContext(patch.dict(os.environ, self.cli.env()))
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False

    def request(self):
        return ReviewRequest(workdir=self.repo, requirements="r", test_evidence="e", base_ref="main",
                             pin=pin_head(self.repo, "test"))

    async def failure(self, reviewer):
        with self.assertRaises((ReviewAborted, ClaudeCodeError)) as caught:
            await reviewer.review(self.request(), self.caps)
        return caught.exception

    async def test_a_cli_is_out_of_quota_when_it_fails_and_stderr_or_stdout_says_so(self):
        reviewer = CLIReviewer(template=CLITemplate(name="grok", command=cli_command(self.root), args=("-p",),
                                                    quota_errors=("never", GROK_PATTERN)))
        long = "x" * 5000
        for step, reason in (
            (OUT_OF_QUOTA, GROK_402),
            ({"stdout": f"{long}\n{GROK_402}\n", "stderr": long, "exit": 7}, GROK_402),
            ({"stdout": long + GROK_402, "exit": 1}, "status 402 Payment Required): Grok Build usage balance exhausted"),
        ):
            with self.subTest(step=step["exit"]):
                self.cli.set(step)
                found = await self.failure(reviewer)
                self.assertIsInstance(found, ReviewQuotaExhausted)
                self.assertEqual((found.reason, found.changed), (reason, False))
                self.assertIn(f"exited with code {step['exit']}", str(found))
        for step in ({"stderr": "402", "exit": 1}, {"stderr": GROK_402, "stdout": "no verdict", "exit": 0}):
            with self.subTest(step=step):
                self.cli.set(step)
                self.assertNotIsInstance(await self.failure(reviewer), ReviewQuotaExhausted)

    async def test_the_format_retry_can_run_out_too(self):
        self.cli.set("no verdict", OUT_OF_QUOTA)
        reviewer = CLIReviewer(template=CLITemplate(name="grok", command=cli_command(self.root), args=("-p",),
                                                    quota_errors=(GROK_PATTERN,)))
        self.assertIsInstance(await self.failure(reviewer), ReviewQuotaExhausted)

    def claude(self, script, **fields):
        command = self.root / "claude"
        command.write_text(f"#!/bin/sh\ncat > /dev/null\n{script}\n")
        command.chmod(0o700)
        return CoderReviewerExecutor(
            profile=ClaudeCodeProfile(name="gpt-reviewer", model="codex/gpt",
                                      launcher=ClaudeCodeLauncher(name="fake", command=str(command)), **fields),
            workspaces=Mock(), tasks=Mock())

    async def test_a_claude_code_reviewer_is_out_of_quota_by_its_exit_and_its_own_error(self):
        init = json.dumps({"type": "system", "subtype": "init", "apiKeySource": "none", "model": "codex/gpt"})
        said = json.dumps({"type": "result", "subtype": "success", "is_error": True, "result": "limit reached, resets 5pm"})
        seen = json.dumps({"type": "user", "message": {"content": "a diff that says limit reached"}})
        other = json.dumps({"type": "result", "subtype": "success", "is_error": True, "result": "overloaded"})
        cases = (
            (f"echo '{init}'; echo '{said}'; exit 1", True, "reported an error: limit reached"),
            (f"echo '{init}'; echo 'limit reached' >&2; exit 2", True, "without a result"),
            ("echo 'limit reached'; exit 1", True, "before reporting its session"),
            # Exit 0, or words from the transcript rather than from the failure, are not.
            (f"echo '{init}'; echo '{said}'; exit 0", False, "reported an error"),
            (f"echo '{init}'; echo '{seen}'; echo '{other}'; exit 1", False, "overloaded"),
        )
        for script, exhausted, message in cases:
            with self.subTest(script=script):
                found = await self.failure(self.claude(script, quota_errors=("limit reached",)))
                self.assertEqual(isinstance(found, ReviewQuotaExhausted), exhausted, found)
                self.assertIn(message, str(found))
        found = await self.failure(self.claude(cases[0][0]))
        self.assertIsInstance(found, ClaudeCodeError)


class TestQuotaInATerminal(TerminalCase):
    async def test_the_terminal_of_a_run_that_ran_out_stays_known(self):
        self.cli.set({"stdout": json.dumps({"session_id": "chat-1"}), **OUT_OF_QUOTA})
        with self.assertRaises(ReviewQuotaExhausted) as caught:
            await self.reviewer(quota_errors=(GROK_PATTERN,)).review(self.request(), self.caps)
        (handle,) = self.handles()
        found = caught.exception
        self.assertEqual((found.terminals, found.sessions), ((handle,), {handle: "chat-1"}))
        self.assertTrue(str(found).endswith(f"(Orca terminal: {handle} (CLI session chat-1))"), str(found))
        self.assertEqual(self.closes(), [handle])


class TestPullRequest(FlowCase, DeliveryCase):
    def setUp(self):
        DeliveryCase.setUp(self)
        del self.config  # DeliveryCase's own; here config() builds the deployment
        FlowCase.setUp(self)
        self.repo = self.clone("checkout", None)
        self.enterContext(patch.dict(os.environ, {"FAKE_GH_STATE": str(self.state_file)}))

    async def test_the_pull_request_says_who_reviewed_each_round_and_why_it_changed(self):
        self.develop(dev({"calc.py": "x = 1\n"}))
        self.reviewers["grok-reviewer"].set(OUT_OF_QUOTA)
        daemon = self.start(project={"delivery": {
            "open_pr": True, "version_file": VERSION_FILE, "gh_command": str(self.state_file.parent / "gh")}})
        await self.task(daemon, base_ref="origin/main")
        (pr,) = json.loads(self.state_file.read_text())["prs"]
        self.assertIn("round 1 pass by cursor-reviewer", pr["body"])
        self.assertIn(f"Reviewers: chose grok-reviewer; round 1: grok-reviewer ran out of quota ({GROK_402}), "
                      "switched to cursor-reviewer", pr["body"])


if __name__ == "__main__":
    unittest.main()
