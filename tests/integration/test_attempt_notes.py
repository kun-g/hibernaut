from __future__ import annotations

import asyncio
import json
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli.attempt_notes import (
    FAILED,
    MERGED,
    STOPPED,
    UNMERGED,
    AttemptNotes,
    AttemptResult,
)
from agent_runtime.adapters.delivery import MERGE_TOOL
from agent_runtime.adapters.executor_cli import CommandResult
from agent_runtime.adapters.executor_cli.sde import SDEExecutor
from agent_runtime.adapters.storage_sqlite import (
    SQLiteAttemptStore,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.core.models import Emit, Event, PermissionDecision, RunDetached, SessionRef, Suspend
from agent_runtime.engine.attempts import ATTEMPT_NOTES_KEY
from agent_runtime.engine.worker import AgentWorker
from tests.support.fakes import InMemoryPermission
from tests.support.pr_delivery import git
from tests.support.reclaim_sweep import SweepCase
from tests.support.resume_runs import INBOX, SDERestartCase, committed, verdict_pass
from tests.support.sde_delivery import SDEDeliveryCase
from tests.support.sde_delivery import dev as delivered_dev
from tests.support.sde_executor import FIXED, PASS, dev, event
from tests.support.sde_memory import ALPHA, BETA, MemoryCase, ungranted
from tests.support.attempt_notes import GOAL, NOTE, Notes, headings


class OwnMergeCase(SweepCase):
    # Attempts of one task (work_key task:9) in its worktree, one after the other.
    def setUp(self) -> None:
        super().setUp()
        self.notes = Notes(self.root)
        self.addCleanup(self.notes.close)
        self.workspaces.claim_owner("task:9", ALPHA)
        plain = self.manager
        self.manager = lambda usage=None: self.settling(plain(usage))
        self.asked = 0

    def settling(self, manager: Any) -> Any:
        manager._settle = self.notes.notes.settle
        return manager

    def runner(self, cmd: Any, cwd: Any) -> Any:
        self.asked += cmd[0] == "gh"
        return super().runner(cmd, cwd)

    def attempt(self, session: str, result: AttemptResult | None, work: Any = None, inbox: str = ALPHA) -> Any:
        self.notes.begin(session, inbox=inbox)
        self.notes.notes.located(session, "task:9", str(self.wt))
        done = work() if work is not None else None
        self.notes.notes.progress(session, f"summary of {session}")
        if result is not None:
            # A PR is always at a pushed head: what lies there, unless the delivery names another one.
            pushed = result.head or (git(self.wt, "rev-parse", "HEAD") if result.pr_url else "")
            self.notes.notes.finish(session, replace(result, head=pushed))
        return done

    def merged(self, head: str, merge: str = "", url: str = "https://x/pull/3") -> None:
        self.gh["merged"] = CommandResult(0, json.dumps([
            {"number": 3, "headRefOid": head, "mergeCommit": {"oid": merge}, "url": url}]), "")

    def settle(self) -> None:
        record = self.workspaces.get("task:9")
        assert record is not None
        self.manager().settle(record)

    def entries(self, project: str = "alpha") -> list[str]:
        return self.notes.entries(project=project)


class TestOnlyItsOwnMergeCounts(OwnMergeCase, unittest.TestCase):
    def test_a_pr_merged_elsewhere_is_recorded_while_the_session_still_holds_the_worktree(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED), self.squashed_task)
        self.sessions.save(SessionRef(session_id="s1", inbox_id=ALPHA, executor_id="sde", work_key="task:9"))
        self.sweeper().sweep_once()
        self.assertEqual(self.entries(), ["没合并 · title of s1"])
        self.merged(head, merge)
        [outcome] = self.sweeper().sweep_once()
        self.assertFalse(outcome.reclaimed)
        self.assertEqual(self.entries(), ["已合并 · title of s1"])
        self.assertIn("https://x/pull/3", self.notes.text())

    def test_a_merge_from_before_is_not_that_of_a_later_attempt_without_commits(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED, pr_url="https://x/pull/3"), self.squashed_task)
        self.merged(head, merge)
        self.settle()
        self.attempt("s2", AttemptResult(FAILED, detail="crashed"))
        self.attempt("s3", AttemptResult(STOPPED, detail="question declined"))
        self.attempt("s4", None)
        self.notes.notes.session_closed("s4")
        expected = ["停止 · title of s4", "停止 · title of s3", "失败 · title of s2", "已合并 · title of s1"]
        self.assertEqual(self.entries(), expected)
        asked = self.asked
        self.settle()
        # Attempts that committed nothing have no merge to wait for.
        self.assertEqual(self.asked, asked)
        self.sweeper().sweep_once()
        self.assertTrue(self.workspaces.get("task:9") is None)
        self.assertEqual(self.entries(), expected)
        self.assertEqual(self.notes.text().count("https://x/pull/3"), 1)

    def test_a_merge_of_earlier_work_after_the_attempt_is_not_its_own_either(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED, pr_url="https://x/pull/3"), self.squashed_task)
        self.attempt("s2", AttemptResult(FAILED))
        self.merged(head, merge)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s1", "失败 · title of s2"])

    def test_commits_made_after_the_merged_head_are_not_merged(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED), self.squashed_task)
        later = self.attempt("s2", AttemptResult(UNMERGED), lambda: self.commit("y = 2\n"))
        self.merged(head, merge)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s1", "没合并 · title of s2"])
        # A PR of more work contains it.
        self.merged(later, url="https://x/pull/4")
        self.settle()
        self.assertEqual(self.entries()[0], "已合并 · title of s2")
        self.assertIn("https://x/pull/4", self.notes.text())

    def test_a_failed_or_closed_attempt_with_commits_follows_its_merge(self) -> None:
        head = self.attempt("s1", AttemptResult(FAILED), lambda: self.commit("x = 1\n"))
        self.merged(head)
        self.settle()
        self.notes.begin("s2")
        self.notes.notes.located("s2", "task:9", str(self.wt))
        head = self.commit("x = 2\n")
        self.notes.notes.session_closed("s2")
        self.assertEqual(self.entries(), ["停止 · title of s2", "已合并 · title of s1"])
        self.merged(head)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s2", "已合并 · title of s1"])

    def test_a_later_attempts_merge_on_top_of_earlier_commits_is_not_the_earlier_ones(self) -> None:
        self.attempt("s1", AttemptResult(FAILED), lambda: self.commit("x = 1\n"))
        self.attempt("s2", AttemptResult(UNMERGED), lambda: self.commit("x = 2\n"))
        self.notes.begin("s3")
        self.notes.notes.located("s3", "task:9", str(self.wt))
        self.commit("x = 3\n")
        self.notes.notes.session_closed("s3")
        head = self.attempt("s4", AttemptResult(UNMERGED), lambda: self.commit("x = 4\n"))
        self.merged(head)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s4", "停止 · title of s3", "没合并 · title of s2",
                                          "失败 · title of s1"])
        self.assertEqual(self.notes.text().count("https://x/pull/3"), 1)

    def test_commits_of_a_task_without_the_field_on_top_are_not_the_attempts_merge(self) -> None:
        self.attempt("s1", AttemptResult(FAILED), lambda: self.commit("x = 1\n"))
        self.merged(self.commit("x = 2\n"))
        self.settle()
        self.assertEqual(self.entries(), ["失败 · title of s1"])

    def test_a_replay_without_commits_is_not_given_the_merge_of_what_came_between(self) -> None:
        for between in ("s2", ""):
            with self.subTest(between or "a task without the field"):
                first = f"first{between}"
                self.attempt(first, AttemptResult(FAILED), lambda: self.commit(f"x = '{first}'\n"))
                later = self.commit(f"y = '{first}'\n")
                old = AttemptResult(MERGED, pr_url="https://x/pull/7", head=later)
                if between:
                    self.notes.begin(between)
                    self.notes.notes.located(between, "task:9", str(self.wt))
                    later = self.commit(f"z = '{first}'\n")
                    old = AttemptResult(MERGED, pr_url="https://x/pull/7", head=later)
                    self.notes.notes.progress(between, "more")
                    self.notes.notes.finish(between, old)
                self.attempt(first, old)
                self.assertEqual(self.entries()[0], f"没有改动 · title of {first}")

    def test_an_attempt_resumed_after_a_later_one_merged_is_not_given_that_merge(self) -> None:
        # Suspended meanwhile, so it does not begin again.
        self.attempt("s1", None, lambda: self.commit("x = 1\n"))
        head = self.attempt("s2", None, lambda: self.commit("x = 2\n"))
        merged = AttemptResult(MERGED, pr_url="https://x/pull/7", head=head)
        self.notes.notes.finish("s2", merged)
        self.notes.notes.finish("s1", merged)
        self.assertEqual(self.entries(), ["没合并 · title of s1", "已合并 · title of s2"])

    def test_a_task_closed_while_its_delivery_waits_follows_its_own_pr(self) -> None:
        self.attempt("s1", None, lambda: self.commit("x = 1\n"))
        # The PR's head has the delivery's own commit (the version) on top.
        self.notes.notes.delivered("s1", "https://x/pull/3", self.commit("version\n"))
        # A delivery that only knows the PR of other work leaves no trace.
        head = git(self.wt, "rev-parse", "HEAD")
        self.attempt("s2", None)
        self.notes.notes.delivered("s2", "https://x/pull/3", head)
        self.notes.notes.session_closed("s1")
        self.notes.notes.session_closed("s2")
        self.merged(head)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s1", "停止 · title of s2"])

    def test_a_task_of_another_project_with_the_same_key_is_left_alone(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED, pr_url="https://x/pull/3"), self.squashed_task, inbox=BETA)
        self.attempt("s2", AttemptResult(FAILED))
        self.merged(head, merge)
        self.settle()
        self.assertTrue(self.manager().reclaim("task:9").reclaimed)
        self.assertEqual(self.entries("beta"), ["没合并 · title of s1"])
        self.assertEqual(self.entries(), ["失败 · title of s2"])

    def test_a_workspace_of_nobody_settles_nothing(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED), self.squashed_task)
        self.merged(head, merge)
        with self.db.transaction() as conn:
            conn.execute("UPDATE workspaces SET inbox_id = NULL")
        self.settle()
        self.assertEqual(self.entries(), ["没合并 · title of s1"])

    def test_a_reclaim_records_the_merge_first(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(STOPPED), self.squashed_task)
        self.merged(head, merge)
        result = self.manager().reclaim("task:9")
        self.assertTrue(result.reclaimed, result.reason)
        self.assertEqual(self.entries(), ["已合并 · title of s1"])

    def test_a_merge_is_recorded_after_the_directory_went_too(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED), self.squashed_task)
        self.merged(head, merge)
        git(self.primary, "worktree", "remove", "--force", str(self.wt))
        self.sweeper().sweep_once()
        self.assertEqual(self.entries(), ["已合并 · title of s1"])

    def test_what_a_delivery_says_counts_only_for_the_commits_it_covers(self) -> None:
        # The delivery record belongs to the work_key: it may still name the PR of an earlier attempt.
        head, _ = self.attempt("s1", AttemptResult(MERGED, pr_url="https://x/pull/3"), self.squashed_task)
        old = {"pr_url": "https://x/pull/3", "head": head}
        self.attempt("s2", AttemptResult(MERGED, **old))
        self.attempt("s3", AttemptResult(UNMERGED, **old))
        self.attempt("s4", AttemptResult(MERGED, **old), lambda: self.commit("y = 2\n"))
        self.attempt("s5", AttemptResult(UNMERGED, **old), lambda: self.commit("y = 3\n"))
        self.assertEqual(self.entries(), ["没合并 · title of s5", "没合并 · title of s4", "没有改动 · title of s3",
                                          "没有改动 · title of s2", "已合并 · title of s1"])
        self.merged(head)
        self.settle()
        self.assertEqual(self.entries()[:2], ["没合并 · title of s5", "没合并 · title of s4"])
        # The delivery's own commits on top (the version, the base) are still this attempt's delivery.
        self.attempt("s6", None, lambda: self.commit("y = 4\n"))
        self.notes.notes.finish("s6", AttemptResult(MERGED, pr_url="https://x/pull/6", head=self.commit("version\n")))
        self.assertEqual(self.entries()[0], "已合并 · title of s6")

    def test_a_follow_up_without_commits_keeps_the_attempts_own_work(self) -> None:
        self.attempt("s1", None, lambda: self.commit("y = 2\n"))
        # The delivery's own commit lies on top when the attempt ends, so it does not count as somebody else's work.
        head = self.commit("version\n")
        self.notes.notes.finish("s1", AttemptResult(UNMERGED, pr_url="https://x/pull/3", head=head))
        self.notes.notes.begin("s1", ALPHA, None, "again")
        self.notes.notes.located("s1", "task:9", str(self.wt))
        self.notes.notes.progress("s1", "nothing to add")
        self.notes.notes.finish("s1", AttemptResult(MERGED, pr_url="https://x/pull/3", head=head))
        self.assertEqual(self.entries(), ["已合并 · title of s1"])

    def test_a_run_started_over_before_its_end_keeps_its_commits(self) -> None:
        # A restart without a usable state begins the same event again, in a worktree that has its commits by then.
        self.notes.begin("s1")
        self.notes.notes.located("s1", "task:9", str(self.wt))
        head = self.commit("y = 2\n")
        self.attempt("s1", AttemptResult(MERGED, pr_url="https://x/pull/3", head=head))
        self.assertEqual(self.entries(), ["已合并 · title of s1"])


class SDENotesCase(MemoryCase):
    def setUp(self) -> None:
        super().setUp()
        self.notes: Any = AttemptNotes(store=SQLiteAttemptStore(self.world.db), memory_for=self.world.registry.memory_for)

    def sde(self, review: bool = False, terminal: Any = None, **options: Any) -> SDEExecutor:
        return super().sde(review, terminal, attempts=self.notes, **options)

    def task(self, **extra: Any) -> Event:
        return super().task(**{ATTEMPT_NOTES_KEY: GOAL, "title": "Faster tests", **extra})

    @property
    def note(self) -> Path:
        return self.world.hub.project_dir("alpha") / NOTE

    def text(self) -> str:
        return self.note.read_text(encoding="utf-8") if self.note.exists() else ""

    def recorded(self) -> list[str]:
        return headings(self.text())

    async def execute(self, *steps: Any, task: Event | None = None, session: SessionRef | None = None, **options: Any) -> Any:
        self.set_plan(sde=steps)
        return await self.sde(**options).execute([task or self.task()], session or self.session(), self.caps)


NO_COMMIT = {"write": {}, "commit": None, "output": {
    "status": "done", "summary": "xdist gains nothing", "test_evidence": "10s before, 10s after",
    "change_kind": "test_docs", "change_rationale": "nothing changed", "code_change": False}}


class TestEnds(SDENotesCase):
    async def test_no_change(self) -> None:
        task = self.task()
        await self.execute(NO_COMMIT, task=task)
        self.assertEqual(self.recorded(), ["没有改动 · Faster tests"])
        self.assertIn("> xdist gains nothing", self.text())
        self.assertIn(task.external_id, self.text())

    async def test_local_commits_are_not_merged(self) -> None:
        outcome = await self.execute(dev(FIXED))
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(outcome.status, "done")
        self.assertEqual(self.recorded(), ["没合并 · Faster tests"])

    async def test_failure_and_its_replay(self) -> None:
        task = self.task()
        outcome = await self.execute({**dev(FIXED), "output": {"status": "nonsense"}}, task=task)
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(self.recorded(), ["失败 · Faster tests"])
        self.assertIn("malformed", self.text())
        await self.execute(dev(FIXED), task=task)
        self.assertEqual(self.recorded(), ["没合并 · Faster tests"])

    async def test_a_task_that_never_started_developing_failed_too(self) -> None:
        outcome = await self.execute(task=self.task(explicit_workdir=str(self.root / "none")))
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(self.recorded(), ["失败 · Faster tests"])

    async def test_an_error_the_executor_does_not_handle_failed_and_still_reaches_the_worker(self) -> None:
        executor = self.sde()
        with patch.object(executor, "_develop", side_effect=KeyError("bug")), self.assertRaises(KeyError):
            await executor.execute([self.task()], self.session(), self.caps)
        self.assertEqual(self.recorded(), ["失败 · Faster tests"])

    async def test_an_interrupted_or_detached_run_has_not_ended(self) -> None:
        for error in (asyncio.CancelledError(), RunDetached("stopping")):
            executor = self.sde()
            with patch.object(executor, "_develop", side_effect=error), self.assertRaises(type(error)):
                await executor.execute([self.task()], self.session(), self.caps)
        self.assertEqual(self.recorded(), [])

    async def test_a_suspended_task_is_recorded_once_a_person_stops_it(self) -> None:
        asked = await self.execute(dev(FIXED, status="needs_input", question="采用哪种方式？", summary="looked at xdist"))
        assert isinstance(asked, Suspend), asked
        self.assertEqual(self.recorded(), [])
        waiting = self.session(status="suspended", pending_request_id=asked.request_id,
                               checkpoint=asked.session_checkpoint, executor_session_id=asked.executor_session_id)
        answer = event({"approval_request_id": asked.request_id, "approved": False}, inbox_id=ALPHA)
        for _ in range(2):
            await self.execute(task=answer, session=waiting)
            self.assertEqual(self.recorded(), ["停止 · Faster tests"])
        self.assertIn("question declined", self.text())
        self.assertIn("> looked at xdist", self.text())

    async def test_a_task_without_the_field_behaves_as_before(self) -> None:
        plain = self.payload()
        outcome = await self.execute(NO_COMMIT, task=event(plain, inbox_id=ALPHA))
        assert isinstance(outcome, Emit), outcome
        self.assertFalse(self.note.parent.exists())
        self.assertIsNone(self.world.db.fetchone("SELECT 1 FROM attempt_notes"))

    async def test_an_unusable_goal_is_refused_before_anything_starts(self) -> None:
        for bad in ("../x", "", "a/b", None):
            outcome = await self.execute(task=self.task(attempt_notes=bad))
            assert isinstance(outcome, Emit), outcome
            self.assertEqual(outcome.status, "refused")
            self.assertIn("attempt_notes", outcome.messages[0])
        self.assertEqual(self.calls("sde"), [])
        self.assertFalse(self.note.parent.exists())

    async def test_a_write_failure_changes_neither_the_result_nor_the_delivery(self) -> None:
        with self.assertLogs(level="ERROR") as logged, \
                patch.object(self.world.hub, "propose_write", side_effect=OSError("disk full")):
            outcome = await self.execute(dev(FIXED))
        assert isinstance(outcome, Emit), outcome
        self.assertIn("[sde] delivered", outcome.messages[0])
        self.assertEqual(outcome.status, "done")
        self.assertIn("disk full", "".join(logged.output))
        self.assertEqual(self.recorded(), [])


class TestRestart(SDERestartCase):
    async def test_a_run_picked_up_after_a_restart_is_one_record(self) -> None:
        kept = Notes(self.root)
        self.addCleanup(kept.close)
        self.sde_extra = {"attempts": kept.notes}
        self.orca.scripts += [[None], [verdict_pass]]
        payload = {"executor_id": "sde", "text": "divide must reject a zero divisor", "title": "Reject zero",
                   "explicit_workdir": str(self.repo), "base_ref": "main", ATTEMPT_NOTES_KEY: GOAL}
        evt, _ = SQLiteInboxAdapter(self.db).append(Event(
            inbox_id=INBOX, source="test", external_id="task-1", correlation_key="task:1", payload=payload))
        worker, runs = self.process(self.db, "old")
        await self.stop_when(worker, runs, "develop")
        project = kept.registry.memory_for(INBOX).project_key
        self.assertFalse(kept.path(project).exists())
        developer = self.orca.session(0)
        [task] = self.tasks(developer)
        committed(self.repo, "def divide(a, b):\n    return a / b if b else 0\n")(developer, task)
        # The next process has the database, not the objects, of the first.
        self.sde_extra = {"attempts": AttemptNotes(store=SQLiteAttemptStore(kept.db), memory_for=kept.registry.memory_for)}
        worker, _, _ = self.restarted()
        [report] = await worker.process_inbox(evt.inbox_id)
        self.assertIsInstance(report.outcome, Emit)
        self.assertEqual(kept.entries(project=project), ["没合并 · Reject zero"])
        self.assertIn("external_id: task-1", kept.text(project))


class TestFirstPrompt(SDENotesCase):
    async def prompt(self, **extra: Any) -> str:
        self.log.unlink(missing_ok=True)
        await self.execute(NO_COMMIT, task=self.task(**extra))
        return self.calls("sde")[0]["prompt"]

    async def test_it_points_to_the_record_only_once_there_is_one(self) -> None:
        first = await self.prompt(text="one")
        self.assertNotIn("attempts", first)
        second = await self.prompt(text="two")
        self.assertIn(f'memory_read (mcp__hibernaut_memory__memory_read), uri "{NOTE}"', second)
        self.assertNotIn("xdist gains nothing", second)
        self.assertEqual(second.count("## Earlier attempts"), 1)
        # Another goal has no record yet, and a task without the field is told of none.
        self.assertNotIn("## Earlier attempts", await self.prompt(text="three", attempt_notes="memory-use"))
        self.log.unlink()
        await self.execute(NO_COMMIT, task=event(self.payload(), inbox_id=ALPHA),
                           session=SessionRef(session_id="sess-other", inbox_id=ALPHA, executor_id="sde"))
        self.assertNotIn("## Earlier attempts", self.calls("sde")[0]["prompt"])

    async def test_without_the_memory_tools_there_is_nothing_to_read_it_with(self) -> None:
        await self.prompt()
        self.access = None
        self.assertNotIn("memory_read", await self.prompt())

    async def test_a_session_that_cannot_be_granted_the_tools_is_not_told_of_the_record(self) -> None:
        self.index("- [Tried](notes/tried.md)\n")
        await self.prompt(text="one")
        granted = await self.prompt(text="two")
        self.assertIn("## Earlier attempts", granted)
        with ungranted(self.world.db):
            denied = await self.prompt(text="two")
        self.assertNotIn("memory_read", denied)
        self.assertNotIn("## Earlier attempts", denied)
        self.assertEqual(self.recorded()[0], "没有改动 · Faster tests")


class TestDelivered(SDEDeliveryCase):
    def setUp(self) -> None:
        super().setUp()
        self.kept = Notes(self.root)
        self.addCleanup(self.kept.close)

    def executor(self, review: bool = False, delivery: Any = None) -> Any:
        executor = super().executor(review, delivery)
        executor._attempts = self.kept.notes
        return executor

    async def test_a_merged_pr_is_linked(self) -> None:
        self.set_plan(sde=[delivered_dev({"calc.py": "x = 1\n"})], reviewer=[PASS])
        outcome = await self.executor(True, self.delivery(auto_merge=True)).execute(
            [event(self.payload(attempt_notes=GOAL))], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(self.kept.entries(project="x"), ["已合并 · Reject zero divisors"])
        self.assertIn("https://github.example/o/r/pull/1", self.kept.text("x"))

    async def test_an_open_pr_is_not_merged_yet(self) -> None:
        self.set_plan(sde=[delivered_dev({"calc.py": "x = 1\n"})])
        outcome = await self.executor(delivery=self.delivery()).execute(
            [event(self.payload(attempt_notes=GOAL))], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(self.kept.entries(project="x"), ["没合并 · Reject zero divisors"])
        self.assertIn("https://github.example/o/r/pull/1", self.kept.text("x"))

    async def test_a_pr_whose_delivery_waits_for_a_person_is_known_to_the_record(self) -> None:
        self.set_plan(sde=[delivered_dev({"calc.py": "x = 1\n"})], reviewer=[PASS])
        self.caps.permissions = Mock()
        self.caps.permissions.check.side_effect = lambda tool, args: PermissionDecision(
            action="defer" if tool == MERGE_TOOL else "allow", reason="ask first")
        outcome = await self.executor(True, self.delivery(auto_merge=True)).execute(
            [event(self.payload(attempt_notes=GOAL))], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.kept.notes.session_closed(str(outcome.session_id))
        self.assertEqual(self.kept.entries(project="x"), ["停止 · Reject zero divisors"])
        self.assertIn("- PR: https://github.example/o/r/pull/1", self.kept.text("x"))

    async def test_later_attempts_are_not_given_the_merge_of_the_first(self) -> None:
        async def run(title: str, step: Any) -> None:
            self.set_plan(sde=[step], reviewer=[PASS])
            self.log.unlink(missing_ok=True)
            await self.executor(True, self.delivery(auto_merge=True)).execute(
                [event(self.payload(attempt_notes=GOAL, title=title))], None, self.caps)

        await run("first", delivered_dev({"calc.py": "x = 1\n"}))
        nothing = delivered_dev({}, kind="test_docs", commit=None, code_change=False)
        await run("second", nothing)
        await run("third", delivered_dev({"calc.py": "x = 2\n"}))
        self.assertEqual(self.kept.entries(project="x"), ["没合并 · third", "没有改动 · second", "已合并 · first"])
        self.assertEqual(self.kept.text("x").count("- PR: "), 1)


class TestTheNextTaskReadsIt(SDENotesCase):
    async def test_what_the_first_task_tried_reaches_the_second_through_memory_read(self) -> None:
        db = self.world.db
        sessions = SQLiteSessionStoreAdapter(db, on_closed=self.notes.session_closed)
        inbox = SQLiteInboxAdapter(db)

        def submit(number: int, text: str) -> None:
            inbox.append(Event(
                inbox_id=ALPHA, source="test", external_id=f"order-{number}", correlation_key=f"task:{number}",
                payload=self.payload(text=text, title=f"Round {number}", attempt_notes=GOAL)))

        async def process(*steps: Any) -> Any:
            self.set_plan(sde=steps)
            worker = AgentWorker(
                worker_id="w", inbox=inbox, timers=SQLiteTimerAdapter(db), memory=self.world.registry.memory_for(ALPHA),
                permissions=InMemoryPermission(), outbox=SQLiteOutboxAdapter(db), lock=SQLiteLeaseLockAdapter(db),
                sessions=sessions, executors={"sde": self.sde()}, global_default_executor="sde")
            [report] = await worker.process_inbox(ALPHA)
            return report

        async with self.world.access.serving():
            submit(1, "make the tests faster")
            await process(NO_COMMIT)
            submit(1, "make the tests faster")
            submit(2, "make the tests faster, again")
            await process({**NO_COMMIT, "memory": [["memory_read", {"uri": NOTE}]]})
        second = self.calls("sde")
        [answer] = [c for c in second if "memory" in c]
        [(failed, read)] = answer["memory"]
        self.assertFalse(failed)
        content = json.loads(read)["content"]
        self.assertEqual(headings(content), ["没有改动 · Round 1"])
        self.assertIn("external_id: order-1", content)
        self.assertIn("> xdist gains nothing", content)
        self.assertIn(NOTE, [c for c in second if "prompt" in c][-1]["prompt"])
        self.assertEqual(self.recorded(), ["没有改动 · Round 2", "没有改动 · Round 1"])
