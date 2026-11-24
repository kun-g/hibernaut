import asyncio
import json
import shutil
import unittest
from pathlib import Path

from agent_runtime.adapters.delivery import DeliveryConfig
from agent_runtime.adapters.executor_cli import CommandResult, OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.reclaim_sweep import SweepTarget, WorktreeReclaimSweeper
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDeliveryStore,
    SQLiteInboxAdapter,
    SQLiteOpenRunStore,
    SQLiteOutboxAdapter,
    SQLiteWorkUsage,
)
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import Event, SessionRef
from tests.test_pr_delivery import git
from tests.test_verified_delivery import ReclaimCase

INBOX = "proj:x"


class SweepCase(ReclaimCase):
    def setUp(self):
        super().setUp()
        self.usage = SQLiteWorkUsage(self.db)
        self.deliveries = SQLiteDeliveryStore(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.inbox = SQLiteInboxAdapter(self.db)
        self.logged: list[str] = []
        self.delivery_config = DeliveryConfig(repo="o/r", open_pr=True, auto_merge=True)

    def target_for(self, record):
        if Path(record.repo_checkout).resolve() != self.primary.resolve():
            return None
        return SweepTarget(manager=self.manager(self.usage.blockers), delivery=self.delivery_config,
                           inbox_id=record.inbox_id or INBOX)

    def sweeper(self):
        return WorktreeReclaimSweeper(workspaces=self.workspaces, deliveries=self.deliveries, outbox=self.outbox,
                                      target_for=self.target_for, log=self.logged.append)

    def delivered(self):
        # What a merged PR delivery leaves behind: the branch pushed, squash-merged and pruned, the record merged.
        head, merge = self.squashed_task()
        self.merged_pr(head, merge)
        self.deliveries.save("task:9", {"state": "merged", "branch": "hb/task-9", "pushed_sha": head,
                                        "merge_sha": merge})
        return head

    def queued_event(self):
        evt, _ = self.inbox.append(Event(inbox_id=INBOX, source="test", external_id="late", correlation_key="task:9",
                                         payload={"text": "one more thing"}))
        return evt.event_id

    def drop(self, event_id):
        with self.db.transaction() as conn:
            conn.execute("UPDATE event_inbox SET status = 'dropped' WHERE event_id = ?", (event_id,))

    def messages(self):
        return [m.content for m in self.outbox.fetch_pending(limit=100)]

    def local_branch(self):
        return git(self.primary, "branch", "--list", "hb/task-9")


class TestSweepRetriesSkippedReclaims(SweepCase, unittest.TestCase):
    def test_a_reclaim_skipped_for_a_pending_event_is_retried_once_the_event_settles(self):
        head = self.delivered()
        event_id = self.queued_event()
        at_delivery = self.manager(self.usage.blockers).reclaim("task:9", notify_outbox=self.outbox, inbox_id=INBOX,
                                                               merged_head=head)
        self.assertFalse(at_delivery.reclaimed)
        self.assertIn(f"event {event_id} (pending)", at_delivery.reason)
        self.assertEqual(len(self.messages()), 1)

        sweeper = self.sweeper()
        for _ in range(2):
            [outcome] = sweeper.sweep_once()
            self.assertFalse(outcome.reclaimed)
            self.assertIn(f"event {event_id} (pending)", outcome.reason)
        self.assertTrue(self.wt.is_dir())
        # Repeated skips stay out of the Outbox and are logged once per reason.
        self.assertEqual(len(self.messages()), 1)
        self.assertEqual(len([line for line in self.logged if event_id in line]), 1)

        self.drop(event_id)
        [outcome] = sweeper.sweep_once()
        self.assertTrue(outcome.reclaimed, outcome.reason)
        self.assertFalse(self.wt.exists())
        self.assertIsNone(self.workspaces.get("task:9"))
        # Same branch rule as the delivery-time reclaim: the merged head lets the local branch go.
        self.assertEqual(self.local_branch(), "")
        reclaimed = self.messages()[1:]
        self.assertEqual(len(reclaimed), 1)
        self.assertIn(str(self.wt), reclaimed[0])
        self.assertIn("deleted local branch hb/task-9", reclaimed[0])
        self.assertEqual(sweeper.sweep_once(), [])

    def test_a_done_event_releases_the_worktree_too(self):
        self.delivered()
        event_id = self.queued_event()
        sweeper = self.sweeper()
        self.assertFalse(sweeper.sweep_once()[0].reclaimed)
        self.inbox.mark_done([event_id])
        self.assertTrue(sweeper.sweep_once()[0].reclaimed)
        self.assertFalse(self.wt.exists())

    def test_a_merged_pr_at_the_worktree_head_is_evidence_without_a_delivery_record(self):
        head, merge = self.squashed_task()
        self.merged_pr(head, merge)
        [outcome] = self.sweeper().sweep_once()
        self.assertTrue(outcome.reclaimed, outcome.reason)
        self.assertFalse(self.wt.exists())
        # No delivery saw the merge, so the local branch stays, as for any reclaim without a merged head.
        self.assertEqual(self.local_branch(), "hb/task-9")


class TestSweepKeepsWorktreesInUse(SweepCase, unittest.TestCase):
    def test_an_unmerged_pr_keeps_the_worktree(self):
        head = self.commit()
        git(self.wt, "push", "-q", "origin", "hb/task-9")
        self.deliveries.save("task:9", {"state": "pr_open", "branch": "hb/task-9", "pushed_sha": head})
        [outcome] = self.sweeper().sweep_once()
        self.assertFalse(outcome.reclaimed)
        self.assertIn("not merged", outcome.reason)
        self.assertTrue(self.wt.is_dir())
        self.assertEqual(self.messages(), [])

    def test_a_fresh_worktree_without_merge_evidence_is_kept(self):
        # Every safety gate passes here (nothing committed, no open PR); only the missing merge keeps it.
        [outcome] = self.sweeper().sweep_once()
        self.assertFalse(outcome.reclaimed)
        self.assertTrue(self.wt.is_dir())

    def test_a_merged_pr_at_another_head_is_not_evidence(self):
        head, merge = self.squashed_task()
        self.merged_pr("f" * 40, merge)
        self.assertNotEqual(head, "f" * 40)
        self.assertFalse(self.sweeper().sweep_once()[0].reclaimed)
        self.assertTrue(self.wt.is_dir())

    def test_an_active_session_keeps_the_worktree(self):
        self.delivered()
        self.sessions.save(SessionRef(session_id="dev-1", inbox_id=INBOX, correlation_key="task:9",
                                      work_key="task:9", status="active"))
        [outcome] = self.sweeper().sweep_once()
        self.assertFalse(outcome.reclaimed)
        self.assertIn("active/suspended session", outcome.reason)
        self.assertTrue(self.wt.is_dir())
        self.sessions.close_session("dev-1")
        self.assertTrue(self.sweeper().sweep_once()[0].reclaimed)

    def test_a_failing_safety_gate_keeps_the_worktree(self):
        self.delivered()
        (self.wt / "scratch.txt").write_text("wip\n")
        [outcome] = self.sweeper().sweep_once()
        self.assertFalse(outcome.reclaimed)
        self.assertIn("uncommitted changes present", outcome.reason)
        self.assertTrue(self.wt.is_dir())
        self.assertEqual(self.workspaces.get("task:9").status, "ready")
        self.assertEqual(self.messages(), [])

    def test_an_open_pr_keeps_the_worktree(self):
        self.delivered()
        self.gh["open"] = CommandResult(0, '[{"number": 4}]', "")
        self.assertIn("open PR", self.sweeper().sweep_once()[0].reason)
        self.assertTrue(self.wt.is_dir())


class TestSweepTouchesOnlyHibernautWorktrees(SweepCase, unittest.TestCase):
    def test_a_worktree_without_a_workspaces_record_is_never_touched(self):
        manual = self.root / "hb-manual"
        git(self.primary, "worktree", "add", "-q", "-b", "hb/manual", str(manual))
        self.delivered()
        outcomes = self.sweeper().sweep_once()
        self.assertEqual([o.work_key for o in outcomes], ["task:9"])
        self.assertTrue(manual.is_dir())

    def test_a_recorded_directory_without_the_hb_prefix_is_refused(self):
        other = self.root / "mine"
        git(self.primary, "worktree", "add", "-q", "-b", "mine", str(other))
        self.workspaces.reserve_creating(work_key="task:10", repo_checkout=str(self.primary), workdir=str(other),
                                         branch="mine")
        self.workspaces.mark_ready("task:10", workdir=str(other), branch="mine")
        self.deliveries.save("task:10", {"state": "merged", "branch": "mine"})
        outcomes = {o.work_key: o for o in self.sweeper().sweep_once()}
        self.assertFalse(outcomes["task:10"].reclaimed)
        self.assertIn("'hb-' prefix", outcomes["task:10"].reason)
        self.assertTrue(other.is_dir())

    def test_a_record_of_an_unconfigured_checkout_is_left_alone(self):
        self.delivered()
        sweeper = WorktreeReclaimSweeper(workspaces=self.workspaces, deliveries=self.deliveries,
                                         outbox=self.outbox, target_for=lambda _record: None,
                                         log=self.logged.append)
        [outcome] = sweeper.sweep_once()
        self.assertFalse(outcome.reclaimed)
        self.assertIn("no configured project", outcome.reason)
        self.assertTrue(self.wt.is_dir())

    def test_a_record_still_being_created_is_skipped(self):
        self.workspaces.reserve_creating(work_key="task:11", repo_checkout=str(self.primary),
                                         workdir=str(self.root / "hb-task-11"), branch="hb/task-11")
        self.assertEqual([o.work_key for o in self.sweeper().sweep_once()], ["task:9"])


class TestSweepForgetsWorktreesRemovedByHand(SweepCase, unittest.TestCase):
    # The directory was deleted outside Hibernaut (by hand, by Orca, by an older flow); only the record is left.
    def session(self, session_id="dev-1", status="active", correlation_key="task:9"):
        self.sessions.save(SessionRef(session_id=session_id, inbox_id=INBOX, correlation_key=correlation_key,
                                      work_key="task:9", status=status))

    def remove_by_hand(self, prune=True):
        shutil.rmtree(self.wt)
        if prune:
            git(self.primary, "worktree", "prune")

    def registered(self):
        return str(self.wt.resolve()) in git(self.primary, "worktree", "list", "--porcelain")

    def test_a_missing_directory_drops_the_record_closes_its_sessions_and_alerts_once(self):
        self.session("dev-1", "active")
        self.session("dev-2", "suspended", correlation_key="task:9:child")
        self.remove_by_hand()
        sweeper = self.sweeper()
        [outcome] = sweeper.sweep_once()
        self.assertTrue(outcome.reclaimed, outcome.reason)
        self.assertIsNone(self.workspaces.get("task:9"))
        self.assertEqual(self.sessions.get("dev-1").status, "closed")
        self.assertEqual(self.sessions.get("dev-2").status, "closed")
        [alert] = self.messages()
        self.assertIn(str(self.wt), alert)
        self.assertIn("no longer exists", alert)
        self.assertIn("record removed", alert)
        self.assertIn("dev-1", alert)
        self.assertIn("dev-2", alert)
        self.assertEqual(self.outbox.fetch_pending(limit=10)[0].metadata.get("kind"), "reclaim_alert")
        self.assertEqual(sweeper.sweep_once(), [])

    def test_no_merge_evidence_is_needed_once_the_directory_is_gone(self):
        # The reused-branch case: HEAD cannot be read, so the merge can never be told; nothing is left to protect.
        self.gh["merged"] = CommandResult(1, "", "gh: no network")
        self.remove_by_hand()
        [outcome] = self.sweeper().sweep_once()
        self.assertTrue(outcome.reclaimed, outcome.reason)
        self.assertIsNone(self.workspaces.get("task:9"))
        [alert] = self.messages()
        self.assertIn("no sessions closed", alert)

    def test_a_worktree_git_still_registers_is_pruned_first(self):
        self.remove_by_hand(prune=False)
        self.assertTrue(self.registered())
        [outcome] = self.sweeper().sweep_once()
        self.assertTrue(outcome.reclaimed, outcome.reason)
        self.assertFalse(self.registered())
        self.assertIsNone(self.workspaces.get("task:9"))

    def test_a_registration_prune_cannot_drop_keeps_the_record(self):
        git(self.primary, "worktree", "lock", str(self.wt))
        self.remove_by_hand(prune=False)
        sweeper = self.sweeper()
        for _ in range(2):
            [outcome] = sweeper.sweep_once()
            self.assertFalse(outcome.reclaimed)
            self.assertIn("git still registers", outcome.reason)
        self.assertEqual(self.workspaces.get("task:9").status, "ready")
        self.assertEqual(self.messages(), [])
        self.assertEqual(len(self.logged), 1)

    def test_a_pending_event_keeps_the_record_and_its_session_and_is_logged_once(self):
        self.session()
        event_id = self.queued_event()
        self.remove_by_hand()
        sweeper = self.sweeper()
        for _ in range(3):
            [outcome] = sweeper.sweep_once()
            self.assertFalse(outcome.reclaimed)
            self.assertIn(f"event {event_id} (pending)", outcome.reason)
        self.assertEqual(self.workspaces.get("task:9").status, "ready")
        self.assertEqual(self.sessions.get("dev-1").status, "active")
        self.assertEqual(self.messages(), [])
        self.assertEqual(len(self.logged), 1)

        self.drop(event_id)
        self.assertTrue(sweeper.sweep_once()[0].reclaimed)
        self.assertEqual(self.sessions.get("dev-1").status, "closed")

    def test_an_open_run_keeps_the_record(self):
        self.session()
        runs = SQLiteOpenRunStore(self.db, self.root / "runs")
        runs.open(session_id="dev-1", inbox_id=INBOX, executor_id="sde", event_ids=[], code_version="t")
        self.remove_by_hand()
        sweeper = self.sweeper()
        for _ in range(2):
            [outcome] = sweeper.sweep_once()
            self.assertFalse(outcome.reclaimed)
            self.assertIn("run of session dev-1 is open", outcome.reason)
        self.assertIsNotNone(self.workspaces.get("task:9"))
        self.assertEqual(self.sessions.get("dev-1").status, "active")
        self.assertEqual(len(self.logged), 1)

        runs.discard("dev-1")
        self.assertTrue(sweeper.sweep_once()[0].reclaimed)

    def test_the_merged_heads_local_branch_goes_with_the_record(self):
        self.delivered()
        self.remove_by_hand()
        [outcome] = self.sweeper().sweep_once()
        self.assertTrue(outcome.reclaimed, outcome.reason)
        self.assertEqual(self.local_branch(), "")
        self.assertIn("deleted local branch hb/task-9", self.messages()[0])

    def test_a_missing_directory_without_the_hb_prefix_is_left_alone(self):
        other = self.root / "mine"
        git(self.primary, "worktree", "add", "-q", "-b", "mine", str(other))
        self.workspaces.reserve_creating(work_key="task:10", repo_checkout=str(self.primary), workdir=str(other),
                                         branch="mine")
        self.workspaces.mark_ready("task:10", workdir=str(other), branch="mine")
        shutil.rmtree(other)
        outcomes = {o.work_key: o for o in self.sweeper().sweep_once()}
        self.assertFalse(outcomes["task:10"].reclaimed)
        self.assertIn("'hb-' prefix", outcomes["task:10"].reason)
        self.assertIsNotNone(self.workspaces.get("task:10"))

    def test_creating_and_reclaiming_records_with_missing_directories_are_not_touched(self):
        self.remove_by_hand()
        record = self.workspaces.get("task:9")
        self.assertIsNotNone(self.workspaces.claim_for_reclaim("task:9", seen_updated_at=record.updated_at,
                                                               stale_before=""))
        self.workspaces.reserve_creating(work_key="task:11", repo_checkout=str(self.primary),
                                         workdir=str(self.root / "hb-task-11"), branch="hb/task-11")
        self.assertEqual(self.sweeper().sweep_once(), [])
        self.assertEqual(self.workspaces.get("task:9").status, "reclaiming")
        self.assertEqual(self.workspaces.get("task:11").status, "creating")

    def test_the_delivery_time_reclaim_handles_a_missing_directory_too(self):
        self.session()
        self.remove_by_hand(prune=False)
        result = self.manager(self.usage.blockers).reclaim("task:9", notify_outbox=self.outbox, inbox_id=INBOX)
        self.assertTrue(result.reclaimed, result.reason)
        self.assertIsNone(self.workspaces.get("task:9"))
        self.assertFalse(self.registered())
        self.assertEqual(self.sessions.get("dev-1").status, "closed")
        [alert] = self.messages()
        self.assertIn("no longer exists", alert)
        self.assertIn("dev-1", alert)

    def test_the_delivery_time_reclaim_keeps_a_missing_directory_still_in_use(self):
        event_id = self.queued_event()
        self.remove_by_hand()
        result = self.manager(self.usage.blockers).reclaim("task:9", notify_outbox=self.outbox, inbox_id=INBOX)
        self.assertFalse(result.reclaimed)
        self.assertIn(f"event {event_id} (pending)", result.reason)
        self.assertEqual(self.workspaces.get("task:9").status, "ready")


class TestSweepAsksOrcaAboutMissingDirectories(SweepCase, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.orca_paths: list[str] = []
        self.orca_calls: list[list[str]] = []
        shutil.rmtree(self.wt)
        git(self.primary, "worktree", "prune")

    def orca_runner(self, cmd, cwd):
        if cmd[0] != "orca":
            return self.runner(cmd, cwd)
        self.orca_calls.append(list(cmd))
        if cmd[:3] == ["orca", "worktree", "list"]:
            rows = [{"path": p} for p in self.orca_paths]
            return CommandResult(0, json.dumps({"ok": True, "result": {"worktrees": rows, "truncated": False}}), "")
        return CommandResult(1, "", "unexpected orca call")

    def target_for(self, record):
        manager = OrcaWorktreeManager(workspace_store=self.workspaces, session_store=self.sessions, backend="orca",
                                      runner=self.orca_runner, usage=self.usage.blockers)
        return SweepTarget(manager=manager, delivery=self.delivery_config, inbox_id=INBOX)

    def test_a_worktree_orca_still_lists_keeps_the_record(self):
        self.orca_paths = [str(self.wt)]
        [outcome] = self.sweeper().sweep_once()
        self.assertFalse(outcome.reclaimed)
        self.assertIn("Orca still lists", outcome.reason)
        self.assertIsNotNone(self.workspaces.get("task:9"))
        self.assertEqual(self.messages(), [])

    def test_a_worktree_orca_no_longer_lists_is_forgotten_without_an_orca_removal(self):
        self.orca_paths = [str(self.primary)]
        [outcome] = self.sweeper().sweep_once()
        self.assertTrue(outcome.reclaimed, outcome.reason)
        self.assertIsNone(self.workspaces.get("task:9"))
        self.assertEqual({tuple(c[:3]) for c in self.orca_calls}, {("orca", "worktree", "list")})


class TestSweepLoop(SweepCase, unittest.IsolatedAsyncioTestCase):
    async def until(self, check):
        async with asyncio.timeout(10):
            while not check():
                await asyncio.sleep(0.01)

    async def test_the_startup_sweep_reclaims_a_leftover_worktree(self):
        self.delivered()
        stop = asyncio.Event()
        loop = asyncio.create_task(self.sweeper().run_forever(interval_seconds=3600, stop_event=stop))
        try:
            await self.until(lambda: not self.wt.exists())
        finally:
            stop.set()
            await asyncio.wait_for(loop, timeout=10)
        self.assertFalse(self.wt.exists())
        self.assertIsNone(self.workspaces.get("task:9"))

    async def test_it_sweeps_again_after_the_interval(self):
        self.delivered()
        event_id = self.queued_event()
        stop = asyncio.Event()
        running = asyncio.create_task(self.sweeper().run_forever(interval_seconds=0.01, stop_event=stop))
        try:
            await self.until(lambda: any(event_id in entry for entry in self.logged))
            self.assertTrue(self.wt.is_dir())
            self.drop(event_id)
            await self.until(lambda: not self.wt.exists())
        finally:
            stop.set()
            await asyncio.wait_for(running, timeout=10)
        self.assertFalse(self.wt.exists())

    async def test_no_round_starts_once_the_process_is_stopping(self):
        self.delivered()
        halt = asyncio.Event()
        halt.set()
        services = asyncio.Event()
        await asyncio.wait_for(self.sweeper().run_forever(interval_seconds=3600, stop_event=services, halt=halt),
                               timeout=5)
        self.assertTrue(self.wt.is_dir())

    async def test_halting_ends_the_wait_between_rounds(self):
        halt = asyncio.Event()
        services = asyncio.Event()
        loop = asyncio.create_task(self.sweeper().run_forever(interval_seconds=3600, stop_event=services, halt=halt))
        await asyncio.sleep(0.2)
        halt.set()
        await asyncio.wait_for(loop, timeout=5)

    def test_a_round_stops_between_worktrees_when_asked(self):
        self.delivered()
        self.assertEqual(self.sweeper().sweep_once(should_stop=lambda: True), [])
        self.assertTrue(self.wt.is_dir())


class TestSweepConfig(unittest.TestCase):
    def project(self, **runtime):
        return parse_config({
            "runtime": {"default_executor": "cli", "db_path": ":memory:", **runtime},
            "executors": {"cli": {"command": "true"}},
            "projects": {"x": {"repo": "o/x", "checkout": "/tmp/x", "inbox_id": INBOX}},
        })

    def test_default_interval_is_five_minutes(self):
        self.assertEqual(parse_config({}).runtime.worktree_sweep_interval_seconds, 300.0)

    def test_zero_turns_the_sweep_off_and_negative_is_refused(self):
        self.assertEqual(self.project(worktree_sweep_interval_seconds=0).runtime.worktree_sweep_interval_seconds, 0.0)
        for bad in (-1, float("inf"), float("nan")):
            with self.assertRaisesRegex(ValueError, "worktree_sweep_interval_seconds"):
                self.project(worktree_sweep_interval_seconds=bad)

    def test_a_configured_daemon_runs_the_sweep_at_the_configured_interval(self):
        daemon = build_daemon_from_config(self.project(worktree_sweep_interval_seconds=120))
        self.addCleanup(daemon.close)
        self.assertIsNotNone(daemon.worktree_sweeper)
        self.assertEqual(daemon.worktree_sweep_interval_seconds, 120.0)

    def test_interval_zero_builds_no_sweeper(self):
        daemon = build_daemon_from_config(self.project(worktree_sweep_interval_seconds=0))
        self.addCleanup(daemon.close)
        self.assertIsNone(daemon.worktree_sweeper)


class TestDaemonServices(unittest.IsolatedAsyncioTestCase):
    async def test_services_run_the_sweep_and_the_stop_signal_halts_it(self):
        daemon = build_daemon_from_config(parse_config({
            "runtime": {"default_executor": "cli", "db_path": ":memory:", "worktree_sweep_interval_seconds": 3600},
            "executors": {"cli": {"command": "true"}},
            "projects": {"x": {"repo": "o/x", "checkout": "/tmp/x", "inbox_id": INBOX}},
        }))
        self.addCleanup(daemon.close)
        rounds: list[int] = []
        assert daemon.worktree_sweeper is not None
        daemon.worktree_sweeper.sweep_once = lambda should_stop=None: rounds.append(1) or []  # type: ignore[method-assign]
        halt, services_stop = asyncio.Event(), asyncio.Event()
        services = asyncio.create_task(daemon.run_services(services_stop, halt=halt))
        for _ in range(100):
            if rounds:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(rounds, [1])
        halt.set()
        await asyncio.sleep(0.1)
        # Halted: the sweep loop has ended while the other services keep running until services_stop.
        self.assertFalse(services.done())
        services_stop.set()
        await asyncio.wait_for(services, timeout=5)
        self.assertEqual(rounds, [1])


if __name__ == "__main__":
    unittest.main()

