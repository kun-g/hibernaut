import asyncio
import unittest
from pathlib import Path

from agent_runtime.adapters.delivery import DeliveryConfig
from agent_runtime.adapters.executor_cli import CommandResult
from agent_runtime.adapters.executor_cli.reclaim_sweep import SweepTarget, WorktreeReclaimSweeper
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDeliveryStore,
    SQLiteInboxAdapter,
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
        head = self.commit()
        git(self.wt, "push", "-q", "origin", "hb/task-9")
        merge = self.squash_merge()
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
        head = self.commit()
        git(self.wt, "push", "-q", "origin", "hb/task-9")
        self.merged_pr(head, self.squash_merge())
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
        head = self.commit()
        git(self.wt, "push", "-q", "origin", "hb/task-9")
        self.merged_pr("f" * 40, self.squash_merge())
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


class TestSweepLoop(SweepCase, unittest.IsolatedAsyncioTestCase):
    async def test_the_startup_sweep_reclaims_a_leftover_worktree(self):
        self.delivered()
        stop = asyncio.Event()
        loop = asyncio.create_task(self.sweeper().run_forever(interval_seconds=3600, stop_event=stop))
        for _ in range(200):
            if not self.wt.exists():
                break
            await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(loop, timeout=10)
        self.assertFalse(self.wt.exists())
        self.assertIsNone(self.workspaces.get("task:9"))

    async def test_it_sweeps_again_after_the_interval(self):
        self.delivered()
        event_id = self.queued_event()
        stop = asyncio.Event()
        loop = asyncio.create_task(self.sweeper().run_forever(interval_seconds=0.05, stop_event=stop))
        await asyncio.sleep(0.2)
        self.assertTrue(self.wt.is_dir())
        self.drop(event_id)
        for _ in range(200):
            if not self.wt.exists():
                break
            await asyncio.sleep(0.05)
        stop.set()
        await asyncio.wait_for(loop, timeout=10)
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

