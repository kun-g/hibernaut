import json
import unittest
from unittest.mock import Mock

from agent_runtime.adapters.delivery import DeliveryRecord, merged_head, parse_delivery
from agent_runtime.adapters.executor_cli import CommandResult, OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDeliveryStore,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteTimerAdapter,
    SQLiteWorkUsage,
)
from agent_runtime.core.models import Emit, Event
from agent_runtime.engine.worker import AgentWorker
from tests.support.fakes import InMemoryMemoryStore, InMemoryPermission
from tests.support.pr_delivery import DeliveryCase, git
from tests.support.verified_delivery import ReclaimCase
from tests.support.merge_cleanup import _FinishingExecutor


ZERO = "0" * 40


class TestRemoteBranchDeletion(DeliveryCase):
    def merging(self, **overrides):
        return self.delivery(auto_merge=True, **overrides)

    def test_delete_branch_is_on_by_default_and_must_be_a_flag(self):
        self.assertTrue(parse_delivery("d", {"open_pr": True}, repo="o/r").delete_branch)
        self.assertFalse(parse_delivery("d", {"open_pr": True, "delete_branch": False}, repo="o/r").delete_branch)
        with self.assertRaisesRegex(ValueError, "delete_branch must be true or false"):
            parse_delivery("d", {"delete_branch": "yes"}, repo="o/r")

    def test_merging_deletes_the_remote_source_branch(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.merging().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((rec.state, rec.problem), ("merged", ""))
        self.assertEqual(self.remote_head("hb/task-a"), "")
        self.assertIn("deleted origin/hb/task-a", "\n".join(rec.render()))
        self.assertNotIn("--delete-branch", self.gh_calls("merge")[0])

    def published_task(self):
        # The task was pushed and the hosting service now reports it merged. Each case owns the real remote ref.
        branch = "hb/task-a"
        head = git(self.bare, "rev-parse", "main")
        git(self.bare, "branch", branch, head)
        req = self.request(self.seed, work_key="task:task-a")
        rec = DeliveryRecord(state="pr_open", branch=branch, head_sha=head, pushed_sha=head, verified_sha=head)
        self.store.save(req.work_key, rec.to_dict())
        pr = {"head": branch, "number": 1, "url": "https://github.example/o/r/pull/1", "state": "MERGED",
              "merged_head": head, "merge_sha": head}
        self.state_file.write_text(json.dumps({**self.gh_state(), "prs": [pr]}))
        return req

    def test_delete_branch_off_keeps_the_remote_branch(self):
        rec = self.merging(delete_branch=False).deliver(self.published_task(), permissions=InMemoryPermission())
        self.assertEqual(rec.state, "merged")
        self.assertEqual(self.remote_head("hb/task-a"), rec.head_sha)
        self.assertNotIn("deleted origin/hb/task-a", "\n".join(rec.render()))

    def test_a_branch_already_gone_counts_as_deleted(self):
        req = self.published_task()
        git(self.bare, "branch", "-D", "hb/task-a")
        again = self.delivery().deliver(req, permissions=InMemoryPermission())
        self.assertEqual((again.state, again.problem, again.notes), ("merged", "", []))
        self.assertIn("origin/hb/task-a was already deleted", "\n".join(again.render()))

    def test_a_refused_deletion_is_noted_and_the_merge_stands(self):
        hook = self.bare / "hooks" / "pre-receive"
        hook.write_text(f'#!/bin/sh\nwhile read old new ref; do [ "$new" = "{ZERO}" ] && '
                        '{ echo "deletion refused by policy" >&2; exit 1; }; done\nexit 0\n')
        hook.chmod(0o755)
        rec = self.merging().deliver(self.published_task(), permissions=InMemoryPermission())
        self.assertEqual((rec.state, rec.problem), ("merged", ""))
        self.assertEqual(self.remote_head("hb/task-a"), rec.head_sha)
        text = "\n".join(rec.render())
        self.assertIn("Note: branch kept: deleting origin/hb/task-a failed", text)
        self.assertIn("deletion refused by policy", text)

    def test_a_branch_that_moved_after_the_merge_is_kept(self):
        req = self.published_task()
        git(self.seed, "commit", "-q", "--allow-empty", "-m", "late")
        git(self.seed, "push", "-q", "origin", "HEAD:refs/heads/hb/task-a")
        again = self.delivery().deliver(req, permissions=InMemoryPermission())
        self.assertEqual(again.state, "merged")
        self.assertEqual(self.remote_head("hb/task-a"), git(self.seed, "rev-parse", "HEAD"))
        self.assertIn("branch kept: origin/hb/task-a is at", "\n".join(again.notes))

    def test_unmerged_or_local_delivery_keeps_the_branch(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.state, "pr_open")
        self.assertEqual(self.remote_head("hb/task-a"), rec.head_sha)
        local = self.task("task-b", {"calc.py": "x = 2\n"})
        rec_b = self.delivery(open_pr=False).deliver(self.request(local), permissions=InMemoryPermission())
        self.assertEqual(rec_b.state, "local")
        self.assertEqual(git(local, "branch", "--list", "hb/task-b"), "* hb/task-b")

    def test_merged_head_is_the_evidence_for_local_branch_deletion(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        cfg = self.merging().config
        self.assertIsNone(merged_head(cfg, self.store, "task:task-a"))
        merged = self.merging().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(merged_head(cfg, self.store, "task:task-a"), rec.head_sha)
        self.assertEqual(merged.pushed_sha, rec.head_sha)
        self.assertIsNone(merged_head(self.merging(delete_branch=False).config, self.store, "task:task-a"))
        self.assertIsNone(merged_head(None, self.store, "task:task-a"))


class TestReclaimDeletesTheMergedBranch(ReclaimCase, unittest.TestCase):
    def merged_and_pruned(self):
        head, merge = self.squashed_task()
        self.merged_pr(head, merge)
        return head

    def local_branch(self):
        return git(self.primary, "branch", "--list", "hb/task-9")

    def test_git_backend_removes_the_worktree_then_the_squash_merged_branch(self):
        head = self.merged_and_pruned()
        result = self.manager().reclaim("task:9", merged_head=head)
        self.assertTrue(result.reclaimed, result.reason)
        self.assertFalse(self.wt.exists())
        self.assertEqual(self.local_branch(), "")
        self.assertIn("deleted local branch hb/task-9", result.branch)

    def test_without_merge_evidence_the_local_branch_stays(self):
        self.merged_and_pruned()
        result = self.manager().reclaim("task:9")
        self.assertTrue(result.reclaimed, result.reason)
        self.assertEqual(self.local_branch(), "hb/task-9")

    def test_a_branch_not_at_the_merged_head_is_kept_and_reported(self):
        self.merged_and_pruned()
        outbox = Mock()
        result = self.manager().reclaim("task:9", merged_head="f" * 40, notify_outbox=outbox, inbox_id="proj:x")
        self.assertTrue(result.reclaimed, result.reason)
        self.assertEqual(self.local_branch(), "hb/task-9")
        self.assertIn("local branch hb/task-9 kept", result.branch)
        self.assertIn("local branch hb/task-9 kept", outbox.enqueue.call_args.args[1])

    def test_uncommitted_changes_keep_the_worktree_and_the_branch(self):
        head = self.merged_and_pruned()
        (self.wt / "scratch.txt").write_text("wip\n")
        outbox = Mock()
        result = self.manager().reclaim("task:9", merged_head=head, notify_outbox=outbox, inbox_id="proj:x")
        self.assertFalse(result.reclaimed)
        self.assertIn("uncommitted changes present", result.reason)
        self.assertTrue(self.wt.is_dir())
        self.assertEqual(self.local_branch(), "+ hb/task-9")
        self.assertIn("uncommitted changes present", outbox.enqueue.call_args.args[1])

    def orca_manager(self, *, orca_deletes):
        calls = []

        def runner(cmd, cwd):
            if cmd[0] == "orca":
                calls.append(cmd)
                git(cwd, "worktree", "remove", cmd[cmd.index("--worktree") + 1].removeprefix("path:"))
                if orca_deletes and "--delete-branch" in cmd:
                    git(cwd, "branch", "-D", "hb/task-9")
                return CommandResult(0, "", "")
            return self.runner(cmd, cwd)

        return OrcaWorktreeManager(workspace_store=self.workspaces, session_store=self.sessions,
                                   backend="orca", runner=runner), calls

    def test_orca_backend_asks_orca_to_delete_the_branch(self):
        head = self.merged_and_pruned()
        manager, calls = self.orca_manager(orca_deletes=True)
        result = manager.reclaim("task:9", merged_head=head)
        self.assertTrue(result.reclaimed, result.reason)
        self.assertEqual(calls, [["orca", "worktree", "rm", "--worktree", f"path:{self.wt}", "--delete-branch"]])
        self.assertEqual(self.local_branch(), "")

    def test_a_branch_orca_retained_is_deleted_on_the_same_evidence(self):
        head = self.merged_and_pruned()
        manager, calls = self.orca_manager(orca_deletes=False)
        result = manager.reclaim("task:9", merged_head=head)
        self.assertTrue(result.reclaimed, result.reason)
        self.assertIn("--delete-branch", calls[0])
        self.assertEqual(self.local_branch(), "")

    def test_orca_without_merge_evidence_keeps_its_branch_policy(self):
        self.merged_and_pruned()
        manager, calls = self.orca_manager(orca_deletes=True)
        self.assertTrue(manager.reclaim("task:9").reclaimed)
        self.assertEqual(calls, [["orca", "worktree", "rm", "--worktree", f"path:{self.wt}"]])
        self.assertEqual(self.local_branch(), "hb/task-9")


class TestMergedTaskReclaimEndToEnd(ReclaimCase, unittest.IsolatedAsyncioTestCase):
    # A merged delivery record, a worker that closes the finished session and the same reclaim callback the CLI
    # daemon builds: the worktree and its branch go, or a gate keeps them and says why.
    def setUp(self):
        super().setUp()
        self.deliveries = SQLiteDeliveryStore(self.db)
        self.inbox = SQLiteInboxAdapter(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.cfg = parse_delivery("d", {"open_pr": True, "auto_merge": True}, repo="o/r")

    def merged_record(self):
        head, merge = self.squashed_task()
        self.merged_pr(head, merge)
        self.deliveries.save("task:9", DeliveryRecord(state="merged", branch="hb/task-9", pushed_sha=head,
                                                      verified_sha=head, merge_sha=merge).to_dict())

    async def run_task(self):
        from agent_runtime.app.main import worktree_reclaimer

        manager = OrcaWorktreeManager(workspace_store=self.workspaces, session_store=self.sessions, backend="git",
                                      runner=self.runner, usage=SQLiteWorkUsage(self.db).blockers)
        worker = AgentWorker(
            worker_id="w", inbox=self.inbox, timers=SQLiteTimerAdapter(self.db), memory=InMemoryMemoryStore(),
            permissions=InMemoryPermission(), outbox=self.outbox, lock=SQLiteLeaseLockAdapter(self.db),
            sessions=self.sessions, executor=_FinishingExecutor(close=True),
            on_worktree_reclaim=worktree_reclaimer(manager, outbox=self.outbox, inbox_id="proj:x",
                                                   delivery=self.cfg, deliveries=self.deliveries),
        )
        self.inbox.append(Event(inbox_id="proj:x", source="test", external_id="e-1", payload={"text": "go"},
                                correlation_key="task:9"))
        return await worker.process_inbox("proj:x")

    async def test_the_worktree_and_local_branch_are_removed(self):
        self.merged_record()
        await self.run_task()
        self.assertFalse(self.wt.exists())
        self.assertIsNone(self.workspaces.get("task:9"))
        self.assertEqual(git(self.primary, "branch", "--list", "hb/task-9"), "")

    async def test_uncommitted_changes_keep_the_worktree_and_say_why(self):
        self.merged_record()
        (self.wt / "scratch.txt").write_text("wip\n")
        reports = await self.run_task()
        self.assertTrue(self.wt.is_dir())
        self.assertEqual(DeliveryRecord.from_dict(self.deliveries.get("task:9")).state, "merged")
        messages = [m.content for m in self.outbox.fetch_pending()]
        self.assertEqual(messages[0], "[sde] delivered")
        self.assertIn("uncommitted changes present", messages[1])
        self.assertIsInstance(reports[0].outcome, Emit)

    async def test_delete_branch_off_keeps_the_local_branch(self):
        self.merged_record()
        self.cfg = parse_delivery("d", {"open_pr": True, "auto_merge": True, "delete_branch": False}, repo="o/r")
        await self.run_task()
        self.assertFalse(self.wt.exists())
        self.assertEqual(git(self.primary, "branch", "--list", "hb/task-9"), "hb/task-9")


if __name__ == "__main__":
    unittest.main()
