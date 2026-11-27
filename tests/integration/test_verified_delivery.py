import json
import os
import unittest
from unittest.mock import Mock, patch

from agent_runtime.adapters.delivery import DeliveryRecord
from agent_runtime.adapters.executor_cli import CommandResult, OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import OrcaVisibleRunner, RoleReply
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA, SDEExecutor, SDEOptions, _State, inspect_worktree
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteDeliveryStore,
    SQLiteInboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteWorkUsage,
)
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore, TerminalBinding
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.models import Emit, Event, SessionRef, Suspend
from tests.support.input_decision import DECISION
from tests.support.empty_guidance import empty_project_guidance
from tests.support.fakes import InMemoryPermission
from tests.support.coder_reviewer import fake_command
from tests.support.pr_delivery import DeliveryCase, git
from tests.support.verified_delivery import ReclaimCase
from tests.support.jsonl import read_jsonl


EVIDENCE = "red: test_zero FAILED\ngreen: 4 passed"


def report(kind="fix", **extra):
    return {"status": "done", "summary": "divide rejects zero", "test_evidence": EVIDENCE, "change_kind": kind,
            "change_rationale": f"{kind}: divide now raises on zero", **extra}


def step(files=None, commit="divide rejects zero", **output):
    return {"write": files or {}, "commit": commit, "output": report(**output)}


def event(payload, key="task:1"):
    return Event(inbox_id="proj:x", source="test", external_id=f"e-{os.urandom(4).hex()}", payload=payload,
                 correlation_key=key)


def resume(outcome, key="task:1"):
    return SessionRef(session_id=outcome.session_id or "s", inbox_id="proj:x", correlation_key=key,
                      executor_id="sde", executor_session_id=outcome.executor_session_id, status="suspended",
                      checkpoint=outcome.session_checkpoint, pending_request_id=outcome.request_id)


class FakeTerminal(OrcaVisibleRunner):
    # A visible-terminal double: each turn applies the next planned step in the worktree; release records what the
    # delivery store held at the moment the terminal was closed.
    def __init__(self, steps, snapshot):
        super().__init__()
        self.steps, self.snapshot = list(steps), snapshot
        self.prompts, self.released = [], []

    async def converse(self, profile, *, title, workdir, prompt, schema, role_args, is_interrupted,
                       session_id=None, native=None, schema_name="result", inbox_id=None):
        planned = self.steps.pop(0) if len(self.steps) > 1 else self.steps[0]
        self.prompts.append(prompt)
        for rel, text in planned.get("write", {}).items():
            (workdir / rel).parent.mkdir(parents=True, exist_ok=True)
            (workdir / rel).write_text(text)
        if planned.get("commit"):
            git(workdir, "add", "-A")
            git(workdir, "commit", "-qm", planned["commit"])
        return RoleReply(data=planned["output"], native_session_id=native or "native-1", handle="term-1")

    async def release(self, session_id, workdir):
        self.released.append((session_id, self.snapshot()))
        return "closed term-1"


class SDECase(DeliveryCase, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.enterContext(empty_project_guidance())
        self.wt = self.clone("hb-task-1", "hb/task-1")
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan)}))
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.caps.memory.load_baseline.return_value = {}

    def set_plan(self, *steps):
        self.plan.write_text(json.dumps({"sde": list(steps), "reviewer": []}))

    def executor(self, delivery=None, terminal=None, live_peers=None):
        launcher = ClaudeCodeLauncher(name="fake", command=fake_command(self.root))
        return SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", launcher=launcher),
            options=SDEOptions(review=False),
            reviewer=None,
            worktree_manager=OrcaWorktreeManager(workspace_store=self.workspaces,
                                                 session_store=SQLiteSessionStoreAdapter(self.db), backend="git"),
            inbox_checkout_map={},
            tasks=SQLiteTaskMetadataStore(self.db),
            delivery=delivery,
            terminal=terminal,
            live_peers=live_peers,
        )

    def payload(self):
        return {"executor_id": "sde", "text": "divide must reject a zero divisor", "title": "Reject zero divisors",
                "explicit_workdir": str(self.wt), "base_ref": "origin/main"}

    def sde_calls(self):
        rows = read_jsonl(self.log)
        return [r for r in rows if r["role"] == "sde"]


class TestWorktreeInspection(SDECase):
    def test_staged_unstaged_untracked_and_commits_are_seen_against_the_base(self):
        base = git(self.wt, "rev-parse", "HEAD")
        clean = inspect_worktree(self.wt, base)
        self.assertEqual((clean.commits, clean.changes, clean.error), (0, (), ""))
        (self.wt / "calc.py").write_text("x = 1\n")
        git(self.wt, "add", "calc.py")
        (self.wt / "pkg" / "__init__.py").write_text('__version__ = "0.4.0"\n# edited\n')
        (self.wt / "tests").mkdir()
        (self.wt / "tests" / "test_new.py").write_text("def test_x():\n    pass\n")
        dirty = inspect_worktree(self.wt, base)
        self.assertEqual(sorted(c[3:] for c in dirty.changes), ["calc.py", "pkg/__init__.py", "tests/test_new.py"])
        self.assertIn("M  calc.py", dirty.changes)
        git(self.wt, "add", "-A")
        git(self.wt, "commit", "-qm", "work")
        done = inspect_worktree(self.wt, base)
        self.assertEqual((done.commits, done.changes), (1, ()))
        self.assertIn("unknown", inspect_worktree(self.wt, "0" * 40).error or "unknown")


class TestCodeDeliveryVerification(SDECase):
    async def test_uncommitted_tests_and_docs_are_sent_back_once_then_left_incomplete_and_kept(self):
        files = {"tests/test_calc.py": "def test_zero():\n    pass\n", "docs/calc.md": "divide rejects zero\n"}
        self.set_plan(step(files, commit=None, kind="test_docs"))
        executor = self.executor(delivery=self.delivery())
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("[sde] incomplete: uncommitted changes", outcome.prompt)
        self.assertIn("tests/test_calc.py", outcome.prompt)
        self.assertIn(", this session and its terminal are kept so the developer can finish", outcome.prompt)
        calls = self.sde_calls()
        self.assertEqual(len(calls), 2)
        self.assertIn("Hibernaut checked the worktree", calls[1]["prompt"])
        self.assertEqual(self.gh_state().get("prs", []), [])
        self.assertEqual(self.remote_head("hb/task-1"), "")

        stopped = await executor.execute(
            [event({"approval_request_id": outcome.request_id, "approved": False})], resume(outcome), self.caps)
        assert isinstance(stopped, Emit), stopped
        self.assertIn("[sde] stopped: incomplete work left in place", stopped.messages[0])
        self.assertNotIn("[sde] delivered", stopped.messages[0])
        self.assertIn("Nothing was delivered; the worktree, this session and its terminal are kept",
                      stopped.messages[0])
        self.assertTrue((self.wt / "tests" / "test_calc.py").exists())
        self.assertIn("tests/", git(self.wt, "status", "--porcelain"))

    async def test_approving_incomplete_work_sends_the_developer_back_and_then_delivers(self):
        self.set_plan(step({"calc.py": "x = 1\n"}, commit=None), step({"calc.py": "x = 1\n"}, commit=None),
                      step({"calc.py": "x = 2\n"}))
        executor = self.executor(delivery=self.delivery())
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        resumed = await executor.execute(
            [event({"approval_request_id": outcome.request_id, "approved": True})], resume(outcome), self.caps)
        assert isinstance(resumed, Emit), resumed
        self.assertIn("Delivery: pr_open", resumed.messages[0])
        self.assertIn("uncommitted changes", self.sde_calls()[2]["prompt"])

    async def test_a_clean_tree_without_commits_is_not_delivery(self):
        self.set_plan(step(commit=None))
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("[sde] incomplete: no commit on this branch since the task's base", outcome.prompt)
        self.assertEqual(self.gh_state().get("prs", []), [])

    async def test_without_a_delivery_step_unfinished_work_is_still_not_reported_delivered(self):
        self.set_plan(step({"calc.py": "x = 1\n"}, commit=None))
        outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("[sde] incomplete: uncommitted changes", outcome.prompt)

    async def test_a_declared_no_code_outcome_skips_review_and_delivery(self):
        self.set_plan(step(commit=None, code_change=False, kind="test_docs"))
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("[sde] finished without a code change", outcome.messages[0])
        self.assertIn(f"no commit and a clean worktree since {git(self.wt, 'rev-parse', 'HEAD')[:12]}",
                      outcome.messages[0])
        self.assertNotIn("Delivery:", outcome.messages[0])
        self.assertEqual(len(self.sde_calls()), 1)
        self.assertEqual(self.gh_state().get("prs", []), [])
        self.assertIn("code_change", SDE_SCHEMA["properties"])

    async def test_no_code_claimed_with_commits_present_is_delivered_as_a_change(self):
        self.set_plan(step({"calc.py": "x = 1\n"}, code_change=False))
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("Delivery: pr_open", outcome.messages[0])

    async def test_no_code_claimed_with_a_dirty_tree_is_incomplete(self):
        self.set_plan(step({"calc.py": "x = 1\n"}, commit=None, code_change=False))
        outcome = await self.executor(delivery=self.delivery()).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("[sde] incomplete: uncommitted changes", outcome.prompt)


class TestTerminalExit(SDECase):
    def snapshot(self):
        return self.store.get("task:1")

    async def run_task(self, steps, delivery, live_peers=None):
        terminal = FakeTerminal(steps, self.snapshot)
        outcome = await self.executor(delivery=delivery, terminal=terminal, live_peers=live_peers).execute(
            [event(self.payload())], None, self.caps)
        return outcome, terminal

    async def test_verified_pr_persists_sha_pr_and_session_before_the_terminal_closes(self):
        outcome, terminal = await self.run_task([step({"calc.py": "x = 1\n"})], self.delivery(close_terminal=True))
        assert isinstance(outcome, Emit), outcome
        [(session_id, at_close)] = terminal.released
        head = self.remote_head("hb/task-1")
        self.assertEqual(session_id, outcome.session_id)
        self.assertEqual((at_close["pushed_sha"], at_close["verified_sha"]), (head, head))
        self.assertEqual(at_close["pr_url"], "https://github.example/o/r/pull/1")
        self.assertEqual((at_close["session_id"], at_close["native_session_id"], at_close["terminal"]),
                         (outcome.session_id, "native-1", "term-1"))
        self.assertIn("Terminal: closed term-1; resume with Claude Code session native-1", outcome.messages[0])
        # Closing the terminal reclaims nothing.
        self.assertTrue(self.wt.is_dir())

    async def finish_open_pr(self, *, close=False, auto_merge=False, live_peers=None):
        delivery = self.delivery(close_terminal=close, auto_merge=auto_merge)
        sha = "a" * 40
        record = DeliveryRecord(state="pr_open", pr_url="https://github.example/o/r/pull/1", head_sha=sha,
                                pushed_sha=sha, verified_sha=sha, checked_sha=sha, checks=[{"command": "test", "exit_code": 0}])
        terminal = FakeTerminal([], self.snapshot)
        executor = self.executor(delivery=delivery, terminal=terminal, live_peers=live_peers)
        state = _State(stage="develop", work_key="task:1", workdir=str(self.wt), base_ref="origin/main",
                       requirements="fix divide", change_kind="fix", terminal="term-1")
        with patch.object(delivery, "deliver", return_value=record):
            outcome = await executor._finish("session", "native-1", state, self.caps, review="off")
        return outcome, terminal

    async def test_terminal_stays_when_closing_is_not_configured(self):
        outcome, terminal = await self.finish_open_pr()
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(terminal.released, [])
        self.assertIn("Terminal: kept term-1 (close_terminal is off)", outcome.messages[0])

    async def test_unconfirmed_pr_head_suspends_with_the_terminal_open(self):
        state = self.gh_state()
        state["view_head_override"] = "f" * 40
        self.state_file.write_text(json.dumps(state))
        outcome, terminal = await self.run_task([step({"calc.py": "x = 1\n"})], self.delivery(close_terminal=True))
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("is not at the pushed commit", outcome.prompt)
        self.assertEqual(terminal.released, [])

    async def test_a_live_reviewer_exchange_keeps_the_terminal(self):
        outcome, terminal = await self.finish_open_pr(
            close=True, live_peers=lambda key: [f"review session r1 on review:{key} (active)"])
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(terminal.released, [])
        self.assertIn("Terminal: kept term-1 (review session r1 on review:task:1 (active))", outcome.messages[0])

    async def test_authorized_merge_policy_keeps_the_terminal_until_the_pr_is_merged(self):
        # auto_merge needs review=pass; with review off the PR stays open, so the target is not reached.
        outcome, terminal = await self.finish_open_pr(close=True, auto_merge=True)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(terminal.released, [])
        self.assertIn("merging is authorized but the PR is not merged", outcome.messages[0])

    async def test_needs_input_keeps_the_terminal(self):
        question = {"write": {}, "output": report(status="needs_input", decision=DECISION, question="向下取整吗？")}
        outcome, terminal = await self.run_task([question], self.delivery(close_terminal=True))
        assert isinstance(outcome, Suspend), outcome
        self.assertEqual(terminal.released, [])

    async def test_a_failed_push_then_restart_resumes_and_closes_only_after_evidence(self):
        state = self.gh_state()
        state["fail"] = {"create": 1}
        self.state_file.write_text(json.dumps(state))
        outcome, terminal = await self.run_task([step({"calc.py": "x = 1\n"})], self.delivery(close_terminal=True))
        assert isinstance(outcome, Suspend), outcome
        self.assertEqual(terminal.released, [])
        # A new process: fresh delivery object over the same database, same terminal binding.
        executor = self.executor(delivery=self.delivery(close_terminal=True), terminal=terminal)
        resumed = await executor.execute(
            [event({"approval_request_id": outcome.request_id, "approved": True})], resume(outcome), self.caps)
        assert isinstance(resumed, Emit), resumed
        self.assertEqual(len(terminal.released), 1)
        self.assertEqual(len(self.gh_state()["prs"]), 1)


class TestRunnerRelease(DeliveryCase, unittest.IsolatedAsyncioTestCase):
    def bound(self):
        bindings = SQLiteTerminalBindingStore(self.db)
        bindings.save(TerminalBinding(session_id="s1", executor_id="sde", agent="claude", workdir=str(self.root),
                                      handle="term-1", incarnation_id="inc-1", native_session_id="native-1"))
        return bindings, OrcaVisibleRunner(bindings=bindings)

    async def test_release_closes_the_terminal_and_keeps_the_conversation_resumable(self):
        bindings, runner = self.bound()
        calls = []

        async def orca(*args, cwd):
            calls.append(args)
            return {}

        with patch("agent_runtime.adapters.executor_cli.orca._orca", new=orca):
            self.assertEqual(await runner.release("s1", self.root), "closed term-1")
        self.assertEqual(calls, [("terminal", "close", "--terminal", "term-1")])
        binding = bindings.get("s1")
        assert binding is not None
        self.assertEqual((binding.handle, binding.native_session_id), (None, "native-1"))

    async def test_a_terminal_that_fails_to_close_stays_bound(self):
        bindings, runner = self.bound()

        async def orca(*args, cwd):
            raise RuntimeError("orca is not running")

        with patch("agent_runtime.adapters.executor_cli.orca._orca", new=orca):
            result = await runner.release("s1", self.root)
        self.assertIn("could not close term-1", result)
        binding = bindings.get("s1")
        assert binding is not None
        self.assertEqual(binding.handle, "term-1")


class TestReclaimEvidence(ReclaimCase, unittest.TestCase):
    def test_orca_removal_targets_the_registered_path(self):
        calls = []

        def runner(cmd, cwd):
            if cmd[0] == "orca":
                calls.append((cmd, cwd))
                return CommandResult(0, "", "")
            return self.runner(cmd, cwd)

        manager = OrcaWorktreeManager(workspace_store=self.workspaces, session_store=self.sessions,
                                      backend="orca", runner=runner)
        self.assertTrue(manager.reclaim("task:9").reclaimed)
        self.assertEqual(calls, [(["orca", "worktree", "rm", "--worktree", f"path:{self.wt}"], self.primary)])

    def test_local_only_commit_keeps_the_worktree(self):
        self.commit()
        result = self.manager().reclaim("task:9")
        self.assertFalse(result.reclaimed)
        self.assertIn("unpushed commits", result.reason)
        self.assertTrue(self.wt.is_dir())

    def test_squash_merged_branch_is_reclaimed_on_merge_evidence(self):
        head, merge = self.squashed_task()
        self.assertFalse(self.manager().reclaim("task:9").reclaimed)
        self.merged_pr(head, merge)
        result = self.manager().reclaim("task:9")
        self.assertTrue(result.reclaimed, result.reason)
        self.assertFalse(self.wt.exists())
        self.assertIsNone(self.workspaces.get("task:9"))

    def test_a_commit_after_the_squash_merge_keeps_the_worktree(self):
        head, merge = self.squashed_task()
        self.merged_pr(head, merge)
        self.commit("x = 2\n")
        result = self.manager().reclaim("task:9")
        self.assertFalse(result.reclaimed)
        self.assertIn("unpushed commits", result.reason)

    def test_merge_commit_missing_from_the_remote_is_not_evidence(self):
        head, _ = self.squashed_task()
        self.merged_pr(head, "e" * 40)
        self.assertFalse(self.manager().reclaim("task:9").reclaimed)

    def test_a_failed_pr_lookup_keeps_the_worktree(self):
        self.gh["open"] = CommandResult(1, "", "gh: could not resolve host")
        result = self.manager().reclaim("task:9")
        self.assertFalse(result.reclaimed)
        self.assertIn("could not check for open PRs", result.reason)
        self.assertTrue(self.wt.is_dir())

    def test_an_open_pr_keeps_the_worktree(self):
        self.gh["open"] = CommandResult(0, '[{"number": 3}]', "")
        self.assertIn("open PR", self.manager().reclaim("task:9").reason)

    def test_outstanding_users_and_work_keep_the_worktree(self):
        usage = SQLiteWorkUsage(self.db)
        self.sessions.save(SessionRef(session_id="rev-1", inbox_id="proj:x", correlation_key="review:task:9",
                                      work_key="review:task:9", status="active"))
        result = self.manager(usage.blockers).reclaim("task:9")
        self.assertFalse(result.reclaimed)
        self.assertIn("session rev-1 (active)", result.reason)
        self.sessions.close_session("rev-1")

        inbox = SQLiteInboxAdapter(self.db)
        inbox.append(Event(inbox_id="proj:x", source="test", external_id="r2", correlation_key="review:x",
                           payload={"review_of": "task:9", "text": "review again"}))
        self.assertIn("event", self.manager(usage.blockers).reclaim("task:9").reason)
        for evt in inbox.fetch_pending("proj:x"):
            inbox.mark_done([evt.event_id])

        SQLiteDeliveryStore(self.db).save("task:9", {"state": "local", "problem": "gh pr create failed"})
        self.assertIn("delivery is unfinished: gh pr create failed",
                      self.manager(usage.blockers).reclaim("task:9").reason)
        SQLiteDeliveryStore(self.db).save("task:9", {"state": "pr_open", "problem": ""})
        self.assertTrue(self.manager(usage.blockers).reclaim("task:9").reclaimed)

    def test_blockers_are_rechecked_after_the_claim(self):
        answers = [[], ["session late-1 (active)"]]
        result = self.manager(lambda _key: answers.pop(0) if answers else []).reclaim("task:9")
        self.assertFalse(result.reclaimed)
        self.assertIn("on recheck", result.reason)
        self.assertIn("late-1", result.reason)
        record = self.workspaces.get("task:9")
        assert record is not None
        self.assertEqual(record.status, "ready")
        self.assertTrue(self.wt.is_dir())

    def test_restart_reads_blockers_from_the_database_file(self):
        SQLiteDeliveryStore(self.db).save("task:9", {"state": "local", "problem": "checks failed"})
        db_path = self.root / "state.db"
        disk = SQLiteDatabase(db_path)
        try:
            SQLiteDeliveryStore(disk).save("task:9", {"state": "local", "problem": "checks failed"})
        finally:
            disk.close()
        reopened = SQLiteDatabase(db_path)
        try:
            self.assertEqual(SQLiteWorkUsage(reopened).blockers("task:9"), ["delivery is unfinished: checks failed"])
        finally:
            reopened.close()


if __name__ == "__main__":
    unittest.main()
