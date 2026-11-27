import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.base_merge import SinceReview
from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, render_review, review_prompt, validate_review
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import Emit, PermissionDecision, SessionRef, Suspend
from agent_runtime.engine import AgentWorker, EventHandler
from tests.support.empty_guidance import empty_project_guidance
from tests.support.fakes import InMemoryPermission
from tests.support.review_flow_support import REPORT, ReviewFlowCase
from tests.support.coder_reviewer import CHANGES_REQUESTED
from tests.support.sde_executor import BUGGY, FIXED, PASS, dev, event


class TestSDEConfig(unittest.TestCase):
    def test_role_only_uses_documented_defaults(self):
        cfg = parse_config({
            "executors": {"sde": {"role": "sde"}, "coder-reviewer": {"role": "coder-reviewer"}},
        })
        role = cfg.roles["sde"]
        p = role.profile
        self.assertEqual(role.role, "sde")
        self.assertEqual(
            (p.harness, p.launcher.name, p.billing, p.model, p.effort),
            ("claude-code", "claude", "subscription", "claude-opus-5-5", "high"),
        )
        self.assertEqual(role.sde, SDEOptions(review=True, reviewer="coder-reviewer", max_review_rounds=3))
        self.assertNotIn("sde", cfg.executors)

    def test_four_settings_and_review_switch_are_separate(self):
        cfg = parse_config({
            "credential_proxy": {"url": "http://127.0.0.1:9000"},
            "launchers": {"sg": {"command": "claude-sg"}},
            "executors": {
                "sde": {"role": "sde", "harness": "claude-code", "launcher": "sg", "billing": "api",
                        "model": "sonnet", "effort": "max", "review": False, "max_review_rounds": 5},
            },
        })
        role = cfg.roles["sde"]
        p = role.profile
        self.assertEqual((p.launcher.command, p.billing, p.model, p.effort), ("claude-sg", "api", "sonnet", "max"))
        self.assertEqual(role.sde, SDEOptions(review=False, reviewer="code-reviewer", max_review_rounds=5))

    def test_invalid_settings_fail_with_a_reason(self):
        reviewer = {"coder-reviewer": {"role": "coder-reviewer"}}
        cases = {
            "executors.sde: review must be true or false": {"review": "false"},
            "executors.sde: max_review_rounds must be a positive integer": {"max_review_rounds": 0},
            "executors.sde: unsupported harness 'codex'": {"harness": "codex"},
            "executors.sde: unsupported effort 'ultra'": {"effort": "ultra"},
            "executors.sde: command not supported": {"command": "claude"},
            "executors.sde: reviewer 'ghost' is not a declared executor; choose a code-reviewer executor or a "
            r"CLI executor \(claude, coder-reviewer, cursor-agent, dsh, grok\)": {"reviewer": "ghost"},
        }
        for message, override in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                parse_config({"executors": {"sde": {"role": "sde", **override}, **reviewer}})
        with self.assertRaisesRegex(ValueError, "reviewer 'code-reviewer' is not a declared executor"):
            parse_config({"executors": {"sde": {"role": "sde"}}})
        # With review off no reviewer is needed.
        cfg = parse_config({"executors": {"sde": {"role": "sde", "review": False}}})
        self.assertFalse(cfg.roles["sde"].sde.review)  # type: ignore[union-attr]
        with self.assertRaisesRegex(ValueError, "executors.reviewer: review not supported for role 'code-reviewer'"):
            parse_config({"executors": {"reviewer": {"role": "coder-reviewer", "review": True}}})

    def test_sde_alone_is_enough_to_leave_ingest_only(self):
        cfg = parse_config({
            "runtime": {"db_path": ":memory:", "default_executor": "sde"},
            "executors": {"sde": {"role": "sde", "review": False}},
        })
        daemon = build_daemon_from_config(cfg)
        try:
            self.assertFalse(daemon.ingest_only)
        finally:
            daemon.close()


class TestSDEReviewState(ReviewFlowCase):
    def setUp(self):
        super().setUp()
        self.executor._delivery = None
        self.turns = Mock()
        self.turns.attach_mock(self.develop, "develop")
        self.turns.attach_mock(self.reviewer.review, "review")

    def role_order(self):
        return [name for name, _, _ in self.turns.mock_calls]

    async def decide(self, outcome, approved):
        session = SessionRef(session_id=outcome.session_id, inbox_id="proj:x", correlation_key="task:1",
                             executor_id="sde", executor_session_id=outcome.executor_session_id, status="suspended",
                             checkpoint=outcome.session_checkpoint, pending_request_id=outcome.request_id)
        return await self.executor.execute(
            [event({"approval_request_id": outcome.request_id, "approved": approved})], session, self.caps)

    async def test_failed_review_stays_pending_and_approval_retries_only_the_review(self):
        outcome = await self.run_flow(snapshots=["changed", "changed"], diffs={}, reports=[REPORT],
                                      reviews=[ReviewAborted("reviewer crashed"), PASS])
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("review pending", outcome.prompt)
        self.assertIn("reviewer crashed", outcome.prompt)
        self.assertNotIn("verdict=pass", outcome.prompt)
        resumed = await self.decide(outcome, True)
        assert isinstance(resumed, Emit), resumed
        self.assertIn("review=pass after 1 round", resumed.messages[0])
        self.assertEqual(self.role_order(), ["develop", "review", "review"])

    async def test_declined_retry_delivers_marked_as_unreviewed(self):
        outcome = await self.run_flow(snapshots=["changed"], diffs={}, reviews=[ReviewAborted("reviewer crashed")])
        assert isinstance(outcome, Suspend), outcome
        declined = await self.decide(outcome, False)
        assert isinstance(declined, Emit), declined
        self.assertIn("review=pending", declined.messages[0])
        self.assertNotIn("review=pass", declined.messages[0])
        self.assertEqual(self.role_order(), ["develop", "review"])

    async def test_rounds_exhausted_keep_the_review_pending_with_blocking_items(self):
        self.executor._options = SDEOptions(max_review_rounds=2)
        outcome = await self.run_flow(snapshots=["buggy", "still-buggy"], reviews=[CHANGES_REQUESTED] * 2,
                                      diffs={("buggy", "still-buggy"): ("+    return a // b", ["calc.py"])})
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("review pending", outcome.prompt)
        self.assertIn("changes_requested after 2 round", outcome.prompt)
        self.assertIn("calc.py:2", outcome.prompt)
        self.assertEqual(self.role_order(), ["develop", "review", "develop", "review"])


class TestSDEThroughWorker(unittest.IsolatedAsyncioTestCase):
    # The worker owns the Inbox, cards and checkpoints. Script only role replies and their repository evidence.
    def setUp(self):
        self.repo = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.inbox = SQLiteInboxAdapter(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.handler = EventHandler(inbox=self.inbox)
        self.permissions = InMemoryPermission()
        self.n = 0
        self.history = []
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.manager = OrcaWorktreeManager(workspace_store=self.workspaces, session_store=self.sessions, backend="git")
        self.enterContext(empty_project_guidance())
        self.enterContext(patch("agent_runtime.adapters.executor_cli.sde._review_delta", return_value=("fix", ["calc.py"])))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.sde.worktree_fingerprint", return_value="head:tree"))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.reviewer.repo_root", return_value=self.repo))

    def pin(self, state, selection):
        state.project = {"workdir": str(self.repo), "commit": "base", "origin": "test"}

    def set_plan(self, sde=(), reviewer=()):
        self.reports, self.reviews = iter(sde), iter(reviewer)

    async def develop(self, session_id, native, state, prompt, caps, progress):
        self.history.append({"role": "sde", "prompt": prompt})
        report = next(self.reports)["output"]
        return {**report, "code_change": True}, native or "native"

    async def review(self, request, caps, **kwargs):
        self.history.append({"role": "reviewer", "prompt": review_prompt(request, "", "+++ b/calc.py")})
        answer = next(self.reviews)
        if "error" in answer:
            raise ReviewAborted(answer["error"])
        return validate_review(answer)

    def worker(self):
        reviewer = Mock()
        reviewer.check_launch.return_value.action = "allow"
        reviewer.review = self.review
        reviewer.format_review.side_effect = lambda review: render_review(review, executor_id="coder-reviewer", settings="test")
        executor = SDEExecutor(profile=ClaudeCodeProfile(name="sde"), options=SDEOptions(), reviewer=reviewer,
                               worktree_manager=self.manager, inbox_checkout_map={}, tasks=SQLiteTaskMetadataStore(self.db))
        executor._pin = self.pin
        executor._develop = self.develop
        executor._unfinished = lambda _: ("", None)
        executor._since_review = lambda *_: SinceReview("previous")
        memory = Mock()
        memory.load_baseline.return_value = {}
        return AgentWorker(worker_id="w", inbox=self.inbox, timers=SQLiteTimerAdapter(self.db), memory=memory,
                           permissions=self.permissions, outbox=self.outbox, lock=SQLiteLeaseLockAdapter(self.db),
                           sessions=self.sessions, executors={"sde": executor}, global_default_executor="sde")

    async def send(self, payload):
        self.n += 1
        self.handler.ingest(inbox_id="proj:x", source="test", external_id=f"w{self.n}",
                            payload=payload, correlation_key="task:1")
        await self.worker().process_inbox("proj:x")

    async def task(self):
        await self.send({"executor_id": "sde", "text": "divide must reject a zero divisor",
                         "explicit_workdir": str(self.repo), "base_ref": "main"})

    async def decide(self, approved, **extra):
        (pending,) = [m for m in self.outbox.fetch_pending(100) if m.metadata.get("kind") == "approval_request"][-1:]
        await self.send({"approval_request_id": pending.metadata["request_id"], "approved": approved, **extra})

    def cards(self):
        return [m for m in self.outbox.fetch_pending(100) if m.metadata.get("kind") == "approval_request"]

    def emits(self):
        return [m.content for m in self.outbox.fetch_pending(100) if m.metadata.get("kind") == "emit"]

    def calls(self, role=None):
        return [r for r in self.history if role is None or r["role"] == role]

    async def test_follow_up_while_suspended_reaches_developer_and_reviewer_without_a_new_card(self):
        self.set_plan(sde=[dev(status="needs_input", question="Raise or return None?"), dev(FIXED)], reviewer=[PASS])
        await self.task()
        self.assertEqual(len(self.cards()), 1)
        await self.send({"text": "Raise ValueError and mention b in the message"})
        # The follow-up is recorded; the same request stays open and no duplicate card is posted.
        self.assertEqual(len(self.cards()), 1)
        self.assertEqual(len(self.calls("sde")), 1)
        session = self.sessions.find_by_correlation_key("proj:x", "task:1")
        assert session is not None
        self.assertEqual((session.status, session.pending_request_id), ("suspended", self.cards()[0].metadata["request_id"]))
        self.assertIn("mention b in the message", session.checkpoint or "")
        await self.decide(True)
        resumed = self.calls("sde")[1]
        self.assertIn("mention b in the message", resumed["prompt"])
        review = self.calls("reviewer")[0]
        self.assertIn("divide must reject a zero divisor", review["prompt"])
        self.assertIn("mention b in the message", review["prompt"])
        self.assertIn("review=pass", self.emits()[-1])

    async def test_follow_up_while_review_pending_is_developed_before_the_retry_review(self):
        self.set_plan(sde=[dev(FIXED), dev(FIXED, summary="also logs")], reviewer=[{"error": "reviewer crashed"}, PASS])
        await self.task()
        self.assertIn("review pending", self.cards()[0].content)
        await self.send({"text": "also log rejected divisions"})
        self.assertEqual(len(self.cards()), 1)
        await self.decide(True)
        self.assertEqual([c["role"] for c in self.calls()], ["sde", "reviewer", "sde", "reviewer"])
        self.assertIn("also log rejected divisions", self.calls("sde")[1]["prompt"])
        self.assertIn("also log rejected divisions", self.calls("reviewer")[1]["prompt"])
        self.assertIn("review=pass", self.emits()[-1])

    async def test_follow_up_batched_with_the_approval_reaches_developer_and_reviewer(self):
        self.set_plan(sde=[dev(status="needs_input", question="Raise or return None?"), dev(FIXED)], reviewer=[PASS])
        await self.task()
        request_id = self.cards()[0].metadata["request_id"]
        # Both arrive before the worker claims the inbox, so they run as one batch.
        self.handler.ingest(inbox_id="proj:x", source="test", external_id="b1",
                            payload={"text": "Raise ValueError naming b"}, correlation_key="task:1")
        self.handler.ingest(inbox_id="proj:x", source="test", external_id="b2",
                            payload={"approval_request_id": request_id, "approved": True})
        await self.worker().process_inbox("proj:x")
        self.assertIn("Raise ValueError naming b", self.calls("sde")[1]["prompt"])
        self.assertNotIn("approved without comment", self.calls("sde")[1]["prompt"])
        self.assertIn("Raise ValueError naming b", self.calls("reviewer")[0]["prompt"])
        self.assertIn("review=pass", self.emits()[-1])

    async def test_rejected_review_retry_lists_unprocessed_follow_ups(self):
        self.set_plan(sde=[dev(FIXED)], reviewer=[{"error": "reviewer crashed"}])
        await self.task()
        await self.send({"text": "also log rejected divisions"})
        await self.decide(False)
        text = self.emits()[-1]
        self.assertIn("review=pending", text)
        self.assertIn("Unprocessed follow-ups", text)
        self.assertIn("also log rejected divisions", text)

    async def test_deferred_launch_resumes_on_approval(self):
        self.permissions = InMemoryPermission({"sde.launch": PermissionDecision(action="defer", reason="approve sde")})
        self.set_plan(sde=[dev(BUGGY), dev(FIXED)], reviewer=[CHANGES_REQUESTED, PASS])
        await self.task()
        self.assertEqual(self.calls(), [])
        self.assertIn("approve sde", self.cards()[0].content)
        await self.decide(True)
        # Approval runs the deferred launch; the fix round asks the gate again and is deferred again.
        self.assertEqual([c["role"] for c in self.calls()], ["sde", "reviewer"])
        self.assertEqual(len(self.cards()), 2)
        self.assertIn("approve sde", self.cards()[1].content)
        await self.decide(True)
        self.assertEqual([c["role"] for c in self.calls()], ["sde", "reviewer", "sde", "reviewer"])
        self.assertIn("calc.py:2", self.calls("sde")[1]["prompt"])
        self.assertIn("review=pass after 2 round", self.emits()[-1])

    async def test_deferred_launch_rejected_stops_without_running(self):
        self.permissions = InMemoryPermission({"sde.launch": PermissionDecision(action="defer", reason="approve sde")})
        self.set_plan(sde=[dev(FIXED)])
        await self.task()
        await self.decide(False)
        self.assertEqual(self.calls(), [])
        self.assertIn("[sde] stopped: launch declined", self.emits()[-1])
        self.assertNotIn("delivered", self.emits()[-1])

    async def test_fix_round_blocked_by_the_gate_stops(self):
        class FixRoundBlocked:
            def __init__(self):
                self.seen = 0

            def check(self, tool_name, args):
                if tool_name == "sde.launch":
                    self.seen += 1
                    if self.seen > 1:
                        return PermissionDecision(action="block", reason="no more launches")
                return PermissionDecision(action="allow", reason="ok")

        self.permissions = FixRoundBlocked()
        self.set_plan(sde=[dev(BUGGY), dev(FIXED)], reviewer=[CHANGES_REQUESTED])
        await self.task()
        self.assertEqual([c["role"] for c in self.calls()], ["sde", "reviewer"])
        self.assertIn("no more launches", self.emits()[-1])
        self.assertNotIn("delivered", self.emits()[-1])

    async def test_r11_a_task_that_waits_for_the_user_to_leave_is_told_from_one_waiting_for_an_answer(self):
        # The contract gives the developer the marker; the status reads the checkpoint the real suspension saved.
        from agent_runtime.adapters.executor_cli.sde import _SDE_CONTRACT, WAIT_IDLE
        from agent_runtime.adapters.storage_sqlite.ops_status import SQLiteOpsStatus

        self.assertIn(f'question that starts with {WAIT_IDLE}', _SDE_CONTRACT)
        status = SQLiteOpsStatus(self.db, timers=tuple, sde_agents=lambda: frozenset({"sde"}), delivers=lambda _i: True)

        def waits() -> list[tuple[str, str]]:
            return [(i["waits_for"], i["why"]) for i in status.report(hours=1, timer_runs=1)["suspended"]["items"]]

        self.set_plan(sde=[dev(status="needs_input", question=f"{WAIT_IDLE} timing needs the user away")])
        await self.task()
        self.assertEqual(waits(), [("user_away", f"{WAIT_IDLE} timing needs the user away")])
        self.set_plan(sde=[dev(status="needs_input", question="Raise or return None?")])
        await self.decide(True)
        self.assertEqual(waits(), [("answer", "Raise or return None?")])

    async def test_declined_question_reports_stopped_not_delivered(self):
        self.set_plan(sde=[dev(status="needs_input", question="Raise or return None?")])
        await self.task()
        await self.decide(False)
        text = self.emits()[-1]
        self.assertIn("[sde] stopped: question declined", text)
        (last,) = [m for m in self.outbox.fetch_pending(100) if m.metadata.get("kind") == "emit"][-1:]
        self.assertEqual(last.metadata["task_context"]["status"], "stopped")
        self.assertIn("Raise or return None?", text)
        self.assertIn(str(self.repo), text)
        self.assertNotIn("delivered", text)
        self.assertNotIn("review=", text)


if __name__ == "__main__":
    unittest.main()
