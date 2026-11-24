import asyncio
import atexit
import functools
import http.client
import json
import os
import shutil
import socket
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.base_merge import SinceReview
from agent_runtime.adapters.executor_cli.project_guidance import ProjectGuidance
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor, ReviewAborted, render_review, review_prompt, validate_review
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
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit, Event, PermissionDecision, SessionRef, Suspend
from agent_runtime.engine import AgentWorker, EventHandler
from tests.fakes import InMemoryPermission
from tests.test_coder_reviewer import (
    CHANGES_REQUESTED,
    FAKE_CLAUDE,
    FAKE_LAUNCHER,
    _Capture,
    fake_command,
    git,
    launcher_env,
)

PROXY = "http://127.0.0.1:9"
PASS = {"verdict": "pass", "summary": "zero is rejected", "blocking": []}
BUGGY = {"calc.py": "def divide(a, b):\n    return a / b\n"}
FIXED = {"calc.py": "def divide(a, b):\n    if b == 0:\n        raise ValueError('b')\n    return a / b\n"}
EVIDENCE = "red: test_zero FAILED (ZeroDivisionError)\ngreen: 4 passed; lint-imports ok; pyright 0 errors"


@functools.lru_cache(maxsize=1)
def _template_base_repo() -> Path:
    tmp = tempfile.TemporaryDirectory(prefix="sde_base_tmpl_")
    atexit.register(tmp.cleanup)
    repo = Path(tmp.name) / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "t@example.invalid")
    git(repo, "config", "user.name", "t")
    (repo / "calc.py").write_text("def divide(a, b):\n    return a\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "base")
    return repo


def make_base_repo(root: Path) -> Path:
    repo = root / "repo"
    shutil.copytree(_template_base_repo(), repo)
    return repo


def dev(write=None, **output):
    out = {"status": "done", "summary": "divide implemented", "test_evidence": EVIDENCE, "change_kind": "fix", "change_rationale": "divide handled zero wrongly"}
    out.update(output)
    return {"write": write or {}, "output": out}


def event(payload, correlation_key="task:1", inbox_id="proj:x"):
    return Event(
        inbox_id=inbox_id,
        source="test",
        external_id=f"e-{time.monotonic_ns()}",
        payload=payload,
        correlation_key=correlation_key,
    )


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


class TestSDEExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.repo = make_base_repo(self.root)
        git(self.repo, "checkout", "-qb", "hb/task-1")
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan)}))
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.wt = OrcaWorktreeManager(
            workspace_store=self.workspaces, session_store=SQLiteSessionStoreAdapter(self.db), backend="git"
        )
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.caps.memory.load_baseline.return_value = {}

    def set_plan(self, sde=(), reviewer=()):
        self.plan.write_text(json.dumps({"sde": list(sde), "reviewer": list(reviewer)}))

    def launcher(self):
        return ClaudeCodeLauncher(name="fake", command=fake_command(self.root))

    def executor(self, review=True, max_review_rounds=3, **overrides):
        overrides.setdefault("launcher", self.launcher())
        reviewer = CoderReviewerExecutor(
            profile=ClaudeCodeProfile(name="coder-reviewer", launcher=self.launcher(), model="claude-sonnet-5-5"),
            workspaces=self.workspaces, tasks=SQLiteTaskMetadataStore(self.db),
        )
        return SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", **overrides),
            options=SDEOptions(review=review, max_review_rounds=max_review_rounds),
            reviewer=reviewer if review else None,
            worktree_manager=self.wt,
            inbox_checkout_map={},
            tasks=SQLiteTaskMetadataStore(self.db),
        )

    def calls(self, role=None):
        rows = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return [r for r in rows if role is None or r["role"] == role]

    def payload(self, **extra):
        return {"executor_id": "sde", "text": "divide must reject a zero divisor",
                "explicit_workdir": str(self.repo), "base_ref": "main", **extra}

    @staticmethod
    def arg(call, flag):
        return call["argv"][call["argv"].index(flag) + 1]

    async def test_review_on_sends_requirements_actual_diff_and_evidence_then_delivers_on_pass(self):
        self.set_plan(sde=[dev(FIXED)], reviewer=[PASS])
        outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        text = outcome.messages[0]
        for part in ("[sde] delivered", "review=pass after 1 round", "divide implemented", EVIDENCE,
                     "model=claude-opus-5-5 effort=high", "billing=subscription"):
            self.assertIn(part, text)
        self.assertEqual([c["role"] for c in self.calls()], ["sde", "reviewer"])
        dev_call, review_call = self.calls()
        # Developer: write tools under an explicit mode; reviewer: its own read-only settings.
        self.assertEqual(self.arg(dev_call, "--permission-mode"), "dontAsk")
        self.assertIn("Edit", self.arg(dev_call, "--allowedTools"))
        disallowed = self.arg(dev_call, "--disallowedTools").split(",")
        for denied in ("Bash(git push --force*)", "Bash(git push * --force*)", "Bash(git push -f)",
                       "Bash(git push -f *)", "Bash(git push * -f)", "Bash(git push * -f *)", "Bash(git push * +*)"):
            self.assertIn(denied, disallowed)
        for allowed_cmd in ("Bash(git push:*)", "Bash(gh pr create:*)", "Bash(gh pr merge:*)"):
            self.assertNotIn(allowed_cmd, disallowed)
        self.assertEqual((self.arg(dev_call, "--model"), self.arg(dev_call, "--effort")), ("claude-opus-5-5", "high"))
        dev_system = self.arg(dev_call, "--append-system-prompt")
        self.assertIn("failing test", dev_system)
        self.assertIn("open a pull request", dev_system)
        self.assertIn("--delete-branch", dev_system)
        self.assertIn("Never force-push", dev_system)
        self.assertIn("divide must reject a zero divisor", dev_call["prompt"])
        self.assertIn("Edit", self.arg(review_call, "--disallowedTools"))
        self.assertEqual(self.arg(review_call, "--model"), "claude-sonnet-5-5")
        for evidence in ("divide must reject a zero divisor", EVIDENCE, "+    if b == 0:"):
            self.assertIn(evidence, review_call["prompt"])
        self.assertTrue((outcome.executor_session_id or "").startswith("fake-sde-"))

    async def test_changes_requested_sends_blocking_items_back_and_rereviews_the_fix(self):
        self.set_plan(sde=[dev(BUGGY), dev(FIXED, summary="zero divisor rejected")], reviewer=[CHANGES_REQUESTED, PASS])
        outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("review=pass after 2 round", outcome.messages[0])
        self.assertIn("zero divisor rejected", outcome.messages[0])
        roles = [c["role"] for c in self.calls()]
        self.assertEqual(roles, ["sde", "reviewer", "sde", "reviewer"])
        first_dev, _, fix_dev, rereview = self.calls()
        # The fix continues the same developer session and receives every blocking field.
        self.assertNotIn("--resume", first_dev["argv"])
        self.assertEqual(self.arg(fix_dev, "--resume"), outcome.executor_session_id)
        for part in ("calc.py:2", "divide(1, 0)", "ZeroDivisionError crashes the caller", "reject zero divisors"):
            self.assertIn(part, fix_dev["prompt"])
        # The re-review is a fresh reviewer session that sees the previous findings and the fix diff.
        self.assertNotIn("--resume", rereview["argv"])
        self.assertIn("Previous review round", rereview["prompt"])
        self.assertIn("calc.py:2", rereview["prompt"])
        self.assertIn("Changes since the previous review", rereview["prompt"])
        self.assertIn("+    if b == 0:", rereview["prompt"])

    async def test_review_off_delivers_after_tests_without_a_reviewer(self):
        self.set_plan(sde=[dev(FIXED)])
        outcome = await self.executor(review=False).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("review=off", outcome.messages[0])
        self.assertIn(EVIDENCE, outcome.messages[0])
        self.assertEqual([c["role"] for c in self.calls()], ["sde"])

    async def test_failed_review_stays_pending_and_approval_retries_only_the_review(self):
        self.set_plan(sde=[dev(FIXED)], reviewer=[{"error": "reviewer crashed"}, PASS])
        executor = self.executor()
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("review pending", outcome.prompt)
        self.assertIn("reviewer crashed", outcome.prompt)
        self.assertNotIn("verdict=pass", outcome.prompt)
        session = SessionRef(session_id=outcome.session_id or "s", inbox_id="proj:x", correlation_key="task:1",
                             executor_id="sde", executor_session_id=outcome.executor_session_id,
                             status="suspended", checkpoint=outcome.session_checkpoint,
                             pending_request_id=outcome.request_id)
        approve = event({"approval_request_id": outcome.request_id, "approved": True})
        resumed = await executor.execute([approve], session, self.caps)
        assert isinstance(resumed, Emit), resumed
        self.assertIn("review=pass after 1 round", resumed.messages[0])
        self.assertEqual([c["role"] for c in self.calls()], ["sde", "reviewer", "reviewer"])

    async def test_declined_retry_delivers_marked_as_unreviewed(self):
        self.set_plan(sde=[dev(FIXED)], reviewer=[{"error": "reviewer crashed"}])
        executor = self.executor()
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        session = SessionRef(session_id="s", inbox_id="proj:x", executor_id="sde", status="suspended",
                             checkpoint=outcome.session_checkpoint, pending_request_id=outcome.request_id)
        declined = await executor.execute(
            [event({"approval_request_id": outcome.request_id, "approved": False})], session, self.caps
        )
        assert isinstance(declined, Emit), declined
        self.assertIn("review=pending", declined.messages[0])
        self.assertNotIn("review=pass", declined.messages[0])
        self.assertEqual(len(self.calls("reviewer")), 1)

    async def test_rounds_exhausted_keep_the_review_pending_with_blocking_items(self):
        self.set_plan(sde=[dev(BUGGY), dev({"calc.py": "def divide(a, b):\n    return a // b\n"})],
                      reviewer=[CHANGES_REQUESTED])
        outcome = await self.executor(max_review_rounds=2).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("review pending", outcome.prompt)
        self.assertIn("changes_requested after 2 round", outcome.prompt)
        self.assertIn("calc.py:2", outcome.prompt)
        self.assertEqual([c["role"] for c in self.calls()], ["sde", "reviewer", "sde", "reviewer"])

    async def test_needs_input_suspends_and_the_answer_resumes_the_developer_session(self):
        self.set_plan(sde=[dev(status="needs_input", question="Raise or return None?"), dev(FIXED)], reviewer=[PASS])
        executor = self.executor()
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("Raise or return None?", outcome.prompt)
        session = SessionRef(session_id="s", inbox_id="proj:x", executor_id="sde", status="suspended",
                             executor_session_id=outcome.executor_session_id,
                             checkpoint=outcome.session_checkpoint, pending_request_id=outcome.request_id)
        answer = event({"approval_request_id": outcome.request_id, "approved": True, "comment": "Raise ValueError"})
        resumed = await executor.execute([answer], session, self.caps)
        assert isinstance(resumed, Emit), resumed
        dev_resume = self.calls("sde")[1]
        self.assertEqual(self.arg(dev_resume, "--resume"), outcome.executor_session_id)
        self.assertIn("Raise ValueError", dev_resume["prompt"])
        # The reviewer still gets the original requirements, not just the answer.
        self.assertIn("divide must reject a zero divisor", self.calls("reviewer")[0]["prompt"])

    async def test_permission_gate_runs_before_launch(self):
        self.set_plan(sde=[dev(FIXED)])
        self.caps.permissions = InMemoryPermission({"sde.launch": PermissionDecision(action="defer", reason="approve sde")})
        outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        self.assertIsInstance(outcome, Suspend)
        self.assertEqual(self.calls(), [])

    async def test_subscription_stops_on_paid_credentials(self):
        # A wrong init must fail even if the CLI immediately offers a usable reply. Leave no Git child to reap.
        self.set_plan(sde=[dev(status="needs_input", question="which divisor?")])
        with patch.dict(os.environ, {"FAKE_API_KEY_SOURCE": "apiKeyHelper"}):
            outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("development failed: billing is 'subscription'", outcome.messages[0])
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(self.calls("reviewer"), [])

    async def test_api_billing_goes_through_the_credential_proxy(self):
        self.set_plan(sde=[dev(FIXED)])
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-real-secret"}):
            outcome = await self.executor(review=False, billing="api", credential_proxy_url=PROXY).execute(
                [event(self.payload())], None, self.caps
            )
        assert isinstance(outcome, Emit), outcome
        call = self.calls("sde")[0]
        self.assertEqual(call["base_url"], PROXY)
        self.assertNotEqual(call["api_key"], "sk-real-secret")

    async def test_configured_gateway_and_model_reach_the_harness(self):
        self.set_plan(sde=[dev(FIXED)])
        env = {"ANTHROPIC_BASE_URL": "http://127.0.0.1:3425", "ANTHROPIC_AUTH_TOKEN": "magpie"}
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-daemon", "ANTHROPIC_BASE_URL": "http://daemon-gw"}):
            outcome = await self.executor(review=False, model="codex/gpt-6.2-astra", env=env).execute(
                [event(self.payload())], None, self.caps
            )
        assert isinstance(outcome, Emit), outcome
        call = self.calls("sde")[0]
        self.assertEqual((call["base_url"], call["auth_token"]), ("http://127.0.0.1:3425", "magpie"))
        self.assertEqual(call["paid_env"], ["ANTHROPIC_AUTH_TOKEN"])
        self.assertEqual(call["argv"][call["argv"].index("--model") + 1], "codex/gpt-6.2-astra")


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
        self.enterContext(patch("agent_runtime.adapters.executor_cli.sde.load_guidance", side_effect=lambda pin: ProjectGuidance(pin, "", (), False)))
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


class TestSDEDispatchAndHotReload(unittest.TestCase):
    def test_events_route_to_sde_through_review_and_reload_changes_the_next_run(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        repo = make_base_repo(root)
        log = root / "calls.jsonl"
        plan = root / "plan.json"
        plan.write_text(json.dumps({"sde": [dev(BUGGY), dev(FIXED)], "reviewer": [CHANGES_REQUESTED, PASS]}))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        db_path = root / "hb.db"
        path = root / "hibernaut.toml"

        def launcher_table(name, strip):
            env = ", ".join(f'{k} = "{v}"' for k, v in launcher_env(root, name, strip).items())
            return f'[launchers.{name}]\ncommand = "{FAKE_LAUNCHER}"\nenv = {{ {env} }}\n'

        def write_config(launcher, billing, model, effort, review, harness="claude-code"):
            path.write_text(
                f'[runtime]\ndb_path = "{db_path}"\nmemory_root = "{root / "mem"}"\ndefault_executor = "sde"\n'
                f"[channels]\nport = {port}\n"
                f'[credential_proxy]\nurl = "{PROXY}"\n'
                + launcher_table("sg-a", True) + launcher_table("sg-b", False)
                + f'[projects.demo]\ninbox_id = "proj:demo"\nrepo = "o/demo"\ncheckout = "{repo}"\n'
                'worktree_backend = "git"\n'
                f'[executors.coder-reviewer]\nrole = "coder-reviewer"\nlauncher = "sg-a"\ninteractive = false\n'
                f'[executors.sde]\nrole = "sde"\nharness = "{harness}"\nlauncher = "{launcher}"\ninteractive = false\n'
                f'billing = "{billing}"\nmodel = "{model}"\neffort = "{effort}"\nreview = {str(review).lower()}\n'
            )

        write_config("sg-a", "subscription", "claude-opus-5-5", "high", True)
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(log), "FAKE_PLAN": str(plan)}))
        stderr = self.enterContext(patch("sys.stderr", new_callable=_Capture))

        def post(n):
            body = {
                "inbox_id": "proj:demo", "external_id": f"t{n}", "correlation_key": f"task-{n}",
                "payload": {"executor_id": "sde", "text": f"task {n}: divide must reject zero", "base_ref": "main"},
            }
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
                self.assertEqual(conn.getresponse().status, 202)
            finally:
                conn.close()

        def calls(role=None):
            rows = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return [r for r in rows if role is None or r["role"] == role]

        def outbox():
            with sqlite3.connect(db_path) as conn:
                return [row[0] for row in conn.execute("SELECT content FROM outbox ORDER BY seq")]

        def settings(call):
            argv = call["argv"]
            self.assertEqual(argv.count("--permission-mode"), 1)
            self.assertNotIn("bypassPermissions", argv)
            return (call["launcher"], argv[argv.index("--model") + 1], argv[argv.index("--effort") + 1],
                    "api" if call["base_url"] == f"{PROXY}/proxy/anthropic" else "subscription")

        async def scenario():
            config = load_config(path)
            daemon = build_daemon_from_config(config)
            stop = asyncio.Event()
            server = asyncio.create_task(serve_configured(path, config, daemon, stop, reload_interval=0.02))

            async def wait_for(check):
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    self.assertFalse(server.done(), stderr.getvalue())
                    try:
                        if check():
                            return
                    except (ConnectionRefusedError, OSError):
                        pass
                    await asyncio.sleep(0.01)
                self.fail(f"timed out; calls={[c['role'] for c in calls()]} outbox={outbox()} {stderr.getvalue()}")

            try:
                await wait_for(lambda: post(0) is None)
                await wait_for(lambda: any("[sde] delivered" in m for m in outbox()))
                self.assertEqual([c["role"] for c in calls()], ["sde", "reviewer", "sde", "reviewer"])
                workdir = Path(calls()[0]["cwd"]).resolve()
                self.assertNotEqual(workdir, repo.resolve())
                self.assertTrue(all(Path(c["cwd"]).resolve() == workdir for c in calls()))
                self.assertEqual(settings(calls("sde")[0]), ("sg-a", "claude-opus-5-5", "high", "subscription"))
                delivered = [m for m in outbox() if "[sde] delivered" in m][0]
                self.assertIn("review=pass after 2 round", delivered)
                self.assertIn("round 1 changes_requested (1 blocking); round 2 pass", delivered)

                write_config("sg-b", "api", "claude-sonnet-5-5", "max", False)
                await wait_for(lambda: "[config] Reloaded" in stderr.getvalue())
                await asyncio.to_thread(post, 1)
                await wait_for(lambda: len([m for m in outbox() if "[sde] delivered" in m]) == 2)
                self.assertEqual(len(calls("reviewer")), 2)
                self.assertEqual(settings(calls("sde")[-1]), ("sg-b", "claude-sonnet-5-5", "max", "api"))
                self.assertIn("review=off", outbox()[-1])

                write_config("sg-b", "api", "claude-sonnet-5-5", "max", True, harness="codex")
                await wait_for(lambda: "unsupported harness 'codex'" in stderr.getvalue())
                await asyncio.to_thread(post, 2)
                await wait_for(lambda: len([m for m in outbox() if "[sde] delivered" in m]) == 3)
                self.assertEqual(len(calls("reviewer")), 2)
                self.assertEqual(settings(calls("sde")[-1]), ("sg-b", "claude-sonnet-5-5", "max", "api"))
            finally:
                stop.set()
                await server

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
