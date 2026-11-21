import asyncio
import http.client
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli.reviewer import (
    CoderReviewerExecutor,
    ReviewerProfile,
    worktree_fingerprint,
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit, Event, PermissionDecision, Suspend
from tests.fakes import InMemoryPermission

FAKE_CLAUDE = Path(__file__).with_name("fake_claude.py")
CHANGES_REQUESTED = {
    "verdict": "changes_requested",
    "summary": "division is unguarded",
    "blocking": [
        {
            "location": "calc.py:2",
            "trigger": "divide(1, 0)",
            "impact": "ZeroDivisionError crashes the caller",
            "fix": "reject zero divisors",
        }
    ],
    "notes": ["name the helper more clearly"],
}


def git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


def make_repo(root: Path) -> Path:
    repo = root / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "t@example.invalid")
    git(repo, "config", "user.name", "t")
    (repo / "calc.py").write_text("def divide(a, b):\n    return a\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "base")
    git(repo, "checkout", "-qb", "feature")
    (repo / "calc.py").write_text("def divide(a, b):\n    return a / b\n")
    git(repo, "commit", "-qam", "divide")
    (repo / "notes.txt").write_text("untracked evidence\n")
    return repo


def fake_command(root: Path) -> str:
    script = root / "claude"
    script.write_text(f"#!/bin/sh\nexec {sys.executable} {FAKE_CLAUDE} \"$@\"\n")
    script.chmod(0o700)
    return str(script)


def event(payload, correlation_key=None):
    return Event(
        inbox_id="proj:x",
        source="test",
        external_id=f"e-{time.monotonic_ns()}",
        payload=payload,
        correlation_key=correlation_key,
    )


class TestReviewerProfile(unittest.TestCase):
    def test_role_only_uses_documented_defaults(self):
        cfg = parse_config({"executors": {"coder-reviewer": {"role": "coder-reviewer"}}})
        profile = cfg.reviewers["coder-reviewer"]
        self.assertEqual(
            (profile.harness, profile.billing, profile.model, profile.effort),
            ("claude-code", "subscription", "claude-opus-5-5", "high"),
        )
        self.assertNotIn("coder-reviewer", cfg.executors)

    def test_invalid_values_and_combinations_fail_with_a_reason(self):
        cases = {
            "unsupported harness 'cursor-agent'": {"harness": "cursor-agent"},
            "unsupported billing 'free'": {"billing": "free"},
            "unsupported effort 'ultra'": {"effort": "ultra"},
            "model 'gpt-5' is not a Claude model": {"model": "gpt-5"},
            "env.ANTHROPIC_API_KEY looks like a credential": {"env": {"ANTHROPIC_API_KEY": "x"}},
            "env.CLAUDE_CODE_USE_BEDROCK looks like a credential": {"env": {"CLAUDE_CODE_USE_BEDROCK": "1"}},
            "args, orca_agent not supported": {"args": ["-p"], "orca_agent": "claude"},
        }
        for message, override in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                parse_config({"executors": {"coder-reviewer": {"role": "coder-reviewer", **override}}})
        with self.assertRaisesRegex(ValueError, "unsupported role 'sde'"):
            parse_config({"executors": {"sde": {"role": "sde"}}})

    def test_reviewer_alone_is_enough_to_leave_ingest_only(self):
        cfg = parse_config({
            "runtime": {"db_path": ":memory:", "default_executor": "coder-reviewer"},
            "executors": {"coder-reviewer": {"role": "coder-reviewer"}},
        })
        daemon = build_daemon_from_config(cfg)
        try:
            self.assertFalse(daemon.ingest_only)
        finally:
            daemon.close()


class TestCoderReviewerExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.repo = make_repo(self.root)
        self.log = self.root / "calls.jsonl"
        self.env = self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log)}))
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False

    def executor(self, **overrides):
        profile = ReviewerProfile(name="coder-reviewer", command=fake_command(self.root), **overrides)
        return CoderReviewerExecutor(profile=profile, workspaces=self.workspaces)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def payload(self, **extra):
        return {
            "executor_id": "coder-reviewer",
            "text": "divide must reject zero",
            "test_evidence": "pytest: 3 passed",
            "explicit_workdir": str(self.repo),
            "base_ref": "main",
            **extra,
        }

    async def test_pass_review_sends_all_three_inputs_with_configured_settings(self):
        outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        self.assertIsInstance(outcome, Emit)
        assert isinstance(outcome, Emit)
        self.assertIn("verdict=pass", outcome.messages[0])
        self.assertIn("model=claude-opus-5-5 effort=high", outcome.messages[0])
        (call,) = self.calls()
        argv = call["argv"]
        self.assertEqual(argv[argv.index("--model") + 1], "claude-opus-5-5")
        self.assertEqual(argv[argv.index("--effort") + 1], "high")
        self.assertEqual(argv[argv.index("--permission-mode") + 1], "dontAsk")
        self.assertIn("Edit", argv[argv.index("--disallowedTools") + 1])
        self.assertEqual(Path(call["cwd"]).resolve(), self.repo.resolve())
        for evidence in ("divide must reject zero", "pytest: 3 passed", "+    return a / b", "notes.txt"):
            self.assertIn(evidence, call["prompt"])

    async def test_changes_requested_lists_location_trigger_and_impact(self):
        with patch.dict(os.environ, {"FAKE_REVIEW": json.dumps(CHANGES_REQUESTED)}):
            outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        text = outcome.messages[0]
        for part in ("verdict=changes_requested", "1. calc.py:2", "Trigger: divide(1, 0)",
                     "Impact: ZeroDivisionError", "Fix: reject zero divisors", "submit again with executor_id=coder-reviewer"):
            self.assertIn(part, text)

    async def test_subscription_strips_api_credentials_from_the_harness(self):
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "placeholder", "CLAUDE_CODE_USE_BEDROCK": "1"}):
            outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("verdict=pass", outcome.messages[0])
        self.assertEqual(self.calls()[0]["paid_env"], [])

    async def test_subscription_stops_when_harness_reports_a_paid_credential(self):
        with patch.dict(os.environ, {"FAKE_API_KEY_SOURCE": "apiKeyHelper", "FAKE_REVIEW": json.dumps(CHANGES_REQUESTED)}):
            outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("review failed: billing is 'subscription'", outcome.messages[0])
        self.assertIn("apiKeyHelper", outcome.messages[0])
        self.assertNotIn("verdict=", outcome.messages[0])

    async def test_api_billing_requires_a_key_in_the_daemon_environment(self):
        env = {k: v for k, v in os.environ.items() if k not in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")}
        with patch.dict(os.environ, env, clear=True):
            outcome = await self.executor(billing="api").execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("billing 'api' needs ANTHROPIC_API_KEY", outcome.messages[0])
        self.assertEqual(self.calls(), [])
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "placeholder"}):
            outcome = await self.executor(billing="api").execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("billing=api", outcome.messages[0])
        self.assertEqual(self.calls()[-1]["paid_env"], ["ANTHROPIC_API_KEY"])

    async def test_model_mismatch_is_reported(self):
        with patch.dict(os.environ, {"FAKE_MODEL": "claude-haiku-5-5"}):
            outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("configured model 'claude-opus-5-5' but Claude Code selected 'claude-haiku-5-5'", outcome.messages[0])

    async def test_review_that_changes_the_worktree_is_discarded(self):
        before = worktree_fingerprint(self.repo)
        with patch.dict(os.environ, {"FAKE_TOUCH": str(self.repo / "calc.py")}):
            outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("working tree", outcome.messages[0])
        self.assertIn("findings discarded", outcome.messages[0])
        self.assertNotEqual(worktree_fingerprint(self.repo), before)

    async def test_fingerprint_tracks_untracked_edits_and_leaves_the_real_index_alone(self):
        def staged():
            return subprocess.run(["git", "diff", "--cached", "--name-only"], cwd=self.repo,
                                  capture_output=True, text=True, check=True).stdout

        before = worktree_fingerprint(self.repo)
        self.assertEqual(worktree_fingerprint(self.repo), before)
        self.assertEqual(staged(), "")
        (self.repo / "notes.txt").write_text("changed\n")
        self.assertNotEqual(worktree_fingerprint(self.repo), before)
        self.assertEqual(staged(), "")

    async def test_review_of_finds_the_developer_workspace_and_never_creates_one(self):
        outcome = await self.executor().execute(
            [event({"executor_id": "coder-reviewer", "text": "t", "review_of": "dev:1", "base_ref": "main"},
                   correlation_key="review:dev:1")],
            None, self.caps,
        )
        assert isinstance(outcome, Emit)
        self.assertIn("no ready workspace for 'dev:1'", outcome.messages[0])
        self.workspaces.reserve_creating(work_key="dev:1", repo_checkout=str(self.repo), workdir=str(self.repo), branch="feature")
        self.workspaces.mark_ready("dev:1", workdir=str(self.repo), branch="feature")
        outcome = await self.executor().execute(
            [event({"executor_id": "coder-reviewer", "text": "t", "review_of": "dev:1", "base_ref": "main"})],
            None, self.caps,
        )
        assert isinstance(outcome, Emit)
        self.assertIn("verdict=pass", outcome.messages[0])

    async def test_missing_requirements_or_changes_are_rejected_before_launch(self):
        outcome = await self.executor().execute([event(self.payload(text=""))], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("task requirements", outcome.messages[0])
        outcome = await self.executor().execute([event(self.payload(base_ref="missing"))], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("cannot find a merge base with missing", outcome.messages[0])
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-qm", "notes")
        outcome = await self.executor().execute([event(self.payload(base_ref="feature"))], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("no changes between feature", outcome.messages[0])
        self.assertEqual(self.calls(), [])

    async def test_contradictory_verdict_is_rejected(self):
        bad = {"verdict": "pass", "summary": "s", "blocking": CHANGES_REQUESTED["blocking"]}
        with patch.dict(os.environ, {"FAKE_REVIEW": json.dumps(bad)}):
            outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("contradicts 1 blocking item", outcome.messages[0])

    async def test_permission_gate_runs_before_launch(self):
        self.caps.permissions = InMemoryPermission(
            {"coder_reviewer.launch": PermissionDecision(action="defer", reason="approve review")}
        )
        outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        self.assertIsInstance(outcome, Suspend)
        self.assertEqual(self.calls(), [])


class TestDispatchAndHotReload(unittest.TestCase):
    def test_executor_id_routes_to_reviewer_and_reload_changes_the_next_run(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        repo = make_repo(root)
        log = root / "calls.jsonl"
        command = fake_command(root)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        db_path = root / "hb.db"
        path = root / "hibernaut.toml"

        def write_config(model, effort):
            path.write_text(
                f'[runtime]\ndb_path = "{db_path}"\nmemory_root = "{root / "mem"}"\ndefault_executor = "claude"\n'
                f"[channels]\nport = {port}\n"
                '[executors.claude]\ncommand = "false"\n'
                f'[executors.coder-reviewer]\nrole = "coder-reviewer"\ncommand = "{command}"\n'
                f'model = "{model}"\neffort = "{effort}"\n'
            )

        write_config("claude-opus-5-5", "high")
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(log)}))
        stderr = self.enterContext(patch("sys.stderr", new_callable=_Capture))

        def post(n):
            body = {
                "inbox_id": "review", "external_id": f"r{n}", "correlation_key": f"review:{n}",
                "payload": {"executor_id": "coder-reviewer", "text": "divide works",
                            "explicit_workdir": str(repo), "base_ref": "main"},
            }
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
                self.assertEqual(conn.getresponse().status, 202)
            finally:
                conn.close()

        def calls():
            return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

        def outbox():
            with sqlite3.connect(db_path) as conn:
                return [row[0] for row in conn.execute("SELECT content FROM outbox ORDER BY seq")]

        def settings(call):
            argv = call["argv"]
            return argv[argv.index("--model") + 1], argv[argv.index("--effort") + 1]

        async def scenario():
            from agent_runtime.app.config import load_config

            config = load_config(path)
            daemon = build_daemon_from_config(config)
            stop = asyncio.Event()
            server = asyncio.create_task(serve_configured(path, config, daemon, stop))

            async def wait_for(check):
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    self.assertFalse(server.done(), stderr.getvalue())
                    try:
                        if check():
                            return
                    except (ConnectionRefusedError, OSError):
                        pass
                    await asyncio.sleep(0.05)
                self.fail(stderr.getvalue())

            async def post_async(n):
                await asyncio.to_thread(post, n)

            try:
                await wait_for(lambda: post(0) is None)
                await wait_for(lambda: len(calls()) == 1)
                self.assertEqual(settings(calls()[0]), ("claude-opus-5-5", "high"))
                await wait_for(lambda: any("verdict=pass" in m for m in outbox()))

                write_config("claude-sonnet-5-5", "max")
                await wait_for(lambda: "[config] Reloaded" in stderr.getvalue())
                await post_async(1)
                await wait_for(lambda: len(calls()) == 2)
                self.assertEqual(settings(calls()[1]), ("claude-sonnet-5-5", "max"))

                write_config("claude-sonnet-5-5", "ultra")
                await wait_for(lambda: "unsupported effort 'ultra'" in stderr.getvalue())
                await post_async(2)
                await wait_for(lambda: len(calls()) == 3)
                self.assertEqual(settings(calls()[2]), ("claude-sonnet-5-5", "max"))
            finally:
                stop.set()
                await server

        asyncio.run(scenario())


class _Capture:
    def __init__(self):
        self._parts = []

    def write(self, text):
        self._parts.append(text)
        return len(text)

    def flush(self):
        pass

    def getvalue(self):
        return "".join(self._parts)


if __name__ == "__main__":
    unittest.main()
