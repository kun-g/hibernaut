import asyncio
import http.client
import json
import os
import socket
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import load_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit, PermissionDecision, SessionRef, Suspend
from tests.support.empty_guidance import empty_project_guidance
from tests.support.fakes import InMemoryPermission
from tests.support.coder_reviewer import CHANGES_REQUESTED, FAKE_LAUNCHER, _Capture, fake_command, git, launcher_env
from tests.support.sde_executor import BUGGY, EVIDENCE, FIXED, PASS, PROXY, dev, event, make_base_repo
from tests.support.jsonl import read_jsonl


class TestSDEExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(empty_project_guidance())
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
        rows = read_jsonl(self.log)
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
            rows = read_jsonl(log)
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
