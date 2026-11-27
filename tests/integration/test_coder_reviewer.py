import asyncio
import dataclasses
import http.client
import json
import os
import socket
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.adapters.executor_cli.claude_code import PROXY_PLACEHOLDER_KEY, ClaudeCodeLauncher, harness_args
from agent_runtime.adapters.executor_cli.project_guidance import GuidancePin
from agent_runtime.adapters.executor_cli.reviewer import (
    ReviewRequest,
    CoderReviewerExecutor,
    worktree_fingerprint,
)
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit, Event, PermissionDecision, Suspend
from tests.support.fakes import InMemoryPermission
from tests.support.coder_reviewer import (
    CHANGES_REQUESTED,
    FAKE_LAUNCHER,
    GATEWAY_ENV,
    GATEWAY_MODEL,
    ReviewerCase,
    _Capture,
    fake_command,
    git,
    launcher_env,
    make_repo,
)
from tests.support.jsonl import read_jsonl


PROXY_URL = "http://127.0.0.1:9/proxy/anthropic"


def sg_launcher(root: Path, name: str = "sg", strip: bool = True) -> ClaudeCodeLauncher:
    return ClaudeCodeLauncher(name=name, command=str(FAKE_LAUNCHER), env=launcher_env(root, name, strip))


def event(payload, correlation_key=None):
    return Event(
        inbox_id="proj:x",
        source="test",
        external_id=f"e-{time.monotonic_ns()}",
        payload=payload,
        correlation_key=correlation_key,
    )


class TestCoderReviewerExecutor(ReviewerCase):
    def setUp(self):
        super().setUp()
        self.repo = make_repo(self.root)

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

    async def test_fingerprint_sees_a_same_size_edit_whose_stat_still_matches_the_index(self):
        # Regression: the scratch index got a fresh timestamp, so git trusted a stale stat entry for a
        # same-size edit with the same mtime and two fingerprints of one working tree could disagree.
        git(self.repo, "config", "core.trustctime", "false")
        calc = self.repo / "calc.py"
        old = time.time_ns() - 60_000_000_000
        os.utime(calc, ns=(old, old))
        git(self.repo, "add", "calc.py")
        before = worktree_fingerprint(self.repo)
        calc.write_text(calc.read_text().replace("return a / b", "return b / a"))
        os.utime(calc, ns=(old, old))
        self.assertNotEqual(worktree_fingerprint(self.repo), before)

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
            {"code_reviewer.launch": PermissionDecision(action="defer", reason="approve review")}
        )
        outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        self.assertIsInstance(outcome, Suspend)
        self.assertEqual(self.calls(), [])


class TestReviewerLauncher(ReviewerCase):
    def setUp(self):
        super().setUp()
        self.repo = self.root
        request = ReviewRequest(workdir=self.repo, requirements="divide", test_evidence="pytest: 3 passed", base_ref="main",
                                pin=GuidancePin(str(self.repo), "commit", "test"))
        self.enterContext(patch.object(CoderReviewerExecutor, "_review_input", return_value=request))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.reviewer.review_inputs", return_value=("diff", "")))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.reviewer.worktree_fingerprint", return_value="head:tree"))

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
        self.assertEqual(outcome.status, "failed")
        self.assertIn("apiKeyHelper", outcome.messages[0])
        self.assertNotIn("verdict=", outcome.messages[0])

    async def test_subscription_drops_daemon_token_and_gateway(self):
        with patch.dict(os.environ, {"CLAUDE_CODE_OAUTH_TOKEN": "daemon-token", "ANTHROPIC_BASE_URL": "http://gw"}):
            outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("verdict=pass", outcome.messages[0])
        call = self.calls()[0]
        self.assertEqual((call["paid_env"], call["base_url"]), ([], None))

    async def test_configured_gateway_and_model_reach_the_harness_while_inherited_credentials_are_stripped(self):
        inherited = {"ANTHROPIC_API_KEY": "sk-daemon", "ANTHROPIC_AUTH_TOKEN": "daemon-token",
                     "ANTHROPIC_BASE_URL": "http://daemon-gw", "CLAUDE_CODE_OAUTH_TOKEN": "daemon-oauth",
                     "CLAUDE_CODE_USE_BEDROCK": "1"}
        launcher = ClaudeCodeLauncher(name="fake", command=fake_command(self.root))
        for where, profile in (
            ("executor", {"launcher": launcher, "env": GATEWAY_ENV}),
            ("launcher", {"launcher": dataclasses.replace(launcher, env=GATEWAY_ENV)}),
        ):
            with self.subTest(where), patch.dict(os.environ, inherited):
                self.log.unlink(missing_ok=True)
                outcome = await self.executor(model=GATEWAY_MODEL, **profile).execute(
                    [event(self.payload())], None, self.caps
                )
                assert isinstance(outcome, Emit)
                self.assertIn("verdict=pass", outcome.messages[0])
                (call,) = self.calls()
                self.assertEqual((call["base_url"], call["auth_token"]), ("http://127.0.0.1:3425", "magpie"))
                self.assertEqual(call["paid_env"], ["ANTHROPIC_AUTH_TOKEN"])
                self.assertEqual(call["argv"][call["argv"].index("--model") + 1], GATEWAY_MODEL)

    async def test_subscription_accepts_only_the_credential_source_written_in_the_config(self):
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-daemon"}):
            outcome = await self.executor(env={**GATEWAY_ENV, "ANTHROPIC_API_KEY": "configured"}).execute(
                [event(self.payload())], None, self.caps
            )
        assert isinstance(outcome, Emit)
        self.assertIn("verdict=pass", outcome.messages[0])
        self.assertEqual(self.calls()[0]["api_key"], "configured")
        # A source the config did not name still stops the session.
        for source in ("ANTHROPIC_API_KEY", "apiKeyHelper"):
            with self.subTest(source), patch.dict(os.environ, {"FAKE_API_KEY_SOURCE": source}):
                outcome = await self.executor(env=GATEWAY_ENV).execute([event(self.payload())], None, self.caps)
                assert isinstance(outcome, Emit)
                self.assertIn("review failed: billing is 'subscription'", outcome.messages[0])

    async def test_api_billing_goes_through_the_credential_proxy_without_the_real_key(self):
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-real-secret", "ANTHROPIC_AUTH_TOKEN": "real-token"}):
            outcome = await self.executor(billing="api", credential_proxy_url=PROXY_URL).execute(
                [event(self.payload())], None, self.caps
            )
        assert isinstance(outcome, Emit)
        self.assertIn("billing=api", outcome.messages[0])
        call = self.calls()[-1]
        self.assertEqual((call["api_key"], call["base_url"]), (PROXY_PLACEHOLDER_KEY, PROXY_URL))
        self.assertEqual(call["paid_env"], ["ANTHROPIC_API_KEY"])

    async def test_api_billing_stops_when_the_launcher_drops_the_proxy_route(self):
        launcher = sg_launcher(self.root)
        outcome = await self.executor(billing="api", credential_proxy_url=PROXY_URL, launcher=launcher).execute(
            [event(self.payload())], None, self.caps
        )
        assert isinstance(outcome, Emit)
        self.assertIn("billing is 'api' but Claude Code (launcher 'sg') authenticated with 'none'", outcome.messages[0])

    async def test_launcher_receives_harness_args_unchanged_and_stays_read_only(self):
        launcher = sg_launcher(self.root)
        executor = self.executor(launcher=launcher, model="claude-sonnet-5-5", effort="max")
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-real-secret"}):
            outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("launcher=sg billing=subscription model=claude-sonnet-5-5 effort=max", outcome.messages[0])
        (call,) = self.calls()
        role_args = call["argv"][call["argv"].index("--permission-mode") + 2:]
        self.assertEqual(call["argv"], harness_args(executor.profile, permission_mode="dontAsk", role_args=role_args))
        self.assertNotIn("bypassPermissions", call["argv"])
        self.assertEqual((call["launcher"], call["config_dir"]), ("sg", str(self.root / "sg-config")))
        self.assertEqual(call["paid_env"], [])

    async def test_model_mismatch_is_reported(self):
        with patch.dict(os.environ, {"FAKE_MODEL": "claude-haiku-5-5"}):
            outcome = await self.executor().execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        self.assertIn("configured model 'claude-opus-5-5' but Claude Code selected 'claude-haiku-5-5'", outcome.messages[0])


class TestDispatchAndHotReload(unittest.TestCase):
    def test_executor_id_routes_to_reviewer_and_reload_changes_the_next_run(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        repo = make_repo(root)
        log = root / "calls.jsonl"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        db_path = root / "hb.db"
        path = root / "hibernaut.toml"

        def launcher_table(name):
            env = ", ".join(f'{k} = "{v}"' for k, v in launcher_env(root, name).items())
            return f'[launchers.{name}]\ncommand = "{FAKE_LAUNCHER}"\nenv = {{ {env} }}\n'

        def write_config(launcher, model, effort):
            path.write_text(
                f'[runtime]\ndb_path = "{db_path}"\nmemory_root = "{root / "mem"}"\ndefault_executor = "claude"\n'
                f"[channels]\nport = {port}\n"
                + launcher_table("sg-a") + launcher_table("sg-b")
                + '[executors.claude]\ncommand = "false"\n'
                f'[executors.coder-reviewer]\nrole = "coder-reviewer"\nlauncher = "{launcher}"\ninteractive = false\n'
                f'model = "{model}"\neffort = "{effort}"\n'
            )

        write_config("sg-a", "claude-opus-5-5", "high")
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(log)}))
        stderr = self.enterContext(patch("sys.stderr", new_callable=_Capture))

        def post(n, **extra):
            body = {
                "inbox_id": "review", "external_id": f"r{n}", "correlation_key": f"review:{n}",
                "payload": {"executor_id": "coder-reviewer", "text": "divide works",
                            "explicit_workdir": str(repo), "base_ref": "main", **extra},
            }
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
                self.assertEqual(conn.getresponse().status, 202)
            finally:
                conn.close()

        def calls():
            return read_jsonl(log)

        def outbox():
            with sqlite3.connect(db_path) as conn:
                return [row[0] for row in conn.execute("SELECT content FROM outbox ORDER BY seq")]

        def settings(call):
            argv = call["argv"]
            # The launcher must not inject its bypassPermissions default over the reviewer's read-only mode.
            self.assertEqual(argv.count("--permission-mode"), 1)
            self.assertNotIn("bypassPermissions", argv)
            return (call["launcher"], argv[argv.index("--model") + 1], argv[argv.index("--effort") + 1],
                    argv[argv.index("--permission-mode") + 1])

        async def scenario():
            from agent_runtime.app.config import load_config

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
                self.fail(stderr.getvalue())

            async def post_async(n, **extra):
                await asyncio.to_thread(post, n, **extra)

            try:
                await wait_for(lambda: post(0) is None)
                await wait_for(lambda: len(calls()) == 1)
                self.assertEqual(settings(calls()[0]), ("sg-a", "claude-opus-5-5", "high", "dontAsk"))
                await wait_for(lambda: any("verdict=pass" in m and "launcher=sg-a" in m for m in outbox()))

                write_config("sg-b", "claude-sonnet-5-5", "max")
                await wait_for(lambda: "[config] Reloaded" in stderr.getvalue())
                await post_async(1)
                await wait_for(lambda: len(calls()) == 2)
                self.assertEqual(settings(calls()[1]), ("sg-b", "claude-sonnet-5-5", "max", "dontAsk"))
                self.assertEqual(calls()[1]["config_dir"], str(root / "sg-b-config"))

                write_config("sg-b", "claude-sonnet-5-5", "ultra")
                await wait_for(lambda: "unsupported effort 'ultra'" in stderr.getvalue())
                await post_async(2)
                await wait_for(lambda: len(calls()) == 3)
                self.assertEqual(settings(calls()[2]), ("sg-b", "claude-sonnet-5-5", "max", "dontAsk"))

                # A reload naming an undeclared launcher is rejected; events cannot pick one either.
                write_config("ghost", "claude-sonnet-5-5", "max")
                await wait_for(lambda: "unknown launcher 'ghost'" in stderr.getvalue())
                await post_async(3, launcher="ghost", command="/bin/sh")
                await wait_for(lambda: len(calls()) == 4)
                self.assertEqual(settings(calls()[3]), ("sg-b", "claude-sonnet-5-5", "max", "dontAsk"))
            finally:
                stop.set()
                await server

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
