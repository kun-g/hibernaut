import asyncio
import atexit
import dataclasses
import functools
import http.client
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import tomllib
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli.claude_code import (
    PROXY_PLACEHOLDER_KEY,
    ClaudeCodeLauncher,
    ClaudeCodeProfile,
    child_env,
    harness_args,
)
from agent_runtime.adapters.executor_cli.reviewer import (
    CoderReviewerExecutor,
    worktree_fingerprint,
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit, Event, PermissionDecision, Suspend
from tests.fakes import InMemoryPermission

FAKE_CLAUDE = Path(__file__).with_name("fake_claude.py")
FAKE_LAUNCHER = Path(__file__).with_name("fake_launcher.sh")
PROXY_URL = "http://127.0.0.1:9/proxy/anthropic"
GATEWAY_ENV = {"ANTHROPIC_BASE_URL": "http://127.0.0.1:3425", "ANTHROPIC_AUTH_TOKEN": "magpie"}
GATEWAY_MODEL = "codex/gpt-6.2-astra"
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


@functools.lru_cache(maxsize=1)
def _template_repo() -> Path:
    tmp = tempfile.TemporaryDirectory(prefix="coder_reviewer_tmpl_")
    atexit.register(tmp.cleanup)
    repo = Path(tmp.name) / "repo"
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


def make_repo(root: Path) -> Path:
    repo = root / "repo"
    shutil.copytree(_template_repo(), repo)
    return repo


def fake_command(root: Path) -> str:
    script = root / "claude"
    script.write_text(f"#!/bin/sh\nexec {sys.executable} {FAKE_CLAUDE} \"$@\"\n")
    script.chmod(0o700)
    return str(script)


def launcher_env(root: Path, name: str, strip: bool = True) -> dict[str, str]:
    return {
        "FAKE_LAUNCHER_NAME": name,
        "FAKE_LAUNCHER_ROOT": str(root),
        "FAKE_LAUNCHER_STRIP": "1" if strip else "0",
        "FAKE_PYTHON": sys.executable,
        "FAKE_CLAUDE": str(FAKE_CLAUDE),
    }


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


class TestReviewerProfile(unittest.TestCase):
    def test_role_only_uses_documented_defaults(self):
        cfg = parse_config({"executors": {"coder-reviewer": {"role": "coder-reviewer"}}})
        role = cfg.roles["coder-reviewer"]
        profile = role.profile
        # The old role name is the same role under its current name.
        self.assertEqual(role.role, "code-reviewer")
        self.assertEqual(
            (profile.harness, profile.launcher.name, profile.launcher.command, profile.billing, profile.model, profile.effort),
            ("claude-code", "claude", "claude", "subscription", "claude-opus-5-5", "high"),
        )
        self.assertNotIn("coder-reviewer", cfg.executors)

    def test_role_harness_launcher_billing_and_model_are_separate_settings(self):
        cfg = parse_config({
            "credential_proxy": {"url": "http://127.0.0.1:9000/"},
            "launchers": {"sg": {"command": "claude-sg", "env": {"HTTPS_PROXY": "http://127.0.0.1:1"}}},
            "executors": {"r": {"role": "coder-reviewer", "launcher": "sg", "billing": "api",
                                "model": "sonnet", "effort": "max"}},
        })
        profile = cfg.roles["r"].profile
        self.assertEqual((profile.launcher.command, profile.launcher.env), ("claude-sg", {"HTTPS_PROXY": "http://127.0.0.1:1"}))
        self.assertEqual((profile.billing, profile.model, profile.effort), ("api", "sonnet", "max"))
        self.assertEqual(profile.credential_proxy_url, "http://127.0.0.1:9000/proxy/anthropic")

    def test_invalid_values_and_combinations_fail_with_a_reason(self):
        cases = {
            "unsupported harness 'cursor-agent'": {"harness": "cursor-agent"},
            "unsupported billing 'free'": {"billing": "free"},
            "unsupported effort 'ultra'": {"effort": "ultra"},
            "executors.coder-reviewer: model must be a non-empty string": {"model": ""},
            "coder-reviewer: model must be a non-empty string": {"model": "  "},
            "model must be a non-empty string": {"model": 5},
            "args, orca_agent not supported": {"args": ["-p"], "orca_agent": "claude"},
            "command not supported": {"command": "/bin/claude"},
            "unknown launcher 'nope'": {"launcher": "nope"},
            "billing 'api' needs \\[credential_proxy\\] url": {"billing": "api"},
        }
        for message, override in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                parse_config({"executors": {"coder-reviewer": {"role": "coder-reviewer", **override}}})
        with self.assertRaisesRegex(ValueError, "unsupported role 'qa'"):
            parse_config({"executors": {"qa": {"role": "qa"}}})
        launcher_cases = {
            "launchers.sg: unsupported harness 'codex'": {"command": "x", "harness": "codex"},
            "launchers.sg: command is required": {},
            "launchers.sg: model not supported": {"command": "x", "model": "opus"},
        }
        for message, launcher in launcher_cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                parse_config({"launchers": {"sg": launcher}})
        with self.assertRaisesRegex(ValueError, "credential_proxy.url must be an http"):
            parse_config({"credential_proxy": {"url": "file:///tmp/x"}})

    def test_gateway_env_and_non_claude_model_load_as_written(self):
        cfg = parse_config(tomllib.loads("""
            [executors.gpt-reviewer]
            role = "code-reviewer"
            billing = "subscription"
            model = "codex/gpt-6.2-astra"
            [executors.gpt-reviewer.env]
            ANTHROPIC_BASE_URL = "http://127.0.0.1:3425"
            ANTHROPIC_AUTH_TOKEN = "magpie"
        """))
        profile = cfg.roles["gpt-reviewer"].profile
        self.assertEqual((profile.billing, profile.model, profile.env), ("subscription", GATEWAY_MODEL, GATEWAY_ENV))
        example = Path(__file__).resolve().parents[1] / "hibernaut.example.toml"
        documented = parse_config(tomllib.loads(example.read_text(encoding="utf-8"))).roles["gpt-reviewer"].profile
        self.assertEqual((documented.billing, documented.model, documented.env), ("subscription", GATEWAY_MODEL, GATEWAY_ENV))

    def test_env_key_names_are_not_restricted(self):
        env = {"ANTHROPIC_API_KEY": "k", "CLAUDE_CODE_USE_BEDROCK": "1", "CLAUDE_CODE_OAUTH_TOKEN": "t",
               "MY_PASSWORD": "p", "SOME_SECRET": "s", **GATEWAY_ENV}
        cfg = parse_config({
            "launchers": {"sg": {"command": "claude-sg", "env": env}},
            "executors": {"r": {"role": "code-reviewer", "launcher": "sg", "env": env}},
        })
        self.assertEqual((cfg.roles["r"].profile.launcher.env, cfg.roles["r"].profile.env), (env, env))

    def test_api_billing_conflicts_with_a_route_written_in_env(self):
        proxy = {"url": "http://127.0.0.1:9000"}
        for key in ("ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN",
                    "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY"):
            for where, role, launcher in (
                ("executors.r", {"env": {key: "v-secret"}}, {}),
                ("launchers.sg", {}, {"env": {key: "v-secret"}}),
            ):
                message = f"executors.r: billing 'api' .*conflicts with {where}.env.{key}"
                with self.subTest(message), self.assertRaisesRegex(ValueError, message) as raised:
                    parse_config({
                        "credential_proxy": proxy,
                        "launchers": {"sg": {"command": "claude-sg", **launcher}},
                        "executors": {"r": {"role": "code-reviewer", "launcher": "sg", "billing": "api", **role}},
                    })
                self.assertNotIn("v-secret", str(raised.exception))
        # Anything that does not choose the route still passes through.
        cfg = parse_config({
            "credential_proxy": proxy,
            "executors": {"r": {"role": "code-reviewer", "billing": "api", "env": {"MY_TOKEN": "t"}}},
        })
        self.assertEqual(cfg.roles["r"].profile.env, {"MY_TOKEN": "t"})

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
        overrides.setdefault("launcher", ClaudeCodeLauncher(name="fake", command=fake_command(self.root)))
        profile = ClaudeCodeProfile(name="coder-reviewer", **overrides)
        return CoderReviewerExecutor(profile=profile, workspaces=self.workspaces,
                                     tasks=SQLiteTaskMetadataStore(self.db))

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
            outcome = await self.executor(env={"ANTHROPIC_API_KEY": "configured"}).execute(
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

    def test_child_env_layers_launcher_then_role_env(self):
        launcher = ClaudeCodeLauncher(name="l", command="x", env={"A": "launcher", "B": "launcher"})
        profile = ClaudeCodeProfile(name="r", launcher=launcher, env={"B": "role"})
        env = child_env(profile, {"PATH": "/bin", "ANTHROPIC_API_KEY": "sk"})
        self.assertEqual(env, {"PATH": "/bin", "A": "launcher", "B": "role"})

    def test_child_env_sets_configured_keys_after_stripping_inherited_ones(self):
        launcher = ClaudeCodeLauncher(name="l", command="x", env={"ANTHROPIC_BASE_URL": "http://l", "CLAUDE_CODE_OAUTH_TOKEN": "l"})
        profile = ClaudeCodeProfile(name="r", launcher=launcher, env=GATEWAY_ENV)
        env = child_env(profile, {"ANTHROPIC_API_KEY": "sk", "ANTHROPIC_AUTH_TOKEN": "d", "ANTHROPIC_BASE_URL": "http://d",
                                  "CLAUDE_CODE_OAUTH_TOKEN": "d", "CLAUDE_CODE_USE_VERTEX": "1"})
        self.assertEqual(env, {**GATEWAY_ENV, "CLAUDE_CODE_OAUTH_TOKEN": "l"})

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
            return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

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
                deadline = time.monotonic() + 15
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
