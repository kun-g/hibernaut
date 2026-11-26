import tomllib
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile, child_env
from agent_runtime.adapters.executor_cli.project_guidance import GuidancePin
from agent_runtime.adapters.executor_cli.reviewer import ReviewRequest, CoderReviewerExecutor
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from tests.support.coder_reviewer import GATEWAY_ENV, GATEWAY_MODEL, ReviewerCase


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
        example = Path(__file__).resolve().parents[2] / "hibernaut.example.toml"
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


class TestReviewerLauncher(ReviewerCase):
    def setUp(self):
        super().setUp()
        self.repo = self.root
        request = ReviewRequest(workdir=self.repo, requirements="divide", test_evidence="pytest: 3 passed", base_ref="main",
                                pin=GuidancePin(str(self.repo), "commit", "test"))
        self.enterContext(patch.object(CoderReviewerExecutor, "_review_input", return_value=request))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.reviewer.review_inputs", return_value=("diff", "")))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.reviewer.worktree_fingerprint", return_value="head:tree"))

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


if __name__ == "__main__":
    unittest.main()
