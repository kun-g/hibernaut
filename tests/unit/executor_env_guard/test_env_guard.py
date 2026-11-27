import io
import tempfile
import tomllib
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from typing import Any
from unittest.mock import patch

from agent_runtime.app.config import parse_config
from agent_runtime.app.main import main

# The two gateway executors running on 2026-11-28, by shape: a local gateway and its 6-character placeholder token.
LIVE = """
[executors.gpt-reviewer]
role = "code-reviewer"
billing = "subscription"
model = "codex/gpt-6.2-astra"
[executors.gpt-reviewer.env]
ANTHROPIC_BASE_URL = "http://127.0.0.1:3425"
ANTHROPIC_AUTH_TOKEN = "magpie"

[executors.model-gateway]
harness = "claude-code"
billing = "subscription"
[executors.model-gateway.env]
ANTHROPIC_BASE_URL = "http://127.0.0.1:3425"
ANTHROPIC_AUTH_TOKEN = "magpie"

[agents.gpt-sde]
role = "sde"
executor = "model-gateway"
model = "codex/gpt-6.2-astra"
review = false
"""
GATEWAY = {"ANTHROPIC_BASE_URL": "http://127.0.0.1:3425", "ANTHROPIC_AUTH_TOKEN": "magpie"}
REAL_VALUES = (
    "sk-ant-api03-abcdef", "ghp_abcdef", "gho_abcdef", "ghs_abcdef", "xoxb-1-2-abc", "github_pat_11AB",
    "a" * 25,
)


def config(env: dict[str, str], *, launcher_env: dict[str, str] | None = None) -> dict[str, Any]:
    return {
        "launchers": {"sg": {"command": "claude-sg", "env": launcher_env or {}}},
        "executors": {"r": {"role": "code-reviewer", "launcher": "sg", "env": env}},
    }


class TestCredentialKeys(unittest.TestCase):
    def test_the_live_gateway_executors_load_unchanged(self):
        cfg = parse_config(tomllib.loads(LIVE))
        self.assertEqual(cfg.roles["gpt-reviewer"].profile.env, GATEWAY)
        self.assertEqual(cfg.roles["gpt-sde"].profile.env, GATEWAY)
        for env in (
            GATEWAY,
            {"ANTHROPIC_BASE_URL": "http://localhost:3425/", "ANTHROPIC_AUTH_TOKEN": "magpie"},
            {"ANTHROPIC_BASE_URL": "http://[::1]:3425", "ANTHROPIC_API_KEY": "local-gateway-dummy"},
        ):
            with self.subTest(env):
                self.assertEqual(parse_config(config(env)).roles["r"].profile.env, env)
        # The gateway and its token may sit in either section.
        profile = parse_config(config({"ANTHROPIC_AUTH_TOKEN": "magpie"},
                                      launcher_env={"ANTHROPIC_BASE_URL": "http://127.0.0.1:3425"})).roles["r"].profile
        self.assertEqual(profile.launcher.env, {"ANTHROPIC_BASE_URL": "http://127.0.0.1:3425"})

    def test_a_credential_key_needs_a_gateway_on_this_machine(self):
        for env, launcher_env, where, key in (
            ({"GH_TOKEN": "x"}, {}, "executors.r.env", "GH_TOKEN"),
            ({"OPENAI_API_KEY": "x"}, {}, "executors.r.env", "OPENAI_API_KEY"),
            ({"ANTHROPIC_AUTH_TOKEN": "magpie", "ANTHROPIC_BASE_URL": "https://gw.example.com"}, {},
             "executors.r.env", "ANTHROPIC_AUTH_TOKEN"),
            ({}, {"ANTHROPIC_API_KEY": "x"}, "launchers.sg.env", "ANTHROPIC_API_KEY"),
            ({"DB_PASSWORD": "x", "ANTHROPIC_BASE_URL": "http://10.0.0.5:3425"}, {}, "executors.r.env", "DB_PASSWORD"),
        ):
            with self.subTest(key), self.assertRaisesRegex(ValueError, f"{where}.{key} .*loopback") as raised:
                parse_config(config(env, launcher_env=launcher_env))
            self.assertNotIn("gw.example.com", str(raised.exception))

    def test_a_value_that_looks_like_a_real_secret_is_refused_without_printing_it(self):
        for value in REAL_VALUES:
            for env, launcher_env, where in (
                ({**GATEWAY, "ANTHROPIC_AUTH_TOKEN": value}, {}, "executors.r.env"),
                ({}, {"GH_TOKEN": value}, "launchers.sg.env"),
            ):
                key = next(k for k, v in {**env, **launcher_env}.items() if v == value)
                with self.subTest(value=value, where=where), \
                        self.assertRaisesRegex(ValueError, f"{where}.{key} looks like a real") as raised:
                    # A launcher no executor uses is still checked.
                    parse_config({"launchers": {"sg": {"command": "x", "env": launcher_env}},
                                  "executors": {"r": {"role": "code-reviewer", "env": env}}})
                self.assertNotIn(value, str(raised.exception))

    def test_a_url_with_a_password_is_refused_without_printing_it(self):
        for env in ({"ANTHROPIC_BASE_URL": "http://gw:s3cret-pw@127.0.0.1:3425"},
                    {"HTTPS_PROXY": "http://me:s3cret-pw@127.0.0.1:7890"}):
            with self.subTest(env), self.assertRaisesRegex(ValueError, "executors.r.env.[A-Z_]+ carries a password") as raised:
                parse_config(config(env))
            self.assertNotIn("s3cret-pw", str(raised.exception))

    def test_settings_named_after_tokens_are_not_credentials(self):
        env = {"MAX_THINKING_TOKENS": "32000", "CLAUDE_CODE_MAX_OUTPUT_TOKENS": "64000", "HTTPS_PROXY": "http://127.0.0.1:1"}
        self.assertEqual(parse_config(config(env)).roles["r"].profile.env, env)

    def test_subscription_refuses_a_remote_gateway_without_its_own_token(self):
        for url in ("https://gw.example.com", "http://192.168.1.4:3425", "http://127.0.0.1.example.com"):
            with self.subTest(url), self.assertRaisesRegex(
                ValueError, "executors.r: .*ANTHROPIC_BASE_URL is not a loopback address.*OAuth"
            ):
                parse_config(config({"ANTHROPIC_BASE_URL": url}))

    def test_check_refuses_the_same_configs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "hibernaut.toml"
            for body, expected in (
                ('[executors.r]\nrole = "code-reviewer"\n[executors.r.env]\nGH_TOKEN = "ghp_realtoken"\n',
                 "executors.r.env.GH_TOKEN"),
                ('[executors.r]\nrole = "code-reviewer"\n[executors.r.env]\nANTHROPIC_BASE_URL = "https://gw.example.com"\n',
                 "not a loopback address"),
            ):
                path.write_text(body, encoding="utf-8")
                err = io.StringIO()
                with self.subTest(expected), patch("sys.argv", ["hibernaut", "--check", "--config", str(path)]), \
                        redirect_stderr(err), redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as stop:
                    main()
                self.assertEqual(stop.exception.code, 1)
                self.assertIn(expected, err.getvalue())
                self.assertNotIn("ghp_realtoken", err.getvalue())
            path.write_text(LIVE, encoding="utf-8")
            out = io.StringIO()
            with patch("sys.argv", ["hibernaut", "--check", "--config", str(path)]), redirect_stdout(out):
                main()
            self.assertIn("OK", out.getvalue())


if __name__ == "__main__":
    unittest.main()
