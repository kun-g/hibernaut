import json
import unittest
from typing import Any

from agent_runtime.adapters.executor_cli.claude_code import (
    ClaudeCodeError,
    ClaudeCodeLauncher,
    ClaudeCodeProfile,
    check_init,
    env_check,
    env_fingerprint,
)

GATEWAY = {"ANTHROPIC_BASE_URL": "http://127.0.0.1:3425", "ANTHROPIC_AUTH_TOKEN": "magpie"}


def init(source: str) -> dict[str, Any]:
    return {"type": "system", "subtype": "init", "apiKeySource": source, "model": "codex/gpt-6.2-astra"}


def report(salt: str, values: dict[str, str | None]) -> dict[str, Any]:
    body = {k: None if v is None else env_fingerprint(salt, v) for k, v in values.items()}
    return {"type": "system", "subtype": "hook_response", "hook_event": "SessionStart",
            "stdout": "", "stderr": f"hibernaut-env-check {json.dumps(body)}", "exit_code": 0}


class TestEffectiveCredential(unittest.TestCase):
    def setUp(self):
        self.profile = ClaudeCodeProfile(name="gw", model="codex/gpt-6.2-astra", env=GATEWAY,
                                         launcher=ClaudeCodeLauncher(name="sg", command="claude-sg"))
        check = env_check(self.profile)
        assert check is not None
        self.check = check

    def test_only_the_configured_routing_and_credential_variables_are_checked(self):
        self.assertEqual(self.check.keys, ("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL"))
        self.assertIsNone(env_check(ClaudeCodeProfile(name="plain", env={"HTTPS_PROXY": "http://127.0.0.1:1"})))
        self.assertNotEqual(env_check(self.profile).salt, self.check.salt)  # pyright: ignore[reportOptionalMemberAccess]

    def test_the_configured_values_pass(self):
        check_init(self.profile, init("ANTHROPIC_AUTH_TOKEN"), check=self.check, hooks=[report(self.check.salt, GATEWAY)])
        check_init(self.profile, init("none"), check=self.check, hooks=[report(self.check.salt, GATEWAY)])

    def test_a_value_the_launcher_replaced_stops_the_session_without_printing_it(self):
        for values in ({**GATEWAY, "ANTHROPIC_AUTH_TOKEN": "sk-ant-from-keychain"},
                       {**GATEWAY, "ANTHROPIC_BASE_URL": "https://api.anthropic.com"},
                       {**GATEWAY, "ANTHROPIC_AUTH_TOKEN": None}):
            changed = next(k for k in values if values[k] != GATEWAY[k])
            with self.subTest(changed), self.assertRaisesRegex(ClaudeCodeError, f"{changed}.*not set to the configured") as raised:
                check_init(self.profile, init("ANTHROPIC_AUTH_TOKEN"), check=self.check,
                           hooks=[report(self.check.salt, values)])
            for value in (*GATEWAY.values(), "sk-ant-from-keychain", self.check.salt):
                self.assertNotIn(value, str(raised.exception))

    def test_a_session_that_did_not_report_its_values_is_stopped(self):
        for hooks in ([], [report("other-salt", GATEWAY)]):
            with self.subTest(len(hooks)), self.assertRaisesRegex(ClaudeCodeError, "could not confirm|not set to the configured"):
                check_init(self.profile, init("ANTHROPIC_AUTH_TOKEN"), check=self.check, hooks=hooks)
        with self.assertRaisesRegex(ClaudeCodeError, "billing is 'subscription'"):
            check_init(self.profile, init("ANTHROPIC_AUTH_TOKEN"))


if __name__ == "__main__":
    unittest.main()
