import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal, session_gate
from agent_runtime.core.models import Emit, Event
from tests.support.coder_reviewer import GATEWAY_ENV, GATEWAY_MODEL, ReviewerCase, fake_command, make_repo
from tests.support.visible_roles import FakeOrcaTerminals

KEYCHAIN_KEY = "sk-ant-api03-from-the-keychain"


def event(payload: dict[str, str]) -> Event:
    return Event(inbox_id="proj:x", source="test", external_id="e-1", payload=payload)


def overriding_launcher(root: Path, name: str = "keychain") -> ClaudeCodeLauncher:
    # A launcher that exports a real key over the configured placeholder, as one reading the keychain would.
    script = root / f"{name}.sh"
    script.write_text(f'#!/bin/sh\nexport ANTHROPIC_AUTH_TOKEN="{KEYCHAIN_KEY}"\nexec {fake_command(root)} "$@"\n')
    script.chmod(0o700)
    return ClaudeCodeLauncher(name=name, command=str(script))


class TestHeadlessRun(ReviewerCase):
    def setUp(self):
        super().setUp()
        self.repo = make_repo(self.root)
        self.enterContext(patch.dict(os.environ, {"FAKE_API_KEY_SOURCE": "ANTHROPIC_AUTH_TOKEN"}))

    async def review(self, launcher: ClaudeCodeLauncher) -> str:
        outcome = await self.executor(launcher=launcher, env=GATEWAY_ENV, model=GATEWAY_MODEL).execute(
            [event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit)
        return outcome.messages[0]

    async def test_the_configured_gateway_token_passes(self):
        text = await self.review(ClaudeCodeLauncher(name="fake", command=fake_command(self.root)))
        self.assertIn("verdict=pass", text)

    async def test_a_launcher_that_replaces_the_configured_token_is_stopped(self):
        text = await self.review(overriding_launcher(self.root))
        self.assertIn("review failed: billing is 'subscription'", text)
        self.assertIn("ANTHROPIC_AUTH_TOKEN", text)
        self.assertNotIn(KEYCHAIN_KEY, text)
        self.assertNotIn("verdict=", text)


class TestSessionGate(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.enterContext(patch.dict(os.environ, {"FAKE_API_KEY_SOURCE": "ANTHROPIC_AUTH_TOKEN"}))
        self.terminal = OrcaCommandTerminal(root=self.root / "runs", poll_interval=0.02, liveness_interval=0.2)

    async def asyncTearDown(self):
        await self.orca.close_all()

    async def run_checked(self, launcher: ClaudeCodeLauncher):
        profile = ClaudeCodeProfile(name="steward", env=GATEWAY_ENV, model=GATEWAY_MODEL, launcher=launcher)
        return await self.terminal.run(title="t", workdir=self.root, argv=["/bin/sh", "-c", f"touch {self.root}/ran"],
                                       env={}, timeout_seconds=20, is_interrupted=lambda: False,
                                       gate=session_gate(profile))

    async def test_the_configured_gateway_token_passes(self):
        run = await self.run_checked(ClaudeCodeLauncher(name="fake", command=fake_command(self.root)))
        self.assertEqual(run.exit_code, 0, run.stderr)
        self.assertTrue((self.root / "ran").exists())

    async def test_a_launcher_that_replaces_the_configured_token_never_starts_the_command(self):
        run = await self.run_checked(overriding_launcher(self.root))
        self.assertNotEqual(run.exit_code, 0)
        self.assertIn(b"ANTHROPIC_AUTH_TOKEN", run.stderr)
        self.assertIn(b"not set to the configured", run.stderr)
        self.assertNotIn(KEYCHAIN_KEY.encode(), run.stderr)
        self.assertFalse((self.root / "ran").exists())


if __name__ == "__main__":
    unittest.main()
