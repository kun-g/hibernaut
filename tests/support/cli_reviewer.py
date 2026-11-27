import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import CLIReviewer, CLITemplate
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal
from agent_runtime.adapters.executor_cli.project_guidance import GuidancePin
from agent_runtime.adapters.executor_cli.reviewer import ReviewRequest
from tests.support.fakes import InMemoryPermission
from tests.support.visible_roles import FakeOrcaTerminals
from tests.support.jsonl import read_jsonl


FAKE_CLI = Path(__file__).with_name("fake_cli_reviewer.py")
PASS = {"verdict": "pass", "summary": "zero is rejected", "blocking": []}
EVIDENCE = "red: test_zero FAILED\ngreen: 4 passed"


def cli_command(root: Path) -> str:
    script = root / "review-cli"
    script.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{FAKE_CLI}" "$@"\n')
    script.chmod(0o700)
    return str(script)


class CLIPlan:
    def __init__(self, root: Path) -> None:
        self.plan = root / "cli-plan.json"
        self.log = root / "cli-calls.jsonl"

    def env(self) -> dict[str, str]:
        return {"FAKE_CLI_PLAN": str(self.plan), "FAKE_CLI_LOG": str(self.log)}

    def set(self, *steps):
        self.plan.write_text(json.dumps([s if isinstance(s, dict) else {"stdout": s} for s in steps]))

    def calls(self):
        return read_jsonl(self.log)


_RUNNER = object()
ORCA_LOG = "agent_runtime.adapters.executor_cli.orca"


class TerminalCase(unittest.IsolatedAsyncioTestCase):
    # The CLI reviewer with terminal = "orca": FakeOrcaTerminals runs each terminal's command in a real shell, so the
    # wrapper script and the fake CLI really run, and their output lands in the terminal's screen file.
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.review_inputs", return_value=("diff", "")))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.cli_reviewer.worktree_fingerprint", return_value="head:tree"))
        self.cli = CLIPlan(self.root)
        self.enterContext(patch.dict(os.environ, self.cli.env()))
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.runs = self.root / "cli-runs"
        self.terminal = OrcaCommandTerminal(root=self.runs, poll_interval=0.02, liveness_interval=0.2)
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False

    async def asyncTearDown(self):
        await self.orca.close_all()

    def reviewer(self, runner=_RUNNER, **overrides):
        fields = {"name": "grok", "command": cli_command(self.root), "args": ("-p",), "terminal": "orca", **overrides}
        return CLIReviewer(template=CLITemplate(**fields), terminal=self.terminal if runner is _RUNNER else runner)

    def request(self, label="review of task-1 round 1"):
        return ReviewRequest(workdir=self.repo, requirements="divide must reject a zero divisor",
                             test_evidence=EVIDENCE, base_ref="main", pin=GuidancePin(str(self.repo), "head", "test"), label=label)

    def handles(self):
        return list(self.orca.terminals)

    def alive(self, handle):
        return self.orca.terminals[handle]["proc"].returncode is None

    async def still_running(self, handle):
        # The fake cannot close a terminal whose command just ended; either way it ends at once.
        try:
            await asyncio.wait_for(self.orca.terminals[handle]["proc"].wait(), 5)
        except TimeoutError:
            return True
        return False

    def closes(self):
        return [c[3] for c in self.orca.calls if c[:2] == ("terminal", "close")]
