from __future__ import annotations

import asyncio
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.app.config import parse_config
from tests.support.fakes import RecordingChannel
from tests.support.coder_reviewer import fake_command
from tests.support.visible_roles import build_terminal_daemon


MAIN = "proj:x"
SDE = "proj:x:sde"


def arg(call: dict[str, Any], flag: str) -> str:
    return call["argv"][call["argv"].index(flag) + 1]


def git_state(repo: Path) -> tuple[str, str, str]:
    def out(*args: str) -> str:
        return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=True).stdout

    return out("status", "--porcelain"), out("rev-parse", "HEAD"), out("worktree", "list")


CONFIG = """
[runtime]
db_path = "{root}/x.db"
memory_root = "{root}/memory"
default_agent = "steward"
worktree_sweep_interval_seconds = 0

[launchers.fake]
command = "{command}"

[executors.claude-code]
launcher = "fake"

[agents.steward]
role = "steward"
executor = "claude-code"
model = "claude-sonnet-5-5"

[agents.sde]
role = "sde"
executor = "claude-code"
review = false
interactive = false
description = "develops tasks"

[projects.x]
repo = "acme/x"
checkout = "{repo}"
worktree_backend = "git"
terminal_backend = "orca"
inbox_id = ["proj:x", "proj:x:.+"]
default_agent = "steward"

[[projects.x.inbox_bindings]]
pattern = "proj:x"
default_agent = "steward"
agents = ["steward"]

[[projects.x.inbox_bindings]]
pattern = "proj:x:sde(:.*)?"
default_agent = "sde"
agents = ["sde"]
"""


class DaemonCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.repo = self.root / "checkout"
        self.channel = RecordingChannel()

    def raw(self) -> dict[str, Any]:
        return tomllib.loads(CONFIG.format(root=self.root, repo=self.repo, command=fake_command(self.root)))

    async def run_daemon(self, *events: tuple[str, str | None, dict[str, Any]]) -> Any:
        daemon = build_terminal_daemon(parse_config(self.raw()), self.root, channel=self.channel)
        self.addCleanup(daemon.db.close)
        stop = asyncio.Event()
        daemon.pool.bind(asyncio.get_running_loop())
        pool = asyncio.create_task(daemon.pool.run_forever(stop))
        for index, (inbox_id, key, payload) in enumerate(events):
            daemon.handler.ingest(inbox_id=inbox_id, source="switchboard:feishu", external_id=f"e{index}",
                                  payload=payload, correlation_key=key)
        await daemon.pool.wait_until_idle(60)
        stop.set()
        await pool
        return daemon
