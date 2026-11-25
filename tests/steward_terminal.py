import asyncio
import os
from pathlib import Path
from unittest.mock import patch

from agent_runtime.adapters.executor_cli.cli_terminal import one_shot_env
from agent_runtime.adapters.executor_cli.orca import TerminalRun
from agent_runtime.adapters.executor_cli.process import supervise


class _StewardTerminal:
    def __init__(self, launcher: Path):
        self.launcher = launcher

    async def run(self, *, argv, workdir, env, stdin, gate, timeout_seconds, is_interrupted, title):
        # The routing suites exercise the configured CLI and its MCP subprocess in the selected checkout.
        # Terminal creation, billing probes and process-group cleanup have dedicated real-shell tests.
        assert Path(argv[0]) == self.launcher
        child_env = {k: v for k, v in {**os.environ, **env}.items() if k not in gate["unset"]}
        proc = await asyncio.create_subprocess_exec(
            *argv, cwd=workdir, env=one_shot_env({**child_env, **gate["set"]}), stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await supervise(proc, proc.communicate(stdin.encode()), is_interrupted=is_interrupted,
                                         timeout_seconds=timeout_seconds, poll_interval=0.05)
        return TerminalRun(handle="steward-test", stdout=stdout, stderr=stderr, exit_code=proc.returncode)


def steward_cli_terminal(root: Path):
    return patch("agent_runtime.app.main.OrcaCommandTerminal", return_value=_StewardTerminal(root / "claude"))
