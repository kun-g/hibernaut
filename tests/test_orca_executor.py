import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from agent_runtime.adapters.executor_cli.orca import run_visible
from agent_runtime.core.models import Emit, PermissionDecision, Suspend
from tests.fakes import InMemoryPermission


class TestVisibleExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.receipt = {"outcome": {"kind": "terminal", "handle": "term_test"}}
        self.command = self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new_callable=AsyncMock))

    def run_task(self, timeout=2):
        return run_visible("claude", self.root, "task", "s1", self.caps, timeout)

    async def test_terminal_receipt_is_not_the_outcome(self):
        def launch(*args, cwd):
            self.assertEqual(args[:6], ("agent", "launch", "--agent", "claude", "--worktree", f"path:{cwd}"))
            self.assertIn("terminal", args)
            self.assertNotIn("--focus", args)
            path = Path(args[-1].split("atomically rename that file to ")[1].split(". Use ")[0])
            path.write_text(json.dumps({"outcome": "emit", "messages": ["tests passed"]}))
            return self.receipt
        self.command.side_effect = launch
        self.assertEqual(await self.run_task(), Emit(messages=("tests passed",), session_id="s1"))
        self.command.assert_awaited_once()

    async def test_missing_launch_receipt_suspends_without_relaunch(self):
        self.command.return_value = {}
        self.assertIsInstance(await self.run_task(), Suspend)
        self.command.assert_awaited_once()

    async def test_timeout_closes_only_the_run_terminal(self):
        self.command.return_value = self.receipt
        self.assertIsInstance(await self.run_task(0.01), Suspend)
        self.command.assert_awaited_with("terminal", "close", "--terminal", "term_test", cwd=self.root)

    async def test_cancel_during_launch_recovers_handle_for_cleanup(self):
        started, ready = asyncio.Event(), asyncio.Event()
        async def launch(*args, cwd):
            if args[:2] == ("agent", "launch"):
                started.set()
                await ready.wait()
            return self.receipt
        self.command.side_effect = launch
        task = asyncio.create_task(self.run_task())
        await started.wait()
        task.cancel()
        ready.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.command.assert_awaited_with("terminal", "close", "--terminal", "term_test", cwd=self.root)

    async def test_permission_block_prevents_launch(self):
        self.caps.permissions = InMemoryPermission({"orca.agent.launch": PermissionDecision(action="block", reason="denied")})
        self.assertEqual(await self.run_task(), Emit(messages=("denied",), session_id="s1"))
        self.command.assert_not_awaited()
