from __future__ import annotations
import asyncio
import sys
import tempfile
import unittest
from pathlib import Path
from agent_runtime.adapters.executor_cli import CLITemplate, CommandResult, LocalCLIExecutor, OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteWorkspaceStore
from agent_runtime.core.models import Emit, Event, Silent, Suspend
from tests.support.fakes import InMemoryMemoryStore, InMemoryPermission, InMemoryTimerStore


class _FakeCaps:
    def __init__(self, *, interrupted: bool = False) -> None:
        self.memory = InMemoryMemoryStore({"instructions.md": "be quiet"})
        self.timers = InMemoryTimerStore()
        self.permissions = InMemoryPermission()
        self._interrupted = interrupted

    def poll_folded_events(self) -> list[Event]:
        return []

    def is_interrupted(self) -> bool:
        return self._interrupted


class TestLocalCLIExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="cli_exec_")
        self.root = Path(self._tmp.name)
        self.db = SQLiteDatabase(":memory:")
        self.store = SQLiteWorkspaceStore(self.db)
        self.mgr = OrcaWorktreeManager(
            workspace_store=self.store,
            backend="git",
            runner=lambda _c, _d: CommandResult(0, "", ""),
        )

    def tearDown(self) -> None:
        self.db.close()
        self._tmp.cleanup()

    async def test_run_target_comes_from_ordinary_events_not_the_approval(self) -> None:
        def tpl(name: str) -> CLITemplate:
            code = f"import os; print('{name}:' + os.path.basename(os.getcwd()))"
            return CLITemplate(name=name, command=sys.executable, args=("-I", "-c", code))

        executor = LocalCLIExecutor(
            worktree_manager=self.mgr,
            inbox_checkout_map={},
            templates={"claude": tpl("default"), "custom": tpl("custom"), "other": tpl("other")},
        )
        for name in ("a", "c"):
            (self.root / name).mkdir()

        def evt(external_id: str, **payload: str) -> Event:
            return Event(inbox_id="proj:x", source="test", external_id=external_id, payload=dict(payload))

        text = evt("t1", explicit_workdir=str(self.root / "a"), executor="custom")
        bare_approval = evt("ap1", approval_request_id="req-1")
        targeted_approval = evt(
            "ap2",
            approval_request_id="req-1",
            explicit_workdir=str(self.root / "c"),
            executor="other",
        )
        cases = [
            ([text, bare_approval], "custom:a"),
            ([text, targeted_approval], "custom:a"),
            ([targeted_approval], "other:c"),
        ]
        for batch, expected in cases:
            with self.subTest(batch=[e.external_id for e in batch]):
                out = await executor.execute(batch, None, _FakeCaps())
                assert isinstance(out, Emit)
                self.assertEqual(out.messages, (expected,))

    async def test_cli_env_passthrough_outcomes_and_interrupt_termination(self) -> None:
        emit_tpl = CLITemplate(
            name="claude",
            command=sys.executable,
            args=(
                "-c",
                "import json, os, sys; print(json.dumps({'outcome': 'emit', 'messages': [os.environ.get('MAGPIE_URL', '')]}))",
            ),
            env={"MAGPIE_URL": "http://127.0.0.1:8787"},
        )
        suspend_tpl = CLITemplate(
            name="suspend_cli",
            command=sys.executable,
            args=(
                "-c",
                "import json; print(json.dumps({'outcome': 'suspend', 'request_id': 'req_cli_1', 'prompt': 'approve deploy?'}))",
            ),
        )
        silent_tpl = CLITemplate(
            name="silent_cli",
            command=sys.executable,
            args=("-c", "import json; print(json.dumps({'outcome': 'silent'}))"),
        )
        slow_tpl = CLITemplate(
            name="slow_cli",
            command=sys.executable,
            args=("-c", "import time; time.sleep(10)"),
        )
        executor = LocalCLIExecutor(
            worktree_manager=self.mgr,
            inbox_checkout_map={"proj:hibernaut": self.root},
            templates={
                "claude": emit_tpl,
                "suspend_cli": suspend_tpl,
                "silent_cli": silent_tpl,
                "slow_cli": slow_tpl,
            },
            interrupt_poll_interval=0.02,
        )

        # 1. Emit + env passthrough (yetone/magpie)
        evt = Event(
            inbox_id="proj:hibernaut",
            source="test",
            external_id="e1",
            payload={"explicit_workdir": str(self.root)},
        )
        out = await executor.execute([evt], None, _FakeCaps())
        self.assertIsInstance(out, Emit)
        assert isinstance(out, Emit)
        self.assertEqual(out.messages, ("http://127.0.0.1:8787",))

        # 2. Suspend mapping
        evt_susp = Event(
            inbox_id="proj:hibernaut",
            source="test",
            external_id="e2",
            payload={"explicit_workdir": str(self.root), "executor": "suspend_cli"},
        )
        out_susp = await executor.execute([evt_susp], None, _FakeCaps())
        self.assertIsInstance(out_susp, Suspend)
        assert isinstance(out_susp, Suspend)
        self.assertEqual(out_susp.request_id, "req_cli_1")

        # 3. Silent mapping
        evt_sil = Event(
            inbox_id="proj:hibernaut",
            source="test",
            external_id="e3",
            payload={"explicit_workdir": str(self.root), "executor": "silent_cli"},
        )
        self.assertIsInstance(await executor.execute([evt_sil], None, _FakeCaps()), Silent)

        # 4. Interrupt safely terminates long-running CLI subprocess
        caps = _FakeCaps(interrupted=False)
        evt_slow = Event(
            inbox_id="proj:hibernaut",
            source="test",
            external_id="e4",
            payload={"explicit_workdir": str(self.root), "executor": "slow_cli"},
        )
        task = asyncio.create_task(executor.execute([evt_slow], None, caps))
        await asyncio.sleep(0.05)
        caps._interrupted = True
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2.0)


if __name__ == "__main__":
    unittest.main()
