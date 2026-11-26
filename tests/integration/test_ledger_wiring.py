from __future__ import annotations

import sys
import tempfile
import unittest

from agent_runtime.adapters.executor_cli import CLITemplate, CommandResult, LocalCLIExecutor, OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import SQLiteWorkspaceStore
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.core.models import Emit, Event
from tests.support.fakes import InMemoryMemoryStore
from tests.support.ledger_wiring import INBOX


"""The four thin wirings between Hibernaut / Switchboard and the Observability Ledger (src/ledger)."""


class _Caps:
    memory = InMemoryMemoryStore()

    def is_interrupted(self) -> bool:
        return False


class TestCLIExecutorTelemetry(unittest.IsolatedAsyncioTestCase):
    async def test_cli_outcomes_carry_output_and_exit_code_for_the_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = SQLiteDatabase(":memory:")
            mgr = OrcaWorktreeManager(workspace_store=SQLiteWorkspaceStore(db), backend="git",
                                      runner=lambda _c, _d: CommandResult(0, "", ""))
            ok = ("import json, sys; print('slow start', file=sys.stderr); "
                  "print(json.dumps({'result': 'hi', 'usage': {'input_tokens': 5, 'output_tokens': 2}}))")
            templates = {
                "claude": CLITemplate(name="claude", command=sys.executable, args=("-I", "-c", ok)),
                "bad": CLITemplate(name="bad", command=sys.executable,
                                   args=("-I", "-c", "import sys; print('invalid api key', file=sys.stderr); sys.exit(3)")),
            }
            executor = LocalCLIExecutor(worktree_manager=mgr, inbox_checkout_map={}, templates=templates)
            event = Event(inbox_id=INBOX, source="t", external_id="1", payload={"explicit_workdir": tmp})
            try:
                good = await executor.execute([event], None, _Caps())  # type: ignore[arg-type]
                bad = await executor.execute(
                    [Event(inbox_id=INBOX, source="t", external_id="2",
                           payload={"explicit_workdir": tmp, "executor": "bad"})], None, _Caps())  # type: ignore[arg-type]
            finally:
                db.close()
        self.assertEqual(good, Emit(messages=("hi",), session_id=good.session_id))  # type: ignore[union-attr]
        assert good.telemetry is not None and bad.telemetry is not None
        self.assertEqual((good.telemetry.exit_code, good.telemetry.stderr), (0, "slow start"))
        self.assertIn('"input_tokens": 5', good.telemetry.stdout)
        self.assertEqual((bad.telemetry.exit_code, bad.telemetry.stderr), (3, "invalid api key"))


if __name__ == "__main__":
    unittest.main()
