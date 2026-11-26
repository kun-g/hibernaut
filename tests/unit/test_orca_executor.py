import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from agent_runtime.adapters.executor_cli.orca import UNCONFIRMED_CHECKPOINT, OrcaVisibleRunner
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore, TerminalBinding
from agent_runtime.core.models import Emit, Event, PermissionDecision, SessionRef, Suspend
from tests.support.fakes import InMemoryPermission
from tests.support.orca_executor import FakeOrca, _RESULT_RE


class TestBindingMigration(unittest.TestCase):
    def test_sessions_that_ran_before_bindings_existed_get_a_marker(self):
        from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter

        path = Path(self.enterContext(tempfile.TemporaryDirectory())) / "hb.db"
        db = SQLiteDatabase(path)
        sessions = SQLiteSessionStoreAdapter(db)
        sessions.save(SessionRef(session_id="old", inbox_id="i", executor_id="claude"))
        sessions.save(SessionRef(session_id="unrouted", inbox_id="i"))
        with db.transaction() as conn:
            conn.execute("DROP TABLE terminal_bindings")
        db.close()
        db = SQLiteDatabase(path)
        self.addCleanup(db.close)
        store = SQLiteTerminalBindingStore(db)
        marker = store.get("old")
        self.assertEqual((marker.executor_id, marker.handle, marker.native_session_id), ("claude", None, None))
        self.assertIsNone(store.get("unrouted"))
        sessions = SQLiteSessionStoreAdapter(db)
        sessions.save(SessionRef(session_id="new", inbox_id="i", executor_id="claude"))
        db.close()
        db = SQLiteDatabase(path)
        self.addCleanup(db.close)
        self.assertIsNone(SQLiteTerminalBindingStore(db).get("new"))


class TestVisibleContinuation(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.orca = FakeOrca(self.root / "claude")
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.bindings = SQLiteTerminalBindingStore(self.db)
        self.runner = OrcaVisibleRunner(bindings=self.bindings, transcript_roots=[self.root / "claude"], poll_interval=0.01)

    def run_task(self, session=None, events=(), timeout=2, workdir=None, session_id="s1"):
        return self.runner.run(executor_id="claude", agent="claude", command="claude", workdir=workdir or self.workdir,
                               prompt="task", session=session, session_id=session_id, events=events,
                               caps=self.caps, timeout=timeout)

    def session(self, native=None, **extra):
        return SessionRef(session_id="s1", inbox_id="i", executor_id="claude", executor_session_id=native, **extra)

    async def first_run(self):
        outcome = await self.run_task()
        assert isinstance(outcome, Emit)
        return outcome

    async def test_first_run_binds_terminal_and_confirmed_native_session(self):
        outcome = await self.first_run()
        sid = outcome.executor_session_id
        self.assertEqual(outcome.messages, (f"turn 1 of {sid}",))
        binding = self.bindings.get("s1")
        self.assertEqual((binding.native_session_id, binding.workdir), (sid, str(self.workdir)))
        self.assertEqual(binding.incarnation_id, self.orca.terminals[binding.handle]["incarnation"])

    async def test_follow_up_reuses_the_live_terminal(self):
        first = await self.first_run()
        outcome = await self.run_task(self.session(first.executor_session_id))
        self.assertEqual(outcome.messages, (f"turn 2 of {first.executor_session_id}",))
        self.assertEqual(self.orca.verbs().count("agent launch"), 1)
        self.assertEqual(self.orca.verbs()[-1], "terminal send")
        self.assertNotIn("terminal create", self.orca.verbs())

    async def test_closed_terminal_resumes_the_native_session(self):
        first = await self.first_run()
        old = self.bindings.get("s1").handle
        self.orca.terminals[old]["alive"] = False
        outcome = await self.run_task(self.session(first.executor_session_id))
        self.assertEqual(outcome.messages, (f"turn 2 of {first.executor_session_id}",))
        create = next(c for c in self.orca.calls if c[:2] == ("terminal", "create"))
        self.assertIn(f"claude --resume {first.executor_session_id}", create)
        self.assertNotEqual(self.bindings.get("s1").handle, old)
        self.assertEqual(self.orca.verbs().count("agent launch"), 1)

    async def test_restarted_process_behind_the_same_handle_is_not_reused(self):
        first = await self.first_run()
        handle = self.bindings.get("s1").handle
        self.orca.terminals[handle]["incarnation"] = "other-process"
        outcome = await self.run_task(self.session(first.executor_session_id))
        self.assertEqual(outcome.messages, (f"turn 2 of {first.executor_session_id}",))
        self.assertIn("terminal create", self.orca.verbs())

    async def test_binding_survives_a_new_hibernaut_session_id_through_the_native_id(self):
        first = await self.first_run()
        session = SessionRef(session_id="s2", inbox_id="i", executor_id="claude",
                             executor_session_id=first.executor_session_id)
        outcome = await self.run_task(session, session_id="s2")
        self.assertEqual(outcome.messages, (f"turn 2 of {first.executor_session_id}",))
        self.assertEqual(self.orca.verbs()[-1], "terminal send")

    async def test_new_session_already_assigned_to_the_executor_launches(self):
        # The worker saves the executor on a session before its first Run.
        outcome = await self.run_task(self.session(None))
        self.assertIn("turn 1 of", outcome.messages[0])
        self.assertEqual(self.orca.verbs()[0], "agent launch")

    async def test_unknown_binding_is_reported_and_starts_nothing(self):
        self.bindings.save(TerminalBinding(session_id="s1", executor_id="claude", agent="", workdir=""))
        outcome = await self.run_task(self.session(None))
        assert isinstance(outcome, Suspend)
        self.assertIn("Cannot confirm the agent conversation bound to session s1", outcome.prompt)
        self.assertIn("no terminal was recorded and no native session ID is known", outcome.prompt)
        self.assertEqual(outcome.session_checkpoint, UNCONFIRMED_CHECKPOINT)
        self.assertEqual(self.orca.calls, [])

    async def test_approval_starts_a_new_conversation_and_rejection_skips(self):
        pending = self.session(None, checkpoint=UNCONFIRMED_CHECKPOINT, pending_request_id="req_1")

        def decision(approved):
            return [Event(inbox_id="i", source="t", external_id=f"a{approved}",
                          payload={"approval_request_id": "req_1", "approved": approved})]

        outcome = await self.run_task(pending, decision(False))
        self.assertIsInstance(outcome, Emit)
        self.assertIn("declined", outcome.messages[0])
        self.assertEqual(self.orca.calls, [])
        outcome = await self.run_task(pending, decision(True))
        self.assertIn("turn 1 of", outcome.messages[0])
        self.assertEqual(self.orca.verbs()[0], "agent launch")

    async def test_closed_terminal_without_native_session_is_reported(self):
        self.orca.root = self.root / "elsewhere"  # transcripts land where Hibernaut cannot see them
        first = await self.first_run()
        self.assertIsNone(first.executor_session_id)
        self.orca.terminals[self.bindings.get("s1").handle]["alive"] = False
        outcome = await self.run_task(self.session(None))
        assert isinstance(outcome, Suspend)
        self.assertIn("is closed and no native session ID is known", outcome.prompt)
        self.assertEqual(self.orca.verbs().count("agent launch"), 1)

    async def test_live_terminal_without_native_session_is_still_reused(self):
        self.orca.root = self.root / "elsewhere"
        await self.first_run()
        outcome = await self.run_task(self.session(None))
        self.assertIn("turn 2 of", outcome.messages[0])

    async def test_workdir_change_is_reported(self):
        first = await self.first_run()
        other = self.root / "other"
        other.mkdir()
        outcome = await self.run_task(self.session(first.executor_session_id), workdir=other)
        assert isinstance(outcome, Suspend)
        self.assertIn(f"ran in {self.workdir}", outcome.prompt)

    async def test_ambiguous_send_suspends_without_redelivery(self):
        first = await self.first_run()
        self.orca.fail["terminal send"] = RuntimeError("transport lost")
        outcome = await self.run_task(self.session(first.executor_session_id))
        assert isinstance(outcome, Suspend)
        self.assertIn("transport lost", outcome.prompt)
        self.assertEqual(self.orca.verbs().count("terminal send"), 1)
        self.assertEqual(self.orca.verbs().count("agent launch"), 1)

    async def test_resume_that_does_not_start_the_agent_is_reported(self):
        first = await self.first_run()
        self.orca.terminals[self.bindings.get("s1").handle]["alive"] = False
        self.orca.conversations.pop(first.executor_session_id)
        outcome = await self.run_task(self.session(first.executor_session_id))
        assert isinstance(outcome, Suspend)
        self.assertIn("did not start agent 'claude'", outcome.prompt)
        self.assertNotIn("terminal send", self.orca.verbs())

    async def test_timeout_closes_the_terminal_but_keeps_the_native_session(self):
        self.orca.answer = False
        outcome = await self.run_task(timeout=0.05)
        self.assertIsInstance(outcome, Suspend)
        self.assertEqual(self.orca.verbs()[-1], "terminal close")
        binding = self.bindings.get("s1")
        self.assertIsNone(binding.handle)
        self.assertIn(binding.native_session_id, self.orca.conversations)


class TestVisibleLaunchSafety(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.receipt = {"outcome": {"kind": "terminal", "handle": "term_test"}}
        self.command = self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new_callable=AsyncMock))
        self.runner = OrcaVisibleRunner(transcript_roots=[self.root / "claude"], poll_interval=0.01)

    def run_task(self, timeout=2):
        return self.runner.run(executor_id="claude", agent="claude", command="claude", workdir=self.root,
                               prompt="task", session=None, session_id="s1", events=(), caps=self.caps, timeout=timeout)

    async def test_terminal_receipt_is_not_the_outcome(self):
        def launch(*args, cwd):
            if args[:2] != ("agent", "launch"):
                return {"terminal": {}}
            self.assertEqual(args[:6], ("agent", "launch", "--agent", "claude", "--worktree", f"path:{cwd}"))
            self.assertIn("terminal", args)
            self.assertNotIn("--focus", args)
            Path(_RESULT_RE.search(args[-1]).group(1)).write_text(json.dumps({"outcome": "emit", "messages": ["tests passed"]}))
            return self.receipt
        self.command.side_effect = launch
        self.assertEqual(await self.run_task(), Emit(messages=("tests passed",), session_id="s1"))
        self.assertEqual(sum(1 for c in self.command.await_args_list if c.args[:2] == ("agent", "launch")), 1)

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
        self.assertEqual(await self.run_task(), Emit(messages=("denied",), session_id="s1", status="refused"))
        self.command.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
