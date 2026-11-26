import asyncio
import json
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from ccpeer.inbox import JsonlLog
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeError
from agent_runtime.adapters.executor_cli.peer_handoff import PeerHandoff, parse_result
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.peer_mailbox import SQLitePeerMailbox
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore
from tests.support.peer_handoff import DONE, FakeClaude, Incoming, PeerCase, RECEIVER, result


class TestPeerTaskAndResult(PeerCase):
    async def test_task_arrives_as_a_peer_message_and_the_json_result_is_returned(self):
        self.orca.scripts.append([result()])
        reply = await self.converse()

        self.assertEqual(reply.data, DONE)
        session = self.orca.session()
        [task] = session.received
        self.assertEqual(task.header["task_id"], "sess-1")
        self.assertEqual((task.header["reply_to"], task.header["schema"]), (RECEIVER, "sde-report"))
        self.assertTrue(task.header["attempt_id"].startswith("att"))
        self.assertIn("divide must reject a zero divisor", task.body)
        self.assertIn('SendMessage to "hibernaut"', task.body)
        self.assertEqual(task.from_addr, self.receiver.address)
        self.assertEqual(task.frame["from"], self.receiver.address)
        # The session starts idle under ~/.claude with SendMessage allowed and no handoff MCP server.
        [spec] = self.orca.specs
        argv = spec["exec"]
        self.assertNotIn("--mcp-config", argv)
        self.assertNotIn("--", argv)
        self.assertEqual(argv[argv.index("--allowedTools") + 1], "Read,Bash,SendMessage")
        self.assertEqual(argv[argv.index("--name") + 1], "sde · task:1")
        self.assertEqual(argv[argv.index("--session-id") + 1], reply.native_session_id)
        self.assertIn("CLAUDE_CONFIG_DIR", spec["unset"])
        binding = self.bindings.get("sess-1")
        assert binding is not None
        self.assertEqual((binding.handle, binding.native_session_id), (reply.handle, reply.native_session_id))
        self.assertEqual((binding.peer_pid, binding.peer_socket, binding.peer_name),
                         (session.pid, session.sock_path, "sde · task:1"))
        self.assertEqual(binding.peer_attempt, task.header["attempt_id"])
        self.assertNotIn("terminal close", self.orca.verbs())

    async def test_each_kind_of_malformed_result_is_answered_once_with_the_reason_and_then_accepted(self):
        cases = {
            "syntax": (result(raw='{"task_id": "sess-1", "status": "done",'), "not valid JSON"),
            "missing field": (result(drop=("test_evidence",)), "result.test_evidence is required"),
            "attempt mismatch": (result(attempt_id="att-stale"), 'attempt_id is "att-stale"'),
        }
        for label, (bad, reason) in cases.items():
            with self.subTest(label):
                self.orca.scripts.append([bad, result()])
                reply = await self.converse(session_id=f"sess-{label}")
                self.assertEqual(reply.data, DONE)
                task, resend = self.orca.session().wait_received(2)
                self.assertIn("was not accepted", resend.body)
                self.assertIn(reason, resend.body)
                self.assertEqual(resend.header["attempt_id"], task.header["attempt_id"])
                self.assertEqual(len(self.orca.session().received), 2)

    async def test_too_many_malformed_results_fail_the_turn_and_close_the_terminal(self):
        self.orca.scripts.append([result(raw="done!")] * 3)
        runner = self.make_runner(max_resends=1)
        with self.assertRaisesRegex(ClaudeCodeError, r"2 result messages that were not accepted .*limit 1"):
            await self.converse(runner)
        session = self.orca.session()
        self.assertEqual(len(session.received), 2)
        self.assertIn("terminal close", self.orca.verbs())
        binding = self.bindings.get("sess-1")
        assert binding is not None
        # The conversation stays resumable; the terminal is gone.
        self.assertIsNone(binding.handle)
        self.assertEqual(binding.native_session_id, session.session_id)

    async def test_rework_goes_to_the_same_session_with_a_new_attempt(self):
        self.orca.scripts.append([result(), result(summary="reworked")])
        first = await self.converse()
        second = await self.converse(prompt="## Review round 1: changes requested\nfix the edge case")

        self.assertEqual(second.data["summary"], "reworked")
        self.assertEqual((first.handle, first.native_session_id), (second.handle, second.native_session_id))
        self.assertEqual(self.orca.verbs().count("terminal create"), 1)
        self.assertNotIn("terminal send", self.orca.verbs())
        task, rework = self.orca.session().received
        self.assertEqual(task.header["task_id"], rework.header["task_id"])
        self.assertNotEqual(task.header["attempt_id"], rework.header["attempt_id"])
        self.assertIn("fix the edge case", rework.body)
        binding = self.bindings.get("sess-1")
        assert binding is not None
        self.assertEqual(binding.peer_attempt, rework.header["attempt_id"])

    async def test_a_repeated_msg_id_is_handled_once(self):
        inbox = JsonlLog(self.state / RECEIVER / "inbox.jsonl")

        def duplicated_bad(session: FakeClaude, msg: Incoming) -> None:
            # The same record twice in the log, as a replayed delivery would leave it.
            record = {"msg_id": "dup-1", "ts": time.time(), "type": "user", "from_addr": session.address,
                      "from_name": session.name, "from_mode": "prompting", "body": "not json"}
            inbox.append(record)
            inbox.append(record)

        self.orca.scripts.append([duplicated_bad, result()])
        reply = await self.converse()
        self.assertEqual(reply.data, DONE)
        received = self.orca.session().received
        self.assertEqual(len(received), 2, [m.body[:60] for m in received])
        self.assertIn("was not accepted", received[1].body)

    async def test_a_held_receipt_is_reported_to_the_outbox(self):
        def held_then_done(session: FakeClaude, msg: Incoming) -> None:
            session.control(msg, action="held", orig_msg_id=msg.frame["msg_id"])
            result()(session, msg)

        self.orca.scripts.append([held_then_done])
        reply = await self.converse()
        self.assertEqual(reply.data, DONE)
        [(inbox, text)] = self.notes
        self.assertEqual(inbox, "proj:x")
        self.assertIn("'held'", text)
        self.assertIn(reply.handle, text)

    async def test_release_exits_claude_code_before_closing_anything(self):
        self.orca.scripts.append([result()])
        reply = await self.converse()
        session = self.orca.session()

        note = await self.runner.release("sess-1", self.workdir)

        self.assertIn("exited", note)
        exit_call = [c for c in self.orca.calls if c[:2] == ("terminal", "send")]
        self.assertEqual([c[c.index("--text") + 1] for c in exit_call], ["/exit"])
        self.assertNotIn("terminal close", self.orca.verbs())
        self.assertIsNotNone(session.proc.poll())
        self.assertFalse((self.registry / f"{session.pid}.json").exists())
        binding = self.bindings.get("sess-1")
        assert binding is not None
        self.assertEqual((binding.handle, binding.peer_pid, binding.native_session_id),
                         (None, None, reply.native_session_id))

    async def test_a_session_that_exits_without_a_result_is_reported(self):
        def quit_(session: FakeClaude, msg: Incoming) -> None:
            for handle, term in self.orca.terminals.items():
                if term["session"] is session:
                    term["ended"] = True
        self.orca.scripts.append([quit_])
        with self.assertRaisesRegex(ClaudeCodeError, "exited before it returned its result"):
            await self.converse()

    async def test_no_receiver_starts_nothing(self):
        runner = self.make_runner(receiver="nobody")
        with self.assertRaisesRegex(ClaudeCodeError, "receiver 'nobody' is not running"):
            await self.converse(runner)
        self.assertEqual(self.orca.calls, [])


class TestResultSurvivesRestart(PeerCase):
    async def test_a_result_sent_while_hibernaut_is_down_is_read_after_restart(self):
        # Hibernaut sends the task, then stops before the reply arrives; the session answers into the receiver's
        # inbox meanwhile. A new process with the same database finds the reply through the stored cursor.
        self.orca.scripts.append([None])
        task_run = asyncio.ensure_future(self.converse())
        session = await asyncio.to_thread(self.wait_for_task)
        handoff = self.runner._peer
        assert handoff is not None
        await asyncio.to_thread(handoff.ingest)
        before = handoff.mailbox.cursor("user")
        binding = self.bindings.get("sess-1")
        assert binding is not None and binding.peer_attempt
        [task] = session.received
        # Hibernaut goes down: nothing of the run is kept in memory, the terminal stays.
        with patch("agent_runtime.adapters.executor_cli.orca._close_quietly", new=_no_close):
            task_run.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task_run
        self.db.close()
        first = session.reply(task, "still working")
        # Each reply is its own connection, handled on its own receiver thread; the order asserted below
        # is the session's send order, so the second is sent once the first is stored.
        await asyncio.to_thread(self.wait_inbox, 1)
        second = session.reply(task, json.dumps({"task_id": "sess-1", "attempt_id": binding.peer_attempt, **DONE}))
        await asyncio.to_thread(self.wait_inbox, 2)

        db = SQLiteDatabase(self.db_path)
        self.addCleanup(db.close)
        restarted = PeerHandoff(self.config(), SQLitePeerMailbox(db, RECEIVER))
        self.assertEqual(restarted.mailbox.cursor("user"), before)
        await asyncio.to_thread(restarted.ingest)
        stored = SQLiteTerminalBindingStore(db).get("sess-1")
        assert stored is not None
        target = restarted.find(stored.native_session_id or "", self.workdir)
        assert target is not None
        replies = restarted.replies(target, since=0)
        self.assertEqual([r.msg_id for r in replies], [first, second])
        data, error = parse_result(replies[1].body, SDE_SCHEMA, task_id="sess-1", attempt_id=stored.peer_attempt or "")
        self.assertEqual((data, error), (DONE, ""))
        # Read once: a second ingest and take returns nothing new.
        await asyncio.to_thread(restarted.ingest)
        self.assertEqual(restarted.replies(target, since=0), [])

    def wait_for_task(self) -> FakeClaude:
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if self.orca.terminals and self.orca.session().received:
                return self.orca.session()
            time.sleep(0.02)
        raise AssertionError("the task never reached the session")

    def wait_inbox(self, count: int) -> None:
        log = JsonlLog(self.state / RECEIVER / "inbox.jsonl")
        deadline = time.monotonic() + 45
        while len(log.read().records) < count:
            if time.monotonic() > deadline:
                raise AssertionError(f"inbox has {log.read().records}")
            time.sleep(0.02)


async def _no_close(handle: str, workdir: Path) -> bool:
    return True


if __name__ == "__main__":
    unittest.main()
