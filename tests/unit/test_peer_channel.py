import json
import os
import tempfile
import unittest
from pathlib import Path
from agent_runtime.adapters.executor_cli.peer import DeveloperPeerResolver
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore, TerminalBinding
from agent_runtime.core.models import SessionRef


NATIVE = "11111111-2222-3333-4444-555555555555"


class TestDeveloperPeerResolver(unittest.TestCase):
    """A review_of key resolves to a SendMessage address only through bindings the runtime recorded itself."""

    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.bindings = SQLiteTerminalBindingStore(self.db)
        self.registry = self.root / "sessions"
        self.registry.mkdir()
        self.workdir = self.root / "hb-task-42"
        self.workdir.mkdir()
        self.resolver = DeveloperPeerResolver(sessions=self.sessions, bindings=self.bindings,
                                              developer_executors=["sde"], registry=[self.registry])

    def task(self, key="task-42", executor="sde", workdir=None, native=NATIVE, inbox="proj:demo", sid="sess_dev"):
        self.sessions.save(SessionRef(session_id=sid, inbox_id=inbox, correlation_key=key, executor_id=executor,
                                      executor_session_id=native, work_key=key))
        self.bindings.save(TerminalBinding(session_id=sid, executor_id=executor, agent="claude",
                                           workdir=str(workdir or self.workdir), handle="term_1",
                                           native_session_id=native))

    def live(self, name="sde · task-42", native=NATIVE, cwd=None, pid=None, slot="a"):
        # The test process itself stands in for the live Claude Code process.
        (self.registry / f"{slot}.json").write_text(json.dumps({
            "pid": pid or os.getpid(), "sessionId": native, "cwd": str(cwd or self.workdir), "name": name,
            "messagingSocketPath": "/tmp/x.sock",
        }))

    def resolve(self, review_of="task-42", inbox="proj:demo", workdir=None):
        return self.resolver.resolve(inbox_id=inbox, review_of=review_of, workdir=workdir or self.workdir)

    def test_the_live_developer_of_the_reviewed_task_is_found_by_its_bound_native_session(self):
        self.task()
        self.live()
        self.live(name="sde · task-43", native="99999999-2222-3333-4444-555555555555", slot="b")
        found = self.resolve()
        assert found.peer is not None, found.reason
        self.assertEqual((found.peer.name, found.peer.native_session_id, found.peer.hibernaut_session_id,
                          found.peer.address), ("sde · task-42", NATIVE, "sess_dev", "uds:/tmp/x.sock"))

    def test_no_channel_unless_every_binding_matches(self):
        other = self.root / "hb-task-43"
        other.mkdir()
        cases = {
            "another inbox": dict(task={"inbox": "proj:other"}),
            "not a developer": dict(task={"executor": "coder-reviewer"}),
            "another worktree": dict(task={"workdir": other}),
            "not running": dict(live={"native": "99999999-2222-3333-4444-555555555555"}),
            "dead process": dict(live={"pid": 2 ** 22 + 12345}),
            "cwd elsewhere": dict(live={"cwd": other}),
        }
        for why, case in cases.items():
            with self.subTest(why):
                for path in self.registry.iterdir():
                    path.unlink()
                with self.db.transaction() as conn:
                    conn.execute("DELETE FROM sessions")
                    conn.execute("DELETE FROM terminal_bindings")
                self.task(**case.get("task", {}))
                self.live(**case.get("live", {}))
                found = self.resolve()
                self.assertIsNone(found.peer, why)
                self.assertTrue(found.reason, why)

    def test_a_name_two_live_sessions_hold_is_refused(self):
        self.task()
        self.live()
        self.live(native="99999999-2222-3333-4444-555555555555", slot="b")
        found = self.resolve()
        self.assertIsNone(found.peer)
        self.assertIn("more than one live session", found.reason)

    def test_an_unknown_key_has_no_channel(self):
        self.task()
        self.live()
        self.assertIsNone(self.resolve(review_of="task-43").peer)


if __name__ == "__main__":
    unittest.main()
