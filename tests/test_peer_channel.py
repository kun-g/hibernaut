import asyncio
import http.client
import json
import os
import socket
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.peer import DeveloperPeerResolver
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore, TerminalBinding
from agent_runtime.app.config import load_config
from agent_runtime.app.main import serve_configured
from agent_runtime.core.models import Emit, SessionRef
from tests.test_coder_reviewer import _Capture, fake_command, git
from tests.test_sde_executor import EVIDENCE, FIXED, PASS, dev, event, make_base_repo
from tests.test_visible_roles import FakeOrcaTerminals, VisibleCase, arg, build_terminal_daemon

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


class TestReviewerChannel(VisibleCase):
    def setUp(self):
        super().setUp()
        self.registry = self.root / "registry"
        self.enterContext(patch.dict(os.environ, {"FAKE_PEER_REGISTRY": str(self.registry)}))
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.workspaces.reserve_creating(work_key="task:1", repo_checkout=str(self.repo), workdir=str(self.repo),
                                         branch="hb/task-1")
        self.workspaces.mark_ready("task:1", workdir=str(self.repo), branch="hb/task-1")

    def peer_reviewer(self):
        return CoderReviewerExecutor(
            profile=ClaudeCodeProfile(name="coder-reviewer", launcher=self.launcher(), model="claude-sonnet-5-5",
                                      effort="medium"),
            workspaces=self.workspaces, tasks=self.tasks, terminal=self.runner,
            peers=DeveloperPeerResolver(sessions=self.sessions, bindings=self.bindings,
                                        developer_executors=["sde"], registry=[self.registry]),
        )

    def review_event(self):
        return event({"executor_id": "coder-reviewer", "text": "divide must reject zero", "review_of": "task:1",
                      "test_evidence": EVIDENCE, "base_ref": "main"}, correlation_key="review:task:1")

    async def test_developer_and_reviewer_get_each_others_verified_addresses(self):
        self.set_plan(sde=[dev(FIXED)], reviewer=[{**PASS, "developer_messages": [
            "sent: [review of task:1] which test covers b == 0?",
            "received: test_zero_divisor, 1 passed",
        ]}])
        developed = await self.sde(review=False).execute([event(self.payload())], None, self.caps)
        assert isinstance(developed, Emit), developed
        self.sessions.save(self.session_after(developed, work_key="task:1"))
        outcome = await self.peer_reviewer().execute([self.review_event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        (dev_call,) = self.calls("sde", "interactive")
        (review_call,) = self.calls("reviewer", "interactive")
        dev_term, review_term = self.orca.created()
        # Each side is told the other's exact session name; the reviewer's name is its terminal title.
        # Claude Code puts the sender's socket in `from` and its session name in `from-name`.
        self.assertIn('from-name="coder-reviewer (code-reviewer) · review of task:1"', dev_call["prompt"])
        self.assertEqual(review_term["title"], "coder-reviewer (code-reviewer) · review of task:1")
        self.assertIn(f'the Claude Code session "sde · task:1" (native session {developed.executor_session_id})',
                      review_call["prompt"])
        self.assertIn('`to` exactly "sde · task:1"', review_call["prompt"])
        (entry,) = [json.loads(p.read_text()) for p in self.registry.glob("*.json")
                    if json.loads(p.read_text())["sessionId"] == developed.executor_session_id]
        self.assertIn(f'from="uds:{entry["messagingSocketPath"]}" from-name="sde · task:1"', review_call["prompt"])
        # The reviewer is launched by the runtime as its own conversation, not by the developer.
        self.assertNotEqual(arg(review_call, "--session-id"), developed.executor_session_id)
        self.assertEqual(dev_term["title"], "sde · task:1")
        # SendMessage is added; no file-writing tool is.
        allowed = arg(review_call, "--allowedTools").split(",")
        self.assertIn("SendMessage", allowed)
        self.assertFalse({"Bash", "Edit", "Write", "MultiEdit", "NotebookEdit"} & set(allowed))
        self.assertTrue({"Edit", "Write", "MultiEdit", "NotebookEdit"}
                        <= set(arg(review_call, "--disallowedTools").split(",")))
        text = outcome.messages[0]
        self.assertIn(f"Developer channel: sde · task:1 (Claude Code session {developed.executor_session_id}, "
                      f"Hibernaut session {developed.session_id})", text)
        self.assertIn("- received: test_zero_divisor, 1 passed", text)

    async def test_without_a_verified_developer_the_reviewer_gets_no_messaging_tool(self):
        self.set_plan(reviewer=[PASS])
        (self.repo / "calc.py").write_text(FIXED["calc.py"])
        outcome = await self.peer_reviewer().execute([self.review_event()], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        (review_call,) = self.calls("reviewer", "interactive")
        self.assertNotIn("SendMessage", arg(review_call, "--allowedTools").split(","))
        self.assertIn("## Developer channel\nNone: no open task session for 'task:1'", review_call["prompt"])
        self.assertIn("Developer channel: none (no open task session", outcome.messages[0])


class TestDeveloperTools(VisibleCase):
    async def test_the_developer_may_message_its_reviewer_but_never_start_one_as_a_subagent(self):
        self.set_plan(sde=[dev(FIXED)])
        outcome = await self.sde(review=False).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        (dev_call,) = self.calls("sde", "interactive")
        self.assertIn("SendMessage", arg(dev_call, "--allowedTools").split(","))
        self.assertFalse({"Agent", "Task"} & set(arg(dev_call, "--allowedTools").split(",")))
        self.assertTrue({"Agent", "Task"} <= set(arg(dev_call, "--disallowedTools").split(",")))
        self.assertIn("Never start a reviewer yourself", dev_call["prompt"])
        # Claude Code suffixes a name another live session holds (a second review of the same task), so that form
        # is accepted too; the exact task tag is what tells tasks apart.
        self.assertIn('"coder-reviewer (code-reviewer) · review of task:1-<words>"', dev_call["prompt"])
        self.assertIn("starts with exactly `[review of task:1]`", dev_call["prompt"])


class TestIndependentEventsTalk(unittest.TestCase):
    def test_sde_and_review_events_keep_their_own_sessions_and_results_and_share_a_verified_channel(self):
        root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        repo = make_base_repo(root)
        workdirs = {n: root / f"hb-task-{n}" for n in (42, 43)}
        for n, workdir in workdirs.items():
            git(repo, "worktree", "add", "-q", "-b", f"hb/task-{n}", str(workdir))
        log, plan = root / "calls.jsonl", root / "plan.json"
        plan.write_text(json.dumps({"sde": [dev(FIXED)], "reviewer": [PASS]}))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        db_path = root / "hb.db"
        path = root / "hibernaut.toml"
        config_dir = root / "claude-config"
        path.write_text(
            f'[runtime]\ndb_path = "{db_path}"\nmemory_root = "{root / "mem"}"\ndefault_executor = "sde"\n'
            f"[channels]\nport = {port}\n"
            f'[launchers.fake]\ncommand = "{fake_command(root)}"\n'
            '[executors.coder-reviewer]\nrole = "coder-reviewer"\nlauncher = "fake"\nmodel = "claude-sonnet-5-5"\n'
            '[executors.sde]\nrole = "sde"\nlauncher = "fake"\nreview = false\n'
            f'[projects.demo]\ninbox_id = "proj:demo"\nrepo = "o/demo"\ncheckout = "{repo}"\nworktree_backend = "orca"\n'
        )
        orca = FakeOrcaTerminals(root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=orca))
        self.enterContext(patch.dict(os.environ, {
            "FAKE_CLAUDE_LOG": str(log), "FAKE_PLAN": str(plan), "FAKE_SESSIONS": str(root / "sessions"),
            "CLAUDE_CONFIG_DIR": str(config_dir), "FAKE_PEER_REGISTRY": str(config_dir / "sessions"),
        }))
        stderr = self.enterContext(patch("sys.stderr", new_callable=_Capture))

        def post(external_id, correlation_key, payload):
            body = {"inbox_id": "proj:demo", "external_id": external_id, "correlation_key": correlation_key,
                    "payload": payload}
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            try:
                conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
                return conn.getresponse().status
            finally:
                conn.close()

        def outbox():
            with sqlite3.connect(db_path) as conn:
                return [row[0] for row in conn.execute("SELECT content FROM outbox ORDER BY seq")]

        def sessions():
            with sqlite3.connect(db_path) as conn:
                return {row[0]: row[1:] for row in conn.execute(
                    "SELECT correlation_key, executor_id, executor_session_id, session_id FROM sessions")}

        def calls(role):
            rows = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            return [r for r in rows if r["role"] == role and r["mode"] == "interactive"]

        async def scenario():
            config = load_config(path)
            daemon = build_terminal_daemon(config, root)
            for n, workdir in workdirs.items():
                daemon.workspaces.reserve_creating(work_key=f"task-{n}", repo_checkout=str(repo),
                                                   workdir=str(workdir), branch=f"hb/task-{n}")
                daemon.workspaces.mark_ready(f"task-{n}", workdir=str(workdir), branch=f"hb/task-{n}")
            stop = asyncio.Event()
            server = asyncio.create_task(serve_configured(path, config, daemon, stop, reload_interval=0.02))

            async def wait_for(check):
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    self.assertFalse(server.done(), stderr.getvalue())
                    try:
                        if check():
                            return
                    except OSError:
                        pass
                    await asyncio.sleep(0.01)
                self.fail(f"outbox={outbox()} calls={orca.verbs()} {stderr.getvalue()}")

            try:
                # Two developer tasks run side by side; the review names one of them.
                for n in (42, 43):
                    task = {"executor_id": "sde", "text": f"task {n}: divide must reject a zero divisor",
                            "base_ref": "main"}
                    await wait_for(lambda n=n, task=task: post(f"task-{n}", f"task-{n}", task) == 202)
                await wait_for(lambda: sum("[sde] delivered (review=off" in m for m in outbox()) == 2)
                review = {"executor_id": "coder-reviewer", "text": "divide must reject a zero divisor",
                          "review_of": "task-42", "test_evidence": EVIDENCE, "base_ref": "main"}
                await asyncio.to_thread(post, "review-42", "review:task-42", review)
                await wait_for(lambda: any("verdict=pass" in m for m in outbox()))
                verdict = next(m for m in outbox() if "verdict=pass" in m)
                by_key = sessions()
                _, sde_native, sde_session = by_key["task-42"]
                rev_exec, rev_native, rev_session = by_key["review:task-42"]
                self.assertEqual(rev_exec, "coder-reviewer")
                self.assertNotEqual(rev_session, sde_session)
                self.assertNotIn(rev_native, (sde_native, by_key["task-43"][1]))
                (independent,) = calls("reviewer")
                self.assertEqual(independent["cwd"], str(workdirs[42]))
                # Only the developer of task-42 is named; the developer of task-43 is not reachable from this review.
                self.assertIn('`to` exactly "sde · task-42"', independent["prompt"])
                self.assertNotIn("sde · task-43", independent["prompt"])
                self.assertIn(f"Developer channel: sde · task-42 (Claude Code session {sde_native}, "
                              f"Hibernaut session {sde_session})", verdict)
                dev_prompts = {c["cwd"]: c["prompt"] for c in calls("sde")}
                self.assertIn('from-name="coder-reviewer (code-reviewer) · review of task-42"', dev_prompts[str(workdirs[42])])
                self.assertIn('from-name="coder-reviewer (code-reviewer) · review of task-43"', dev_prompts[str(workdirs[43])])
            finally:
                stop.set()
                await server
                await orca.close_all()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
