import asyncio
import http.client
import json
import os
import socket
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.support.orca_executor import FakeOrca


class TestContinuationThroughEvents(unittest.TestCase):
    def test_two_events_on_one_correlation_key_continue_one_conversation(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        repo = root / "repo"
        repo.mkdir()
        for args in (["init", "-q", "-b", "main"], ["-c", "user.email=t@example.invalid", "-c", "user.name=t",
                                                     "commit", "-q", "--allow-empty", "-m", "base"]):
            subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)
        workdir = root / "hb-task-1"
        subprocess.run(["git", "worktree", "add", "-q", "-b", "hb/task-1", str(workdir)], cwd=repo,
                       check=True, capture_output=True)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        db_path = root / "hb.db"
        path = root / "hibernaut.toml"
        path.write_text(
            f'[runtime]\ndb_path = "{db_path}"\nmemory_root = "{root / "mem"}"\ndefault_executor = "claude"\n'
            f"[channels]\nport = {port}\n"
            '[executors.claude]\ncommand = "claude"\norca_agent = "claude"\ntimeout_seconds = 10\n'
            f'[projects.demo]\ninbox_id = "proj:demo"\nrepo = "o/demo"\ncheckout = "{repo}"\nworktree_backend = "orca"\n'
        )
        orca = FakeOrca(root / "claude")
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=orca))
        self.enterContext(patch.dict(os.environ, {"CLAUDE_CONFIG_DIR": str(root / "claude")}))

        def post(n):
            body = {"inbox_id": "proj:demo", "external_id": f"e{n}", "correlation_key": "task:demo",
                    "payload": {"text": f"message {n}"}}
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
                return conn.execute("SELECT session_id, executor_session_id FROM sessions").fetchall()

        async def scenario():
            from agent_runtime.app.config import load_config
            from agent_runtime.app.main import build_daemon_from_config, serve_configured

            config = load_config(path)
            daemon = build_daemon_from_config(config)
            daemon.workspaces.reserve_creating(work_key="task:demo", repo_checkout=str(repo.resolve()),
                                               workdir=str(workdir.resolve()), branch="hb/task-1")
            daemon.workspaces.mark_ready("task:demo", workdir=str(workdir.resolve()), branch="hb/task-1")
            stop = asyncio.Event()
            server = asyncio.create_task(serve_configured(path, config, daemon, stop, reload_interval=0.02))

            async def wait_for(check):
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    self.assertFalse(server.done())
                    try:
                        if check():
                            return
                    except OSError:
                        pass
                    await asyncio.sleep(0.01)
                self.fail(f"outbox={outbox()} calls={orca.verbs()}")

            try:
                await wait_for(lambda: post(1) == 202)
                await wait_for(lambda: len(outbox()) == 1)
                ((session_id, native),) = sessions()
                self.assertEqual(outbox(), [f"turn 1 of {native}"])
                await asyncio.to_thread(post, 2)
                await wait_for(lambda: len(outbox()) == 2)
                self.assertEqual(outbox()[1], f"turn 2 of {native}")
                self.assertEqual(sessions(), [(session_id, native)])
                self.assertEqual(orca.verbs().count("agent launch"), 1)
                self.assertIn("message 1", orca.conversations[native][0])
                self.assertIn("message 2", orca.conversations[native][1])
            finally:
                stop.set()
                await server

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
