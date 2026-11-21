import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

from scripts import run_updating


class AutoUpdateTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.origin = self.root / "source"
        self.checkout = self.root / "checkout"
        self.git(self.root, "init", "-b", "main", str(self.origin))
        repo = run_updating.ROOT
        for folder in ("src", "scripts"):
            shutil.copytree(repo / folder, self.origin / folder, ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copyfile(repo / ".gitignore", self.origin / ".gitignore")
        self.commit(self.origin)
        self.git(self.root, "clone", str(self.origin), str(self.checkout))

    def git(self, cwd, *args):
        return subprocess.check_output(["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgSign=false", *args], cwd=cwd, text=True, stderr=subprocess.PIPE).strip()

    def commit(self, cwd):
        self.git(cwd, "add", ".")
        self.git(cwd, "commit", "-qm", "fixture")

    def test_skips_dirty_diverged_and_non_main(self):
        with patch.object(run_updating, "ROOT", self.checkout):
            self.assertIsNone(run_updating.candidate())
            (self.origin / "release").touch()
            self.commit(self.origin)
            (self.checkout / "local").touch()
            self.assertIsNone(run_updating.candidate())
            self.git(self.checkout, "switch", "-c", "topic")
            self.commit(self.checkout)
            self.assertIsNone(run_updating.candidate())
            self.git(self.checkout, "switch", "main")
            self.assertIsNotNone(run_updating.candidate())
            self.git(self.checkout, "merge", "--ff-only", "topic")
            self.assertIsNone(run_updating.candidate())

    def test_upgrade_serves_new_response_and_keeps_history(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        db = self.root / "events.db"
        proc = subprocess.Popen(
            [sys.executable, "scripts/run_updating.py", "--interval", "1", "--port", str(port), "--db", str(db)],
            cwd=self.checkout, env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        def send_until_reply(prefix):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                self.assertIsNone(proc.poll(), "supervisor exited")
                body = {"external_id": str(time.monotonic_ns()), "payload": {"text": prefix}}
                req = urllib.request.Request(f"http://127.0.0.1:{port}/events", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(req, timeout=1) as response:
                        self.assertEqual(response.status, 202)
                except (urllib.error.URLError, ConnectionError):
                    pass
                else:
                    with sqlite3.connect(db) as conn:
                        if conn.execute("SELECT 1 FROM outbox WHERE content = ? AND status = 'sent'", (prefix + prefix,)).fetchone():
                            return
                time.sleep(.15)
            self.fail(f"Missing reply for {prefix!r}")

        try:
            send_until_reply("收到事件：")
            backend = self.origin / "src/agent_runtime/adapters/executor_whitebox/executor.py"
            backend.write_text(backend.read_text().replace("收到事件：", "updated: "))
            self.commit(self.origin)
            send_until_reply("updated: ")
            self.assertEqual(self.git(self.checkout, "rev-parse", "HEAD"), self.git(self.origin, "rev-parse", "HEAD"))
            with sqlite3.connect(db) as conn:
                self.assertTrue(conn.execute("SELECT 1 FROM outbox WHERE content = '收到事件：收到事件：'").fetchone())
        finally:
            proc.terminate()
            proc.wait(timeout=10)
