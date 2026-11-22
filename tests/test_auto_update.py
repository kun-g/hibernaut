import contextlib
import http.client
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

from agent_runtime import __version__
from scripts import run_updating
from tests.process_cleanup import processes_under, stop_supervisor

SUPERVISOR = run_updating.ROOT / "scripts/run_updating.py"

# Runs run_updating.py, but the first time its main loop waits (on a threading.Event, or on the daemon through
# Popen.wait), the wait's lock is taken and SIGTERM is raised while it is held: the Python-level handler runs at the
# next bytecode boundary, inside that critical section. This makes a race that production hits by chance certain.
SIGNAL_UNDER_WAIT_LOCK = '''
import os, runpy, signal, subprocess, sys, threading

fired = False


def deliver_while_holding(lock):
    global fired
    if fired:
        return
    fired = True
    with lock:
        os.kill(os.getpid(), signal.SIGTERM)
        for _ in range(100000):
            pass


event_wait = threading.Event.wait
popen_wait = subprocess.Popen.wait


def hooked_event_wait(self, timeout=None):
    deliver_while_holding(self._cond)
    return event_wait(self, timeout)


def hooked_popen_wait(self, timeout=None):
    deliver_while_holding(self._waitpid_lock)
    return popen_wait(self, timeout)


threading.Event.wait = hooked_event_wait
subprocess.Popen.wait = hooked_popen_wait
script = sys.argv[1]
sys.argv = sys.argv[1:]
runpy.run_path(script, run_name="__main__")
'''


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class SupervisorSignalTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.log_path = self.root / "supervisor.log"
        self.log = self.enterContext(self.log_path.open("w"))

    def test_a_signal_that_lands_while_the_main_loop_holds_its_wait_lock_still_stops_the_supervisor(self):
        wrapper = self.root / "signal_under_wait_lock.py"
        wrapper.write_text(SIGNAL_UNDER_WAIT_LOCK)
        proc = subprocess.Popen(
            [sys.executable, str(wrapper), str(SUPERVISOR), "--interval", "300", "--port", str(free_port()),
             "--db", str(self.root / "hb.db")],
            cwd=self.root, stdout=self.log, stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.addCleanup(stop_supervisor, proc, self.root)
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.fail(f"supervisor still running 15s after a signal under its wait lock\n{self.log_path.read_text()}")
        self.assertEqual(proc.returncode, 0, self.log_path.read_text())
        self.assertEqual(processes_under(self.root), [])



class AutoUpdateTests(unittest.TestCase):
    stop_grace = 5.0

    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
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

    def start_supervisor(self) -> subprocess.Popen:
        self.port = free_port()
        self.db = self.root / "events.db"
        self.log_path = self.root / "daemon.log"
        log = self.enterContext(self.log_path.open("w"))
        # Absolute paths put the temp root on every command line (supervisor, each daemon generation), which is how
        # the cleanup finds them; it runs before the temp root is removed.
        proc = subprocess.Popen(
            [
                sys.executable,
                str(self.checkout / "scripts/run_updating.py"),
                "--interval", "0.1",
                "--outbox-interval", "0.02",
                "--port", str(self.port),
                "--db", str(self.db),
            ],
            stdout=log,
            cwd=self.checkout, env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self.addCleanup(stop_supervisor, proc, self.root, self.stop_grace)
        return proc

    def send_until_reply(self, proc: subprocess.Popen, prefix: str) -> None:
        deadline = time.monotonic() + 15
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            self.assertIsNone(proc.poll(), "supervisor exited")
            body = {"external_id": str(time.monotonic_ns()), "payload": {"text": prefix}}
            req = urllib.request.Request(f"http://127.0.0.1:{self.port}/events", data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=1) as response:
                    self.assertEqual(response.status, 202)
            # A single read can time out under load (or while the daemon restarts) well inside the overall deadline.
            except (OSError, http.client.HTTPException) as exc:
                last_error = exc
            else:
                with contextlib.closing(sqlite3.connect(self.db)) as conn:
                    if conn.execute("SELECT 1 FROM outbox WHERE content = ? AND status = 'sent'", (prefix + prefix,)).fetchone():
                        return
            time.sleep(.03)
        self.fail(f"Missing reply for {prefix!r}; last request error: {last_error!r}")

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
        proc = self.start_supervisor()
        self.send_until_reply(proc, "收到事件：")
        self.assertEqual(self.log_path.read_text().splitlines()[0], f"Hibernaut {__version__}")
        self.assertRegex(__version__, r"\A(?:0|[1-9][0-9]*)(?:\.(?:0|[1-9][0-9]*)){2}\Z")
        major, minor, patch_number = __version__.split(".")
        next_version = f"{major}.{minor}.{int(patch_number) + 1}"
        version_file = self.origin / "src/agent_runtime/__init__.py"
        version_file.write_text(version_file.read_text().replace(__version__, next_version))
        backend = self.origin / "src/agent_runtime/adapters/executor_whitebox/executor.py"
        backend.write_text(backend.read_text().replace("收到事件：", "updated: "))
        self.commit(self.origin)
        self.send_until_reply(proc, "updated: ")
        self.assertIn(f"Hibernaut {next_version}", self.log_path.read_text().splitlines())
        self.assertEqual(self.git(self.checkout, "rev-parse", "HEAD"), self.git(self.origin, "rev-parse", "HEAD"))
        with contextlib.closing(sqlite3.connect(self.db)) as conn:
            self.assertTrue(conn.execute("SELECT 1 FROM outbox WHERE content = '收到事件：收到事件：'").fetchone())


class AutoUpdateCleanupTests(unittest.TestCase):
    def run_case(self, case: AutoUpdateTests) -> Path:
        result = unittest.TestResult()
        case.run(result)
        self.assertEqual((len(result.errors), len(result.failures)), (0, 1), result.errors + result.failures)
        return case.root

    def test_a_case_that_fails_mid_run_leaves_no_process(self):
        class FailsMidRun(AutoUpdateTests):
            def fail_after_the_first_reply(self):
                self.send_until_reply(self.start_supervisor(), "收到事件：")
                self.fail("deliberate failure while the supervisor runs")

        self.assertEqual(processes_under(self.run_case(FailsMidRun("fail_after_the_first_reply"))), [])

    def test_a_case_whose_daemon_stalls_past_the_stop_grace_leaves_no_process(self):
        class DaemonStalls(AutoUpdateTests):
            stop_grace = 1.0

            def stall_the_daemon_then_fail(self):
                self.send_until_reply(self.start_supervisor(), "收到事件：")
                for pid, command in processes_under(self.root):
                    if "agent_runtime.app.main" in command:
                        os.kill(pid, signal.SIGSTOP)
                self.fail("deliberate failure with the daemon stopped")

        self.assertEqual(processes_under(self.run_case(DaemonStalls("stall_the_daemon_then_fail"))), [])
