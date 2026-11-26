import contextlib
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from agent_runtime import __version__
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteOutboxAdapter
from scripts import run_updating
from tests.support.process_cleanup import kill_processes_under, processes_under
from tests.support.reload_handover import GATED_CLI
from tests.support.graceful_stop import BLOCKED, OTHER, free_port, post


SRC = run_updating.ROOT / "src"


class GatedDaemonCase(unittest.TestCase):
    # A daemon process whose Runs on text "block..." wait for the gate file, standing in for a long SDE Run.
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.cli = self.root / "gated_cli.py"
        self.cli.write_text(GATED_CLI)
        self.run_log = self.root / "runs.jsonl"
        self.gate = self.root / "gate"
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.db_path = self.root / "hb.db"
        self.config = self.root / "hibernaut.toml"
        self.port = free_port()
        self.config.write_text(
            f'[runtime]\ndb_path = "{self.db_path}"\nmemory_root = "{self.root / "mem"}"\n'
            'default_executor = "cli"\nmax_concurrency = 4\n'
            f'[channels]\nhost = "127.0.0.1"\nport = {self.port}\n'
            f'[executors.cli]\ncommand = "{sys.executable}"\nargs = ["{self.cli}"]\ntimeout_seconds = 60\n'
            f'[executors.cli.env]\nGEN = "1"\nRUN_LOG = "{self.run_log}"\nGATE = "{self.gate}"\n'
        )
        self.log_path = self.root / "daemon.log"
        self.log = self.enterContext(self.log_path.open("w"))

    def spawn(self, args: list[str], cwd: Path) -> subprocess.Popen:
        env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1",
               "PYTHONPATH": str(SRC) + os.pathsep + os.environ.get("PYTHONPATH", "")}
        proc = subprocess.Popen(args, cwd=cwd, env=env, stdout=self.log, stderr=subprocess.STDOUT)

        def cleanup():
            self.gate.touch()
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=10)
            kill_processes_under(self.root)

        self.addCleanup(cleanup)
        return proc

    def post(self, inbox_id: str, text: str, external_id: str | None = None):
        return post(self.port, inbox_id, text, self.workdir, external_id)

    def wait_for(self, check, proc: subprocess.Popen, timeout: float = 20.0, alive: bool = True):
        deadline = time.monotonic() + timeout
        while not check():
            if alive and proc.poll() is not None:
                self.fail(f"process exited ({proc.returncode})\n{self.log_path.read_text()}")
            if time.monotonic() > deadline:
                self.fail(f"timed out\n{self.log_path.read_text()}")
            time.sleep(0.03)

    def rows(self, sql: str, *params):
        with contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            return conn.execute(sql, params).fetchall()

    def wait_for_drain(self, proc):
        self.wait_for(lambda: "Stopping: waiting for runs in progress" in self.log_path.read_text(), proc)

    def runs(self):
        if not self.run_log.exists():
            return []
        return [r["text"] for r in map(json.loads, self.run_log.read_text().splitlines())]

    def event_status(self, event_id: str) -> str:
        return self.rows("SELECT status FROM event_inbox WHERE event_id = ?", event_id)[0][0]

    def outbox(self, inbox_id: str):
        return self.rows("SELECT content, status FROM outbox WHERE inbox_id = ? ORDER BY seq", inbox_id)

    def start_blocked_run(self, proc: subprocess.Popen) -> str:
        self.wait_for(lambda: self.post(BLOCKED, "warmup")[0] == 202, proc)
        self.wait_for(lambda: self.outbox(BLOCKED) == [("warmup by gen 1", "sent")], proc)
        status, body = self.post(BLOCKED, "block-1")
        self.assertEqual(status, 202)
        self.wait_for(lambda: "block-1" in self.runs(), proc)
        return body["event_id"]


class TestStopWaitsForRunsWithIngressUp(GatedDaemonCase):
    def daemon(self) -> subprocess.Popen:
        return self.spawn([sys.executable, "-m", "agent_runtime.app.main", "--config", str(self.config),
                           "--outbox-interval", "0.02"], cwd=self.root)

    def test_stop_keeps_ingress_and_delivery_until_the_run_finishes(self):
        proc = self.daemon()
        blocked_id = self.start_blocked_run(proc)
        proc.send_signal(signal.SIGTERM)
        self.wait_for_drain(proc)
        self.assertIsNone(proc.poll())

        # (a) Ingress accepts and stores the event; nothing runs it in this process.
        status, other = self.post(OTHER, "other-1", external_id="other-1")
        self.assertEqual(status, 202)
        self.assertFalse(other["duplicate"])
        status, again = self.post(OTHER, "other-1", external_id="other-1")
        self.assertEqual((status, again["duplicate"], again["status"]), (202, True, "dropped"))
        # (b) Outbox delivery keeps running while the Run is blocked.
        with contextlib.closing(SQLiteDatabase(self.db_path)) as db:
            SQLiteOutboxAdapter(db).enqueue(OTHER, "notice during drain")
        self.wait_for(lambda: self.outbox(OTHER) == [("notice during drain", "sent")], proc)
        self.assertEqual(self.event_status(other["event_id"]), "pending")
        self.assertNotIn("other-1", self.runs())
        self.assertEqual(self.event_status(blocked_id), "processing")
        self.assertIsNone(proc.poll())

        # (c) The Run finishes and writes its result, then the process exits.
        self.gate.touch()
        proc.wait(timeout=20)
        self.assertEqual(proc.returncode, 0, self.log_path.read_text())
        self.assertEqual(self.event_status(blocked_id), "done")
        self.assertIn("block-1 by gen 1", [content for content, _ in self.outbox(BLOCKED)])
        self.assertEqual(self.event_status(other["event_id"]), "pending")
        self.assertNotIn("other-1", self.runs())
        self.assertEqual(self.rows("SELECT COUNT(*) FROM worker_leases"), [(0,)])

        # The next process runs what the drained one accepted.
        proc = self.daemon()
        self.wait_for(lambda: self.event_status(other["event_id"]) == "done", proc)
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=20)

    def test_a_second_signal_exits_without_waiting_or_writing_a_partial_result(self):
        proc = self.daemon()
        blocked_id = self.start_blocked_run(proc)
        proc.send_signal(signal.SIGTERM)
        self.wait_for_drain(proc)
        self.assertEqual(self.post(OTHER, "other-1")[0], 202)
        self.assertIsNone(proc.poll())

        # (d) The second signal does not wait for the gate.
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        self.assertFalse(self.gate.exists())
        self.assertEqual(self.event_status(blocked_id), "pending")
        self.assertEqual([content for content, _ in self.outbox(BLOCKED)], ["warmup by gen 1"])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM worker_leases"), [(0,)])


class TestUpdateWaitsForDrain(GatedDaemonCase):
    # maintenance.auto=false: the detached repack a commit may start races the local clone that follows it.
    def git(self, cwd, *args):
        return subprocess.check_output(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgSign=false", "-c", "maintenance.auto=false",
             *args], cwd=cwd, text=True, stderr=subprocess.PIPE).strip()

    def test_supervisor_restarts_only_after_the_old_daemon_drains(self):
        # (e)
        origin = self.root / "origin"
        checkout = self.root / "checkout"
        self.git(self.root, "init", "-b", "main", str(origin))
        for folder in ("src", "scripts"):
            shutil.copytree(run_updating.ROOT / folder, origin / folder, ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copyfile(run_updating.ROOT / ".gitignore", origin / ".gitignore")
        self.git(origin, "add", ".")
        self.git(origin, "commit", "-qm", "fixture")
        self.git(self.root, "clone", "-q", str(origin), str(checkout))

        proc = self.spawn([sys.executable, "scripts/run_updating.py", "--interval", "0.1",
                           "--config", str(self.config), "--outbox-interval", "0.02"], cwd=checkout)
        blocked_id = self.start_blocked_run(proc)
        old_head = self.git(checkout, "rev-parse", "HEAD")

        major, minor, patch_number = __version__.split(".")
        next_version = f"{major}.{minor}.{int(patch_number) + 1}"
        version_file = origin / "src/agent_runtime/__init__.py"
        version_file.write_text(version_file.read_text().replace(__version__, next_version))
        self.git(origin, "commit", "-qam", "release")
        self.wait_for(lambda: "Draining daemon" in self.log_path.read_text(), proc)

        self.wait_for_drain(proc)
        status, during = self.post(OTHER, "during-drain")
        self.assertEqual(status, 202)
        self.assertEqual(self.event_status(during["event_id"]), "pending")
        self.assertEqual(self.event_status(blocked_id), "processing")
        self.assertEqual(self.git(checkout, "rev-parse", "HEAD"), old_head)
        self.assertNotIn(f"Hibernaut {next_version}", self.log_path.read_text())

        self.gate.touch()
        self.wait_for(lambda: f"Hibernaut {next_version}" in self.log_path.read_text(), proc)
        self.assertEqual(self.git(checkout, "rev-parse", "HEAD"), self.git(origin, "rev-parse", "HEAD"))
        self.assertEqual(self.event_status(blocked_id), "done")
        self.wait_for(lambda: self.event_status(during["event_id"]) == "done", proc)
        self.wait_for(lambda: self.outbox(OTHER) == [("during-drain by gen 1", "sent")], proc)
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=20)

    def test_a_second_signal_to_the_supervisor_reaches_the_draining_daemon(self):
        proc = self.spawn([sys.executable, str(run_updating.ROOT / "scripts/run_updating.py"), "--interval", "300",
                           "--wait-log-interval", "0.2", "--config", str(self.config), "--outbox-interval", "0.02"],
                          cwd=self.root)
        blocked_id = self.start_blocked_run(proc)
        [daemon_pid] = [pid for pid, command in processes_under(self.root) if "agent_runtime.app.main" in command]
        proc.send_signal(signal.SIGTERM)
        # The daemon stalls on the blocked Run; the supervisor keeps reporting who waits on whom, and for how long.
        waiting = f"Supervisor {proc.pid} still waiting for daemon {daemon_pid} to exit ("
        self.wait_for(lambda: self.log_path.read_text().count(waiting) >= 2, proc)
        self.assertIsNone(proc.poll())
        self.assertEqual(self.post(OTHER, "other-1")[0], 202)

        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=10)
        self.assertEqual(proc.returncode, 0, self.log_path.read_text())
        self.assertFalse(self.gate.exists())
        self.assertEqual(self.event_status(blocked_id), "pending")
        self.assertEqual(self.rows("SELECT COUNT(*) FROM worker_leases"), [(0,)])

    def test_sighup_to_the_supervisor_drains_and_stops_the_daemon(self):
        # The daemon runs in its own session, so a closed terminal's SIGHUP reaches only the supervisor.
        proc = self.spawn([sys.executable, str(run_updating.ROOT / "scripts/run_updating.py"), "--interval", "300",
                           "--config", str(self.config), "--outbox-interval", "0.02"], cwd=self.root)
        blocked_id = self.start_blocked_run(proc)
        proc.send_signal(signal.SIGHUP)
        self.wait_for_drain(proc)
        self.assertIsNone(proc.poll())

        self.gate.touch()
        proc.wait(timeout=20)
        self.assertEqual(proc.returncode, 0, self.log_path.read_text())
        self.assertEqual(self.event_status(blocked_id), "done")
        self.assertEqual(processes_under(self.root), [])


class TestCaseCleanupLeavesNoProcesses(unittest.TestCase):
    def run_case(self, case: GatedDaemonCase, failures: int) -> Path:
        result = unittest.TestResult()
        case.run(result)
        self.assertEqual((len(result.errors), len(result.failures)), (0, failures), result.errors + result.failures)
        return case.root

    def test_a_supervisor_case_that_fails_mid_run_leaves_no_process(self):
        class FailsMidRun(GatedDaemonCase):
            def fail_with_a_run_in_progress(self):
                proc = self.spawn([sys.executable, str(run_updating.ROOT / "scripts/run_updating.py"), "--interval",
                                   "300", "--config", str(self.config), "--outbox-interval", "0.02"], cwd=self.root)
                self.start_blocked_run(proc)
                self.fail("deliberate failure while the daemon runs")

        root = self.run_case(FailsMidRun("fail_with_a_run_in_progress"), failures=1)
        self.assertEqual(processes_under(root), [])

    def test_a_passing_supervisor_case_leaves_no_process(self):
        case = TestUpdateWaitsForDrain("test_a_second_signal_to_the_supervisor_reaches_the_draining_daemon")
        self.assertEqual(processes_under(self.run_case(case, failures=0)), [])


if __name__ == "__main__":
    unittest.main()
