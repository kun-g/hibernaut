import asyncio
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
from pathlib import Path

from agent_runtime import __version__
from agent_runtime.adapters.channels import WebhookHTTPServer
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteOutboxAdapter
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit
from scripts import run_updating
from tests.test_reload_handover import GATED_CLI

BLOCKED = "proj:blocked"
OTHER = "proj:other"
SRC = run_updating.ROOT / "src"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def post(port: int, inbox_id: str, text: str, workdir: Path, external_id: str | None = None):
    body = {"inbox_id": inbox_id, "external_id": external_id or f"{text}-{time.monotonic_ns()}",
            "payload": {"text": text, "explicit_workdir": str(workdir)}}
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
        response = conn.getresponse()
        return response.status, json.load(response)
    except OSError:
        return None, None
    finally:
        conn.close()


def processes_under(root: Path) -> list[tuple[int, str]]:
    listing = subprocess.run(["ps", "-A", "-ww", "-o", "pid=", "-o", "command="], stdout=subprocess.PIPE, text=True,
                             check=True).stdout
    found = []
    for line in listing.splitlines():
        pid, _, command = line.strip().partition(" ")
        if str(root) in command and int(pid) != os.getpid():
            found.append((int(pid), command))
    return found


def kill_processes_under(root: Path, timeout: float = 10.0) -> None:
    # run_updating.py starts the daemon in its own session, so neither killing the supervisor nor its process group
    # reaches it; every process this case started (supervisor, daemon, gated CLI) names the case's temp root instead.
    deadline = time.monotonic() + timeout
    while found := processes_under(root):
        for pid, _ in found:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
        if time.monotonic() > deadline:
            raise AssertionError(f"processes still running under {root}: {found}")
        time.sleep(0.05)


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
        time.sleep(0.3)
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
        time.sleep(0.3)
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
        time.sleep(0.3)
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
    def git(self, cwd, *args):
        return subprocess.check_output(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgSign=false",
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

        time.sleep(0.5)
        status, during = self.post(OTHER, "during-drain")
        self.assertEqual(status, 202)
        time.sleep(0.3)
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
                           "--config", str(self.config), "--outbox-interval", "0.02"], cwd=self.root)
        blocked_id = self.start_blocked_run(proc)
        proc.send_signal(signal.SIGTERM)
        time.sleep(0.5)
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
        time.sleep(0.5)
        self.assertIsNone(proc.poll())

        self.gate.touch()
        proc.wait(timeout=20)
        self.assertEqual(proc.returncode, 0, self.log_path.read_text())
        self.assertEqual(self.event_status(blocked_id), "done")
        self.assertEqual(processes_under(self.root), [])


class _GatedExecutor:
    executor_id = "gated"

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.started: list[str] = []

    async def execute(self, events, session, caps):
        text = events[-1].payload["text"]
        self.started.append(text)
        if text.startswith("block"):
            await self.release.wait()
        return Emit(messages=(f"{text} done",))


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


class TestDaemonRunDrains(unittest.IsolatedAsyncioTestCase):
    # AlwaysOnDaemon.run, used without --config, drains the same way.
    policy = "defer"

    async def asyncSetUp(self):
        self.executor = _GatedExecutor()
        self.daemon = build_daemon(db_path=":memory:", executor_factory=lambda _inbox: self.executor,
                                   new_event_policy=self.policy)
        self.addCleanup(self.daemon.close)
        self.port = free_port()
        self.workdir = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.stop = asyncio.Event()
        self.force = asyncio.Event()
        self.serving = asyncio.create_task(self.daemon.run(
            stop_event=self.stop, force_stop_event=self.force, webhook_host="127.0.0.1", webhook_port=self.port,
            timer_interval_seconds=0.05, outbox_interval_seconds=0.02,
        ))

    async def asyncTearDown(self):
        self.executor.release.set()
        self.stop.set()
        await asyncio.wait_for(self.serving, timeout=10)

    async def post(self, inbox_id: str, text: str):
        return await asyncio.to_thread(post, self.port, inbox_id, text, self.workdir)

    async def wait_for(self, check, timeout: float = 10.0):
        deadline = time.monotonic() + timeout
        while not check():
            self.assertLess(time.monotonic(), deadline)
            await asyncio.sleep(0.02)

    def status(self, event_id: str) -> str:
        return self.daemon.db.fetchall("SELECT status FROM event_inbox WHERE event_id = ?", (event_id,))[0]["status"]

    def outbox(self, inbox_id: str) -> list[tuple[str, str]]:
        rows = self.daemon.db.fetchall("SELECT content, status FROM outbox WHERE inbox_id = ? ORDER BY seq", (inbox_id,))
        return [(row["content"], row["status"]) for row in rows]

    async def start_blocked(self) -> str:
        while (await self.post(BLOCKED, "block-1"))[0] != 202:
            await asyncio.sleep(0.02)
        await self.wait_for(lambda: "block-1" in self.executor.started)
        return self.daemon.inbox.list_events(inbox_id=BLOCKED)[0].event_id

    async def test_stop_waits_for_the_run_and_keeps_ingress_and_delivery(self):
        blocked_id = await self.start_blocked()
        self.stop.set()
        await asyncio.sleep(0.2)
        status, other = await self.post(OTHER, "other-1")
        self.assertEqual(status, 202)
        self.daemon.outbox.enqueue(OTHER, "notice")
        await self.wait_for(lambda: self.outbox(OTHER) == [("notice", "sent")])
        await asyncio.sleep(0.2)
        self.assertFalse(self.serving.done())
        self.assertEqual(self.status(other["event_id"]), "pending")
        self.assertNotIn("other-1", self.executor.started)

        self.executor.release.set()
        await asyncio.wait_for(self.serving, timeout=10)
        self.assertEqual(self.status(blocked_id), "done")
        self.assertEqual(self.status(other["event_id"]), "pending")
        self.assertEqual((await self.post(OTHER, "after"))[0], None)

    async def test_force_stop_cancels_the_run_and_leaves_it_pending(self):
        blocked_id = await self.start_blocked()
        self.stop.set()
        await asyncio.sleep(0.2)
        self.assertFalse(self.serving.done())
        self.force.set()
        await asyncio.wait_for(self.serving, timeout=5)
        self.assertEqual(self.status(blocked_id), "pending")
        self.assertEqual(self.outbox(BLOCKED), [])


class TestDrainUnderInterruptPolicy(TestDaemonRunDrains):
    policy = "interrupt"

    async def test_an_event_for_the_draining_inbox_does_not_interrupt_its_run(self):
        blocked_id = await self.start_blocked()
        self.stop.set()
        await asyncio.sleep(0.2)
        status, follow_up = await self.post(BLOCKED, "follow-up")
        self.assertEqual(status, 202)
        await asyncio.sleep(0.3)
        self.assertEqual(self.executor.started, ["block-1"])
        self.assertEqual(self.status(blocked_id), "processing")

        self.executor.release.set()
        await asyncio.wait_for(self.serving, timeout=10)
        self.assertEqual(self.status(blocked_id), "done")
        self.assertEqual(self.outbox(BLOCKED)[-1][0], "block-1 done")
        self.assertEqual(self.status(follow_up["event_id"]), "pending")
        self.assertEqual(self.executor.started, ["block-1"])


if __name__ == "__main__":
    unittest.main()
