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
import threading
import time
import unittest
import urllib.request
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from agent_runtime import __version__
from scripts import run_updating
from switchboard.app import build_switchboard
from switchboard.ports import RecordingAuditSink
from switchboard.relay_hub import RelayHub
from tests.support.process_cleanup import processes_under, stop_supervisor
from tests.support.switchboard_support import (
    MCP_TOKEN,
    RELAY_TOKEN,
    FakeHttpClient,
    StaticCredentials,
    make_config,
    mcp_call,
    tool_result,
)

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



class SupervisorFixture(unittest.TestCase):
    stop_grace = 5.0
    update_interval = 300.0

    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.checkout = self.root / "checkout"
        self.prepare_checkout()

    def prepare_checkout(self):
        # A shutdown test needs the real supervisor and daemon, but never publishes an update.
        scripts = self.checkout / "scripts"
        scripts.mkdir(parents=True)
        shutil.copyfile(SUPERVISOR, scripts / "run_updating.py")

    def start_supervisor(self, daemon_args: list[str] | None = None, env: dict[str, str] | None = None,
                         stderr: int | None = None) -> subprocess.Popen:
        self.port = getattr(self, "port", None) or free_port()
        self.db = self.root / "events.db"
        self.log_path = self.root / "daemon.log"
        log = self.enterContext(self.log_path.open("w"))
        # Absolute paths put the temp root on every command line (supervisor, each daemon generation), which is how
        # the cleanup finds them; it runs before the temp root is removed.
        proc = subprocess.Popen(
            [
                sys.executable,
                str(self.checkout / "scripts/run_updating.py"),
                "--interval", str(self.update_interval),
                "--outbox-interval", "0.02",
                *(daemon_args if daemon_args is not None else ["--port", str(self.port), "--db", str(self.db)]),
            ],
            stdout=log, stderr=stderr,
            cwd=self.checkout,
            env={**os.environ, **(env or {}), "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1",
                 "PYTHONPATH": str(run_updating.ROOT / "src") + os.pathsep + os.environ.get("PYTHONPATH", "")},
        )
        self.addCleanup(stop_supervisor, proc, self.root, self.stop_grace)
        return proc

    def send_until_reply(self, proc: subprocess.Popen, prefix: str) -> None:
        deadline = time.monotonic() + 45
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


class UpdateRepository(SupervisorFixture):
    def prepare_checkout(self):
        self.origin = self.root / "source"
        self.git(self.root, "init", "-b", "main", str(self.origin))
        self.populate_origin()
        self.commit(self.origin)
        self.git(self.root, "clone", str(self.origin), str(self.checkout))

    # maintenance.auto=false: the detached repack a commit may start races the local clone that follows it.
    def git(self, cwd, *args):
        return subprocess.check_output(["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "-c", "commit.gpgSign=false", "-c", "maintenance.auto=false", *args], cwd=cwd, text=True, stderr=subprocess.PIPE).strip()

    def commit(self, cwd):
        self.git(cwd, "add", ".")
        self.git(cwd, "commit", "-qm", "fixture")


class UpdateCandidateTests(UpdateRepository):
    def populate_origin(self):
        (self.origin / "tracked").touch()

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


class AutoUpdateTests(UpdateRepository):
    update_interval = 0.1

    def populate_origin(self):
        repo = run_updating.ROOT
        for folder in ("src", "scripts"):
            shutil.copytree(repo / folder, self.origin / folder, ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copyfile(repo / ".gitignore", self.origin / ".gitignore")

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

    def bump_origin_version(self) -> str:
        major, minor, patch_number = __version__.split(".")
        next_version = f"{major}.{minor}.{int(patch_number) + 1}"
        version_file = self.origin / "src/agent_runtime/__init__.py"
        version_file.write_text(version_file.read_text().replace(__version__, next_version))
        self.commit(self.origin)
        return next_version

    def start_relay_hub(self) -> RelayHub:
        sb_tmp = self.enterContext(tempfile.TemporaryDirectory())
        config = make_config(sb_tmp, relay={"enabled": True, "transport": "hybrid", "heartbeat_sec": 2},
                             delivery={"retry_backoff_seconds": [0.2, 0.2, 0.2, 0.2, 0.2]})
        board = build_switchboard(replace(config, http=replace(config.http, port=0)),
                                  credentials=StaticCredentials(), audit=RecordingAuditSink(),
                                  http_client=FakeHttpClient())
        board.start()
        self.addCleanup(board.stop)
        self.board = board
        assert board.relay is not None
        return board.relay

    def test_a_restart_queues_ingress_instead_of_refusing_it_and_the_relay_reconnects_at_once(self):
        hub = self.start_relay_hub()
        self.port = free_port()
        config = self.root / "hibernaut.toml"
        config.write_text(
            f'[runtime]\ndb_path = "{self.root / "events.db"}"\nmemory_root = "{self.root / "memory"}"\n'
            'default_executor = "demo"\n'
            f'[channels]\nhost = "127.0.0.1"\nport = {self.port}\n'
            '[switchboard]\nenabled = true\nmode = "relay"\nconsumer_id = "hibernaut"\n'
            f'relay_url = "{self.board.relay_url}"\nmcp_url = "{self.board.http.base_url}/mcp"\n'
        )

        # Online/offline transitions of the relay consumer, and one Switchboard event sent while it is offline.
        transitions: list[tuple[float, bool]] = []
        offline_event: list[str] = []
        watching = threading.Event()

        def watch() -> None:
            online = False
            while not watching.is_set():
                now_online = hub.is_online("hibernaut")
                if now_online != online:
                    online = now_online
                    transitions.append((time.monotonic(), online))
                    if not online and not offline_event:
                        status, response = mcp_call(self.board.http.base_url, "post_message", {
                            "correlation_key": "github:kun-g/hibernaut#7", "body": "sent while offline",
                            "scope": "proj:hibernaut", "channel": "github", "external_id": "offline-1",
                        })
                        offline_event.append(str(tool_result(response)["event_id"]) if status == 200 else repr(response))
                time.sleep(0.002)

        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        self.addCleanup(watcher.join)
        self.addCleanup(watching.set)

        stderr_path = self.root / "stderr.log"
        stderr = self.enterContext(stderr_path.open("w"))
        proc = self.start_supervisor(
            ["--config", str(config), "--demo", "--config-poll-interval", "0.5"],
            env={"SWITCHBOARD_RELAY_TOKEN": RELAY_TOKEN, "SWITCHBOARD_MCP_TOKEN": MCP_TOKEN},
            stderr=stderr.fileno(),
        )
        self.db = self.root / "events.db"
        self.send_until_reply(proc, "收到事件：")
        deadline = time.monotonic() + 45
        while not hub.is_online("hibernaut"):
            self.assertLess(time.monotonic(), deadline, stderr_path.read_text())
            time.sleep(0.02)

        # (sent, answered, status or error) for every request; each waits up to 60s, as a queued request would.
        results: list[tuple[float, float, object]] = []
        posting = threading.Event()

        def post_continuously() -> None:
            n = 0
            while not posting.is_set():
                n += 1
                body = {"external_id": f"restart-{n}", "payload": {"text": f"restart-{n}"}}
                req = urllib.request.Request(f"http://127.0.0.1:{self.port}/events", data=json.dumps(body).encode(),
                                             headers={"Content-Type": "application/json"})
                sent = time.monotonic()
                try:
                    with urllib.request.urlopen(req, timeout=60) as response:
                        outcome: object = response.status
                except (OSError, http.client.HTTPException) as exc:
                    outcome = repr(exc)
                results.append((sent, time.monotonic(), outcome))
                time.sleep(0.01)

        poster = threading.Thread(target=post_continuously, daemon=True)
        poster.start()
        self.addCleanup(poster.join)
        self.addCleanup(posting.set)

        next_version = self.bump_origin_version()
        deadline = time.monotonic() + 45
        while not (f"Hibernaut {next_version}" in self.log_path.read_text()
                   and len(transitions) >= 3 and transitions[-1][1]
                   and results and results[-1][0] > transitions[-1][0]):
            self.assertLess(time.monotonic(), deadline, stderr_path.read_text())
            self.assertIsNone(proc.poll(), stderr_path.read_text())
            time.sleep(0.05)
        posting.set()
        poster.join(60)

        log = stderr_path.read_text()
        self.assertEqual([outcome for _, _, outcome in results if outcome != 202], [], log)
        waits = [answered - sent for sent, answered, _ in results]
        offline = [(transitions[i + 1][0] - at) for i, (at, online) in enumerate(transitions[:-1]) if not online]
        print(f"\n[restart] {len(results)} requests, longest wait {max(waits):.3f}s; "
              f"relay offline {', '.join(f'{s:.3f}s' for s in offline)}", file=sys.stderr)
        # Proves the requests actually spanned the handover rather than all landing on one daemon.
        self.assertGreater(max(waits), 0.1)
        self.assertEqual(log.count("Serving HTTP on the listener inherited"), 1, log)
        self.assertEqual(len(offline), 1, transitions)
        self.assertNotIn("connect to", log)

        external_ids = {f"restart-{n}" for n in range(1, len(results) + 1)}
        deadline = time.monotonic() + 45
        while True:
            with contextlib.closing(sqlite3.connect(self.db)) as conn:
                stored = {row[0] for row in conn.execute("SELECT external_id FROM event_inbox WHERE source = 'webhook'")}
                relayed = conn.execute("SELECT 1 FROM event_inbox WHERE source = 'sb:mcp' AND external_id = ?",
                                       (offline_event[0] if offline_event else "",)).fetchone()
            if external_ids <= stored and relayed:
                break
            self.assertLess(time.monotonic(), deadline, f"missing {sorted(external_ids - stored)}; "
                                                        f"offline event {offline_event} relayed={relayed}\n{log}")
            time.sleep(0.05)


class AutoUpdateCleanupTests(unittest.TestCase):
    def run_case(self, case: SupervisorFixture) -> Path:
        result = unittest.TestResult()
        case.run(result)
        self.assertEqual((len(result.errors), len(result.failures)), (0, 1), result.errors + result.failures)
        return case.root

    def test_a_case_that_fails_mid_run_leaves_no_process(self):
        class FailsMidRun(SupervisorFixture):
            def fail_after_the_first_reply(self):
                self.send_until_reply(self.start_supervisor(), "收到事件：")
                self.fail("deliberate failure while the supervisor runs")

        self.assertEqual(processes_under(self.run_case(FailsMidRun("fail_after_the_first_reply"))), [])

    def test_a_case_whose_daemon_stalls_past_the_stop_grace_leaves_no_process(self):
        class DaemonStalls(SupervisorFixture):
            stop_grace = 0.1

            def stall_the_daemon_then_fail(self):
                self.send_until_reply(self.start_supervisor(), "收到事件：")
                for pid, command in processes_under(self.root):
                    if "agent_runtime.app.main" in command:
                        os.kill(pid, signal.SIGSTOP)
                self.fail("deliberate failure with the daemon stopped")

        self.assertEqual(processes_under(self.run_case(DaemonStalls("stall_the_daemon_then_fail"))), [])
