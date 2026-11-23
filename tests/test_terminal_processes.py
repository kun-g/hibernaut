import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import cli_terminal
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import (
    CLI_TERMINAL_SCRIPT,
    OrcaCommandTerminal,
    OrcaVisibleRunner,
    TerminalRunFailed,
    _close_quietly,  # pyright: ignore[reportPrivateUsage]
    close_terminals,
    session_gate,
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore, TerminalBinding
from tests.test_coder_reviewer import fake_command
from tests.test_visible_roles import FakeOrcaTerminals

# Processes started in a terminal must all end once the terminal is closed or its run is over: Orca can report a close
# that never reaches them, so the wrapper scripts end them on their own.


# What Orca sets in a terminal for agent hooks to report the session; a one-shot run must not have any of it.
ORCA_SESSION_ENV = {name: f"value of {name}" for name in (
    "ORCA_AGENT_HOOK_ENDPOINT", "ORCA_AGENT_HOOK_ENV", "ORCA_AGENT_HOOK_PORT", "ORCA_AGENT_HOOK_TOKEN",
    "ORCA_AGENT_HOOK_TRANSPORT", "ORCA_AGENT_HOOK_VERSION", "ORCA_AGENT_LAUNCH", "ORCA_AGENT_LAUNCH_TOKEN",
    "ORCA_AGENT_PANE", "ORCA_PANE_KEY", "ORCA_TAB_ID",
)}
OTHER_ENV = {"ORCA_WORKTREE_ID": "wt-1", "ORCA_TERMINAL_HANDLE": "term_1", "ORCA_TAB_ID_OF_OTHERS": "t",
             "ORCA_PANE_KEYS": "k", "MY_ORCA_PANE_KEY": "mine", "REVIEW_MARK": "kept"}


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def running(pid: int) -> bool:
    # A killed orphan stays a zombie until init reaps it, and a zombie no longer runs anything.
    stat = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return bool(stat) and not stat.startswith("Z")


async def until(condition, seconds: float = 30.0) -> bool:
    # Less than the 60s the processes under test sleep, so one left behind cannot end by itself in time.
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if condition():
            return True
        await asyncio.sleep(0.05)
    return condition()


class Case(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.repo = self.root / "repo"
        self.repo.mkdir()
        self.pids = self.root / "pids"
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))

    async def asyncTearDown(self):
        await self.orca.close_all()
        for pid in self.recorded():
            if alive(pid):
                os.kill(pid, signal.SIGKILL)

    def recorded(self) -> list[int]:
        return [int(p) for p in self.pids.read_text().split()] if self.pids.exists() else []

    async def started(self, count: int) -> list[int]:
        # The processes can be up before `terminal create` has returned to the runner.
        self.assertTrue(await until(lambda: self.orca.terminals and len(self.recorded()) == count), "the CLI did not start")
        return self.recorded()

    async def all_gone(self, pids):
        self.assertTrue(await until(lambda: not any(alive(p) for p in pids)),
                        f"still running: {[p for p in pids if alive(p)]}")

    def busy_cli(self) -> list[str]:
        # The CLI and a child of its own, both recorded, still running when the terminal closes.
        return ["/bin/sh", "-c", f'echo $$ > "{self.pids}"; sleep 60 & echo $! >> "{self.pids}"; wait']


class TestCLITerminalProcesses(Case):
    def setUp(self):
        super().setUp()
        self.terminal = OrcaCommandTerminal(root=self.root / "runs", poll_interval=0.02, liveness_interval=0.2)
        self.interrupted = False

    def run_cli(self, argv, timeout=20.0):
        return self.terminal.run(title="grok (code-reviewer) · review", workdir=self.repo, argv=argv, env={},
                                 timeout_seconds=timeout, is_interrupted=lambda: self.interrupted)

    def wrapper(self):
        (handle,) = self.orca.terminals
        return handle, self.orca.terminals[handle]["proc"]

    async def test_a_finished_cli_ends_its_terminal_and_leaves_nothing_behind(self):
        # A child left behind by the CLI keeps its output pipe open; it must not keep the run or the terminal going.
        run = await self.run_cli(["/bin/sh", "-c", f'echo verdict; sleep 60 & echo $! > "{self.pids}"'])
        self.assertEqual((run.exit_code, run.stdout), (0, b"verdict\n"))
        handle, wrapper = self.wrapper()
        # The close can reach the wrapper after it wrote the result but before it exited.
        self.assertIn(await asyncio.wait_for(wrapper.wait(), 8), (0, 128 + signal.SIGTERM))
        await self.all_gone(self.recorded())
        self.assertIn(("terminal", "close", "--terminal", handle), self.orca.calls)
        # Delivery closes it once more; a terminal that already ended counts as closed.
        self.assertEqual(await close_terminals([handle], self.repo), f"closed {handle}")

    async def test_the_cli_does_not_get_the_variables_orca_tracks_agent_sessions_by(self):
        # With them the CLI's hooks report its session to Orca, which resumes it in a new tab once the run is over.
        script = "import json, os; print(json.dumps(dict(os.environ)))"
        with patch.dict(os.environ, {**ORCA_SESSION_ENV, **OTHER_ENV}):
            run = await self.terminal.run(
                title="t", workdir=self.repo, argv=[sys.executable, "-I", "-c", script],
                env={"FROM_TEMPLATE": "yes", "ORCA_PANE_KEY": "from the template"}, timeout_seconds=20.0,
                is_interrupted=lambda: False)
            terminal_env = dict(os.environ)
        env = json.loads(run.stdout)
        self.assertEqual(set(ORCA_SESSION_ENV) & set(env), set())
        self.assertEqual((env["FROM_TEMPLATE"], {k: env.get(k) for k in OTHER_ENV}), ("yes", OTHER_ENV))
        # Everything else arrives as it was. Only names are compared in the message: the values are the machine's own,
        # and the shell that starts the wrapper sets a few itself.
        dropped = {k for k in terminal_env if k.startswith(("ORCA_AGENT_HOOK_", "ORCA_AGENT_LAUNCH"))}
        self.assertEqual(sorted(k for k, v in terminal_env.items() if env.get(k) != v and k not in dropped
                                and k not in ("ORCA_AGENT_PANE", "ORCA_PANE_KEY", "ORCA_TAB_ID")
                                and k not in ("PWD", "OLDPWD", "SHLVL", "_")), [])

    async def test_closing_the_terminal_while_the_cli_runs_ends_the_cli_and_its_children(self):
        task = asyncio.create_task(self.run_cli(self.busy_cli()))
        pids = await self.started(2)
        handle, wrapper = self.wrapper()
        # The fake signals only the wrapper (SIGTERM); the wrapper ends the CLI's process group itself.
        await self.orca("terminal", "close", "--terminal", handle, cwd=self.repo)
        self.assertEqual(wrapper.returncode, 128 + signal.SIGTERM)
        await self.all_gone(pids)
        with self.assertRaisesRegex(TerminalRunFailed, f"Orca terminal {handle} was closed before"):
            await task

    async def test_an_interrupted_run_stops_the_cli_when_orca_loses_the_close(self):
        self.orca.lose_closes = True
        task = asyncio.create_task(self.run_cli(self.busy_cli()))
        pids = await self.started(2)
        self.interrupted = True
        with self.assertRaises(asyncio.CancelledError):
            await task
        handle, wrapper = self.wrapper()
        self.assertIn(("terminal", "close", "--terminal", handle), self.orca.calls)
        # Nothing was signalled: the wrapper sees that Hibernaut stopped waiting and ends the CLI.
        self.assertEqual(await asyncio.wait_for(wrapper.wait(), 8), 1)
        await self.all_gone(pids)

    async def test_input_reaches_the_cli(self):
        text = "a prompt, é\n" * 30_000
        run = await self.terminal.run(title="t", workdir=self.repo, argv=["/bin/cat"], env={}, timeout_seconds=20,
                                      is_interrupted=lambda: False, stdin=text)
        self.assertEqual((run.exit_code, run.stdout.decode()), (0, text))
        handle, wrapper = self.wrapper()
        self.assertIn(("terminal", "close", "--terminal", handle), self.orca.calls)
        self.assertIsNotNone(wrapper.returncode)
        self.assertEqual(list((self.root / "runs").iterdir()), [])


class TestCLITerminalScript(unittest.IsolatedAsyncioTestCase):
    # The wrapper on its own, without Hibernaut watching: its deadline and the signals of a closing terminal.
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.pids = self.root / "pids"

    def start(self, timeout_seconds=60.0, script=None, stdout=subprocess.DEVNULL):
        script = script or f'echo $$ > "{self.pids}"; sleep 60 & echo $! >> "{self.pids}"; wait'
        spec = {"title": "t", "cwd": str(self.root), "env": {}, "timeout_seconds": timeout_seconds,
                "argv": ["/bin/sh", "-c", script],
                **{name: str(self.root / name) for name in ("stdout", "stderr", "exit.json")}}
        (self.root / "spec.json").write_text(json.dumps(spec))
        proc = subprocess.Popen([sys.executable, "-I", str(CLI_TERMINAL_SCRIPT), str(self.root / "spec.json")],
                                stdin=subprocess.PIPE, stdout=stdout, stderr=subprocess.DEVNULL)
        self.addCleanup(lambda: proc.poll() is None and proc.kill())
        return proc

    async def pids_of_cli(self):
        self.assertTrue(await until(lambda: self.pids.exists() and len(self.pids.read_text().split()) == 2))
        pids = [int(p) for p in self.pids.read_text().split()]
        self.addCleanup(lambda: [os.kill(p, signal.SIGKILL) for p in pids if alive(p)])
        return pids

    async def test_the_wrapper_stops_the_cli_at_its_own_deadline(self):
        proc = self.start(timeout_seconds=0.5)
        pids = await self.pids_of_cli()
        self.assertTrue(await until(lambda: proc.poll() is not None))
        self.assertEqual(proc.returncode, 1)
        self.assertTrue(await until(lambda: not any(alive(p) for p in pids)))
        self.assertFalse((self.root / "exit.json").exists())

    async def test_a_hangup_or_terminate_ends_the_wrapper_and_the_cli(self):
        for sig in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            with self.subTest(sig.name):
                self.pids.unlink(missing_ok=True)
                proc = self.start()
                pids = await self.pids_of_cli()
                proc.send_signal(sig)
                self.assertTrue(await until(lambda: proc.poll() is not None))
                self.assertEqual(proc.returncode, 128 + sig)
                # The wrapper waits for the whole group to end before it exits.
                self.assertEqual([p for p in pids if running(p)], [])

    async def test_the_exit_code_names_the_signal_when_the_terminal_is_already_gone(self):
        # Output the dead terminal refused must not turn the exit code into Python's own at shutdown.
        go = self.root / "go"
        os.mkfifo(go)
        proc = self.start(script=f'echo up; cat < "{go}"; sleep 60', stdout=subprocess.PIPE)
        assert proc.stdout is not None
        self.assertEqual(os.read(proc.stdout.fileno(), 5), b"[hibe")
        copy = self.root / "stdout"
        self.assertTrue(await until(lambda: copy.exists() and copy.read_bytes() == b"up\n"))
        proc.stdout.close()
        with go.open("wb", buffering=0) as cli:
            for line in (b"one\n", b"two\n"):
                # A second chunk in the copy means that the first one was offered to the screen.
                cli.write(line)
                self.assertTrue(await until(lambda line=line: copy.read_bytes().endswith(line)))
        proc.send_signal(signal.SIGHUP)
        self.assertTrue(await until(lambda: proc.poll() is not None))
        self.assertEqual(proc.returncode, 128 + signal.SIGHUP)


class TestSessionCheckBeforeACommand(Case):
    # A one-off Claude Code command: the gate checks the session first and then becomes the command, so the wrapper
    # ends it like any other.
    def setUp(self):
        super().setUp()
        self.terminal = OrcaCommandTerminal(root=self.root / "runs", poll_interval=0.02, liveness_interval=0.2)
        self.interrupted = False

    def run_checked(self, argv, env):
        profile = ClaudeCodeProfile(name="steward", env=env,
                                    launcher=ClaudeCodeLauncher(name="fake", command=fake_command(self.root)))
        return self.terminal.run(title="t", workdir=self.repo, argv=argv, env={}, timeout_seconds=20,
                                 is_interrupted=lambda: self.interrupted, gate=session_gate(profile))

    async def test_stopping_during_the_session_check_ends_the_probe(self):
        for lose in (False, True):
            with self.subTest(lose_closes=lose):
                self.pids.unlink(missing_ok=True)
                self.orca.lose_closes, self.interrupted = lose, False
                task = asyncio.create_task(self.run_checked(["/bin/echo", "ran"], {"FAKE_PROBE_PIDFILE": str(self.pids)}))
                pids = await self.started(2)
                self.interrupted = True
                with self.assertRaises(asyncio.CancelledError):
                    await task
                await self.all_gone(pids)

    async def test_the_command_takes_the_gates_place_and_ends_with_its_terminal(self):
        task = asyncio.create_task(self.run_checked(self.busy_cli(), {}))
        pids = await self.started(2)
        self.interrupted = True
        with self.assertRaises(asyncio.CancelledError):
            await task
        await self.all_gone(pids)

    async def test_a_failed_check_never_starts_the_command(self):
        run = await self.run_checked(["/bin/sh", "-c", f"touch {self.root}/ran"],
                                     {"FAKE_API_KEY_SOURCE": "ANTHROPIC_API_KEY"})
        self.assertNotEqual(run.exit_code, 0)
        self.assertIn(b"billing is 'subscription'", run.stderr)
        self.assertEqual(run.stdout, b"")
        self.assertFalse((self.root / "ran").exists())

    async def test_configured_variables_orca_tracks_sessions_by_do_not_come_back(self):
        # The gate applies the configured environment after the wrapper removed Orca's variables.
        script = "import json, os; print(json.dumps(dict(os.environ)))"
        with patch.dict(os.environ, ORCA_SESSION_ENV):
            run = await self.run_checked([sys.executable, "-I", "-c", script], {**ORCA_SESSION_ENV, **OTHER_ENV})
        env = json.loads(run.stdout)
        self.assertEqual(set(ORCA_SESSION_ENV) & set(env), set())
        self.assertEqual({k: env.get(k) for k in OTHER_ENV}, OTHER_ENV)

    async def test_only_the_commands_own_output_is_returned(self):
        run = await self.run_checked(["/bin/sh", "-c", "echo out; echo err >&2; exit 5"], {})
        self.assertEqual((run.exit_code, run.stdout, run.stderr), (5, b"out\n", b"err\n"))


# Runs one wrapper script with stop signals delivered at one exact point. The CLI reports its pids through a FIFO, which
# the script reads at that point, so it goes on only once the CLI and its child really run.
SIGNALLED_WRAPPER = """
import os, signal, subprocess, sys, threading
from pathlib import Path
from agent_runtime.adapters.executor_cli import claude_gate, cli_terminal

start, window, root = sys.argv[1], sys.argv[2], Path(sys.argv[3])
argv = ["/bin/sh", "-c", f'sleep 60 & echo "$$ $!" > "{root / "started"}"; wait']
if start == "once":
    # cli_terminal.py kills a one-off gate after its own grace, so the gate must spend none on a probe that ignores TERM.
    argv[2] = "trap '' TERM; " + argv[2]
    cli_terminal.GRACE_SECONDS = 3600
lease = root / "lease"
lease.touch()
pids = []

def cli_started():
    pids.append((root / "started").read_text())
    (root / "pids").write_text(pids[0])

def stop(*signals):
    # All pending at once for the main thread, so the wrapper takes them one right after the other.
    signal.pthread_sigmask(signal.SIG_BLOCK, signals)
    for sig in signals:
        signal.pthread_kill(threading.main_thread().ident, sig)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, signals)

real_popen, real_read, real_exited = subprocess.Popen, os.read, cli_terminal.exited

def read(fd, size):
    # Popen reads the pipe on which the forked child reports a failed exec.
    os.read = real_read
    cli_started()
    stop(signal.SIGHUP)
    return real_read(fd, size)

def popen(*args, **kwargs):
    if window == "before the fork":
        stop(signal.SIGHUP)
    if window == "inside Popen":
        os.read = read
    proc = real_popen(*args, **kwargs)
    if not pids:
        cli_started()
    if window == "before Popen returns":
        stop(signal.SIGHUP)
    if window == "as the cleanup starts":
        lease.unlink()
        sys.setprofile(after_the_body)
    return proc

body = {"cli": "_run", "probe": "_probe", "once": "_probe", "agent": "_supervise"}[start]
ended = False

def after_the_body(frame, event, arg):
    # The first function called once the body has ended for another reason (deadline, lease).
    global ended
    if event == "return" and frame.f_code.co_name == body:
        ended = True
    elif event == "call" and ended:
        sys.setprofile(None)
        stop(signal.SIGHUP)

class Lost:
    # Python drops an exception raised in a finalizer, so this stop never reaches the code it interrupted.
    def __del__(self):
        stop(signal.SIGHUP)

watched = False

def exited(pid, seconds):
    global watched
    if not watched:
        watched = True
        if window == "two signals at once":
            stop(signal.SIGHUP, signal.SIGTERM)
        if window == "in a finalizer":
            Lost()
    return real_exited(pid, seconds)

subprocess.Popen = popen
cli_terminal.exited = claude_gate.exited = exited
real_run = cli_terminal._run

def run(spec, running):
    if window == "before the start":
        stop(signal.SIGHUP)
    if start == "cli":
        return real_run(spec, running)
    if start in ("probe", "once"):
        return claude_gate._probe(argv, dict(os.environ), 30.0, lease, running)
    return claude_gate._supervise(argv, dict(os.environ), lease, running)

cli_terminal._run = claude_gate._main = run
os._exit((cli_terminal if start == "cli" else claude_gate).main(str(root / "spec.json")))
"""


class TestStopAtEveryStage(Case):
    def setUp(self):
        super().setUp()
        os.mkfifo(self.root / "started")

    async def stopped(self, start: str, window: str) -> list[int]:
        self.pids.unlink(missing_ok=True)
        # The CLI reaches its deadline at once where the wrapper is to end it by itself.
        spec = {"title": "t", "cwd": str(self.root), "env": {}, "timeout_seconds": 0 if "cleanup" in window else 30.0,
                "argv": ["/bin/sh", "-c", f'sleep 60 & echo "$$ $!" > "{self.root / "started"}"; wait'],
                "once": start == "once", **{name: str(self.root / name) for name in ("stdout", "stderr", "exit.json")}}
        (self.root / "spec.json").write_text(json.dumps(spec))
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", SIGNALLED_WRAPPER, start, window, str(self.root),
            env={**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, sys.path))},
            stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL)
        self.addCleanup(lambda: proc.returncode is None and proc.kill())
        code = await asyncio.wait_for(proc.wait(), 60)
        pids = self.recorded()
        self.addCleanup(lambda: [os.kill(p, signal.SIGKILL) for p in pids if alive(p)])
        self.assertEqual([p for p in pids if running(p)], [], "left running")
        self.assertEqual(code, 128 + signal.SIGHUP)
        self.assertFalse((self.root / "exit.json").exists())
        return pids

    async def each_start(self, window: str, started: int = 2):
        for start in ("cli", "probe", "once", "agent"):
            with self.subTest(start):
                self.assertEqual(len(await self.stopped(start, window)), started)

    async def test_a_signal_before_the_start_starts_nothing(self):
        await self.each_start("before the start", started=0)

    async def test_a_signal_before_the_fork_ends_the_process_it_starts(self):
        await self.each_start("before the fork")

    async def test_a_signal_while_the_started_process_is_not_known_yet_ends_it(self):
        await self.each_start("inside Popen")

    async def test_a_signal_before_the_process_is_recorded_ends_it(self):
        await self.each_start("before Popen returns")

    async def test_a_second_signal_does_not_cut_the_cleanup_short(self):
        await self.each_start("two signals at once")

    async def test_a_signal_as_another_way_out_starts_its_cleanup_does_not_skip_it(self):
        await self.each_start("as the cleanup starts")

    async def test_a_stop_lost_in_a_finalizer_still_stops_the_wrapper(self):
        await self.each_start("in a finalizer")


class TestCLIGroupSignals(unittest.TestCase):
    # The CLI's process group is named by its leader's pid. Once the leader is reaped that pid can name an unrelated
    # group, so every signal to the group must go out while the leader is still unreaped (running or a zombie).
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.sent: list[tuple[int, int, bool]] = []
        # SIGKILLs to lose, as when a member forks while its group is being signalled.
        self.missed: list[bool] = []
        real = os.killpg

        def record(pgid, sig):
            try:
                os.waitid(os.P_PID, pgid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                reaped = False
            except ChildProcessError:
                reaped = True
            self.sent.append((pgid, sig, reaped))
            return None if self.missed and sig == signal.SIGKILL and self.missed.pop() else real(pgid, sig)

        self.enterContext(patch("os.killpg", new=record))

    def popen(self, script):
        # Returns once the script has written a line to $READY, with that line.
        ready = self.root / "ready"
        os.mkfifo(ready)
        proc = subprocess.Popen(["/bin/sh", "-c", script], start_new_session=True,
                                env={**os.environ, "READY": str(ready)})
        self.addCleanup(lambda: proc.poll() is None and proc.kill())
        return proc, ready.read_text().strip()

    def sent_signals(self):
        return [s[1] for s in self.sent if s[1] != 0]

    def assert_never_after_reaping(self):
        self.assertTrue(self.sent)
        self.assertEqual([s for s in self.sent if s[2]], [], f"signalled a reaped group: {self.sent}")

    def test_a_cli_that_exits_on_its_own_is_never_signalled_after_it_is_reaped(self):
        spec = {"title": "t", "cwd": str(self.root), "env": {}, "timeout_seconds": 30, "argv": ["/bin/sh", "-c", "exit 3"],
                **{name: str(self.root / name) for name in ("stdout", "stderr", "exit.json")}}
        (self.root / "spec.json").write_text(json.dumps(spec))
        running: list = []
        with patch.object(sys, "stdout", open(os.devnull, "w")), patch.object(sys, "stderr", open(os.devnull, "w")):
            self.assertEqual(cli_terminal._run(self.root / "spec.json", running), 0)  # pyright: ignore[reportPrivateUsage]
        self.assertEqual(json.loads((self.root / "exit.json").read_text()), {"exit": 3})
        self.assert_never_after_reaping()

    def test_a_stopped_cli_that_ends_within_the_grace_is_not_signalled_after_it_is_reaped(self):
        proc, _ = self.popen('echo > "$READY"; exec sleep 60')
        cli_terminal.end_group(proc)
        self.assertEqual(proc.returncode, -signal.SIGTERM)
        # SIGKILL still goes out for anything left in the group, but while the leader is an unreaped zombie.
        self.assertEqual(self.sent_signals(), [signal.SIGTERM, signal.SIGKILL])
        self.assert_never_after_reaping()

    def test_a_cli_that_ignores_sigterm_is_killed_while_it_is_still_running(self):
        proc, _ = self.popen("trap '' TERM; sleep 60 & echo > \"$READY\"; wait; sleep 60")
        cli_terminal.end_group(proc, grace=0.3)
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        self.assertEqual(self.sent_signals()[0], signal.SIGTERM)
        self.assertEqual(set(self.sent_signals()[1:]), {signal.SIGKILL})
        self.assert_never_after_reaping()

    def test_a_member_that_outlives_the_leader_and_the_first_sigkill_is_killed_too(self):
        proc, child = self.popen("sh -c 'trap \"\" TERM; echo $$ > \"$READY\"; exec sleep 60' & wait")
        self.addCleanup(lambda: alive(int(child)) and os.kill(int(child), signal.SIGKILL))
        self.missed = [True]
        cli_terminal.end_group(proc, grace=0)
        self.assertFalse(running(int(child)))
        self.assert_never_after_reaping()

    def test_a_group_of_only_zombies_has_ended_where_the_system_still_accepts_signals_to_it(self):
        # Linux accepts them, so the wrapper reads /proc, here one made up for group 70.
        def member(pid, state, pgrp, threads=1):
            (self.root / str(pid)).mkdir()
            (self.root / str(pid) / "stat").write_text(
                f"{pid} (a) name)) {state} 1 {pgrp} 70 {' '.join(['0'] * 13)} {threads} 0 0 0 0\n")

        def is_running():
            with patch("os.killpg"), patch.object(cli_terminal, "PROC", self.root):
                return cli_terminal._group_running(70)  # pyright: ignore[reportPrivateUsage]

        self.assertTrue(is_running())  # nothing known about the group: the signal decides
        member(70, "Z", 70)
        member(71, "Z", 70)
        member(80, "S", 80)
        self.assertFalse(is_running())
        member(72, "Z", 70, threads=2)
        self.assertTrue(is_running())
        (self.root / "72" / "stat").unlink()
        member(73, "S", 70)
        self.assertTrue(is_running())

    def test_the_rest_of_the_group_has_the_grace_to_end_after_its_leader_ended(self):
        # The leader is gone with the first SIGTERM; its child ends by itself, only later.
        done = self.root / "done"
        proc, child = self.popen(
            f"sh -c 'trap \"sleep 0.2; echo cleaned > {done}; exit\" TERM; echo $$ > \"$READY\"; "
            "while :; do sleep 60 & wait; done' & wait")
        self.addCleanup(lambda: alive(int(child)) and os.kill(int(child), signal.SIGKILL))
        cli_terminal.end_group(proc, grace=30)
        self.assertEqual(proc.returncode, -signal.SIGTERM)
        self.assertEqual(done.read_text() if done.exists() else "killed", "cleaned\n")
        self.assert_never_after_reaping()


class TestGateProcesses(Case):
    # claude_gate.py runs the session check and then the interactive agent as children it supervises, so a close
    # that reaches it, or one that Orca loses (Hibernaut drops the terminal's lease either way), ends them all.
    def setUp(self):
        super().setUp()
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.bindings = SQLiteTerminalBindingStore(self.db)
        self.runner = OrcaVisibleRunner(bindings=self.bindings, poll_interval=0.02, liveness_interval=0.2,
                                        handoff_root=self.root / "handoff")
        self.turn = self.root / "turn"
        self.turn.mkdir()

    async def start_gate(self, args=("--session-id", "s1"), **env):
        self.enterContext(patch.dict(os.environ, env))
        profile = ClaudeCodeProfile(name="sde", launcher=ClaudeCodeLauncher(name="fake", command=fake_command(self.root)))
        handle, _ = await self.runner._start(  # pyright: ignore[reportPrivateUsage]
            profile, "sde · task:1", self.repo, self.turn, args, "go")
        return handle, self.orca.terminals[handle]["proc"]

    async def test_closing_the_terminal_during_the_session_check_ends_the_probe_and_its_children(self):
        handle, gate = await self.start_gate(FAKE_PROBE_PIDFILE=str(self.pids))
        pids = await self.started(2)
        await self.orca("terminal", "close", "--terminal", handle, cwd=self.repo)
        self.assertEqual(gate.returncode, 128 + signal.SIGTERM)
        await self.all_gone(pids)

    async def test_a_lost_close_during_the_session_check_ends_the_probe(self):
        handle, gate = await self.start_gate(FAKE_PROBE_PIDFILE=str(self.pids))
        pids = await self.started(2)
        self.orca.lose_closes = True
        self.assertTrue(await _close_quietly(handle, self.repo))
        self.assertEqual(await asyncio.wait_for(gate.wait(), 8), 1)
        await self.all_gone(pids)

    async def test_closing_the_terminal_of_a_live_agent_ends_it_and_its_children(self):
        handle, gate = await self.start_gate(FAKE_INTERACTIVE_PIDFILE=str(self.pids))
        pids = await self.started(2)
        await self.orca("terminal", "close", "--terminal", handle, cwd=self.repo)
        self.assertEqual(gate.returncode, 128 + signal.SIGTERM)
        await self.all_gone(pids)

    async def test_a_lost_close_ends_the_live_agent_and_its_children(self):
        handle, gate = await self.start_gate(FAKE_INTERACTIVE_PIDFILE=str(self.pids))
        pids = await self.started(2)
        self.orca.lose_closes = True
        self.assertTrue(await _close_quietly(handle, self.repo))
        self.assertEqual(await asyncio.wait_for(gate.wait(), 8), 1)
        await self.all_gone(pids)

    async def test_releasing_a_delivered_conversation_ends_the_agent_even_when_orca_loses_the_close(self):
        handle, gate = await self.start_gate(FAKE_INTERACTIVE_PIDFILE=str(self.pids))
        pids = await self.started(2)
        self.bindings.save(TerminalBinding(session_id="s1", executor_id="sde", agent="claude", workdir=str(self.repo),
                                           handle=handle, native_session_id="n1"))
        self.orca.lose_closes = True
        self.assertEqual(await self.runner.release("s1", self.repo), f"closed {handle}")
        self.assertEqual(await asyncio.wait_for(gate.wait(), 8), 1)
        await self.all_gone(pids)

    async def test_an_interactive_agent_keeps_the_variables_orca_tracks_it_by(self):
        log = self.root / "calls.jsonl"
        # --add-dir makes the fake agent record itself and exit with code 4.
        _, gate = await self.start_gate(args=("--add-dir", str(self.repo), "--session-id", "s1"),
                                        FAKE_CLAUDE_LOG=str(log), **ORCA_SESSION_ENV)
        self.assertEqual(await asyncio.wait_for(gate.wait(), 8), 4)
        interactive = json.loads(log.read_text().splitlines()[-1])
        self.assertEqual(interactive["mode"], "interactive")
        self.assertLessEqual(set(ORCA_SESSION_ENV), set(interactive["orca_env"]))

    async def test_an_agent_that_exits_ends_the_gate_with_its_code(self):
        # --add-dir makes the fake agent stop at a trust dialog and exit with code 4.
        handle, gate = await self.start_gate(args=("--add-dir", str(self.repo), "--session-id", "s1"))
        self.assertEqual(await asyncio.wait_for(gate.wait(), 8), 4)
        self.assertIn("Is this a directory you trust?", self.orca.screen(handle))


if __name__ == "__main__":
    unittest.main()
