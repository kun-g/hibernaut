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
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore, TerminalBinding
from tests.test_coder_reviewer import fake_command
from tests.test_visible_roles import FakeOrcaTerminals

# Processes started in a terminal must all end once the terminal is closed or its run is over: Orca can report a close
# that never reaches them, so the wrapper scripts end them on their own.


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def until(condition, seconds: float = 8.0) -> bool:
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
        self.assertTrue(await until(lambda: len(self.recorded()) == count), "the CLI did not start")
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
        self.assertEqual(await asyncio.wait_for(wrapper.wait(), 8), 0)
        await self.all_gone(self.recorded())
        self.assertNotIn("terminal close", self.orca.verbs())
        # Delivery still closes it; a terminal that already ended counts as closed.
        self.assertEqual(await close_terminals([handle], self.repo), f"closed {handle}")

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


class TestCLITerminalScript(unittest.IsolatedAsyncioTestCase):
    # The wrapper on its own, without Hibernaut watching: its deadline and the signals of a closing terminal.
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.pids = self.root / "pids"

    def start(self, timeout_seconds=30.0):
        spec = {"title": "t", "cwd": str(self.root), "env": {}, "timeout_seconds": timeout_seconds,
                "argv": ["/bin/sh", "-c", f'echo $$ > "{self.pids}"; sleep 60 & echo $! >> "{self.pids}"; wait'],
                **{name: str(self.root / name) for name in ("stdout", "stderr", "exit.json")}}
        (self.root / "spec.json").write_text(json.dumps(spec))
        proc = subprocess.Popen([sys.executable, "-I", str(CLI_TERMINAL_SCRIPT), str(self.root / "spec.json")],
                                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
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
                self.assertTrue(await until(lambda: not any(alive(p) for p in pids)))


class TestCLIGroupSignals(unittest.TestCase):
    # The CLI's process group is named by its leader's pid. Once the leader is reaped that pid can name an unrelated
    # group, so every signal to the group must go out while the leader is still unreaped (running or a zombie).
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.sent: list[tuple[int, int, bool]] = []
        real = os.killpg

        def record(pgid, sig):
            try:
                os.waitid(os.P_PID, pgid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                reaped = False
            except ChildProcessError:
                reaped = True
            self.sent.append((pgid, sig, reaped))
            return real(pgid, sig)

        self.enterContext(patch("os.killpg", new=record))

    def popen(self, script):
        proc = subprocess.Popen(["/bin/sh", "-c", script], start_new_session=True)
        self.addCleanup(lambda: proc.poll() is None and proc.kill())
        return proc

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
        proc = self.popen("sleep 60")
        time.sleep(0.2)
        cli_terminal.end_group(proc)
        self.assertEqual(proc.returncode, -signal.SIGTERM)
        # SIGKILL still goes out for anything left in the group, but while the leader is an unreaped zombie.
        self.assertEqual([s[1] for s in self.sent], [signal.SIGTERM, signal.SIGKILL])
        self.assert_never_after_reaping()

    def test_a_cli_that_ignores_sigterm_is_killed_while_it_is_still_running(self):
        proc = self.popen("trap '' TERM; sleep 60 & wait; sleep 60")
        time.sleep(0.2)
        cli_terminal.end_group(proc, grace=0.3)
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        self.assertEqual([s[1] for s in self.sent], [signal.SIGTERM, signal.SIGKILL])
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

    async def test_an_agent_that_exits_ends_the_gate_with_its_code(self):
        # --add-dir makes the fake agent stop at a trust dialog and exit with code 4.
        handle, gate = await self.start_gate(args=("--add-dir", str(self.repo), "--session-id", "s1"))
        self.assertEqual(await asyncio.wait_for(gate.wait(), 8), 4)
        self.assertIn("Is this a directory you trust?", self.orca.screen(handle))


if __name__ == "__main__":
    unittest.main()
