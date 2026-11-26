import os
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.adapters.executor_cli import cli_terminal


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


if __name__ == "__main__":
    unittest.main()
