import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from scripts import run_updating
from tests.support.process_cleanup import kill_processes_under, processes_under


ADOPT_AFTER_EXEC = """
import os, socket, sys
listener = socket.socket(fileno=int(os.environ["HIBERNAUT_LISTEN_FD"]))
conn, _ = listener.accept()
conn.sendall(b"served after exec")
conn.close()
"""
EXEC_WITH_LISTENER = """
import os, socket, sys
sys.path.insert(0, sys.argv[1])
from scripts import run_updating
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    fd = os.dup(sock.fileno())
    port = sock.getsockname()[1]
os.set_inheritable(fd, False)
print(port, flush=True)
run_updating.keep_across_exec(fd)
os.execv(sys.executable, [sys.executable, "-c", sys.argv[2], sys.argv[3]])
"""


class KeepAcrossExecTests(unittest.TestCase):
    def test_the_listener_stays_open_and_named_across_execv(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.addCleanup(kill_processes_under, root)
        # The temp root on both command lines lets the cleanup find the process before and after exec.
        proc = subprocess.Popen(
            [sys.executable, "-c", EXEC_WITH_LISTENER, str(run_updating.ROOT), ADOPT_AFTER_EXEC, str(root)],
            stdout=subprocess.PIPE, text=True,
        )
        self.addCleanup(proc.wait, 10)
        assert proc.stdout is not None
        port = int(proc.stdout.readline())
        with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
            self.assertEqual(conn.recv(100), b"served after exec")
        self.assertEqual(proc.wait(10), 0)
        proc.stdout.close()
        self.assertEqual(processes_under(root), [])


if __name__ == "__main__":
    unittest.main()
