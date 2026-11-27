from __future__ import annotations

import contextlib
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from collections.abc import Sequence
from pathlib import Path

from switchboard.relay_protocol import Close, Push
from tests.support.switchboard_support import RELAY_TOKEN, mcp_call
from tests.support.switchboard_relay import handshake, recv_message, wait_until
from tests.support.switchboard_shutdown import refused


REPO = Path(__file__).resolve().parents[2]
# Linux lets a thread that is just creating another one take a signal sent to the process, and Python runs handlers on
# the main thread only. Each byte on stdin is a signal that this helper thread takes.
SIGNALLED_OFF_MAIN = """
import os, signal, sys, threading

import switchboard.app

def take() -> None:
    while data := os.read(0, 1):
        signal.pthread_kill(threading.get_ident(), data[0])

threading.Thread(target=take, daemon=True).start()
sys.exit(switchboard.app.main(sys.argv[1:]))
"""


class SignalTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory(prefix="sb_signal_")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.db = self.root / "switchboard.db"
        self.log = self.root / "stderr.log"

    def start(
        self, timeout: float, launcher: Sequence[str] = ("-m", "switchboard.app")
    ) -> tuple[subprocess.Popen[bytes], int]:
        config = self.root / "switchboard.toml"
        config.write_text(
            f'shutdown_timeout_seconds = {timeout}\n[http]\nport = 0\n[sqlite]\npath = "{self.db}"\n'
            '[relay]\nenabled = true\ntransport = "hybrid"\nack_timeout_sec = 60\nheartbeat_sec = 60\n',
            encoding="utf-8",
        )
        env = {k: v for k, v in os.environ.items() if not k.startswith("SWITCHBOARD_")}
        env.update(
            PYTHONPATH=str(REPO / "src"), PYTHONUNBUFFERED="1",
            SWITCHBOARD_RELAY_WEBHOOK_SECRET=RELAY_TOKEN, SWITCHBOARD_MCP_WEBHOOK_SECRET="test-mcp-bearer",
        )
        with self.log.open("wb") as log:
            proc = subprocess.Popen(
                [sys.executable, *launcher, "--config", str(config)],
                cwd=self.root, env=env, stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT,
            )
        assert proc.stdin is not None
        self.addCleanup(proc.stdin.close)
        self.addCleanup(proc.wait)
        self.addCleanup(proc.kill)
        self.assertTrue(wait_until(lambda: "relay endpoint" in self.output(), 20), self.output())
        port = int(self.output().split("listening on http://127.0.0.1:")[1].split()[0])
        return proc, port

    def output(self) -> str:
        return self.log.read_text(errors="replace")

    # An accepted request whose body never arrives keeps its handler thread busy.
    def hang_request(self, port: int) -> socket.socket:
        sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.addCleanup(sock.close)
        sock.sendall(b"POST /mcp HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer test-mcp-bearer\r\nContent-Length: 100\r\n\r\n{")
        time.sleep(0.3)
        return sock

    def test_first_signal_drains_and_exits_zero(self) -> None:
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signum=signum.name):
                proc, _ = self.start(10)
                proc.send_signal(signum)
                self.assertEqual(proc.wait(8), 0, self.output())
                self.assertNotIn("WARNING", self.output())
                self.assertNotIn("Traceback", self.output())
                self.assertEqual([p.name for p in self.root.glob("switchboard.db*")], ["switchboard.db"])

    def test_stuck_work_is_cut_at_the_timeout_with_exit_zero(self) -> None:
        proc, port = self.start(0.2)
        # Both events wait for the consumer, so its connection makes them due in one parallel round.
        for number in (1, 2):
            status, body = mcp_call(
                f"http://127.0.0.1:{port}",
                "post_message",
                {"correlation_key": f"github:a/b#{number}", "body": "hi", "scope": f"proj:{number}", "external_id": f"m{number}"},
            )
            self.assertEqual(status, 200, body)
        time.sleep(0.2)
        ws, _ = handshake(f"ws://127.0.0.1:{port}/ws/relay")
        self.addCleanup(ws.close)
        for _ in range(2):
            self.assertIsInstance(recv_message(ws), Push)
        self.hang_request(port)
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(8), 0, self.output())
        self.assertLess(time.monotonic() - started, 4)
        self.assertRegex(self.output(), r"WARNING .*HTTP requests, deliveries")
        self.assertNotIn("Traceback", self.output())
        with contextlib.closing(sqlite3.connect(self.db)) as conn:
            rows = conn.execute("SELECT status, attempts FROM delivery_log").fetchall()
        self.assertEqual(rows, [("pending", 0)] * 2)

    def test_signals_taken_by_another_thread_are_not_lost(self) -> None:
        for first, second in ((signal.SIGTERM, None), (signal.SIGTERM, signal.SIGINT), (signal.SIGINT, signal.SIGTERM)):
            with self.subTest(first=first.name, second=second and second.name):
                proc, port = self.start(60, ("-c", SIGNALLED_OFF_MAIN))
                assert proc.stdin is not None
                if second is not None:
                    ws, _ = handshake(f"ws://127.0.0.1:{port}/ws/relay")
                    self.addCleanup(ws.close)
                os.write(proc.stdin.fileno(), bytes([first]))
                if second is None:
                    self.assertEqual(proc.wait(8), 0, self.output())
                    continue
                self.assertIsInstance(recv_message(ws), Close)
                self.assertTrue(refused(port))
                os.write(proc.stdin.fileno(), bytes([second]))
                self.assertEqual(proc.wait(8), 128 + second, self.output())

    def test_second_signal_exits_at_once(self) -> None:
        for first, second in ((signal.SIGTERM, signal.SIGTERM), (signal.SIGTERM, signal.SIGINT), (signal.SIGINT, signal.SIGTERM)):
            with self.subTest(first=first.name, second=second.name):
                proc, port = self.start(60)
                ws, _ = handshake(f"ws://127.0.0.1:{port}/ws/relay")
                self.addCleanup(ws.close)
                proc.send_signal(first)
                # The unacknowledged close frame proves the first signal reached the drain.
                self.assertIsInstance(recv_message(ws), Close)
                self.assertTrue(refused(port))
                self.assertIsNone(proc.poll(), self.output())
                started = time.monotonic()
                proc.send_signal(second)
                self.assertEqual(proc.wait(8), 128 + second)
                self.assertLess(time.monotonic() - started, 2)


if __name__ == "__main__":
    unittest.main()
