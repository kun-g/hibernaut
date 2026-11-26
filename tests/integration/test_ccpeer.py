from __future__ import annotations
import json
import os
import shutil
import socket
import tempfile
import unittest
from pathlib import Path
from ccpeer import AmbiguousTarget, PeerRefused, SendTimeout, TargetNotFound, discover, resolve, send
from tests.support.ccpeer_support import dead_pid, live_pid_process, short_socket_dir, write_registration


class DiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.socks = short_socket_dir()
        self.addCleanup(shutil.rmtree, self.socks, True)
        self.cfg_a = self.root / "claude-a"
        self.cfg_b = self.root / "claude-b"

    def sock(self, name: str) -> str:
        path = self.socks / name
        path.touch()
        return str(path)

    def live(self) -> int:
        proc = live_pid_process()
        self.addCleanup(proc.wait)
        self.addCleanup(proc.kill)
        return proc.pid

    def test_lists_live_sessions_across_config_dirs_and_skips_dead_or_socketless(self) -> None:
        p1, p2, p3 = self.live(), self.live(), self.live()
        write_registration(self.cfg_a / "sessions", pid=p1, sock=self.sock("1.sock"), name="alpha")
        write_registration(self.cfg_b / "sessions", pid=p2, sock=self.sock("2.sock"), name="beta")
        write_registration(self.cfg_a / "sessions", pid=dead_pid(), sock=self.sock("3.sock"), name="dead")
        write_registration(self.cfg_b / "sessions", pid=p3, sock=str(self.socks / "gone.sock"), name="no-socket")
        (self.cfg_a / "sessions" / "garbage.json").write_text("{not json")
        names = sorted(s.name for s in discover([self.cfg_a, self.cfg_b / "sessions"]))
        self.assertEqual(names, ["alpha", "beta"])

    def test_same_session_in_two_dirs_is_listed_once_with_both_dirs(self) -> None:
        pid, sock = self.live(), self.sock("x.sock")
        reg = write_registration(self.cfg_a / "sessions", pid=pid, sock=sock, name="twin")
        (self.cfg_b / "sessions").mkdir(parents=True)
        (self.cfg_b / "sessions" / f"{pid}.json").write_text(json.dumps(reg))
        [only] = discover([self.cfg_a, self.cfg_b])
        self.assertEqual(only.registry_dirs, (self.cfg_a / "sessions", self.cfg_b / "sessions"))

    def test_resolve_by_name_pid_session_id_and_address(self) -> None:
        pid, sock = self.live(), self.sock("r.sock")
        reg = write_registration(self.cfg_a / "sessions", pid=pid, sock=sock, name="orch-1")
        dirs = [self.cfg_a]
        for target in ("orch-1", str(pid), reg["sessionId"], f"uds:{sock}", sock):
            with self.subTest(target=target):
                self.assertEqual(resolve(target, dirs).pid, pid)
        with self.assertRaises(TargetNotFound):
            resolve("orch", dirs)

    def test_resolve_ambiguous_name_names_every_candidate(self) -> None:
        p1, p2 = self.live(), self.live()
        write_registration(self.cfg_a / "sessions", pid=p1, sock=self.sock("a.sock"), name="dup")
        write_registration(self.cfg_b / "sessions", pid=p2, sock=self.sock("b.sock"), name="dup")
        with self.assertRaises(AmbiguousTarget) as ctx:
            resolve("dup", [self.cfg_a, self.cfg_b])
        self.assertIn(str(p1), str(ctx.exception))
        self.assertIn(str(p2), str(ctx.exception))


class SendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.socks = short_socket_dir()
        self.addCleanup(shutil.rmtree, self.socks, True)
        self.sessions = self.root / "sessions"

    def test_missing_target_refused_socket_and_timeout_are_distinct_errors(self) -> None:
        with self.assertRaises(TargetNotFound):
            send("nobody", "hi", sender_name="hb", config_dirs=[self.root])

        stale = self.socks / f"{os.getpid()}.sock"
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.bind(str(stale))
        s.close()  # the file stays; nobody listens, so connect() is refused
        write_registration(self.sessions, pid=os.getpid(), sock=str(stale), name="refuser")
        with self.assertRaises(PeerRefused):
            send("refuser", "hi", sender_name="hb", config_dirs=[self.root])

        proc = live_pid_process()
        self.addCleanup(proc.wait)
        self.addCleanup(proc.kill)
        slow_path = str(self.socks / "slow.sock")
        slow = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        slow.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        slow.bind(slow_path)
        slow.listen(1)
        self.addCleanup(slow.close)
        write_registration(self.sessions, pid=proc.pid, sock=slow_path, name="stuck")
        with self.assertRaises(SendTimeout):
            send("stuck", "y" * 4_000_000, sender_name="hb", config_dirs=[self.root], timeout=0.5)


if __name__ == "__main__":
    unittest.main()
