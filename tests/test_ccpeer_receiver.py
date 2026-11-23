from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from ccpeer import Inbox, decode_envelope, discover, receiver_info, send
from tests.ccpeer_support import FakeSession, cc_user_frame, key_name, short_socket_dir, write_frame

REPO = Path(__file__).resolve().parents[1]


class ReceiverProcessTests(unittest.TestCase):
    """The receiver runs as its own process; a FakeSession plays the Claude Code side of the socket protocol."""

    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.socks = short_socket_dir()
        self.addCleanup(shutil.rmtree, self.socks, True)
        self.cfg = self.root / "claude"
        self.state = self.root / "state"
        self.fake = FakeSession(self.cfg / "sessions", self.socks, "orch")
        self.addCleanup(self.fake.close)

    def start_receiver(self, name: str = "hb") -> subprocess.Popen[str]:
        env = dict(os.environ, PYTHONPATH=str(REPO / "src"))
        proc = subprocess.Popen(
            [sys.executable, "-m", "ccpeer", "serve", "--as", name, "--config-dir", str(self.cfg),
             "--socket-dir", str(self.socks), "--state-dir", str(self.state)],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self._reap, proc)
        return proc

    @staticmethod
    def _reap(proc: subprocess.Popen[str]) -> None:
        if proc.poll() is None:
            proc.kill()
        proc.communicate()

    def wait_registered(self, proc: subprocess.Popen[str], name: str = "hb") -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                self.fail(f"receiver exited {proc.returncode}: {proc.communicate()}")
            # receiver.json is written after the registration and still names the previous receiver until then.
            info = receiver_info(name, self.state)
            if info is not None and info.pid == proc.pid and any(s.pid == proc.pid for s in discover([self.cfg])):
                return
            time.sleep(0.05)
        self.fail("receiver never registered")

    def reply_from_fake(self, body: str, *, to: str = "hb", msg_id: str | None = None) -> str:
        # What Claude Code does for SendMessage(to): find the named session, read its key, write the frame.
        [target] = [s for s in discover([self.cfg]) if s.name == to]
        key = json.loads((self.cfg / "sessions" / key_name(target.pid, target.socket_path)).read_text())
        frame = cc_user_frame(from_addr=self.fake.address, from_name="orch", body=body, msg_id=msg_id)
        write_frame(target.socket_path, key["peerToken"], frame)
        return frame["msg_id"]

    def wait_records(self, inbox_log_read, count: int):  # type: ignore[no-untyped-def]
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            page = inbox_log_read()
            if len(page.records) >= count:
                return page
            time.sleep(0.05)
        self.fail(f"expected {count} records, got {inbox_log_read().records}")

    def test_send_reply_and_resume_after_receiver_is_killed(self) -> None:
        proc = self.start_receiver()
        self.wait_registered(proc)
        reg_files = sorted(p.name for p in (self.cfg / "sessions").iterdir() if p.name.startswith(f"{proc.pid}."))
        self.assertEqual(len(reg_files), 2)
        for f in reg_files:
            self.assertEqual(os.stat(self.cfg / "sessions" / f).st_mode & 0o777, 0o600, f)

        info = receiver_info("hb", self.state)
        assert info is not None
        self.assertEqual(info.pid, proc.pid)
        body = "请回复。\n" + "混排 text 六十千字节 " * 3000 + "\n"
        self.assertGreater(len(body.encode()), 60_000)
        result = send("orch", body, sender_name="hb", reply_addr=info.address, config_dirs=[self.cfg])
        sent = json.loads(self.fake.wait_lines(2)[1])
        env = decode_envelope(sent["message"]["content"])
        assert env is not None
        self.assertEqual((sent["msg_id"], env.body, env.from_addr), (result.msg_id, body, info.address))

        reply_id = self.reply_from_fake("收到\n两行")
        write_frame(info.socket_path, json.loads((self.cfg / "sessions" / key_name(proc.pid, info.socket_path))
                                                  .read_text())["peerToken"],
                    {"msgV": 1, "msg_id": "c1", "type": "control", "action": "held", "orig_msg_id": result.msg_id,
                     "from": self.fake.address})
        inbox = Inbox(self.state / "hb")
        page = self.wait_records(inbox.messages.read, 1)
        [rec] = page.records
        self.assertEqual((rec["msg_id"], rec["type"], rec["from_name"], rec["from_mode"], rec["from_addr"], rec["body"]),
                         (reply_id, "user", "orch", "prompting", self.fake.address, "收到\n两行"))
        control = self.wait_records(inbox.control.read, 1)
        self.assertEqual(control.records[0]["action"], "held")

        # A frame with the wrong token is dropped.
        write_frame(info.socket_path, "0" * 32, cc_user_frame(from_addr="uds:x", from_name="x", body="forged"))

        proc.send_signal(signal.SIGKILL)
        proc.wait()
        restarted = self.start_receiver()
        self.wait_registered(restarted)
        self.assertFalse((self.cfg / "sessions" / f"{proc.pid}.json").exists(), "stale registration taken over")
        self.reply_from_fake("收到", msg_id=reply_id)  # a redelivered message is not stored twice
        second = self.reply_from_fake("重启后的回复")
        resumed = self.wait_records(lambda: inbox.messages.read(page.cursor), 1)
        self.assertEqual([r["msg_id"] for r in resumed.records], [second])
        self.assertEqual([r["msg_id"] for r in inbox.messages.read(reply_id).records], [second])
        self.assertEqual([r["body"] for r in inbox.messages.read().records], ["收到\n两行", "重启后的回复"])

        restarted.send_signal(signal.SIGTERM)
        self.assertEqual(restarted.wait(timeout=5), 0)
        leftovers = [p.name for p in (self.cfg / "sessions").iterdir() if p.name.startswith(f"{restarted.pid}.")]
        self.assertEqual(leftovers, [])
        self.assertFalse(Path(info.socket_path.replace(str(proc.pid), str(restarted.pid))).exists())
        self.assertIsNone(receiver_info("hb", self.state))

    def test_second_receiver_with_the_same_name_refuses_to_start(self) -> None:
        first = self.start_receiver()
        self.wait_registered(first)
        other_state = self.root / "other-state"
        env = dict(os.environ, PYTHONPATH=str(REPO / "src"))
        second = subprocess.run(
            [sys.executable, "-m", "ccpeer", "serve", "--as", "hb", "--config-dir", str(self.cfg),
             "--socket-dir", str(self.socks), "--state-dir", str(other_state)],
            env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(second.returncode, 2, second.stderr)
        self.assertIn(f"pid {first.pid}", second.stderr)
        self.assertIsNone(first.poll())

    def test_lost_registration_is_rewritten_by_the_heartbeat(self) -> None:
        env = dict(os.environ, PYTHONPATH=str(REPO / "src"))
        proc = subprocess.Popen(
            [sys.executable, "-m", "ccpeer", "serve", "--as", "hb", "--config-dir", str(self.cfg),
             "--socket-dir", str(self.socks), "--state-dir", str(self.state), "--heartbeat", "0.2"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(self._reap, proc)
        self.wait_registered(proc)
        (self.cfg / "sessions" / f"{proc.pid}.json").unlink()
        self.wait_registered(proc)


if __name__ == "__main__":
    unittest.main()
