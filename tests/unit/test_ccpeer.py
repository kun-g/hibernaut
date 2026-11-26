from __future__ import annotations
import ast
import json
import plistlib
import shutil
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from ccpeer import CursorError, Inbox, UnsupportedProtocol, decode_envelope, encode_envelope, frame_to_record, send, uds_address
from ccpeer.__main__ import build_parser
from ccpeer.sessions import default_config_dirs, sessions_dir
from tests.support.ccpeer_support import FakeSession, key_name, live_pid_process, short_socket_dir


REPO = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO / "src" / "ccpeer"
STDLIB = frozenset(sys.stdlib_module_names) | {"__future__"}


class BoundaryTests(unittest.TestCase):
    def test_ccpeer_imports_only_stdlib_and_itself(self) -> None:
        for py_file in PACKAGE_ROOT.rglob("*.py"):
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names: list[str] = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                    names = [node.module]
                for name in names:
                    with self.subTest(file=py_file.name, module=name):
                        self.assertIn(name.split(".")[0], STDLIB | {"ccpeer"})

    def test_importlinter_contract_and_strict_typing_cover_the_package(self) -> None:
        linter = (REPO / ".importlinter").read_text()
        self.assertIn("[importlinter:contract:ccpeer-zero-business-deps]", linter)
        section = linter.split("[importlinter:contract:ccpeer-zero-business-deps]", 1)[1]
        for forbidden in ("agent_runtime", "taskboard", "runner", "memory_hub"):
            self.assertIn(forbidden, section)
        self.assertIn("src/ccpeer", json.loads((REPO / "pyrightconfig.json").read_text())["include"])


class EnvelopeTests(unittest.TestCase):
    def roundtrip(self, body: str) -> None:
        content = encode_envelope(body, from_addr="uds:/tmp/cc-socks/1.sock", from_name="hb", from_mode="prompting")
        env = decode_envelope(content)
        assert env is not None
        self.assertEqual(env.body, body)
        self.assertEqual((env.from_addr, env.from_name, env.from_mode), ("uds:/tmp/cc-socks/1.sock", "hb", "prompting"))

    def test_matches_claude_code_envelope_byte_for_byte(self) -> None:
        self.assertEqual(
            encode_envelope("hi", from_addr="uds:/tmp/cc-socks/9.sock", from_name="hb", from_mode="prompting"),
            '<cross-session-message from="uds:/tmp/cc-socks/9.sock" from-name="hb" from-mode="prompting">\n'
            "hi\n</cross-session-message>",
        )

    def test_long_body_with_newlines_and_whitespace_is_preserved(self) -> None:
        body = "\n  开头空行与缩进\r\n" + ("中英文 mixed 正文\n" * 4000) + "\t结尾空白  \n\n"
        self.assertGreater(len(body.encode()), 60_000)
        self.roundtrip(body)

    def test_special_tags_inside_body_are_not_escaped(self) -> None:
        body = ('quote: <cross-session-message from="uds:x" from-name="evil">\ninner\n</cross-session-message>\n'
                "and an escaped one <\\/cross-session-message> stays as is")
        content = encode_envelope(body, from_addr="uds:/a.sock", from_name="hb", from_mode="prompting")
        self.assertIn(body, content)
        self.roundtrip(body)

    def test_empty_body_and_body_ending_with_closing_tag(self) -> None:
        self.roundtrip("")
        self.roundtrip("tail </cross-session-message>")

    def test_rejects_unknown_mode_and_unsafe_names(self) -> None:
        with self.assertRaises(ValueError):
            encode_envelope("x", from_addr=None, from_name="hb", from_mode="dontAsk")
        with self.assertRaises(ValueError):
            encode_envelope("x", from_addr=None, from_name="a\nb", from_mode="prompting")
        content = encode_envelope("x", from_addr=None, from_name='a"<b>', from_mode="bypass")
        self.assertIn('from-name="ab"', content)
        self.assertNotIn(" from=", content)

    def test_decode_returns_none_for_plain_text(self) -> None:
        self.assertIsNone(decode_envelope("just a prompt"))

    def test_uds_address_percent_encodes_like_claude_code(self) -> None:
        self.assertEqual(uds_address("/tmp/cc-socks/12.sock"), "uds:/tmp/cc-socks/12.sock")
        self.assertEqual(uds_address("/tmp/a b/é.sock"), "uds:/tmp/a%20b/%C3%A9.sock")


class FrameRecordTests(unittest.TestCase):
    def test_user_frame_becomes_message_record(self) -> None:
        content = encode_envelope("reply\n", from_addr="uds:/s/1.sock", from_name="orch", from_mode="prompting")
        rec = frame_to_record({"msgV": 1, "msg_id": "m1", "type": "user", "from": "uds:/s/1.sock",
                               "message": {"role": "user", "content": content}}, now=5.0)
        self.assertEqual(rec, {"msg_id": "m1", "ts": 5.0, "type": "user", "from_addr": "uds:/s/1.sock",
                               "from_name": "orch", "from_mode": "prompting", "body": "reply\n"})

    def test_unwrapped_and_block_content(self) -> None:
        rec = frame_to_record({"msg_id": "m2", "type": "user",
                               "message": {"content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]}},
                              now=1.0)
        assert rec is not None
        self.assertEqual((rec["body"], rec["from_name"], rec["unwrapped"]), ("a\nb", None, True))

    def test_control_frame_keeps_receipt_fields(self) -> None:
        rec = frame_to_record({"msg_id": "c1", "type": "control", "action": "held", "status": "pending",
                               "reason": "mode mismatch", "orig_msg_id": "m1", "from": "uds:/s/1.sock"}, now=2.0)
        assert rec is not None
        self.assertEqual(rec["type"], "control")
        self.assertEqual((rec["action"], rec["orig_msg_id"], rec["from_addr"]), ("held", "m1", "uds:/s/1.sock"))

    def test_unknown_frame_type_is_ignored(self) -> None:
        self.assertIsNone(frame_to_record({"type": "ping"}, now=1.0))


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

    def test_default_config_dirs_include_claude_config_dir(self) -> None:
        dirs = default_config_dirs({"HOME": "/home/u", "CLAUDE_CONFIG_DIR": "/opt/cfg"})
        self.assertEqual(dirs, (Path("/home/u/.claude"), Path("/opt/cfg")))
        self.assertEqual(sessions_dir(Path("/opt/cfg/sessions")), Path("/opt/cfg/sessions"))


class SendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.socks = short_socket_dir()
        self.addCleanup(shutil.rmtree, self.socks, True)
        self.sessions = self.root / "sessions"

    def test_send_writes_auth_then_claude_code_frame(self) -> None:
        fake = FakeSession(self.sessions, self.socks, "orch")
        self.addCleanup(fake.close)
        body = "  第一行\n" + "x" * 70_000 + "\n</cross-session-message>\n"
        result = send("orch", body, sender_name="hb", reply_addr="uds:/tmp/cc-socks/42.sock",
                      config_dirs=[self.root])
        auth, frame_line = fake.wait_lines(2)
        self.assertEqual(json.loads(auth), {"type": "auth", "token": fake.token})
        frame = json.loads(frame_line)
        self.assertEqual(frame["msg_id"], result.msg_id)
        self.assertEqual((frame["msgV"], frame["type"], frame["priority"], frame["from"]),
                         (1, "user", "next", "uds:/tmp/cc-socks/42.sock"))
        env = decode_envelope(frame["message"]["content"])
        assert env is not None
        self.assertEqual((env.body, env.from_name, env.from_mode), (body, "hb", "prompting"))
        self.assertEqual((result.pid, result.name), (fake.pid, "orch"))

    def test_unknown_peer_protocol_is_refused_before_connecting(self) -> None:
        fake = FakeSession(self.sessions, self.socks, "future")
        self.addCleanup(fake.close)
        reg = dict(fake.registration, peerProtocol=2, version="9.0.0")
        (self.sessions / f"{fake.pid}.json").write_text(json.dumps(reg))
        with self.assertRaises(UnsupportedProtocol) as ctx:
            send("future", "hi", sender_name="hb", config_dirs=[self.root])
        self.assertIn("peerProtocol=2", str(ctx.exception))
        self.assertIn("9.0.0", str(ctx.exception))
        reg.pop("peerProtocol")
        (self.sessions / f"{fake.pid}.json").write_text(json.dumps(reg))
        with self.assertRaises(UnsupportedProtocol):
            send("future", "hi", sender_name="hb", config_dirs=[self.root])
        self.assertEqual(fake.lines, [])


class InboxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, True)

    def test_routes_control_receipts_to_their_own_log(self) -> None:
        inbox = Inbox(self.dir)
        self.assertTrue(inbox.record({"msg_id": "m1", "ts": 1.0, "type": "user", "body": "a"}))
        self.assertTrue(inbox.record({"msg_id": "c1", "ts": 2.0, "type": "control", "action": "held"}))
        self.assertEqual([r["msg_id"] for r in inbox.messages.read().records], ["m1"])
        self.assertEqual([r["msg_id"] for r in inbox.control.read().records], ["c1"])

    def test_cursor_by_offset_and_msg_id(self) -> None:
        inbox = Inbox(self.dir)
        for i in range(3):
            inbox.record({"msg_id": f"m{i}", "ts": float(i), "type": "user", "body": f"第{i}条\n"})
        first = inbox.messages.read(limit=1)
        self.assertEqual([r["msg_id"] for r in first.records], ["m0"])
        rest = inbox.messages.read(first.cursor)
        self.assertEqual([r["msg_id"] for r in rest.records], ["m1", "m2"])
        self.assertEqual(inbox.messages.read(rest.cursor).records, [])
        self.assertEqual([r["msg_id"] for r in inbox.messages.read("m0").records], ["m1", "m2"])
        with self.assertRaises(CursorError):
            inbox.messages.read("missing")
        with self.assertRaises(CursorError):
            inbox.messages.read(3)  # not a line boundary
        with self.assertRaises(CursorError):
            inbox.messages.read(rest.cursor + 100)

    def test_duplicate_msg_id_is_dropped_even_after_reopen(self) -> None:
        Inbox(self.dir).record({"msg_id": "m1", "ts": 1.0, "type": "user", "body": "a"})
        reopened = Inbox(self.dir)
        self.assertFalse(reopened.record({"msg_id": "m1", "ts": 2.0, "type": "user", "body": "a"}))
        self.assertEqual(len(reopened.messages.read().records), 1)

    def test_torn_tail_is_repaired_and_never_returned(self) -> None:
        inbox = Inbox(self.dir)
        inbox.record({"msg_id": "m1", "ts": 1.0, "type": "user", "body": "a"})
        with open(self.dir / "inbox.jsonl", "ab") as f:
            f.write(b'{"msg_id": "torn", "bo')
        page = Inbox(self.dir).messages.read()
        self.assertEqual([r["msg_id"] for r in page.records], ["m1"])
        reopened = Inbox(self.dir)
        reopened.record({"msg_id": "m2", "ts": 2.0, "type": "user", "body": "b"})
        self.assertEqual([r["msg_id"] for r in reopened.messages.read(page.cursor).records], ["m2"])

    def test_reader_does_not_consume_a_line_still_being_written(self) -> None:
        inbox = Inbox(self.dir)
        inbox.record({"msg_id": "m1", "ts": 1.0, "type": "user", "body": "a"})
        with open(self.dir / "inbox.jsonl", "ab") as f:
            f.write(b'{"msg_id": "m2"')
        page = inbox.messages.read()
        self.assertEqual(len(page.records), 1)
        with open(self.dir / "inbox.jsonl", "ab") as f:
            f.write(b', "type": "user", "ts": 2.0, "body": "b"}\n')
        self.assertEqual([r["msg_id"] for r in inbox.messages.read(page.cursor).records], ["m2"])


class CliAndDeployTests(unittest.TestCase):
    def test_cli_has_the_four_commands(self) -> None:
        parser = build_parser()
        for argv in (["list"], ["send", "--as", "hb", "--to", "x", "-"], ["serve", "--as", "hb"],
                     ["inbox", "--as", "hb", "--cursor", "0"]):
            with self.subTest(argv=argv):
                self.assertEqual(parser.parse_args(argv).cmd, argv[0])

    def test_launchd_example_runs_serve_in_the_foreground_and_keeps_it_alive(self) -> None:
        plist = plistlib.loads((REPO / "deploy" / "ccpeer" / "com.hibernaut.ccpeer.plist.example").read_bytes())
        self.assertTrue(plist["RunAtLoad"])
        self.assertTrue(plist["KeepAlive"])
        args = plist["ProgramArguments"]
        self.assertEqual(args[1:3], ["-m", "ccpeer"])
        parsed = build_parser().parse_args(args[3:])
        self.assertEqual(parsed.cmd, "serve")
        self.assertTrue(parsed.config_dirs, "launchd has no CLAUDE_CONFIG_DIR; config dirs must be explicit")


class LocalSendRoundTripTests(unittest.TestCase):
    """send() to a FakeSession bound in this process exercises the socket path without a receiver."""

    def test_concurrent_sends_each_get_their_own_frame(self) -> None:
        root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, root, True)
        socks = short_socket_dir()
        self.addCleanup(shutil.rmtree, socks, True)
        fake = FakeSession(root / "sessions", socks, "orch")
        self.addCleanup(fake.close)
        threads = [threading.Thread(target=send, args=("orch", f"m{i}"),
                                    kwargs={"sender_name": "hb", "config_dirs": [root]}) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        frames = [json.loads(line) for line in fake.wait_lines(10) if b'"auth"' not in line]
        self.assertEqual(sorted(decode_envelope(f["message"]["content"]).body for f in frames),  # type: ignore[union-attr]
                         [f"m{i}" for i in range(5)])
        self.assertTrue((root / "sessions" / key_name(fake.pid, fake.sock_path)).exists())


if __name__ == "__main__":
    unittest.main()
