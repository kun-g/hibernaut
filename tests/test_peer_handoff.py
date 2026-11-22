import asyncio
import json
import os
import shlex
import shutil
import socket
import tempfile
import threading
import time
import unittest
import urllib.parse
import uuid
from collections.abc import Callable
from pathlib import Path
from unittest.mock import patch

from ccpeer import Receiver, ReceiverConfig, decode_envelope
from ccpeer.inbox import JsonlLog
from tests.ccpeer_support import cc_user_frame, live_pid_process, short_socket_dir, write_frame, write_registration

from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeError, ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import OrcaVisibleRunner
from agent_runtime.adapters.executor_cli.peer_handoff import (
    TASK_HEADER,
    PeerHandoff,
    PeerHandoffConfig,
    parse_result,
)
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.peer_mailbox import SQLitePeerMailbox
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config

RECEIVER = "hibernaut"
DONE = {"status": "done", "summary": "fixed", "test_evidence": "pytest: 1 passed", "change_kind": "fix",
        "change_rationale": "a wrong result is corrected", "code_change": True}


class Incoming:
    def __init__(self, frame: dict, body: str, from_addr: str | None) -> None:
        self.frame, self.body, self.from_addr = frame, body, from_addr
        first, _, _ = body.partition("\n")
        self.header = json.loads(first[len(TASK_HEADER) + 1:]) if first.startswith(TASK_HEADER + " ") else None


class FakeClaude:
    """A Claude Code session behind an Orca terminal: registered like Claude Code 2.1.x under its own pid, socket,
    sessionId and --name, taking peer frames on its socket and answering with frames written to the sender's `from`.
    `answers` holds one step per incoming message: a callable (session, message) that replies, or None to stay quiet."""

    def __init__(self, registry: Path, socket_dir: Path, *, session_id: str, name: str, cwd: str,
                 answers: list[Callable[["FakeClaude", Incoming], None] | None]) -> None:
        self.proc = live_pid_process()
        self.pid = self.proc.pid
        self.registry = registry
        self.sock_path = str(socket_dir / f"{self.pid}.sock")
        self.session_id, self.name = session_id, name
        self.answers = answers
        self.received: list[Incoming] = []
        self.cond = threading.Condition()
        self.srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.srv.bind(self.sock_path)
        self.srv.listen(8)
        write_registration(registry, pid=self.pid, sock=self.sock_path, name=name, token=uuid.uuid4().hex,
                           sessionId=session_id, cwd=cwd, startedAt=int(time.time() * 1000))
        threading.Thread(target=self._serve, daemon=True).start()

    @property
    def address(self) -> str:
        return f"uds:{self.sock_path}"

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self.srv.accept()
            except OSError:
                return
            with conn:
                data = b""
                while chunk := conn.recv(65536):
                    data += chunk
            for line in data.split(b"\n"):
                if not line.strip():
                    continue
                frame = json.loads(line)
                if frame.get("type") != "user":
                    continue
                env = decode_envelope(frame["message"]["content"])
                assert env is not None
                msg = Incoming(frame, env.body, env.from_addr)
                with self.cond:
                    self.received.append(msg)
                    self.cond.notify_all()
                step = self.answers.pop(0) if self.answers else None
                if step is not None:
                    step(self, msg)

    def reply(self, msg: Incoming, body: str, *, msg_id: str | None = None) -> str:
        frame = cc_user_frame(from_addr=self.address, from_name=self.name, body=body, msg_id=msg_id)
        write_frame(urllib.parse.unquote(str(msg.from_addr)[4:]), None, frame)
        return frame["msg_id"]

    def control(self, msg: Incoming, **fields) -> None:
        frame = {"msgV": 1, "msg_id": str(uuid.uuid4()), "type": "control", "from": self.address, **fields}
        write_frame(urllib.parse.unquote(str(msg.from_addr)[4:]), None, frame)

    def wait_received(self, count: int, timeout: float = 5.0) -> list[Incoming]:
        deadline = time.monotonic() + timeout
        with self.cond:
            while len(self.received) < count:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise AssertionError(f"expected {count} messages, got {[m.body[:80] for m in self.received]}")
                self.cond.wait(left)
            return list(self.received)

    def stop(self) -> None:
        self.srv.close()
        for path in (Path(self.sock_path), self.registry / f"{self.pid}.json"):
            path.unlink(missing_ok=True)
        for key in self.registry.glob(f"{self.pid}.*.key"):
            key.unlink()
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()


def result(**overrides) -> Callable[[FakeClaude, Incoming], None]:
    # Answers with the task's own ids unless overridden; a string `raw` is sent as it is.
    def step(session: FakeClaude, msg: Incoming) -> None:
        raw = overrides.get("raw")
        if raw is None:
            body = {"task_id": msg.header["task_id"], "attempt_id": msg.header["attempt_id"], **DONE}
            body.update({k: v for k, v in overrides.items() if k != "drop"})
            for key in overrides.get("drop", ()):
                body.pop(key)
            raw = json.dumps(body)
        session.reply(msg, raw)
    return step


class PeerOrca:
    """Orca CLI double for peer handoff: `terminal create` reads the gate spec the runner wrote, reports the session
    check passed and starts a FakeClaude with the --session-id/--resume and --name of the spec's exec argv; `/exit`
    sent to a terminal ends its session the way Claude Code quits."""

    def __init__(self, registry: Path, socket_dir: Path) -> None:
        self.registry, self.socket_dir = registry, socket_dir
        self.terminals: dict[str, dict] = {}
        self.calls: list[tuple[str, ...]] = []
        self.specs: list[dict] = []
        self.scripts: list[list] = []

    def _info(self, handle: str) -> dict:
        return {"handle": handle, "incarnationId": self.terminals[handle]["incarnation"], "connected": True,
                "writable": True, "agentIdentity": "claude"}

    async def __call__(self, *args: str, cwd: Path) -> dict:
        self.calls.append(args)
        verb = " ".join(args[:2])
        flags = dict(zip(args[2::2], args[3::2]))
        if verb == "terminal create":
            spec = json.loads(Path(shlex.split(flags["--command"])[-1]).read_text())
            self.specs.append(spec)
            Path(spec["status"]).write_text(json.dumps({"ok": True}))
            argv = spec["exec"]
            sid = argv[argv.index("--session-id") + 1] if "--session-id" in argv else argv[argv.index("--resume") + 1]
            handle = f"term_{uuid.uuid4().hex[:8]}"
            session = FakeClaude(self.registry, self.socket_dir, session_id=sid, name=argv[argv.index("--name") + 1],
                                 cwd=flags["--worktree"].removeprefix("path:"),
                                 answers=self.scripts.pop(0) if self.scripts else [])
            self.terminals[handle] = {"session": session, "incarnation": uuid.uuid4().hex, "ended": False}
            return {"terminal": {"handle": handle, "incarnationId": self.terminals[handle]["incarnation"]}}
        if verb == "terminal list":
            return {"terminals": [self._info(h) for h, t in self.terminals.items() if not t["ended"]]}
        term = self.terminals.get(flags.get("--terminal", ""))
        if term is None or term["ended"]:
            raise RuntimeError('{"ok": false, "error": {"code": "terminal_handle_stale"}}')
        if verb == "terminal show":
            return {"terminal": self._info(flags["--terminal"])}
        if verb == "terminal send":
            if flags["--text"] == "/exit":
                self.end(flags["--terminal"])
            return {"send": {"accepted": True, "prompt": {"processIncarnation": term["incarnation"]}}}
        if verb == "terminal close":
            self.end(flags["--terminal"])
            return {"close": {"handle": flags["--terminal"]}}
        raise AssertionError(f"unexpected orca call {args}")

    def end(self, handle: str) -> None:
        term = self.terminals[handle]
        term["session"].stop()
        term["ended"] = True

    def session(self, index: int = -1) -> FakeClaude:
        return list(self.terminals.values())[index]["session"]

    def verbs(self) -> list[str]:
        return [" ".join(c[:2]) for c in self.calls]

    def stop_all(self) -> None:
        for term in self.terminals.values():
            term["session"].stop()


class PeerCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.socks = short_socket_dir()
        self.addCleanup(shutil.rmtree, self.socks, ignore_errors=True)
        self.claude = self.root / "claude"
        self.registry = self.claude / "sessions"
        self.registry.mkdir(parents=True)
        self.state = self.root / "ccpeer"
        self.workdir = self.root / "worktree"
        self.workdir.mkdir()
        self.receiver = self.start_receiver()
        self.orca = PeerOrca(self.registry, self.socks)
        self.addCleanup(self.orca.stop_all)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.db_path = self.root / "hb.db"
        self.db = SQLiteDatabase(self.db_path)
        self.addCleanup(self.db.close)
        self.bindings = SQLiteTerminalBindingStore(self.db)
        self.notes: list[tuple[str, str]] = []
        self.runner = self.make_runner()

    def start_receiver(self) -> Receiver:
        receiver = Receiver(ReceiverConfig(name=RECEIVER, config_dirs=(self.claude,), state_root=self.state,
                                           socket_dir=self.socks), log=lambda _msg: None)
        receiver.start()
        thread = threading.Thread(target=receiver.serve_forever, daemon=True)
        thread.start()

        def stop() -> None:
            receiver.stop()
            thread.join(timeout=5)
            receiver.close()
        self.addCleanup(stop)
        return receiver

    def config(self, **overrides) -> PeerHandoffConfig:
        values = {"receiver": RECEIVER, "state_dir": self.state, "config_dirs": (self.claude,), "max_resends": 3,
                  "register_timeout": 5.0, "exit_timeout": 5.0, **overrides}
        return PeerHandoffConfig(**values)

    def make_runner(self, db: SQLiteDatabase | None = None, **overrides) -> OrcaVisibleRunner:
        db = db or self.db
        config = self.config(**overrides)
        return OrcaVisibleRunner(
            bindings=SQLiteTerminalBindingStore(db), poll_interval=0.02, liveness_interval=0.2,
            handoff_root=self.root / "handoff",
            peer=PeerHandoff(config, SQLitePeerMailbox(db, config.receiver)),
            notify=lambda inbox, text: self.notes.append((inbox, text)),
        )

    def profile(self) -> ClaudeCodeProfile:
        return ClaudeCodeProfile(name="sde", launcher=ClaudeCodeLauncher(name="fake", command="claude"),
                                 timeout_seconds=20.0)

    async def converse(self, runner: OrcaVisibleRunner | None = None, *, session_id: str | None = "sess-1",
                       prompt: str = "## Task requirements\ndivide must reject a zero divisor"):
        return await (runner or self.runner).converse(
            self.profile(), title="sde · task:1", workdir=self.workdir, prompt=prompt, schema=SDE_SCHEMA,
            role_args=("--allowedTools", "Read,Bash", "--append-system-prompt", "contract"),
            is_interrupted=lambda: False, session_id=session_id, schema_name="sde-report", inbox_id="proj:x",
        )

    def tasks(self, session: FakeClaude) -> list[Incoming]:
        return [m for m in session.received if m.header is not None]


class TestPeerTaskAndResult(PeerCase):
    async def test_task_arrives_as_a_peer_message_and_the_json_result_is_returned(self):
        self.orca.scripts.append([result()])
        reply = await self.converse()

        self.assertEqual(reply.data, DONE)
        session = self.orca.session()
        [task] = session.received
        self.assertEqual(task.header["task_id"], "sess-1")
        self.assertEqual((task.header["reply_to"], task.header["schema"]), (RECEIVER, "sde-report"))
        self.assertTrue(task.header["attempt_id"].startswith("att"))
        self.assertIn("divide must reject a zero divisor", task.body)
        self.assertIn('SendMessage to "hibernaut"', task.body)
        self.assertEqual(task.from_addr, self.receiver.address)
        self.assertEqual(task.frame["from"], self.receiver.address)
        # The session starts idle under ~/.claude with SendMessage allowed and no handoff MCP server.
        [spec] = self.orca.specs
        argv = spec["exec"]
        self.assertNotIn("--mcp-config", argv)
        self.assertNotIn("--", argv)
        self.assertEqual(argv[argv.index("--allowedTools") + 1], "Read,Bash,SendMessage")
        self.assertEqual(argv[argv.index("--name") + 1], "sde · task:1")
        self.assertEqual(argv[argv.index("--session-id") + 1], reply.native_session_id)
        self.assertIn("CLAUDE_CONFIG_DIR", spec["unset"])
        binding = self.bindings.get("sess-1")
        assert binding is not None
        self.assertEqual((binding.handle, binding.native_session_id), (reply.handle, reply.native_session_id))
        self.assertEqual((binding.peer_pid, binding.peer_socket, binding.peer_name),
                         (session.pid, session.sock_path, "sde · task:1"))
        self.assertEqual(binding.peer_attempt, task.header["attempt_id"])
        self.assertNotIn("terminal close", self.orca.verbs())

    async def test_each_kind_of_malformed_result_is_answered_once_with_the_reason_and_then_accepted(self):
        cases = {
            "syntax": (result(raw='{"task_id": "sess-1", "status": "done",'), "not valid JSON"),
            "missing field": (result(drop=("test_evidence",)), "result.test_evidence is required"),
            "attempt mismatch": (result(attempt_id="att-stale"), 'attempt_id is "att-stale"'),
        }
        for label, (bad, reason) in cases.items():
            with self.subTest(label):
                self.orca.scripts.append([bad, result()])
                reply = await self.converse(session_id=f"sess-{label}")
                self.assertEqual(reply.data, DONE)
                task, resend = self.orca.session().wait_received(2)
                self.assertIn("was not accepted", resend.body)
                self.assertIn(reason, resend.body)
                self.assertEqual(resend.header["attempt_id"], task.header["attempt_id"])
                self.assertEqual(len(self.orca.session().received), 2)

    async def test_too_many_malformed_results_fail_the_turn_and_close_the_terminal(self):
        self.orca.scripts.append([result(raw="done!")] * 3)
        runner = self.make_runner(max_resends=1)
        with self.assertRaisesRegex(ClaudeCodeError, r"2 result messages that were not accepted .*limit 1"):
            await self.converse(runner)
        session = self.orca.session()
        self.assertEqual(len(session.received), 2)
        self.assertIn("terminal close", self.orca.verbs())
        binding = self.bindings.get("sess-1")
        assert binding is not None
        # The conversation stays resumable; the terminal is gone.
        self.assertIsNone(binding.handle)
        self.assertEqual(binding.native_session_id, session.session_id)

    async def test_rework_goes_to_the_same_session_with_a_new_attempt(self):
        self.orca.scripts.append([result(), result(summary="reworked")])
        first = await self.converse()
        second = await self.converse(prompt="## Review round 1: changes requested\nfix the edge case")

        self.assertEqual(second.data["summary"], "reworked")
        self.assertEqual((first.handle, first.native_session_id), (second.handle, second.native_session_id))
        self.assertEqual(self.orca.verbs().count("terminal create"), 1)
        self.assertNotIn("terminal send", self.orca.verbs())
        task, rework = self.orca.session().received
        self.assertEqual(task.header["task_id"], rework.header["task_id"])
        self.assertNotEqual(task.header["attempt_id"], rework.header["attempt_id"])
        self.assertIn("fix the edge case", rework.body)
        binding = self.bindings.get("sess-1")
        assert binding is not None
        self.assertEqual(binding.peer_attempt, rework.header["attempt_id"])

    async def test_a_repeated_msg_id_is_handled_once(self):
        inbox = JsonlLog(self.state / RECEIVER / "inbox.jsonl")

        def duplicated_bad(session: FakeClaude, msg: Incoming) -> None:
            # The same record twice in the log, as a replayed delivery would leave it.
            record = {"msg_id": "dup-1", "ts": time.time(), "type": "user", "from_addr": session.address,
                      "from_name": session.name, "from_mode": "prompting", "body": "not json"}
            inbox.append(record)
            inbox.append(record)

        self.orca.scripts.append([duplicated_bad, result()])
        reply = await self.converse()
        self.assertEqual(reply.data, DONE)
        received = self.orca.session().received
        self.assertEqual(len(received), 2, [m.body[:60] for m in received])
        self.assertIn("was not accepted", received[1].body)

    async def test_a_held_receipt_is_reported_to_the_outbox(self):
        def held_then_done(session: FakeClaude, msg: Incoming) -> None:
            session.control(msg, action="held", orig_msg_id=msg.frame["msg_id"])
            result()(session, msg)

        self.orca.scripts.append([held_then_done])
        reply = await self.converse()
        self.assertEqual(reply.data, DONE)
        [(inbox, text)] = self.notes
        self.assertEqual(inbox, "proj:x")
        self.assertIn("'held'", text)
        self.assertIn(reply.handle, text)

    async def test_release_exits_claude_code_before_closing_anything(self):
        self.orca.scripts.append([result()])
        reply = await self.converse()
        session = self.orca.session()

        note = await self.runner.release("sess-1", self.workdir)

        self.assertIn("exited", note)
        exit_call = [c for c in self.orca.calls if c[:2] == ("terminal", "send")]
        self.assertEqual([c[c.index("--text") + 1] for c in exit_call], ["/exit"])
        self.assertNotIn("terminal close", self.orca.verbs())
        self.assertIsNotNone(session.proc.poll())
        self.assertFalse((self.registry / f"{session.pid}.json").exists())
        binding = self.bindings.get("sess-1")
        assert binding is not None
        self.assertEqual((binding.handle, binding.peer_pid, binding.native_session_id),
                         (None, None, reply.native_session_id))

    async def test_a_session_that_exits_without_a_result_is_reported(self):
        def quit_(session: FakeClaude, msg: Incoming) -> None:
            for handle, term in self.orca.terminals.items():
                if term["session"] is session:
                    term["ended"] = True
        self.orca.scripts.append([quit_])
        with self.assertRaisesRegex(ClaudeCodeError, "exited before it returned its result"):
            await self.converse()

    async def test_no_receiver_starts_nothing(self):
        runner = self.make_runner(receiver="nobody")
        with self.assertRaisesRegex(ClaudeCodeError, "receiver 'nobody' is not running"):
            await self.converse(runner)
        self.assertEqual(self.orca.calls, [])


class TestResultSurvivesRestart(PeerCase):
    async def test_a_result_sent_while_hibernaut_is_down_is_read_after_restart(self):
        # Hibernaut sends the task, then stops before the reply arrives; the session answers into the receiver's
        # inbox meanwhile. A new process with the same database finds the reply through the stored cursor.
        self.orca.scripts.append([None])
        task_run = asyncio.ensure_future(self.converse())
        session = await asyncio.to_thread(self.wait_for_task)
        handoff = self.runner._peer
        assert handoff is not None
        await asyncio.to_thread(handoff.ingest)
        before = handoff.mailbox.cursor("user")
        binding = self.bindings.get("sess-1")
        assert binding is not None and binding.peer_attempt
        [task] = session.received
        # Hibernaut goes down: nothing of the run is kept in memory, the terminal stays.
        with patch("agent_runtime.adapters.executor_cli.orca._close_quietly", new=_no_close):
            task_run.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task_run
        self.db.close()
        first = session.reply(task, "still working")
        second = session.reply(task, json.dumps({"task_id": "sess-1", "attempt_id": binding.peer_attempt, **DONE}))
        await asyncio.to_thread(self.wait_inbox, 2)

        db = SQLiteDatabase(self.db_path)
        self.addCleanup(db.close)
        restarted = PeerHandoff(self.config(), SQLitePeerMailbox(db, RECEIVER))
        self.assertEqual(restarted.mailbox.cursor("user"), before)
        await asyncio.to_thread(restarted.ingest)
        stored = SQLiteTerminalBindingStore(db).get("sess-1")
        assert stored is not None
        target = restarted.find(stored.native_session_id or "", self.workdir)
        assert target is not None
        replies = restarted.replies(target, since=0)
        self.assertEqual([r.msg_id for r in replies], [first, second])
        data, error = parse_result(replies[1].body, SDE_SCHEMA, task_id="sess-1", attempt_id=stored.peer_attempt or "")
        self.assertEqual((data, error), (DONE, ""))
        # Read once: a second ingest and take returns nothing new.
        await asyncio.to_thread(restarted.ingest)
        self.assertEqual(restarted.replies(target, since=0), [])

    def wait_for_task(self) -> FakeClaude:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.orca.terminals and self.orca.session().received:
                return self.orca.session()
            time.sleep(0.02)
        raise AssertionError("the task never reached the session")

    def wait_inbox(self, count: int) -> None:
        log = JsonlLog(self.state / RECEIVER / "inbox.jsonl")
        deadline = time.monotonic() + 5
        while len(log.read().records) < count:
            if time.monotonic() > deadline:
                raise AssertionError(f"inbox has {log.read().records}")
            time.sleep(0.02)


async def _no_close(handle: str, workdir: Path) -> bool:
    return True


class TestParseResult(unittest.TestCase):
    def test_order_of_checks_and_fenced_json(self):
        ok = {"task_id": "t", "attempt_id": "a", **DONE}
        self.assertEqual(parse_result(json.dumps(ok), SDE_SCHEMA, task_id="t", attempt_id="a"), (DONE, ""))
        fenced = "```json\n" + json.dumps(ok) + "\n```"
        self.assertEqual(parse_result(fenced, SDE_SCHEMA, task_id="t", attempt_id="a")[0], DONE)
        self.assertIn("not a JSON object", parse_result("[1]", SDE_SCHEMA, task_id="t", attempt_id="a")[1])
        no_ids = json.dumps(DONE)
        self.assertIn("result.task_id is required", parse_result(no_ids, SDE_SCHEMA, task_id="t", attempt_id="a")[1])
        wrong_task = json.dumps({**ok, "task_id": "other"})
        self.assertIn('task_id is "other"', parse_result(wrong_task, SDE_SCHEMA, task_id="t", attempt_id="a")[1])
        bad_enum = json.dumps({**ok, "status": "maybe"})
        self.assertIn("result.status must be one of", parse_result(bad_enum, SDE_SCHEMA, task_id="t", attempt_id="a")[1])


class TestPeerConfig(unittest.TestCase):
    BASE = {
        "runtime": {"default_executor": "sde"},
        "executors": {"sde": {"role": "sde", "review": False}},
        "projects": {"demo": {"inbox_id": "proj:demo", "repo": "o/demo", "checkout": "/tmp", "worktree_backend": "git",
                              "terminal_backend": "orca"}},
    }

    def config(self, project: dict | None = None, peer: dict | None = None, **runtime):
        raw = json.loads(json.dumps(self.BASE))
        raw["projects"]["demo"].update(project or {})
        raw["runtime"].update(runtime)
        if peer is not None:
            raw["peer"] = peer
        return parse_config(raw, base_dir=Path("/etc/hb"))

    def test_handoff_defaults_to_mcp_and_peer_settings_parse(self):
        self.assertEqual(self.config().projects["demo"].handoff, "mcp")
        cfg = self.config({"handoff": "peer"}, {"receiver": "hb-2", "state_dir": "state", "config_dirs": ["~/.claude"],
                                               "max_resends": 0, "register_timeout_seconds": 30})
        self.assertEqual(cfg.projects["demo"].handoff, "peer")
        self.assertEqual((cfg.peer.receiver, cfg.peer.state_dir, cfg.peer.max_resends, cfg.peer.register_timeout),
                         ("hb-2", Path("/etc/hb/state"), 0, 30.0))
        self.assertEqual(cfg.peer.config_dirs, (Path.home() / ".claude",))
        self.assertEqual(self.config().peer, PeerHandoffConfig())

    def test_invalid_values_are_rejected(self):
        for project, peer, message in (
            ({"handoff": "files"}, None, "Unsupported handoff"),
            ({}, {"receiver": "a b"}, "peer.receiver"),
            ({}, {"max_resends": -1}, "peer.max_resends"),
            ({}, {"exit_timeout_seconds": 0}, "peer.exit_timeout_seconds"),
            ({}, {"config_dirs": []}, "peer.config_dirs"),
            ({}, {"socket": "x"}, "unknown keys: socket"),
        ):
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                self.config(project, peer)

    def test_startup_fails_clearly_when_the_receiver_is_not_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self.config({"handoff": "peer"}, {"state_dir": tmp}, db_path=f"{tmp}/hb.db",
                              memory_root=f"{tmp}/mem")
            with self.assertRaisesRegex(RuntimeError, r'handoff = "peer" for demo, but the ccpeer receiver '
                                                      r"'hibernaut' is not running"):
                build_daemon_from_config(cfg)


if __name__ == "__main__":
    unittest.main()
