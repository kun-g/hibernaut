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
from tests.support.ccpeer_support import (
    cc_user_frame,
    live_pid_process,
    short_socket_dir,
    write_frame,
    write_registration,
)
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import OrcaVisibleRunner
from agent_runtime.adapters.executor_cli.peer_handoff import TASK_HEADER, PeerHandoff, PeerHandoffConfig
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.peer_mailbox import SQLitePeerMailbox
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore


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
            # Wake accept() so the receiver sees the stop before its next heartbeat poll.
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as wake:
                wake.connect(receiver.socket_path)
            thread.join(timeout=5)
            receiver.close()
            self.assertFalse(thread.is_alive())
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
