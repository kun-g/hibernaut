from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any


def short_socket_dir() -> Path:
    # AF_UNIX paths are capped at 104 bytes on macOS; the default temp dir there is too deep.
    return Path(tempfile.mkdtemp(prefix="ccp-", dir="/tmp"))


def dead_pid() -> int:
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def live_pid_process() -> subprocess.Popen[bytes]:
    return subprocess.Popen(["sleep", "300"])


def key_name(pid: int, sock: str) -> str:
    return f"{pid}.{hashlib.sha256(sock.encode()).hexdigest()}.key"


def write_registration(sessions_dir: Path, *, pid: int, sock: str, name: str, token: str | None = None,
                       **extra: Any) -> dict[str, Any]:
    sessions_dir.mkdir(parents=True, exist_ok=True)
    reg: dict[str, Any] = {
        "pid": pid, "sessionId": str(uuid.uuid4()), "startedAt": 1, "version": "2.1.330", "peerProtocol": 1,
        "peerFeatures": ["notify_idle"], "kind": "interactive", "entrypoint": "cli", "pidDomain": "darwin",
        "messagingSocketPath": sock, "name": name, "status": "idle", "cwd": "/tmp",
    }
    reg.update(extra)
    (sessions_dir / f"{pid}.json").write_text(json.dumps(reg))
    if token is not None:
        (sessions_dir / key_name(pid, sock)).write_text(json.dumps({"peerToken": token, "pidDomain": "darwin"}))
    return reg


def cc_user_frame(*, from_addr: str, from_name: str, body: str, msg_id: str | None = None) -> dict[str, Any]:
    # Mirrors the frame Claude Code 2.1.330 writes for SendMessage: envelope builder `<tag attrs>\nbody\n</tag>`.
    content = (f'<cross-session-message from="{from_addr}" from-name="{from_name}" from-mode="prompting">\n'
               f"{body}\n</cross-session-message>")
    return {"msgV": 1, "msg_id": msg_id or str(uuid.uuid4()), "type": "user",
            "message": {"role": "user", "content": content}, "priority": "next", "from": from_addr}


def write_frame(sock_path: str, token: str | None, frame: dict[str, Any]) -> None:
    lines: list[str] = []
    if token is not None:
        lines.append(json.dumps({"type": "auth", "token": token}))
    lines.append(json.dumps(frame, ensure_ascii=False))
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.settimeout(5)
        s.connect(sock_path)
        s.sendall(("\n".join(lines) + "\n").encode())


class FakeSession:
    """A stand-in Claude Code session: registered in a sessions dir, listening on its own socket."""

    def __init__(self, sessions_dir: Path, socket_dir: Path, name: str, pid: int | None = None) -> None:
        self.pid = pid if pid is not None else os.getpid()
        self.sock_path = str(socket_dir / f"{self.pid}.sock")
        self.token = uuid.uuid4().hex
        self.name = name
        self.lines: list[bytes] = []
        self._cond = threading.Condition()
        self._srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._srv.bind(self.sock_path)
        self._srv.listen(8)
        self.registration = write_registration(sessions_dir, pid=self.pid, sock=self.sock_path, name=name,
                                               token=self.token)
        threading.Thread(target=self._serve, daemon=True).start()

    @property
    def address(self) -> str:
        return f"uds:{self.sock_path}"

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self._srv.accept()
            except OSError:
                return
            with conn:
                data = b""
                while chunk := conn.recv(65536):
                    data += chunk
            with self._cond:
                self.lines.extend(line for line in data.split(b"\n") if line)
                self._cond.notify_all()

    def wait_lines(self, count: int, timeout: float = 5.0) -> list[bytes]:
        deadline = time.monotonic() + timeout
        with self._cond:
            while len(self.lines) < count:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise AssertionError(f"expected {count} lines, got {self.lines!r}")
                self._cond.wait(left)
            return list(self.lines)

    def close(self) -> None:
        self._srv.close()
        try:
            os.unlink(self.sock_path)
        except FileNotFoundError:
            pass
