from __future__ import annotations

import fcntl
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import IO, Any, Final, cast

from ccpeer.errors import ReceiverConflict
from ccpeer.inbox import Inbox
from ccpeer.protocol import PEER_PROTOCOL, frame_to_record, key_filename, uds_address
from ccpeer.sessions import (
    PathLike,
    default_config_dirs,
    load_json,
    pid_alive,
    registry_dirs,
    scan_registrations,
)

NAME_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
DEFAULT_SOCKET_DIR: Final = Path("/tmp/cc-socks")
DEFAULT_STATE_ROOT: Final = Path("~/.ccpeer")
MAX_LINE_BYTES: Final = 64 * 1024 * 1024
# AF_UNIX sun_path is 104 bytes on macOS (108 on Linux), including the terminating NUL.
MAX_SOCKET_PATH: Final = 103

EXIT_CONFLICT: Final = 2


def state_dir(name: str, state_root: PathLike | None = None) -> Path:
    root = Path(os.path.expanduser(str(state_root if state_root is not None else DEFAULT_STATE_ROOT)))
    return root / name


@dataclass(frozen=True, slots=True)
class ReceiverInfo:
    pid: int
    name: str
    socket_path: str
    address: str
    config_dirs: tuple[str, ...]


def receiver_info(name: str, state_root: PathLike | None = None) -> ReceiverInfo | None:
    data = load_json(state_dir(name, state_root) / "receiver.json")
    if data is None:
        return None
    pid, sock = data.get("pid"), data.get("socket_path")
    if not isinstance(pid, int) or not isinstance(sock, str) or not pid_alive(pid) or not os.path.exists(sock):
        return None
    dirs = data.get("config_dirs")
    return ReceiverInfo(pid=pid, name=name, socket_path=sock, address=uds_address(sock),
                        config_dirs=tuple(str(d) for d in cast(list[object], dirs)) if isinstance(dirs, list) else ())


def _stderr_log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def _proc_start(pid: int) -> str | None:
    # Claude Code records `ps -o lstart` (C locale, UTC) to tell a live owner from a recycled pid. Optional.
    try:
        out = subprocess.run(["ps", "-o", "lstart=", "-p", str(pid)], capture_output=True, text=True, timeout=5,
                             env=dict(os.environ, LC_ALL="C", TZ="UTC"), check=True).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None
    return out or None


def _write_private(path: Path, obj: Any) -> None:
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def _unlink(path: Path | str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _connectable(sock_path: str) -> bool:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(1.0)
    try:
        s.connect(sock_path)
        return True
    except OSError:
        return False
    finally:
        s.close()


@dataclass(frozen=True)
class ReceiverConfig:
    name: str
    config_dirs: tuple[Path, ...]
    state_root: Path = DEFAULT_STATE_ROOT
    socket_dir: Path = DEFAULT_SOCKET_DIR
    heartbeat_sec: float = 60.0
    read_timeout_sec: float = 30.0

    def __post_init__(self) -> None:
        if not NAME_RE.fullmatch(self.name):
            raise ValueError(f"receiver name must match {NAME_RE.pattern}: {self.name!r}")


class Receiver:
    """Foreground receiver: addressable as SendMessage(<name>), stores what arrives in its Inbox.

    It never forks, has no owner process and no idle timeout; a supervisor (launchd KeepAlive) restarts it.
    """

    def __init__(self, config: ReceiverConfig, log: Callable[[str], None] | None = None) -> None:
        self.config = config
        self.log: Callable[[str], None] = log or _stderr_log
        self.pid = os.getpid()
        self.dir = state_dir(config.name, config.state_root)
        self.inbox = Inbox(self.dir)
        self.socket_path = str(config.socket_dir / f"{self.pid}.sock")
        self.address = uds_address(self.socket_path)
        self.registry = registry_dirs(config.config_dirs)
        self._token = uuid.uuid4().hex
        self._stop = threading.Event()
        self._lock_file: IO[str] | None = None
        self._server: socket.socket | None = None
        self._registered: list[Path] = []
        self._proc_start: str | None = None
        self._started_ms = int(time.time() * 1000)
        self._session_id = str(uuid.uuid4())

    # --- lifecycle -------------------------------------------------------------------------------------------

    def start(self) -> None:
        if len(self.socket_path) > MAX_SOCKET_PATH:
            raise ValueError(f"socket path too long for AF_UNIX ({len(self.socket_path)} bytes): {self.socket_path}")
        self.dir.mkdir(parents=True, exist_ok=True)
        self._acquire_lock()
        self._check_conflicts()
        self.inbox.prepare()
        self.config.socket_dir.mkdir(parents=True, exist_ok=True)
        _unlink(self.socket_path)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(self.socket_path)
        os.chmod(self.socket_path, 0o600)
        server.listen(64)
        server.settimeout(0.5)
        self._server = server
        self._proc_start = _proc_start(self.pid)
        self._register()
        _write_private(self.dir / "receiver.json", {
            "pid": self.pid, "name": self.config.name, "socket_path": self.socket_path, "address": self.address,
            "config_dirs": [str(d) for d in self.registry], "started_at": self._started_ms})
        self.log(f"ccpeer receiver {self.config.name} (pid {self.pid}) listening on {self.socket_path}; "
                 f"registered in {', '.join(str(d) for d in self.registry)}")

    def stop(self) -> None:
        self._stop.set()

    def serve_forever(self) -> None:
        server = self._server
        if server is None:
            raise RuntimeError("start() first")
        next_beat = time.monotonic() + self.config.heartbeat_sec
        while not self._stop.is_set():
            if time.monotonic() >= next_beat:
                self._register()
                next_beat = time.monotonic() + self.config.heartbeat_sec
            try:
                conn, _ = server.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._stop.is_set():
                    break
                raise
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def close(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
        _unlink(self.socket_path)
        for sdir in self._registered:
            _unlink(sdir / f"{self.pid}.json")
            _unlink(sdir / key_filename(self.pid, self.socket_path))
        self._registered = []
        info = load_json(self.dir / "receiver.json")
        if info is not None and info.get("pid") == self.pid:
            _unlink(self.dir / "receiver.json")
        if self._lock_file is not None:
            self._lock_file.close()
            self._lock_file = None

    # --- registration ----------------------------------------------------------------------------------------

    def _acquire_lock(self) -> None:
        lock = open(self.dir / "receiver.lock", "a+", encoding="utf-8")
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock.seek(0)
            holder = lock.read().strip() or "unknown"
            lock.close()
            raise ReceiverConflict(f"receiver {self.config.name} is already running from {self.dir} "
                                   f"(pid {holder})") from None
        lock.seek(0)
        lock.truncate()
        lock.write(str(self.pid))
        lock.flush()
        self._lock_file = lock

    def _check_conflicts(self) -> None:
        # Policy: a live session already answering to this name wins and we refuse to start; a registration whose
        # process or socket is gone and that a standalone receiver left behind is taken over (its files removed).
        for reg in scan_registrations(self.registry):
            if reg.data.get("name") != self.config.name or reg.pid == self.pid:
                continue
            pid, sock = reg.pid, reg.socket_path
            if pid is not None and sock is not None and pid_alive(pid) and _connectable(sock):
                raise ReceiverConflict(f"a session named {self.config.name!r} is already live "
                                       f"(pid {pid}, {reg.path}); stop it before starting another receiver")
            if reg.data.get("standalone") and pid is not None:
                for stale in reg.path.parent.glob(f"{pid}.*.key"):
                    _unlink(stale)
                _unlink(reg.path)
                if sock is not None and Path(sock).parent == self.config.socket_dir:
                    _unlink(sock)
                self.log(f"took over stale registration of {self.config.name!r} (dead pid {pid}): {reg.path}")

    def _registration(self) -> dict[str, Any]:
        now = int(time.time() * 1000)
        reg: dict[str, Any] = {
            "pid": self.pid, "sessionId": self._session_id, "cwd": os.getcwd(), "startedAt": self._started_ms,
            "version": "ccpeer", "peerProtocol": PEER_PROTOCOL, "peerFeatures": [], "kind": "interactive",
            "entrypoint": "cli", "pidDomain": sys.platform, "messagingSocketPath": self.socket_path,
            "name": self.config.name, "nameSource": "explicit", "nameSince": self._started_ms, "status": "idle",
            "updatedAt": now, "statusUpdatedAt": now, "standalone": True}
        if self._proc_start is not None:
            reg["procStart"] = self._proc_start
        return reg

    def _register(self) -> None:
        # Rewritten on every heartbeat too, so a registration swept by someone else comes back.
        reg = self._registration()
        key: dict[str, Any] = {"peerToken": self._token, "pidDomain": sys.platform}
        if self._proc_start is not None:
            key["procStart"] = self._proc_start
        for sdir in self.registry:
            try:
                sdir.mkdir(mode=0o700, parents=True, exist_ok=True)
                _write_private(sdir / key_filename(self.pid, self.socket_path), key)
                _write_private(sdir / f"{self.pid}.json", reg)
            except OSError as exc:
                self.log(f"cannot register in {sdir}: {exc}")
                continue
            if sdir not in self._registered:
                self._registered.append(sdir)

    # --- connections -----------------------------------------------------------------------------------------

    def _handle(self, conn: socket.socket) -> None:
        try:
            conn.settimeout(self.config.read_timeout_sec)
            reader = conn.makefile("rb")
            while True:
                line = reader.readline(MAX_LINE_BYTES + 1)
                if not line:
                    return
                if len(line) > MAX_LINE_BYTES:
                    self.log("dropping connection: frame larger than MAX_LINE_BYTES")
                    return
                if not line.strip():
                    continue
                try:
                    frame = json.loads(line)
                except ValueError:
                    self.log("dropping connection: frame is not JSON")
                    return
                if not isinstance(frame, dict):
                    continue
                obj = cast(dict[str, Any], frame)
                if obj.get("type") == "auth":
                    if obj.get("token") != self._token:
                        self.log("dropping connection: wrong auth token")
                        return
                    continue
                # The socket is 0600, so only this user can connect; frames without an auth line are accepted
                # the way earlier standalone receivers accepted them.
                self._accept(obj)
        except (OSError, TimeoutError) as exc:
            self.log(f"connection error: {exc}")
        finally:
            conn.close()

    def _accept(self, frame: dict[str, Any]) -> None:
        record = frame_to_record(frame, now=time.time())
        if record is None:
            self.log(f"ignoring frame of type {frame.get('type')!r}")
            return
        stored = self.inbox.record(record)
        self.log(f"{'stored' if stored else 'duplicate'} {record['type']} {record['msg_id']} "
                 f"from {record.get('from_name') or record.get('from_addr')}")


def run(config: ReceiverConfig, log: Callable[[str], None] | None = None) -> int:
    receiver = Receiver(config, log)

    def on_signal(signum: int, _frame: FrameType | None) -> None:
        receiver.log(f"received {signal.Signals(signum).name}; shutting down")
        receiver.stop()

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, on_signal)
    try:
        receiver.start()
    except ReceiverConflict as exc:
        receiver.log(str(exc))
        receiver.close()
        return EXIT_CONFLICT
    except BaseException:
        receiver.close()
        raise
    try:
        receiver.serve_forever()
    finally:
        receiver.close()
        receiver.log(f"ccpeer receiver {config.name} (pid {receiver.pid}) stopped")
    return 0


def config_from(name: str, config_dirs: Iterable[PathLike] | None, state_root: PathLike | None = None,
                socket_dir: PathLike | None = None, heartbeat_sec: float = 60.0) -> ReceiverConfig:
    dirs = tuple(Path(os.path.expanduser(str(d))) for d in config_dirs) if config_dirs else default_config_dirs()
    return ReceiverConfig(
        name=name, config_dirs=dirs,
        state_root=Path(os.path.expanduser(str(state_root))) if state_root is not None else DEFAULT_STATE_ROOT,
        socket_dir=Path(str(socket_dir)) if socket_dir is not None else DEFAULT_SOCKET_DIR,
        heartbeat_sec=heartbeat_sec)
