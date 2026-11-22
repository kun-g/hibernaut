from __future__ import annotations

import json
import os
import urllib.parse
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from ccpeer.errors import AmbiguousTarget, TargetNotFound
from ccpeer.protocol import key_filename, uds_address

PathLike = str | os.PathLike[str]


def sessions_dir(config_dir: PathLike) -> Path:
    path = Path(os.path.expanduser(str(config_dir)))
    return path if path.name == "sessions" else path / "sessions"


def default_config_dirs(env: Mapping[str, str] | None = None) -> tuple[Path, ...]:
    # A session started with CLAUDE_CONFIG_DIR registers under that dir, not ~/.claude.
    env = os.environ if env is None else env
    home = env.get("HOME") or os.path.expanduser("~")
    dirs = [Path(home) / ".claude"]
    extra = env.get("CLAUDE_CONFIG_DIR")
    if extra and Path(extra) not in dirs:
        dirs.append(Path(extra))
    return tuple(dirs)


def registry_dirs(config_dirs: Iterable[PathLike] | None) -> tuple[Path, ...]:
    out: list[Path] = []
    seen: set[str] = set()
    for d in (default_config_dirs() if config_dirs is None else config_dirs):
        sdir = sessions_dir(d)
        real = os.path.realpath(sdir)
        if real not in seen:
            seen.add(real)
            out.append(sdir)
    return tuple(out)


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return cast(dict[str, Any], data) if isinstance(data, dict) else None


@dataclass(frozen=True)
class Registration:
    path: Path
    data: Mapping[str, Any]

    @property
    def pid(self) -> int | None:
        pid = self.data.get("pid")
        return pid if isinstance(pid, int) and pid > 0 else None

    @property
    def socket_path(self) -> str | None:
        sock = self.data.get("messagingSocketPath")
        return sock if isinstance(sock, str) and sock else None


def scan_registrations(dirs: Iterable[Path]) -> list[Registration]:
    found: list[Registration] = []
    for sdir in dirs:
        try:
            files = sorted(sdir.glob("*.json"))
        except OSError:
            continue
        for f in files:
            data = load_json(f)
            if data is not None:
                found.append(Registration(f, data))
    return found


def _str(data: Mapping[str, Any], key: str) -> str | None:
    value = data.get(key)
    return value if isinstance(value, str) else None


@dataclass(frozen=True)
class Session:
    pid: int
    socket_path: str
    name: str | None
    session_id: str | None
    version: str | None
    peer_protocol: int | None
    status: str | None
    cwd: str | None
    standalone: bool
    registry_dirs: tuple[Path, ...]
    raw: Mapping[str, Any] = field(repr=False)

    @property
    def address(self) -> str:
        return uds_address(self.socket_path)

    def label(self) -> str:
        return f"{self.name or '?'}(pid {self.pid}, {self.registry_dirs[0]})"

    def token(self) -> str | None:
        exact = key_filename(self.pid, self.socket_path)
        for sdir in self.registry_dirs:
            data = load_json(sdir / exact)
            if data is not None and isinstance(data.get("peerToken"), str):
                return cast(str, data["peerToken"])
        for sdir in self.registry_dirs:
            for f in sorted(sdir.glob(f"{self.pid}.*.key")):
                data = load_json(f)
                if data is not None and isinstance(data.get("peerToken"), str):
                    return cast(str, data["peerToken"])
        return None

    def summary(self) -> dict[str, Any]:
        return {"pid": self.pid, "name": self.name, "session_id": self.session_id, "status": self.status,
                "version": self.version, "peer_protocol": self.peer_protocol, "socket": self.socket_path,
                "cwd": self.cwd, "standalone": self.standalone,
                "registry_dirs": [str(d) for d in self.registry_dirs]}


def discover(config_dirs: Iterable[PathLike] | None = None) -> list[Session]:
    by_pid: dict[int, tuple[Registration, list[Path]]] = {}
    for reg in scan_registrations(registry_dirs(config_dirs)):
        pid, sock = reg.pid, reg.socket_path
        if pid is None or sock is None or not pid_alive(pid) or not os.path.exists(sock):
            continue
        if pid in by_pid:
            by_pid[pid][1].append(reg.path.parent)
        else:
            by_pid[pid] = (reg, [reg.path.parent])
    sessions: list[Session] = []
    for pid, (reg, dirs) in by_pid.items():
        d = reg.data
        proto = d.get("peerProtocol")
        sessions.append(Session(
            pid=pid, socket_path=cast(str, reg.socket_path), name=_str(d, "name"), session_id=_str(d, "sessionId"),
            version=_str(d, "version"), peer_protocol=proto if isinstance(proto, int) else None,
            status=_str(d, "status"), cwd=_str(d, "cwd"), standalone=bool(d.get("standalone")),
            registry_dirs=tuple(dirs), raw=d))
    sessions.sort(key=lambda s: (s.name or "", s.pid))
    return sessions


def resolve(target: str, config_dirs: Iterable[PathLike] | None = None,
            sessions: Sequence[Session] | None = None) -> Session:
    pool = list(discover(config_dirs) if sessions is None else sessions)
    sock = urllib.parse.unquote(target[4:]) if target.startswith("uds:") else target
    hits = [s for s in pool
            if str(s.pid) == target or s.session_id == target or s.name == target
            or s.socket_path == sock or s.address == target]
    if not hits:
        raise TargetNotFound(f"no live Claude Code session matches {target!r} "
                             f"(searched {', '.join(str(d) for d in registry_dirs(config_dirs))})")
    if len(hits) > 1:
        raise AmbiguousTarget(f"{target!r} matches {len(hits)} sessions: "
                              f"{', '.join(s.label() for s in hits)}; use a pid or sessionId")
    return hits[0]
