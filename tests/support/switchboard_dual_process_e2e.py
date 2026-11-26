from __future__ import annotations

import os
import signal
import socket
import sqlite3
import subprocess
import time
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any


# Real `python -m switchboard.app` and Hibernaut main() as separate processes on loopback ports.
REPO = Path(__file__).resolve().parents[2]
HIBERNAUT_ENTRY = REPO / "tests" / "support" / "dual_process_hibernaut.py"


def free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def wait_until(predicate: Callable[[], bool], timeout: float, message: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    if not predicate():
        raise AssertionError(f"timed out after {timeout}s waiting for {message}")


def query(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    if not db_path.exists():
        return []
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)) as conn:
        try:
            return [tuple(row) for row in conn.execute(sql, params).fetchall()]
        except sqlite3.OperationalError:
            # The child may not have created the table yet.
            return []


class Child:
    def __init__(self, name: str, argv: list[str], env: dict[str, str], log_dir: Path) -> None:
        self.name = name
        self.argv = argv
        self.env = env
        self.log_dir = log_dir
        self.proc: subprocess.Popen[bytes] | None = None
        self.starts = 0

    def start(self) -> None:
        assert self.proc is None or self.proc.poll() is not None, f"{self.name} already running"
        self.starts += 1
        log = open(self.log_dir / f"{self.name}.{self.starts}.log", "wb")  # noqa: SIM115
        with log:
            self.proc = subprocess.Popen(
                self.argv, cwd=str(self.log_dir), env=self.env, stdout=log, stderr=subprocess.STDOUT
            )

    def stop(self, timeout: float = 15.0) -> int:
        proc = self.proc
        if proc is None or proc.poll() is not None:
            return proc.returncode if proc is not None else 0
        proc.send_signal(signal.SIGTERM)
        try:
            return proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            return proc.wait(timeout=5)

    def assert_alive(self) -> None:
        assert self.proc is not None and self.proc.poll() is None, f"{self.name} exited:\n{self.logs()}"

    def logs(self) -> str:
        return "\n".join(
            f"--- {path.name}\n{path.read_text(errors='replace')[-4000:]}"
            for path in sorted(self.log_dir.glob(f"{self.name}.*.log"))
        )


def child_env(**extra: str) -> dict[str, str]:
    # Only test placeholders: no SWITCHBOARD_*/GITHUB_*/ANTHROPIC_* value from the developer's shell leaks in.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("SWITCHBOARD_", "GITHUB_", "FEISHU_", "ANTHROPIC_", "HIBERNAUT_"))
    }
    env["PYTHONPATH"] = os.pathsep.join([str(REPO / "src"), str(REPO)])
    env["PYTHONUNBUFFERED"] = "1"
    env.update(extra)
    return env
