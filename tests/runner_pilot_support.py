from __future__ import annotations

import atexit
import hashlib
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from functools import cache
from pathlib import Path

from runner.protocol_check import PROTOCOL_FILES

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
RUNNER_ID = "pilot-node"


def git(*args: str, cwd: Path) -> str:
    return subprocess.run(
        ["git", "-c", "maintenance.auto=false", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


@cache
def _protocol_seed() -> tuple[Path, str]:
    tmp = tempfile.TemporaryDirectory(prefix="runner_protocol_seed_")
    atexit.register(tmp.cleanup)
    repo = Path(tmp.name)
    git("init", "-q", "-b", "main", cwd=repo)
    git("config", "user.email", "test@example.com", cwd=repo)
    git("config", "user.name", "Test", cwd=repo)
    for path in PROTOCOL_FILES:
        (repo / path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / path, repo / path)
    return repo, commit(repo, "protocol files")


def protocol_repo(repo: Path) -> str:
    seed, head = _protocol_seed()
    shutil.copytree(seed, repo, symlinks=True)
    return head


def commit(repo: Path, message: str) -> str:
    git("add", "-A", cwd=repo)
    git("commit", "-q", "-m", message, cwd=repo)
    return git("rev-parse", "HEAD", cwd=repo)


def recomputed_manifest(repo: Path, commit_id: str) -> str:
    # Independent of runner.protocol_check: git objects and SHA-256 only.
    lines = ""
    for path in sorted(PROTOCOL_FILES):
        blob = subprocess.run(
            ["git", "cat-file", "blob", f"{commit_id}:{path}"], cwd=repo, check=True, capture_output=True
        ).stdout
        lines += f"{hashlib.sha256(blob).hexdigest()}  {path}\n"
    return hashlib.sha256(lines.encode()).hexdigest()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def write_token(path: Path) -> str:
    token = secrets.token_urlsafe(32)
    path.touch(mode=0o600)
    path.write_text(token + "\n", encoding="ascii")
    return token


def runner_toml(path: Path, *, repo: Path, port: int, token_file: Path, runner_id: str = RUNNER_ID, slots: int = 1) -> Path:
    path.write_text(
        f'[runner]\nrunner_id = "{runner_id}"\nmax_concurrent = {slots}\nworktree_backend = "git"\n'
        'interactive_surface = "none"\nreadonly_only = true\nstate_dir = "state"\nlogs_dir = "logs"\n'
        '[namespaces.hb]\nprefix = "hb"\nallowed_callers = ["hibernaut"]\n'
        f'[projects.hibernaut]\ncheckout_path = "{repo}"\ndefault_base_ref = "main"\n'
        f'[relay]\nurl = "ws://127.0.0.1:{port}/ws/runner"\ntoken_file = "{token_file}"\n',
        encoding="utf-8",
    )
    return path


def start_service(config: Path, log: Path, *, entry: tuple[str, ...] = ("-m", "runner.service")) -> subprocess.Popen[bytes]:
    # A restarted service appends to the same log. Wait for the new process to publish its own ready line.
    previous = log.stat().st_size if log.exists() else 0
    with log.open("ab") as stream:
        proc = subprocess.Popen(
            [sys.executable, *entry, "--config", str(config)],
            cwd=config.parent,
            env={**os.environ, "PYTHONPATH": str(SRC), "GH_TOKEN": "synthetic-service-token"},
            stdout=stream,
            stderr=stream,
        )
    deadline = time.monotonic() + 20
    while b" serving " not in log.read_bytes()[previous:]:
        assert proc.poll() is None and time.monotonic() < deadline, log.read_text(encoding="utf-8")
        time.sleep(0.05)
    return proc


def children_of(pid: int) -> dict[int, str]:
    listing = subprocess.run(
        ["ps", "-A", "-ww", "-o", "pid=", "-o", "ppid=", "-o", "command="], stdout=subprocess.PIPE, text=True, check=True
    ).stdout
    found: dict[int, str] = {}
    for line in listing.splitlines():
        child, parent, command = line.split(None, 2)
        if int(parent) == pid:
            found[int(child)] = command
    return found


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def report_of(stdout: str) -> dict[str, object]:
    parsed: dict[str, object] = json.loads(stdout)
    return parsed
