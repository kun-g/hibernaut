#!/usr/bin/env python3
# Runs inside a visible Orca terminal: starts the CLI from the spec file, shows its stdout and stderr as they come
# and copies both into the run's files, writes the exit code and exits, which ends the terminal. Hibernaut reads
# only those files, never the screen. Started as `python -I <this file> <spec.json>` and uses only the standard
# library, so it imports nothing from the worktree it runs in.
#
# Orca can report a terminal closed without its signal ever reaching this process, so the wrapper also stops on its
# own: on SIGHUP/SIGTERM/SIGINT, when Hibernaut removes the run directory while the CLI still runs, and at the CLI's
# deadline. Every way out ends the CLI's process group first.
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import IO, Any

_POLL_SECONDS = 0.25
_GRACE_SECONDS = 3.0
_STOP_SIGNALS = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)


class _Stopped(Exception):
    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


def _on_signal(signum: int, _frame: object) -> None:
    raise _Stopped(signum)


def _write_atomic(path: Path, body: dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(body), encoding="utf-8")
    os.replace(tmp, path)


def _tee(source: IO[bytes], screen: IO[bytes], path: str) -> None:
    with open(path, "wb") as copy:
        while chunk := os.read(source.fileno(), 65536):
            copy.write(chunk)
            copy.flush()
            try:
                screen.write(chunk)
                screen.flush()
            except OSError:
                pass


def _end_group(proc: subprocess.Popen[bytes]) -> None:
    # The CLI leads its own session and process group, so this also reaches anything it started.
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    try:
        proc.wait(timeout=_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    proc.wait()


def main(spec_path: str) -> int:
    running: list[subprocess.Popen[bytes]] = []
    for sig in _STOP_SIGNALS:
        signal.signal(sig, _on_signal)
    try:
        return _run(Path(spec_path), running)
    except _Stopped as stop:
        return 128 + stop.signum
    finally:
        # A second signal must not cut the cleanup short.
        for sig in _STOP_SIGNALS:
            signal.signal(sig, signal.SIG_IGN)
        for proc in running:
            _end_group(proc)


def _run(spec_path: Path, running: list[subprocess.Popen[bytes]]) -> int:
    spec: dict[str, Any] = json.loads(spec_path.read_text(encoding="utf-8"))
    argv: list[str] = spec["argv"]
    exit_path = Path(spec["exit.json"])
    deadline = time.monotonic() + float(spec["timeout_seconds"])
    print(f"[hibernaut] {spec['title']}: running {argv[0]} in {spec['cwd']}", flush=True)
    try:
        # A session of its own: the group can be ended as a whole, and a CLI that opens /dev/tty fails instead of
        # stopping in the background.
        proc = subprocess.Popen(argv, cwd=spec["cwd"], env={**os.environ, **spec["env"]}, start_new_session=True,
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as exc:
        _write_atomic(exit_path, {"exit": None, "error": f"cannot start '{argv[0]}': {exc.strerror or exc}"})
        print(f"[hibernaut] cannot start {argv[0]}: {exc.strerror or exc}", file=sys.stderr, flush=True)
        return 1
    running.append(proc)
    assert proc.stdout is not None and proc.stderr is not None
    threads = [
        threading.Thread(target=_tee, args=(proc.stdout, sys.stdout.buffer, spec["stdout"]), daemon=True),
        threading.Thread(target=_tee, args=(proc.stderr, sys.stderr.buffer, spec["stderr"]), daemon=True),
    ]
    for thread in threads:
        thread.start()
    while True:
        try:
            code = proc.wait(timeout=_POLL_SECONDS)
            break
        except subprocess.TimeoutExpired:
            pass
        # Hibernaut removes the run directory once it stops waiting (timeout, interrupt, failure) and then closes the
        # terminal; this covers a close that never arrives.
        if not spec_path.exists():
            print(f"\n[hibernaut] Hibernaut stopped waiting for {argv[0]}; stopping it.", flush=True)
            return 1
        if time.monotonic() >= deadline:
            print(f"\n[hibernaut] {argv[0]} passed its deadline; stopping it.", flush=True)
            return 1
    # Whatever the finished CLI left behind could hold the pipes open and outlive the run.
    _end_group(proc)
    # The group is gone and its ID may be reused, so it is never signalled again.
    running.clear()
    for thread in threads:
        thread.join(timeout=_GRACE_SECONDS)
    # Written last: its presence means both copies are complete.
    _write_atomic(exit_path, {"exit": code})
    print(f"\n[hibernaut] {argv[0]} exited with code {code}.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
