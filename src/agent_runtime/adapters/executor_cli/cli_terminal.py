#!/usr/bin/env python3
# Runs inside a visible Orca terminal: starts the CLI from the spec file, shows its stdout and stderr as they come
# and copies both into the run's files, writes the exit code and exits, which ends the terminal. Hibernaut reads
# only those files, never the screen. Started as `python -I <this file> <spec.json>` and uses only the standard
# library, so it imports nothing from the worktree it runs in. claude_gate.py imports its process-group helpers.
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
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any

POLL_SECONDS = 0.25
GRACE_SECONDS = 3.0
STOP_SIGNALS = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)


class Stopped(Exception):
    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


def _on_signal(signum: int, _frame: object) -> None:
    raise Stopped(signum)


def stop_on_signals() -> None:
    for sig in STOP_SIGNALS:
        signal.signal(sig, _on_signal)


def ignore_signals() -> None:
    # Once cleanup starts, a second signal must not cut it short.
    for sig in STOP_SIGNALS:
        signal.signal(sig, signal.SIG_IGN)


def exited(pid: int, seconds: float) -> bool:
    # Waits up to `seconds` for the child to exit without reaping it: an unreaped child, even a zombie, keeps its
    # pid, so the process group it leads cannot be taken over by another process.
    end = time.monotonic() + seconds
    while os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
        if time.monotonic() >= end:
            return False
        time.sleep(min(0.05, max(0.0, end - time.monotonic())))
    return True


@contextmanager
def _signals_held() -> Iterator[None]:
    # Reaping and recording the exit status must not be split by a signal, or the cleanup would see a live leader.
    signal.pthread_sigmask(signal.SIG_BLOCK, STOP_SIGNALS)
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, STOP_SIGNALS)


def end_group(proc: subprocess.Popen[bytes], grace: float | None = None) -> None:
    # Ends the process group `proc` leads (it was started in its own session or group) and reaps `proc`. Every
    # signal goes out while `proc` is unreaped; once it is reaped its pid is never signalled again.
    if proc.returncode is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        exited(proc.pid, GRACE_SECONDS if grace is None else grace)
        # The leader is still running or a zombie here, so this reaches only what is left of its own group.
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        # macOS answers EPERM for a group whose only member is the zombie leader.
        pass
    with _signals_held():
        proc.wait()


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


def main(spec_path: str) -> int:
    running: list[subprocess.Popen[bytes]] = []
    stop_on_signals()
    try:
        return _run(Path(spec_path), running)
    except Stopped as stop:
        return 128 + stop.signum
    finally:
        ignore_signals()
        for proc in running:
            end_group(proc)


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
    while not exited(proc.pid, POLL_SECONDS):
        # Hibernaut removes the run directory once it stops waiting (timeout, interrupt, failure) and then closes the
        # terminal; this covers a close that never arrives.
        if not spec_path.exists():
            print(f"\n[hibernaut] Hibernaut stopped waiting for {argv[0]}; stopping it.", flush=True)
            return 1
        if time.monotonic() >= deadline:
            print(f"\n[hibernaut] {argv[0]} passed its deadline; stopping it.", flush=True)
            return 1
    # Whatever the finished CLI left in its group could hold the pipes open and outlive the run.
    end_group(proc)
    running.remove(proc)
    for thread in threads:
        thread.join(timeout=GRACE_SECONDS)
    # Written last: its presence means both copies are complete.
    _write_atomic(exit_path, {"exit": proc.returncode})
    print(f"\n[hibernaut] {argv[0]} exited with code {proc.returncode}.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
