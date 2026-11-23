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
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any

POLL_SECONDS = 0.25
GRACE_SECONDS = 3.0
PROC = Path("/proc")
# Orca's hooks in the CLI report its session with these, and Orca then takes the end of a one-shot run for an
# interrupted session and resumes it interactively in a new tab. Without them Orca never registers the session.
ORCA_SESSION_VARS = ("ORCA_PANE_KEY", "ORCA_AGENT_PANE", "ORCA_TAB_ID")
ORCA_SESSION_PREFIXES = ("ORCA_AGENT_HOOK_", "ORCA_AGENT_LAUNCH")
STOP_SIGNALS = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)


class Stopped(Exception):
    def __init__(self, signum: int) -> None:
        super().__init__(signum)
        self.signum = signum


_starting = False
_stop: int | None = None


def _on_signal(signum: int, _frame: object) -> None:
    # Raises once at most: whatever the stop interrupts, the cleanup after it runs through (see supervised).
    global _stop
    if _stop is None:
        _stop = signum
        if not _starting:
            raise Stopped(signum)


def check_stop() -> None:
    # Python drops an exception raised in a finalizer or a weakref callback, and the stop raised there with it.
    if _stop is not None:
        raise Stopped(_stop)


@contextmanager
def starting() -> Iterator[None]:
    # A stop raised inside Popen would lose the process it just started, so it waits until the caller has recorded
    # it. The signals are not blocked for this: the started process would inherit the mask and ignore SIGTERM.
    global _starting
    _starting = True
    try:
        check_stop()
        yield
    finally:
        _starting = False
        check_stop()


def supervised(body: Callable[[list[subprocess.Popen[bytes]]], int],
               grace: Callable[[], float | None] = lambda: None) -> int:
    # Runs `body`, which keeps the processes it started in the list it gets, and ends their groups on every way out,
    # with the grace (see end_group) known by then.
    running: list[subprocess.Popen[bytes]] = []
    try:
        try:
            for sig in STOP_SIGNALS:
                signal.signal(sig, _on_signal)
            return body(running)
        finally:
            _end_all(running, grace())
    except Stopped as stop:
        # The stop may have cut the cleanup above short, or come before it began.
        _end_all(running, grace())
        return 128 + stop.signum


def _end_all(running: list[subprocess.Popen[bytes]], grace: float | None) -> None:
    for proc in running:
        end_group(proc, grace)
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


def _group_running(pgid: int) -> bool:
    # Whether the group of an unreaped leader has a member besides zombies. macOS refuses to signal a group of only
    # zombies (EPERM); Linux accepts it, so /proc answers there.
    try:
        os.killpg(pgid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    try:
        pids = [name for name in os.listdir(PROC) if name.isdigit()]
    except OSError:
        return True
    seen = False
    for pid in pids:
        try:
            stat = (PROC / pid / "stat").read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        # After the command name, which may hold spaces and parentheses: state, ppid, pgrp, ..., num_threads (18th).
        fields = stat.rpartition(")")[2].split()
        if len(fields) < 18 or int(fields[2]) != pgid:
            continue
        seen = True
        # A leader thread that exited shows Z while its other threads still run.
        if fields[0] not in ("Z", "X") or int(fields[17]) > 1:
            return True
    return not seen


def _group_ended(pgid: int, seconds: float, *, kill: bool = False) -> bool:
    # With `kill` SIGKILL goes out again on every look: a member that forks while the group is being signalled can
    # leave a child the signal missed.
    end = time.monotonic() + seconds
    while True:
        if kill:
            os.killpg(pgid, signal.SIGKILL)
        if not _group_running(pgid):
            return True
        if time.monotonic() >= end:
            return False
        time.sleep(0.02)


def end_group(proc: subprocess.Popen[bytes], grace: float | None = None) -> None:
    # Ends the process group `proc` leads (it was started in its own session or group), waits until nothing in it
    # runs any more and reaps `proc`. Every signal goes out while `proc` is unreaped; once it is reaped its pid is
    # never signalled again.
    if proc.returncode is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        _group_ended(proc.pid, GRACE_SECONDS if grace is None else grace)
        # The leader is still running or a zombie here, so this reaches only what is left of its own group.
        _group_ended(proc.pid, GRACE_SECONDS, kill=True)
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
    return supervised(lambda running: _run(Path(spec_path), running))


def one_shot_env(env: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in env.items() if k not in ORCA_SESSION_VARS and not k.startswith(ORCA_SESSION_PREFIXES)}


def _run(spec_path: Path, running: list[subprocess.Popen[bytes]]) -> int:
    spec: dict[str, Any] = json.loads(spec_path.read_text(encoding="utf-8"))
    argv: list[str] = spec["argv"]
    exit_path = Path(spec["exit.json"])
    deadline = time.monotonic() + float(spec["timeout_seconds"])
    stdin_path: str | None = spec.get("stdin")
    print(f"[hibernaut] {spec['title']}: running {argv[0]} in {spec['cwd']}", flush=True)
    try:
        # A session of its own: the group can be ended as a whole, and a CLI that opens /dev/tty fails instead of
        # stopping in the background.
        with open(stdin_path or os.devnull, "rb") as stdin, starting():
            proc = subprocess.Popen(argv, cwd=spec["cwd"], env=one_shot_env({**os.environ, **spec["env"]}),
                                    start_new_session=True, stdin=stdin, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            running.append(proc)
    except OSError as exc:
        _write_atomic(exit_path, {"exit": None, "error": f"cannot start '{argv[0]}': {exc.strerror or exc}"})
        print(f"[hibernaut] cannot start {argv[0]}: {exc.strerror or exc}", file=sys.stderr, flush=True)
        return 1
    assert proc.stdout is not None and proc.stderr is not None
    threads = [
        threading.Thread(target=_tee, args=(proc.stdout, sys.stdout.buffer, spec["stdout"]), daemon=True),
        threading.Thread(target=_tee, args=(proc.stderr, sys.stderr.buffer, spec["stderr"]), daemon=True),
    ]
    for thread in threads:
        thread.start()
    while not exited(proc.pid, POLL_SECONDS):
        check_stop()
        # Hibernaut removes the run directory once it stops waiting (timeout, interrupt, failure) and then closes the
        # terminal; this covers a close that never arrives.
        if not spec_path.exists():
            print(f"\n[hibernaut] Hibernaut stopped waiting for {argv[0]}; stopping it.", flush=True)
            return 1
        if time.monotonic() >= deadline:
            print(f"\n[hibernaut] {argv[0]} passed its deadline; stopping it.", flush=True)
            return 1
    # Whatever the finished CLI left in its group could hold the pipes open and outlive the run.
    end_group(proc, grace=0)
    running.remove(proc)
    for thread in threads:
        thread.join(timeout=GRACE_SECONDS)
    # Written last: its presence means both copies are complete.
    _write_atomic(exit_path, {"exit": proc.returncode})
    print(f"\n[hibernaut] {argv[0]} exited with code {proc.returncode}.", flush=True)
    return 0


if __name__ == "__main__":
    # Python's own shutdown would replace the exit code when it cannot flush output the closed terminal refused, or
    # abort on a copying thread stuck in a write. Everything that matters has been written.
    os._exit(main(sys.argv[1]))
