import contextlib
import os
import signal
import subprocess
import time
from pathlib import Path


def processes_under(root: Path) -> list[tuple[int, str]]:
    listing = subprocess.run(["ps", "-A", "-ww", "-o", "pid=", "-o", "command="], stdout=subprocess.PIPE, text=True,
                             check=True).stdout
    found = []
    for line in listing.splitlines():
        pid, _, command = line.strip().partition(" ")
        if str(root) in command and int(pid) != os.getpid():
            found.append((int(pid), command))
    return found


def kill_processes_under(root: Path, timeout: float = 10.0) -> None:
    # run_updating.py starts the daemon in its own session, so neither killing the supervisor nor its process group
    # reaches it; every process a case starts (supervisor, each daemon generation, gated CLI) names the case's temp
    # root on its command line instead.
    deadline = time.monotonic() + timeout
    while found := processes_under(root):
        for pid, _ in found:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
        if time.monotonic() > deadline:
            raise AssertionError(f"processes still running under {root}: {found}")
        time.sleep(0.05)


def stop_supervisor(proc: subprocess.Popen, root: Path, grace: float = 5.0) -> None:
    # TERM first so a healthy supervisor drains its daemon; anything left after the grace (a stalled daemon, a
    # supervisor stuck in its wait) is killed by the root it names, then the direct child is reaped.
    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=grace)
    kill_processes_under(root)
    proc.wait(timeout=10)
    if found := processes_under(root):
        raise AssertionError(f"processes still running under {root}: {found}")
