import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def log(message: str) -> None:
    print(f"[auto-update] {message}", file=sys.stderr, flush=True)


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True, timeout=60, check=True,
    ).stdout.strip()


def clean_main() -> bool:
    return git("branch", "--show-current") == "main" and not git("status", "--porcelain")


def candidate() -> tuple[str, str] | None:
    if not clean_main():
        log("Skipping update: expected a clean main checkout.")
        return None
    old = git("rev-parse", "HEAD")
    ref = "refs/hibernaut/update-main"
    git("fetch", "--quiet", "--no-tags", "--no-write-fetch-head", "origin", f"+refs/heads/main:{ref}")
    new = git("rev-parse", f"{ref}^{{commit}}")
    if old == new:
        return None
    if git("merge-base", old, new) != old:
        log("Skipping update: origin/main is not a fast-forward.")
        return None
    return old, new


TICK = 0.25


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the daemon and fast-forward from origin/main. Extra flags go to the daemon.", allow_abbrev=False)
    parser.add_argument("--interval", type=float, default=300, help="seconds between checks (default: 300)")
    parser.add_argument("--wait-log-interval", type=float, default=60,
                        help="seconds between log lines while waiting for the daemon to exit (default: 60)")
    args, daemon_args = parser.parse_known_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")
    if args.wait_log_interval <= 0:
        parser.error("--wait-log-interval must be positive")

    signals = 0

    def on_signal(*_: object) -> None:
        # Runs between two bytecodes of the main thread, possibly inside a critical section of a lock it holds (an
        # Event's Condition, Popen's waitpid lock, the stderr buffer); taking any lock here can deadlock the
        # supervisor for good, so it only counts and the main loop acts on the count.
        nonlocal signals
        signals += 1

    # The daemon runs in its own session (so a terminal's Ctrl-C is not delivered twice): a closed terminal's SIGHUP
    # reaches only this process, which must stop the daemon too. SIGKILL cannot be handled and leaves the daemon running.
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, on_signal)

    def stop(child: subprocess.Popen[bytes]) -> None:
        # The first stop signal (or an update) lets the daemon drain its Runs in progress while it keeps serving
        # ingress; a second signal is passed on so the daemon exits without waiting for them. No upper bound: a
        # stalled daemon is reported, not killed.
        if child.poll() is None:
            child.terminate()
        started = time.monotonic()
        next_report = started + args.wait_log_interval
        forwarded = False
        while True:
            try:
                child.wait(timeout=TICK)
                return
            except subprocess.TimeoutExpired:
                pass
            if signals > 1 and not forwarded:
                forwarded = True
                log("Second stop signal: asking the daemon to exit without waiting for runs in progress.")
                try:
                    child.send_signal(signal.SIGTERM)
                except OSError:
                    pass
            if time.monotonic() >= next_report:
                next_report += args.wait_log_interval
                log(f"Supervisor {os.getpid()} still waiting for daemon {child.pid} to exit "
                    f"({time.monotonic() - started:.0f}s).")

    env = {**os.environ, "PYTHONPATH": str(ROOT / "src") + os.pathsep + os.environ.get("PYTHONPATH", "")}
    errors = (OSError, subprocess.SubprocessError)
    while not signals:
        child = subprocess.Popen([sys.executable, "-m", "agent_runtime.app.main", *daemon_args], env=env, start_new_session=True)
        update = None
        try:
            next_check = time.monotonic() + args.interval
            while not signals:
                remaining = next_check - time.monotonic()
                if remaining > 0:
                    try:
                        child.wait(timeout=min(remaining, TICK))
                    except subprocess.TimeoutExpired:
                        continue
                    return child.returncode if child.returncode >= 0 else 128 - child.returncode
                try:
                    update = candidate()
                except errors as exc:
                    log(f"Update check failed: {exc}")
                if update:
                    log(f"Draining daemon for {update[0][:12]} -> {update[1][:12]}.")
                    break
                next_check = time.monotonic() + args.interval
        finally:
            stop(child)

        if signals or update is None:
            break
        old, new = update
        try:
            # Recheck after drain: an operator may have changed the checkout.
            if not clean_main() or git("rev-parse", "HEAD") != old:
                log("Checkout changed during drain; skipping update.")
                continue
            git("merge", "--ff-only", "--no-overwrite-ignore", new)
        except errors as exc:
            log(f"Update failed: {exc}; restarting daemon from current checkout.")
            continue
        if not signals:
            log(f"Starting {new[:12]}.")
            os.execv(sys.executable, [sys.executable, *sys.orig_argv[1:]])
    return 0


if __name__ == "__main__":
    sys.exit(main())
