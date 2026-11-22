import argparse
import os
import signal
import subprocess
import sys
import threading
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the daemon and fast-forward from origin/main. Extra flags go to the daemon.", allow_abbrev=False)
    parser.add_argument("--interval", type=float, default=300, help="seconds between checks (default: 300)")
    args, daemon_args = parser.parse_known_args()
    if args.interval <= 0:
        parser.error("--interval must be positive")

    stopping = threading.Event()
    child: subprocess.Popen[bytes] | None = None
    signals = 0

    def on_signal(*_: object) -> None:
        # The first signal stops the daemon, which drains its Runs in progress; a second one is passed on so the
        # daemon exits without waiting for them.
        nonlocal signals
        signals += 1
        stopping.set()
        if signals > 1 and child is not None and child.returncode is None:
            log("Second stop signal: asking the daemon to exit without waiting for runs in progress.")
            try:
                child.send_signal(signal.SIGTERM)
            except OSError:
                pass

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, on_signal)

    env = {**os.environ, "PYTHONPATH": str(ROOT / "src") + os.pathsep + os.environ.get("PYTHONPATH", "")}
    errors = (OSError, subprocess.SubprocessError)
    while not stopping.is_set():
        child = subprocess.Popen([sys.executable, "-m", "agent_runtime.app.main", *daemon_args], env=env, start_new_session=True)
        update = None
        try:
            next_check = time.monotonic() + args.interval
            while not stopping.is_set():
                if child.poll() is not None:
                    return child.returncode if child.returncode >= 0 else 128 - child.returncode
                remaining = next_check - time.monotonic()
                if remaining > 0:
                    stopping.wait(min(remaining, 1))
                    continue
                try:
                    update = candidate()
                except errors as exc:
                    log(f"Update check failed: {exc}")
                if update:
                    log(f"Draining daemon for {update[0][:12]} -> {update[1][:12]}.")
                    break
                next_check = time.monotonic() + args.interval
        finally:
            if child.poll() is None:
                # The daemon keeps serving ingress while it drains; the update starts only once it has exited.
                child.terminate()
                child.wait()

        if stopping.is_set() or update is None:
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
        if not stopping.is_set():
            log(f"Starting {new[:12]}.")
            os.execv(sys.executable, [sys.executable, *sys.orig_argv[1:]])
    return 0


if __name__ == "__main__":
    sys.exit(main())
