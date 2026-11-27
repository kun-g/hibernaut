#!/usr/bin/env python3
# Runs inside a visible Orca terminal: applies the billing environment, checks the session the launcher would open
# exactly as a headless run does, then runs the interactive Claude Code as a child in the terminal's foreground and
# exits with it, so the terminal still ends with the agent.
#
# Orca can report a terminal closed without its signal ever reaching it, so the gate supervises: the probe and the
# agent each run in a process group of their own, ended with everything in it on SIGHUP/SIGTERM/SIGINT or when
# Hibernaut removes this terminal's lease file (it does so whenever it closes the terminal), and the probe at its
# deadline. Started as `python -I <this file> <spec.json>`, so it never imports code from the worktree it runs in.
#
# With "once" in the spec it checks a one-off command instead, inside cli_terminal.py: it reports only by its exit code
# and stderr, prints nothing on the command's stdout, and becomes the command, which that wrapper then supervises.
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, cast

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from agent_runtime.adapters.executor_cli.claude_code import (
    PROBE_PROMPT,
    ClaudeCodeError,
    ClaudeCodeLauncher,
    ClaudeCodeProfile,
    check_init,
)
from agent_runtime.adapters.executor_cli.cli_terminal import (
    GRACE_SECONDS,
    check_stop,
    end_group,
    exited,
    one_shot_env,
    starting,
    supervised,
)

_POLL_SECONDS = 0.1


class _Released(Exception):
    pass


def _report(path: Path, body: dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(body), encoding="utf-8")
    os.replace(tmp, path)


def main(spec_path: str) -> int:
    spec: dict[str, Any] = {}

    def body(running: list[subprocess.Popen[bytes]]) -> int:
        spec.update(json.loads(Path(spec_path).read_text(encoding="utf-8")))
        try:
            return _main(spec, running)
        except _Released:
            print("\n[hibernaut] Hibernaut closed this terminal; ending it.", file=sys.stderr, flush=True)
            return 1

    # Only the probe can be running in a one-off check; cli_terminal.py ends this process after its own grace.
    return supervised(body, grace=lambda: 0 if spec.get("once") else None)


def _main(spec: dict[str, Any], running: list[subprocess.Popen[bytes]]) -> int:
    once = spec.get("once") is True
    lease = Path(spec["lease"])
    env = {k: v for k, v in os.environ.items() if k not in spec["unset"]}
    env.update(spec["set"])
    if once:
        # The wrapper stripped what it inherited; the configured environment must not bring it back.
        env = one_shot_env(env)
    p = spec["profile"]
    # A daemon started before this file was updated writes specs without the env fields.
    profile = ClaudeCodeProfile(
        name=p["name"],
        launcher=ClaudeCodeLauncher(name=p["launcher"], command=p["launcher_command"], env=p.get("launcher_env", {})),
        billing=p["billing"],
        model=p["model"],
        effort=p["effort"],
        env=p.get("env", {}),
        credential_proxy_url=p["credential_proxy_url"],
    )
    try:
        check_init(profile, _probe(spec["probe"], env, spec["probe_timeout"], lease, running))
    except ClaudeCodeError as exc:
        if not once:
            _report(Path(spec["status"]), {"ok": False, "error": str(exc)})
        print(f"[hibernaut] {profile.name}: {exc}", file=sys.stderr, flush=True)
        return 1
    if once:
        return _become(spec["exec"], env, lease)
    _report(Path(spec["status"]), {"ok": True})
    print(f"[hibernaut] {spec['title']}: starting {p['launcher']} ({p['billing']}, {p['model']}, effort {p['effort']})",
          flush=True)
    return _supervise(spec["exec"], env, lease, running)


def _probe(argv: list[str], env: dict[str, str], timeout: float, lease: Path,
           running: list[subprocess.Popen[bytes]]) -> dict[str, Any]:
    # Starts the harness headless and stops it at its init message, before any model request matters.
    try:
        with starting():
            proc = subprocess.Popen(argv, env=env, start_new_session=True,
                                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            running.append(proc)
    except OSError as exc:
        raise ClaudeCodeError(f"cannot start launcher ({argv[0]}): {exc.strerror or exc}") from exc
    assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None
    try:
        proc.stdin.write(PROBE_PROMPT.encode())
        proc.stdin.close()
    except (BrokenPipeError, ConnectionResetError):
        pass
    found: list[dict[str, Any]] = []
    errors = bytearray()
    stdout, stderr = proc.stdout, proc.stderr

    def read_init() -> None:
        for line in stdout:
            try:
                parsed: object = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                message = cast(dict[str, Any], parsed)
                if message.get("type") == "system" and message.get("subtype") == "init":
                    found.append(message)
                    return

    def read_errors() -> None:
        while chunk := stderr.read(65536):
            errors.extend(chunk)

    reader = threading.Thread(target=read_init, daemon=True)
    drain = threading.Thread(target=read_errors, daemon=True)
    reader.start()
    drain.start()
    deadline = time.monotonic() + timeout
    while not found:
        if exited(proc.pid, _POLL_SECONDS):
            reader.join(timeout=GRACE_SECONDS)
            break
        check_stop()
        if not lease.exists():
            raise _Released
        if time.monotonic() >= deadline:
            raise ClaudeCodeError(f"no session report within {timeout}s")
    end_group(proc, grace=0)
    running.remove(proc)
    drain.join(timeout=GRACE_SECONDS)
    if not found:
        text = errors.decode("utf-8", errors="replace").strip()
        raise ClaudeCodeError(f"exited ({proc.returncode}) before reporting its session: {text[-2000:]}")
    return found[0]


def _become(argv: list[str], env: dict[str, str], lease: Path) -> int:
    check_stop()
    if not lease.exists():
        raise _Released
    # Python ignores these, and an ignored signal stays ignored in the program that takes this one's place.
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    signal.signal(signal.SIGXFSZ, signal.SIG_DFL)
    try:
        os.execvpe(argv[0], argv, env)
    except OSError as exc:
        print(f"[hibernaut] cannot start {argv[0]}: {exc.strerror or exc}", file=sys.stderr, flush=True)
        return 127


def _supervise(argv: list[str], env: dict[str, str], lease: Path, running: list[subprocess.Popen[bytes]]) -> int:
    try:
        with starting():
            proc = subprocess.Popen(argv, env=env, process_group=0)
            running.append(proc)
    except OSError as exc:
        print(f"[hibernaut] cannot start {argv[0]}: {exc.strerror or exc}", file=sys.stderr, flush=True)
        return 127
    if sys.stdin.isatty():
        # The agent's group takes the terminal's foreground, as a shell hands it to a job; it may already have
        # stopped on reading the terminal before that, so it is continued.
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        try:
            os.tcsetpgrp(sys.stdin.fileno(), proc.pid)
            os.killpg(proc.pid, signal.SIGCONT)
        except OSError:
            pass
    while not exited(proc.pid, _POLL_SECONDS):
        check_stop()
        if not lease.exists():
            raise _Released
    # Exit with the agent, after ending whatever it left in its group.
    end_group(proc, grace=0)
    running.remove(proc)
    code = proc.returncode
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
