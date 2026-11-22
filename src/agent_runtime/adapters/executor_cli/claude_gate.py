#!/usr/bin/env python3
# Runs inside a visible Orca terminal: applies the billing environment, checks the session the launcher
# would open exactly as a headless run does, then replaces itself with the interactive Claude Code.
# Started as `python -I <this file> <spec.json>`, so it never imports code from the worktree it runs in.
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from agent_runtime.adapters.executor_cli.claude_code import (  # noqa: E402
    ClaudeCodeError,
    ClaudeCodeLauncher,
    ClaudeCodeProfile,
    check_init,
    probe_init,
)


def _report(path: Path, body: dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(body), encoding="utf-8")
    os.replace(tmp, path)


async def _probe(argv: list[str], env: dict[str, str], timeout: float) -> dict[str, Any]:
    # A closed terminal (SIGHUP/SIGTERM) cancels the probe, so probe_init kills the harness it started instead of
    # leaving it behind this process.
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGHUP, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    return await probe_init(argv, cwd=Path.cwd(), env=env, timeout_seconds=timeout)


def main(spec_path: str) -> int:
    spec: dict[str, Any] = json.loads(Path(spec_path).read_text(encoding="utf-8"))
    status = Path(spec["status"])
    env = {k: v for k, v in os.environ.items() if k not in spec["unset"]}
    env.update(spec["set"])
    p = spec["profile"]
    profile = ClaudeCodeProfile(
        name=p["name"],
        launcher=ClaudeCodeLauncher(name=p["launcher"], command=p["launcher_command"]),
        billing=p["billing"],
        model=p["model"],
        effort=p["effort"],
        credential_proxy_url=p["credential_proxy_url"],
    )
    try:
        init = asyncio.run(_probe(spec["probe"], env, spec["probe_timeout"]))
        check_init(profile, init)
    except asyncio.CancelledError:
        print(f"[hibernaut] {spec['title']}: terminal closed during the session check", file=sys.stderr, flush=True)
        return 1
    except ClaudeCodeError as exc:
        _report(status, {"ok": False, "error": str(exc)})
        print(f"[hibernaut] {profile.name}: {exc}", file=sys.stderr, flush=True)
        return 1
    _report(status, {"ok": True})
    print(f"[hibernaut] {spec['title']}: starting {p['launcher']} ({p['billing']}, {p['model']}, effort {p['effort']})",
          flush=True)
    argv: list[str] = spec["exec"]
    os.execvpe(argv[0], argv, env)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
