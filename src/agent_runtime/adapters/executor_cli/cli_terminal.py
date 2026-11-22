#!/usr/bin/env python3
# Runs inside a visible Orca terminal: starts the CLI from the spec file, shows its stdout and stderr as they come
# and copies both into the run's files, then writes the exit code. Hibernaut reads only those files, never the screen.
# Started as `python -I <this file> <spec.json>` and uses only the standard library, so it imports nothing from the
# worktree it runs in.
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import IO, Any


def _write_atomic(path: Path, body: dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(body), encoding="utf-8")
    os.replace(tmp, path)


def _tee(source: IO[bytes], screen: IO[bytes], path: str) -> None:
    with open(path, "wb") as copy:
        while chunk := os.read(source.fileno(), 65536):
            copy.write(chunk)
            copy.flush()
            screen.write(chunk)
            screen.flush()


def main(spec_path: str) -> int:
    spec: dict[str, Any] = json.loads(Path(spec_path).read_text(encoding="utf-8"))
    argv: list[str] = spec["argv"]
    exit_path = Path(spec["exit.json"])
    print(f"[hibernaut] {spec['title']}: running {argv[0]} in {spec['cwd']}", flush=True)
    try:
        proc = subprocess.Popen(argv, cwd=spec["cwd"], env={**os.environ, **spec["env"]},
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as exc:
        _write_atomic(exit_path, {"exit": None, "error": f"cannot start '{argv[0]}': {exc.strerror or exc}"})
        print(f"[hibernaut] cannot start {argv[0]}: {exc.strerror or exc}", file=sys.stderr, flush=True)
        return _hold()
    assert proc.stdout is not None and proc.stderr is not None
    threads = [
        threading.Thread(target=_tee, args=(proc.stdout, sys.stdout.buffer, spec["stdout"])),
        threading.Thread(target=_tee, args=(proc.stderr, sys.stderr.buffer, spec["stderr"])),
    ]
    for thread in threads:
        thread.start()
    code = proc.wait()
    for thread in threads:
        thread.join()
    # Written last: its presence means both copies are complete.
    _write_atomic(exit_path, {"exit": code})
    print(f"\n[hibernaut] {argv[0]} exited with code {code}.", flush=True)
    return _hold()


def _hold() -> int:
    # The terminal stays open so a person can read the output; Hibernaut closes it when the task is delivered
    # (close_terminal), or a person can press Enter.
    print("[hibernaut] This terminal closes when the task is delivered, or press Enter to close it now.", flush=True)
    try:
        sys.stdin.readline()
    except (KeyboardInterrupt, OSError):
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
