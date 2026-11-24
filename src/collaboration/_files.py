from __future__ import annotations

import fcntl
import os
import tempfile
import threading
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="") as temp:
            temp.write(text)
            temp.flush()
            os.fsync(temp.fileno())
        os.replace(name, path)
    except BaseException:
        Path(name).unlink(missing_ok=True)
        raise


class ExclusiveLock:
    # Threads of this process and other processes (the stdio MCP companion runs beside the daemon) take turns.
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._path = path
        self._thread_lock = threading.RLock()
        self._held = 0

    @contextmanager
    def hold(self) -> Generator[None]:
        with self._thread_lock:
            if self._held:
                self._held += 1
                try:
                    yield
                finally:
                    self._held -= 1
                return
            with self._path.open("a") as lock_file:
                fcntl.flock(lock_file, fcntl.LOCK_EX)
                self._held = 1
                try:
                    yield
                finally:
                    self._held = 0
