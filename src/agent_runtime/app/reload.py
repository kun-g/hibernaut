from __future__ import annotations

import hashlib
import threading
import time
from pathlib import Path

RELOAD_TIMEOUT_SECONDS = 60.0


class ReloadFailed(Exception):
    pass


class ReloadStatus:
    # What became of each version of the configuration file. Shared by every generation; safe from any thread.
    def __init__(self) -> None:
        self._changed = threading.Condition()
        self._watched: Path | None = None
        self._version: str | None = None
        self._settled = False
        self._error: str | None = None

    @staticmethod
    def _digest(contents: bytes) -> str:
        return hashlib.sha256(contents).hexdigest()

    def watch(self, path: Path | None) -> None:
        with self._changed:
            self._watched = path.resolve() if path is not None else None
            self._changed.notify_all()

    @property
    def path(self) -> Path | None:
        with self._changed:
            return self._watched

    def seen(self, contents: bytes) -> None:
        with self._changed:
            self._version, self._settled, self._error = self._digest(contents), False, None

    def settle(self, error: str | None = None) -> None:
        # The version seen last is in force, or was refused for this reason.
        with self._changed:
            self._settled, self._error = True, error
            self._changed.notify_all()

    def wait(self, contents: bytes, timeout: float = RELOAD_TIMEOUT_SECONDS) -> None:
        wanted = self._digest(contents)
        deadline = time.monotonic() + timeout
        with self._changed:
            while not (self._version == wanted and self._settled):
                left = deadline - time.monotonic()
                if self._watched is None or left <= 0:
                    raise ReloadFailed(f"the running Hibernaut did not take it up within {timeout:g} seconds")
                self._changed.wait(left)
            if self._error is not None:
                raise ReloadFailed(self._error)
