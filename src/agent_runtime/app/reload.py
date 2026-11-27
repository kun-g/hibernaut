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
        self._latest = ""
        self._settled = ""
        self._error: str | None = None
        self._active = ""
        self._rejection: tuple[str, str] | None = None

    @staticmethod
    def _digest(contents: bytes) -> str:
        return hashlib.sha256(contents).hexdigest()

    def on_disk(self, version: str) -> bool:
        path = self.path
        try:
            return path is not None and self._digest(path.read_bytes()) == version
        except OSError:
            return False

    def settled(self, version: str) -> bool:
        with self._changed:
            return self._settled == version

    def watch(self, path: Path | None) -> None:
        with self._changed:
            self._watched = path.resolve() if path is not None else None
            self._changed.notify_all()

    @property
    def path(self) -> Path | None:
        with self._changed:
            return self._watched

    @property
    def latest(self) -> str:
        with self._changed:
            return self._latest

    def seen(self, contents: bytes) -> str:
        with self._changed:
            self._latest, self._settled, self._error = self._digest(contents), "", None
            self._changed.notify_all()
            return self._latest

    def settle(self, version: str, error: str | None = None) -> None:
        # That version is in force, or was refused for this reason.
        with self._changed:
            self._settled, self._error = version, error
            if error is None:
                self._active, self._rejection = version, None
            else:
                self._rejection = (version, error)
            self._changed.notify_all()

    def rejection(self) -> dict[str, str] | None:
        with self._changed:
            if self._watched is None or self._rejection is None:
                return None
            version, error = self._rejection
            return {"path": str(self._watched), "active_sha256": self._active,
                    "rejected_sha256": version, "error": error}

    def wait(self, contents: bytes, timeout: float = RELOAD_TIMEOUT_SECONDS) -> None:
        wanted = self._digest(contents)
        deadline = time.monotonic() + timeout
        was_seen = False
        with self._changed:
            while not self._latest == self._settled == wanted:
                if was_seen and self._latest != wanted:
                    raise ReloadFailed("another version of it came after")
                was_seen = self._latest == wanted
                left = deadline - time.monotonic()
                if self._watched is None or left <= 0:
                    raise ReloadFailed(f"the running Hibernaut did not take it up within {timeout:g} seconds")
                self._changed.wait(left)
            if self._error is not None:
                raise ReloadFailed(self._error)
