from __future__ import annotations

import contextlib
import json
import os
import tempfile
from collections.abc import Callable, Generator, Mapping
from pathlib import Path
from typing import Any, cast

from agent_runtime.core.ports import RunContext

STATE_FILE = "state.json"
_MAX_STATE_BYTES = 8 * 1024 * 1024


def write_state(run_dir: str | Path, data: Mapping[str, Any]) -> None:
    # Atomic: a reader sees the previous state or this one, never a partial file.
    directory = Path(run_dir)
    fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, directory / STATE_FILE)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def read_state(run_dir: str | Path, format_version: int) -> tuple[dict[str, Any] | None, str]:
    # (state, "") or (None, why it cannot be used).
    path = Path(run_dir) / STATE_FILE
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None, f"{path} is missing"
    except OSError as exc:
        return None, f"{path} cannot be read ({exc.strerror})"
    if len(raw) > _MAX_STATE_BYTES:
        return None, f"{path} is larger than {_MAX_STATE_BYTES} bytes"
    try:
        data: object = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, f"{path} is not valid JSON"
    if not isinstance(data, dict):
        return None, f"{path} is not a JSON object"
    state = cast(dict[str, Any], data)
    if state.get("format") != format_version:
        return None, f"{path} has format {state.get('format')!r}, this version reads format {format_version}"
    return state, ""


class TurnSlot:
    # One phase's link to the run directory for the agent turn it waits on. The runner records the live turn before
    # it sends the task, and on resume takes the recorded one to wait for its result instead of sending it again.
    def __init__(
        self,
        run: RunContext,
        saved: Mapping[str, Any] | None,
        persist: Callable[[dict[str, Any] | None], None],
        notes: list[str] | None = None,
    ) -> None:
        self.run = run
        self._saved = dict(saved) if saved is not None else None
        self._persist = persist
        self.notes = notes if notes is not None else []

    @property
    def turns_dir(self) -> Path:
        path = Path(self.run.run_dir) / "turns"
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        return path

    def peek(self, label: str) -> dict[str, Any] | None:
        saved = self._saved
        return dict(saved) if saved is not None and saved.get("label") == label else None

    def take(self, label: str) -> dict[str, Any] | None:
        # The recorded turn for this label, once; any later turn of the phase starts fresh.
        found = self.peek(label)
        self._saved = None
        return found

    def record(self, label: str, turn: Mapping[str, Any]) -> None:
        self._persist({**turn, "label": label})

    def note(self, text: str) -> None:
        self.notes.append(text)

    @contextlib.contextmanager
    def parked(self) -> Generator[None]:
        with self.run.detachable():
            yield


def parked(slot: TurnSlot | None) -> contextlib.AbstractContextManager[None]:
    return slot.parked() if slot is not None else contextlib.nullcontext()


def detached(slot: TurnSlot | None) -> bool:
    return slot is not None and slot.run.detached()
