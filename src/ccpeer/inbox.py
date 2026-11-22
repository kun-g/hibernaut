from __future__ import annotations

import json
import os
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from ccpeer.errors import CursorError

Cursor = int | str | None


@dataclass(frozen=True, slots=True)
class InboxPage:
    records: list[dict[str, Any]]
    # Byte offset just past the last returned record; pass it back to continue.
    cursor: int


class JsonlLog:
    """Append-only JSONL file. Readers only ever see complete lines, so the cursor is always a line boundary."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def repair(self) -> None:
        # A writer killed mid-append leaves a line without "\n"; drop it so the next append starts clean.
        try:
            with open(self.path, "rb+") as f:
                data = f.read()
                if data and not data.endswith(b"\n"):
                    f.truncate(data.rfind(b"\n") + 1)
        except FileNotFoundError:
            pass

    def append(self, record: Mapping[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False).encode("utf-8") + b"\n"
        fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            view = memoryview(line)
            while view:
                view = view[os.write(fd, view):]
            os.fsync(fd)
        finally:
            os.close(fd)

    def _complete(self) -> bytes:
        try:
            data = self.path.read_bytes()
        except FileNotFoundError:
            return b""
        return data[: data.rfind(b"\n") + 1]

    def msg_ids(self) -> set[str]:
        ids: set[str] = set()
        for _, rec in self._iter(self._complete(), 0):
            msg_id = rec.get("msg_id")
            if isinstance(msg_id, str):
                ids.add(msg_id)
        return ids

    @staticmethod
    def _iter(data: bytes, start: int) -> list[tuple[int, dict[str, Any]]]:
        out: list[tuple[int, dict[str, Any]]] = []
        pos = start
        while pos < len(data):
            end = data.index(b"\n", pos) + 1
            try:
                rec = json.loads(data[pos:end])
            except ValueError:
                rec = None
            if isinstance(rec, dict):
                out.append((end, cast(dict[str, Any], rec)))
            pos = end
        return out

    def read(self, cursor: Cursor = None, limit: int | None = None) -> InboxPage:
        data = self._complete()
        start = self._start(data, cursor)
        entries = self._iter(data, start)
        if limit is not None:
            entries = entries[:limit]
        end = entries[-1][0] if entries else start
        if limit is None:
            end = len(data)
        return InboxPage(records=[rec for _, rec in entries], cursor=end)

    def _start(self, data: bytes, cursor: Cursor) -> int:
        if cursor is None:
            return 0
        if isinstance(cursor, int):
            if cursor < 0 or cursor > len(data) or (cursor > 0 and data[cursor - 1:cursor] != b"\n"):
                raise CursorError(f"offset {cursor} is not a record boundary in {self.path} ({len(data)} bytes)")
            return cursor
        for end, rec in self._iter(data, 0):
            if rec.get("msg_id") == cursor:
                return end
        raise CursorError(f"msg_id {cursor!r} not found in {self.path}")


class Inbox:
    """A receiver's mailbox: messages in inbox.jsonl, control receipts (e.g. `held`) in control.jsonl."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.messages = JsonlLog(directory / "inbox.jsonl")
        self.control = JsonlLog(directory / "control.jsonl")
        self._lock = threading.Lock()
        self._seen: set[str] | None = None

    def prepare(self) -> None:
        with self._lock:
            self._seen_ids()

    def _seen_ids(self) -> set[str]:
        if self._seen is None:
            self.directory.mkdir(parents=True, exist_ok=True)
            self.messages.repair()
            self.control.repair()
            self._seen = self.messages.msg_ids() | self.control.msg_ids()
        return self._seen

    def record(self, record: Mapping[str, Any]) -> bool:
        """Append unless this msg_id was already stored; returns whether it was appended."""
        msg_id = record.get("msg_id")
        log = self.control if record.get("type") == "control" else self.messages
        with self._lock:
            seen = self._seen_ids()
            if isinstance(msg_id, str):
                if msg_id in seen:
                    return False
                seen.add(msg_id)
            log.append(record)
        return True
