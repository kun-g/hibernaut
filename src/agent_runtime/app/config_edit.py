from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import secrets
import threading
import time
import tomllib
from collections.abc import Callable, Generator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

LOCK_TIMEOUT_SECONDS = 60.0
_BARE_KEY = re.compile(r"[A-Za-z0-9_-]+")
_STRING_ENDS = ('"""', "'''", '"', "'")


class EditRefused(ValueError):
    # The file is laid out in a way this code does not edit; a person has to.
    pass


class ConfigChanged(Exception):
    # Somebody else is changing the file.
    pass


def quoted(value: str) -> str:
    # Printable text is the same in a JSON and a TOML basic string.
    if not value.isprintable():
        raise ValueError(f"{value!r} holds a line break or a control character")
    return json.dumps(value, ensure_ascii=False)


def toml_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return quoted(value)
    if isinstance(value, (list, tuple)):
        return f"[{', '.join(toml_value(item) for item in cast(Sequence[object], value))}]"
    raise TypeError(f"no TOML for {type(value).__name__}")


def toml_key(name: str) -> str:
    return name if _BARE_KEY.fullmatch(name) else quoted(name)


@dataclass(frozen=True, slots=True)
class _Part:
    # A table header or a key with its value: text[start:end], the line break included.
    start: int
    end: int
    path: tuple[str, ...]
    header: bool
    array: bool = False
    # Where the value of a key starts.
    value: int = 0


def _string_end(text: str, at: int) -> int:
    quote = next(q for q in _STRING_ENDS if text.startswith(q, at))
    i = at + len(quote)
    while i < len(text):
        if text[i] == "\\" and quote[0] == '"':
            i += 2
        elif text.startswith(quote, i):
            i += len(quote)
            # Up to two quotes may stand right before the closing three.
            while len(quote) == 3 and text.startswith(quote[0], i):
                i += 1
            return i
        else:
            i += 1
    raise EditRefused("a string is not closed")


def _line_end(text: str, at: int) -> int:
    found = text.find("\n", at)
    return len(text) if found < 0 else found + 1


def _key_path(source: str) -> tuple[tuple[str, ...], bool]:
    # A header or "key =" as TOML itself reads it.
    node: object = tomllib.loads(source)
    path: list[str] = []
    array = False
    while isinstance(node, dict) and node:
        key, node = next(iter(cast(dict[str, object], node).items()))
        path.append(key)
        if isinstance(node, list):
            array, node = True, None
    return tuple(path), array


def _parts(text: str) -> list[_Part]:
    parts: list[_Part] = []
    i = 0
    while i < len(text):
        start = i
        while i < len(text) and text[i] in " \t":
            i += 1
        if i >= len(text) or text[i] in "\r\n#":
            i = _line_end(text, i)
            continue
        if text[i] == "[":
            i = _line_end(text, i)
            path, array = _key_path(text[start:i])
            parts.append(_Part(start, i, path, header=True, array=array))
            continue
        while text[i] != "=":
            i = _string_end(text, i) if text[i] in "\"'" else i + 1
        path, _ = _key_path(f"{text[start:i]} = 0")
        value, depth = i + 1, 0
        i = value
        while i < len(text) and (depth or text[i] != "\n"):
            if text[i] == "#":
                i = _line_end(text, i) - (1 if text.find("\n", i) >= 0 else 0)
            elif text[i] in "\"'":
                i = _string_end(text, i)
            else:
                depth += (text[i] in "[{") - (text[i] in "]}")
                i += 1
        i = min(i + 1, len(text))
        parts.append(_Part(start, i, path, header=False, value=value))
    return parts


class TomlEdit:
    # Adds values to a TOML file and leaves every byte of it in place. result() refuses anything but those values.
    def __init__(self, text: str) -> None:
        self._text = text
        self._before: dict[str, Any] = tomllib.loads(text)
        self._break = "\r\n" if "\r\n" in text else "\n"
        self._inserted: list[tuple[int, str]] = []
        self._changes: list[Callable[[dict[str, Any]], None]] = []
        try:
            self._parts = _parts(text)
        except (IndexError, tomllib.TOMLDecodeError) as exc:
            raise EditRefused("the file could not be told apart into its tables") from exc

    def _insert(self, at: int, lines: str) -> None:
        # Whole lines, at the start of one.
        if at and not self._text.startswith("\n", at - 1):
            lines = f"\n{lines}"
        self._inserted.append((at, lines.replace("\n", self._break)))

    def _header(self, path: tuple[str, ...]) -> _Part:
        found = [p for p in self._parts if p.header and not p.array and p.path == path]
        if len(found) != 1:
            raise EditRefused(f"[{'.'.join(path)}] is not written as a table of its own")
        return found[0]

    def add_key(self, path: tuple[str, ...], key: str, value: object) -> None:
        self._insert(self._header(path).end, f"{toml_key(key)} = {toml_value(value)}\n")
        self._changes.append(lambda data: _table(data, path).update({key: value}))

    def add_table(self, path: tuple[str, ...], name: str, values: Mapping[str, object]) -> None:
        # One more [[path.name]], behind everything written under path.
        self._header(path)
        end, inside = 0, False
        for part in self._parts:
            if part.header:
                inside = part.path[: len(path)] == path
            if inside:
                end = part.end
        header = ".".join(map(toml_key, (*path, name)))
        body = "".join(f"{toml_key(k)} = {toml_value(v)}\n" for k, v in values.items())
        self._insert(end, f"\n[[{header}]]\n{body}")
        self._changes.append(lambda data: _table(data, path).setdefault(name, []).append(_plain(values)))

    def append_item(self, path: tuple[str, ...], index: int, key: str, item: str) -> None:
        # One more string in a list of the index-th [[path]].
        headers = [n for n, p in enumerate(self._parts) if p.header and p.array and p.path == path]
        if index >= len(headers):
            raise EditRefused(f"[[{'.'.join(path)}]] number {index + 1} is not written as a table of its own")
        found: _Part | None = None
        for part in self._parts[headers[index] + 1:]:
            if part.header:
                break
            if part.path == (key,):
                found = part
        if found is None:
            raise EditRefused(f"{key} of [[{'.'.join(path)}]] number {index + 1} is not written there")
        at, inserted = self._list_end(found, quoted(item))
        self._inserted.append((at, inserted))
        self._changes.append(lambda data: _array(data, path)[index][key].append(item))

    def _list_end(self, part: _Part, item: str) -> tuple[int, str]:
        text, i, last, start = self._text, part.value, -1, -1
        comma = False
        while i < part.end:
            char = text[i]
            if char == "#":
                i = _line_end(text, i)
            elif char in "\"'":
                start, i = i, _string_end(text, i)
                last, comma = i, False
            elif char == "]":
                break
            else:
                comma = comma or char == ","
                i += 1
        if last < 0:
            raise EditRefused("an empty list is left to a person")
        line = text.rfind("\n", 0, start) + 1
        indent = text[line:start]
        if comma and text.find("\n", last, i) >= 0 and not indent.strip():
            return _line_end(text, last), f"{indent}{item},{self._break}"
        return last, f", {item}"

    def append_text(self, block: str) -> None:
        # The block holds whole tables the file does not have yet.
        added = tomllib.loads(block)
        self._insert(len(self._text), block)
        self._changes.append(lambda data: _merge(data, added))

    @property
    def added(self) -> str:
        return "".join(text for _, text in sorted(self._inserted, key=lambda found: found[0])).replace("\r\n", "\n")

    def result(self) -> str:
        pieces: list[str] = []
        done = 0
        # In the order they were asked for where they share a place.
        for at, inserted in sorted(self._inserted, key=lambda found: found[0]):
            pieces += [self._text[done:at], inserted]
            done = at
        text = "".join([*pieces, self._text[done:]])
        expected = tomllib.loads(self._text)
        for change in self._changes:
            change(expected)
        try:
            found = tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            raise EditRefused(f"the change does not fit the file as it is written: {exc}") from exc
        if found != expected:
            raise EditRefused("the change would touch more than what it adds")
        return text


def _table(data: dict[str, Any], path: tuple[str, ...]) -> dict[str, Any]:
    for key in path:
        data = data[key]
    return data


def _array(data: dict[str, Any], path: tuple[str, ...]) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], _table(data, path[:-1])[path[-1]])


def _plain(values: Mapping[str, object]) -> dict[str, object]:
    return {k: list(cast(Sequence[object], v)) if isinstance(v, tuple) else v for k, v in values.items()}


def _merge(data: dict[str, Any], added: Mapping[str, Any]) -> None:
    for key, value in added.items():
        if key not in data:
            data[key] = value
        elif isinstance(data[key], dict) and isinstance(value, dict):
            _merge(data[key], cast(dict[str, Any], value))
        else:
            raise EditRefused(f"{key} is there already")


_LOCKS: dict[Path, threading.Lock] = {}
_LOCKS_GUARD = threading.Lock()


def _write_new(path: Path, data: bytes, mode: int) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fchmod(stream.fileno(), mode)
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


class ConfigFile:
    def __init__(self, path: Path) -> None:
        # A link stays a link: the file it names is replaced.
        self.path = path.resolve()

    @contextlib.contextmanager
    def locked(self, timeout: float = LOCK_TIMEOUT_SECONDS) -> Generator[None]:
        # Between the threads of this process and, through the directory, with the command line.
        busy = ConfigChanged(f"{self.path.name} is being changed by somebody else; try again later")
        with _LOCKS_GUARD:
            lock = _LOCKS.setdefault(self.path, threading.Lock())
        deadline = time.monotonic() + timeout
        if not lock.acquire(timeout=timeout):
            raise busy
        try:
            fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() >= deadline:
                            raise busy from None
                        time.sleep(0.05)
                yield
            finally:
                os.close(fd)
        finally:
            lock.release()

    def read(self) -> bytes:
        return self.path.read_bytes()

    def owns(self, path: Path) -> bool:
        # The file itself, its backups and what a write leaves while it runs.
        own, name = self.path.name.casefold(), path.name.casefold()
        return path.parent.resolve() == self.path.parent and (
            name == own or name.startswith((f"{own}.", f".{own}."))
        )

    def replace(self, original: bytes, updated: bytes, *, backup: bool = True) -> Path:
        # Returns the backup. Nothing is written unless the file still is `original`.
        stamp = f"{datetime.now():%Y%m%dT%H%M%S}-{secrets.token_hex(3)}"
        saved = self.path.with_name(f"{self.path.name}.bak-{stamp}")
        new = self.path.with_name(f".{self.path.name}.{stamp}.tmp")
        mode = self.path.stat().st_mode & 0o777
        try:
            if backup:
                # It holds whatever the file holds, so nobody else reads it.
                _write_new(saved, original, 0o600)
            _write_new(new, updated, mode)
            if self.path.read_bytes() != original:
                raise ConfigChanged(f"{self.path.name} was changed by somebody else meanwhile; nothing was written")
            os.replace(new, self.path)
        except BaseException:
            new.unlink(missing_ok=True)
            saved.unlink(missing_ok=True)
            raise
        self._sync_directory()
        return saved

    def _sync_directory(self) -> None:
        with contextlib.suppress(OSError):
            fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
