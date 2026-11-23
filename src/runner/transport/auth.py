from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Final

MIN_TOKEN_CHARS: Final = 32


def read_relay_token(path: Path) -> str:
    # Both ends of the Relay read the token from an owner-only file, never from an environment variable that a child
    # process could inherit. Messages name the file, never its contents.
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC)  # a FIFO must not block here
        try:
            info = os.fstat(fd)
            data = os.read(fd, 4096)
        finally:
            os.close(fd)
    except OSError as exc:
        raise ValueError(f"relay token file {path} cannot be read ({exc.strerror})") from None
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError(f"relay token file {path} must be a regular file owned by this user")
    if info.st_mode & 0o077:
        raise ValueError(f"relay token file {path} is readable by group or others; run chmod 600 on it")
    token = data.decode("ascii", "replace").strip()
    if len(token) < MIN_TOKEN_CHARS or not token.isascii() or not token.isprintable() or " " in token:
        raise ValueError(f"relay token file {path} must hold one printable ASCII token of at least {MIN_TOKEN_CHARS} characters")
    return token
